"""Compaction: fold staged deltas into period partitions, deduplicated and sorted.

Why this exists
---------------
Incremental runs produce one small file per run. At the measured arrival rate
(~272k observations/day across ~950 actively reporting histories) a daily run
writes roughly 1 MiB. Leaving those in place would give ~365 tiny files per year
in the hot partition -- the classic small-file problem, where a query spends all
its time on HTTP round trips and Parquet footers.

Compaction reads the base partition plus the deltas that land in it, resolves
duplicates last-write-wins on (station_id, variable_id, obs_time), re-sorts, and
rewrites target-sized files with the full encoding treatment.

DuckDB does the sort/dedupe because it spills to disk; pyarrow does the write
because the encoding knobs that matter (page index, bloom filters, explicit
DELTA_BINARY_PACKED on obs_time) are not reachable through DuckDB's COPY.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import uuid

import pyarrow as pa

from . import paths
from .config import Settings
from .pack import RollingWriter
from .partitioning import Layout
from .schema import OBSERVATIONS
from .storage import Store, duckdb_connect

log = logging.getLogger("pcds.compact")

EMPTY_SOURCE = (
    "SELECT NULL::INTEGER AS station_id, NULL::SMALLINT AS variable_id, "
    "NULL::TIMESTAMP AS obs_time, NULL::DOUBLE AS value, "
    "NULL::TIMESTAMP AS ingested_at WHERE false"
)


def dedupe_sql(base_glob: str | None, delta_glob: str | None, y0: int, y1: int) -> str:
    """Union base + delta, keep the newest row per key, sorted for writing.

    The base partition is stamped with the epoch so any delta row wins over it;
    within the deltas, `ingested_at` orders revisions.
    """
    base = (
        f"""SELECT station_id, variable_id, obs_time, value,
               CAST('1970-01-01' AS TIMESTAMP) AS ingested_at
        FROM read_parquet('{base_glob}', union_by_name=true)"""
        if base_glob
        else EMPTY_SOURCE
    )
    delta = (
        f"""SELECT station_id, variable_id, obs_time, value, ingested_at
        FROM read_parquet('{delta_glob}', union_by_name=true)
        WHERE EXTRACT(year FROM obs_time) BETWEEN {y0} AND {y1}"""
        if delta_glob
        else EMPTY_SOURCE
    )
    return f"""
WITH unioned AS (
    {base}
    UNION ALL
    {delta}
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY station_id, variable_id, obs_time
        ORDER BY ingested_at DESC
    ) AS rn
    FROM unioned
)
SELECT station_id::INTEGER AS station_id,
       variable_id::SMALLINT AS variable_id,
       obs_time,
       value
FROM ranked
WHERE rn = 1
ORDER BY station_id, variable_id, obs_time
"""


def _glob(store: Store, prefix: str) -> str:
    base = store.uri if store.uri.startswith("s3://") else store.root
    return f"{base.rstrip('/')}/{prefix.strip('/')}/**/*.parquet"


def _generation(path: str) -> str:
    """The generation tag in `part-<gen>-NNNNN.parquet`, or "" for a file
    written before generations existed (`part-NNNNN.parquet`)."""
    stem = path.rsplit("/", 1)[-1].removesuffix(".parquet")
    bits = stem.split("-")
    return bits[1] if len(bits) == 3 else ""


def _parquet_files(store: Store, prefix: str) -> list[str]:
    return [i.path for i in store.ls(prefix, recursive=True) if i.path.endswith(".parquet")]


def _committed_generation(store: Store, period: str) -> str | None:
    marker = paths.period_success(period)
    if not store.exists(marker):
        return None
    try:
        with store.fs.open_input_stream(store.join(marker)) as f:
            return json.loads(f.read().decode())["generation"]
    except (ValueError, KeyError, OSError):
        # An unreadable marker means we cannot tell which generation is current,
        # so leave every file alone rather than guess and delete the live one.
        return None


def _write_success(store: Store, period: str, generation: str, *, rows: int, files: int) -> None:
    payload = {
        "generation": generation,
        "rows": rows,
        "files": files,
        "completed_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
    }
    with store.fs.open_output_stream(store.join(paths.period_success(period))) as f:
        f.write(json.dumps(payload, indent=2).encode())


def reconcile_period(store: Store, period: str) -> int:
    """Roll a partition forward or back after an interrupted swap.

    `_SUCCESS` is written between "new files are all in place" and "old files
    are gone", so it is the commit point: whatever generation it names is the
    complete one, and any other generation present is either a half-moved new
    set (crash before the commit) or a superseded old set (crash after it).
    Both are removed. Returns the number of files dropped.
    """
    committed = _committed_generation(store, period)
    if committed is None:
        return 0
    stale = [p for p in _parquet_files(store, paths.period_prefix(period))
             if _generation(p) != committed]
    for path in stale:
        store.fs.delete_file(path)
    if stale:
        log.warning(
            "period=%s: dropped %d file(s) from an interrupted swap, keeping generation %s",
            period, len(stale), committed or "(legacy)",
        )
    return len(stale)


def compact_period(
    store: Store,
    settings: Settings,
    period: str,
    year_range: tuple[int, int],
    *,
    include_deltas: bool = True,
) -> dict:
    """Rewrite one period partition. Returns a small stats dict."""
    base_prefix = paths.period_prefix(period)
    # A previous run may have died mid-swap; settle that before reading, so the
    # dedupe never sees two generations of the same rows.
    reconcile_period(store, period)

    con = duckdb_connect(settings, store)
    con.execute("SET preserve_insertion_order=true;")

    has_base = bool(_parquet_files(store, base_prefix))
    delta_prefix = paths.DELTA
    has_delta = include_deltas and bool(store.ls(delta_prefix, recursive=True))
    if not has_base and not has_delta:
        return {"period": period, "rows": 0, "files": 0, "skipped": "empty"}

    sql = dedupe_sql(
        _glob(store, base_prefix) if has_base else None,
        _glob(store, delta_prefix) if has_delta else None,
        *year_range,
    )

    # The generation tag keeps the new files from colliding with the ones they
    # replace, which is what lets them be moved in before the old ones go. The
    # random suffix is load-bearing: a bare second-resolution timestamp repeats
    # when a period is recompacted twice in the same second, and the new file
    # then lands on the old one's path and is deleted as superseded.
    generation = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ") + uuid.uuid4().hex[:6]
    staging = f"{paths.COMPACT_STAGING}/period={period}"
    store.delete(staging)
    store.mkdirs(staging)
    writer = RollingWriter(store, staging, settings, name=f"part-{generation}")
    reader = con.execute(sql).fetch_record_batch(settings.row_group_rows)
    rows = 0
    try:
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            if batch.num_rows == 0:
                continue
            rows += batch.num_rows
            writer.write(pa.Table.from_batches([batch]).cast(OBSERVATIONS))
    finally:
        written = writer.close()
    con.close()

    if rows == 0:
        store.delete(staging)
        return {"period": period, "rows": 0, "files": 0, "skipped": "no rows"}

    # Swap. Object stores give no atomic directory rename, so a reader can catch
    # this mid-flight either way; the choice is what they see. Moving the new
    # generation in first and deleting the old one after means the worst case is
    # briefly duplicated rows rather than a partition that is empty or missing
    # half its data, and a crash can never destroy a period the deltas can no
    # longer rebuild. `_SUCCESS` is written between the two, so it names the
    # generation to keep if this dies partway; see reconcile_period.
    # Select what to drop by generation rather than by "everything that was here
    # before", so a file belonging to the generation being written can never end
    # up in the delete list however the names collide.
    superseded = [p for p in _parquet_files(store, base_prefix) if _generation(p) != generation]
    store.mkdirs(base_prefix)
    final = []
    for p in written:
        dest = store.join(base_prefix, p.rsplit("/", 1)[-1])
        store.fs.move(p, dest)
        final.append(dest)

    _write_success(store, period, generation, rows=rows, files=len(final))

    for path in superseded:
        store.fs.delete_file(path)
    store.delete(staging)
    log.info(
        "compacted period=%s: %d rows into %d files (generation %s, replaced %d)",
        period, rows, len(final), generation, len(superseded),
    )
    return {"period": period, "rows": rows, "files": len(final), "generation": generation}


def compact_all(
    store: Store, settings: Settings, layout: Layout, periods: list[str] | None = None
) -> list[dict]:
    by_name = {p.period: p for p in layout.periods}
    names = periods or sorted(by_name)
    out = []
    for name in names:
        p = by_name.get(name)
        span = (p.start_year, p.end_year) if p else (int(name[:4]), int(name[-4:]))
        out.append(compact_period(store, settings, name, span))
    return out


def clear_deltas(store: Store, before: dt.datetime | None = None) -> int:
    """Drop staged deltas once they have been folded in. Keep the most recent
    runs if `before` is given, so a failed compaction is recoverable."""
    removed = 0
    for info in store.ls(paths.DELTA, recursive=True):
        if before is not None and info.mtime and info.mtime.replace(tzinfo=None) >= before:
            continue
        store.fs.delete_file(info.path)
        removed += 1
    return removed
