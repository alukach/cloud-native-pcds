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
because the encoding knobs that matter (page index, per-column dictionary,
explicit DELTA_BINARY_PACKED on obs_time) are not reachable through DuckDB's
COPY.
"""

from __future__ import annotations

import datetime as dt
import logging

import pyarrow as pa
import pyarrow.parquet as pq

from . import paths
from .config import Settings
from .pack import RollingWriter
from .partitioning import Layout
from .schema import OBSERVATIONS, OBSERVATIONS_MONTHLY
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


def compact_period(
    store: Store,
    settings: Settings,
    period: str,
    year_range: tuple[int, int],
    *,
    include_deltas: bool = True,
) -> dict:
    """Rewrite one period partition. Returns a small stats dict."""
    con = duckdb_connect(settings, store)
    con.execute("SET preserve_insertion_order=true;")

    base_prefix = paths.period_prefix(period)
    has_base = bool(store.ls(base_prefix, recursive=True))
    delta_prefix = paths.DELTA
    has_delta = include_deltas and bool(store.ls(delta_prefix, recursive=True))
    if not has_base and not has_delta:
        return {"period": period, "rows": 0, "files": 0, "skipped": "empty"}

    sql = dedupe_sql(
        _glob(store, base_prefix) if has_base else None,
        _glob(store, delta_prefix) if has_delta else None,
        *year_range,
    )

    staging = f"{paths.COMPACT_STAGING}/period={period}"
    store.delete(staging)
    store.mkdirs(staging)
    writer = RollingWriter(store, staging, settings)
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

    # Swap. Move the new files in first, then delete whatever of the old set
    # they did not replace. Object stores give no atomic directory rename, but
    # every period is a single object at current sizes, so move-then-prune makes
    # the common case one atomic object replacement: a reader sees the old file
    # or the new one, never a gap. The previous order deleted the prefix first,
    # which left a window where the period did not exist at all. It degrades to
    # the old behaviour only if a period ever holds more than one file, and then
    # only for the files beyond the first.
    stale = {i.path for i in store.ls(base_prefix)}
    store.mkdirs(base_prefix)
    final = []
    for p in written:
        dest = store.join(base_prefix, p.rsplit("/", 1)[-1])
        store.fs.move(p, dest)
        final.append(dest)
    for path in sorted(stale - set(final)):
        # fs.delete_file, not store.delete: these paths came out of store.ls and
        # are already absolute within the filesystem, and store.delete would join
        # them onto the root again and silently delete nothing.
        store.fs.delete_file(path)
    store.delete(staging)
    log.info("compacted period=%s: %d rows into %d files", period, rows, len(final))
    return {"period": period, "rows": rows, "files": len(final)}


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


def build_monthly(store: Store, settings: Settings) -> dict:
    """Rebuild the monthly rollup from every published partition.

    Whole, never incrementally. The rollup is a pure function of the partitions
    and the whole-archive rebuild is seconds, so there is no reconciliation path
    to get wrong and no way for it to drift except the publish window that is
    already not atomic. Do not add an incremental path here without a very good
    reason: a rollup that disagrees with its source is worse than no rollup.

    Sorted (variable_id, month, station_id) on purpose. The queries this serves
    are "one variable across many stations for a span of months"; station-first
    measured 21x worse on exactly that shape, and the per-station queries stay
    cheap regardless because the whole file is tens of MiB.
    """
    con = duckdb_connect(settings, store)
    base = store.uri if store.uri.startswith("s3://") else store.root
    try:
        table = (
            con.execute(
                f"""
                SELECT station_id, variable_id,
                       date_trunc('month', obs_time)::DATE AS month,
                       count(value)::INT AS n,
                       avg(value) AS mean, min(value) AS lo, max(value) AS hi
                FROM read_parquet('{paths.observations_glob(base)}')
                WHERE value IS NOT NULL
                GROUP BY 1, 2, 3
                ORDER BY variable_id, month, station_id
                """
            )
            .arrow()
            .read_all()
            .cast(OBSERVATIONS_MONTHLY)
        )
    finally:
        con.close()

    path = store.join(paths.MONTHLY_FILE)
    store.mkdirs(paths.OBSERVATIONS_MONTHLY)
    with store.fs.open_output_stream(path) as sink:
        pq.write_table(
            table,
            sink,
            compression=settings.compression,
            compression_level=settings.compression_level,
            version="2.6",
            data_page_version="2.0",
            write_statistics=True,
            write_page_index=True,
            row_group_size=settings.row_group_rows,
        )
    log.info("monthly rollup: %d rows", table.num_rows)
    return {"rows": table.num_rows, "path": paths.MONTHLY_FILE}


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
