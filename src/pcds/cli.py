"""Command line interface."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
import uuid

import typer

from . import paths
from .config import SETTINGS, Settings
from .partitioning import Layout, Period, plan_periods

app = typer.Typer(add_completion=False, help="Build cloud-optimized Parquet from PCIC's PCDS.")
log = logging.getLogger("pcds")


def _setup(verbose: bool = True) -> None:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
        stream=sys.stderr,
    )


def _store(root: str | None):
    from .storage import open_store

    return open_store(SETTINGS, root)


def _layout(store) -> Layout:
    path = store.join(paths.LAYOUT_FILE)
    if store.exists(paths.LAYOUT_FILE):
        with store.fs.open_input_file(path) as f:
            return Layout.from_json(f.read())
    # No plan yet: one partition per year is a safe default; `pcds layout` will
    # merge the sparse ones once there is data to measure.
    return Layout.yearly(1870, dt.date.today().year + 1)


# ---------------------------------------------------------------- metadata --


@app.command()
def metadata(root: str = typer.Option(None, help="Dataset root (overrides PCDS_ROOT)")):
    """Fetch the station catalog and write metadata/*.parquet."""
    _setup()
    from . import metadata as md

    store = _store(root)
    raw = md.fetch_all(SETTINGS)
    tables = md.build_tables(raw)
    md.write(store, tables, raw["frequencies"])
    for name, table in tables.items():
        log.info("metadata/%s.parquet: %d rows", name, table.num_rows)


# -------------------------------------------------------------------- plan --


@app.command()
def plan(
    root: str = typer.Option(None),
    freshness_hours: int = typer.Option(24, help="How stale may published data be?"),
    active_days: int = typer.Option(30, help="A history is 'active' if it reported this recently"),
):
    """Measure arrival rate and byte cost; recommend append/compaction cadence."""
    _setup(verbose=False)
    from . import catalog, metadata as md, plan as planner

    store = _store(root)
    histories = md.load(store, "histories").to_pylist()
    rate = planner.arrival_rate(
        histories, dt.datetime.now(dt.UTC).replace(tzinfo=None), active_days
    )

    bytes_per_row = planner.DEFAULT_BYTES_PER_ROW
    source = "default estimate (no data written yet)"
    if store.exists(paths.MANIFEST_FILE):
        summary = catalog.summarize(catalog.load(store))
        if summary.get("bytes_per_row"):
            bytes_per_row = summary["bytes_per_row"]
            source = f"measured over {summary['files']} files / {summary['rows']:,} rows"

    cad = planner.cadence(
        rate.obs_per_day,
        bytes_per_row,
        min_file_bytes=SETTINGS.min_file_bytes,
        target_file_bytes=SETTINGS.target_file_bytes,
        freshness_hours=freshness_hours,
    )
    print(planner.report(rate, cad, target_file_bytes=SETTINGS.target_file_bytes))
    print(f"\n  bytes/row source: {source}")


# ------------------------------------------------------------------ layout --


@app.command()
def layout(
    root: str = typer.Option(None),
    start_year: int = typer.Option(1870),
    end_year: int = typer.Option(None, help="Defaults to next calendar year"),
):
    """(Re)plan the period partitioning so every partition clears the size floor."""
    _setup()
    from . import catalog, metadata as md, plan as planner

    store = _store(root)
    end_year = end_year or dt.date.today().year + 1
    bytes_per_row = planner.DEFAULT_BYTES_PER_ROW
    year_rows: dict[int, int] = {}

    if store.exists(paths.MANIFEST_FILE):
        # Measured: rows actually written per year.
        from .storage import duckdb_connect

        con = duckdb_connect(SETTINGS, store)
        base = store.uri if store.uri.startswith("s3://") else store.root
        rows = con.execute(
            f"SELECT EXTRACT(year FROM obs_time)::INT AS y, count(*) "
            f"FROM read_parquet('{paths.observations_glob(base)}') GROUP BY 1"
        ).fetchall()
        year_rows = {int(y): int(c) for y, c in rows}
        summary = catalog.summarize(catalog.load(store))
        bytes_per_row = summary.get("bytes_per_row") or bytes_per_row
        con.close()
    else:
        # Estimated from metadata: each history contributes its frequency x
        # variable count to every year it spans.
        for h in md.load(store, "histories").to_pylist():
            lo, hi = h.get("min_obs_time"), h.get("max_obs_time")
            if not lo or not hi:
                continue
            nvars = len(h.get("variable_ids") or [])
            per_day = planner.OBS_PER_DAY.get(h.get("freq"), planner.OBS_PER_DAY[None]) * nvars
            for y in range(max(lo.year, start_year), min(hi.year, end_year) + 1):
                lo_y = max(lo, dt.datetime(y, 1, 1))
                hi_y = min(hi, dt.datetime(y + 1, 1, 1))
                days = max((hi_y - lo_y).days, 0)
                year_rows[y] = year_rows.get(y, 0) + int(per_day * days)

    year_bytes = {y: int(n * bytes_per_row) for y, n in year_rows.items()}
    periods = plan_periods(
        year_bytes, min_file_bytes=SETTINGS.min_file_bytes
    ) or [Period(str(y), y, y) for y in range(start_year, end_year + 1)]
    lay = Layout(periods)
    with store.fs.open_output_stream(store.join(paths.LAYOUT_FILE)) as sink:
        sink.write(lay.to_json().encode())
    log.info("wrote layout.json: %d periods (%s ... %s)", len(periods),
             periods[0].period, periods[-1].period)
    for p in periods:
        est = sum(year_bytes.get(y, 0) for y in range(p.start_year, p.end_year + 1))
        print(f"  period={p.period:<12} est {est / (1024 * 1024):8.1f} MiB")


# ---------------------------------------------------------------- backfill --


@app.command()
def backfill(
    root: str = typer.Option(None),
    start_year: int = typer.Option(..., help="Inclusive"),
    end_year: int = typer.Option(..., help="Inclusive"),
    networks: str = typer.Option("", help="Comma-separated network names; empty = all"),
    shard: str = typer.Option("0/1", help="i/N -- process stations where station_id %% N == i"),
    limit: int = typer.Option(0, help="Stop after N stations (smoke tests)"),
    chunk_years: int = typer.Option(None),
):
    """Walk a closed year range and write period partitions directly."""
    _setup()
    import pyarrow.compute as pc

    from . import metadata as md
    from .ingest import Target, fetch_many
    from .pack import RollingWriter, sort_table
    from .state import Watermarks

    store = _store(root)
    lay = _layout(store)
    variables = md.variable_lookup(md.load(store, "variables"))
    stations = md.load(store, "stations").to_pylist()

    i, n = (int(x) for x in shard.split("/"))
    wanted = {s.strip() for s in networks.split(",") if s.strip()}
    lo = dt.datetime(start_year, 1, 1)
    hi = dt.datetime(end_year + 1, 1, 1)

    targets = [
        Target(s["station_id"], s["network_id"], s["network_name"], s["native_id"])
        for s in stations
        if (not wanted or s["network_name"] in wanted)
        and s["station_id"] % n == i
        and s["max_obs_time"] is not None
        and s["max_obs_time"] >= lo
        and s["min_obs_time"] is not None
        and s["min_obs_time"] < hi
    ]
    targets.sort(key=lambda t: t.station_id)
    if limit:
        targets = targets[:limit]
    log.info("backfill %d-%d: %d stations (shard %s)", start_year, end_year, len(targets), shard)

    settings = Settings(**{**SETTINGS.__dict__, "backfill_chunk_years":
                           chunk_years or SETTINGS.backfill_chunk_years})
    marks = Watermarks.load(store)
    writers: dict[str, RollingWriter] = {}
    shard_tag = f"s{i:03d}"
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None, microsecond=0)
    total_rows = 0
    failures = 0

    # Results arrive out of order from the pool; buffer by station so each
    # partition file stays sorted by station_id.
    pending: dict[int, object] = {}
    order = [t.station_id for t in targets]
    cursor = 0

    def flush(station_id: int, table) -> None:
        nonlocal total_rows
        if table.num_rows == 0:
            return
        years = pc.year(table.column("obs_time"))
        for period in sorted({lay.period_for(y) for y in set(years.to_pylist())}):
            p = next((x for x in lay.periods if x.period == period), None)
            y0, y1 = (p.start_year, p.end_year) if p else (int(period[:4]), int(period[-4:]))
            mask = pc.and_(pc.greater_equal(years, y0), pc.less_equal(years, y1))
            part = sort_table(table.filter(mask))
            if part.num_rows == 0:
                continue
            w = writers.get(period)
            if w is None:
                prefix = paths.period_prefix(period)
                store.mkdirs(prefix)
                w = writers[period] = RollingWriter(store, prefix, settings, name=shard_tag)
            w.write(part)
            total_rows += part.num_rows

    for res in fetch_many(
        settings, targets, variables, lambda t: (lo, hi, True)
    ):
        pending[res.target.station_id] = res
        while cursor < len(order) and order[cursor] in pending:
            r = pending.pop(order[cursor])
            cursor += 1
            if r.error:
                failures += 1
                log.warning("station %s/%s: %s", r.target.network_name, r.target.native_id, r.error)
                marks.record_failure(
                    r.target.station_id, r.target.network_name, r.target.native_id,
                    error=r.error, now=now,
                )
            if r.unknown_columns:
                log.warning("station %s: unmapped columns %s -- refresh metadata",
                            r.target.native_id, r.unknown_columns)
            flush(r.target.station_id, r.table)
            if r.table.num_rows and not r.error:
                marks.record_success(
                    r.target.station_id, r.target.network_name, r.target.native_id,
                    max_obs_time=pc.max(r.table.column("obs_time")).as_py(),
                    rows=r.table.num_rows, now=now,
                )
    for r in pending.values():  # anything left (shouldn't happen, but be safe)
        flush(r.target.station_id, r.table)

    written = [p for w in writers.values() for p in w.close()]
    marks.save(store)
    log.info("wrote %d rows across %d files; %d station failures", total_rows, len(written), failures)
    if failures:
        raise typer.Exit(code=1 if failures > len(targets) // 10 else 0)


# ------------------------------------------------------------------ append --


@app.command()
def append(
    root: str = typer.Option(None),
    active_days: int = typer.Option(45, help="Only poll stations that reported this recently"),
    revision_window_days: int = typer.Option(None),
    limit: int = typer.Option(0),
    run_id: str = typer.Option(None),
):
    """Incremental pull from each station's watermark, staged as delta files."""
    _setup()
    import pyarrow as pa
    import pyarrow.compute as pc

    from . import metadata as md
    from .ingest import Target, fetch_many
    from .pack import write_delta
    from .state import Watermarks

    store = _store(root)
    variables = md.variable_lookup(md.load(store, "variables"))
    stations = md.load(store, "stations").to_pylist()
    marks = Watermarks.load(store)
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None, microsecond=0)
    window = dt.timedelta(days=revision_window_days or SETTINGS.revision_window_days)

    targets = []
    for s in stations:
        last = s["max_obs_time"]
        if last is None or (now - last).days > active_days:
            continue
        if marks.quarantined(s["station_id"]):
            continue
        targets.append(
            Target(s["station_id"], s["network_id"], s["network_name"], s["native_id"])
        )
    targets.sort(key=lambda t: t.station_id)
    if limit:
        targets = targets[:limit]
    log.info("append: polling %d stations", len(targets))

    def win(t: Target):
        wm = marks.watermark(t.station_id)
        # Re-read the trailing revision window: PCDS is preliminary data and
        # late/corrected observations are normal. The compactor dedupes.
        start = (wm - window) if wm else None
        return (start, None, False)

    run_id = run_id or now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    tables = []
    rows = failures = 0
    for res in fetch_many(SETTINGS, targets, variables, win):
        if res.error:
            failures += 1
            log.warning("station %s/%s: %s", res.target.network_name,
                        res.target.native_id, res.error)
            marks.record_failure(res.target.station_id, res.target.network_name,
                                 res.target.native_id, error=res.error, now=now)
            continue
        if res.table.num_rows == 0:
            marks.record_success(res.target.station_id, res.target.network_name,
                                 res.target.native_id, max_obs_time=None, rows=0, now=now)
            continue
        tables.append(res.table)
        rows += res.table.num_rows
        marks.record_success(
            res.target.station_id, res.target.network_name, res.target.native_id,
            max_obs_time=pc.max(res.table.column("obs_time")).as_py(),
            rows=res.table.num_rows, now=now,
        )

    path = None
    if tables:
        path = write_delta(store, SETTINGS, pa.concat_tables(tables), run_id, "all")
    marks.save(store)
    log.info("append: %d rows staged -> %s (%d failures)", rows, path, failures)
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
            fh.write(f"rows={rows}\nfailures={failures}\nrun_id={run_id}\n")


# ----------------------------------------------------------------- compact --


@app.command()
def compact(
    root: str = typer.Option(None),
    period: str = typer.Option("", help="Comma-separated periods; empty = every period with deltas"),
    clear: bool = typer.Option(True, help="Delete folded delta files afterwards"),
):
    """Fold staged deltas into period partitions; dedupe, re-sort, re-pack."""
    _setup()
    from .compact import clear_deltas, compact_all

    store = _store(root)
    lay = _layout(store)
    periods = [p.strip() for p in period.split(",") if p.strip()] or None
    # Only the auto path derives its periods *from* the deltas, so only it can
    # prove it folded all of them. Clearing after an explicitly scoped run would
    # delete rows for every other period unread, and the watermarks have already
    # advanced past them, so `append` will not fetch them again.
    targeted = periods is not None

    if periods is None:
        # Only touch partitions that actually have new data.
        from .storage import duckdb_connect

        if store.ls(paths.DELTA, recursive=True):
            con = duckdb_connect(SETTINGS, store)
            base = store.uri if store.uri.startswith("s3://") else store.root
            years = [
                int(r[0])
                for r in con.execute(
                    f"SELECT DISTINCT EXTRACT(year FROM obs_time)::INT "
                    f"FROM read_parquet('{paths.delta_glob(base)}')"
                ).fetchall()
            ]
            con.close()
            periods = sorted({lay.period_for(y) for y in years})
        else:
            periods = []
    if not periods:
        log.info("nothing to compact")
        return
    stats = compact_all(store, SETTINGS, lay, periods)
    for s in stats:
        log.info("%s", s)
    if clear and targeted:
        log.warning(
            "keeping staged deltas: --period %s compacted only some of them, and "
            "clearing would drop rows for the rest. Re-run without --period to fold "
            "and clear everything.",
            period,
        )
    elif clear:
        removed = clear_deltas(store)
        log.info("removed %d delta files", removed)


# ----------------------------------------------------------------- catalog --


@app.command("catalog")
def catalog_cmd(root: str = typer.Option(None)):
    """Rebuild catalog/files.parquet and catalog/summary.json from Parquet footers."""
    _setup()
    from . import catalog as cat

    store = _store(root)
    table = cat.build(store)
    summary = cat.write(store, table)
    print(json.dumps(summary, indent=2, default=str))


# ---------------------------------------------------------------- portolan --


@app.command()
def portolan(
    root: str = typer.Option(None),
    base_url: str = typer.Option(None, help="https base the catalog is served from"),
    s3_uri: str = typer.Option(None, help="s3:// equivalent, used for the partition glob"),
    host_name: str = typer.Option(None, help="Organization hosting this copy"),
    host_url: str = typer.Option(None),
    host_email: str = typer.Option(None),
):
    """Write the Portolan STAC layer: catalog.json, collections, README/AGENTS, DEVIATIONS.

    Deliberately non-conformant on two requirements; see DEVIATIONS.md in the
    output for which and why. Run `rashid` against the result to see what a
    validator makes of it.
    """
    _setup()
    from .portolan import CatalogConfig, Publisher, build

    store = _store(root)
    cfg = CatalogConfig(
        base_url=base_url if base_url is not None else SETTINGS.base_url,
        s3_uri=s3_uri if s3_uri is not None else SETTINGS.public_s3_uri,
        publisher=Publisher(
            name=host_name or SETTINGS.host_name,
            url=host_url if host_url is not None else SETTINGS.host_url,
            email=host_email if host_email is not None else SETTINGS.host_email,
        ),
    )
    if not cfg.base_url:
        log.warning(
            "no --base-url: asset hrefs will be relative, and the root catalog will "
            "carry no absolute self link (Portolan SHOULD)"
        )
    if not cfg.publisher.url and not cfg.publisher.email:
        raise typer.BadParameter(
            "the host provider MUST be contactable: pass --host-url or --host-email"
        )
    if cfg.base_url and not cfg.s3_uri and store.exists(paths.OBSERVATIONS):
        # partition:glob would come out as https://..., which no reader can expand:
        # globbing needs a LIST and plain https has none. PORTO-FMT-020 exempts the
        # glob from the https-only rule precisely so it can be s3://.
        raise typer.BadParameter(
            "--base-url without --s3-uri would publish an https partition:glob, which "
            "cannot be expanded (glob expansion needs a bucket listing). Pass --s3-uri, "
            "or drop --base-url for a relative local build."
        )
    report = build(store, SETTINGS, cfg)
    print(json.dumps(report, indent=2))


# ------------------------------------------------------------------ verify --

# PORTO-FMT-009. Portolan writes it for GeoParquet; the observation files honour
# it anyway, so it is the number to assert against.
ROW_GROUP_CAP = 150_000


def _check_partition_files(store) -> list[str]:
    """Footer-only invariants over every observation partition file.

    Reads metadata, never row data: a few KiB per file regardless of its size.
    """
    import pyarrow.parquet as pq

    problems: list[str] = []
    schemas: dict[tuple, list[str]] = {}
    oversized: list[str] = []
    unsorted: list[str] = []

    for info in store.ls(paths.OBSERVATIONS, recursive=True):
        if not info.path.endswith(".parquet"):
            continue
        rel = info.path[len(store.root) :].lstrip("/")
        with store.fs.open_input_file(info.path) as f:
            parquet = pq.ParquetFile(f)
            footer = parquet.metadata
            arrow_schema = parquet.schema_arrow
        schemas.setdefault(
            (tuple(arrow_schema.names), tuple(str(t) for t in arrow_schema.types)), []
        ).append(rel)

        groups = [footer.row_group(i) for i in range(footer.num_row_groups)]
        if any(g.num_rows > ROW_GROUP_CAP for g in groups):
            oversized.append(f"{rel} ({max(g.num_rows for g in groups):,} rows)")

        # Files are sorted by (station_id, ...), so a row group's station_id range
        # must start at or after the previous one's end. Cheaper than reading rows
        # and it catches an unsorted write, which is what makes the stats useless.
        if "station_id" not in footer.schema.names:
            continue  # the schema check above already reports this file
        col = list(footer.schema.names).index("station_id")
        prev_max = None
        for g in groups:
            st = g.column(col).statistics
            if st is None or not st.has_min_max:
                continue
            if prev_max is not None and st.min < prev_max:
                unsorted.append(rel)
                break
            prev_max = st.max

    if oversized:
        problems.append(
            f"{len(oversized)} file(s) with a row group over {ROW_GROUP_CAP:,} rows "
            f"(PORTO-FMT-009): {', '.join(oversized[:3])}"
        )
    if len(schemas) > 1:
        shapes = "; ".join(f"{files[0]} -> {list(cols)}" for (cols, _), files in schemas.items())
        problems.append(f"partition files do not share one schema (PORTO-FMT-021): {shapes}")
    if unsorted:
        problems.append(
            f"{len(unsorted)} file(s) not sorted by station_id: {', '.join(unsorted[:3])}"
        )
    return problems


@app.command()
def verify(root: str = typer.Option(None), sample: int = typer.Option(5)):
    """Sanity checks: row groups, partition schema, sort order, duplicate keys, file sizes.

    The first three are here because rashid cannot reach them. Its data pass
    iterates a collection's declared assets, and `observations` deliberately has
    no `data` asset (see DEVIATIONS.md, PORTO-FMT-034), so nothing in the
    validator ever opens a partition file. Planting a 224,000-row row group and a
    renamed column in one partition both pass `rashid check` silently. Until
    rashid#130 closes, this command is the only gate on those MUSTs.
    """
    _setup(verbose=False)
    from . import catalog as cat
    from .storage import duckdb_connect

    store = _store(root)
    table = cat.load(store)
    summary = cat.summarize(table)
    problems: list[str] = []

    small = [
        (r["path"], r["file_bytes"])
        for r in table.to_pylist()
        if r["file_bytes"] < SETTINGS.min_file_bytes // 4
    ]
    if len(small) > 1:
        problems.append(f"{len(small)} files under {SETTINGS.min_file_bytes // 4 // 1024**2} MiB "
                        f"(run `pcds compact`)")

    problems += _check_partition_files(store)

    con = duckdb_connect(SETTINGS, store)
    base = store.uri if store.uri.startswith("s3://") else store.root
    glob = paths.observations_glob(base)
    dupes = con.execute(
        f"SELECT count(*) FROM (SELECT station_id, variable_id, obs_time FROM "
        f"read_parquet('{glob}') GROUP BY 1,2,3 HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        problems.append(f"{dupes:,} duplicate (station, variable, time) keys")
    con.close()

    print(json.dumps(summary, indent=2, default=str))
    if problems:
        print("\nproblems:")
        for p in problems:
            print(f"  - {p}")
        raise typer.Exit(code=1)
    print("\nok")


if __name__ == "__main__":
    app()
