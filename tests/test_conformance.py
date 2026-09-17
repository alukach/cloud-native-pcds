"""The catalog a build actually produces, checked by a real validator.

Two gates live here, and they cover different ground.

`rashid check` is the Portolan gate, but it has a hole this dataset falls
straight into: its data pass iterates a collection's declared assets, and
`observations` deliberately has no `data` asset (DEVIATIONS.md, PORTO-FMT-034),
so nothing in the validator ever opens a partition file. A 224,000-row row group
and a renamed column in one partition both pass `rashid check` silently. So the
second gate is `pcds verify`, which reads the footers directly, and the tests
below plant each violation to prove it still fires.

The catalog is built with *relative* hrefs on purpose. Given an absolute https
base, rashid's data pass tries to fetch the assets over the network, 404s, and
downgrades every byte-level check to an info. A clean run against a published
base URL therefore proves almost nothing; this one reads local bytes.
"""

from __future__ import annotations

import datetime as dt
import random

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from pcds import catalog as manifest  # noqa: E402
from pcds import metadata as md  # noqa: E402
from pcds import paths, portolan, schema  # noqa: E402
from pcds.cli import ROW_GROUP_CAP, _check_partition_files  # noqa: E402
from pcds.config import Settings  # noqa: E402
from pcds.pack import RollingWriter, sort_table  # noqa: E402
from pcds.partitioning import Layout  # noqa: E402
from pcds.storage import open_store  # noqa: E402

STATIONS = 8
HOURS = 300
PERIODS = ("2024", "2025")


def _settings(root) -> Settings:
    # Small enough that a handful of stations still produces several row groups,
    # which is what makes the sort-order and row-group checks meaningful.
    return Settings(root=str(root), row_group_rows=500, target_file_bytes=4 * 1024 * 1024)


def _metadata_tables():
    random.seed(7)
    n = STATIONS
    lons = [-139.0 + random.random() * 25 for _ in range(n)]
    lats = [48.3 + random.random() * 11 for _ in range(n)]
    t0, t1 = dt.datetime(2024, 1, 1), dt.datetime(2025, 6, 1)
    ids = list(range(1, n + 1))

    networks = pa.table(
        {
            "network_id": pa.array([1], pa.int16()),
            "network_name": ["EC_raw"],
            "long_name": ["Environment Canada"],
            "color": ["#1f77b4"],
            "publish": [True],
            "station_count": pa.array([n], pa.int32()),
        },
        schema=schema.NETWORKS,
    )
    variables = pa.table(
        {
            "variable_id": pa.array([1, 2], pa.int16()),
            "network_id": pa.array([1, 1], pa.int16()),
            "name": ["air_temperature", "total_precipitation"],
            "display_name": ["Air Temperature", "Precipitation"],
            "short_name": ["air_temperature", "total_precipitation"],
            "standard_name": ["air_temperature", "precipitation_amount"],
            "cell_method": ["time: point", "time: sum"],
            "unit": ["celsius", "mm"],
            "precision": [0.1, 0.1],
            "tags": [["observation"], ["observation"]],
        },
        schema=schema.VARIABLES,
    )
    common = {
        "station_id": pa.array(ids, pa.int32()),
        "network_name": ["EC_raw"] * n,
        "native_id": [f"S{i}" for i in ids],
        "station_name": [f"Station {i}" for i in ids],
        "lon": lons,
        "lat": lats,
        "elevation": [100.0] * n,
        "province": ["BC"] * n,
        "freq": ["1-hourly"] * n,
        "min_obs_time": pa.array([t0] * n, pa.timestamp("s")),
        "max_obs_time": pa.array([t1] * n, pa.timestamp("s")),
        "variable_ids": pa.array([[1, 2]] * n, pa.list_(pa.int16())),
    }
    stations = pa.table(
        {
            **common,
            "network_id": pa.array([1] * n, pa.int16()),
            "history_count": pa.array([1] * n, pa.int16()),
        },
        schema=schema.STATIONS,
    )
    histories = pa.table(
        {
            **common,
            "history_id": pa.array(ids, pa.int32()),
            "country": ["CA"] * n,
            "tz_offset": ["-08:00"] * n,
            "sdate": pa.array([t0] * n, pa.timestamp("s")),
            "edate": pa.array([t1] * n, pa.timestamp("s")),
        },
        schema=schema.HISTORIES,
    )
    return {
        "networks": networks,
        "variables": variables,
        "stations": stations,
        "histories": histories,
    }


def _observations(year: int):
    rows = [
        (sid, vid, dt.datetime(year, 1, 1) + dt.timedelta(hours=h), 10.0 + h % 17)
        for sid in range(1, STATIONS + 1)
        for vid in (1, 2)
        for h in range(HOURS)
    ]
    return pa.table(
        {
            "station_id": pa.array([r[0] for r in rows], pa.int32()),
            "variable_id": pa.array([r[1] for r in rows], pa.int16()),
            "obs_time": pa.array([r[2] for r in rows], pa.timestamp("s")),
            "value": pa.array([r[3] for r in rows], schema.VALUE_TYPE),
        },
        schema=schema.OBSERVATIONS,
    )


@pytest.fixture
def catalog(tmp_path):
    """A complete, small, structurally real catalog on local disk."""
    settings = _settings(tmp_path)
    store = open_store(settings)

    md.write(store, _metadata_tables(), ["daily", "1-hourly"])

    layout = Layout.yearly(2024, 2025)
    with store.fs.open_output_stream(store.join(paths.LAYOUT_FILE)) as sink:
        sink.write(layout.to_json().encode())

    for period in PERIODS:
        prefix = paths.period_prefix(period)
        store.mkdirs(prefix)
        writer = RollingWriter(store, prefix, settings)
        writer.write(sort_table(_observations(int(period))))
        writer.close()

    manifest.write(store, manifest.build(store))
    portolan.build(store, settings, portolan.CatalogConfig())
    return store


def _partition_file(store, period: str = "2024") -> str:
    files = [i.path for i in store.ls(paths.period_prefix(period)) if i.path.endswith(".parquet")]
    assert files
    return files[0]


# ------------------------------------------------------------------ rashid --


def test_catalog_passes_rashid(catalog):
    """The Portolan gate. Errors here are real conformance failures."""
    runner = pytest.importorskip("rashid.runner")

    report = runner.validate(catalog.root, schema=True, data=True)
    errors = [f for f in report.findings if f.severity is runner.Severity.ERROR]
    assert not errors, "\n".join(f"{f.rule_id} {f.message}" for f in errors)
    assert report.files_checked >= 6  # root + five collections


def test_rashid_gate_is_not_vacuous(catalog):
    """Falsify the green run: a planted defect must actually fail it."""
    runner = pytest.importorskip("rashid.runner")
    import json

    path = catalog.join(paths.STATIONS, paths.COLLECTION_JSON)
    with catalog.fs.open_input_file(path) as f:
        col = json.loads(f.read())
    del col["providers"]
    with catalog.fs.open_output_stream(path) as sink:
        sink.write(json.dumps(col).encode())

    report = runner.validate(catalog.root, schema=True, data=True)
    assert any(f.rule_id == "PTL-PRV-001" for f in report.findings)


# ----------------------------------------------------- the partition gate --


def test_clean_catalog_has_no_partition_problems(catalog):
    assert _check_partition_files(catalog) == []


def test_oversized_row_group_is_caught(catalog):
    """Our own bound on row group size. rashid cannot see this: observations has
    no data asset. Not PORTO-FMT-009, which does not reach a non-spatial table."""
    path = _partition_file(catalog)
    table = pq.read_table(path)
    oversized = pa.concat_tables([table] * (ROW_GROUP_CAP // table.num_rows + 1))
    pq.write_table(oversized, path, row_group_size=len(oversized), compression="zstd")

    problems = _check_partition_files(catalog)
    assert any("row group over" in p for p in problems), problems


def test_schema_drift_between_partitions_is_caught(catalog):
    """PORTO-FMT-021: every partition file shares one schema."""
    path = _partition_file(catalog, "2025")
    table = pq.read_table(path)
    renamed = table.rename_columns(["station_id", "variable_id", "obs_time", "measurement"])
    pq.write_table(renamed, path, compression="zstd")

    problems = _check_partition_files(catalog)
    assert any("PORTO-FMT-021" in p for p in problems), problems


def test_unsorted_partition_is_caught(catalog):
    """Sorting is what makes the row-group statistics selective at all."""
    settings = _settings(catalog.root)
    path = _partition_file(catalog)
    table = pq.read_table(path)
    order = list(range(table.num_rows))
    random.Random(3).shuffle(order)
    pq.write_table(
        table.take(pa.array(order)),
        path,
        row_group_size=settings.row_group_rows,
        compression="zstd",
        write_statistics=True,
    )

    problems = _check_partition_files(catalog)
    assert any("not sorted by station_id" in p for p in problems), problems
