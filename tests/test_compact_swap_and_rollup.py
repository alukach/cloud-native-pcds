"""Compaction's swap order, and the monthly rollup it derives.

The swap is the interesting one. Compaction used to delete the base prefix and
then move the new files in, which left a window where the period did not exist
at all -- a visibility gap for readers and, on a crash, a durability gap. It now
moves first and prunes the leftovers, so the common case (a period that is one
object) is a single atomic replacement. The test that matters is the one that
proves stragglers from a *wider* previous write still get cleaned up, because
that is what move-then-prune can get wrong and delete-then-move could not.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq

from pcds import paths
from pcds.compact import build_monthly, compact_period
from pcds.config import Settings
from pcds.schema import DELTA, OBSERVATIONS
from pcds.storage import open_store

KIB = 1024
PERIOD = f"{paths.OBSERVATIONS}/period=2024"


def _settings(root) -> Settings:
    return Settings(
        root=str(root),
        row_group_rows=500,
        target_file_bytes=400 * KIB,
        min_file_bytes=1,
        max_file_bytes=4096 * KIB,
    )


def _obs(station, day, value, n=4):
    base = dt.datetime(2024, 1, day)
    return pa.table(
        {
            "station_id": pa.array([station] * n, pa.int32()),
            "variable_id": pa.array([1] * n, pa.int16()),
            "obs_time": pa.array(
                [base + dt.timedelta(hours=h) for h in range(n)], pa.timestamp("s")
            ),
            "value": pa.array([float(value + h) for h in range(n)], pa.float64()),
        },
        schema=OBSERVATIONS,
    )


def _write_base(store, table, names):
    """Split `table` across several part files, as a wider earlier run would."""
    store.mkdirs(PERIOD)
    per = table.num_rows // len(names)
    for i, name in enumerate(names):
        last = i == len(names) - 1
        chunk = table.slice(i * per, table.num_rows - i * per if last else per)
        with store.fs.open_output_stream(store.join(PERIOD, name)) as sink:
            pq.write_table(chunk, sink, compression="zstd")


def _stage_delta(store, table):
    store.mkdirs(paths.DELTA)
    ingested = pa.array([dt.datetime(2024, 6, 1)] * table.num_rows, pa.timestamp("s"))
    delta = pa.Table.from_arrays([*table.columns, ingested], schema=DELTA)
    with store.fs.open_output_stream(store.join(paths.DELTA, "d-00000.parquet")) as sink:
        pq.write_table(delta, sink, compression="zstd")


def _names(store, prefix):
    return sorted(p.path.rsplit("/", 1)[-1] for p in store.ls(prefix))


def test_swap_prunes_stragglers_from_a_wider_previous_write(tmp_path):
    """Move-then-prune must still delete old parts the new write did not replace.

    Three files in, one file out: parts 1 and 2 have no counterpart in the new
    set, so nothing overwrites them and only the prune can remove them. If it
    does not, the partition silently keeps stale duplicate rows.
    """
    settings = _settings(tmp_path)
    store = open_store(settings)
    base = pa.concat_tables([_obs(s, 1, s * 10) for s in (1, 2, 3)])
    _write_base(store, base, [f"part-{i:05d}.parquet" for i in range(3)])
    _stage_delta(store, _obs(4, 2, 99))

    compact_period(store, settings, "2024", (2024, 2024))

    assert _names(store, PERIOD) == ["part-00000.parquet"]
    out = pq.read_table(store.join(PERIOD, "part-00000.parquet"))
    assert out.num_rows == base.num_rows + 4
    assert set(out.column("station_id").to_pylist()) == {1, 2, 3, 4}


def test_partition_is_never_absent_during_a_swap(tmp_path):
    """The point of the reorder: at no moment is the period prefix empty.

    Checked from inside the move, which is the only place the old order could be
    observed to have deleted everything first.
    """
    settings = _settings(tmp_path)
    store = open_store(settings)
    _write_base(store, _obs(1, 1, 10), ["part-00000.parquet"])
    _stage_delta(store, _obs(2, 2, 20))

    seen = []
    real_fs = store.fs

    class WatchedFS:
        """pyarrow filesystem attributes are read-only, so wrap rather than patch."""

        def __getattr__(self, name):
            return getattr(real_fs, name)

        def move(self, src, dest):
            seen.append(len(store.ls(PERIOD)))
            return real_fs.move(src, dest)

    store.fs = WatchedFS()
    try:
        compact_period(store, settings, "2024", (2024, 2024))
    finally:
        store.fs = real_fs

    assert seen and all(n >= 1 for n in seen), seen


def test_monthly_rollup_matches_the_raw_table(tmp_path):
    """The rollup is only worth having if it is exact. `n` is count(value)."""
    settings = _settings(tmp_path)
    store = open_store(settings)
    table = pa.concat_tables([_obs(s, d, s * 10) for s in (1, 2) for d in (1, 2)])
    _write_base(store, table, ["part-00000.parquet"])

    build_monthly(store, settings)

    roll = pq.read_table(store.join(paths.MONTHLY_FILE))
    assert roll.num_rows == 2  # two stations, one variable, one month
    assert sum(roll.column("n").to_pylist()) == table.num_rows
    means = dict(
        zip(
            roll.column("station_id").to_pylist(),
            roll.column("mean").to_pylist(),
            strict=True,
        )
    )
    raw = table.to_pydict()
    for station in (1, 2):
        vals = [v for s, v in zip(raw["station_id"], raw["value"], strict=True) if s == station]
        assert means[station] == sum(vals) / len(vals)


def test_monthly_rollup_is_sorted_variable_first(tmp_path):
    """Sort order is load-bearing: station-first measured 21x worse on the
    many-stations-one-variable query the rollup exists to serve."""
    settings = _settings(tmp_path)
    store = open_store(settings)
    rows = []
    for station in (5, 1, 3):
        for var in (2, 1):
            t = _obs(station, 1, 10)
            rows.append(t.set_column(1, "variable_id", pa.array([var] * t.num_rows, pa.int16())))
    _write_base(store, pa.concat_tables(rows), ["part-00000.parquet"])

    build_monthly(store, settings)

    roll = pq.read_table(store.join(paths.MONTHLY_FILE))
    keys = list(
        zip(
            roll.column("variable_id").to_pylist(),
            roll.column("station_id").to_pylist(),
            strict=True,
        )
    )
    assert keys == sorted(keys), keys
