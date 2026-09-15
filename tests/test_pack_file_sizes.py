"""Every file compaction emits has to land in [min_file_bytes, max_file_bytes].

The floor is not free: a roll happens the moment a file passes the target, so
the leftover at the end is whatever it happens to be, and before `_merge_tail`
a period of target + a bit produced one good file and one stub. The stub is the
whole point of these tests -- the sizes here are scaled down so a few MiB of
rows crosses several thresholds, but the arithmetic is the same at 256 MiB.
"""

from __future__ import annotations

import datetime as dt
import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pcds.config import Settings
from pcds.pack import RollingWriter, sort_table
from pcds.schema import OBSERVATIONS
from pcds.storage import open_store

KIB = 1024


def _settings(root, **kw) -> Settings:
    base = dict(
        root=str(root),
        row_group_rows=2_000,
        target_file_bytes=400 * KIB,
        min_file_bytes=200 * KIB,
        max_file_bytes=4096 * KIB,
    )
    base.update(kw)
    return Settings(**base)


EPOCH = dt.datetime(1970, 1, 1)


def _rows(n: int, seed: int = 0) -> pa.Table:
    """Random-ish values so zstd cannot crush this to nothing; sizes matter."""
    rng = random.Random(seed)
    return sort_table(
        pa.table(
            {
                "station_id": pa.array([rng.randrange(400) for _ in range(n)], pa.int32()),
                "variable_id": pa.array([rng.randrange(90) for _ in range(n)], pa.int16()),
                "obs_time": pa.array(
                    [EPOCH + dt.timedelta(seconds=rng.randrange(10**9)) for _ in range(n)],
                    pa.timestamp("s"),
                ),
                "value": pa.array([rng.gauss(0, 50) for _ in range(n)], pa.float64()),
            }
        ).cast(OBSERVATIONS)
    )


def _write(tmp_path, n_rows, **kw):
    settings = _settings(tmp_path, **kw)
    store = open_store(settings)
    store.mkdirs("out")
    writer = RollingWriter(store, "out", settings)
    writer.write(_rows(n_rows))
    paths = writer.close()
    sizes = [store.fs.get_file_info(p).size for p in paths]
    return settings, store, paths, sizes


@pytest.mark.parametrize("n_rows", [20_000, 60_000, 130_000, 400_000])
def test_every_emitted_file_is_within_the_size_band(tmp_path, n_rows):
    settings, _, paths, sizes = _write(tmp_path, n_rows)
    assert paths, "something must be written"
    total = sum(sizes)
    if total < settings.min_file_bytes:
        # Less than one floor exists in the first place. A writer cannot invent
        # bytes; the layout is what has to merge more years into the period.
        assert len(paths) == 1
        return
    assert all(s >= settings.min_file_bytes for s in sizes), sizes
    assert all(s <= settings.max_file_bytes for s in sizes), sizes


def test_a_sub_floor_tail_is_merged_not_left_behind(tmp_path):
    """Sized to land just past one target, which is what strands a stub."""
    settings, _, paths, sizes = _write(tmp_path, 70_000)
    assert sum(sizes) > settings.target_file_bytes, "must be enough to roll"
    assert min(sizes) >= settings.min_file_bytes, f"stub survived: {sizes}"


def test_merging_the_tail_keeps_every_row_and_the_sort_order(tmp_path):
    settings, store, paths, _ = _write(tmp_path, 70_000)
    tables = [pq.read_table(store.fs.open_input_file(p)) for p in paths]
    combined = pa.concat_tables(tables)
    assert combined.num_rows == 70_000
    keys = combined.select(["station_id", "variable_id", "obs_time"])
    assert keys.equals(keys.sort_by([(c, "ascending") for c in keys.column_names]))


def test_settings_that_could_breach_the_ceiling_are_refused(tmp_path):
    settings = _settings(tmp_path, target_file_bytes=4000 * KIB, max_file_bytes=4096 * KIB)
    store = open_store(settings)
    # 4000 + 200 KiB of worst case is over the 4096 KiB ceiling.
    with pytest.raises(ValueError, match="ceiling"):
        RollingWriter(store, "out", settings)
