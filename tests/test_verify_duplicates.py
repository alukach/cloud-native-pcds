"""`pcds verify`'s duplicate-key gate, which has to survive the whole archive.

It used to be one `GROUP BY station_id, variable_id, obs_time` over every
partition. At 979M rows that ran out of memory and the check simply stopped
happening, which is the worst way for a correctness gate to fail: it is quiet,
and it starts exactly when the data gets big enough to need it.

The replacement leans on the sort order the writer already guarantees, so a
duplicate is always adjacent to its twin and a streaming window finds it
without building a hash table.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pcds.cli import duplicate_keys
from pcds.schema import OBSERVATIONS

T0 = dt.datetime(2020, 1, 1)


def write(path, keys):
    """Write rows in the given order, which is the order the check relies on."""
    table = pa.table(
        {
            "station_id": pa.array([s for s, _, _ in keys], pa.int32()),
            "variable_id": pa.array([v for _, v, _ in keys], pa.int16()),
            "obs_time": pa.array([T0 + dt.timedelta(hours=h) for _, _, h in keys],
                                 pa.timestamp("ms")),
            "value": pa.array([1.0] * len(keys), pa.float64()),
        }
    ).cast(OBSERVATIONS)
    pq.write_table(table, path)


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def test_clean_partition_has_no_duplicates(con, tmp_path):
    f = tmp_path / "part-00000.parquet"
    write(f, [(1, 10, 0), (1, 10, 1), (1, 11, 0), (2, 10, 0)])
    assert duplicate_keys(con, str(f)) == 0


def test_a_repeated_key_is_found(con, tmp_path):
    f = tmp_path / "part-00000.parquet"
    write(f, [(1, 10, 0), (1, 10, 0), (1, 10, 1)])
    assert duplicate_keys(con, str(f)) == 1


def test_three_of_a_kind_counts_twice(con, tmp_path):
    # Two repeats of the same key, which is what `GROUP BY ... HAVING count > 1`
    # would have reported as one group. Either number tells you the same thing;
    # this one is the count of offending rows.
    f = tmp_path / "part-00000.parquet"
    write(f, [(1, 10, 0), (1, 10, 0), (1, 10, 0)])
    assert duplicate_keys(con, str(f)) == 2


def test_same_time_on_different_series_is_not_a_duplicate(con, tmp_path):
    # The key is all three columns. Two stations reporting the same hour, and
    # one station reporting two variables in the same hour, are both normal.
    f = tmp_path / "part-00000.parquet"
    write(f, [(1, 10, 0), (1, 11, 0), (2, 10, 0), (2, 11, 0)])
    assert duplicate_keys(con, str(f)) == 0


def test_duplicates_are_found_across_files_of_one_period(con, tmp_path):
    # A period is allowed more than one file, and the writer rolls mid-sort, so
    # a duplicate can land on a file boundary. The glob has to be read as one
    # ordered stream or that pair is invisible.
    write(tmp_path / "part-00000.parquet", [(1, 10, 0), (1, 10, 1)])
    write(tmp_path / "part-00001.parquet", [(1, 10, 1), (1, 10, 2)])
    assert duplicate_keys(con, str(tmp_path / "*.parquet")) == 1
