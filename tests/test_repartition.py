"""`scripts/repartition.py` decides nesting from rows, not from period labels.

The label is only an upper bound on what a partition holds. A walk that stops
early, or data dropped after the fact, leaves `period=1872-1999` holding
nothing past 1997; a re-plan then says `1872-1998`, and judging by the label
would refuse a move that is entirely safe. That happened for real, which is
why these exist.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pcds.partitioning import Layout, Period
from pcds.schema import OBSERVATIONS

_spec = importlib.util.spec_from_file_location(
    "repartition", pathlib.Path(__file__).resolve().parents[1] / "scripts" / "repartition.py"
)
repartition = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(repartition)


# The re-plan the user landed on once 1998 was dropped.
LAYOUT = Layout(
    [
        Period("1872-1998", 1872, 1998),
        Period("1999-2002", 1999, 2002),
        Period("2003-2006", 2003, 2006),
    ]
)
WRITTEN = {"1872-1999": ["a.parquet"]}


def test_a_label_that_overruns_its_data_still_nests():
    moves = repartition.plan_moves(LAYOUT, WRITTEN, {"1872-1999": (1872, 1997)})
    assert moves == {"1872-1998": ["1872-1999"]}


def test_data_that_genuinely_straddles_is_refused():
    with pytest.raises(SystemExit, match="1872-2000"):
        repartition.plan_moves(LAYOUT, WRITTEN, {"1872-1999": (1872, 2000)})


def test_no_statistics_falls_back_to_the_label_and_refuses():
    """Without footer stats the label is all there is, and it does straddle."""
    with pytest.raises(SystemExit, match="spans"):
        repartition.plan_moves(LAYOUT, WRITTEN, {"1872-1999": None})


def test_a_partition_already_in_the_right_place_is_not_moved():
    written = {"1872-1998": ["a.parquet"]}
    assert repartition.plan_moves(LAYOUT, written, {"1872-1998": (1872, 1997)}) == {}


def test_data_years_reads_the_real_span_from_footers(tmp_path):
    from pcds.config import Settings
    from pcds.storage import open_store

    store = open_store(Settings(root=str(tmp_path)))
    path = store.join("x.parquet")
    table = pa.table(
        {
            "station_id": pa.array([1, 2], pa.int32()),
            "variable_id": pa.array([1, 1], pa.int16()),
            "obs_time": pa.array(
                [dt.datetime(1889, 3, 1), dt.datetime(1997, 12, 31)], pa.timestamp("s")
            ),
            "value": pa.array([1.0, 2.0], pa.float64()),
        }
    ).cast(OBSERVATIONS)
    with store.fs.open_output_stream(path) as sink:
        pq.write_table(table, sink, write_statistics=True)
    assert repartition.data_years(store, [path]) == (1889, 1997)
