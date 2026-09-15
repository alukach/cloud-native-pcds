"""One unreadable cell used to discard the whole station-window.

`melt` cast every mapped column with `safe=False`, which still raises outright
on an unparseable token, and a null timestamp failed the final cast to
OBSERVATIONS because obs_time is non-nullable. Either one surfaced from
fetch_window as `error="parse: ..."`, so every other variable's good data for
that window was thrown away, on every run, until the station quarantined itself.
"""

from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa  # noqa: E402

from pcds.opendap import TIME_COL, melt  # noqa: E402

VARIABLE_IDS = {"air_temperature": 1, "precipitation": 2}
T0 = dt.datetime(2024, 1, 1)


def _wide(times, temps, precips) -> pa.Table:
    return pa.table({TIME_COL: times, "air_temperature": temps, "precipitation": precips})


def test_clean_input_is_unchanged():
    times = [T0, T0 + dt.timedelta(hours=1)]
    table, unknown = melt(_wide(times, [1.0, 2.0], [0.0, 0.5]), 7, VARIABLE_IDS)
    assert table.num_rows == 4
    assert unknown == []


def test_one_unparseable_value_does_not_discard_the_window():
    times = [T0, T0 + dt.timedelta(hours=1), T0 + dt.timedelta(hours=2)]
    wide = _wide(times, ["1.0", "BAD", "3.0"], ["0.0", "0.5", "1.5"])

    table, _ = melt(wide, 7, VARIABLE_IDS)

    # The bad temperature is gone; its two good neighbours and the whole of the
    # precipitation column survive.
    temps = table.filter(pa.compute.equal(table.column("variable_id"), 1))
    assert temps.column("value").to_pylist() == [1.0, 3.0]
    precip = table.filter(pa.compute.equal(table.column("variable_id"), 2))
    assert precip.column("value").to_pylist() == [0.0, 0.5, 1.5]


def test_a_null_timestamp_drops_only_its_own_row():
    times = [T0, None, T0 + dt.timedelta(hours=2)]
    wide = _wide(times, [1.0, 2.0, 3.0], [0.0, 0.5, 1.5])

    table, _ = melt(wide, 7, VARIABLE_IDS)

    assert table.num_rows == 4
    assert None not in table.column("obs_time").to_pylist()
    temps = table.filter(pa.compute.equal(table.column("variable_id"), 1))
    assert temps.column("value").to_pylist() == [1.0, 3.0]


def test_an_unparseable_timestamp_drops_only_its_own_row():
    wide = _wide(["2024-01-01 00:00:00", "not a time", "2024-01-01 02:00:00"],
                 [1.0, 2.0, 3.0], [0.0, 0.5, 1.5])

    table, _ = melt(wide, 7, VARIABLE_IDS)

    assert table.num_rows == 4
    assert table.column("obs_time").to_pylist().count(T0) == 2


def test_an_entirely_unreadable_column_does_not_take_the_others_with_it():
    times = [T0, T0 + dt.timedelta(hours=1)]
    wide = _wide(times, ["x", "y"], [0.0, 0.5])

    table, _ = melt(wide, 7, VARIABLE_IDS)

    assert set(table.column("variable_id").to_pylist()) == {2}
    assert table.column("value").to_pylist() == [0.0, 0.5]


def test_the_result_still_satisfies_the_published_schema():
    from pcds.schema import OBSERVATIONS

    wide = _wide([T0, None], ["BAD", "2.0"], [0.0, 0.5])
    table, _ = melt(wide, 7, VARIABLE_IDS)
    assert table.schema.equals(OBSERVATIONS)
