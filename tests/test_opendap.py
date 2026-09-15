"""Parsing and URL construction.

The fixtures are verbatim captures from the live service (EC_raw station
1145M29, Nelson BC) -- the point is to pin the quirks: sequence-name preamble,
space-padded header, arbitrary column order, `None` for missing.
"""

from __future__ import annotations

import datetime as dt
import pathlib

import pytest

from pcds.opendap import (
    agg_url,
    lister_url,
    strip_sequence_header,
    time_constraints,
)

FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures"
DATA_BASE = "https://services.pacificclimate.org/met-data-portal-pcds/api/data"


def test_lister_url_has_rsql_infix():
    url = lister_url(DATA_BASE, "EC_raw", "1145M29")
    assert url.endswith("/lister/raw/EC_raw/1145M29.rsql.csv")


def test_lister_url_encodes_constraints_the_way_pydap_accepts():
    url = lister_url(
        DATA_BASE,
        "EC_raw",
        "1145M29",
        constraints=time_constraints("2026-09-08 00:00:00", "2026-09-09 00:00:00"),
    )
    # Verified against the live service: quotes/spaces/colons encoded, the DAP
    # operators and the clause separator left alone.
    assert "?station_observations.time>%222026-09-08%2000%3A00%3A00%22" in url
    assert "&station_observations.time<%222026-09-09" in url


def test_projection_precedes_selection():
    url = lister_url(
        DATA_BASE, "EC_raw", "1145M29",
        projection=["time", "air_temperature"],
        constraints=['station_observations.air_temperature<0'],
    )
    q = url.split("?", 1)[1]
    assert q.startswith("station_observations.time,station_observations.air_temperature")


def test_inclusive_lower_bound_steps_back_one_second():
    (c,) = time_constraints("2026-09-08 00:00:00", start_inclusive=True)
    assert c == 'station_observations.time>"2026-09-07 23:59:59"'


def test_datetime_and_string_bounds_agree():
    a = time_constraints(dt.datetime(2020, 1, 1))
    b = time_constraints("2020-01-01 00:00:00")
    assert a == b


def test_agg_url_shape():
    url = agg_url(DATA_BASE + "/pcds/agg/", network_name="PC-Aval",
                  from_date="2026-09-01", to_date="2026-09-03")
    assert "download-timeseries=Timeseries" in url
    assert "network-name=PC-Aval" in url
    assert "data-format=csv" in url


def test_strip_sequence_header():
    raw = (FIXTURES / "lister_EC_raw_1145M29.csv").read_bytes()
    parsed = strip_sequence_header(raw)
    assert parsed.columns[0] == "wind_direction"
    assert "time" in parsed.columns
    assert parsed.columns.index("time") == 10  # time is not first or last
    # header is normalized (no padding) and the body is CSV-reader ready
    assert parsed.body.split(b"\n", 1)[0] == b",".join(
        c.encode() for c in parsed.columns
    )


def test_empty_station_payload_is_header_only():
    raw = (FIXTURES / "lister_empty.csv").read_bytes()
    parsed = strip_sequence_header(raw)
    assert parsed.columns == ["time"]
    assert parsed.body.split(b"\n", 1)[1].strip() == b""


def test_rejects_non_lister_payload():
    with pytest.raises(ValueError):
        strip_sequence_header(b"Error {\n code = -1;\n}")
    with pytest.raises(ValueError):
        strip_sequence_header(b"")


def test_melt_drops_nulls_and_maps_variable_ids():
    pytest.importorskip("pyarrow")
    from pcds.opendap import melt, read_wide

    raw = (FIXTURES / "lister_EC_raw_1145M29.csv").read_bytes()
    wide = read_wide(raw)
    # EC_raw ids, from the live variables endpoint.
    var_ids = {
        "air_temperature": 542,
        "wind_gust_speed": 543,
        "air_temperature_yesterday_high": 544,
        "air_temperature_yesterday_low": 545,
        "total_precipitation": 546,
        "wind_direction": 550,
        "tendency_amount": 551,
        "wind_speed": 552,
        "mean_sea_level": 553,
        "dew_point": 554,
        "relative_humidity": 555,
        "snow_amount": 556,
    }
    long, unknown = melt(wide, station_id=1234, variable_ids=var_ids)
    assert unknown == []
    d = long.to_pydict()
    # 5 rows x 12 variable columns, minus every `None`
    assert long.num_rows == 38
    assert set(d["station_id"]) == {1234}
    # the one row with wind_gust_speed present
    gust = [v for i, v in enumerate(d["value"]) if d["variable_id"][i] == 543]
    assert gust == [31.0]
    # wind_direction is null on the 22:00 row and must not appear
    wd_times = [d["obs_time"][i] for i, v in enumerate(d["variable_id"]) if v == 550]
    assert dt.datetime(2026, 9, 8, 22) not in wd_times


def test_unmapped_columns_are_reported_not_silently_dropped():
    pytest.importorskip("pyarrow")
    from pcds.opendap import melt, read_wide

    raw = (FIXTURES / "lister_EC_raw_1145M29.csv").read_bytes()
    long, unknown = melt(read_wide(raw), 1, {"air_temperature": 542})
    assert "wind_direction" in unknown
    assert long.num_rows == 5
