from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("pyarrow")

from pcds.state import Watermarks  # noqa: E402

NOW = dt.datetime(2026, 9, 11, 14, 20)


def test_watermark_only_advances():
    w = Watermarks({})
    w.record_success(1, "EC_raw", "1145M29", max_obs_time=dt.datetime(2026, 9, 10),
                     rows=5, now=NOW)
    w.record_success(1, "EC_raw", "1145M29", max_obs_time=dt.datetime(2026, 9, 1),
                     rows=2, now=NOW)
    assert w.watermark(1) == dt.datetime(2026, 9, 10)
    assert w.get(1)["rows_total"] == 7


def test_empty_result_does_not_move_the_watermark():
    w = Watermarks({})
    w.record_success(1, "EC_raw", "x", max_obs_time=dt.datetime(2026, 9, 10), rows=5, now=NOW)
    w.record_success(1, "EC_raw", "x", max_obs_time=None, rows=0, now=NOW)
    assert w.watermark(1) == dt.datetime(2026, 9, 10)


def test_failures_accumulate_then_quarantine_then_reset():
    w = Watermarks({})
    for _ in range(10):
        w.record_failure(7, "MoTIe", "42", error="HTTP 500", now=NOW)
    assert w.quarantined(7)
    w.record_success(7, "MoTIe", "42", max_obs_time=NOW, rows=1, now=NOW)
    assert not w.quarantined(7)
    assert w.get(7)["last_error"] is None


def test_unknown_station_has_no_watermark():
    assert Watermarks({}).watermark(999) is None
