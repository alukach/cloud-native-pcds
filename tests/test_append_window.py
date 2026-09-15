"""`append` used to hand fetch_chunked `start=None, end=None` for any station
without a watermark. Both of those make fetch_chunked skip chunking and issue a
single unbounded full-station request, which is the one shape CLAUDE.md says
never to send: it can exceed 45s, and the whole response is buffered in memory
before a single delta is written. On a first run that was every polled station.
"""

from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("pyarrow")

from pcds.cli import append_window  # noqa: E402

NOW = dt.datetime(2026, 9, 15, 6, 0)
WINDOW = dt.timedelta(days=30)
ACTIVE_DAYS = 45


def test_both_bounds_are_always_set():
    for watermark in (None, NOW - dt.timedelta(days=1)):
        start, end, inclusive = append_window(watermark, NOW, WINDOW, ACTIVE_DAYS)
        assert start is not None and end is not None, watermark
        assert start < end
        assert inclusive is False


def test_a_watermark_is_re_read_over_the_revision_window():
    watermark = NOW - dt.timedelta(days=1)
    start, end, _ = append_window(watermark, NOW, WINDOW, ACTIVE_DAYS)
    assert start == watermark - WINDOW
    assert end == NOW


def test_a_first_run_is_bounded_not_the_whole_archive():
    start, end, _ = append_window(None, NOW, WINDOW, ACTIVE_DAYS)
    assert start == NOW - dt.timedelta(days=ACTIVE_DAYS) - WINDOW
    assert end == NOW
    # Comfortably inside one backfill chunk, so this stays a single small
    # request rather than a walk over the station's whole span.
    assert (end - start).days == ACTIVE_DAYS + WINDOW.days


def test_the_first_run_span_covers_everything_append_can_owe():
    """A station is only polled if it reported within `active_days`, so a first
    run must reach back at least that far plus the revision window."""
    start, _, _ = append_window(None, NOW, WINDOW, ACTIVE_DAYS)
    oldest_possible_report = NOW - dt.timedelta(days=ACTIVE_DAYS)
    assert start <= oldest_possible_report - WINDOW
