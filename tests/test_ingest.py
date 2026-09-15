from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("pyarrow")

from pcds.ingest import RateLimiter, chunk_ranges, clip_window  # noqa: E402


def test_chunks_align_to_calendar_boundaries():
    got = list(chunk_ranges(dt.datetime(2018, 6, 1), dt.datetime(2026, 9, 11), 5))
    assert got[0] == (dt.datetime(2018, 6, 1), dt.datetime(2020, 1, 1))
    assert got[1] == (dt.datetime(2020, 1, 1), dt.datetime(2025, 1, 1))
    assert got[-1][1] == dt.datetime(2026, 9, 11)


def test_chunks_are_contiguous_and_cover_the_range():
    lo, hi = dt.datetime(1872, 1, 1), dt.datetime(2026, 9, 11)
    chunks = list(chunk_ranges(lo, hi, 5))
    assert chunks[0][0] == lo and chunks[-1][1] == hi
    for a, b in zip(chunks, chunks[1:], strict=False):
        assert a[1] == b[0]


def test_short_range_is_one_chunk():
    assert len(list(chunk_ranges(dt.datetime(2026, 9, 1), dt.datetime(2026, 9, 11), 5))) == 1


# A full-archive walk asks for 1870-2026, but most stations report over a small
# slice of it. Without clipping, every station pays for all 32 chunks.
FULL = (dt.datetime(1870, 1, 1), dt.datetime(2027, 1, 1))


def test_clip_drops_the_chunks_before_a_station_existed():
    modern = clip_window(*FULL, dt.datetime(2000, 3, 15), dt.datetime(2026, 8, 1))
    assert len(list(chunk_ranges(*modern, 5))) == 6
    assert len(list(chunk_ranges(*FULL, 5))) == 32


def test_clip_keeps_the_final_observation():
    # `time_constraints` ends with a strict `<`, so the bound must sit past it.
    _, end = clip_window(*FULL, dt.datetime(1900, 1, 1), dt.datetime(1905, 6, 30, 23, 59, 59))
    assert end > dt.datetime(1905, 6, 30, 23, 59, 59)


def test_clip_never_widens_the_requested_range():
    lo, hi = dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1)
    got = clip_window(lo, hi, dt.datetime(1890, 1, 1), dt.datetime(2026, 1, 1))
    assert got == (lo, hi)


def test_a_station_outside_the_range_costs_no_requests():
    lo, hi = dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1)
    gone = clip_window(lo, hi, dt.datetime(1890, 1, 1), dt.datetime(1900, 1, 1))
    assert list(chunk_ranges(*gone, 5)) == []


def test_clip_is_a_no_op_without_metadata():
    assert clip_window(*FULL, None, None) == FULL


def test_rate_limiter_spaces_requests(monkeypatch):
    slept = []
    monkeypatch.setattr("pcds.ingest.time.sleep", slept.append)
    lim = RateLimiter(max_rps=2.0)
    for _ in range(4):
        lim.acquire()
    assert sum(slept) > 1.0  # 4 requests at 2/s cannot finish instantly


def test_zero_rate_limit_is_a_noop(monkeypatch):
    monkeypatch.setattr("pcds.ingest.time.sleep", lambda s: (_ for _ in ()).throw(AssertionError))
    RateLimiter(max_rps=0).acquire()
