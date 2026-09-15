from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("pyarrow")

from pcds.ingest import RateLimiter, chunk_ranges  # noqa: E402


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
