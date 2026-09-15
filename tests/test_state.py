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


# ------------------------------------------------- concurrent shard writers --


def _store(tmp_path):
    from pcds.config import Settings
    from pcds.storage import open_store

    return open_store(Settings(root=str(tmp_path)))


def test_concurrent_shards_do_not_lose_each_others_watermarks(tmp_path):
    """8 shards saving at once must not end up as one shard's snapshot.

    Each writes its own file; `load` merges them. Station ids are disjoint
    across shards, so nothing here depends on who finished first.
    """
    store = _store(tmp_path)
    now = dt.datetime(2026, 9, 14)
    for shard in range(8):
        marks = Watermarks.load(store)  # every shard starts from the same snapshot
        station = 100 + shard
        marks.record_success(
            station, "EC_raw", f"S{station}",
            max_obs_time=dt.datetime(2026, 1, 1 + shard), rows=10, now=now,
        )
        marks.save(store, writer=f"s{shard:03d}")

    merged = Watermarks.load(store)
    assert sorted(merged.rows) == [100 + s for s in range(8)]
    assert merged.watermark(107) == dt.datetime(2026, 1, 8)


def test_temp_files_are_not_shared_between_writers(tmp_path):
    """A shared .tmp name lets two shards interleave bytes into one object."""
    store = _store(tmp_path)
    marks = Watermarks({})
    marks.record_success(
        1, "EC_raw", "S1", max_obs_time=dt.datetime(2026, 1, 1), rows=1,
        now=dt.datetime(2026, 9, 14),
    )
    marks.save(store, writer="s000")
    marks.save(store, writer="s001")
    names = {i.path.rsplit("/", 1)[-1] for i in store.ls("_state")}
    assert names == {"watermarks-s000.parquet", "watermarks-s001.parquet"}


def test_later_watermark_wins_when_files_overlap(tmp_path):
    """An un-sharded `append` file and a shard file can both hold a station."""
    store = _store(tmp_path)
    now = dt.datetime(2026, 9, 14)
    for writer, when in (("s000", dt.datetime(2026, 1, 1)), (None, dt.datetime(2026, 6, 1))):
        marks = Watermarks({})
        marks.record_success(7, "EC_raw", "S7", max_obs_time=when, rows=1, now=now)
        marks.save(store, writer=writer)

    assert Watermarks.load(store).watermark(7) == dt.datetime(2026, 6, 1)
