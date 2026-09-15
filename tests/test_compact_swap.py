"""The partition swap used to delete the live partition before writing its
replacement, so a crash in the window destroyed a period that no delta could
rebuild, and `walk.sh` then treated the truncated result as done forever.

The swap now moves the new generation in first, writes `_SUCCESS` as the commit
point, and only then drops the superseded files. These tests pin the three
states a crash can leave behind.
"""

from __future__ import annotations

import datetime as dt
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pcds import paths
from pcds.compact import (
    _generation,
    _parquet_files,
    compact_period,
    mark_compacted,
    reconcile_period,
)
from pcds.config import Settings
from pcds.pack import write_delta
from pcds.schema import OBSERVATIONS
from pcds.storage import open_store

PERIOD = "2024"


def _settings(root) -> Settings:
    return Settings(root=str(root), row_group_rows=200, target_file_bytes=1 << 20)


def _obs(n: int, *, value: float, start_hour: int = 0) -> pa.Table:
    t0 = dt.datetime(2024, 1, 1)
    return pa.table(
        {
            "station_id": pa.array([1] * n, pa.int32()),
            "variable_id": pa.array([1] * n, pa.int16()),
            "obs_time": pa.array(
                [t0 + dt.timedelta(hours=start_hour + i) for i in range(n)], pa.timestamp("s")
            ),
            "value": pa.array([value] * n, pa.float64()),
        }
    ).cast(OBSERVATIONS)


def _seed_base(store, settings, table: pa.Table, name: str = "part-00000.parquet") -> str:
    """Write a base partition file directly, bypassing compaction."""
    prefix = paths.period_prefix(PERIOD)
    store.mkdirs(prefix)
    path = store.join(prefix, name)
    with store.fs.open_output_stream(path) as sink:
        pq.write_table(table, sink)
    return path


def _read_all(store) -> pa.Table:
    files = sorted(_parquet_files(store, paths.period_prefix(PERIOD)))
    return pa.concat_tables([pq.read_table(f) for f in files])


@pytest.fixture
def store_and_settings(tmp_path):
    settings = _settings(tmp_path)
    return open_store(settings), settings


def test_generation_parsing():
    assert _generation("a/b/part-20260915T060000Z-00000.parquet") == "20260915T060000Z"
    # Files written before generations existed must still be recognised.
    assert _generation("a/b/part-00000.parquet") == ""


def test_compaction_replaces_the_old_generation_without_duplicating_rows(store_and_settings):
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(50, value=1.0))
    stats = compact_period(store, settings, PERIOD, (2024, 2024))

    assert stats["rows"] == 50
    # The legacy file is gone, exactly one generation remains, and no row doubled.
    gens = {_generation(p) for p in _parquet_files(store, paths.period_prefix(PERIOD))}
    assert gens == {stats["generation"]}
    assert _read_all(store).num_rows == 50


def test_success_marker_names_the_current_generation(store_and_settings):
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(20, value=1.0))
    stats = compact_period(store, settings, PERIOD, (2024, 2024))

    marker = store.join(paths.period_success(PERIOD))
    with store.fs.open_input_stream(marker) as f:
        payload = json.loads(f.read().decode())
    assert payload["generation"] == stats["generation"]
    assert payload["rows"] == 20
    # The marker must not be picked up as data.
    assert marker not in _parquet_files(store, paths.period_prefix(PERIOD))


def test_delta_revision_wins_and_the_base_is_not_resurrected(store_and_settings):
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(10, value=1.0))
    write_delta(store, settings, _obs(10, value=9.0), run_id="r1", shard="s0")

    compact_period(store, settings, PERIOD, (2024, 2024))
    table = _read_all(store)
    assert table.num_rows == 10
    assert set(table.column("value").to_pylist()) == {9.0}


def test_crash_before_the_commit_point_rolls_back(store_and_settings):
    """New files half-moved in, `_SUCCESS` still naming the old generation."""
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(10, value=1.0))
    compact_period(store, settings, PERIOD, (2024, 2024))
    good = _parquet_files(store, paths.period_prefix(PERIOD))

    # Simulate the interrupted next swap: a stray newer-generation file arrives
    # before its `_SUCCESS` is written.
    stray = store.join(paths.period_prefix(PERIOD), "part-29991231T235959Z-00000.parquet")
    with store.fs.open_output_stream(stray) as sink:
        pq.write_table(_obs(10, value=5.0), sink)

    assert reconcile_period(store, PERIOD) == 1
    assert _parquet_files(store, paths.period_prefix(PERIOD)) == good
    assert set(_read_all(store).column("value").to_pylist()) == {1.0}


def test_crash_after_the_commit_point_rolls_forward(store_and_settings):
    """`_SUCCESS` already names the new generation; the superseded files linger."""
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(10, value=1.0))
    stats = compact_period(store, settings, PERIOD, (2024, 2024))

    # Simulate the old generation surviving the crash.
    leftover = _seed_base(store, settings, _obs(10, value=1.0), name="part-00000.parquet")
    assert leftover in _parquet_files(store, paths.period_prefix(PERIOD))

    assert reconcile_period(store, PERIOD) == 1
    remaining = _parquet_files(store, paths.period_prefix(PERIOD))
    assert {_generation(p) for p in remaining} == {stats["generation"]}


def test_a_crash_during_the_swap_never_empties_the_partition(store_and_settings, monkeypatch):
    """The blocker this change exists for. Compaction used to delete the base
    prefix before writing its replacement, so dying in that window left the
    period empty with the deltas already advanced past it."""
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(30, value=1.0))
    write_delta(store, settings, _obs(30, value=9.0), run_id="r1", shard="s0")

    import pcds.compact as compact_mod

    boom = RuntimeError("killed mid-swap")

    def die(*args, **kwargs):
        raise boom

    monkeypatch.setattr(compact_mod, "_write_success", die)
    with pytest.raises(RuntimeError):
        compact_period(store, settings, PERIOD, (2024, 2024))

    # Every original row is still readable, which is the whole point.
    table = _read_all(store)
    assert table.num_rows >= 30
    assert 1.0 in set(table.column("value").to_pylist())

    # And the half-finished generation is cleaned up on the next run.
    monkeypatch.undo()
    compact_period(store, settings, PERIOD, (2024, 2024))
    final = _read_all(store)
    assert final.num_rows == 30
    assert set(final.column("value").to_pylist()) == {9.0}


def test_mark_compacted_adopts_a_partition_written_before_markers(store_and_settings):
    """Every partition compacted before `_SUCCESS` existed looks unfinished to
    walk.sh, which would re-walk the archive and put the whole request cost back
    on PCIC. Marking them is the migration."""
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(10, value=1.0))

    assert mark_compacted(store, PERIOD) is True
    assert store.exists(paths.period_success(PERIOD))
    # Already marked, so a second call is a no-op rather than a rewrite.
    assert mark_compacted(store, PERIOD) is False

    # The legacy files must survive reconciliation, not be mistaken for debris.
    assert reconcile_period(store, PERIOD) == 0
    assert len(_parquet_files(store, paths.period_prefix(PERIOD))) == 1


def test_mark_compacted_refuses_a_generation_tagged_partition(store_and_settings):
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(10, value=1.0))
    compact_period(store, settings, PERIOD, (2024, 2024))
    store.delete(paths.period_success(PERIOD))

    with pytest.raises(ValueError, match="run a compaction"):
        mark_compacted(store, PERIOD)


def test_mark_compacted_refuses_un_compacted_shard_output(store_and_settings):
    """Backfill shard files are untagged too, so the generation alone cannot
    tell them from a legacy compacted partition. Marking a period that holds
    only some shards' output would hide the missing shards permanently. This is
    the exact state `period=1998` was left in by an interrupted walk."""
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(10, value=1.0), name="s005-00000.parquet")
    _seed_base(store, settings, _obs(10, value=2.0), name="s007-00000.parquet")

    with pytest.raises(ValueError, match="un-compacted shard output"):
        mark_compacted(store, PERIOD)
    assert not store.exists(paths.period_success(PERIOD))


def test_mark_compacted_ignores_an_empty_partition(store_and_settings):
    store, _ = store_and_settings
    assert mark_compacted(store, PERIOD) is False


def test_reconcile_is_a_noop_without_a_marker(store_and_settings):
    """No `_SUCCESS` means we cannot tell which generation is current, so
    nothing may be deleted on a guess."""
    store, settings = store_and_settings
    _seed_base(store, settings, _obs(10, value=1.0))
    assert reconcile_period(store, PERIOD) == 0
    assert len(_parquet_files(store, paths.period_prefix(PERIOD))) == 1
