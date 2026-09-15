from __future__ import annotations

import datetime as dt

from pcds.plan import MIB, arrival_rate, cadence

NOW = dt.datetime(2026, 9, 11)


def _h(freq, nvars, last_days_ago):
    return {
        "freq": freq,
        "variable_ids": list(range(nvars)),
        "max_obs_time": NOW - dt.timedelta(days=last_days_ago),
    }


def test_arrival_rate_counts_only_recent_histories():
    hist = [_h("1-hourly", 10, 1), _h("daily", 5, 2), _h("1-hourly", 10, 400)]
    rate = arrival_rate(hist, NOW, active_days=30)
    assert rate.active_histories == 2
    assert rate.obs_per_day == 24 * 10 + 1 * 5


def test_unknown_frequency_falls_back_to_hourly():
    rate = arrival_rate([_h(None, 3, 0), _h("irregular", 3, 0)], NOW)
    assert rate.obs_per_day == 24 * 3 * 2


def test_histories_with_no_variables_contribute_nothing():
    assert arrival_rate([_h("1-hourly", 0, 0)], NOW).active_histories == 0


def test_cadence_matches_the_measured_pcds_rate():
    # 271,966 obs/day and ~3 bytes/row is what the live metadata implies.
    cad = cadence(271_966, 3.0, min_file_bytes=64 * MIB, target_file_bytes=256 * MIB)
    assert 750_000 < cad.bytes_per_day < 850_000
    assert 75 < cad.days_to_min_file < 90
    assert cad.append_cron == "20 14 * * *"       # daily, freshness-driven
    assert cad.compact_cron == "30 15 1 */3 *"    # quarterly, size-driven


def test_a_firehose_dataset_gets_a_tighter_compaction_cron():
    cad = cadence(500_000_000, 3.0, min_file_bytes=64 * MIB, target_file_bytes=256 * MIB)
    assert cad.days_to_min_file < 1
    assert cad.compact_cron == "30 15 * * 0"


def test_hourly_freshness_moves_append_not_compaction():
    slow = cadence(271_966, 3.0, freshness_hours=24)
    fast = cadence(271_966, 3.0, freshness_hours=1)
    assert fast.append_cron == "0 * * * *"
    assert fast.compact_cron == slow.compact_cron
