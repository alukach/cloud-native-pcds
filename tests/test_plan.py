from __future__ import annotations

import datetime as dt

from pcds.partitioning import plan_periods
from pcds.plan import (
    DEFAULT_BYTES_PER_ROW,
    MIB,
    arrival_rate,
    cadence,
    estimated_year_rows,
    year_bytes_for_plan,
)

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


# --- layout planning over a partially backfilled archive ---------------------
#
# `pcds layout` used to plan from measured years alone whenever a catalog
# existed. Mid-backfill that means the plan covers only what has been written:
# every future year vanishes, and the whole loaded span collapses into one
# partition because it cannot clear the size floor. Compaction never moves rows
# between periods, so such a layout cannot be repaired without re-fetching.

SPAN = [
    {
        "freq": "hourly",
        "variable_ids": [1, 2, 3],
        "min_obs_time": dt.datetime(1900, 1, 1),
        "max_obs_time": dt.datetime(2026, 1, 1),
    }
]
# Only the early years are loaded, and they pack far smaller than the default.
PARTIAL = {y: 200_000 for y in range(1900, 1971)}


def test_plan_spans_the_range_when_only_early_years_are_written():
    yb = year_bytes_for_plan(
        SPAN, 1870, 2027, measured_rows=PARTIAL, measured_bytes_per_row=0.98
    )
    assert max(yb) >= 2026, "years with no data yet must still be planned for"


def test_partial_backfill_does_not_collapse_the_plan():
    yb = year_bytes_for_plan(
        SPAN, 1870, 2027, measured_rows=PARTIAL, measured_bytes_per_row=0.98
    )
    assert len(plan_periods(yb, min_file_bytes=64 * MIB)) > 1
    # Planning from the written years alone is what produced a single period.
    written_only = {y: int(n * 0.98) for y, n in PARTIAL.items()}
    assert len(plan_periods(written_only, min_file_bytes=64 * MIB)) == 1


def test_measured_years_use_the_measured_rate_and_the_rest_do_not():
    yb = year_bytes_for_plan(
        SPAN, 1870, 2027, measured_rows={1950: 1_000_000}, measured_bytes_per_row=0.5
    )
    assert yb[1950] == 500_000
    # An unwritten year keeps the conservative default, so a small measured rate
    # taken from a partial load cannot shrink the plan for data still to come.
    est = estimated_year_rows(SPAN, 1870, 2027)
    assert yb[2000] == int(est[2000] * DEFAULT_BYTES_PER_ROW)
