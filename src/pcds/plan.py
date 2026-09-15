"""Sizing planner: how often should the cron run?

The append interval is not a taste question, it falls out of two measurements:

1. How fast observations arrive. Measured from the station metadata (reporting
   frequency x number of variables per active history) before any data exists,
   and from actual delta sizes once it does.
2. How many bytes a row costs once packed. Measured from the catalog.

From those: the number of days needed to accumulate a partition worth writing.
Where the two pull apart -- and for PCDS they pull apart hard -- you run the
*append* often enough for the data to be useful and the *compaction* rarely
enough that it produces target-sized files.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

MIB = 1024 * 1024

# Observations per day implied by each reporting frequency, per variable.
OBS_PER_DAY = {
    "1-hourly": 24,
    "hourly": 24,
    "daily": 1,
    "12-hourly": 2,
    "semidaily": 2,
    "15-minute": 96,
    "5-minute": 288,
    "1-minute": 1440,
    # `irregular` and null are event-driven (fire weather, avalanche, tipping
    # buckets). Hourly is the honest middle of the observed distribution.
    "irregular": 24,
    None: 24,
}

# Measured over the written catalog in September 2026, not guessed: 1.06 B/row
# across 115.7M archive rows (1872-1997) and 1.36-1.39 B/row over 2020 and 2021.
# The modern figure is the one that matters here, because the years this default
# is applied to are the unwritten dense ones. It was 3.0, which overstated every
# partition by ~2.2x and is half of why a 64 MiB plan produced 2.5-13 MiB files.
DEFAULT_BYTES_PER_ROW = 1.4

# The other half. `estimated_year_rows` assumes every variable on a history
# reports at that history's frequency for every day it spans; most do not, so it
# overpredicts. Against the only years with both an estimate and a measurement it
# runs 1.78x high for 2020 and 1.90x for 2021, so the estimate is scaled by the
# reciprocal of their mean. Calibrated on the modern regime and correct only
# there -- it runs 2.6-15x high over 1872-1997, where the bias varies with how
# sparse the network was, and those years are measured now anyway. Re-measure
# this against a full year of real data before trusting a fresh plan.
ESTIMATE_CALIBRATION = 1 / 1.84


def estimated_year_rows(
    histories: list[dict], start_year: int, end_year: int
) -> dict[int, int]:
    """Rows expected per calendar year, from station metadata alone.

    Each history contributes its reporting frequency times its variable count
    for every day it spans. This is the only source available for years that
    have no data yet, which during a backfill is most of them.
    """
    year_rows: dict[int, int] = {}
    for h in histories:
        lo, hi = h.get("min_obs_time"), h.get("max_obs_time")
        if not lo or not hi:
            continue
        nvars = len(h.get("variable_ids") or [])
        per_day = OBS_PER_DAY.get(h.get("freq"), OBS_PER_DAY[None]) * nvars
        for y in range(max(lo.year, start_year), min(hi.year, end_year) + 1):
            lo_y = max(lo, dt.datetime(y, 1, 1))
            hi_y = min(hi, dt.datetime(y + 1, 1, 1))
            days = max((hi_y - lo_y).days, 0)
            year_rows[y] = year_rows.get(y, 0) + int(per_day * days)
    return {y: int(n * ESTIMATE_CALIBRATION) for y, n in year_rows.items()}


def year_bytes_for_plan(
    histories: list[dict],
    start_year: int,
    end_year: int,
    *,
    measured_rows: dict[int, int] | None = None,
    measured_bytes_per_row: float | None = None,
) -> dict[int, int]:
    """Expected packed bytes per calendar year across the whole requested range.

    Years that have been written use their measured row count at the measured
    bytes/row; every other year falls back to the metadata estimate at the
    conservative default rate. Mixing the two matters during a backfill: a plan
    built from written years alone covers only the past, and because compaction
    never moves rows between periods, a layout that omits a year cannot be
    repaired afterwards without re-fetching it.

    The rate is kept per-year for the same reason. Bytes/row measured over a
    partial load is not representative of the years still to come, so applying
    it to them would shrink the plan for data nobody has seen yet.
    """
    rows = estimated_year_rows(histories, start_year, end_year)
    measured = measured_rows or {}
    rows.update(measured)
    rate = measured_bytes_per_row or DEFAULT_BYTES_PER_ROW
    return {
        y: int(n * (rate if y in measured else DEFAULT_BYTES_PER_ROW))
        for y, n in rows.items()
    }


@dataclass
class ArrivalRate:
    active_histories: int
    variable_slots: int
    obs_per_day: float
    by_frequency: dict[str, int]


def arrival_rate(
    histories: list[dict], as_of: dt.datetime, active_days: int = 30
) -> ArrivalRate:
    """Rows per day from histories that reported within `active_days`."""
    active = 0
    slots = 0
    total = 0.0
    by_freq: dict[str, int] = {}
    for h in histories:
        last = h.get("max_obs_time")
        if last is None:
            continue
        if (as_of - last).days > active_days:
            continue
        nvars = len(h.get("variable_ids") or [])
        if nvars == 0:
            continue
        active += 1
        slots += nvars
        freq = h.get("freq")
        total += OBS_PER_DAY.get(freq, OBS_PER_DAY[None]) * nvars
        by_freq[str(freq)] = by_freq.get(str(freq), 0) + 1
    return ArrivalRate(active, slots, total, dict(sorted(by_freq.items())))


@dataclass
class Cadence:
    obs_per_day: float
    bytes_per_row: float
    bytes_per_day: float
    min_file_bytes: int
    days_to_min_file: float
    days_to_target_file: float
    append_cron: str
    append_note: str
    compact_cron: str
    compact_note: str


def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"


def cadence(
    obs_per_day: float,
    bytes_per_row: float,
    *,
    min_file_bytes: int = 128 * MIB,
    target_file_bytes: int = 256 * MIB,
    freshness_hours: int = 24,
) -> Cadence:
    bpd = obs_per_day * bytes_per_row
    to_min = min_file_bytes / bpd if bpd else float("inf")
    to_target = target_file_bytes / bpd if bpd else float("inf")

    # Appending is about freshness, not file size: a delta file is staged, not
    # published as a partition, so it can be as small as it likes.
    if freshness_hours <= 1:
        append_cron, append_desc = "0 * * * *", "hourly"
    elif freshness_hours <= 6:
        append_cron, append_desc = "0 */6 * * *", "every 6 hours"
    else:
        append_cron, append_desc = "20 14 * * *", "daily at 14:20 UTC (06:20 PT)"
    append_note = (
        f"{append_desc}; each run stages ~{_fmt_bytes(bpd * freshness_hours / 24)} "
        f"({obs_per_day * freshness_hours / 24:,.0f} rows)"
    )

    # Compaction is mostly about file size: pick the coarsest cadence that still
    # clears the floor, so the hot partition is rewritten as rarely as possible.
    #
    # Quarterly is the floor on that, though, and it is not a size argument. A
    # staged delta lives under `_staging/`, which is pipeline machinery and not
    # part of the published dataset, so a row is invisible to readers until the
    # compaction that folds it. The interval is therefore the real publication
    # lag, whatever the daily append does. Once the partition floor went to
    # 128 MiB, a period took 583 days of arrivals to fill and the size rule on
    # its own said to fold annually: correct about bytes, and a year of
    # invisible observations.
    if to_min <= 7:
        compact_cron, compact_desc, span = "30 15 * * 0", "weekly (Sundays)", 7
    elif to_min <= 31:
        compact_cron, compact_desc, span = "30 15 1 * *", "monthly (1st)", 30.4
    else:
        compact_cron, compact_desc, span = "30 15 1 */3 *", "quarterly", 91
    years_to_target = to_target / 365
    compact_note = (
        f"{compact_desc}; folds ~{_fmt_bytes(bpd * span)} of deltas into the "
        f"current period. A full year accumulates ~{_fmt_bytes(bpd * 365)}, so a "
        f"period needs ~{years_to_target:.1f} years to reach the "
        f"{_fmt_bytes(target_file_bytes)} target -- `pcds layout` is what merges "
        f"years to get there, compaction only packs within one period."
    )
    return Cadence(
        obs_per_day=obs_per_day,
        bytes_per_row=bytes_per_row,
        bytes_per_day=bpd,
        min_file_bytes=min_file_bytes,
        days_to_min_file=to_min,
        days_to_target_file=to_target,
        append_cron=append_cron,
        append_note=append_note,
        compact_cron=compact_cron,
        compact_note=compact_note,
    )


def report(rate: ArrivalRate, cad: Cadence, *, target_file_bytes: int) -> str:
    lines = [
        "PCDS ingest sizing",
        "==================",
        f"  actively reporting histories : {rate.active_histories:,}",
        f"  station x variable slots     : {rate.variable_slots:,}",
        f"  by frequency                 : {rate.by_frequency}",
        "",
        f"  observations / day           : {cad.obs_per_day:,.0f}",
        f"  packed bytes / row           : {cad.bytes_per_row:.2f}",
        f"  bytes / day                  : {_fmt_bytes(cad.bytes_per_day)}",
        f"  bytes / year                 : {_fmt_bytes(cad.bytes_per_day * 365)}",
        "",
        f"  days to {_fmt_bytes(cad.min_file_bytes):>9}            : {cad.days_to_min_file:,.0f}",
        f"  days to {_fmt_bytes(target_file_bytes):>9}            : {cad.days_to_target_file:,.0f}",
        "",
        "Recommended schedule",
        "--------------------",
        f"  append   {cad.append_cron:<14} {cad.append_note}",
        f"  compact  {cad.compact_cron:<14} {cad.compact_note}",
    ]
    return "\n".join(lines)
