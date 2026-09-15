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

DEFAULT_BYTES_PER_ROW = 3.0  # long schema, sorted, zstd-9: typically 2-4


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
    min_file_bytes: int = 64 * MIB,
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

    # Compaction is about file size. Pick the coarsest cadence that still clears
    # the floor, so the hot partition is rewritten as rarely as possible.
    if to_min <= 7:
        compact_cron, compact_desc, span = "30 15 * * 0", "weekly (Sundays)", 7
    elif to_min <= 31:
        compact_cron, compact_desc, span = "30 15 1 * *", "monthly (1st)", 30.4
    elif to_min <= 95:
        compact_cron, compact_desc, span = "30 15 1 */3 *", "quarterly", 91
    else:
        compact_cron, compact_desc, span = "30 15 1 1 *", "annually (Jan 1)", 365
    compact_note = (
        f"{compact_desc}; folds ~{_fmt_bytes(bpd * span)} of deltas into the "
        f"current period. A full year accumulates ~{_fmt_bytes(bpd * 365)}, so "
        f"yearly partitions land near the {_fmt_bytes(target_file_bytes)} target."
    )
    return Cadence(
        obs_per_day=obs_per_day,
        bytes_per_row=bytes_per_row,
        bytes_per_day=bpd,
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
        f"  days to 64 MiB               : {cad.days_to_min_file:,.0f}",
        f"  days to {_fmt_bytes(target_file_bytes):>9}            : {cad.days_to_target_file:,.0f}",
        "",
        "Recommended schedule",
        "--------------------",
        f"  append   {cad.append_cron:<14} {cad.append_note}",
        f"  compact  {cad.compact_cron:<14} {cad.compact_note}",
    ]
    return "\n".join(lines)
