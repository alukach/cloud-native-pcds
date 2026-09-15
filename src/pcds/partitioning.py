"""Period (partition) planning.

PCDS is wildly non-uniform in time: 1872-1950 is a handful of daily manual
stations, 2010-present is ~950 mostly-hourly automatic stations. Partitioning
strictly by year would produce ~150 partitions ranging from a few hundred KB to
several hundred MB, and small files are the single most common way to make a
"cloud-optimized" dataset slow.

So the partition key is a *period*: a contiguous run of years chosen so every
partition clears a size floor. Sparse history gets bucketed into decades or
multi-decade spans; recent years stand alone. The chosen layout is written to
``layout.json`` at the dataset root so writers and readers agree.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

MIB = 1024 * 1024


@dataclass(frozen=True)
class Period:
    period: str
    start_year: int
    end_year: int

    def contains(self, year: int) -> bool:
        return self.start_year <= year <= self.end_year


def label(start: int, end: int) -> str:
    return str(start) if start == end else f"{start}-{end}"


def plan_periods(
    year_bytes: dict[int, int],
    *,
    min_file_bytes: int = 64 * MIB,
    max_span_years: int = 50,
) -> list[Period]:
    """Greedily merge consecutive years until each bucket clears the floor.

    `year_bytes` is an estimate (from row counts x observed bytes/row); it only
    needs to be right to within a factor of two. A trailing bucket that cannot
    reach the floor is merged backwards into its predecessor so the dataset never
    ends on a stub.
    """
    if not year_bytes:
        return []
    years = sorted(year_bytes)
    periods: list[Period] = []
    start = years[0]
    acc = 0
    prev = years[0]
    for year in years:
        # A gap in coverage does not break the bucket -- empty years cost nothing
        # and keeping periods contiguous makes `period_for` total.
        acc += year_bytes.get(year, 0)
        if acc >= min_file_bytes or (year - start + 1) >= max_span_years:
            periods.append(Period(label(start, year), start, year))
            start = year + 1
            acc = 0
        prev = year
    if acc > 0 or start <= prev:
        end = years[-1]
        if start > end:
            end = start
        if periods and acc < min_file_bytes:
            last = periods.pop()
            periods.append(Period(label(last.start_year, end), last.start_year, end))
        else:
            periods.append(Period(label(start, end), start, end))
    return periods


class Layout:
    """The period map, plus an open-ended tail for years beyond the plan."""

    def __init__(self, periods: list[Period]):
        self.periods = sorted(periods, key=lambda p: p.start_year)

    @classmethod
    def yearly(cls, start: int, end: int) -> "Layout":
        return cls([Period(str(y), y, y) for y in range(start, end + 1)])

    @classmethod
    def from_json(cls, blob: bytes | str) -> "Layout":
        data = json.loads(blob)
        return cls([Period(**p) for p in data["periods"]])

    def to_json(self) -> str:
        return json.dumps(
            {"version": 1, "periods": [asdict(p) for p in self.periods]}, indent=2
        )

    def period_for(self, year: int) -> str:
        for p in self.periods:
            if p.contains(year):
                return p.period
        # Years past the planned tail each get their own partition; the next
        # compaction run re-plans and merges them if they turn out small.
        if self.periods and year < self.periods[0].start_year:
            return label(year, year)
        return str(year)

    def periods_for_range(self, start_year: int, end_year: int) -> list[str]:
        seen: list[str] = []
        for y in range(start_year, end_year + 1):
            p = self.period_for(y)
            if p not in seen:
                seen.append(p)
        return seen
