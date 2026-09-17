from __future__ import annotations

from pcds.partitioning import MIB, Layout, Period, label, plan_periods


def test_sparse_years_merge_recent_years_stand_alone():
    year_bytes = {}
    for y in range(1872, 2027):
        year_bytes[y] = 2 * MIB if y < 1950 else (20 * MIB if y < 1990 else 300 * MIB)
    periods = plan_periods(year_bytes, min_file_bytes=64 * MIB)
    by_year = {p.start_year: p for p in periods}
    assert by_year[1872].end_year > 1900          # a multi-decade bucket
    assert any(p.start_year == p.end_year == 2024 for p in periods)  # a solo year
    assert len(periods) < 60                       # not one partition per year


def test_every_year_maps_to_exactly_one_period():
    periods = plan_periods({y: 100 * MIB for y in range(2000, 2011)}, min_file_bytes=64 * MIB)
    years = [y for p in periods for y in range(p.start_year, p.end_year + 1)]
    assert years == sorted(set(years)) == list(range(2000, 2011))


def test_trailing_stub_merges_backwards():
    # last year is tiny; it must not become its own sub-floor partition
    yb = {2020: 100 * MIB, 2021: 100 * MIB, 2022: 1 * MIB}
    periods = plan_periods(yb, min_file_bytes=64 * MIB)
    assert periods[-1].end_year == 2022
    assert periods[-1].start_year <= 2021


def test_a_century_of_sparse_years_is_one_bucket_not_a_stub():
    # A `max_span_years` cap used to cut here at 20 (and in production at 50),
    # emitting a partition far below the floor. The floor is now the only cut
    # rule, so a sparse run merges for as long as it needs to.
    yb = {y: 1 * MIB for y in range(1800, 1900)}
    # 100 MiB total: one 64 MiB cut, then the 36 MiB remainder is a trailing
    # stub and merges backwards, so the sparse run ends up whole.
    assert plan_periods(yb, min_file_bytes=64 * MIB) == [Period("1800-1899", 1800, 1899)]


def test_layout_roundtrip_and_lookup():
    lay = Layout([Period("1872-1949", 1872, 1949), Period("2024", 2024, 2024)])
    again = Layout.from_json(lay.to_json())
    assert again.period_for(1900) == "1872-1949"
    assert again.period_for(2024) == "2024"
    # years beyond the plan get their own partition until the next re-plan
    assert again.period_for(2031) == "2031"


def test_periods_for_range_is_deduped_and_ordered():
    lay = Layout([Period("2020-2022", 2020, 2022), Period("2023", 2023, 2023)])
    assert lay.periods_for_range(2020, 2023) == ["2020-2022", "2023"]


def test_label():
    assert label(2024, 2024) == "2024"
    assert label(1872, 1949) == "1872-1949"


def test_cut_after_keeps_boundaries_on_existing_partitions():
    # A re-plan is only useful if it can be applied to data already on disk.
    # Here the floor falls mid-partition: unconstrained, the plan cuts after
    # 2003, which splits a partition that runs 2003-2006 and forces a re-fetch.
    yb = {y: 40 * MIB for y in range(2000, 2008)}
    ends = {2002, 2006}  # what is written: 2000-2002, 2003-2006

    free = plan_periods(yb, min_file_bytes=128 * MIB)
    assert free[0].end_year == 2003, "precondition: the greedy cut lands inside a partition"

    held = plan_periods(yb, min_file_bytes=128 * MIB, cut_after=ends)
    # The invariant, not a particular shape: no period may end part-way through
    # one that is already written. 2007 has nothing written past it, so a cut
    # there (or a merge back over it) is free either way.
    last_written = max(ends)
    for p in held:
        assert p.end_year in ends or p.end_year >= last_written, (
            f"{p.period} ends inside a written partition"
        )


def test_cut_after_still_merges_a_trailing_stub_backwards():
    yb = {2020: 200 * MIB, 2021: 200 * MIB, 2022: 1 * MIB}
    periods = plan_periods(yb, min_file_bytes=128 * MIB, cut_after={2020, 2022})
    assert [p.period for p in periods] == ["2020", "2021-2022"]


def test_cut_after_none_is_the_old_behaviour():
    yb = {y: 40 * MIB for y in range(2000, 2008)}
    assert plan_periods(yb, min_file_bytes=128 * MIB) == plan_periods(
        yb, min_file_bytes=128 * MIB, cut_after=None
    )


def test_replanning_the_measured_archive_merges_whole_partitions():
    """The case this exists for, with the sizes actually measured on disk.

    Five of ten periods came out under the 128 MiB floor because the pre-walk
    estimate overpredicts. Re-planning has to fix that without cutting inside
    any of the ten, or the rows have to be fetched again.
    """
    written = {  # period end year -> MiB actually written
        1998: 129, 2002: 71, 2006: 138, 2009: 125, 2010: 0, 2012: 108,
        2015: 115, 2018: 131, 2020: 116, 2022: 143, 2026: 207,
    }
    spans = [(1872, 1998), (1999, 2002), (2003, 2006), (2007, 2009), (2010, 2012),
             (2013, 2015), (2016, 2018), (2019, 2020), (2021, 2022), (2023, 2026)]
    yb = {}
    for lo, hi in spans:
        total = written[hi] * MIB
        for y in range(lo, hi + 1):  # spread evenly; only the totals matter
            yb[y] = total // (hi - lo + 1)

    periods = plan_periods(
        yb, min_file_bytes=128 * MIB, cut_after={hi for _, hi in spans}
    )
    ends = {hi for _, hi in spans}
    for p in periods:
        assert p.end_year in ends, f"{p.period} cuts inside a written partition"
    # and every period now clears the floor
    for p in periods:
        got = sum(yb[y] for y in range(p.start_year, p.end_year + 1))
        assert got >= 128 * MIB, f"{p.period} is still under the floor"
    assert len(periods) < len(spans), "re-planning should merge, not keep ten"
