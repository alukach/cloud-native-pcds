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
