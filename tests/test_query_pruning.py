"""The documented query pattern must actually prune partitions.

`period` is an opaque partition label, not a year. Recent years stand alone
(`period=2024`) but sparse history is bucketed (`period=1872-1903`), so treating
the label as a year is silently wrong rather than an error:

    WHERE period = '1890'                  -> 0 rows, no error
    WHERE period >= '1900'                 -> drops 1900-1903, because
                                              '1872-1903' < '1900' as strings

And filtering on `obs_time` alone is correct but prunes nothing, because DuckDB
cannot relate a string partition key to a timestamp column. There is no finer
fallback either: files are sorted `(station_id, variable_id, obs_time)`, so
`obs_time` is interleaved per station and every row group spans nearly the whole
period. The partition is the time granularity.

So the docs teach one pattern: resolve years to labels through `layout.json`.
These tests pin that it prunes, so the snippet in `sql/demo.sql`, the README and
the generated AGENTS.md cannot rot into a full scan.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import random

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

import duckdb  # noqa: E402
import pyarrow as pa  # noqa: E402
from test_conformance import _metadata_tables, _settings  # noqa: E402  (sibling module)

from pcds import metadata as md  # noqa: E402
from pcds import paths, portolan, schema  # noqa: E402
from pcds.pack import RollingWriter, sort_table  # noqa: E402
from pcds.partitioning import Layout, Period  # noqa: E402
from pcds.storage import open_store  # noqa: E402

# The shape the README describes: sparse history in wide buckets, recent years
# standing alone. A catalog of only standalone years would hide the whole bug.
PERIODS = [
    Period("1872-1903", 1872, 1903),
    Period("1904-1935", 1904, 1935),
    Period("2024", 2024, 2024),
    Period("2025", 2025, 2025),
]
PER_YEAR_SPARSE, PER_YEAR_DENSE = 200, 4000

# The generated documents query a named station in the Kootenays. If the fixture
# does not contain it the join comes back empty, DuckDB skips the probe side,
# and every assertion about files read passes without measuring anything.
DOC_STATION_ID, DOC_NATIVE_ID = 1, "1145M29"
NELSON_BC = (-117.29, 49.49)  # inside the lon/lat box the README example uses


def _metadata_with_the_documented_station():
    """test_conformance's tables, with station 1 renamed to the one the docs name."""
    tables = dict(_metadata_tables())
    for key in ("stations", "histories"):
        rows = tables[key].to_pylist()
        row = next(r for r in rows if r["station_id"] == DOC_STATION_ID)
        row["native_id"] = DOC_NATIVE_ID
        row["network_name"] = "EC_raw"
        row["lon"], row["lat"] = NELSON_BC
        tables[key] = pa.Table.from_pylist(rows, schema=tables[key].schema)
    return tables


@pytest.fixture(scope="module")
def catalog(tmp_path_factory):
    root = tmp_path_factory.mktemp("pruning")
    settings = _settings(root)
    store = open_store(settings)
    md.write(store, _metadata_with_the_documented_station(), ["daily"])
    with store.fs.open_output_stream(store.join(paths.LAYOUT_FILE)) as sink:
        sink.write(Layout(PERIODS).to_json().encode())

    rng = random.Random(1)
    for period in PERIODS:
        years = range(period.start_year, period.end_year + 1)
        n = PER_YEAR_SPARSE if len(list(years)) > 1 else PER_YEAR_DENSE
        rows = [
            (rng.randint(1, 8), rng.choice([1, 2]),
             dt.datetime(y, 1, 1) + dt.timedelta(minutes=43 * i), rng.random())
            for y in years
            for i in range(n)
        ]
        table = pa.table(
            {
                "station_id": pa.array([r[0] for r in rows], pa.int32()),
                "variable_id": pa.array([r[1] for r in rows], pa.int16()),
                "obs_time": pa.array([r[2] for r in rows], pa.timestamp("s")),
                "value": pa.array([r[3] for r in rows], schema.VALUE_TYPE),
            },
            schema=schema.OBSERVATIONS,
        )
        store.mkdirs(paths.period_prefix(period.period))
        writer = RollingWriter(store, paths.period_prefix(period.period), settings)
        writer.write(sort_table(table))
        writer.close()
    portolan.build(store, settings, portolan.CatalogConfig(base_url=str(root)))
    return store.root


def _setup_sql(root: str) -> str:
    """The view and macro `sql/demo.sql` defines. Kept identical on purpose."""
    return f"""
    SET VARIABLE root = '{root}';
    CREATE OR REPLACE VIEW obs AS
      SELECT * FROM read_parquet(getvariable('root') || '/observations/period=*/*.parquet',
                                 hive_partitioning := true);
    CREATE OR REPLACE VIEW layout AS
      SELECT * FROM (SELECT unnest(periods, recursive := true)
                     FROM read_json_auto(getvariable('root') || '/layout.json'));
    CREATE OR REPLACE MACRO periods_for(lo, hi) AS TABLE
      SELECT period FROM layout WHERE end_year >= lo AND start_year <= hi;
    """


def _run(con, sql: str) -> tuple[int, int]:
    """Returns (rows, parquet files read) for a counting query."""
    plan = con.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    read = [ln for ln in plan.splitlines() if "Total Files Read" in ln]
    assert read, plan
    files = max(int(ln.split("Total Files Read:")[1].split("│")[0].strip()) for ln in read)
    return con.execute(sql).fetchone()[0], files


@pytest.fixture
def con(catalog):
    c = duckdb.connect()
    c.execute(_setup_sql(catalog))
    return c


def _count(where: str) -> str:
    return f"SELECT count(*) FROM obs o WHERE {where}"


def test_documented_pattern_prunes_a_bucketed_range(con):
    """Years inside one merged bucket: one file, right answer."""
    rows, files = _run(con, _count(
        "o.period IN (SELECT period FROM periods_for(1890, 1895)) "
        "AND o.obs_time >= '1890-01-01' AND o.obs_time < '1896-01-01'"
    ))
    truth, _ = _run(con, _count("o.obs_time >= '1890-01-01' AND o.obs_time < '1896-01-01'"))
    assert rows == truth > 0
    assert files == 1, "the layout lookup stopped pruning; the docs now teach a full scan"


def test_documented_pattern_prunes_a_standalone_year(con):
    rows, files = _run(con, _count(
        "o.period IN (SELECT period FROM periods_for(2024, 2024)) "
        "AND o.obs_time >= '2024-01-01' AND o.obs_time < '2025-01-01'"
    ))
    assert rows == PER_YEAR_DENSE
    assert files == 1


def test_documented_pattern_spans_buckets_and_years(con):
    """A range crossing a merged bucket and a standalone year."""
    _, files = _run(con, _count("o.period IN (SELECT period FROM periods_for(1930, 2024))"))
    assert files == 2, "expected 1904-1935 and 2024, not 1872-1903 or 2025"


def test_filtering_on_time_alone_is_correct_but_scans_everything(con):
    """Why the docs cannot just say 'filter obs_time'."""
    _, files = _run(con, _count("o.obs_time >= '2024-01-01' AND o.obs_time < '2025-01-01'"))
    assert files == len(PERIODS)


def test_treating_the_label_as_a_year_is_silently_empty(con):
    """The failure the documentation exists to prevent."""
    rows, _ = _run(con, _count("o.period = '1890'"))
    assert rows == 0

    dropped, _ = _run(con, _count("o.period >= '1900' AND o.obs_time < '1904-01-01'"))
    truth, _ = _run(con, _count("o.obs_time >= '1900-01-01' AND o.obs_time < '1904-01-01'"))
    assert truth > 0
    assert dropped == 0, "string comparison on period silently dropped 1900-1903"


# ------------------------------------------------- the docs we actually ship --


def _sql_block(markdown: str) -> str:
    import re

    block = re.search(r"```sql\n(.*?)```", markdown, re.S)
    assert block, "no sql block in the generated document"
    return block.group(1).replace("INSTALL httpfs; LOAD httpfs;", "")


def _explain_last_statement(con, script: str) -> int:
    """Run the setup, then EXPLAIN ANALYZE the final SELECT.

    Returns the widest fan-out of any parquet scan in the plan. These queries
    scan several files: observations plus the single-file stations and variables.
    Reading only the first counter reports 1 whatever observations does, which
    hid a deliberately broken document until the numbers were printed out
    ([1, 1, 1, 1] documented against [1, 1, 4] regressed).
    """
    statements = [s.strip() for s in script.split(";") if s.strip()]
    for stmt in statements[:-1]:
        con.execute(stmt)
    plan = con.execute("EXPLAIN ANALYZE " + statements[-1]).fetchall()[0][1]
    read = [ln for ln in plan.splitlines() if "Total Files Read" in ln]
    assert read, plan
    return max(int(ln.split("Total Files Read:")[1].split("\u2502")[0].strip()) for ln in read)


@pytest.mark.parametrize("document", ["AGENTS.md", "observations/README.md",
                                      "observations/AGENTS.md"])
def test_the_published_documentation_prunes(catalog, document):
    """Execute the snippet the catalog ships, not a copy of it.

    These files are what a consumer or an agent reads first. If one of them
    drifts back to `WHERE period = '<year>'` it will still return plausible
    rows for a recent year while scanning the entire archive, and for a
    historical year it will return nothing at all.
    """
    text = (pathlib.Path(catalog) / document).read_text()
    script = _sql_block(text).replace("<id>", str(DOC_STATION_ID))
    con = duckdb.connect()
    files = _explain_last_statement(con, script)

    # Guard against a vacuous pass: an empty join lets DuckDB skip the probe
    # side entirely, so files-read stays at 1 however badly the query prunes.
    statements = [s.strip() for s in script.split(";") if s.strip()]
    assert con.execute(statements[-1]).fetchall(), (
        f"{document} returned no rows against the fixture, so it measures nothing"
    )
    assert files == 1, f"{document}: a scan reads {files} files, expected 1"


def test_demo_sql_defines_the_same_helper():
    """`sql/demo.sql` is where the README sends people; it must not drift.

    Executing it here would need the spatial extension, which is a network
    install, so pin the definition instead. The behaviour of this definition is
    what every test above measures.
    """
    demo = (pathlib.Path(__file__).resolve().parent.parent / "sql" / "demo.sql").read_text()
    macro = "SELECT period FROM layout WHERE end_year >= lo AND start_year <= hi"
    assert macro in demo, "sql/demo.sql no longer defines periods_for() as tested"
    assert macro in _setup_sql("/tmp")
    assert "o.period = '20" not in demo, (
        "sql/demo.sql compares period to a literal year again; that silently "
        "returns nothing for any year inside a merged bucket"
    )
