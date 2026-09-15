"""Generated prose, and the register of known spec deviations.

Split out of `portolan.py`, which is otherwise about STAC structure. The two
concerns were roughly the same size in one file and only one of them is about
JSON shapes, so they read better apart. Nothing here decides catalog structure:
these functions return Markdown, and `portolan.build` writes it.

The deviation register lives here because it is documentation first. It is
rendered into `DEVIATIONS.md` and mirrored, machine-readably, into
`portolan:deviations` on each affected collection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from . import paths

if TYPE_CHECKING:  # avoids a cycle: portolan imports this module, not the reverse
    from .portolan import CatalogConfig

SOURCE_PORTAL = "https://services.pacificclimate.org/met-data-portal-pcds/app/"
SOURCE_DOCS = "https://services.pacificclimate.org/portal/docs/mdp/root.html"
LICENSE_URL = "https://www.pacificclimate.org/data/bc-station-data-disclaimer"


# --------------------------------------------------------------- deviations --


@dataclass(frozen=True)
class Deviation:
    id: str
    severity: str
    enforcement: str
    requirement: str
    collection: str
    why: str
    mitigation: str
    upstream: str = ""


DEVIATIONS: tuple[Deviation, ...] = (
    Deviation(
        id="PORTO-FMT-018",
        severity="MUST",
        enforcement="process",
        requirement=(
            "The scheme's path structure MUST reflect spatial extent so readers "
            "can prune files without reading metadata."
        ),
        collection=paths.OBSERVATIONS,
        why=(
            "The observation table is partitioned by time (`period=`), not by space. "
            "Adding a spatial key would multiply the file count by the number of "
            "spatial cells while the partitions are already at the spec's own "
            "200 MiB-1 GiB size target, and every realistic query against station "
            "observations is bounded by time first. Spatial pruning is served instead "
            "by the `stations` collection: filter it to the station_ids you want, then "
            "push those into the observation scan."
        ),
        mitigation=(
            "`_manifest/files.parquet` carries per-file station_id and obs_time ranges, "
            "so a reader can prune files from one small metadata read rather than from "
            "the path."
        ),
        upstream="https://github.com/portolan-sdi/portolan-spec/issues/196",
    ),
    Deviation(
        id="PORTO-FMT-034",
        severity="MUST",
        enforcement="validator",
        requirement=(
            "Data MUST be provided as a Parquet file (`application/vnd.apache.parquet`) "
            "exposed as a collection-level asset with role `[\"data\"]`, following the "
            "single-file collection pattern."
        ),
        collection=paths.OBSERVATIONS,
        why=(
            "Tabular collections are specified as single-file; partitioning is specified "
            "only under Vector. The observation table is roughly 10-15 GB across many "
            "files, so neither shape fits it as written. A single-file tabular collection "
            "would mean one multi-gigabyte Parquet object, which the spec's own file-size "
            "guidance argues against."
        ),
        mitigation=(
            "The collection carries the full partition extension "
            "(`partition:scheme`, `partition:keys`, `partition:glob`) so partition-aware "
            "readers have a normative bulk-access path, plus a `metadata`-role asset "
            "pointing at the file manifest."
        ),
        upstream="https://github.com/portolan-sdi/portolan-spec/issues/196",
    ),
)


def deviations_for(collection: str) -> list[Deviation]:
    return [d for d in DEVIATIONS if d.collection == collection]


# --------------------------------------------------------------------- docs --


def _provenance_block() -> str:
    return (
        "## Provenance\n\n"
        "Mirrored from the Pacific Climate Impacts Consortium's Meteorological Data "
        f"Portal ([portal]({SOURCE_PORTAL}), [docs]({SOURCE_DOCS})). PCIC maintains the "
        "Provincial Climate Data Set under British Columbia's Climate Related Monitoring "
        "Program; the observations originate with the individual member networks. This "
        "catalog is a format conversion and adds no data.\n\n"
        "## License\n\n"
        f"Subject to PCIC's [BC Station Data Disclaimer]({LICENSE_URL}). The data is "
        "provided AS IS, is preliminary and subject to change, and carries no warranty. "
        "Use of it is also subject to the originating agencies' restrictions and the "
        "Climate Related Monitoring Program agreement.\n"
    )


def root_readme(cfg: CatalogConfig, summary: dict | None) -> str:
    rows = f"{summary['rows']:,}" if summary and summary.get("rows") else "not yet built"
    size = (
        f"{summary['bytes'] / 1024**3:.1f} GiB" if summary and summary.get("bytes") else "n/a"
    )
    return f"""# {cfg.title}

{cfg.description}

| | |
|---|---|
| Observations | {rows} |
| Packed size | {size} |
| Stations | 6,977 |
| Networks | 22 |
| Variables | 358 |
| Coverage | 1872 to present, British Columbia |

## Collections

| Collection | What it is |
|---|---|
| [`stations`](stations/) | One GeoParquet point per station. The spatial index for everything else. |
| [`histories`](histories/) | One point per station configuration, with time zone offsets. |
| [`variables`](variables/) | Variable definitions: CF names, cell methods, units. |
| [`networks`](networks/) | The 22 contributing agencies. |
| [`observations`](observations/) | The observation table, partitioned by time. |

## Conformance

This catalog follows the [Portolan specification](https://github.com/portolan-sdi/portolan-spec)
and **knowingly deviates from two requirements**. Both are recorded with their
stable requirement IDs in [DEVIATIONS.md](DEVIATIONS.md), and both come down to
the same thing: Portolan has no shape yet for a large, time-partitioned,
non-spatial table. Everything else, including GeoParquet 1.1 with covering
columns on the spatial collections and the partition extension on the
observations, conforms.

{_provenance_block()}"""


def _example_root(cfg: CatalogConfig) -> str:
    """The URL to put in code examples.

    https first: DuckDB reads both, but a browser or an agent without bucket
    credentials only reads https. The s3 form stays where it belongs, on
    `partition:glob` and the alternate assets.
    """
    return cfg.base_url or cfg.s3_uri or "<catalog root>"


def root_agents(cfg: CatalogConfig) -> str:
    base = _example_root(cfg)
    return f"""# Agent guidance

## Shape of this catalog

Five collections. Four are small reference tables you can load whole; one is
large and partitioned. Never scan `observations` without a predicate.

```
{base}/
  stations/stations.parquet        6,977 rows   GeoParquet, point per station
  histories/histories.parquet      9,632 rows   GeoParquet, point per configuration
  variables/variables.parquet        358 rows   Parquet
  networks/networks.parquet           22 rows   Parquet
  observations/period=*/*.parquet              Parquet, partitioned by period
  _manifest/files.parquet                      per-file stats for pruning
```

## The join

`observations` is a long table of `(station_id, variable_id, obs_time, value)`.
It carries no names, no units and no coordinates. Every useful query joins it to
`stations` (for where) and `variables` (for what and in what unit).

```sql
INSTALL httpfs; LOAD httpfs;
SET VARIABLE root = '{base}';

-- `period` is an opaque label, not a year. Resolve years through layout.json.
CREATE OR REPLACE VIEW layout AS
  SELECT * FROM (SELECT unnest(periods, recursive := true)
                 FROM read_json_auto(getvariable('root') || '/layout.json'));
CREATE OR REPLACE MACRO periods_for(lo, hi) AS TABLE
  SELECT period FROM layout WHERE end_year >= lo AND start_year <= hi;

SELECT s.station_name, v.display_name, v.unit, o.obs_time, o.value
FROM read_parquet(getvariable('root') || '/observations/period=*/*.parquet',
                  hive_partitioning := true)                        o
JOIN read_parquet(getvariable('root') || '/stations/stations.parquet')   s USING (station_id)
JOIN read_parquet(getvariable('root') || '/variables/variables.parquet') v USING (variable_id)
WHERE s.native_id = '1145M29' AND s.network_name = 'EC_raw'
  AND v.name = 'air_temperature'
  AND o.period IN (SELECT period FROM periods_for(2025, 2025))
  AND o.obs_time >= '2025-01-01' AND o.obs_time < '2026-01-01'
ORDER BY o.obs_time;
```

## How to keep a query cheap

1. **Always constrain `period`, and never compare it to a year.** It is the Hive
   partition key and an opaque label: recent years stand alone (`period=2025`),
   sparse early decades are bucketed (`period=1872-1903`). `WHERE period = '1890'`
   returns zero rows without erroring, and `WHERE period >= '1900'` silently drops
   1900 to 1903, because as strings `'1872-1903'` sorts below `'1900'`. Resolve
   years through `layout.json`, as `periods_for()` above does.
   Filtering on `obs_time` alone is correct but reads every partition: DuckDB
   cannot relate a string partition key to a timestamp, and the sort order leaves
   `obs_time` interleaved, so row-group statistics do not help either. Pair the
   two.
2. **Resolve stations first.** Filter the small `stations` table by geometry,
   network or name, then push the resulting `station_id` list into the
   observation scan. Files are sorted by `station_id`, so this prunes row groups.
3. **Resolve variables first, too.** `variable_id` is network-scoped: air
   temperature has a different id in each network. Filter `variables` on
   `standard_name` and `cell_method`, not on a single id.
4. **Read `_manifest/files.parquet`** if you want to choose files explicitly
   rather than globbing. It has per-file `station_id` and `obs_time` ranges.

## Things that will bite you

- **Timestamps are not UTC.** `obs_time` is naive local standard time as PCIC
  publishes it. `histories.tz_offset` carries the offset where upstream knows it,
  which is not everywhere. Do not assume, and do not silently localize.
- **The data is preliminary.** PCIC revises observations and late data arrives for
  weeks. A value you read today may change. The mirror re-reads a trailing 30-day
  window on every sync and resolves duplicates last-write-wins.
- **`variable_id` is not a global concept.** See point 3 above.
- **Stations have histories.** A station that moved has several locations. The
  `stations` row carries the most recent; `histories` has all of them. Observations
  are keyed to the station, not the history, because the upstream API is.
- **Sparse reporting is normal.** A station listed as reporting 13 variables will
  have nulls for most of them at most timestamps; the long format simply omits
  those rows rather than storing nulls.

## Related

- Upstream portal: {SOURCE_PORTAL}
- Upstream API docs: {SOURCE_DOCS}
- Known spec deviations: `DEVIATIONS.md`
"""


def collection_readme(cid: str, col: dict, cfg: CatalogConfig) -> str:
    base = _example_root(cfg)
    dev = deviations_for(cid)
    body = f"# {col['title']}\n\n{col['description']}\n\n"
    if col.get("table:row_count"):
        body += f"**Rows:** {col['table:row_count']:,}\n\n"

    if cid == paths.OBSERVATIONS:
        body += f"""## Access

Partitioned Parquet. The normative bulk path is the `partition:glob` in
`collection.json`:

```
{col['partition:glob']}
```

## Joining to geometry

The observation table has no geometry. Locations live in the `stations`
collection and are joined on `station_id`; units live in `variables` and are
joined on `variable_id`.

```sql
-- Monthly mean temperature for stations in the Kootenays, 2024.
INSTALL httpfs; LOAD httpfs;
SET VARIABLE root = '{base}';

CREATE OR REPLACE VIEW layout AS
  SELECT * FROM (SELECT unnest(periods, recursive := true)
                 FROM read_json_auto(getvariable('root') || '/layout.json'));
CREATE OR REPLACE MACRO periods_for(lo, hi) AS TABLE
  SELECT period FROM layout WHERE end_year >= lo AND start_year <= hi;

WITH kootenay AS (
  SELECT station_id, station_name
  FROM read_parquet(getvariable('root') || '/stations/stations.parquet')
  WHERE lon BETWEEN -118.5 AND -116.0 AND lat BETWEEN 49.0 AND 50.5
), temp_vars AS (
  SELECT variable_id
  FROM read_parquet(getvariable('root') || '/variables/variables.parquet')
  WHERE standard_name = 'air_temperature' AND cell_method = 'time: point'
)
SELECT k.station_name,
       date_trunc('month', o.obs_time) AS month,
       round(avg(o.value), 2)          AS mean_c,
       count(*)                        AS n
FROM read_parquet(getvariable('root') || '/observations/period=*/*.parquet',
                  hive_partitioning := true) o
JOIN kootenay  k USING (station_id)
JOIN temp_vars   USING (variable_id)
WHERE o.period IN (SELECT period FROM periods_for(2024, 2024))
  AND o.obs_time >= '2024-01-01' AND o.obs_time < '2025-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;
```

`period` is an opaque partition label rather than a year, so it is resolved
through `layout.json` above. Comparing it to a year is silently wrong:
`period = '1890'` returns nothing, and `period >= '1900'` drops 1900 to 1903,
because as strings `'1872-1903'` sorts below `'1900'`.

## Layout

- Hive partition key `period`, mapped in `layout.json` at the catalog root. A
  period is a contiguous run of years, so it is a label and not a year.
- Sorted within each file by `(station_id, variable_id, obs_time)`. That leaves
  `obs_time` interleaved per station, so its row-group statistics prune almost
  nothing and the partition is effectively the time granularity.
- Row groups capped at 150,000 rows; page index and a `station_id` bloom filter
  written so a point lookup skips row groups rather than decoding them.
- zstd level 9.

"""
    elif cid in paths.SPATIAL_COLLECTIONS:
        body += f"""## Access

```sql
SELECT * FROM read_parquet('{base}/{paths.metadata_file(cid)}');
```

GeoParquet 1.1: WKB point geometry in `geometry`, a `bbox` covering column for
row-group pruning, rows Hilbert ordered. `lon` and `lat` are kept as plain
columns too, because most queries against a few thousand points want them
directly.

Small enough to render from source, so there is no PMTiles derivative and no
separate style file; the thumbnail is the default rendering.

"""
    else:
        body += f"""## Access

```sql
SELECT * FROM read_parquet('{base}/{paths.metadata_file(cid)}');
```

A reference table with no geometry. Load it whole.

"""
    if dev:
        body += "## Spec deviations\n\n"
        for d in dev:
            body += f"- **{d.id}** ({d.severity}): see [DEVIATIONS.md](../DEVIATIONS.md).\n"
        body += "\n"
    body += _provenance_block()
    return body


def collection_agents(cid: str, col: dict, cfg: CatalogConfig) -> str:
    base = _example_root(cfg)
    lines = [f"# Agent guidance: {col['title']}", ""]
    if cid == paths.OBSERVATIONS:
        lines += [
            "Large, partitioned, and meaningless on its own. Read the catalog root's",
            "`AGENTS.md` first: it has the join, the pruning rules, and the timestamp",
            "caveat.",
            "",
            "Minimum viable query shape:",
            "",
            "```sql",
            "-- `period` is a label, not a year: resolve it through layout.json.",
            "CREATE OR REPLACE VIEW layout AS",
            "  SELECT * FROM (SELECT unnest(periods, recursive := true)",
            f"                 FROM read_json_auto('{base}/layout.json'));",
            "CREATE OR REPLACE MACRO periods_for(lo, hi) AS TABLE",
            "  SELECT period FROM layout WHERE end_year >= lo AND start_year <= hi;",
            "",
            f"SELECT * FROM read_parquet('{base}/observations/period=*/*.parquet',",
            "                           hive_partitioning := true)",
            "WHERE period IN (SELECT period FROM periods_for(2025, 2025))",
            "  AND station_id = <id> AND variable_id = <id>;",
            "```",
            "",
            "Dropping the `period` predicate scans the whole archive. Dropping",
            "`station_id` scans every file in the period.",
            "",
            "Never compare `period` to a year. `period = '1890'` returns zero rows",
            "without erroring, and `period >= '1900'` silently drops 1900 to 1903,",
            "because as strings `'1872-1903'` sorts below `'1900'`. Filtering on",
            "`obs_time` alone is correct but prunes nothing.",
            "",
            "`obs_time` is naive local standard time, not UTC.",
        ]
    elif cid == paths.STATIONS:
        lines += [
            "The spatial entry point. Filter this table first, then push `station_id`",
            "into the observation scan.",
            "",
            "- `network_name` + `native_id` is how humans name a station upstream.",
            "- `station_id` is what `observations` joins on.",
            "- `variable_ids` tells you what a station reports before you query for it.",
            "- `min_obs_time` / `max_obs_time` bound what exists; a station whose",
            "  `max_obs_time` is years old is not reporting any more.",
            "- A station with `history_count > 1` has moved; `histories` has each location.",
        ]
    elif cid == paths.HISTORIES:
        lines += [
            "One row per station configuration. Use this rather than `stations` when",
            "the question is about a specific era of a station: where it was in 1974,",
            "or what its time zone offset is.",
            "",
            "`tz_offset` is null for many rows. Upstream does not always know it.",
            "",
            "Observations are keyed to `station_id`, not `history_id`, because the",
            "upstream per-station API does not break them out by history.",
        ]
    elif cid == paths.VARIABLES:
        lines += [
            "`variable_id` is scoped to a network. Air temperature has a different id",
            "in each of the 22 networks, so never hardcode one.",
            "",
            "Select variables by `standard_name` plus `cell_method`:",
            "",
            "```sql",
            f"SELECT variable_id FROM read_parquet('{base}/variables/variables.parquet')",
            "WHERE standard_name = 'air_temperature' AND cell_method = 'time: point';",
            "```",
            "",
            "`name` is the network's own label and is also the column name in the",
            "upstream OPeNDAP CSV. `short_name` is the CF-style name with the cell",
            "method folded in. They are different fields and mixing them up is the",
            "single easiest way to get an empty result.",
        ]
    else:
        lines += [
            "Twenty-two rows. `network_name` is the code that appears in station",
            "records and in upstream URLs; `long_name` is the agency.",
        ]
    lines += ["", f"Upstream: {SOURCE_PORTAL}", ""]
    return "\n".join(lines)


def deviations_md(cfg: CatalogConfig) -> str:
    out = [
        "# Known Portolan deviations",
        "",
        "This catalog declares the Portolan STAC profile and follows the specification",
        "except where listed below. Each entry cites the stable requirement ID from",
        "[`specs/portolan/requirements.yaml`](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/portolan/requirements.yaml).",
        "",
        "The deviations are published rather than papered over. Both stem from the same",
        "gap: Portolan currently specifies partitioning only for vector data and tabular",
        "data only as a single file, so a large time-partitioned non-spatial table has no",
        "conformant shape. This dataset is offered as a concrete case for that discussion.",
        "",
        "The same information is machine-readable in `portolan:deviations` on each",
        "affected `collection.json`.",
        "",
    ]
    for d in DEVIATIONS:
        out += [
            f"## {d.id} ({d.severity}, enforcement: {d.enforcement})",
            "",
            f"**Collection:** `{d.collection}`",
            "",
            f"> {d.requirement}",
            "",
            "**Why we deviate.** " + d.why,
            "",
            "**What we do instead.** " + d.mitigation,
            "",
        ]
        if d.upstream:
            out += [f"**Upstream discussion:** {d.upstream}", ""]
    out += [
        "## Not deviations",
        "",
        "Worth stating explicitly, because they look like they might be:",
        "",
        "- **Row groups.** PORTO-FMT-009 caps GeoParquet row groups at 150,000 rows.",
        "  It sits under Vector and so does not reach a non-spatial table, but the",
        "  observation files honour it anyway: at roughly 3 bytes per row that is about",
        "  450 KiB per row group, which is a sensible floor for a range request.",
        "- **Visualization.** Non-geospatial collections are exempt from the render-path",
        "  requirement. The two spatial collections are small enough to render from",
        "  source, which the spec allows without a separate style file, and both carry a",
        "  thumbnail.",
        "- **Items.** PORTO-FMT-022 says partition files SHOULD NOT be modelled as items",
        "  when there are hundreds of them. There are, and they are not.",
        "",
    ]
    return "\n".join(out)
