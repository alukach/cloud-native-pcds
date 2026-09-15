"""Emit a Portolan catalog over the published dataset.

This layer is deliberately *non-conformant* and says so. Two of Portolan's
requirements do not currently have a shape that fits a large, time-partitioned
observation table, and rather than distort the data to satisfy them, the catalog
records each deviation with its stable requirement ID in `DEVIATIONS.md` and in
`portolan:deviations` on the affected collection.

Everything else follows the spec: STAC 1.1.0, one `catalog.json` at the root and
a `collection.json` per collection, `README.md` and `AGENTS.md` everywhere,
providers with a contactable host, an SPDX-or-`other` license, `via` provenance
because this is a mirror rather than the source, GeoParquet 1.1 with a bbox
covering column and Hilbert ordering for the two spatial collections, and the
partition extension on the observations.

Spec: https://github.com/portolan-sdi/portolan-spec
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from . import paths

if TYPE_CHECKING:  # keeps this module importable (and testable) without pyarrow
    from .config import Settings
    from .storage import Store

# Pinned schema URIs. v0.2.0 is the newest Portolan profile published at
# schemas.portolan-sdi.org; the partition extension is still incubating.
PORTOLAN_SCHEMA = "https://schemas.portolan-sdi.org/portolan/v0.2.0/schema.json"
PARTITION_SCHEMA = "https://schemas.portolan-sdi.org/incubating/partition/v1.0.0/schema.json"
TABLE_SCHEMA = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
FILE_SCHEMA = "https://stac-extensions.github.io/file/v2.1.0/schema.json"
ALTERNATE_SCHEMA = "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json"

STAC_VERSION = "1.1.0"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
MARKDOWN = "text/markdown"
JSON_TYPE = "application/json"

SOURCE_PORTAL = "https://services.pacificclimate.org/met-data-portal-pcds/app/"
SOURCE_DOCS = "https://services.pacificclimate.org/portal/docs/mdp/root.html"
LICENSE_URL = "https://www.pacificclimate.org/data/bc-station-data-disclaimer"

# Rough BC envelope, used as the informational extent for tabular collections
# that have no geometry of their own.
BC_BBOX = [-139.06, 48.30, -114.03, 60.00]


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


# -------------------------------------------------------------- publisher ---


@dataclass
class Publisher:
    """Who hosts this copy. Producer is PCIC; anything else makes it a mirror."""

    name: str = "Development Seed"
    url: str = "https://developmentseed.org"
    email: str = ""
    roles: list[str] = field(default_factory=lambda: ["host", "processor"])

    def as_provider(self) -> dict:
        p = {"name": self.name, "roles": list(self.roles)}
        if self.url:
            p["url"] = self.url
        if self.email:
            p["email"] = self.email
        return p


PCIC_PROVIDER = {
    "name": "Pacific Climate Impacts Consortium",
    "description": (
        "PCIC maintains the Provincial Climate Data Set under British Columbia's "
        "Climate Related Monitoring Program. Observations originate with the "
        "individual member networks."
    ),
    "roles": ["producer", "licensor"],
    "url": "https://www.pacificclimate.org/",
}


@dataclass
class CatalogConfig:
    catalog_id: str = "pcds"
    title: str = "BC Provincial Climate Data Set"
    description: str = (
        "Weather and climate observations from 6,977 stations across British "
        "Columbia, 1872 to present, mirrored from PCIC's Meteorological Data "
        "Portal as cloud-optimized Parquet. Station and history locations are "
        "GeoParquet; the observation table is partitioned by time."
    )
    base_url: str = ""  # https base the catalog is served from
    s3_uri: str = ""  # optional s3:// equivalent, exposed via alternate assets
    publisher: Publisher = field(default_factory=Publisher)
    license_id: str = "other"


# ------------------------------------------------------------------ helpers --


def multihash_sha256(data: bytes) -> str:
    """STAC `file:checksum` wants multihash, not a bare digest.

    0x12 = sha2-256, 0x20 = 32-byte length, then the digest.
    """
    return "1220" + hashlib.sha256(data).hexdigest()


def _rfc3339(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.replace(microsecond=0).isoformat() + "Z"


def _link(rel: str, href: str, media_type: str | None = None, title: str | None = None) -> dict:
    out = {"rel": rel, "href": href}
    if media_type:
        out["type"] = media_type
    if title:
        out["title"] = title
    return out


def _doc_links() -> list[dict]:
    """README and AGENTS are links, not assets: they describe the data."""
    return [
        _link("describedby", f"./{paths.README}", MARKDOWN, "README"),
        _link("agents", f"./{paths.AGENTS}", MARKDOWN, "Agent guidance"),
    ]


def _asset(
    href: str,
    media_type: str,
    roles: list[str],
    title: str,
    *,
    description: str | None = None,
    size: int | None = None,
    checksum: str | None = None,
    s3_href: str | None = None,
) -> dict:
    asset = {"href": href, "type": media_type, "roles": roles, "title": title}
    if description:
        asset["description"] = description
    if size is not None:
        asset["file:size"] = size
    if checksum:
        asset["file:checksum"] = checksum
    if s3_href:
        # Browsers cannot fetch s3://, so the primary href stays https and the
        # bucket-native URL rides along on the alternate-assets extension.
        asset["alternate"] = {"s3": {"href": s3_href, "title": "S3 (direct bucket access)"}}
    return asset


def table_columns(schema, descriptions: dict[str, str]) -> list[dict]:
    """`table:columns` from an Arrow schema plus hand-written descriptions."""
    out = []
    for f in schema:
        col = {"name": f.name, "type": str(f.type)}
        if f.name in descriptions:
            col["description"] = descriptions[f.name]
        out.append(col)
    return out


OBS_COLUMN_DESCRIPTIONS = {
    "station_id": "PCDS station id. Join to stations.station_id.",
    "variable_id": "PCDS variable id. Join to variables.variable_id.",
    "obs_time": (
        "Observation timestamp as published: naive local standard time for the "
        "station, not UTC. See histories.tz_offset."
    ),
    "value": "Observed value in the unit given by variables.unit for this variable_id.",
}

STATION_COLUMN_DESCRIPTIONS = {
    "station_id": "PCDS station id. Primary key; join target for observations.",
    "network_name": "Operating network code, e.g. EC_raw, FLNRO-WMB.",
    "native_id": "Station id as assigned by its own network.",
    "min_obs_time": "Earliest observation available upstream for this station.",
    "max_obs_time": "Latest observation available upstream for this station.",
    "variable_ids": "Variable ids this station reports. Join to variables.variable_id.",
    "geometry": "WKB point, OGC:CRS84.",
    "bbox": "GeoParquet 1.1 covering column for row-group pruning.",
}

NETWORK_COLUMN_DESCRIPTIONS = {
    "network_id": "PCDS network id. Join to variables.network_id.",
    "network_name": "Network code as it appears in station records and upstream URLs.",
    "long_name": "Operating agency or monitoring program.",
    "station_count": "Stations upstream attributes to this network.",
}

VARIABLE_COLUMN_DESCRIPTIONS = {
    "variable_id": "PCDS variable id. Join target for observations.variable_id.",
    "name": (
        "Network-scoped variable name. This is the column label used by the "
        "upstream OPeNDAP CSV, and the key the ingest joins on."
    ),
    "short_name": "CF-style name with cell method, e.g. air_temperature_maximum.",
    "standard_name": "CF standard name.",
    "cell_method": "CF cell method, e.g. 'time: mean'.",
    "unit": "Unit of the observed value.",
}


# ----------------------------------------------------------------- building --


def _extent(bbox: list[float], start: dt.datetime | None, end: dt.datetime | None) -> dict:
    return {
        "spatial": {"bbox": [bbox]},
        "temporal": {"interval": [[_rfc3339(start), _rfc3339(end)]]},
    }


def _base_collection(
    cid: str,
    title: str,
    description: str,
    extent: dict,
    cfg: CatalogConfig,
    *,
    extensions: list[str],
    updated: dt.datetime,
) -> dict:
    providers = [PCIC_PROVIDER, cfg.publisher.as_provider()]
    col = {
        "type": "Collection",
        "stac_version": STAC_VERSION,
        "stac_extensions": extensions,
        "id": cid,
        "title": title,
        "description": description,
        "license": cfg.license_id,
        "extent": extent,
        "providers": providers,
        # A mirror records when it last synced from its source.
        "updated": _rfc3339(updated),
        "links": [
            _link("root", "../catalog.json", JSON_TYPE, cfg.title),
            _link("parent", "../catalog.json", JSON_TYPE, cfg.title),
            *_doc_links(),
            # Producer and host differ, so this catalog is a mirror and MUST
            # point back at the authoritative source. PCIC publishes no STAC, so
            # there is no `canonical` link to give.
            _link("via", SOURCE_PORTAL, "text/html", "PCIC Meteorological Data Portal"),
            _link("license", LICENSE_URL, "text/html", "BC Station Data Disclaimer"),
        ],
        "assets": {},
    }
    if cfg.base_url:
        col["links"].insert(
            0, _link("self", f"{cfg.base_url.rstrip('/')}/{cid}/{paths.COLLECTION_JSON}", JSON_TYPE)
        )
    dev = deviations_for(cid)
    if dev:
        col["portolan:deviations"] = [
            {
                "requirement": d.id,
                "severity": d.severity,
                "reason": d.why,
                "mitigation": d.mitigation,
                "upstream_issue": d.upstream,
            }
            for d in dev
        ]
    return col


def _file_facts(store: "Store", path: str, *, hash_bytes: bool) -> tuple[int | None, str | None]:
    """file:size and file:checksum, which are SHOULDs worth honouring for small
    assets. Big partition files are skipped: a stale checksum is worse than none,
    and hashing 10 GB on every catalog build buys nothing."""
    info = store.fs.get_file_info(path)
    size = info.size if info.size is not None else None
    if not hash_bytes:
        return size, None
    with store.fs.open_input_file(path) as f:
        return size, multihash_sha256(f.readall())


def _href(cfg: CatalogConfig, *parts: str, relative: str) -> str:
    """Absolute https where we know the base, otherwise the caller's relative form.

    Never s3: a browser cannot fetch it. The bucket-native URL rides along on the
    alternate-assets extension instead.
    """
    if not cfg.base_url:
        return relative
    return f"{cfg.base_url.rstrip('/')}/" + "/".join(p.strip("/") for p in parts)


def _s3(cfg: CatalogConfig, *parts: str) -> str | None:
    if not cfg.s3_uri:
        return None
    return f"{cfg.s3_uri.rstrip('/')}/" + "/".join(p.strip("/") for p in parts)


def build_metadata_collection(
    store: "Store",
    cfg: CatalogConfig,
    collection: str,
    table,
    *,
    updated: dt.datetime,
) -> dict:
    """networks / variables / stations / histories."""
    spatial = collection in paths.SPATIAL_COLLECTIONS
    filename = f"{paths.METADATA_COLLECTIONS[collection]}.parquet"
    rel = paths.metadata_file(collection)

    if spatial:
        lons = [x for x in table.column("lon").to_pylist() if x is not None]
        lats = [y for y in table.column("lat").to_pylist() if y is not None]
        bbox = (
            [min(lons), min(lats), max(lons), max(lats)] if lons else list(BC_BBOX)
        )
        descriptions = STATION_COLUMN_DESCRIPTIONS
    elif collection == paths.NETWORKS:
        bbox = list(BC_BBOX)
        descriptions = NETWORK_COLUMN_DESCRIPTIONS
    else:
        bbox = list(BC_BBOX)
        descriptions = VARIABLE_COLUMN_DESCRIPTIONS

    def col_range(name):
        if name not in table.schema.names:
            return None
        vals = [v for v in table.column(name).to_pylist() if v is not None]
        return (min(vals), max(vals)) if vals else None

    lo = col_range("min_obs_time")
    hi = col_range("max_obs_time")
    start = lo[0] if lo else None
    end = hi[1] if hi else None

    titles = {
        paths.NETWORKS: "Observing Networks",
        paths.VARIABLES: "Observed Variables",
        paths.STATIONS: "Weather Stations",
        paths.HISTORIES: "Station Histories",
    }
    descs = {
        paths.NETWORKS: (
            "The 22 agencies and monitoring programs contributing stations to the "
            "Provincial Climate Data Set, with their PCDS network ids and station counts."
        ),
        paths.VARIABLES: (
            "The 358 variable definitions across all networks, with CF standard names, "
            "cell methods and units. `name` is the join key used by the upstream "
            "OPeNDAP responses and by the observation table's variable_id."
        ),
        paths.STATIONS: (
            "One point per station: location, elevation, operating network, reporting "
            "frequency, the variables it reports, and the time range of its "
            "observations. This is the spatial index for the observation table."
        ),
        paths.HISTORIES: (
            "One point per station configuration. A station gets a new history when it "
            "moves or its instrumentation changes, so a station with several histories "
            "has several locations. Carries tz_offset where upstream knows it."
        ),
    }

    extensions = [PORTOLAN_SCHEMA, TABLE_SCHEMA, FILE_SCHEMA]
    if cfg.s3_uri:
        extensions.append(ALTERNATE_SCHEMA)
    col = _base_collection(
        collection,
        titles[collection],
        descs[collection],
        _extent(bbox, start, end),
        cfg,
        extensions=extensions,
        updated=updated,
    )
    col["table:columns"] = table_columns(table.schema, descriptions)
    col["table:row_count"] = table.num_rows
    if spatial:
        col["table:primary_geometry"] = "geometry"

    size, checksum = _file_facts(store, store.join(rel), hash_bytes=True)
    col["assets"]["data"] = _asset(
        _href(cfg, rel, relative=f"./{filename}"),
        PARQUET_MEDIA_TYPE,
        ["data"],
        titles[collection],
        description=("GeoParquet 1.1, Hilbert ordered, bbox covering column."
                     if spatial else "Parquet."),
        size=size,
        checksum=checksum,
        s3_href=_s3(cfg, rel),
    )
    if collection == paths.VARIABLES and store.exists(collection, "frequencies.json"):
        f_size, f_sum = _file_facts(
            store, store.join(collection, "frequencies.json"), hash_bytes=True
        )
        col["assets"]["frequencies"] = _asset(
            _href(cfg, collection, "frequencies.json", relative="./frequencies.json"),
            "application/json",
            ["metadata"],
            "Reporting frequency vocabulary",
            description="The controlled list of values taken by stations.freq.",
            size=f_size,
            checksum=f_sum,
        )
    if spatial and store.exists(collection, paths.THUMBNAIL):
        t_size, t_sum = _file_facts(
            store, store.join(collection, paths.THUMBNAIL), hash_bytes=True
        )
        col["assets"]["thumbnail"] = _asset(
            _href(cfg, collection, paths.THUMBNAIL, relative=f"./{paths.THUMBNAIL}"),
            "image/png",
            ["thumbnail"],
            "Station locations",
            description="Point scatter over the collection's own extent.",
            size=t_size,
            checksum=t_sum,
        )
    return col


def build_observations_collection(
    store: "Store",
    cfg: CatalogConfig,
    *,
    summary: dict | None,
    periods: list[str],
    obs_schema,
    updated: dt.datetime,
) -> dict:
    start = summary.get("obs_time_min") if summary else None
    end = summary.get("obs_time_max") if summary else None
    if isinstance(start, str):
        start = dt.datetime.fromisoformat(start)
    if isinstance(end, str):
        end = dt.datetime.fromisoformat(end)

    extensions = [PORTOLAN_SCHEMA, TABLE_SCHEMA, PARTITION_SCHEMA, FILE_SCHEMA]
    col = _base_collection(
        paths.OBSERVATIONS,
        "Station Observations",
        (
            "Every observation in the Provincial Climate Data Set as a long table: one "
            "row per (station, variable, time, value). Partitioned by `period`, a "
            "contiguous run of years chosen so each partition clears a size floor, and "
            "sorted within each file by (station_id, variable_id, obs_time). Join to "
            "the `stations` collection for location and to `variables` for units. "
            "Timestamps are naive local standard time as published upstream."
        ),
        _extent(list(BC_BBOX), start, end),
        cfg,
        extensions=extensions,
        updated=updated,
    )
    col["table:columns"] = table_columns(obs_schema, OBS_COLUMN_DESCRIPTIONS)
    if summary and summary.get("rows"):
        col["table:row_count"] = summary["rows"]

    glob_base = cfg.s3_uri or cfg.base_url or "."
    col["partition:scheme"] = "hive"
    col["partition:strategy"] = "temporal"
    col["partition:keys"] = [
        {
            "name": "period",
            "type": "string",
            "description": (
                "A contiguous run of years: a single year where that year carries "
                "enough data to fill a partition, a multi-year span where it does not. "
                "The full map is in layout.json at the catalog root."
            ),
        }
    ]
    col["partition:glob"] = paths.observations_glob(glob_base)
    if summary and summary.get("files"):
        col["partition:file_count"] = summary["files"]

    # Not a `data` asset: see PORTO-FMT-034 in DEVIATIONS.md. The manifest is
    # genuinely useful on its own, so it is registered as metadata.
    if store.exists(paths.MANIFEST_FILE):
        size, checksum = _file_facts(store, store.join(paths.MANIFEST_FILE), hash_bytes=True)
        col["assets"]["manifest"] = _asset(
            _href(cfg, paths.MANIFEST_FILE, relative=f"../{paths.MANIFEST_FILE}"),
            PARQUET_MEDIA_TYPE,
            ["metadata"],
            "Partition file manifest",
            description=(
                "One row per partition file with its row count, byte size, and "
                "station_id and obs_time ranges. Read this to choose files without "
                "listing the bucket."
            ),
            size=size,
            checksum=checksum,
            s3_href=_s3(cfg, paths.MANIFEST_FILE),
        )
    col["portolan:periods"] = periods
    return col


def build_root_catalog(cfg: CatalogConfig, collections: list[tuple[str, str]]) -> dict:
    cat = {
        "type": "Catalog",
        "stac_version": STAC_VERSION,
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": cfg.catalog_id,
        "title": cfg.title,
        "description": cfg.description,
        "links": [
            *_doc_links(),
            _link("via", SOURCE_PORTAL, "text/html", "PCIC Meteorological Data Portal"),
        ],
    }
    if cfg.base_url:
        # A catalog served from one fixed URL SHOULD carry an absolute self link;
        # it is also the base a validator uses to resolve absolute child links.
        cat["links"].insert(0, _link("self", f"{cfg.base_url.rstrip('/')}/{paths.CATALOG_JSON}",
                                     JSON_TYPE, cfg.title))
    cat["links"].append(_link("root", f"./{paths.CATALOG_JSON}", JSON_TYPE, cfg.title))
    for cid, title in collections:
        cat["links"].append(
            _link("child", f"./{cid}/{paths.COLLECTION_JSON}", JSON_TYPE, title)
        )
    return cat


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

SELECT s.station_name, v.display_name, v.unit, o.obs_time, o.value
FROM read_parquet(getvariable('root') || '/observations/period=*/*.parquet',
                  hive_partitioning := true)                        o
JOIN read_parquet(getvariable('root') || '/stations/stations.parquet')   s USING (station_id)
JOIN read_parquet(getvariable('root') || '/variables/variables.parquet') v USING (variable_id)
WHERE s.native_id = '1145M29' AND s.network_name = 'EC_raw'
  AND v.name = 'air_temperature'
  AND o.period = '2025'
ORDER BY o.obs_time;
```

## How to keep a query cheap

1. **Always constrain `period`.** It is the Hive partition key. `layout.json` at
   the catalog root maps years to periods: recent years stand alone (`period=2025`),
   sparse early decades are bucketed (`period=1872-1903`).
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
WHERE o.period = '2024'
GROUP BY 1, 2
ORDER BY 1, 2;
```

## Layout

- Hive partition key `period`, mapped in `layout.json` at the catalog root.
- Sorted within each file by `(station_id, variable_id, obs_time)`.
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
            f"SELECT * FROM read_parquet('{base}/observations/period=*/*.parquet',",
            "                           hive_partitioning := true)",
            "WHERE period = '2025' AND station_id = <id> AND variable_id = <id>;",
            "```",
            "",
            "Dropping the `period` predicate scans the whole archive. Dropping",
            "`station_id` scans every file in the period.",
            "",
            "`obs_time` is naive local standard time, not UTC.",
        ]
    elif cid == paths.STATIONS:
        lines += [
            "The spatial entry point. Filter this table first, then push `station_id`",
            "into the observation scan.",
            "",
            f"- `network_name` + `native_id` is how humans name a station upstream.",
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


# ------------------------------------------------------------------- write --


def _write_text(store: "Store", path: str, text: str) -> None:
    with store.fs.open_output_stream(store.join(path)) as sink:
        sink.write(text.encode())


def _write_json(store: "Store", path: str, obj: dict) -> None:
    _write_text(store, path, json.dumps(obj, indent=2) + "\n")


def build(store: "Store", settings: "Settings", cfg: CatalogConfig) -> dict:
    """Write the whole Portolan tree over an existing dataset. Idempotent."""
    import json as _json

    from . import catalog as manifest
    from . import metadata as md
    from .partitioning import Layout
    from .schema import OBSERVATIONS as OBS_SCHEMA

    updated = dt.datetime.now(dt.UTC).replace(tzinfo=None, microsecond=0)

    summary = None
    if store.exists(paths.SUMMARY_FILE):
        with store.fs.open_input_file(store.join(paths.SUMMARY_FILE)) as f:
            summary = _json.loads(f.read())
    elif store.exists(paths.MANIFEST_FILE):
        summary = manifest.summarize(manifest.load(store))

    periods: list[str] = []
    if store.exists(paths.LAYOUT_FILE):
        with store.fs.open_input_file(store.join(paths.LAYOUT_FILE)) as f:
            periods = [p.period for p in Layout.from_json(f.read()).periods]

    written: list[str] = []
    children: list[tuple[str, str]] = []

    for cid in paths.METADATA_COLLECTIONS:
        if not store.exists(paths.metadata_file(cid)):
            continue
        table = md.load(store, paths.METADATA_COLLECTIONS[cid])
        col = build_metadata_collection(store, cfg, cid, table, updated=updated)
        _write_json(store, f"{cid}/{paths.COLLECTION_JSON}", col)
        _write_text(store, f"{cid}/{paths.README}", collection_readme(cid, col, cfg))
        _write_text(store, f"{cid}/{paths.AGENTS}", collection_agents(cid, col, cfg))
        children.append((cid, col["title"]))
        written += [f"{cid}/{paths.COLLECTION_JSON}", f"{cid}/{paths.README}",
                    f"{cid}/{paths.AGENTS}"]

    if store.ls(paths.OBSERVATIONS, recursive=True):
        col = build_observations_collection(
            store, cfg, summary=summary, periods=periods, obs_schema=OBS_SCHEMA, updated=updated
        )
        cid = paths.OBSERVATIONS
        _write_json(store, f"{cid}/{paths.COLLECTION_JSON}", col)
        _write_text(store, f"{cid}/{paths.README}", collection_readme(cid, col, cfg))
        _write_text(store, f"{cid}/{paths.AGENTS}", collection_agents(cid, col, cfg))
        children.append((cid, col["title"]))
        written += [f"{cid}/{paths.COLLECTION_JSON}", f"{cid}/{paths.README}",
                    f"{cid}/{paths.AGENTS}"]

    root = build_root_catalog(cfg, children)
    root["updated"] = _rfc3339(updated)
    _write_json(store, paths.CATALOG_JSON, root)
    _write_text(store, paths.README, root_readme(cfg, summary))
    _write_text(store, paths.AGENTS, root_agents(cfg))
    _write_text(store, paths.DEVIATIONS, deviations_md(cfg))
    written += [paths.CATALOG_JSON, paths.README, paths.AGENTS, paths.DEVIATIONS]

    return {
        "collections": [c for c, _ in children],
        "files_written": len(written),
        "deviations": [d.id for d in DEVIATIONS],
        "updated": _rfc3339(updated),
        "base_url": cfg.base_url or None,
    }
