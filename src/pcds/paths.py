"""Dataset paths, in one place.

The layout is the Portolan catalog shape: collections are directories one level
below the root, each holding its own `collection.json`, `README.md`, `AGENTS.md`
and data. Everything that is pipeline machinery rather than published data lives
under an underscore-prefixed directory, following the convention the spec itself
uses for `_assets/`, so a validator walking `child` links never trips over it.

    {root}/
      catalog.json  README.md  AGENTS.md  DEVIATIONS.md  layout.json
      networks/       collection.json + networks.parquet          (tabular)
      variables/      collection.json + variables.parquet         (tabular)
      stations/       collection.json + stations.parquet          (GeoParquet)
      histories/      collection.json + histories.parquet         (GeoParquet)
      observations/   collection.json + period=<p>/part-*.parquet (partitioned)
      _assets/        logo, shared images
      _manifest/      files.parquet, summary.json  (per-file stats for pruning)
      _state/         watermarks.parquet
      _staging/       delta/, compact/  (never published, never linked)
"""

from __future__ import annotations

OBSERVATIONS = "observations"
STATIONS = "stations"
HISTORIES = "histories"
VARIABLES = "variables"
NETWORKS = "networks"

# Collection id -> the metadata table that fills it.
METADATA_COLLECTIONS = {
    NETWORKS: "networks",
    VARIABLES: "variables",
    STATIONS: "stations",
    HISTORIES: "histories",
}
# The two with a geometry column; the rest are plain tabular.
SPATIAL_COLLECTIONS = (STATIONS, HISTORIES)

MANIFEST = "_manifest"
STATE = "_state"
STAGING = "_staging"
ASSETS = "_assets"

DELTA = f"{STAGING}/delta"
COMPACT_STAGING = f"{STAGING}/compact"

LAYOUT_FILE = "layout.json"
MANIFEST_FILE = f"{MANIFEST}/files.parquet"
SUMMARY_FILE = f"{MANIFEST}/summary.json"
WATERMARKS_FILE = f"{STATE}/watermarks.parquet"

THUMBNAIL = "thumbnail.png"
README = "README.md"
AGENTS = "AGENTS.md"
DEVIATIONS = "DEVIATIONS.md"
COLLECTION_JSON = "collection.json"
CATALOG_JSON = "catalog.json"


def period_prefix(period: str) -> str:
    return f"{OBSERVATIONS}/period={period}"


def metadata_file(collection: str) -> str:
    return f"{collection}/{METADATA_COLLECTIONS[collection]}.parquet"


def observations_glob(base: str) -> str:
    return f"{base.rstrip('/')}/{OBSERVATIONS}/period=*/*.parquet"


def delta_glob(base: str) -> str:
    return f"{base.rstrip('/')}/{DELTA}/**/*.parquet"
