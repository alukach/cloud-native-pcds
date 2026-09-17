"""Dataset paths, in one place.

The layout is the Portolan catalog shape: collections are directories one level
below the root, each holding its own `collection.json`, `README.md`, `AGENTS.md`
and data. Everything that is pipeline machinery rather than published data lives
under an underscore-prefixed directory, following the convention the spec itself
uses for `_assets/`, so a validator walking `child` links never trips over it.

The tree is drawn in README.md, under "Dataset layout". It is not repeated here:
the two copies drifted apart while both claimed to be current.
"""

from __future__ import annotations

OBSERVATIONS = "observations"
OBSERVATIONS_MONTHLY = "observations_monthly"
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

DELTA = f"{STAGING}/delta"
COMPACT_STAGING = f"{STAGING}/compact"

LAYOUT_FILE = "layout.json"
MONTHLY_FILE = f"{OBSERVATIONS_MONTHLY}/{OBSERVATIONS_MONTHLY}.parquet"
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
