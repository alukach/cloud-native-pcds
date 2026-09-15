"""The Portolan layer.

These run without pyarrow: the STAC builders take a schema-shaped object and a
store-shaped object, both of which are small enough to fake, and that is the
point. The rules being checked here are Portolan's, not Arrow's.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass

import pytest

from pcds import paths
from pcds.portolan import (
    DEVIATIONS,
    JSON_TYPE,
    PARQUET_MEDIA_TYPE,
    CatalogConfig,
    Publisher,
    build_metadata_collection,
    build_observations_collection,
    build_root_catalog,
    collection_agents,
    collection_readme,
    deviations_for,
    deviations_md,
    multihash_sha256,
    root_agents,
    root_readme,
    table_columns,
)

BASE = "https://data.source.coop/example/pcds"
S3 = "s3://us-west-2.opendata.source.coop/example/pcds"
NOW = dt.datetime(2026, 9, 15, 12, 0, 0)


# ----------------------------------------------------------------- fakes ----


@dataclass
class Field:
    name: str
    type: str


class Schema(list):
    @property
    def names(self):
        return [f.name for f in self]


class Column(list):
    def to_pylist(self):
        return list(self)


class Table:
    def __init__(self, columns: dict, schema: Schema):
        self._cols = columns
        self.schema = schema
        self.num_rows = len(next(iter(columns.values()))) if columns else 0

    def column(self, name):
        return Column(self._cols[name])


class FakeInfo:
    def __init__(self, size):
        self.size = size


class FakeFile:
    def __init__(self, data):
        self._data = data

    def readall(self):
        return self._data

    def read(self):
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeFs:
    def __init__(self, files):
        self.files = files

    def get_file_info(self, path):
        return FakeInfo(len(self.files.get(path, b"")))

    def open_input_file(self, path):
        return FakeFile(self.files[path])


class FakeStore:
    def __init__(self, files: dict[str, bytes]):
        self.root = "/data"
        self.uri = "/data"
        self.fs = FakeFs({f"/data/{k}": v for k, v in files.items()})
        self._files = files

    def join(self, *parts):
        return "/data/" + "/".join(p.strip("/") for p in parts)

    def exists(self, *parts):
        return "/".join(p.strip("/") for p in parts) in self._files

    def ls(self, prefix, recursive=False):
        return [k for k in self._files if k.startswith(prefix)]


def cfg(**kw):
    return CatalogConfig(
        base_url=kw.pop("base_url", BASE),
        s3_uri=kw.pop("s3_uri", S3),
        publisher=Publisher(name="Development Seed", url="https://developmentseed.org"),
        **kw,
    )


STATION_SCHEMA = Schema(
    [Field("station_id", "int32"), Field("network_name", "string"),
     Field("native_id", "string"), Field("lon", "double"), Field("lat", "double"),
     Field("min_obs_time", "timestamp[s]"), Field("max_obs_time", "timestamp[s]"),
     Field("geometry", "binary"), Field("bbox", "struct")]
)


def station_table():
    return Table(
        {
            "station_id": [1, 2, 3],
            "network_name": ["EC_raw", "EC_raw", "MoTIe"],
            "native_id": ["1145M29", "x", "y"],
            "lon": [-117.3, -123.1, None],
            "lat": [49.49, 49.28, None],
            "min_obs_time": [dt.datetime(1990, 1, 1), dt.datetime(2001, 5, 1), None],
            "max_obs_time": [dt.datetime(2026, 9, 10), dt.datetime(2020, 1, 1), None],
        },
        STATION_SCHEMA,
    )


def station_store():
    return FakeStore(
        {
            paths.metadata_file(paths.STATIONS): b"parquet-bytes",
            f"{paths.STATIONS}/{paths.THUMBNAIL}": b"png-bytes",
        }
    )


# ------------------------------------------------------------ deviations ----


def test_deviations_are_the_two_we_decided_on():
    assert {d.id for d in DEVIATIONS} == {"PORTO-FMT-018", "PORTO-FMT-034"}
    assert all(d.collection == paths.OBSERVATIONS for d in DEVIATIONS)


def test_every_deviation_explains_itself_and_offers_a_mitigation():
    for d in DEVIATIONS:
        assert d.severity in {"MUST", "SHOULD"}
        assert d.enforcement in {"validator", "process"}
        assert len(d.why) > 80, d.id
        assert len(d.mitigation) > 40, d.id
        assert d.requirement


def test_deviations_are_scoped_to_observations_only():
    assert deviations_for(paths.STATIONS) == []
    assert len(deviations_for(paths.OBSERVATIONS)) == 2


def test_deviations_md_cites_every_id_and_the_upstream_issue():
    doc = deviations_md(cfg())
    for d in DEVIATIONS:
        assert d.id in doc
        assert d.upstream in doc
    assert "requirements.yaml" in doc
    # the things that look like deviations but are not
    assert "PORTO-FMT-009" in doc
    assert "PORTO-FMT-022" in doc


# --------------------------------------------------------------- checksum ----


def test_multihash_is_sha256_with_the_right_prefix():
    got = multihash_sha256(b"hello")
    assert got.startswith("1220")  # 0x12 sha2-256, 0x20 32 bytes
    assert got[4:] == hashlib.sha256(b"hello").hexdigest()
    assert len(got) == 68


# ------------------------------------------------------------- collections ----


def test_metadata_collection_is_valid_portolan():
    col = build_metadata_collection(
        station_store(), cfg(), paths.STATIONS, station_table(), updated=NOW
    )
    assert col["type"] == "Collection"
    assert col["stac_version"] == "1.1.0"
    assert col["title"] and col["description"]
    assert col["license"] == "other"
    # host provider is last and contactable; producer is present
    roles = [p["roles"] for p in col["providers"]]
    assert "producer" in roles[0]
    assert col["providers"][-1]["roles"][0] == "host"
    assert col["providers"][-1].get("url") or col["providers"][-1].get("email")
    # a mirror records its sync time and points back at the source
    assert col["updated"].startswith("2026-09-15")
    rels = {l["rel"] for l in col["links"]}
    assert {"root", "parent", "describedby", "agents", "via", "license", "self"} <= rels
    assert all(l.get("type") for l in col["links"])


def test_spatial_extent_comes_from_the_data_and_ignores_null_coords():
    col = build_metadata_collection(
        station_store(), cfg(), paths.STATIONS, station_table(), updated=NOW
    )
    bbox = col["extent"]["spatial"]["bbox"][0]
    assert bbox == [-123.1, 49.28, -117.3, 49.49]
    interval = col["extent"]["temporal"]["interval"][0]
    assert interval[0].startswith("1990-01-01")
    assert interval[1].startswith("2026-09-10")


def test_spatial_collection_carries_data_and_thumbnail_assets():
    col = build_metadata_collection(
        station_store(), cfg(), paths.STATIONS, station_table(), updated=NOW
    )
    data = col["assets"]["data"]
    assert data["type"] == PARQUET_MEDIA_TYPE
    assert data["roles"] == ["data"]
    assert data["href"].startswith("https://")           # browsers cannot fetch s3://
    assert data["alternate"]["s3"]["href"].startswith("s3://")
    assert data["file:size"] == len(b"parquet-bytes")
    assert data["file:checksum"].startswith("1220")
    assert col["assets"]["thumbnail"]["roles"] == ["thumbnail"]


def test_tabular_collection_still_carries_an_informational_bbox():
    store = FakeStore({paths.metadata_file(paths.VARIABLES): b"v"})
    table = Table(
        {"variable_id": [1, 2], "name": ["air_temperature", "total_precipitation"]},
        Schema([Field("variable_id", "int16"), Field("name", "string")]),
    )
    col = build_metadata_collection(store, cfg(), paths.VARIABLES, table, updated=NOW)
    # PORTO-FMT-036: required, but informational for a tabular collection
    assert len(col["extent"]["spatial"]["bbox"][0]) == 4
    assert "thumbnail" not in col["assets"]
    assert col["table:row_count"] == 2


def test_observations_collection_carries_the_partition_extension():
    store = FakeStore({paths.MANIFEST_FILE: b"manifest"})
    schema = Schema(
        [Field("station_id", "int32"), Field("variable_id", "int16"),
         Field("obs_time", "timestamp[s]"), Field("value", "double")]
    )
    col = build_observations_collection(
        store,
        cfg(),
        summary={"rows": 1_000_000, "files": 49,
                 "obs_time_min": "1872-01-01T00:00:00", "obs_time_max": "2026-09-10T01:00:00"},
        periods=["1872-1903", "2025", "2026"],
        obs_schema=schema,
        updated=NOW,
    )
    assert col["partition:scheme"] == "hive"
    assert col["partition:strategy"] == "temporal"
    assert col["partition:keys"][0]["name"] == "period"
    assert col["partition:glob"].startswith("s3://")
    assert col["partition:glob"].endswith("/observations/period=*/*.parquet")
    assert col["partition:file_count"] == 49
    from pcds.portolan import PARTITION_SCHEMA
    assert PARTITION_SCHEMA in col["stac_extensions"]


def test_observations_declares_its_deviations_machine_readably():
    store = FakeStore({})
    schema = Schema([Field("station_id", "int32")])
    col = build_observations_collection(
        store, cfg(), summary=None, periods=[], obs_schema=schema, updated=NOW
    )
    ids = {d["requirement"] for d in col["portolan:deviations"]}
    assert ids == {"PORTO-FMT-018", "PORTO-FMT-034"}
    # the deviation we cannot mitigate: no single-file data asset
    assert "data" not in col["assets"]


def test_manifest_is_registered_as_metadata_not_data():
    store = FakeStore({paths.MANIFEST_FILE: b"manifest"})
    col = build_observations_collection(
        store, cfg(), summary=None, periods=[],
        obs_schema=Schema([Field("station_id", "int32")]), updated=NOW
    )
    assert col["assets"]["manifest"]["roles"] == ["metadata"]
    assert "data" not in col["assets"]


def test_table_columns_uses_descriptions_where_given():
    cols = table_columns(
        Schema([Field("station_id", "int32"), Field("mystery", "string")]),
        {"station_id": "PCDS station id."},
    )
    assert cols[0] == {"name": "station_id", "type": "int32",
                       "description": "PCDS station id."}
    assert "description" not in cols[1]


# ------------------------------------------------------------------- root ----


def test_root_catalog_links_every_child_with_a_title():
    cat = build_root_catalog(cfg(), [("stations", "Weather Stations"),
                                     ("observations", "Station Observations")])
    children = [l for l in cat["links"] if l["rel"] == "child"]
    assert len(children) == 2
    assert all(l["title"] and l["type"] == JSON_TYPE for l in children)
    self_link = next(l for l in cat["links"] if l["rel"] == "self")
    assert self_link["href"] == f"{BASE}/catalog.json"
    assert {"describedby", "agents", "root", "via"} <= {l["rel"] for l in cat["links"]}


def test_root_catalog_without_base_url_has_no_self_link():
    cat = build_root_catalog(cfg(base_url=""), [])
    assert not any(l["rel"] == "self" for l in cat["links"])


# ------------------------------------------------------------------- docs ----


def test_readmes_carry_the_four_required_things():
    doc = root_readme(cfg(), {"rows": 12, "bytes": 2 * 1024**3})
    for required in ("# ", "## Provenance", "## License", "Disclaimer"):
        assert required in doc


def test_observations_readme_documents_the_join_with_working_sql():
    store = FakeStore({})
    col = build_observations_collection(
        store, cfg(), summary=None, periods=[],
        obs_schema=Schema([Field("station_id", "int32")]), updated=NOW
    )
    doc = collection_readme(paths.OBSERVATIONS, col, cfg())
    # PORTO-FMT-038: join columns documented AND a working code example
    assert "station_id" in doc and "variable_id" in doc
    assert "read_parquet" in doc and "JOIN" in doc
    assert "hive_partitioning" in doc


def test_agents_docs_warn_about_the_traps():
    doc = root_agents(cfg())
    assert "not UTC" in doc or "naive local standard time" in doc
    assert "preliminary" in doc
    assert "period" in doc


def test_variables_agents_explains_network_scoped_ids():
    col = {"title": "Observed Variables"}
    doc = collection_agents(paths.VARIABLES, col, cfg())
    assert "scoped to a network" in doc
    assert "standard_name" in doc


def test_docs_are_non_empty_for_every_collection():
    for cid in [*paths.METADATA_COLLECTIONS, paths.OBSERVATIONS]:
        col = {"title": "T", "description": "D", "partition:glob": "g"}
        assert len(collection_agents(cid, col, cfg())) > 100
        assert len(collection_readme(cid, col, cfg())) > 200


def test_catalog_json_is_serializable():
    cat = build_root_catalog(cfg(), [("stations", "Weather Stations")])
    assert json.loads(json.dumps(cat))["id"] == "pcds"


# -------------------------------------------------------- structural sweep ----


def _all_collections():
    out = {}
    out[paths.STATIONS] = build_metadata_collection(
        station_store(), cfg(), paths.STATIONS, station_table(), updated=NOW
    )
    vstore = FakeStore({paths.metadata_file(paths.VARIABLES): b"v"})
    out[paths.VARIABLES] = build_metadata_collection(
        vstore,
        cfg(),
        paths.VARIABLES,
        Table({"variable_id": [1]}, Schema([Field("variable_id", "int16")])),
        updated=NOW,
    )
    out[paths.OBSERVATIONS] = build_observations_collection(
        FakeStore({paths.MANIFEST_FILE: b"m"}),
        cfg(),
        summary={"rows": 10, "files": 2},
        periods=["2025"],
        obs_schema=Schema([Field("station_id", "int32")]),
        updated=NOW,
    )
    return out


def test_every_link_declares_a_type():
    for cid, col in _all_collections().items():
        for link in col["links"]:
            assert link.get("type"), f"{cid} link {link['rel']} has no type"
            assert link.get("href")


def test_every_asset_has_href_type_and_a_role():
    # STAC leaves type and roles optional; Portolan makes both mandatory.
    for cid, col in _all_collections().items():
        for key, asset in col["assets"].items():
            assert asset.get("href"), f"{cid}.{key}"
            assert asset.get("type"), f"{cid}.{key}"
            assert asset.get("roles"), f"{cid}.{key}"
            assert asset.get("title"), f"{cid}.{key}"


def test_no_asset_href_is_an_s3_url():
    # Browsers cannot fetch s3://; it belongs on `alternate`, not `href`.
    for cid, col in _all_collections().items():
        for key, asset in col["assets"].items():
            assert not asset["href"].startswith("s3://"), f"{cid}.{key}"


def test_relative_hrefs_when_no_base_url_is_configured():
    store = station_store()
    col = build_metadata_collection(
        store, cfg(base_url="", s3_uri=""), paths.STATIONS, station_table(), updated=NOW
    )
    assert col["assets"]["data"]["href"] == "./stations.parquet"
    assert col["assets"]["thumbnail"]["href"] == "./thumbnail.png"
    assert "alternate" not in col["assets"]["data"]


def test_every_collection_is_json_serializable_and_titled():
    for cid, col in _all_collections().items():
        blob = json.loads(json.dumps(col))
        assert blob["id"] == cid
        assert blob["title"] and blob["description"]
        assert blob["license"]
        assert blob["extent"]["spatial"]["bbox"]
