"""Fetch and normalize the PCDS station catalog.

Five JSON endpoints under ``{mdp_base}/api/metadata``:
  networks, variables, frequencies, stations, histories

`stations` embeds its histories; `histories` is the flat form with the extra
fields (tz_offset, sdate/edate, country). We pull both: stations for the
one-row-per-station view, histories for the full configuration record.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings
from . import paths
from .schema import HISTORIES, NETWORKS, STATIONS, VARIABLES
from .storage import Store


def _get(client: httpx.Client, base: str, path: str, params: dict[str, Any]) -> Any:
    r = client.get(f"{base}/{path}", params=params, timeout=300.0)
    r.raise_for_status()
    return r.json()


def _ts(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def _uri_id(uri: str | None) -> int | None:
    if not uri:
        return None
    return int(uri.rstrip("/").rsplit("/", 1)[-1])


def fetch_all(settings: Settings) -> dict[str, Any]:
    params = {"provinces": settings.provinces} if settings.provinces else {}
    with httpx.Client(headers=settings.extra_headers, follow_redirects=True) as client:
        return {
            "networks": _get(client, settings.metadata_base, "networks", params),
            "variables": _get(client, settings.metadata_base, "variables", params),
            "frequencies": _get(client, settings.metadata_base, "frequencies", params),
            "stations": _get(client, settings.metadata_base, "stations", params),
            "histories": _get(client, settings.metadata_base, "histories", params),
        }


def build_tables(raw: dict[str, Any]) -> dict[str, pa.Table]:
    networks = pa.Table.from_pylist(
        [
            {
                "network_id": n["id"],
                "network_name": n["name"],
                "long_name": n.get("long_name"),
                "color": n.get("color"),
                "publish": n.get("publish"),
                "station_count": n.get("station_count"),
            }
            for n in raw["networks"]
        ],
        schema=NETWORKS,
    )
    net_name = {n["id"]: n["name"] for n in raw["networks"]}

    variables = pa.Table.from_pylist(
        [
            {
                "variable_id": v["id"],
                "network_id": _uri_id(v.get("network_uri")),
                "name": v["name"],
                "display_name": v.get("display_name"),
                "short_name": v.get("short_name"),
                "standard_name": v.get("standard_name"),
                "cell_method": v.get("cell_method"),
                "unit": v.get("unit"),
                "precision": v.get("precision"),
                "tags": v.get("tags") or [],
            }
            for v in raw["variables"]
        ],
        schema=VARIABLES,
    )

    station_rows = []
    for s in raw["stations"]:
        hs = s.get("histories") or []
        nid = _uri_id(s.get("network_uri"))
        mins = [_ts(h.get("min_obs_time")) for h in hs if h.get("min_obs_time")]
        maxs = [_ts(h.get("max_obs_time")) for h in hs if h.get("max_obs_time")]
        # Prefer the most recent history for the representative location.
        primary = max(hs, key=lambda h: (h.get("max_obs_time") or ""), default={})
        vids = sorted({v for h in hs for v in (h.get("variable_ids") or [])})
        station_rows.append(
            {
                "station_id": s["id"],
                "network_id": nid,
                "network_name": net_name.get(nid),
                "native_id": str(s["native_id"]),
                "station_name": primary.get("station_name"),
                "lon": primary.get("lon"),
                "lat": primary.get("lat"),
                "elevation": primary.get("elevation"),
                "province": primary.get("province"),
                "freq": primary.get("freq"),
                "min_obs_time": min(mins) if mins else None,
                "max_obs_time": max(maxs) if maxs else None,
                "history_count": len(hs),
                "variable_ids": vids,
            }
        )
    stations = pa.Table.from_pylist(station_rows, schema=STATIONS)

    # histories endpoint is flat and has no station back-reference, so key it
    # through the embedded histories in `stations`.
    hist_station: dict[int, dict[str, Any]] = {}
    for s in raw["stations"]:
        for h in s.get("histories") or []:
            hist_station[h["id"]] = s
    history_rows = []
    for h in raw["histories"]:
        s = hist_station.get(h["id"])
        if s is None:
            continue  # history outside the province filter applied to stations
        nid = _uri_id(s.get("network_uri"))
        history_rows.append(
            {
                "history_id": h["id"],
                "station_id": s["id"],
                "network_name": net_name.get(nid),
                "native_id": str(s["native_id"]),
                "station_name": h.get("station_name"),
                "lon": h.get("lon"),
                "lat": h.get("lat"),
                "elevation": h.get("elevation"),
                "province": h.get("province"),
                "country": h.get("country"),
                "freq": h.get("freq"),
                "tz_offset": None if h.get("tz_offset") is None else str(h["tz_offset"]),
                "sdate": _ts(h.get("sdate")),
                "edate": _ts(h.get("edate")),
                "min_obs_time": _ts(h.get("min_obs_time")),
                "max_obs_time": _ts(h.get("max_obs_time")),
                "variable_ids": h.get("variable_ids") or [],
            }
        )
    histories = pa.Table.from_pylist(history_rows, schema=HISTORIES)
    return {
        "networks": networks,
        "variables": variables,
        "stations": stations,
        "histories": histories,
    }


def write(store: Store, tables: dict[str, pa.Table], frequencies: list[str]) -> None:
    """One collection directory per metadata table.

    The two tables that carry coordinates are written as GeoParquet (Hilbert
    sorted, bbox covering column) with a thumbnail beside them; the other two are
    plain Parquet, which is what Portolan calls a tabular collection.
    """
    from .geoparquet import to_geoparquet, write_geoparquet
    from .thumbnail import points_png

    for collection, name in paths.METADATA_COLLECTIONS.items():
        table = tables[name]
        store.mkdirs(collection)
        target = store.join(paths.metadata_file(collection))
        if collection in paths.SPATIAL_COLLECTIONS:
            geo, _bbox = to_geoparquet(table)
            write_geoparquet(store, target, geo)
            coords = list(zip(table.column("lon").to_pylist(), table.column("lat").to_pylist()))
            with store.fs.open_output_stream(
                store.join(collection, paths.THUMBNAIL)
            ) as sink:
                sink.write(points_png(coords))
        else:
            with store.fs.open_output_stream(target) as sink:
                pq.write_table(table, sink, compression="zstd", compression_level=9)
    with store.fs.open_output_stream(store.join(paths.VARIABLES, "frequencies.json")) as sink:
        sink.write(json.dumps(sorted(frequencies)).encode())


def load(store: Store, name: str) -> pa.Table:
    collection = next(c for c, n in paths.METADATA_COLLECTIONS.items() if n == name)
    with store.fs.open_input_file(store.join(paths.metadata_file(collection))) as f:
        table = pq.read_table(f)
    drop = [c for c in ("geometry", "bbox") if c in table.schema.names]
    return table.drop(drop) if drop else table


def variable_lookup(variables: pa.Table) -> dict[int, dict[str, int]]:
    """network_id -> {lister column name: variable_id}."""
    out: dict[int, dict[str, int]] = {}
    for row in variables.select(["network_id", "name", "variable_id"]).to_pylist():
        out.setdefault(row["network_id"], {})[row["name"]] = row["variable_id"]
    return out
