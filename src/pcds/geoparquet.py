"""Minimal GeoParquet 1.1 writer for point tables.

The station and history tables are the only genuinely spatial part of this
dataset: a few thousand points with an id, a name and a time range. That does not
justify a geopandas dependency, and writing it by hand keeps the three things
Portolan actually checks visible and testable:

  PORTO-FMT-004  GeoParquet 1.1 or 2.0
  PORTO-FMT-006  rows spatially ordered so nearby features are nearby in the file
  PORTO-FMT-007  per-row-group spatial statistics (a 1.1 `bbox` covering column)

A WKB point is 21 bytes of struct.pack, the covering column is a struct of four
doubles, and "spatially ordered" is a Hilbert sort on quantized lon/lat.
"""

from __future__ import annotations

import json
import struct
from collections.abc import Iterable

GEOPARQUET_VERSION = "1.1.0"
GEOMETRY_COLUMN = "geometry"
BBOX_COLUMN = "bbox"

_WKB_POINT = struct.Struct("<BIdd")


def wkb_point(lon: float, lat: float) -> bytes:
    """Little-endian WKB Point. 1 byte order + 4 byte type + 2 doubles."""
    return _WKB_POINT.pack(1, 1, lon, lat)


# ---------------------------------------------------------------- Hilbert --


def hilbert_index(x: int, y: int, order: int = 16) -> int:
    """Hilbert curve index of a cell on a 2**order square grid.

    Standard xy->d conversion. A Hilbert sort keeps nearby features nearby in
    the file far better than a naive lon-then-lat sort, which is what makes
    per-row-group bounding boxes tight enough to be worth reading.
    """
    rx = ry = 0
    d = 0
    s = 1 << (order - 1)
    while s > 0:
        rx = 1 if (x & s) > 0 else 0
        ry = 1 if (y & s) > 0 else 0
        d += s * s * ((3 * rx) ^ ry)
        # rotate
        if ry == 0:
            if rx == 1:
                x = s - 1 - x
                y = s - 1 - y
            x, y = y, x
        s >>= 1
    return d


def hilbert_key(lon: float, lat: float, order: int = 16) -> int:
    """Hilbert index for a WGS84 coordinate, quantized to a 2**order grid."""
    side = (1 << order) - 1
    x = min(side, max(0, int((lon + 180.0) / 360.0 * side)))
    y = min(side, max(0, int((lat + 90.0) / 180.0 * side)))
    return hilbert_index(x, y, order)


def hilbert_order(coords: Iterable[tuple[float | None, float | None]]) -> list[int]:
    """Row indices sorted by Hilbert key. Rows with no coordinate sort last,
    keeping them out of the spatial runs rather than smeared through them."""
    keyed: list[tuple[int, int, int]] = []
    for i, (lon, lat) in enumerate(coords):
        if lon is None or lat is None:
            keyed.append((1, 0, i))
        else:
            keyed.append((0, hilbert_key(lon, lat), i))
    keyed.sort()
    return [i for _, _, i in keyed]


# ---------------------------------------------------------------- writing --


def geo_metadata(
    bbox: tuple[float, float, float, float],
    *,
    geometry_types: list[str] | None = None,
    primary_column: str = GEOMETRY_COLUMN,
) -> bytes:
    """The `geo` file-metadata value.

    `crs` is omitted deliberately: in GeoParquet 1.1 an absent crs means
    OGC:CRS84, while an explicit null means "unknown". These are lon/lat.
    """
    return json.dumps(
        {
            "version": GEOPARQUET_VERSION,
            "primary_column": primary_column,
            "columns": {
                primary_column: {
                    "encoding": "WKB",
                    "geometry_types": geometry_types or ["Point"],
                    "bbox": list(bbox),
                    "covering": {
                        "bbox": {
                            "xmin": [BBOX_COLUMN, "xmin"],
                            "ymin": [BBOX_COLUMN, "ymin"],
                            "xmax": [BBOX_COLUMN, "xmax"],
                            "ymax": [BBOX_COLUMN, "ymax"],
                        }
                    },
                }
            },
        },
        separators=(",", ":"),
    ).encode()


def to_geoparquet(table, lon_column: str = "lon", lat_column: str = "lat"):
    """Add geometry + bbox columns, Hilbert-sort, and attach `geo` metadata.

    The lon/lat columns are kept as ordinary attributes: they are useful on their
    own and cost almost nothing next to the WKB.
    """
    import pyarrow as pa

    lons = table.column(lon_column).to_pylist()
    lats = table.column(lat_column).to_pylist()
    order = hilbert_order(zip(lons, lats, strict=True))
    table = table.take(pa.array(order, pa.int32()))

    lons = table.column(lon_column).to_pylist()
    lats = table.column(lat_column).to_pylist()
    geoms = [None if x is None or y is None else wkb_point(x, y) for x, y in zip(lons, lats, strict=True)]
    bbox_struct = pa.StructArray.from_arrays(
        [
            pa.array(lons, pa.float64()),
            pa.array(lats, pa.float64()),
            pa.array(lons, pa.float64()),
            pa.array(lats, pa.float64()),
        ],
        names=["xmin", "ymin", "xmax", "ymax"],
    )
    table = table.append_column(
        pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True), pa.array(geoms, pa.binary())
    ).append_column(pa.field(BBOX_COLUMN, bbox_struct.type, nullable=True), bbox_struct)

    present = [(x, y) for x, y in zip(lons, lats, strict=True) if x is not None and y is not None]
    if present:
        xs = [p[0] for p in present]
        ys = [p[1] for p in present]
        bbox = (min(xs), min(ys), max(xs), max(ys))
    else:
        bbox = (-180.0, -90.0, 180.0, 90.0)

    meta = dict(table.schema.metadata or {})
    meta[b"geo"] = geo_metadata(bbox)
    return table.replace_schema_metadata(meta), bbox


def write_geoparquet(store, path: str, table, *, row_group_rows: int = 150_000):
    """Write a GeoParquet file. Row groups are capped per PORTO-FMT-009."""
    import pyarrow.parquet as pq

    with store.fs.open_output_stream(path) as sink:
        pq.write_table(
            table,
            sink,
            compression="zstd",
            compression_level=9,
            version="2.6",
            data_page_version="2.0",
            write_statistics=True,
            write_page_index=True,
            row_group_size=row_group_rows,
        )
