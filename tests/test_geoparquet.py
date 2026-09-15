"""GeoParquet encoding pieces.

The parts Portolan actually validates are the ones worth pinning: WKB bytes,
the `geo` metadata shape including the covering column, and the claim that rows
are spatially ordered.
"""

from __future__ import annotations

import json
import struct

from pcds.geoparquet import (
    BBOX_COLUMN,
    GEOMETRY_COLUMN,
    geo_metadata,
    hilbert_index,
    hilbert_key,
    hilbert_order,
    wkb_point,
)


def test_wkb_point_is_21_bytes_little_endian():
    b = wkb_point(-117.305, 49.4914)
    assert len(b) == 21
    order, gtype, x, y = struct.unpack("<BIdd", b)
    assert order == 1          # little endian
    assert gtype == 1          # Point
    assert (round(x, 4), round(y, 4)) == (-117.305, 49.4914)


def test_hilbert_index_is_a_bijection_on_the_grid():
    n = 4
    seen = {hilbert_index(x, y, n) for x in range(1 << n) for y in range(1 << n)}
    assert len(seen) == (1 << n) ** 2
    assert min(seen) == 0 and max(seen) == (1 << n) ** 2 - 1


def test_hilbert_index_is_locality_preserving():
    # consecutive indices are adjacent cells: the property that makes row-group
    # bounding boxes tight
    n = 4
    cells = {}
    for x in range(1 << n):
        for y in range(1 << n):
            cells[hilbert_index(x, y, n)] = (x, y)
    for d in range((1 << n) ** 2 - 1):
        (x0, y0), (x1, y1) = cells[d], cells[d + 1]
        assert abs(x0 - x1) + abs(y0 - y1) == 1


def test_hilbert_key_puts_nearby_places_nearby():
    nelson = hilbert_key(-117.305, 49.4914)
    castlegar = hilbert_key(-117.66, 49.32)
    halifax = hilbert_key(-63.57, 44.65)
    assert abs(nelson - castlegar) < abs(nelson - halifax)


def test_hilbert_key_clamps_the_corners():
    assert hilbert_key(-180.0, -90.0) >= 0
    assert hilbert_key(180.0, 90.0) >= 0
    assert hilbert_key(1e9, 1e9) >= 0  # garbage in, still in range


def test_hilbert_order_sorts_and_pushes_null_coords_last():
    order = hilbert_order([(-117.3, 49.5), (None, None), (-117.31, 49.51), (None, 3.0)])
    assert order[-2:] == [1, 3] or set(order[-2:]) == {1, 3}
    assert set(order) == {0, 1, 2, 3}


def test_geo_metadata_declares_1_1_and_a_covering_column():
    meta = json.loads(geo_metadata((-139.0, 48.3, -114.0, 60.0)))
    assert meta["version"] == "1.1.0"
    assert meta["primary_column"] == GEOMETRY_COLUMN
    col = meta["columns"][GEOMETRY_COLUMN]
    assert col["encoding"] == "WKB"
    assert col["geometry_types"] == ["Point"]
    assert col["bbox"] == [-139.0, 48.3, -114.0, 60.0]
    # PORTO-FMT-007: per-row-group spatial statistics
    assert col["covering"]["bbox"] == {
        "xmin": [BBOX_COLUMN, "xmin"],
        "ymin": [BBOX_COLUMN, "ymin"],
        "xmax": [BBOX_COLUMN, "xmax"],
        "ymax": [BBOX_COLUMN, "ymax"],
    }


def test_geo_metadata_omits_crs_which_means_crs84():
    # GeoParquet 1.1: an absent crs means OGC:CRS84; an explicit null means unknown.
    col = json.loads(geo_metadata((0, 0, 1, 1)))["columns"][GEOMETRY_COLUMN]
    assert "crs" not in col
