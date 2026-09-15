"""A dependency-free PNG thumbnail of a point collection.

Portolan requires every geospatial collection to carry a thumbnail "generated
from default styling". For 7,000 station points that is a scatter plot, and a
scatter plot is not worth a matplotlib dependency in a CI job: a PNG is a zlib
stream of filtered scanlines plus three chunks.

Points are drawn in an equirectangular projection scaled to the data's own
bounding box, with latitude compressed by cos(mean latitude) so BC does not come
out stretched.
"""

from __future__ import annotations

import math
import struct
import zlib

BG = (245, 245, 243)
FG = (196, 78, 51)
FRAME = (210, 210, 206)


def _png(width: int, height: int, rgb: bytearray) -> bytes:
    """Encode an RGB buffer (row-major, 3 bytes per pixel) as a PNG."""
    raw = bytearray()
    stride = width * 3
    for y in range(height):
        raw.append(0)  # filter type 0 (None)
        raw += rgb[y * stride : (y + 1) * stride]

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)  # 8-bit truecolour
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(bytes(raw), 9))
        + chunk(b"IEND", b"")
    )


def _plot(buf: bytearray, width: int, x: int, y: int, color: tuple[int, int, int], r: int = 1):
    for dy in range(-r, r + 1):
        for dx in range(-r, r + 1):
            if dx * dx + dy * dy > r * r + 1:
                continue
            px, py = x + dx, y + dy
            if 0 <= px < width and 0 <= py < len(buf) // (width * 3):
                i = (py * width + px) * 3
                buf[i : i + 3] = bytes(color)


def points_png(
    coords: list[tuple[float, float]],
    *,
    width: int = 480,
    height: int = 320,
    pad: int = 12,
) -> bytes:
    """Scatter of lon/lat points, framed to their own extent."""
    pts = [(x, y) for x, y in coords if x is not None and y is not None]
    buf = bytearray(BG * (width * height))
    if not pts:
        return _png(width, height, buf)

    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    x0, x1 = min(xs), max(xs)
    y0, y1 = min(ys), max(ys)
    # Compress longitude by cos(lat) so the aspect ratio is not wrong at 55N.
    k = max(0.1, math.cos(math.radians((y0 + y1) / 2)))
    span_x = max((x1 - x0) * k, 1e-6)
    span_y = max(y1 - y0, 1e-6)
    scale = min((width - 2 * pad) / span_x, (height - 2 * pad) / span_y)
    ox = (width - span_x * scale) / 2
    oy = (height - span_y * scale) / 2

    for px in range(pad // 2, width - pad // 2):
        for py in (pad // 2, height - pad // 2 - 1):
            i = (py * width + px) * 3
            buf[i : i + 3] = bytes(FRAME)
    for py in range(pad // 2, height - pad // 2):
        for px in (pad // 2, width - pad // 2 - 1):
            i = (py * width + px) * 3
            buf[i : i + 3] = bytes(FRAME)

    for lon, lat in pts:
        x = int(ox + (lon - x0) * k * scale)
        y = int(height - oy - (lat - y0) * scale)
        _plot(buf, width, x, y, FG, r=1)
    return _png(width, height, buf)
