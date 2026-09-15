"""The dependency-free PNG writer."""

from __future__ import annotations

import struct
import zlib

from pcds.thumbnail import points_png

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def _chunks(data: bytes):
    i = len(PNG_MAGIC)
    while i < len(data):
        (length,) = struct.unpack(">I", data[i : i + 4])
        tag = data[i + 4 : i + 8]
        payload = data[i + 8 : i + 8 + length]
        (crc,) = struct.unpack(">I", data[i + 8 + length : i + 12 + length])
        yield tag, payload, crc
        i += 12 + length


def test_emits_a_structurally_valid_png():
    png = points_png([(-117.3, 49.5), (-123.1, 49.2)], width=64, height=48)
    assert png.startswith(PNG_MAGIC)
    tags = []
    for tag, payload, crc in _chunks(png):
        assert zlib.crc32(tag + payload) & 0xFFFFFFFF == crc
        tags.append(tag)
    assert tags == [b"IHDR", b"IDAT", b"IEND"]


def test_header_matches_requested_size_and_is_8bit_truecolour():
    png = points_png([(0.0, 0.0)], width=120, height=90)
    _, ihdr, _ = next(iter(_chunks(png)))
    w, h, depth, colour, comp, filt, interlace = struct.unpack(">IIBBBBB", ihdr)
    assert (w, h) == (120, 90)
    assert (depth, colour) == (8, 2)
    assert (comp, filt, interlace) == (0, 0, 0)


def test_scanlines_decompress_to_the_right_length():
    w, h = 40, 30
    png = points_png([(1.0, 1.0), (2.0, 2.0)], width=w, height=h)
    idat = next(p for t, p, _ in _chunks(png) if t == b"IDAT")
    raw = zlib.decompress(idat)
    assert len(raw) == h * (1 + w * 3)
    assert all(raw[y * (1 + w * 3)] == 0 for y in range(h))  # filter byte 0


def test_empty_input_still_produces_an_image():
    png = points_png([], width=32, height=32)
    assert png.startswith(PNG_MAGIC)


def test_points_land_inside_the_frame():
    w, h = 60, 60
    png = points_png([(-130.0, 50.0), (-115.0, 59.0)], width=w, height=h)
    raw = zlib.decompress(next(p for t, p, _ in _chunks(png) if t == b"IDAT"))
    stride = 1 + w * 3
    from pcds.thumbnail import BG

    drawn = [
        (x, y)
        for y in range(h)
        for x in range(w)
        if tuple(raw[y * stride + 1 + x * 3 : y * stride + 4 + x * 3]) != BG
    ]
    assert drawn, "nothing was drawn"
    assert all(0 <= x < w and 0 <= y < h for x, y in drawn)
