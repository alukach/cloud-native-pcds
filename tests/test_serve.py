"""The viewer's bucket has to behave like S3, or the reader silently degrades.

Range handling is the part with edge cases, and the failure mode is quiet: a
server that answers 200 to a ranged request still returns correct bytes, so
nothing breaks, it just downloads the whole archive. These pin the shapes a
Parquet reader actually asks for.
"""

import importlib.util
import json
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

# scripts/ is a directory of entry points, not a package.
_spec = importlib.util.spec_from_file_location(
    "pcds_serve", Path(__file__).parent.parent / "scripts" / "serve.py")
serve = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(serve)
BucketHandler, Threaded = serve.BucketHandler, serve.Threaded

PAYLOAD = bytes(range(256)) * 8  # 2048 bytes, every offset distinguishable


@pytest.fixture(scope="module")
def bucket(tmp_path_factory):
    root = tmp_path_factory.mktemp("bucket")
    (root / "observations" / "period=1997").mkdir(parents=True)
    (root / "observations" / "period=1997" / "part-00000.parquet").write_bytes(PAYLOAD)
    (root / "layout.json").write_text('{"version": 1}')
    (root / ".DS_Store").write_bytes(b"junk")

    server = Threaded(("127.0.0.1", 0), BucketHandler)
    server.object_root = root.resolve()
    server.bucket = "pcds"
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def fetch(url, headers=None, method="GET"):
    req = urllib.request.Request(url, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


KEY = "/pcds/observations/period=1997/part-00000.parquet"


def test_whole_object(bucket):
    status, headers, body = fetch(bucket + KEY)
    assert status == 200
    assert body == PAYLOAD
    assert headers["Accept-Ranges"] == "bytes"
    assert headers["ETag"].startswith('"')


def test_range_returns_206(bucket):
    status, headers, body = fetch(bucket + KEY, {"Range": "bytes=10-19"})
    assert status == 206
    assert body == PAYLOAD[10:20]
    assert headers["Content-Range"] == f"bytes 10-19/{len(PAYLOAD)}"
    assert headers["Content-Length"] == "10"


def test_suffix_range_reads_the_footer(bucket):
    # How every Parquet reader starts: the last N bytes, to find the metadata.
    status, _, body = fetch(bucket + KEY, {"Range": "bytes=-8"})
    assert status == 206
    assert body == PAYLOAD[-8:]


def test_open_ended_range_is_clamped(bucket):
    status, headers, body = fetch(bucket + KEY, {"Range": "bytes=2040-9999"})
    assert status == 206
    assert body == PAYLOAD[2040:]
    assert headers["Content-Range"] == f"bytes 2040-2047/{len(PAYLOAD)}"


def test_head_with_range_answers_206(bucket):
    # DuckDB-WASM probes with exactly this and only uses partial reads on a 206.
    status, headers, body = fetch(bucket + KEY, {"Range": "bytes=0-"}, method="HEAD")
    assert status == 206
    assert body == b""
    assert headers["Content-Length"] == str(len(PAYLOAD))


def test_range_past_the_end_is_416(bucket):
    status, headers, _ = fetch(bucket + KEY, {"Range": "bytes=9999-"})
    assert status == 416
    assert headers["Content-Range"] == f"bytes */{len(PAYLOAD)}"


def test_missing_key_is_s3_xml(bucket):
    status, _, body = fetch(bucket + "/pcds/nope.parquet")
    assert status == 404
    assert b"<Code>NoSuchKey</Code>" in body


def test_cors_exposes_content_range(bucket):
    # Without this a cross-origin reader cannot see the object size and gives up.
    _, headers, _ = fetch(bucket + KEY)
    assert headers["Access-Control-Allow-Origin"] == "*"
    assert "Content-Range" in headers["Access-Control-Expose-Headers"]


def test_list_objects_v2(bucket):
    status, _, body = fetch(bucket + "/pcds/?list-type=2&prefix=observations/")
    assert status == 200
    assert b"observations/period=1997/part-00000.parquet" in body
    assert b".DS_Store" not in body  # a real bucket has no local junk in it


def test_path_traversal_is_refused(bucket):
    status, _, _ = fetch(bucket + "/pcds/../../etc/passwd")
    assert status == 404


def test_byte_counter_tracks_what_was_sent(bucket):
    before = json.loads(fetch(bucket + "/__stats")[2])["bytes"]
    fetch(bucket + KEY, {"Range": "bytes=0-99"})
    after = json.loads(fetch(bucket + "/__stats")[2])["bytes"]
    assert after - before == 100
