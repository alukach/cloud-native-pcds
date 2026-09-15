#!/usr/bin/env python3
"""Serve ./data as a public S3 bucket, and the viewer on a separate origin.

The archive is going to live on Source Cooperative, so the useful local server
is not a static file server, it is the smallest thing that behaves like the one
the viewer will actually talk to in production:

  * range requests, answered 206, on GET *and* on HEAD (DuckDB-WASM probes with
    `HEAD` + `Range: bytes=0-` and only uses partial reads if that is a 206);
  * ListObjectsV2, so a client can discover partitions without the manifest;
  * S3-shaped XML errors, ETag and Last-Modified;
  * CORS, including `Access-Control-Expose-Headers: Content-Range`, without
    which a cross-origin reader cannot see how big an object is.

The viewer is deliberately served from a *different* port than the data, because
that is the production shape (page on one origin, objects on data.source.coop)
and it is the only way the CORS configuration above is actually exercised.

Anonymous reads only: Source Cooperative serves public repositories without
credentials, so there is no SigV4 here and nothing to sign.

    scripts/serve.py                 # viewer :8777, bucket :8788
    scripts/serve.py -v              # log every object request and its range

/__stats on the bucket origin is the one non-S3 route: a running total of bytes
this server has actually written, which is how the viewer reports what a query
cost over the wire rather than estimating it.
"""

import argparse
import hashlib
import http.server
import json
import os
import re
import socketserver
import sys
import threading
import urllib.parse
from datetime import UTC, datetime
from email.utils import formatdate
from pathlib import Path
from xml.sax.saxutils import escape

_lock = threading.Lock()
_stats = {"bytes": 0, "requests": 0}
_etags = {}
_RANGE = re.compile(r"^bytes=(\d*)-(\d*)$")
VERBOSE = False

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "range, if-match, if-none-match, x-amz-content-sha256, x-amz-date",
    # Content-Range is the one that matters: without it a cross-origin client can
    # read the bytes but not the object size, and every reader gives up.
    "Access-Control-Expose-Headers": "Content-Range, Content-Length, ETag, Accept-Ranges, Last-Modified",
    "Access-Control-Max-Age": "600",
}


def _count(n):
    with _lock:
        _stats["bytes"] += n
        _stats["requests"] += 1


def _etag(path):
    """S3 gives a single-part object the MD5 of its bytes. Cache it per mtime."""
    st = os.stat(path)
    key = (path, st.st_mtime_ns, st.st_size)
    with _lock:
        hit = _etags.get(key)
    if hit:
        return hit
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    tag = f'"{h.hexdigest()}"'
    with _lock:
        _etags[key] = tag
    return tag


def _content_type(path):
    if path.endswith(".parquet"):
        return "application/vnd.apache.parquet"
    if path.endswith(".json"):
        return "application/json"
    if path.endswith(".png"):
        return "image/png"
    return "application/octet-stream"


class BucketHandler(http.server.BaseHTTPRequestHandler):
    """Enough of the S3 REST API to stand in for a public bucket."""

    protocol_version = "HTTP/1.1"   # a column read is dozens of small ranges
    server_version = "AmazonS3"
    sys_version = ""

    # -- plumbing ---------------------------------------------------------

    def log_message(self, *args):
        pass

    def _trace(self, note):
        if VERBOSE:
            print(f"  {self.command:4} {self.path}  Range={self.headers.get('Range')}  -> {note}",
                  file=sys.stderr, flush=True)

    def _key(self):
        """Split /<bucket>/<key> and return (bucket, key, query)."""
        parsed = urllib.parse.urlsplit(self.path)
        parts = urllib.parse.unquote(parsed.path).lstrip("/").split("/", 1)
        bucket = parts[0] if parts else ""
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key, urllib.parse.parse_qs(parsed.query)

    def _resolve(self, key):
        """Map an object key to a path under the root, refusing escapes."""
        root = self.server.object_root
        target = (root / key).resolve()
        if not str(target).startswith(str(root)) or not target.is_file():
            return None
        return target

    def _head(self, code, headers, body_len=None):
        self.send_response(code)
        for k, v in CORS.items():
            self.send_header(k, v)
        for k, v in headers.items():
            self.send_header(k, v)
        if body_len is not None:
            self.send_header("Content-Length", str(body_len))
        self.end_headers()

    def _error(self, code, s3code, message, resource=""):
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f"<Error><Code>{s3code}</Code><Message>{escape(message)}</Message>"
            f"<Resource>{escape(resource)}</Resource></Error>"
        ).encode()
        self._head(code, {"Content-Type": "application/xml"}, len(body))
        if self.command != "HEAD":
            self.wfile.write(body)

    # -- routes -----------------------------------------------------------

    def do_OPTIONS(self):
        self._trace(f"204 preflight for {self.headers.get('Access-Control-Request-Headers')}")
        self._head(204, {}, 0)

    def do_HEAD(self):
        self.do_GET(body=False)

    def do_GET(self, body=True):
        bucket, key, query = self._key()

        if bucket == "__stats":
            with _lock:
                payload = json.dumps(_stats).encode()
            self._head(200, {"Content-Type": "application/json", "Cache-Control": "no-store"},
                       len(payload))
            if body:
                self.wfile.write(payload)
            return

        if bucket != self.server.bucket:
            self._error(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
            return

        if not key or "list-type" in query or "prefix" in query:
            self._list(query, body)
            return

        path = self._resolve(key)
        if path is None:
            self._trace("404")
            self._error(404, "NoSuchKey", "The specified key does not exist.", key)
            return
        self._object(path, body)

    def _object(self, path, body):
        size = path.stat().st_size
        base = {
            "Content-Type": _content_type(path.name),
            "Accept-Ranges": "bytes",
            "ETag": _etag(str(path)),
            "Last-Modified": formatdate(path.stat().st_mtime, usegmt=True),
            # Parquet is immutable once written; re-querying should not re-fetch a
            # footer the client already holds.
            "Cache-Control": "public, max-age=300",
        }

        rng = self.headers.get("Range")
        if not rng:
            self._trace(f"200 whole object ({size} B)")
            self._head(200, base, size)
            if body:
                self._send_file(path, 0, size)
            else:
                _count(0)
            return

        m = _RANGE.match(rng.strip())
        if not m:
            self._trace("416")
            self._head(416, {**base, "Content-Range": f"bytes */{size}"}, 0)
            return
        lo, hi = m.group(1), m.group(2)
        if lo == "":  # suffix range: the last N bytes, how a Parquet footer is read
            start, end = max(0, size - int(hi or 0)), size - 1
        else:
            start = int(lo)
            end = min(int(hi), size - 1) if hi else size - 1
        if start > end or start >= size:
            self._trace("416")
            self._head(416, {**base, "Content-Range": f"bytes */{size}"}, 0)
            return

        length = end - start + 1
        self._trace(f"206 {start}-{end} ({length} B)")
        self._head(206, {**base, "Content-Range": f"bytes {start}-{end}/{size}"}, length)
        if body:
            self._send_file(path, start, length)

    def _send_file(self, path, start, length):
        remaining = length
        with open(path, "rb") as fh:
            fh.seek(start)
            while remaining:
                chunk = fh.read(min(1 << 16, remaining))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    break
                remaining -= len(chunk)
        _count(length - remaining)

    def _list(self, query, body):
        """ListObjectsV2, enough of it to walk the archive without the manifest."""
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [""])[0]
        max_keys = min(int(query.get("max-keys", ["1000"])[0]), 1000)
        after = query.get("continuation-token", [query.get("start-after", [""])[0]])[0]

        root = self.server.object_root
        keys = []
        for p in sorted(root.rglob("*")):
            if not p.is_file() or any(part.startswith(".") for part in p.parts):
                continue
            k = str(p.relative_to(root))
            if k.startswith(prefix) and k > after:
                keys.append((k, p))

        contents, common = [], set()
        for k, p in keys:
            if delimiter:
                rest = k[len(prefix):]
                if delimiter in rest:
                    common.add(prefix + rest.split(delimiter)[0] + delimiter)
                    continue
            contents.append((k, p))
            if len(contents) >= max_keys:
                break

        truncated = len(contents) >= max_keys
        rows = "".join(
            f"<Contents><Key>{escape(k)}</Key>"
            f"<LastModified>{datetime.fromtimestamp(p.stat().st_mtime, UTC).strftime('%Y-%m-%dT%H:%M:%S.000Z')}</LastModified>"
            f"<ETag>{escape(_etag(str(p)))}</ETag><Size>{p.stat().st_size}</Size>"
            f"<StorageClass>STANDARD</StorageClass></Contents>"
            for k, p in contents)
        prefixes = "".join(f"<CommonPrefixes><Prefix>{escape(c)}</Prefix></CommonPrefixes>"
                           for c in sorted(common))
        payload = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Name>{escape(self.server.bucket)}</Name><Prefix>{escape(prefix)}</Prefix>"
            f"<Delimiter>{escape(delimiter)}</Delimiter><MaxKeys>{max_keys}</MaxKeys>"
            f"<KeyCount>{len(contents) + len(common)}</KeyCount>"
            f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>"
            + (f"<NextContinuationToken>{escape(contents[-1][0])}</NextContinuationToken>"
               if truncated and contents else "")
            + rows + prefixes + "</ListBucketResult>"
        ).encode()
        self._trace(f"200 list ({len(contents)} keys)")
        self._head(200, {"Content-Type": "application/xml"}, len(payload))
        if body:
            self.wfile.write(payload)


class ViewerHandler(http.server.SimpleHTTPRequestHandler):
    """Plain static hosting for viewer/, on its own origin. No data lives here."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass


class Threaded(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    ap = argparse.ArgumentParser(description="Serve ./data as an S3 bucket, plus the viewer.")
    ap.add_argument("--root", default=".", help="repository root holding data/ and viewer/")
    ap.add_argument("--port", type=int, default=8777, help="viewer origin")
    ap.add_argument("--s3-port", type=int, default=8788, help="bucket origin")
    ap.add_argument("--bucket", default="pcds", help="bucket name in the object path")
    ap.add_argument("-v", "--verbose", action="store_true", help="log every object request")
    args = ap.parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    root = Path(args.root).resolve()
    data = root / "data"
    if not data.is_dir():
        sys.exit(f"no data/ under {root}: nothing to serve")

    bucket = Threaded(("127.0.0.1", args.s3_port), BucketHandler)
    bucket.object_root = data.resolve()
    bucket.bucket = args.bucket
    threading.Thread(target=bucket.serve_forever, daemon=True).start()

    def viewer_handler(*a, **kw):
        return ViewerHandler(*a, directory=str(root), **kw)

    print(f"bucket  http://127.0.0.1:{args.s3_port}/{args.bucket}/")
    print(f"viewer  http://127.0.0.1:{args.port}/viewer/")
    with Threaded(("127.0.0.1", args.port), viewer_handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
