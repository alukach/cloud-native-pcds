"""Filesystem abstraction over a local directory or an S3-compatible endpoint.

Source Cooperative publishes through an S3-compatible data proxy, so the same
code path covers local development, plain S3, R2 and Source. Credentials come
from the standard AWS environment variables; the endpoint override is the only
Source-specific knob.
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass

import pyarrow.fs as pafs

from .config import Settings


@dataclass
class Store:
    fs: pafs.FileSystem
    root: str  # path *within* the filesystem (no scheme)
    uri: str  # original URI, for logging and DuckDB

    def join(self, *parts: str) -> str:
        return posixpath.join(self.root, *[p.strip("/") for p in parts])

    def uri_for(self, *parts: str) -> str:
        return posixpath.join(self.uri.rstrip("/"), *[p.strip("/") for p in parts])

    def mkdirs(self, *parts: str) -> str:
        path = self.join(*parts)
        self.fs.create_dir(path, recursive=True)
        return path

    def exists(self, *parts: str) -> bool:
        info = self.fs.get_file_info(self.join(*parts))
        return info.type != pafs.FileType.NotFound

    def ls(self, *parts: str, recursive: bool = False) -> list[pafs.FileInfo]:
        sel = pafs.FileSelector(self.join(*parts), recursive=recursive, allow_not_found=True)
        return [i for i in self.fs.get_file_info(sel) if i.type == pafs.FileType.File]

    def delete(self, *parts: str) -> None:
        path = self.join(*parts)
        info = self.fs.get_file_info(path)
        if info.type == pafs.FileType.File:
            self.fs.delete_file(path)
        elif info.type == pafs.FileType.Directory:
            self.fs.delete_dir(path)


def open_store(settings: Settings, root: str | None = None) -> Store:
    uri = (root or settings.root).rstrip("/")
    if uri.startswith("s3://"):
        without = uri[len("s3://") :]
        fs = pafs.S3FileSystem(
            endpoint_override=settings.s3_endpoint,
            region=settings.s3_region,
            # Source Cooperative and R2 both want path-style addressing.
            force_virtual_addressing=False,
            allow_bucket_creation=False,
        )
        return Store(fs=fs, root=without, uri=uri)
    fs = pafs.LocalFileSystem()
    import os

    abs_root = os.path.abspath(uri)
    fs.create_dir(abs_root, recursive=True)
    return Store(fs=fs, root=abs_root, uri=abs_root)


def duckdb_connect(settings: Settings, store: Store):
    """A DuckDB connection configured to read/write the same store."""
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET memory_limit='{_memory_limit()}';")
    con.execute("SET preserve_insertion_order=false;")
    if store.uri.startswith("s3://"):
        import os

        if settings.s3_endpoint:
            ep = settings.s3_endpoint.replace("https://", "").replace("http://", "")
            con.execute(f"SET s3_endpoint='{ep}';")
            con.execute("SET s3_url_style='path';")
            con.execute(f"SET s3_use_ssl={'true' if settings.s3_endpoint.startswith('https') else 'false'};")
        con.execute(f"SET s3_region='{settings.s3_region}';")
        if os.environ.get("AWS_ACCESS_KEY_ID"):
            con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
            con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
        if os.environ.get("AWS_SESSION_TOKEN"):
            con.execute(f"SET s3_session_token='{os.environ['AWS_SESSION_TOKEN']}';")
    return con


def _memory_limit() -> str:
    import os

    # GitHub-hosted runners are 7GB (public) / 16GB (larger runners).
    return os.environ.get("PCDS_DUCKDB_MEMORY", "5GB")
