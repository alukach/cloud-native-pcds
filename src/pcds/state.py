"""Per-station ingest watermarks, stored beside the data.

GitHub Actions runners are ephemeral, so the state lives in object storage:
``_state/watermarks*.parquet``. It is small (one row per station, ~7k rows) and
is rewritten whole on every run, which is cheap.

A whole-object rewrite is only safe when one writer runs at a time, and the
backfill matrix deliberately runs 8 shards at once: they would each load the
same snapshot, and the last to finish would silently drop the other seven. So a
writer that shares the prefix writes its *own* file and `load` merges them.
Merging is well defined without coordination because the rows are keyed by
station_id and the later watermark always wins, so the result does not depend on
which shard finished first.
"""

from __future__ import annotations

import datetime as dt
import os

import pyarrow as pa
import pyarrow.parquet as pq

from . import paths
from .schema import WATERMARKS
from .storage import Store

PATH = (paths.STATE, "watermarks.parquet")
PREFIX = "watermarks"


def _filename(writer: str | None) -> str:
    return f"{PREFIX}.parquet" if writer is None else f"{PREFIX}-{writer}.parquet"


def _merge(into: dict[int, dict], row: dict) -> None:
    """Later watermark wins, so the merge does not depend on file order."""
    current = into.get(row["station_id"])
    if current is None:
        into[row["station_id"]] = row
        return
    a, b = current["watermark"], row["watermark"]
    if a is None or (b is not None and b > a):
        into[row["station_id"]] = row


class Watermarks:
    def __init__(self, rows: dict[int, dict]):
        self.rows = rows

    @classmethod
    def load(cls, store: Store) -> Watermarks:
        rows: dict[int, dict] = {}
        for info in store.ls(paths.STATE):
            name = info.path.rsplit("/", 1)[-1]
            if not (name.startswith(PREFIX) and name.endswith(".parquet")):
                continue
            with store.fs.open_input_file(info.path) as f:
                for row in pq.read_table(f).to_pylist():
                    _merge(rows, row)
        return cls(rows)

    def save(self, store: Store, writer: str | None = None) -> None:
        """Persist. `writer` names a concurrent writer (a backfill shard); without
        it this rewrites the single shared file, which only one job may do."""
        store.mkdirs(paths.STATE)
        table = pa.Table.from_pylist(
            sorted(self.rows.values(), key=lambda r: r["station_id"]), schema=WATERMARKS
        )
        name = _filename(writer)
        # Write to a sibling then move, so a crashed run never leaves a torn file.
        # The temp name carries the writer too: a shared one would let two shards
        # interleave their bytes into a single object and then publish it.
        tmp = store.join(paths.STATE, f"{name}.{os.getpid()}.tmp")
        with store.fs.open_output_stream(tmp) as sink:
            pq.write_table(table, sink, compression="zstd")
        final = store.join(paths.STATE, name)
        try:
            store.fs.move(tmp, final)
        except Exception:  # some object stores lack atomic move; copy semantics
            store.fs.copy_file(tmp, final)
            store.fs.delete_file(tmp)

    def get(self, station_id: int) -> dict | None:
        return self.rows.get(station_id)

    def watermark(self, station_id: int) -> dt.datetime | None:
        row = self.rows.get(station_id)
        return row["watermark"] if row else None

    def ensure(self, station_id: int, network_name: str, native_id: str) -> dict:
        return self.rows.setdefault(
            station_id,
            {
                "station_id": station_id,
                "network_name": network_name,
                "native_id": native_id,
                "watermark": None,
                "last_attempt_at": None,
                "last_success_at": None,
                "rows_total": 0,
                "consecutive_failures": 0,
                "last_error": None,
            },
        )

    def record_success(
        self,
        station_id: int,
        network_name: str,
        native_id: str,
        *,
        max_obs_time: dt.datetime | None,
        rows: int,
        now: dt.datetime,
    ) -> None:
        row = self.ensure(station_id, network_name, native_id)
        row["last_attempt_at"] = now
        row["last_success_at"] = now
        row["rows_total"] = (row["rows_total"] or 0) + rows
        row["consecutive_failures"] = 0
        row["last_error"] = None
        if max_obs_time is not None:
            current = row["watermark"]
            row["watermark"] = max_obs_time if current is None else max(current, max_obs_time)

    def record_failure(
        self, station_id: int, network_name: str, native_id: str, *, error: str, now: dt.datetime
    ) -> None:
        row = self.ensure(station_id, network_name, native_id)
        row["last_attempt_at"] = now
        row["consecutive_failures"] = (row["consecutive_failures"] or 0) + 1
        row["last_error"] = error[:500]

    def quarantined(self, station_id: int, threshold: int = 10) -> bool:
        """Stations that have failed many runs in a row stop being retried every
        run; a weekly full sweep picks them back up."""
        row = self.rows.get(station_id)
        return bool(row and (row["consecutive_failures"] or 0) >= threshold)
