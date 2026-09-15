"""Per-station ingest watermarks, stored beside the data.

GitHub Actions runners are ephemeral, so the state lives in object storage:
``state/watermarks.parquet``. It is small (one row per station, ~7k rows) and is
rewritten whole on every run -- cheap, and it sidesteps concurrent-append
headaches entirely as long as only one ingest job runs at a time.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq

from . import paths
from .schema import WATERMARKS
from .storage import Store

PATH = (paths.STATE, "watermarks.parquet")


class Watermarks:
    def __init__(self, rows: dict[int, dict]):
        self.rows = rows

    @classmethod
    def load(cls, store: Store) -> "Watermarks":
        if not store.exists(*PATH):
            return cls({})
        with store.fs.open_input_file(store.join(*PATH)) as f:
            table = pq.read_table(f)
        return cls({r["station_id"]: r for r in table.to_pylist()})

    def save(self, store: Store) -> None:
        store.mkdirs(paths.STATE)
        table = pa.Table.from_pylist(
            sorted(self.rows.values(), key=lambda r: r["station_id"]), schema=WATERMARKS
        )
        # Write to a sibling then move, so a crashed run never leaves a torn file.
        tmp = store.join(paths.STATE, "watermarks.parquet.tmp")
        with store.fs.open_output_stream(tmp) as sink:
            pq.write_table(table, sink, compression="zstd")
        final = store.join(*PATH)
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
