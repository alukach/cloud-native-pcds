"""Parquet writing, tuned for range-request reads out of object storage.

Two knobs do most of the work:

* **row group size** -- a reader that pushes down `station_id = X` still has to
  fetch whole column chunks for any row group whose statistics match. 1M rows of
  this schema is roughly 4-8 MiB compressed across all four columns, which is a
  reasonable minimum useful read.
* **sort order** -- (station_id, variable_id, obs_time). Sorting is what makes
  the statistics selective: a station's rows live in one or two row groups
  instead of being smeared across every row group in the file.

Everything else (dictionary encoding, delta-packed timestamps, zstd) follows
from the schema and is set explicitly rather than left to defaults, because the
defaults drift between pyarrow versions.
"""

from __future__ import annotations

import datetime as dt
import inspect

import pyarrow as pa
import pyarrow.parquet as pq

from . import paths
from .config import Settings
from .schema import OBSERVATIONS
from .storage import Store

SORT_KEYS = [("station_id", "ascending"), ("variable_id", "ascending"), ("obs_time", "ascending")]


def writer_options(settings: Settings) -> dict:
    opts = dict(
        compression=settings.compression,
        compression_level=settings.compression_level,
        version="2.6",
        data_page_version="2.0",
        data_page_size=settings.data_page_bytes,
        write_statistics=True,
        # Page index lets a reader skip to the pages that can match instead of
        # decoding a whole column chunk.
        write_page_index=True,
        column_encoding={
            "obs_time": "DELTA_BINARY_PACKED",
        },
        use_dictionary=["station_id", "variable_id"],
        sorting_columns=pq.SortingColumn.from_ordering(OBSERVATIONS, SORT_KEYS),
    )
    # Bloom filters on station_id turn a point lookup into a row-group skip even
    # when min/max ranges overlap. Guarded: the kwarg is newer than pyarrow 17.
    if "write_bloom_filter" in inspect.signature(pq.ParquetWriter.__init__).parameters:
        opts["write_bloom_filter"] = ["station_id"]
    return opts


def sort_table(table: pa.Table) -> pa.Table:
    return table.sort_by(SORT_KEYS)


class RollingWriter:
    """Writes a sorted table into `part-NNNNN.parquet` files under one prefix,
    rolling to a new file when the current one reaches the target size.

    Size is measured from the output stream position, which is the only honest
    way to do it: compressed size is not predictable from row count, and the
    ratio varies by an order of magnitude between a 1920s daily station and a
    modern 15-minute one.
    """

    def __init__(self, store: Store, prefix: str, settings: Settings, name: str = "part"):
        self.store = store
        self.prefix = prefix
        self.name = name
        self.settings = settings
        self.index = 0
        self.sink = None
        self.writer = None
        self.paths: list[str] = []
        self._opts = writer_options(settings)

    def _open(self) -> None:
        name = f"{self.name}-{self.index:05d}.parquet"
        path = self.store.join(self.prefix, name)
        self.sink = self.store.fs.open_output_stream(path)
        self.writer = pq.ParquetWriter(self.sink, OBSERVATIONS, **self._opts)
        self.paths.append(path)

    def write(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        if self.writer is None:
            self._open()
        for batch in table.to_batches(max_chunksize=self.settings.row_group_rows):
            self.writer.write_table(pa.Table.from_batches([batch]), row_group_size=len(batch))
            if self.sink.tell() >= self.settings.target_file_bytes:
                self.close()
                self.index += 1
                self._open()

    def close(self) -> list[str]:
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        if self.sink is not None:
            self.sink.close()
            self.sink = None
        return self.paths


def write_delta(
    store: Store, settings: Settings, table: pa.Table, run_id: str, shard: str
) -> str | None:
    """Stage an increment. Deltas are small, unsorted-across-stations, and carry
    an ingest timestamp so the compactor can resolve revisions."""
    if table.num_rows == 0:
        return None
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None, microsecond=0)
    table = sort_table(table).append_column(
        "ingested_at", pa.array([now] * table.num_rows, pa.timestamp("s"))
    )
    prefix = f"{paths.DELTA}/run_id={run_id}"
    store.mkdirs(prefix)
    path = store.join(prefix, f"{shard}.parquet")
    with store.fs.open_output_stream(path) as sink:
        pq.write_table(
            table,
            sink,
            compression=settings.compression,
            compression_level=settings.compression_level,
            version="2.6",
            data_page_version="2.0",
            write_statistics=True,
        )
    return path
