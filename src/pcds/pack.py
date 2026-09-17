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
        # `value` is ~95% of every file and is the only column that was left
        # PLAIN. These are finite-precision instrument readings, so a row group
        # holds a few hundred distinct doubles: measured on a contiguous 12M-row
        # slice of period=2023-2026, dictionary takes the whole file from 1.337
        # to 1.108 bytes/row and writes 3.6x faster. pyarrow falls back to PLAIN
        # per chunk if a column turns out high-cardinality, so the downside on
        # an unrounded variable is a few percent, not a cliff.
        use_dictionary=["station_id", "variable_id", "value"],
        sorting_columns=pq.SortingColumn.from_ordering(OBSERVATIONS, SORT_KEYS),
    )
    # No bloom filter on station_id, deliberately. It is the leading sort key,
    # so min/max statistics already prune exactly and a bloom filter cannot beat
    # exact: measured, it cost 2-4 extra round trips and saved ~0 bytes. This
    # used to be a feature-detect on a `write_bloom_filter` kwarg that pyarrow
    # has never had under that name (it is `bloom_filter_options`), so the guard
    # was a permanent false negative and no filter was ever written, while four
    # documents claimed one was. Revisit only if the sort order changes.
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

    Every file it emits lands in [min_file_bytes, max_file_bytes]. A roll only
    happens once the current file has passed the target, so every file except
    the last is at least that big; the last one is whatever was left over, which
    is the only way a sub-floor file can be born here. `close` merges such a tail
    back into its predecessor. The single exception is a period holding less than
    the floor in total: there is nothing to merge it with, and no writer can fix
    what the layout asked for. `pcds verify` reports those.
    """

    def __init__(self, store: Store, prefix: str, settings: Settings, name: str = "part"):
        # A merged tail is at most one target plus one floor, so that sum is the
        # real worst case and the only thing that has to stay under the ceiling.
        worst = settings.target_file_bytes + settings.min_file_bytes
        if worst > settings.max_file_bytes:
            raise ValueError(
                f"target ({settings.target_file_bytes}) + floor "
                f"({settings.min_file_bytes}) = {worst} bytes, over the "
                f"{settings.max_file_bytes} ceiling; lower PCDS_TARGET_FILE_BYTES"
            )
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
                self._close_current()
                self.index += 1
                self._open()

    def _close_current(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        if self.sink is not None:
            self.sink.close()
            self.sink = None

    def _size(self, path: str) -> int:
        return self.store.fs.get_file_info(path).size

    def _merge_tail(self) -> None:
        """Fold the last file into the one before it, row group by row group.

        Streamed rather than read whole: the two together can be most of a
        gigabyte compressed and several times that in Arrow. Input to the writer
        is globally sorted and files are written in order, so every row of the
        earlier file precedes every row of the later one and concatenating them
        keeps the sort.
        """
        keep, tail = self.paths[-2], self.paths[-1]
        merged = f"{keep}.merging"
        sink = self.store.fs.open_output_stream(merged)
        writer = pq.ParquetWriter(sink, OBSERVATIONS, **self._opts)
        try:
            for src in (keep, tail):
                with self.store.fs.open_input_file(src) as f:
                    reader = pq.ParquetFile(f)
                    for i in range(reader.num_row_groups):
                        # Parquet has no SECOND timestamp, so obs_time round
                        # trips as timestamp[ms] and the writer rejects it.
                        writer.write_table(
                            reader.read_row_group(i).cast(OBSERVATIONS),
                            row_group_size=self.settings.row_group_rows,
                        )
        finally:
            writer.close()
            sink.close()
        self.store.fs.delete_file(keep)
        self.store.fs.delete_file(tail)
        self.store.fs.move(merged, keep)
        self.paths.pop()

    def close(self) -> list[str]:
        self._close_current()
        if len(self.paths) > 1 and self._size(self.paths[-1]) < self.settings.min_file_bytes:
            self._merge_tail()
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
