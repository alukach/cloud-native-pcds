"""Arrow schemas for the published tables.

Design notes
------------
The observation table is *long* (tidy): one row per measured value. That mirrors
the CRMP ``obs_raw`` model, survives the fact that 22 networks report 358
distinct variable definitions with no common wide schema, and -- once the file
is sorted by (station_id, variable_id, obs_time) -- compresses to a few bytes
per row because the two id columns collapse to RLE runs and obs_time becomes a
near-constant delta.

Column types are chosen for encoding, not just for range:
  station_id   int32  - ~7k distinct, dictionary+RLE to nothing when sorted
  variable_id  int16  - 358 distinct, same
  obs_time     timestamp[s] - DELTA_BINARY_PACKED; seconds is exact for PCDS
  value        float64 - honest to the source; set PCDS_VALUE_FLOAT32=1 to halve
"""

from __future__ import annotations

import os

import pyarrow as pa

VALUE_TYPE = pa.float32() if os.environ.get("PCDS_VALUE_FLOAT32") == "1" else pa.float64()

# Observations as published. Partition columns (period) are Hive-encoded in the
# path and deliberately not repeated in the file.
OBSERVATIONS = pa.schema(
    [
        pa.field("station_id", pa.int32(), nullable=False),
        pa.field("variable_id", pa.int16(), nullable=False),
        pa.field("obs_time", pa.timestamp("s"), nullable=False),
        pa.field("value", VALUE_TYPE, nullable=True),
    ],
    metadata={
        b"pcds.source": b"https://services.pacificclimate.org/met-data-portal-pcds/",
        b"pcds.time_zone": (
            b"Naive local standard time as published by PCIC. "
            b"Per-station offset is in histories.tz_offset where known."
        ),
        b"pcds.sort_order": b"station_id, variable_id, obs_time",
    },
)

# Monthly rollup. A pure function of OBSERVATIONS, rebuilt whole on every
# compaction, and ~2% of the archive. It exists because the partition is the
# only time-pruning granularity: a reader issues one range request per row group
# per column, so any month-or-coarser aggregate over the raw table pays for the
# whole obs_time column of a period. `n` is count(value), not count(*), so the
# means compose weighted: sum(mean*n)/sum(n). Quantiles do not compose and are
# deliberately absent; read the raw table for those.
OBSERVATIONS_MONTHLY = pa.schema(
    [
        pa.field("station_id", pa.int32(), nullable=False),
        pa.field("variable_id", pa.int16(), nullable=False),
        pa.field("month", pa.date32(), nullable=False),
        pa.field("n", pa.int32(), nullable=False),
        pa.field("mean", pa.float64(), nullable=True),
        pa.field("lo", pa.float64(), nullable=True),
        pa.field("hi", pa.float64(), nullable=True),
    ],
    metadata={
        b"pcds.source": b"Derived from the observations collection; rebuilt whole each compaction.",
        b"pcds.sort_order": b"variable_id, month, station_id",
    },
)

# Staging deltas carry provenance so the compactor can resolve revisions.
DELTA = pa.schema(
    [
        *OBSERVATIONS,
        pa.field("ingested_at", pa.timestamp("s"), nullable=False),
    ]
)

NETWORKS = pa.schema(
    [
        pa.field("network_id", pa.int16(), nullable=False),
        pa.field("network_name", pa.string(), nullable=False),
        pa.field("long_name", pa.string()),
        pa.field("color", pa.string()),
        pa.field("publish", pa.bool_()),
        pa.field("station_count", pa.int32()),
    ]
)

VARIABLES = pa.schema(
    [
        pa.field("variable_id", pa.int16(), nullable=False),
        pa.field("network_id", pa.int16(), nullable=False),
        # `name` is the column label used by the OPeNDAP lister response.
        pa.field("name", pa.string(), nullable=False),
        pa.field("display_name", pa.string()),
        pa.field("short_name", pa.string()),
        pa.field("standard_name", pa.string()),
        pa.field("cell_method", pa.string()),
        pa.field("unit", pa.string()),
        pa.field("precision", pa.float64()),
        pa.field("tags", pa.list_(pa.string())),
    ]
)

STATIONS = pa.schema(
    [
        pa.field("station_id", pa.int32(), nullable=False),
        pa.field("network_id", pa.int16(), nullable=False),
        pa.field("network_name", pa.string(), nullable=False),
        pa.field("native_id", pa.string(), nullable=False),
        pa.field("station_name", pa.string()),
        pa.field("lon", pa.float64()),
        pa.field("lat", pa.float64()),
        pa.field("elevation", pa.float64()),
        pa.field("province", pa.string()),
        pa.field("freq", pa.string()),
        pa.field("min_obs_time", pa.timestamp("s")),
        pa.field("max_obs_time", pa.timestamp("s")),
        pa.field("history_count", pa.int16()),
        pa.field("variable_ids", pa.list_(pa.int16())),
    ]
)

# One row per station history (a distinct station configuration/location).
HISTORIES = pa.schema(
    [
        pa.field("history_id", pa.int32(), nullable=False),
        pa.field("station_id", pa.int32(), nullable=False),
        pa.field("network_name", pa.string(), nullable=False),
        pa.field("native_id", pa.string(), nullable=False),
        pa.field("station_name", pa.string()),
        pa.field("lon", pa.float64()),
        pa.field("lat", pa.float64()),
        pa.field("elevation", pa.float64()),
        pa.field("province", pa.string()),
        pa.field("country", pa.string()),
        pa.field("freq", pa.string()),
        pa.field("tz_offset", pa.string()),
        pa.field("sdate", pa.timestamp("s")),
        pa.field("edate", pa.timestamp("s")),
        pa.field("min_obs_time", pa.timestamp("s")),
        pa.field("max_obs_time", pa.timestamp("s")),
        pa.field("variable_ids", pa.list_(pa.int16())),
    ]
)

# Per-station ingest state. Lives beside the data so Actions stays stateless.
WATERMARKS = pa.schema(
    [
        pa.field("station_id", pa.int32(), nullable=False),
        pa.field("network_name", pa.string(), nullable=False),
        pa.field("native_id", pa.string(), nullable=False),
        pa.field("watermark", pa.timestamp("s")),  # max obs_time successfully stored
        pa.field("last_attempt_at", pa.timestamp("s")),
        pa.field("last_success_at", pa.timestamp("s")),
        pa.field("rows_total", pa.int64()),
        pa.field("consecutive_failures", pa.int16()),
        pa.field("last_error", pa.string()),
    ]
)

# File-level catalog: lets a client prune without a LIST against object storage.
CATALOG = pa.schema(
    [
        pa.field("path", pa.string(), nullable=False),
        pa.field("period", pa.string(), nullable=False),
        pa.field("rows", pa.int64()),
        pa.field("file_bytes", pa.int64()),
        pa.field("row_groups", pa.int32()),
        pa.field("station_id_min", pa.int32()),
        pa.field("station_id_max", pa.int32()),
        pa.field("obs_time_min", pa.timestamp("s")),
        pa.field("obs_time_max", pa.timestamp("s")),
        pa.field("written_at", pa.timestamp("s")),
    ]
)
