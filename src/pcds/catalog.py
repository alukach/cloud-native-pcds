"""File-level catalog.

A client that wants "station 1234, air temperature, 2021-2023" should not have to
LIST the bucket to find out which files can possibly contain it. The catalog is
one small Parquet file holding per-file row counts, byte sizes, and min/max for
station_id and obs_time -- read once, then fetch only the files that can match.

It is written from Parquet footers, so it costs one metadata read per file and
never re-scans the data.
"""

from __future__ import annotations

import datetime as dt
import json

import pyarrow as pa
import pyarrow.parquet as pq

from . import paths
from .schema import CATALOG
from .storage import Store


def _stat(md, col_index: int, key: str):
    lo = hi = None
    for rg in range(md.num_row_groups):
        st = md.row_group(rg).column(col_index).statistics
        if st is None or not st.has_min_max:
            continue
        lo = st.min if lo is None else min(lo, st.min)
        hi = st.max if hi is None else max(hi, st.max)
    return lo, hi


def build(store: Store) -> pa.Table:
    rows = []
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None, microsecond=0)
    for info in store.ls(paths.OBSERVATIONS, recursive=True):
        if not info.path.endswith(".parquet"):
            continue
        rel = info.path[len(store.root) :].lstrip("/")
        period = next(
            (seg.split("=", 1)[1] for seg in rel.split("/") if seg.startswith("period=")), ""
        )
        with store.fs.open_input_file(info.path) as f:
            md = pq.ParquetFile(f).metadata
            names = list(md.schema.names)
            s_lo, s_hi = _stat(md, names.index("station_id"), "station_id")
            t_lo, t_hi = _stat(md, names.index("obs_time"), "obs_time")
        rows.append(
            {
                "path": rel,
                "period": period,
                "rows": md.num_rows,
                "file_bytes": info.size,
                "row_groups": md.num_row_groups,
                "station_id_min": s_lo,
                "station_id_max": s_hi,
                "obs_time_min": t_lo,
                "obs_time_max": t_hi,
                "written_at": now,
            }
        )
    rows.sort(key=lambda r: r["path"])
    return pa.Table.from_pylist(rows, schema=CATALOG)


def write(store: Store, table: pa.Table) -> dict:
    store.mkdirs(paths.MANIFEST)
    with store.fs.open_output_stream(store.join(paths.MANIFEST_FILE)) as sink:
        pq.write_table(table, sink, compression="zstd", compression_level=9)
    summary = summarize(table)
    with store.fs.open_output_stream(store.join(paths.SUMMARY_FILE)) as sink:
        sink.write(json.dumps(summary, indent=2, default=str).encode())
    return summary


def summarize(table: pa.Table) -> dict:
    if table.num_rows == 0:
        return {"files": 0, "rows": 0, "bytes": 0}
    d = table.to_pydict()
    rows = sum(d["rows"])
    nbytes = sum(d["file_bytes"])
    by_period: dict[str, dict] = {}
    for i, period in enumerate(d["period"]):
        e = by_period.setdefault(period, {"files": 0, "rows": 0, "bytes": 0})
        e["files"] += 1
        e["rows"] += d["rows"][i]
        e["bytes"] += d["file_bytes"][i]
    return {
        "files": table.num_rows,
        "rows": rows,
        "bytes": nbytes,
        "bytes_per_row": round(nbytes / rows, 3) if rows else None,
        "obs_time_min": min(x for x in d["obs_time_min"] if x is not None),
        "obs_time_max": max(x for x in d["obs_time_max"] if x is not None),
        "periods": dict(sorted(by_period.items())),
    }


def load(store: Store) -> pa.Table:
    path = store.join(paths.MANIFEST_FILE)
    with store.fs.open_input_file(path) as f:
        return pq.read_table(f)
