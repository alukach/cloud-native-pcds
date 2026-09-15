"""Talking to PCIC's Pydap (OPeNDAP) station lister.

URL shape, confirmed against the live service:

    {data_base}/lister/{raw|climo}/{network_name}/{native_id}.rsql.{csv|ascii|nc|xls}

with an optional DAP constraint expression as the query string:

    ?station_observations.time>"2026-01-01 00:00:00"&station_observations.time<"..."

The `.rsql` infix is required -- it names the Pydap handler; dropping it yields
a 404 "Could not make sense of path".

Response body (csv) is *wide* and looks like::

    station_observations
    wind_direction, air_temperature, ..., time, total_precipitation
    225.0, 13.6, None, ..., 2026-09-08 01:00:00, None

Notes that matter for parsing:
  * line 1 is the sequence name, not a header
  * column order is arbitrary -- `time` is not necessarily first or last
  * header cells are space-padded
  * missing values are the literal string `None`
  * column labels are the network-scoped variable *name* (metadata field `name`,
    not `short_name`), so (network_id, name) -> variable_id is the join key
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import quote

TIME_COL = "time"
SEQUENCE = "station_observations"
NULL_TOKENS = ["None", "", "NaN", "nan", "-9999", "-9999.0"]

# Leading whitespace on a field: start of line, or just after a delimiter.
_PAD = re.compile(rb"(?m)(^|,) +")

# Characters that must survive unencoded for Pydap to parse the constraint.
_CE_SAFE = ".<>=!,&_-~"


def _fmt_time(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    return value.strftime("%Y-%m-%d %H:%M:%S")


def time_constraints(
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    *,
    start_inclusive: bool = False,
) -> list[str]:
    """DAP selection clauses bounding `station_observations.time`.

    Pydap exposes only strict `<` / `>`. An inclusive lower bound is emulated by
    stepping back one second. Incremental runs want the *exclusive* form -- data
    strictly after the stored watermark -- so that is the default.
    """
    out: list[str] = []
    if start is not None:
        s = _fmt_time(start)
        if start_inclusive:
            s = _fmt_time(datetime.strptime(s, "%Y-%m-%d %H:%M:%S") - timedelta(seconds=1))
        out.append(f'{SEQUENCE}.{TIME_COL}>"{s}"')
    if end is not None:
        out.append(f'{SEQUENCE}.{TIME_COL}<"{_fmt_time(end)}"')
    return out


def lister_url(
    data_base: str,
    network_name: str,
    native_id: str,
    *,
    kind: str = "raw",
    ext: str = "csv",
    constraints: list[str] | None = None,
) -> str:
    """Build a lister URL. Every column is returned; a mirror wants all of them."""
    if kind not in ("raw", "climo"):
        raise ValueError(f"kind must be raw or climo, got {kind!r}")
    path = (
        f"{data_base.rstrip('/')}/lister/{kind}/"
        f"{quote(network_name, safe='')}/{quote(str(native_id), safe='')}.rsql.{ext}"
    )
    if not constraints:
        return path
    return path + "?" + "&".join(quote(c, safe=_CE_SAFE) for c in constraints)


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ListerCsv:
    columns: list[str]
    body: bytes  # header line + data rows, ready for pyarrow.csv


def strip_sequence_header(data: bytes) -> ListerCsv:
    """Drop the leading sequence-name line and normalize the header.

    Returns the cleaned column names plus a CSV body whose first line is that
    header, so it can be handed straight to a vectorized CSV reader.

    Raises ValueError if the payload is not a lister CSV (e.g. a Pydap error
    document, which is served as text/plain with HTTP 200 in some cases).
    """
    if not data:
        raise ValueError("empty response")
    first, _, rest = data.partition(b"\n")
    if first.strip() != SEQUENCE.encode():
        head = data[:200].decode("utf-8", "replace")
        raise ValueError(f"unexpected lister payload: {head!r}")
    header_line, sep, body = rest.partition(b"\n")
    columns = [c.strip() for c in header_line.decode("utf-8").split(",")]
    if TIME_COL not in columns:
        raise ValueError(f"no {TIME_COL!r} column in {columns!r}")
    clean_header = ",".join(columns).encode()
    # Values are space-padded too, not just the header, and pyarrow's converters
    # reject ' 2026-09-08 01:00:00' outright. Strip padding after each delimiter;
    # the space *inside* a timestamp does not follow a comma, so it survives.
    # ponytail: byte-level strip, safe because Pydap emits no quoted fields; if
    # it ever does, parse with a real CSV dialect instead.
    return ListerCsv(columns=columns, body=clean_header + sep + _PAD.sub(rb"\1", body))


def read_wide(data: bytes):
    """Parse a lister CSV response into a wide Arrow table. Requires pyarrow."""
    import pyarrow as pa
    from pyarrow import csv as pacsv

    parsed = strip_sequence_header(data)
    table = pacsv.read_csv(
        io.BytesIO(parsed.body),
        read_options=pacsv.ReadOptions(use_threads=True),
        parse_options=pacsv.ParseOptions(ignore_empty_lines=True),
        convert_options=pacsv.ConvertOptions(
            null_values=NULL_TOKENS,
            strings_can_be_null=True,
            timestamp_parsers=["%Y-%m-%d %H:%M:%S"],
            column_types={TIME_COL: pa.timestamp("s")},
        ),
    )
    return table


def melt(table, station_id: int, variable_ids: dict[str, int], value_type=None):
    """Wide station table -> long (station_id, variable_id, obs_time, value).

    Columns with no entry in `variable_ids` are dropped and returned to the
    caller. That only happens when the cached metadata is stale relative to the
    data service, which is itself worth knowing about, so it is reported rather
    than swallowed.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    from .schema import OBSERVATIONS, VALUE_TYPE

    value_type = value_type or VALUE_TYPE
    times = table.column(TIME_COL)
    parts: list[pa.Table] = []
    unknown: list[str] = []
    for name in table.schema.names:
        if name == TIME_COL:
            continue
        vid = variable_ids.get(name)
        if vid is None:
            unknown.append(name)
            continue
        values = table.column(name)
        mask = pc.is_valid(values)
        n = pc.sum(mask).as_py() or 0
        if n == 0:
            continue
        parts.append(
            pa.table(
                {
                    "station_id": pa.array([station_id] * n, pa.int32()),
                    "variable_id": pa.array([vid] * n, pa.int16()),
                    "obs_time": pc.filter(times, mask).cast(pa.timestamp("s")),
                    "value": pc.filter(values, mask).cast(value_type, safe=False),
                }
            )
        )
    if not parts:
        return pa.table([[], [], [], []], schema=OBSERVATIONS), unknown
    return pa.concat_tables(parts).cast(OBSERVATIONS), unknown
