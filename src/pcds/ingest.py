"""The fetch loop: PCDS OPeNDAP -> long Arrow, politely and restartably.

Two entry points share the machinery:

* ``backfill`` walks a closed time range for a set of stations, chunked into
  N-year requests so a failure costs one chunk rather than one station-century.
* ``increment`` walks forward from each station's stored watermark, minus a
  trailing revision window, because PCDS data is explicitly preliminary and gets
  revised for weeks after the fact.
"""

from __future__ import annotations

import datetime as dt
import logging
import random
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import httpx
import pyarrow as pa

from .config import Settings
from .opendap import lister_url, melt, read_wide, strip_sequence_header, time_constraints
from .schema import OBSERVATIONS

log = logging.getLogger("pcds.ingest")


@dataclass(frozen=True)
class Target:
    station_id: int
    network_id: int
    network_name: str
    native_id: str


@dataclass
class FetchResult:
    target: Target
    table: pa.Table
    unknown_columns: list[str]
    requests: int
    bytes_in: int
    error: str | None = None


class RateLimiter:
    """Shared token bucket. PCIC is a small academic shop; the whole point of
    mirroring to Parquet is that nobody has to hammer this service again."""

    def __init__(self, max_rps: float):
        self.interval = 1.0 / max_rps if max_rps > 0 else 0.0
        self._lock = threading.Lock()
        self._next = 0.0

    def acquire(self) -> None:
        if self.interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next - now)
            self._next = max(now, self._next) + self.interval
        if wait:
            time.sleep(wait)


RETRY_STATUS = {429, 500, 502, 503, 504}

# A lister 500 is reproducible, not transient: some stations fail every request,
# the .dds schema descriptor included, so no amount of waiting fixes them. One
# retry covers a genuine blip; more just hammers PCIC and stretches the run.
# The station lands in watermarks.last_error either way.
MAX_ATTEMPTS_500 = 2


def _get_with_retry(
    client: httpx.Client, url: str, settings: Settings, limiter: RateLimiter
) -> bytes:
    last: Exception | None = None
    for attempt in range(settings.max_retries):
        limiter.acquire()
        try:
            r = client.get(url, timeout=settings.request_timeout_s)
            if r.status_code in RETRY_STATUS:
                raise httpx.HTTPStatusError(
                    f"HTTP {r.status_code}", request=r.request, response=r
                )
            r.raise_for_status()
            return r.content
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
            if status is not None and status not in RETRY_STATUS:
                raise
            last = exc
            limit = MAX_ATTEMPTS_500 if status == 500 else settings.max_retries
            log.warning("attempt %d/%d failed for %s: %s", attempt + 1, limit, url, exc)
            if attempt + 1 >= limit:
                break
            time.sleep(min(60.0, 2.0**attempt) * (0.5 + random.random()))
    raise RuntimeError(f"exhausted retries for {url}") from last


def chunk_ranges(
    start: dt.datetime, end: dt.datetime, chunk_years: int
) -> Iterator[tuple[dt.datetime, dt.datetime]]:
    """Closed-open [lo, hi) windows aligned to calendar years."""
    lo = start
    while lo < end:
        hi_year = ((lo.year // chunk_years) + 1) * chunk_years
        hi = min(end, dt.datetime(hi_year, 1, 1))
        if hi <= lo:  # chunk_years == 0 guard
            hi = end
        yield lo, hi
        lo = hi


def clip_window(
    lo: dt.datetime,
    hi: dt.datetime,
    min_obs: dt.datetime | None,
    max_obs: dt.datetime | None,
) -> tuple[dt.datetime, dt.datetime]:
    """Narrow a requested range to the span a station actually reports over.

    Selecting a station because its overall span overlaps the range is not the
    same as every chunk of that range having data in it. A station that started
    reporting in 2000, backfilled over 1870-2026, otherwise costs 26 requests
    that return a bare header before the 6 that return rows. Measured over the
    real metadata, 88% of a full-archive walk is such requests.

    A non-overlapping span collapses to an empty range, which `chunk_ranges`
    turns into zero windows, so the station costs no requests at all.
    """
    if min_obs is None or max_obs is None:
        return (lo, hi)
    # `time_constraints` closes the range with a strict `<`, so step one second
    # past the last observation to keep it.
    return (max(lo, min_obs), min(hi, max_obs + dt.timedelta(seconds=1)))


def fetch_window(
    client: httpx.Client,
    settings: Settings,
    limiter: RateLimiter,
    target: Target,
    variable_ids: dict[str, int],
    start: dt.datetime | None,
    end: dt.datetime | None,
    *,
    start_inclusive: bool = False,
) -> FetchResult:
    url = lister_url(
        settings.data_base,
        target.network_name,
        target.native_id,
        constraints=time_constraints(start, end, start_inclusive=start_inclusive),
    )
    try:
        payload = _get_with_retry(client, url, settings, limiter)
    except Exception as exc:  # noqa: BLE001 - recorded per station, run continues
        return FetchResult(target, _empty(), [], 1, 0, error=f"{type(exc).__name__}: {exc}")
    try:
        # A station with no data in the window returns just the two header lines.
        parsed = strip_sequence_header(payload)
        if not parsed.body.split(b"\n", 1)[1].strip():
            return FetchResult(target, _empty(), [], 1, len(payload))
        wide = read_wide(payload)
        long_table, unknown = melt(wide, target.station_id, variable_ids)
    except Exception as exc:  # noqa: BLE001
        return FetchResult(target, _empty(), [], 1, len(payload), error=f"parse: {exc}")
    return FetchResult(target, long_table, unknown, 1, len(payload))


def _empty() -> pa.Table:
    return pa.table([[], [], [], []], schema=OBSERVATIONS)


def fetch_station(
    client: httpx.Client,
    settings: Settings,
    limiter: RateLimiter,
    target: Target,
    variable_ids: dict[str, int],
    start: dt.datetime | None,
    end: dt.datetime | None,
    *,
    chunk_years: int | None = None,
    start_inclusive: bool = False,
) -> FetchResult:
    """Fetch a station over [start, end), chunked to keep single requests small."""
    chunk_years = chunk_years if chunk_years is not None else settings.backfill_chunk_years
    if start is None or end is None or chunk_years <= 0:
        return fetch_window(
            client, settings, limiter, target, variable_ids, start, end,
            start_inclusive=start_inclusive,
        )
    tables: list[pa.Table] = []
    unknown: set[str] = set()
    requests = 0
    nbytes = 0
    errors: list[str] = []
    first = True
    for lo, hi in chunk_ranges(start, end, chunk_years):
        res = fetch_window(
            client, settings, limiter, target, variable_ids, lo, hi,
            start_inclusive=start_inclusive and first,
        )
        first = False
        requests += res.requests
        nbytes += res.bytes_in
        unknown.update(res.unknown_columns)
        if res.error:
            errors.append(f"[{lo:%Y-%m-%d}..{hi:%Y-%m-%d}] {res.error}")
            continue
        if res.table.num_rows:
            tables.append(res.table)
    table = pa.concat_tables(tables) if tables else _empty()
    return FetchResult(
        target, table, sorted(unknown), requests, nbytes,
        error="; ".join(errors) if errors else None,
    )


def fetch_many(
    settings: Settings,
    targets: list[Target],
    variable_lookup: dict[int, dict[str, int]],
    window: Callable[[Target], tuple],
) -> Iterator[FetchResult]:
    """Run `fetch_station` across a thread pool, yielding results as they land.

    `window(target) -> (start, end, start_inclusive)` lets the caller express
    both backfill (fixed range) and increment (per-station watermark).
    """
    limiter = RateLimiter(settings.max_rps)
    limits = httpx.Limits(
        max_connections=settings.concurrency, max_keepalive_connections=settings.concurrency
    )
    with httpx.Client(
        headers=settings.extra_headers, follow_redirects=True, limits=limits
    ) as client:
        with ThreadPoolExecutor(max_workers=settings.concurrency) as pool:
            futures = {}
            for t in targets:
                start, end, inclusive = window(t)
                futures[
                    pool.submit(
                        fetch_station,
                        client,
                        settings,
                        limiter,
                        t,
                        variable_lookup.get(t.network_id, {}),
                        start,
                        end,
                        start_inclusive=inclusive,
                    )
                ] = t
            for fut in as_completed(futures):
                yield fut.result()
