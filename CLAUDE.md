# CLAUDE.md

Context for anyone, human or agent, picking this up. `README.md` explains what
the project does; this file explains what has already been decided, what is
unverified, and which mistakes are easy to make here.

## Status

The offline paths now run and are gated in CI. Nothing has yet touched the live
API or an S3 endpoint, so treat a smoke-test failure as informative rather than
surprising.

- 100 tests pass, pyarrow ones included. `tests/test_conformance.py` builds a
  small catalog on disk and runs a real `rashid check` over it.
- `ruff check .` is clean. The lint rule set is pinned explicitly in
  `pyproject.toml`; ruff's implicit default widens between releases and took the
  repo from clean to 56 findings on an upgrade.
- Still first-run-unverified: everything that talks to PCIC or to S3, and the
  DuckDB compaction SQL against real volumes.

Six bugs were found and fixed getting there. The first three made the pipeline
unrunnable; the last three lose data quietly, which is worse:

| Where | What |
|---|---|
| `compact.py` | `paths = writer.close()` shadowed the `paths` module for the whole function, so `compact_period` raised `UnboundLocalError` on every call. Nothing had ever compacted. |
| `opendap.py` | Lister *values* are space-padded, not just header cells. pyarrow rejected `' 2026-09-08 01:00:00'`, so every ingest failed against the pinned fixture. |
| `cli.py` | `pcds backfill` had the same `paths` shadowing, through a closure: `flush()` captured it as a free variable, so every run died on the first station that returned rows. Ruff's F823 catches the straight-line case but not this one, so `tests/test_no_shadowed_imports.py` walks the AST for both. |
| `cli.py` | `pcds portolan --base-url` without `--s3-uri` published an https `partition:glob`, which cannot be expanded. Now a hard error. |
| `cli.py` | `compact --period X` folded X and then deleted **every** staged delta, losing rows for every other period unrecoverably, since the watermarks had already advanced past them. A scoped run now keeps the deltas. |
| `state.py` | The 8 backfill shards each rewrote the whole of `watermarks.parquet` and shared one `.tmp` path, so shards silently overwrote each other and could publish a torn file. Concurrent writers now write `watermarks-<shard>.parquet` and `load` merges, later watermark winning. |

First thing to do:

```bash
uv sync --extra dev
uv run pytest -q
uv run ruff check .
scripts/smoke.sh          # ~12 stations, a few dozen requests, writes ./data
```

## Decisions already made

Do not relitigate these without a reason; each was argued through once.

| Decision | Why |
|---|---|
| Long/tidy observation schema | 22 networks define 358 variables with no common wide schema. Wide is mostly nulls and needs schema evolution per sensor. Sorted long packs to ~1.1 bytes/row measured, against the ~3 this row used to claim. |
| Partition by `period`, not year | PCDS spans a handful of manual daily stations in 1872 to ~950 mostly-hourly ones now. Strict yearly gives ~155 partitions from KiB to hundreds of MiB. `pcds layout` merges sparse years to clear a **128 MiB** floor, the low end of the conventional 128 MiB - 1 GiB range. Ten periods of 129-242 MiB cover 1872-2026. The floor is the only cut rule; a `max_span_years=50` cap used to fire first over the sparse head and emit a 2.5 MiB partition, which is what the floor exists to prevent. Low end deliberately: the partition is the only time-pruning granularity, so every doubling doubles what a one-year query reads. |
| Per-station lister, not the bulk `agg` zip | `agg` aggregates server-side, is not restartable, and took >35s for one network for one day. The lister parallelizes and resumes. |
| Daily append, quarterly compaction | Arrival is ~218k obs/day, ~225 KiB packed. You would wait ~1,166 days to fill one target-sized file, so freshness and file size are separate schedules. Append stages small deltas; compaction packs them. Quarterly is also a floor and not only a sizing choice: a staged delta is not published, so the compaction interval is the real publication lag. |
| Plan the layout once, before the walk | Compaction rewrites a period *in place* and never moves a row between periods, so the layout is the only thing that decides time-pruning granularity. Re-planning once data exists still moves boundaries under written partitions and orphans them; `pcds layout` warns which ones. But re-backfilling is now the fallback, not the only option: when the floor goes **up**, old periods merge without splitting, every orphan nests whole inside one new period, and `scripts/repartition.py` moves the rows locally with nothing re-fetched. It refuses when a written period straddles two new ones, which does need those years re-fetched. |
| 30-day trailing re-read | PCDS is explicitly preliminary. Observations get corrected and late data arrives for weeks. Append-only would bake in wrong values. |
| 150k-row row groups | Matches Portolan's GeoParquet cap so one fewer thing to explain. The "~450 KiB compressed" that justified it assumed 3 bytes/row; measured, it is 1.06-1.4, so a row group is ~160-210 KiB. Smaller than ideal for a range request. Not yet changed, since the cap is the other half of the reason. |
| Portolan, knowingly non-conformant | Two MUSTs cannot be met by a large time-partitioned non-spatial table. See `DEVIATIONS.md` in built output and `src/pcds/portolan.py::DEVIATIONS`. |
| Hand-rolled GeoParquet and PNG | 7k points does not justify geopandas; a scatter plot does not justify matplotlib in CI. Both are ~100 lines and tested. |

## Upstream gotchas

Every one of these cost real time to find. They are pinned in
`tests/test_opendap.py` against verbatim captures in `fixtures/`.

- The lister URL needs the **`.rsql` infix**: `{native_id}.rsql.csv`. Without it,
  `404 Could not make sense of path`.
- CSV column labels are the variables API's **`name`**, not `short_name`. Using
  `short_name` silently maps nothing and yields empty results.
- Response line 1 is the sequence name `station_observations`, not a header.
- Header cells **and values** are space-padded, and **`time` is at an arbitrary
  column index**, not first or last. `strip_sequence_header` strips padding after
  every delimiter; pyarrow's timestamp converter rejects a leading space outright.
- Missing values are the literal string `None`.
- Timestamps are **naive local standard time**, not UTC. `histories.tz_offset`
  has the offset where upstream knows it, which is not everywhere. Do not
  localize silently.
- The `frequencies` endpoint returns a **literal `null`** in its list, for
  histories whose reporting frequency upstream does not know. It is not a
  frequency code; `metadata.write` drops it before sorting.
- `variable_id` is **network-scoped**. Air temperature has a different id in each
  of the 22 networks. Select on `standard_name` + `cell_method`.
- `period` is an **opaque partition label, not a year**, and comparing it to one
  fails silently rather than loudly: `period = '1890'` returns zero rows, and
  `period >= '1900'` drops 1900 to 1903 because `'1872-1903'` sorts below
  `'1900'` as a string. Resolve years through `layout.json`. Filtering on
  `obs_time` alone is correct but prunes nothing, and the
  `(station_id, variable_id, obs_time)` sort leaves `obs_time` interleaved, so
  row-group statistics do not rescue it: the partition is the time granularity.
  Every documented snippet is executed by `tests/test_query_pruning.py`.
- Some stations **500 on every lister request**, deterministically and in about
  0.1s. `FLNRO-FERN/Endako` is one: it fails with no query string at all and
  fails on `.rsql.dds`, the schema descriptor, so it breaks before any data is
  serialized and no request shape avoids it. A bogus station id returns a clean
  404 instead, and its neighbours in the same network return 200. Treat a 500 as
  a broken station, not a busy server: `ingest.MAX_ATTEMPTS_500` allows one
  retry, and `pcds failures` lists what is stuck.
- A full-station request with no time constraint can exceed 45s. Always chunk.
- A window a station has no data for still costs a full request: the response
  is the sequence name and a header row, nothing else. Selecting stations on
  their overall span is not enough, because the chunker then walks the whole
  requested range anyway. `ingest.clip_window` narrows the range to each
  station's own `min_obs_time`/`max_obs_time` first; without it, 88% of a
  1870-2026 walk (190k of 215k requests) returns a bare header.

## Constraints

PCIC runs this on modest academic infrastructure and the entire point of
mirroring to Parquet is that nobody has to hammer it again. `PCDS_MAX_RPS`
defaults to 2 and `PCDS_CONCURRENCY` to 4; the backfill workflow drops to 1.5
and 3 across 8 shards. Do not raise these to make a backfill finish sooner.

## Where the numbers came from

Measured against the live metadata API in September 2026: 6,977 BC stations,
9,632 histories, 22 networks, 358 variables, ~947 histories reporting within the
last 30 days, ~272k observations/day.

Measured against the **written catalog**, which is the set that matters for
sizing and was guessed at until now: **1.06 bytes/row** over 115.7M rows for
1872-1997, and **1.36-1.39** for 2020 and 2021. `DEFAULT_BYTES_PER_ROW` was 3.0.
Separately, `estimated_year_rows` overpredicts row counts by 1.78x for 2020 and
1.90x for 2021 (and 2.6-15x over the sparse years), because it assumes every
variable on a history reports at that history's frequency. `ESTIMATE_CALIBRATION`
divides it back out, calibrated on the modern regime and honest only there.
Together those two were a ~4x byte overestimate and are why the first layout
asked for a 64 MiB floor and produced files of 2.5 to 13 MiB.

`pcds plan` recomputes all of it and switches from the metadata estimate to
measured bytes/row once the catalog exists. Re-run it after a season of real
data before trusting the cron cadence in `.github/workflows/`.

## Conventions

- Prose in docs and comments: no em-dashes.
- **`README.md` ships with the change that makes it wrong.** It is the only
  document a reader outside this repo sees, and its commands get copied and run
  without checking whether they still hold. It taught a 5-year-range backfill
  loop that the per-run writer names make destructive, and described `backfill`
  as resuming from watermarks it has never read. Someone followed it. A
  stale README is not untidy, it is a recipe that loses data. When behaviour,
  a default, a flag or a measured number changes, fix the README in the same
  commit, and check the claims either side of the line you are editing while
  you are there.
- Comments explain *why*, not *what*. The modules are written to be read in
  order: `opendap` → `ingest` → `pack` → `compact` → `portolan`.
- Paths live in `src/pcds/paths.py`. Do not hardcode prefixes anywhere else.
- New Portolan deviations go in `src/pcds/portolan.py::DEVIATIONS` with a stable
  requirement ID, never as a silent omission.

## Next

1. Get the smoke test green against the live service.
2. Backfill 2020-present, then walk backwards in 5-year increments.
3. Wire the Source Cooperative endpoint and credentials, publish, then run
   `rashid check --live --live-base-url <published base>`, which probes range
   support and CORS. The offline pass is already gated in CI.
4. File the time-partitioning case on
   https://github.com/portolan-sdi/portolan-spec/issues/196 with the sizing math.

Not yet done: climatologies (`lister/climo/...`), an atomic partition swap, and
a LICENSE file.
