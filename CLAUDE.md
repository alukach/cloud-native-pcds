# CLAUDE.md

Context for anyone, human or agent, picking this up. `README.md` explains what
the project does; this file explains what has already been decided, what is
unverified, and which mistakes are easy to make here.

## Status

**A live backfill has run against PCIC, and was still running as this was
written.** An earlier version of this section said nothing had touched the live
API; that was wrong by tens of thousands of requests, and every risk judgement
made from it was made on a false premise. Numbers below are from `logs/` and
`data/_manifest/summary.json` at 2026-09-15 06:10 local and will have moved on.

- 35,653 requests to `services.pacificclimate.org`, 35,647 of them `200`.
  115,658,039 rows across 20 files, 1.056 bytes/row, `obs_time` spanning
  1872-01-01 to 1997-12-31. Periods `1872-1921` through `1998` are on disk.
- **Zero 5xx in 35,653 requests.** The deterministic-500 gotcha below is
  therefore still unconfirmed in production, and with it every code path that
  hangs off it: `ingest.MAX_ATTEMPTS_500`, the retry branch, `quarantined()`
  and `pcds failures`. The named example station `FLNRO-FERN/Endako` did not
  500 here.
- The only 404s are 6 requests for one station, `FLNRO-FERN/WADF2 Burn`, whose
  `native_id` contains a space. Separately, 20 of the 6,977 stations have a
  `native_id` with leading or trailing whitespace (`' OKNGTAPN'`, `'Hourglass '`)
  because the padding strip is applied to lister CSV but not to metadata. 24
  more have legitimate internal spaces and are fetched fine.
- **`scripts/walk.sh` puts ~12 req/s on PCIC, six times the documented
  ceiling.** `RateLimiter` is per interpreter, and the script runs 8 shards at
  `PCDS_MAX_RPS=1.5`. Measured: 224 requests in 149s per shard. The comment at
  the top of walk.sh acknowledges the multiplication, so this is a deliberate
  default rather than an oversight, but it contradicts the Constraints section
  below, which is the part a reader is likely to trust. Decide which one is the
  policy before the next walk.
- 144 tests pass, pyarrow ones included. `tests/test_conformance.py` builds a
  small catalog on disk and runs a real `rashid check` over it.
- `ruff check .` is clean. The lint rule set is pinned explicitly in
  `pyproject.toml`; ruff's implicit default widens between releases and took the
  repo from clean to 56 findings on an upgrade.
- Still first-run-unverified: **everything that talks to S3**, the published
  catalog, and both scheduled workflows. `append` and `compact` have never run
  against real data either; only `backfill` has.

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

Six more came out of a design review. Every one of them is silent, and every
one is in a path that had never actually run:

| Where | What |
|---|---|
| `_setup.yml` | `uv run $PCDS_COMMAND` is unquoted, and bash word-splits an expansion without applying quote removal to it, so a caller writing `--period "${{ ... }}"` shipped the quote characters into argv. `pcds compact --period ""` compacted a period literally named `""`, folded nothing, and kept the deltas: **the quarterly cron had never compacted anything**. The same bug put literal quotes into every published STAC href. Callers must not quote argument values; `tests/test_workflow_args.py` is the guard. |
| `compact.py` | The swap deleted the live partition and then moved its replacement in. `PCDS_ROOT` is the publication prefix, so readers saw the partition empty and then partial, and a crash in the window destroyed a period the deltas could no longer rebuild. New files now carry a generation tag and move in alongside the old ones, `_SUCCESS` commits the swap, and `reconcile_period` settles an interrupted one either way. |
| `walk.sh` | Used `part-*.parquet` as its "already done" marker, which also matches a partition a killed run left half-written, so it would be skipped forever with rows missing and nothing to say so. Now tests for `_SUCCESS`. |
| `state.py` | `record_success` took `max(current, max_obs_time)` with no clamp, so one observation from a station with a bad clock parked the watermark years ahead. Every later append then asked for `time > <future> - window`, got nothing, and left the watermark alone, so the station stopped being mirrored while still counting as a success. |
| `cli.py` | `append` passed `(None, None)` as the window for any station without a watermark, and `fetch_chunked` skips chunking when either bound is None. That made it the unbounded full-station request this file says never to send, for every station on a first run. |
| `opendap.py` | `melt` cast with `safe=False`, which still raises on an unparseable token, and a null `obs_time` failed the cast to the non-nullable published schema. Either one discarded **every other variable's good data for the same window**, every run, until the station quarantined itself. Bad cells are now dropped individually and logged. |

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
| Long/tidy observation schema | 22 networks define 358 variables with no common wide schema. Wide is mostly nulls and needs schema evolution per sensor. Sorted long packs to ~3 bytes/row. |
| Partition by `period`, not year | PCDS spans a handful of manual daily stations in 1872 to ~950 mostly-hourly ones now. Strict yearly gives ~155 partitions from KiB to hundreds of MiB. `pcds layout` merges sparse years to clear a 64 MiB floor. |
| Per-station lister, not the bulk `agg` zip | `agg` aggregates server-side, is not restartable, and took >35s for one network for one day. The lister parallelizes and resumes. |
| Daily append, quarterly compaction | Arrival is ~272k obs/day, ~800 KiB packed. You would wait ~330 days to fill one target-sized file, so freshness and file size are separate schedules. Append stages small deltas; compaction packs them. |
| Plan the layout once, before the walk | Compaction rewrites a period *in place* and never moves a row between periods, so the layout is the only thing that decides time-pruning granularity and nothing repairs it afterwards. Re-planning once data exists moves boundaries under written partitions and orphans them; `pcds layout` warns which ones. Re-plan when the walk is done, and re-backfill the years whose boundaries moved. |
| 30-day trailing re-read | PCDS is explicitly preliminary. Observations get corrected and late data arrives for weeks. Append-only would bake in wrong values. |
| 150k-row row groups | ~450 KiB compressed, a sensible floor for a range request, and it matches Portolan's GeoParquet cap so one fewer thing to explain. |
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
last 30 days, ~272k observations/day. `pcds plan` recomputes all of it and
switches from the metadata estimate to measured bytes/row once the catalog
exists. Re-run it after a season of real data before trusting the cron cadence
in `.github/workflows/`.

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

1. Settle the request-rate policy: either raise the documented ceiling to what
   `walk.sh` actually does, or make the limiter cross-process so 8 shards share
   one budget. The two cannot both stand.
2. Finish the walk. `1872-1998` is on disk; `1999-present` is not.
3. Establish the redistribution right before publishing anything. PCIC's
   disclaimer is what `portolan.py` points `license_id = "other"` at, and a
   disclaimer is not a grant. The originating agencies and the BC Climate
   Related Monitoring Program agreement are the things to check. Add a LICENSE
   file for the code while you are there.
4. Wire the Source Cooperative endpoint and credentials, publish, then run
   `rashid check --live --live-base-url <published base>`, which probes range
   support and CORS. The offline pass is already gated in CI.
5. File the time-partitioning case on
   https://github.com/portolan-sdi/portolan-spec/issues/196 with the sizing math.

Known and not yet addressed, from the same review:

- `portolan.py` and `sql/demo.sql` both tell consumers there is a bloom filter
  on `station_id`. There is not: `pack.py` guards on a `write_bloom_filter`
  kwarg that does not exist in the installed pyarrow (25.0.1), so the guard has
  never fired. Either use pyarrow's real bloom-filter API or drop the claim.
- Temporal extents in the STAC are stamped `Z` while carrying naive local
  standard time, so a client's time search is wrong by 7-8 hours. This is the
  one place the repo localizes silently, which the gotchas below forbid.
- Chunk boundaries are exclusive at both ends (`time<X` then `time>X`), so an
  observation exactly on a boundary is fetched by neither chunk.
- A correction to `None` upstream cannot propagate: nulls are dropped at melt
  time and compaction is insert-only, so the superseded value survives.
- `state.quarantined` promises a weekly full sweep that picks stations back up.
  No such workflow or flag exists, so quarantine is permanent.
- The CI backfill's `compact` job runs bare `pcds compact`, whose auto path
  derives periods from *deltas*. Backfill writes base files, so it finds none,
  logs "nothing to compact", and never merges the 8 shard files. Only
  `walk.sh`, which passes `--period`, actually compacts.
- Backfill shards do not go through `_setup.yml`, so they never enter the
  `pcds-write` concurrency group that serializes every other writer.

Not yet done: climatologies (`lister/climo/...`), and a genuinely atomic
partition swap. The swap is now crash-safe, which is not the same thing: a
reader who catches it mid-flight still sees some rows twice. Pointing the
catalog at an explicit file list, rather than at a glob, is the way out.
