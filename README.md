# pcds-parquet

A cloud-optimized Parquet mirror of PCIC's **Provincial Climate Data Set** (BC weather and climate observations from 1872 to now), built to demonstrate what partitioning, sorting and encoding actually buy you when the data lives in object storage.

- **Source**: [Meteorological Data Portal (PCDS)](https://services.pacificclimate.org/met-data-portal-pcds/app/) · [docs](https://services.pacificclimate.org/portal/docs/mdp/root.html)
- **Destination**: Source Cooperative (any S3-compatible endpoint works)
- **Schedule**: GitHub Actions, daily append and quarterly compaction

---

## How it works

The upstream source is an OPeNDAP (Pydap) service that serves one wide CSV per station. Useful, but you cannot ask it "monthly mean temperature for every station in the Kootenays" without pulling every station in full. The Parquet build answers that in a couple of range requests.

Six steps, one CLI command each.

1. **Metadata** (`pcds metadata`, weekly) pulls the five JSON endpoints and writes them as small Parquet tables. Everything downstream joins against these: station ids, which variables exist, who reports what. `stations` and `histories` carry lat/lon and become GeoParquet.

2. **Layout** (`pcds layout`) decides how to slice observations into files. Not by year: 1872 is a handful of manual daily stations and 2024 is ~950 hourly ones, so yearly slices would range from KiB to hundreds of MiB. Instead it groups years into *periods* that each clear a 64 MiB floor, and writes the year to period map to `layout.json`. Sizes come from station metadata, or from measured row counts for the years already written, whichever is available per year. Plan this once, before the backfill: see the warning under [Backfilling the rest of the archive](#backfilling-the-rest-of-the-archive).

3. **Fetch** (`pcds backfill` / `pcds append`) loops over stations, one request per station per time chunk, behind a token bucket capped at 2 req/s. The bucket is per process, so N parallel shards put N times that on PCIC. The response is wide, one column per variable, so `opendap.py` parses it and melts it to the long form `(station_id, variable_id, obs_time, value)`.

   Both entry points clip their range to each station's own `min_obs_time`/`max_obs_time` first. A window a station has no data for still costs a full request that returns a bare header, and over 1870-2026 that is 88% of them.

   `append` starts from each watermark in `_state/` *minus 30 days*, because PCDS observations are explicitly preliminary and get revised for weeks after the fact; append-only would silently bake in wrong values. Watermarks make `append` resumable. They do not make `backfill` resumable: it walks a fixed year range regardless of what has been fetched, so a crashed shard redoes its whole range. Resume granularity for a long walk is the period, via `scripts/walk.sh`.

4. **Write** (`pack.py`) emits Parquet sorted by `(station_id, variable_id, obs_time)`, with dictionary-encoded ids, delta-packed timestamps, zstd, a page index and 150,000-row row groups. The sort is what makes the statistics selective, so a reader after one station skips nearly every row group. Daily appends do not write into the partitions; they drop a small *delta* into `_staging/delta/`, because at ~800 KiB/day you would wait most of a year to fill one properly sized file.

5. **Compact** (`pcds compact`, quarterly) reads a period partition plus the deltas that land in it, sorts and dedupes in DuckDB last-write-wins on `(station_id, variable_id, obs_time)` by the delta's `ingested_at`, and rewrites target-sized files through the same writer. Freshness and file size are on separate schedules precisely because one day of arrivals is nowhere near one good file.

6. **Publish** (`pcds catalog`, `pcds verify`, `pcds portolan`). `catalog` records per-file row counts, byte sizes and station/time ranges into `_manifest/`, so a reader can choose files without a LIST against the bucket. `verify` reads Parquet footers directly to check sort order, duplicate keys and file sizes. `portolan` writes the STAC metadata that turns the tree into a browsable catalog, including the two spec requirements it knowingly breaks.

The result is five published collections (`observations`, `stations`, `histories`, `variables`, `networks`) plus three underscore-prefixed directories that are pipeline machinery rather than data: `_manifest/`, `_state/` and `_staging/`. Paths are defined in `src/pcds/paths.py` and nowhere else.

The modules are written to be read in order: `opendap` → `ingest` → `pack` → `compact` → `portolan`.

---

## What upstream actually offers

Verified against the live service, September 2026.

### Metadata: JSON, fast, cache it

```
GET {base}/api/metadata/{networks|variables|frequencies|stations|histories}?provinces=BC
    base = https://services.pacificclimate.org/met-data-portal-pcds
```

| endpoint      | rows (BC) | notes                                                                   |
| ------------- | --------- | ----------------------------------------------------------------------- |
| `networks`    | 22        | `id`, `name` (e.g. `EC_raw`), `station_count`                           |
| `variables`   | 358       | **`name` is the CSV column label**; `short_name` is not                 |
| `frequencies` | 6         | `daily`, `1-hourly`, `12-hourly`, `15-minute`, `irregular`, null        |
| `stations`    | 6,977     | embeds `histories[]` with lat/lon, freq, obs time range, `variable_ids` |
| `histories`   | 9,632     | flat form, adds `tz_offset`, `sdate`/`edate`, `country`                 |

### Observations: per station, over OPeNDAP

```
{base}/api/data/lister/{raw|climo}/{network}/{native_id}.rsql.{csv|ascii|nc|xls}
    ?station_observations.time>"2026-01-01 00:00:00"
    &station_observations.time<"2027-01-01 00:00:00"
```

The `.rsql` infix names the Pydap handler; without it you get `404 Could not make sense of path`. The response is *wide*:

```
station_observations
wind_direction, air_temperature, ..., time, total_precipitation
225.0, 13.6, ..., 2026-09-08 01:00:00, None
```

Quirks that the parser pins down in [`tests/test_opendap.py`](tests/test_opendap.py): line 1 is the sequence name rather than a header; header cells are space-padded; **`time` is not first or last**, column order is arbitrary; missing values are the literal string `None`; timestamps are naive local standard time (per-station offset lives in `histories.tz_offset`).

Measured: one hourly station with 13 variables, one year → **797 KiB of CSV in 0.7 s**. A full-station request with no time constraint can exceed 45 s, which is why every request is chunked into N-year windows.

### Bulk zip: exists, not used

```
{base}/api/data/pcds/agg/?from-date=&to-date=&network-name=&input-vars=
    &input-freq=&input-polygon=&only-with-climatology=
    &download-timeseries=Timeseries&data-format={csv|nc|ascii|xls}
```

Returns `pcds_data.zip` containing `{NETWORK}/{native_id}.csv` plus `{NETWORK}/variables.csv`. It aggregates server-side and is slow (one network for a single day took over 35 s), and it is not restartable. `agg_url()` is implemented for spot checks; the per-station lister is the workhorse.

---

## Dataset layout

The output is a [Portolan](https://www.portolan-sdi.org/) catalog: STAC 1.1.0 metadata over cloud-native files, no server. The underscore prefix on the machinery directories is what keeps a validator walking `child` links from tripping over them.

```
{root}/
├── catalog.json              STAC root, child links to the five collections
├── layout.json               year -> period map, written by `pcds layout`
├── README.md  AGENTS.md  DEVIATIONS.md
│
├── stations/                 GeoParquet, one point per station
│   ├── stations.parquet
│   └── thumbnail.png
├── histories/                GeoParquet, one point per station configuration
│   ├── histories.parquet
│   └── thumbnail.png
├── variables/                tabular, 358 variable definitions
│   ├── variables.parquet
│   └── frequencies.json
├── networks/                 tabular, 22 contributing agencies
│   └── networks.parquet
├── observations/             tabular, partitioned by period
│   ├── period=1872-1903/part-00000.parquet
│   └── period=2024/part-00000.parquet
│
├── _manifest/                files.parquet, summary.json
├── _state/                   watermarks*.parquet
└── _staging/                 delta/, compact/
```

Every collection directory also carries `collection.json`, `README.md` and `AGENTS.md`, generated from the same facts as the STAC, so only the files that differ between collections are listed above.

`_state/` is a glob because concurrent backfill shards each write their own `watermarks-<shard>.parquet`, which `load` merges with the later watermark winning. `_staging/` is never published and never linked.

### Observation schema: long, not wide

| column        | type           | why                                                                 |
| ------------- | -------------- | ------------------------------------------------------------------- |
| `station_id`  | `int32`        | ~7k distinct; dictionary + RLE collapses to ~nothing once sorted    |
| `variable_id` | `int16`        | 358 distinct; same                                                  |
| `obs_time`    | `timestamp[s]` | `DELTA_BINARY_PACKED`; seconds is exact for PCDS                    |
| `value`       | `float64`      | `PCDS_VALUE_FLOAT32=1` halves it if you accept 7 significant digits |

Long beats wide here because 22 networks report 358 variable definitions with no common schema. A wide table is mostly nulls and needs schema evolution every time a network adds a sensor. Sorted by `(station_id, variable_id, obs_time)`, the long form packs to roughly **3 bytes per row**.

That 3 is the planning figure, and it is deliberately conservative. The 23M rows of pre-1971 data loaded so far measure **0.98 bytes/row**, but those are rounded daily readings that dictionary-encode almost for free. Modern 15-minute sensor floats carry far more entropy in `value` and will pack worse. Treat the modern end as unmeasured until it is loaded.

### Partitioning by *period*, not by year

Strict yearly partitioning gives ~155 partitions ranging from a few hundred KiB to several hundred MiB, and small files are the fastest way to make a "cloud-optimized" dataset slow. A **period** is instead a contiguous run of years chosen so every partition clears a 64 MiB floor: sparse history buckets into decades, recent years stand alone.

### Why the reads are cheap

- **Hive partition pruning** on `period`: a year query never opens other years.
- **Sort order**: a station's rows live in one or two row groups instead of being smeared across all of them, so min/max statistics are actually selective.
- **Bloom filter on `station_id`**: point lookups skip row groups whose ranges overlap.
- **Page index**: read the pages that can match, not whole column chunks.
- **Row groups capped at 150,000 rows**, roughly 450 KiB compressed, which is a sensible floor for a range request and matches the cap Portolan puts on GeoParquet.
- **`_manifest/files.parquet`**: pick files without a LIST against the bucket.

---

## Portolan, deliberately non-conformant

The catalog declares the [Portolan STAC profile](https://github.com/portolan-sdi/portolan-spec) (`v0.2.0`) and follows it except for two requirements, both recorded by stable ID in `DEVIATIONS.md` and in `portolan:deviations` on the affected collection.

| Requirement     | Severity         | Why we break it                                                                                                                                                                                                                                                                                                  |
| --------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PORTO-FMT-018` | MUST (process)   | "The scheme's path structure MUST reflect spatial extent." Observations are partitioned by time. A spatial key would multiply the file count while the partitions are already inside the spec's own 200 MiB to 1 GiB target, and spatial pruning is served by joining through the `stations` collection instead. |
| `PORTO-FMT-034` | MUST (validator) | Tabular collections are specified as a single Parquet file; partitioning is specified only under Vector. A 10 to 15 GB observation table fits neither shape.                                                                                                                                                     |

Both trace to the same gap: Portolan has no shape yet for a large, time-partitioned, non-spatial table. That is the subject of [portolan-spec#196](https://github.com/portolan-sdi/portolan-spec/issues/196), and this dataset is offered as a second concrete case for it.

What does conform, and is worth having regardless:

- `stations` and `histories` are GeoParquet 1.1, Hilbert ordered, with a `bbox` covering column so readers skip row groups from metadata alone.
- `observations` carries the full [partition extension](https://github.com/portolan-sdi/stac-partition-extension): `partition:scheme: hive`, `partition:strategy: temporal`, `partition:keys`, and a `partition:glob` that is the normative bulk-access path.
- `table:columns` on every collection, because a long table of four integer-ish columns is meaningless without the join documented.
- Providers with a contactable host, a `via` link back to PCIC because this is a mirror rather than the source, and an `updated` timestamp per sync.
- `AGENTS.md` at the root and in every collection, covering the things that actually bite: timestamps are not UTC, `variable_id` is network-scoped, the data is preliminary and gets revised.

```bash
uv run pcds portolan \
  --base-url https://data.source.coop/<account>/<repo>/pcds \
  --s3-uri   s3://us-west-2.opendata.source.coop/<account>/<repo>/pcds

uvx rashid check ./data         # 0 errors; see below for why
```

Both deviations are against the spec *text*; neither is visible to the
validator. `rashid check` passes this catalog with zero errors, because the rule
carrying `PORTO-FMT-034` (`PTL-COL-001`) returns early for partitioned
collections. That cuts both ways: rashid's data pass iterates a collection's
declared assets, and `observations` has no `data` asset precisely because of
`PORTO-FMT-034`, so the validator never opens a partition file. A row group
1.5x over the 150,000 cap and a renamed column in one partition both pass it
silently ([rashid#130](https://github.com/portolan-sdi/rashid/issues/130)).
`pcds verify` reads the footers directly and is the gate on those; the planted
violations are pinned in [`tests/test_conformance.py`](tests/test_conformance.py).

Validate with relative hrefs, not a published base URL. Given an absolute https
base, rashid tries to fetch each asset, 404s against a catalog that is not
uploaded yet, and quietly downgrades every byte-level check to an info.

---

## Cadence: how often should the cron run?

`pcds plan` measures this rather than guessing. Against live metadata today:

```
  actively reporting histories : 947
  observations / day           : 271,966
  packed bytes / row           : 3.00
  bytes / day                  : 796.8 KiB
  bytes / year                 : 284.0 MiB
  days to 64 MiB               : 82
  days to 256 MiB              : 329

  append   20 14 * * *     daily 14:20 UTC (06:20 PT); ~797 KiB staged per run
  compact  30 15 1 */3 *   quarterly; folds ~71 MiB of deltas into the period
```

A full year of arrivals lands at ~284 MiB, which is why yearly partitions are the right grain for the modern end of the record.

Re-run `pcds plan` after a season of real data; it switches from the metadata estimate to measured bytes/row from the catalog and will tell you if the cron should move.

---

## Usage

```bash
uv sync
export PCDS_ROOT=./data          # or s3://<bucket>/<prefix>

uv run pcds metadata             # fetch the station catalog
uv run pcds layout               # plan period partitions -> layout.json
uv run pcds plan                 # sizing + cadence report

# Proof of concept: 2020-present. Start and end must be period boundaries;
# `scripts/walk.sh` takes them from layout.json so they always are.
uv run pcds backfill --start-year 2020 --end-year 2026
uv run pcds compact              # fold + dedupe + re-pack
uv run pcds catalog              # per-file stats
uv run pcds verify               # sort order, duplicate keys, file sizes
uv run pcds portolan --base-url https://.../pcds --s3-uri s3://.../pcds

# Day to day.
uv run pcds append               # incremental, staged as a delta
```

`scripts/smoke.sh` runs the whole loop against a dozen stations in a couple of minutes. `scripts/walk.sh` drives the full archive backfill, one period at a time.

### Backfilling the rest of the archive

```bash
./scripts/walk.sh
```

One layout period per run, 8 shards inside it, compaction after each. Restartable: a compacted partition is a finished one, so re-running resumes at the first period without a `part-*.parquet`.

**Batch by period, never by year range.** `backfill` names its output `s<shard>-<NNNNN>.parquet` and resets the index every run, so two runs that both write into a partition overwrite each other's files rather than adding to them. A run covering 1971-1980 writes its 1971 rows into `period=1970-1971` under the same filenames a previous 1800-1970 run used for its 1970 rows, and the 1970 data is gone with no error and nothing in the log. Taking start and end straight from `layout.json` makes that impossible, which is the whole reason the script exists.

Do not re-plan the layout part way through. `pcds layout` is a planning tool for a partitioning that does not exist yet; once data is written, moving a boundary orphans the partition that carried the old label, and compaction will not move rows between periods to repair it. It warns when a new plan would orphan a written partition. Re-plan when the walk is finished, if at all.

If a mid-walk re-plan has already happened, `pcds layout --ignore-catalog` rebuilds the pre-backfill plan from station metadata alone, which is the one that matches partitions written before a catalog existed.

### Configuration

| env var                              | default          |                                                   |
| ------------------------------------ | ---------------- | ------------------------------------------------- |
| `PCDS_ROOT`                          | `./data`         | local path or `s3://bucket/prefix`                |
| `PCDS_S3_ENDPOINT`                   | none             | Source Cooperative / R2 endpoint                  |
| `PCDS_CONCURRENCY`                   | `4`              | parallel station fetches, per process             |
| `PCDS_MAX_RPS`                       | `2.0`            | token bucket against PCIC, per process            |
| `PCDS_TARGET_FILE_BYTES`             | 256 MiB          | roll to a new part file at this size              |
| `PCDS_MIN_FILE_BYTES`                | 64 MiB           | partition size floor for `pcds layout`            |
| `PCDS_ROW_GROUP_ROWS`                | 150,000          | ~450 KiB compressed; matches PORTO-FMT-009        |
| `PCDS_REVISION_WINDOW_DAYS`          | 30               | trailing re-read window                           |
| `PCDS_BACKFILL_CHUNK_YEARS`          | 5                | request window size                               |
| `PCDS_BASE_URL`                      | none             | https base the catalog is served from             |
| `PCDS_PUBLIC_S3_URI`                 | none             | `s3://` equivalent, used for the partition glob   |
| `PCDS_HOST_NAME` / `_URL` / `_EMAIL` | Development Seed | the `host` provider; one of url/email is required |

Credentials come from the standard `AWS_*` environment variables.

### GitHub Actions

| workflow       | trigger         | what it does                                        |
| -------------- | --------------- | --------------------------------------------------- |
| `metadata.yml` | Mondays         | refresh the station catalog                         |
| `append.yml`   | daily 14:20 UTC | incremental pull → delta files                      |
| `compact.yml`  | quarterly       | fold deltas, rebuild manifest, rewrite STAC, verify |
| `backfill.yml` | manual          | 8-shard matrix over a year range                    |

All writing jobs share a `pcds-write` concurrency group: `watermarks.parquet` and the partition swap are whole-object rewrites and must not overlap.

Repository variables: `PCDS_ROOT`, `PCDS_S3_ENDPOINT`, `PCDS_S3_REGION`, `PCDS_BASE_URL`, `PCDS_PUBLIC_S3_URI`. The last two are not optional: `pcds portolan` refuses to publish an https `partition:glob`, which no reader can expand. Secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

---

## Querying it

> **`layout.json` is not in the published bucket yet.** The snippet below is
> correct against a local build, but `read_json_auto(root || '/layout.json')`
> 404s against <https://data.source.coop/alukach/pcds/>: the upload carried the
> seven prefixes and not the plan file beside them. Either push it, or resolve
> years through `_manifest/files.parquet`, which records the years each object
> actually holds and is published. `viewer/index.html` takes the second route.

See [`sql/demo.sql`](sql/demo.sql). The short version:

```sql
INSTALL httpfs; LOAD httpfs;
SET VARIABLE root = 'https://data.source.coop/.../pcds';

CREATE VIEW layout AS SELECT * FROM (SELECT unnest(periods, recursive := true)
                                     FROM read_json_auto(getvariable('root') || '/layout.json'));
CREATE MACRO periods_for(lo, hi) AS TABLE
  SELECT period FROM layout WHERE end_year >= lo AND start_year <= hi;

SELECT o.obs_time, o.value
FROM read_parquet(getvariable('root') || '/observations/period=*/*.parquet',
                  hive_partitioning := true) o
JOIN read_parquet(getvariable('root') || '/stations/stations.parquet') s USING (station_id)
WHERE s.native_id = '1145M29' AND o.variable_id = 542
  AND o.period IN (SELECT period FROM periods_for(2025, 2025))
  AND o.obs_time >= '2025-01-01' AND o.obs_time < '2026-01-01';
```

### `period` is a label, not a year

Read this before writing a time filter. `period` is an opaque partition key:
recent years stand alone (`period=2025`), sparse history is bucketed
(`period=1872-1903`). Comparing it to a year is silently wrong rather than an
error:

| Query                                                        | What you get                                                                                                              |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `WHERE period = '1890'`                                      | **0 rows, no error.** 1890 lives in `period=1872-1903`.                                                                   |
| `WHERE period >= '1900'`                                     | **Drops 1900 to 1903.** As strings, `'1872-1903'` sorts below `'1900'`, so the bucket containing those years is excluded. |
| `WHERE obs_time >= '2024-01-01' AND obs_time < '2025-01-01'` | Correct, but reads **every** partition.                                                                                   |

Resolve years to labels through `layout.json`, as `periods_for()` above does.
The subquery is pushed into Hive partition pruning, so a range inside one bucket
still opens exactly one file. `tests/test_query_pruning.py` pins that.

There is no finer fallback: files are sorted `(station_id, variable_id,
obs_time)`, so `obs_time` is interleaved per station and every row group spans
nearly the whole period. The partition is the time granularity, which is also
why partitions are sized down to a 64 MiB floor rather than merged further.

For programmatic access, `_manifest/files.parquet` is more precise still: it
carries `obs_time_min`/`obs_time_max` and `station_id_min`/`station_id_max` per
file, so you can select files without touching `period` at all. It needs two
statements, because `read_parquet` will not take a subquery as its argument.

---

## The viewer

**<https://alukach.com/cloud-native-pcds/>**, reading the published archive on
Source Cooperative. (The account's Pages custom domain applies to project sites,
so `alukach.github.io/cloud-native-pcds/` redirects here.)

`viewer/index.html` is a single page that reads the archive the way a client on
the internet will: DuckDB-WASM in the browser, querying Parquet over HTTP from a
bucket on a different origin. There is no build step and no server-side query
layer, and nothing is preloaded.

```bash
scripts/serve.py            # bucket on :8788, viewer on :8777
open http://127.0.0.1:8777/viewer/
```

The page picks its bucket from the hostname it is served on: the deployed build
reads <https://data.source.coop/alukach/pcds/>, a local checkout reads
`scripts/serve.py`. Nothing is templated at deploy time, so the file in the
repository is the file that ships. `?base=<url>` overrides either, which is how
you point a local checkout at the published bucket.

`.github/workflows/pages.yml` publishes `viewer/` on every push to `main` that
touches it. It needs Settings -> Pages -> Source set to "GitHub Actions" once;
until that is set the deploy step fails with a 404 on the Pages API.

`scripts/serve.py` is not a static file server. The archive is destined for
Source Cooperative, so it answers the S3 REST API instead: ranged `GET` and
`HEAD`, `ListObjectsV2`, S3-shaped XML errors, `ETag`, and CORS with
`Access-Control-Expose-Headers: Content-Range`. It serves the page from a
*different* port than the objects on purpose, because that is the production
shape and it is the only way the CORS configuration is actually exercised. Point
the page at the real thing with
`?base=https://data.source.coop/<account>/<repo>/` and nothing else changes.

What the page demonstrates is partition pruning, with the arithmetic on screen.
Every query is logged with its SQL, its wall time, the partitions it could reach,
and the bytes it pulled. Asking one station for 1997 reaches 1 of 20 objects and
leaves 90% of the archive untouched; asking about 1872 to 1997 reaches all
twenty, and the log says so.

The byte count is derived, not measured, and it is exact anyway: DuckDB fetches
from inside a worker where Resource Timing does not reach, but it also pulls each
object whole, so a query against partitions the page has not seen transfers
exactly those objects and `_manifest/files.parquet` already carries their sizes.

Year windows resolve to partition labels through `_manifest/files.parquet`
rather than `layout.json`, because the manifest records the years each object
actually holds while layout plans the entire walk, including years nobody has
ingested yet. It is also the only one of the two that is published: see below.

Four panels share one year window and one variable: a station map coloured by
each station's mean, a time series, a monthly climatology, and a SQL console over
the same views (`observations`, `stations`, `histories`, `variables`,
`networks`, `files`). The variable selector groups by `standard_name` and
`cell_method` rather than `variable_id`, because ids are network-scoped; see
above.

### It fetches whole objects, not row groups

DuckDB-WASM buffers each Parquet object in full. Chrome strips the `Range` header
from the probe its worker makes, so it never learns the object is range-readable
and falls back to a whole-object `GET`. Partition pruning still does the real
work, one object instead of twenty, and a partition already pulled answers every
later question for free.

Native DuckDB against the same bucket goes further, down to row groups. For a
single station in `period=1995`, measured against `scripts/serve.py`:

| Reader | Bytes for one station-year | Of a 10.7 MiB object |
| --- | --- | --- |
| DuckDB (httpfs) | 50,601 | 0.5% |
| DuckDB-WASM | 10,717,553 | 100% |

So the browser page is the demo, and a notebook is still the better tool for a
narrow slice of a wide archive.

---

## Known gaps

- **`layout.json` is missing from the published bucket.** Every documented year-to-partition query reads it, and it 404s on Source Cooperative; `_manifest/files.parquet` is the published substitute. Uploading the file is the smaller fix.
- **Climatologies** (`.../lister/climo/...`) are not ingested yet; the lister supports them and they would become a sixth collection.
- **Two Portolan requirements are knowingly unmet.** See `DEVIATIONS.md` in the built catalog, and the table above.
- **Partition swap is not atomic.** Compaction stages to `obs/_staging/`, deletes the old partition, then moves. There is a short window where a reader LISTing the prefix sees a partial partition. The catalog is written last, so catalog-driven readers are safe; a generation-suffixed prefix or an Iceberg table would close it properly. It is also a durability window, not only a visibility one: a crash after the delete and part way through the moves leaves the staged remainder to be wiped by the next run, and those rows have to be re-fetched.
- **Nothing re-partitions written data.** Compaction only ever rewrites a period in place, so a layout change after a backfill cannot be applied to what is already on disk. `pcds layout` warns which partitions a new plan would orphan, but acting on it means re-fetching those years. A `pcds repartition` reading across old period directories is the missing piece.
- **Time zones.** Observations are stored as published, in naive local standard time. `histories.tz_offset` is carried through but not applied.
- **Non-BC records.** Everything is filtered to `provinces=BC`; PCDS also holds some Alberta data, and PCIC runs a separate Yukon/NWT instance on the same API.

## Terms

PCDS data is served by PCIC on an "AS IS" basis, is preliminary and subject to change, and is subject to PCIC's terms of use, the originating agencies' restrictions, and the BC Climate Related Monitoring Program agreement. Any republished copy should carry that notice and credit PCIC.

## Development

```bash
uv sync --extra dev
uv run pytest -q          # includes a real `rashid check` over a built catalog
uv run ruff check .
```

`tests/` uses verbatim response captures in `fixtures/` rather than mocks, so the upstream quirks stay pinned. Nothing in the test suite touches the network.
