# pcds-parquet

A cloud-optimized Parquet mirror of PCIC's **Provincial Climate Data Set** (BC weather and climate observations from 1872 to now), built to demonstrate what partitioning, sorting and encoding actually buy you when the data lives in object storage.

The upstream source is an OPeNDAP (Pydap) service that serves one wide CSV per station. Useful, but you cannot ask it "monthly mean temperature for every station in the Kootenays" without pulling every station in full. The Parquet build answers that in a couple of range requests.

- **Source**: [Meteorological Data Portal (PCDS)](https://services.pacificclimate.org/met-data-portal-pcds/app/) · [docs](https://services.pacificclimate.org/portal/docs/mdp/root.html)
- **Destination**: Source Cooperative (any S3-compatible endpoint works)
- **Schedule**: GitHub Actions, daily append and quarterly compaction

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

The output is a [Portolan](https://www.portolan-sdi.org/) catalog: STAC 1.1.0 metadata over cloud-native files, no server. Collections sit one level below the root; pipeline machinery lives under underscore-prefixed directories so a validator walking `child` links never trips over it.

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

The three underscore-prefixed directories are pipeline machinery rather than published data. `_manifest/` holds per-file row counts, byte sizes and station/time ranges so a reader can choose files without a LIST against the bucket. `_state/` holds per-station ingest watermarks; concurrent backfill shards each write their own `watermarks-<shard>.parquet`, which is why it is a glob. `_staging/` holds delta and compaction scratch and is never published or linked.

### Observation schema: long, not wide

| column        | type           | why                                                                 |
| ------------- | -------------- | ------------------------------------------------------------------- |
| `station_id`  | `int32`        | ~7k distinct; dictionary + RLE collapses to ~nothing once sorted    |
| `variable_id` | `int16`        | 358 distinct; same                                                  |
| `obs_time`    | `timestamp[s]` | `DELTA_BINARY_PACKED`; seconds is exact for PCDS                    |
| `value`       | `float64`      | `PCDS_VALUE_FLOAT32=1` halves it if you accept 7 significant digits |

Long beats wide here because 22 networks report 358 variable definitions with no common schema. A wide table is mostly nulls and needs schema evolution every time a network adds a sensor. Sorted by `(station_id, variable_id, obs_time)`, the long form packs to roughly **3 bytes per row**.

### Partitioning by *period*, not by year

PCDS is wildly non-uniform in time: 1872–1950 is a handful of manual daily stations, 2010–present is ~950 mostly-hourly automatic ones. Strict yearly partitioning gives ~155 partitions ranging from a few hundred KiB to several hundred MiB. Small files are the fastest way to make a "cloud-optimized" dataset slow.

So the partition key is a **period**: a contiguous run of years chosen so every partition clears a 64 MiB floor. `pcds layout` computes it and writes `layout.json`; sparse history buckets into decades, recent years stand alone.

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

The important result: **appending and packing are different problems.** At ~800 KiB/day you would wait most of a year to accumulate one target-sized file, which is unacceptable for freshness. So the append runs daily and writes a small *delta* file, and compaction folds deltas into the period partition on a schedule chosen purely for file size. A full year of arrivals lands at ~284 MiB, which is why yearly partitions are the right grain for the modern end of the record.

Re-run `pcds plan` after a season of real data; it switches from the metadata estimate to measured bytes/row from the catalog and will tell you if the cron should move.

### Revisions are not optional

PCDS is explicitly preliminary: observations get corrected and late data arrives for weeks. Append-only would silently bake in wrong values. Every incremental run re-reads a trailing **30-day revision window** from each station's watermark, and compaction resolves duplicates last-write-wins on `(station_id, variable_id, obs_time)` using the delta's `ingested_at`.

---

## Usage

```bash
uv sync
export PCDS_ROOT=./data          # or s3://<bucket>/<prefix>

uv run pcds metadata             # fetch the station catalog
uv run pcds layout               # plan period partitions -> layout.json
uv run pcds plan                 # sizing + cadence report

# Proof of concept: 2020-present.
uv run pcds backfill --start-year 2020 --end-year 2026 --shard 0/8
uv run pcds compact              # fold + dedupe + re-pack
uv run pcds catalog              # per-file stats
uv run pcds verify               # sort order, duplicate keys, file sizes
uv run pcds portolan --base-url https://.../pcds --s3-uri s3://.../pcds

# Day to day.
uv run pcds append               # incremental, staged as a delta
```

`scripts/smoke.sh` runs the whole loop against a dozen stations in a couple of minutes.

### Backfilling the rest of the archive

Walk backwards in 5-year increments, one `workflow_dispatch` at a time, then re-plan the layout so the sparse early decades merge into larger partitions:

```bash
for y in 2015 2010 2005 2000 1995 1990 1985 1980 1975 1970; do
  gh workflow run backfill.yml -f start_year=$y -f end_year=$((y+4))
done
uv run pcds layout && uv run pcds compact
```

### Configuration

| env var                              | default          |                                                   |
| ------------------------------------ | ---------------- | ------------------------------------------------- |
| `PCDS_ROOT`                          | `./data`         | local path or `s3://bucket/prefix`                |
| `PCDS_S3_ENDPOINT`                   | none             | Source Cooperative / R2 endpoint                  |
| `PCDS_CONCURRENCY`                   | `4`              | parallel station fetches                          |
| `PCDS_MAX_RPS`                       | `2.0`            | shared token bucket against PCIC                  |
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

See [`sql/demo.sql`](sql/demo.sql). The short version:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT o.obs_time, o.value
FROM read_parquet('https://data.source.coop/.../observations/period=*/*.parquet',
                  hive_partitioning := true) o
JOIN read_parquet('.../stations/stations.parquet') s USING (station_id)
WHERE s.native_id = '1145M29' AND o.variable_id = 542 AND o.period = '2025';
```

---

## Known gaps

- **Climatologies** (`.../lister/climo/...`) are not ingested yet; the lister supports them and they would become a sixth collection.
- **Two Portolan requirements are knowingly unmet.** See `DEVIATIONS.md` in the built catalog, and the table above.
- **Partition swap is not atomic.** Compaction stages to `obs/_staging/`, deletes the old partition, then moves. There is a short window where a reader LISTing the prefix sees a partial partition. The catalog is written last, so catalog-driven readers are safe; a generation-suffixed prefix or an Iceberg table would close it properly.
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
