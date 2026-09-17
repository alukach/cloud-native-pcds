-- Why this layout is "cloud optimized": every query below reads kilobytes to a
-- few megabytes over HTTP range requests, not the whole dataset.
--
--   duckdb -init sql/demo.sql
--
-- Swap the ROOT for your published prefix.
INSTALL httpfs; LOAD httpfs;
SET VARIABLE root = 'https://data.source.coop/EXAMPLE/pcds';

CREATE OR REPLACE VIEW stations  AS SELECT * FROM read_parquet(getvariable('root') || '/stations/stations.parquet');
CREATE OR REPLACE VIEW variables AS SELECT * FROM read_parquet(getvariable('root') || '/variables/variables.parquet');
CREATE OR REPLACE VIEW manifest  AS SELECT * FROM read_parquet(getvariable('root') || '/_manifest/files.parquet');
CREATE OR REPLACE VIEW obs       AS SELECT * FROM read_parquet(getvariable('root') || '/observations/period=*/*.parquet', hive_partitioning := true);
CREATE OR REPLACE VIEW layout    AS SELECT * FROM (SELECT unnest(periods, recursive := true) FROM read_json_auto(getvariable('root') || '/layout.json'));
CREATE OR REPLACE VIEW monthly   AS SELECT * FROM read_parquet(getvariable('root') || '/observations_monthly/observations_monthly.parquet');

-- `period` is an opaque partition label, NOT a year. Recent years stand alone
-- (period=2025) but sparse history is bucketed (period=1872-1903), so comparing
-- the label to a year is silently wrong rather than an error:
--
--     WHERE period = '1890'      -> 0 rows, no error; 1890 lives in 1872-1903
--     WHERE period >= '1900'     -> drops 1900-1903, because as strings
--                                   '1872-1903' sorts below '1900'
--
-- Resolve years to labels through layout.json instead. This still prunes: the
-- subquery is pushed into Hive partition pruning, so a range inside one bucket
-- opens one file.
CREATE OR REPLACE MACRO periods_for(lo, hi) AS TABLE
  SELECT period FROM layout WHERE end_year >= lo AND start_year <= hi;

-- Same thing straight from the manifest, which needs no layout.json and takes
-- timestamps rather than years. Prefer this one: `_manifest/files.parquet` is
-- always published, layout.json is not always uploaded alongside it.
CREATE OR REPLACE MACRO periods_between(lo, hi) AS TABLE
  SELECT DISTINCT period FROM manifest WHERE obs_time_max >= lo AND obs_time_min <= hi;

-- Filtering on obs_time alone is correct but prunes nothing: DuckDB cannot
-- relate a string partition key to a timestamp column, and the files are sorted
-- (station_id, variable_id, obs_time), so obs_time is interleaved per station
-- and every row group spans nearly the whole period. Always pair a time filter
-- with periods_for().


-- 1. What is published, without listing the bucket.
--    The same numbers a Portolan client gets from partition:file_count and the
--    collection extent, but with per-file detail.
SELECT period, count(*) AS files, sum(rows) AS rows,
       round(sum(file_bytes) / 1024.0 / 1024, 1) AS mib,
       round(sum(file_bytes)::DOUBLE / sum(rows), 2) AS bytes_per_row
FROM manifest GROUP BY period ORDER BY period;

-- 2. One station, one variable, one year.
--    periods_for() picks the partition; because station_id is the leading sort
--    key its min/max statistics prune exactly, so only a couple of row groups
--    get fetched. (There is no bloom filter, and deliberately so: it cannot beat
--    exact pruning on the leading sort key, and it costs extra round trips.)
SELECT o.obs_time, o.value
FROM obs o
JOIN stations s USING (station_id)
JOIN variables v USING (variable_id)
WHERE s.network_name = 'EC_raw'
  AND s.native_id = '1145M29'          -- Nelson, BC
  AND v.name = 'air_temperature'
  AND o.period IN (SELECT period FROM periods_for(2025, 2025))
  AND o.obs_time >= '2025-01-01' AND o.obs_time < '2026-01-01'
ORDER BY o.obs_time
LIMIT 20;

-- 3. Monthly mean temperature for every station in the Kootenays, 2024.
--    The spatial filter runs against the small GeoParquet; only the matching
--    station_ids are pushed into the observation scan.
WITH kootenay AS (
  SELECT station_id, station_name FROM stations
  WHERE lon BETWEEN -118.5 AND -116.0 AND lat BETWEEN 49.0 AND 50.5
), temp_vars AS (
  SELECT variable_id FROM variables
  WHERE standard_name = 'air_temperature' AND cell_method = 'time: point'
)
SELECT k.station_name, date_trunc('month', o.obs_time) AS month,
       round(avg(o.value), 2) AS mean_c, count(*) AS n
FROM obs o
JOIN kootenay k USING (station_id)
JOIN temp_vars USING (variable_id)
WHERE o.period IN (SELECT period FROM periods_for(2024, 2024))
  AND o.obs_time >= '2024-01-01' AND o.obs_time < '2025-01-01'
GROUP BY 1, 2 ORDER BY 1, 2;

-- 4. The stations collection is real GeoParquet: spatial predicates hit the
--    bbox covering column, not a full scan.
INSTALL spatial; LOAD spatial;
SELECT station_name, network_name, lon, lat
FROM stations
WHERE bbox.xmin > -118.5 AND bbox.xmax < -116.0
  AND bbox.ymin > 49.0   AND bbox.ymax < 50.5
ORDER BY station_name
LIMIT 20;

-- 5. Freshness: what the last append run picked up.
--    NOT `WHERE period = strftime(current_date, '%Y')`. That compares the
--    partition label to a year, which is the exact footgun documented above:
--    the current year usually lives in a multi-year bucket, so it matches no
--    partition and returns zero rows with no error. Resolve through the
--    manifest, which knows which label holds today.
SELECT max(obs_time) AS latest_observation,
       count(DISTINCT station_id) AS stations_reporting
FROM obs WHERE period IN (
  SELECT period FROM periods_between(current_date - INTERVAL 30 DAY, current_date)
);

-- 7. Monthly aggregates: read the rollup, not the raw table.
--    Same answer, ~2% of the archive. The raw table cannot prune below the
--    partition on time, so a month-grain question over `obs` pays for the whole
--    obs_time column of a period: measured, 1,191 range requests and 17 MiB
--    against 28 requests and 1.35 MiB here.
SELECT m.month, round(avg(m.mean), 2) AS mean_c, sum(m.n) AS observations
FROM monthly m
JOIN variables v USING (variable_id)
WHERE v.standard_name = 'air_temperature' AND v.cell_method = 'time: point'
  AND m.month >= DATE '2024-01-01' AND m.month < DATE '2025-01-01'
GROUP BY 1 ORDER BY 1;

-- 8. `mean` composes only weighted. Quantiles do not compose at all and are not
--    in the rollup; go to `obs` for those, naming a station so it stays cheap.
SELECT round(sum(m.mean * m.n) / sum(m.n), 2) AS mean_c
FROM monthly m WHERE m.station_id = 1 AND m.month >= DATE '2024-01-01';

-- 6. How much did that actually read?
-- PRAGMA enable_profiling;
-- SET enable_http_metadata_cache = true;
