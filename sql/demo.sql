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

-- 1. What is published, without listing the bucket.
--    The same numbers a Portolan client gets from partition:file_count and the
--    collection extent, but with per-file detail.
SELECT period, count(*) AS files, sum(rows) AS rows,
       round(sum(file_bytes) / 1024.0 / 1024, 1) AS mib,
       round(sum(file_bytes)::DOUBLE / sum(rows), 2) AS bytes_per_row
FROM manifest GROUP BY period ORDER BY period;

-- 2. One station, one variable, one year.
--    Hive partition pruning picks the period; sorted-by-station row groups plus
--    the station_id bloom filter mean only a couple of row groups get fetched.
SELECT o.obs_time, o.value
FROM obs o
JOIN stations s USING (station_id)
JOIN variables v USING (variable_id)
WHERE s.network_name = 'EC_raw'
  AND s.native_id = '1145M29'          -- Nelson, BC
  AND v.name = 'air_temperature'
  AND o.period = '2025'
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
WHERE o.period = '2024'
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
SELECT max(obs_time) AS latest_observation,
       count(DISTINCT station_id) AS stations_reporting
FROM obs WHERE period = strftime(current_date, '%Y');

-- 6. How much did that actually read?
-- PRAGMA enable_profiling;
-- SET enable_http_metadata_cache = true;
