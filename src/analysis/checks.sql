-- src/analysis/checks.sql — verification queries for the "Done when" checks and the evidence pack.
-- Set the environment once: replace dev_ with prd_ (or use the SQL editor's find/replace).

-- Task 1.2 — schemas, volume, entitlements
SHOW SCHEMAS IN workspace LIKE 'dev_*';
SHOW VOLUMES IN workspace.dev_landing;
SELECT * FROM workspace.dev_ops.entitlements;

-- Task 1.3 — landed files and manifest
LIST '/Volumes/workspace/dev_landing/raw/tripdata_csv/';
SELECT * FROM workspace.dev_ops.file_manifest ORDER BY period, unpacked_file;

-- Task 2.1 — Bronze rows per file vs manifest (E2 building block)
SELECT m.unpacked_file, m.expected_rows, b.bronze_rows, m.expected_rows = b.bronze_rows AS matches
FROM workspace.dev_ops.file_manifest m
LEFT JOIN (SELECT _source_file, count(*) AS bronze_rows FROM workspace.dev_lakehouse.bronze_trips GROUP BY 1) b
  ON b._source_file = m.unpacked_file
ORDER BY 1;

-- Task 2.2 — profile Bronze before choosing thresholds (paste results into README)
SELECT count(*) AS rows,
       count_if(ride_id IS NULL) AS null_ride_id,
       count_if(start_station_id IS NULL) AS null_start_station,
       count_if(end_station_id IS NULL) AS null_end_station,
       count_if(end_lat IS NULL) AS null_end_coords,
       count(DISTINCT member_casual) AS member_values,
       count(DISTINCT rideable_type) AS bike_values,
       min(started_at), max(started_at)
FROM workspace.dev_lakehouse.bronze_trips;
SELECT rideable_type, member_casual, count(*) FROM workspace.dev_lakehouse.bronze_trips GROUP BY ALL;

-- Task 2.2 — clean + quarantine = bronze, per file
WITH b AS (SELECT _source_file, count(*) n FROM workspace.dev_lakehouse.bronze_trips GROUP BY 1),
     c AS (SELECT _source_file, count(*) n FROM workspace.dev_lakehouse.silver_trips_clean GROUP BY 1),
     q AS (SELECT _source_file, count(*) n FROM workspace.dev_lakehouse.silver_trips_quarantine GROUP BY 1)
SELECT b._source_file, b.n AS bronze, coalesce(c.n,0) AS clean, coalesce(q.n,0) AS quarantined,
       b.n = coalesce(c.n,0) + coalesce(q.n,0) AS balanced
FROM b LEFT JOIN c USING (_source_file) LEFT JOIN q USING (_source_file) ORDER BY 1;

-- Task 2.2 — rows affected per drop rule (README rules table)
SELECT rule, count(*) AS rows_failed
FROM workspace.dev_lakehouse.silver_trips_quarantine LATERAL VIEW explode(failed_rules) AS rule
GROUP BY rule ORDER BY 2 DESC;

-- Task 2.2 — warn-rule counts (also visible in the pipeline data-quality tab)
SELECT count_if(NOT coalesce(period = file_period, false))                       AS period_mismatch,
       count_if(NOT coalesce(start_lat BETWEEN 40.4 AND 41.0 AND start_lng BETWEEN -74.3 AND -73.6, false)) AS coords_outside,
       count_if(start_station_id IS NULL OR end_station_id IS NULL)              AS missing_station
FROM workspace.dev_lakehouse.silver_trips_clean;

-- Task 2.2 — duplicates removed by de-duplication (is ride_id unique in the source?)
SELECT (SELECT count(*) FROM workspace.dev_lakehouse.silver_trips_clean) AS clean_rows,
       (SELECT count(*) FROM workspace.dev_lakehouse.silver_trips)       AS dedup_rows,
       (SELECT count(*) FROM workspace.dev_lakehouse.silver_trips_clean)
     - (SELECT count(*) FROM workspace.dev_lakehouse.silver_trips)       AS duplicates_removed;

-- Pipeline expectation metrics from the event log (E4)
SELECT timestamp, details:flow_progress.data_quality.expectations AS expectations
FROM event_log(TABLE(workspace.dev_lakehouse.silver_trips_clean))
WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC LIMIT 20;

-- Task 3.1 / 4.1 — reconciliation (E2), incidents (E8), releases (E9)
SELECT period, dataset, expected_rows, bronze_rows, silver_rows, quarantined_rows, status, checked_at
FROM workspace.dev_ops.reconciliation
QUALIFY row_number() OVER (PARTITION BY period, dataset ORDER BY checked_at DESC) = 1
ORDER BY period, dataset;
SELECT sum(expected_rows) AS expected_total, sum(bronze_rows) AS bronze_total,
       sum(silver_rows) AS clean_total, sum(quarantined_rows) AS quarantined_total
FROM (SELECT * FROM workspace.dev_ops.reconciliation
      QUALIFY row_number() OVER (PARTITION BY period, dataset ORDER BY checked_at DESC) = 1);
SELECT * FROM workspace.dev_ops.incidents ORDER BY raised_at DESC;
SELECT * FROM workspace.dev_ops.release ORDER BY certified_at DESC;

-- Task 4.1 — per-month trips vs the reference table in Section 3 (NYC + JC, by file period)
SELECT period, sum(expected_rows) AS file_rows FROM workspace.prd_ops.file_manifest GROUP BY 1 ORDER BY 1;
SELECT count(*) AS silver_dedup_rows FROM workspace.prd_lakehouse.silver_trips;   -- must be > 50M

-- Schema-evolution evidence (E3)
SELECT _source_file, count(*) AS rows, count(feed_version) AS with_feed_version
FROM workspace.dev_lakehouse.bronze_trips GROUP BY 1 ORDER BY 1;
