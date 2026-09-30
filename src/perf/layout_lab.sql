-- src/perf/layout_lab.sql — partitioning vs liquid clustering on one period of real data (Task 4.4)
-- Step 0: pick the busiest station from BQ2 (replace BUSY_ID below)
SELECT station_id, station_name, sum(departures_total) AS dep
FROM workspace.prd_gold.gold_station_flow WHERE system = 'NYC' GROUP BY ALL ORDER BY dep DESC LIMIT 1;

CREATE OR REPLACE TABLE workspace.prd_ops.perf_partitioned
PARTITIONED BY (start_date)
AS SELECT * FROM workspace.prd_lakehouse.silver_trips WHERE period = '2025-02';

CREATE OR REPLACE TABLE workspace.prd_ops.perf_clustered
CLUSTER BY (start_date, start_station_id)
AS SELECT * FROM workspace.prd_lakehouse.silver_trips WHERE period = '2025-02';
OPTIMIZE workspace.prd_ops.perf_clustered;

DESCRIBE DETAIL workspace.prd_ops.perf_partitioned;     -- record numFiles and sizeInBytes
DESCRIBE DETAIL workspace.prd_ops.perf_clustered;

-- The same selective query on both. In the query profile compare files read, bytes read and pruning.
SELECT count(*), avg(duration_min) FROM workspace.prd_ops.perf_partitioned WHERE start_station_id = 'BUSY_ID' AND start_date BETWEEN '2025-02-10' AND '2025-02-16';
SELECT count(*), avg(duration_min) FROM workspace.prd_ops.perf_clustered   WHERE start_station_id = 'BUSY_ID' AND start_date BETWEEN '2025-02-10' AND '2025-02-16';

-- Let predictive optimization choose clustering keys from real query patterns
ALTER TABLE workspace.prd_ops.perf_clustered CLUSTER BY AUTO;
DESCRIBE TABLE EXTENDED workspace.prd_ops.perf_clustered;
-- Predictive optimization status: Catalog Explorer -> workspace -> Details (record it)

-- Clean up afterwards to save storage
-- DROP TABLE workspace.prd_ops.perf_partitioned; DROP TABLE workspace.prd_ops.perf_clustered;
