-- src/pipeline/gold.sql — consumer-facing objects, written to the gold schema by fully qualified name.
-- Every Gold object reads Silver (silver_trips, silver_station_status, silver_weather_daily, dim_station),
-- never another Gold object. Why MV everywhere: every Gold object is an aggregation or join whose result
-- must reflect ALL source changes (late files, de-duplication), which a streaming table cannot guarantee.

-- ---------------------------------------------------------------------------------------------------
-- BQ1 · hourly demand. MV: aggregation over the whole de-duplicated history; clustered by date for pruning.
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_demand_hourly
CLUSTER BY (start_date)
COMMENT 'Trips per hour by system, rider type and bike type'
AS SELECT
  system, start_date, start_hour, start_dow, period, member_type, rideable_type,
  count(*)                                 AS trips,
  percentile_approx(duration_min, 0.5)     AS median_duration_min
FROM silver_trips
GROUP BY ALL;

-- ---------------------------------------------------------------------------------------------------
-- BQ1 · daily trips joined to Central Park weather. MV: join of two changing sources (a re-fetched
-- weather value must update past days, which only a recomputed result guarantees).
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_weather_demand
CLUSTER BY (start_date)
COMMENT 'Daily trips per system with Central Park (USW00094728) precipitation and temperature classes'
AS WITH daily AS (
  SELECT system, start_date,
         count(*)                                  AS trips,
         count_if(rideable_type = 'electric_bike') AS ebike_trips,
         count_if(member_type = 'member')          AS member_trips,
         count_if(member_type = 'casual')          AS casual_trips
  FROM silver_trips
  GROUP BY ALL
)
SELECT
  d.system, d.start_date,
  date_format(d.start_date, 'yyyy-MM')              AS period,
  date_format(d.start_date, 'E')                    AS start_dow,
  dayofweek(d.start_date) IN (1, 7)                 AS is_weekend,
  d.trips, d.ebike_trips, d.member_trips, d.casual_trips,
  w.prcp AS prcp_mm, w.snow AS snow_mm, w.tmax AS tmax_c, w.tmin AS tmin_c,
  CASE WHEN w.prcp IS NULL THEN 'unknown'
       WHEN w.prcp >= 5    THEN 'wet (>= 5 mm)'
       ELSE 'dry (< 5 mm)' END                      AS rain_class,
  CASE WHEN w.tmax IS NULL THEN 'unknown'
       WHEN w.tmax < 5     THEN '1 cold (< 5 C)'
       WHEN w.tmax < 15    THEN '2 cool (5-15 C)'
       WHEN w.tmax < 25    THEN '3 mild (15-25 C)'
       ELSE '4 hot (>= 25 C)' END                   AS temp_band
FROM daily d
LEFT JOIN silver_weather_daily w
  ON w.obs_date = d.start_date AND w.station_id = 'USW00094728';

-- ---------------------------------------------------------------------------------------------------
-- BQ2 · weekday peak net flow per station (arrivals - departures, averaged per weekday).
-- MV: full outer join of departures and arrivals plus a global weekday count: needs the whole data set.
-- Trips without a station id cannot be attributed to a station and are excluded (counted in BQ2 query).
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_station_flow
COMMENT 'Weekday AM (07-10) and PM (16-19) departures, arrivals and net flow per station, averaged per weekday'
AS WITH weekdays AS (
  SELECT system, count(DISTINCT start_date) AS n_weekdays
  FROM silver_trips WHERE dayofweek(start_date) BETWEEN 2 AND 6
  GROUP BY system
),
dep AS (
  SELECT system, start_station_id AS station_id,
         CASE WHEN start_hour BETWEEN 7 AND 9 THEN 'AM 07-10' ELSE 'PM 16-19' END AS peak,
         count(*) AS departures
  FROM silver_trips
  WHERE start_station_id IS NOT NULL
    AND dayofweek(started_at) BETWEEN 2 AND 6
    AND (start_hour BETWEEN 7 AND 9 OR start_hour BETWEEN 16 AND 18)
  GROUP BY ALL
),
arr AS (
  SELECT system, end_station_id AS station_id,
         CASE WHEN hour(ended_at) BETWEEN 7 AND 9 THEN 'AM 07-10' ELSE 'PM 16-19' END AS peak,
         count(*) AS arrivals
  FROM silver_trips
  WHERE end_station_id IS NOT NULL
    AND dayofweek(ended_at) BETWEEN 2 AND 6
    AND (hour(ended_at) BETWEEN 7 AND 9 OR hour(ended_at) BETWEEN 16 AND 18)
  GROUP BY ALL
),
stations AS (   -- docked trips report the station's coordinates, so the median start point is the station
  SELECT system, start_station_id AS station_id,
         max_by(start_station_name, started_at)  AS station_name,
         percentile_approx(start_lat, 0.5)       AS lat,
         percentile_approx(start_lng, 0.5)       AS lng
  FROM silver_trips
  WHERE start_station_id IS NOT NULL AND rideable_type = 'classic_bike'
  GROUP BY ALL
)
SELECT
  coalesce(d.system, a.system)           AS system,
  coalesce(d.station_id, a.station_id)   AS station_id,
  s.station_name,
  coalesce(d.peak, a.peak)               AS peak,
  coalesce(d.departures, 0)              AS departures_total,
  coalesce(a.arrivals, 0)                AS arrivals_total,
  w.n_weekdays,
  round(coalesce(d.departures, 0) / w.n_weekdays, 2)                          AS avg_departures,
  round(coalesce(a.arrivals, 0) / w.n_weekdays, 2)                            AS avg_arrivals,
  round((coalesce(a.arrivals, 0) - coalesce(d.departures, 0)) / w.n_weekdays, 2) AS avg_net_flow,
  s.lat, s.lng
FROM dep d
FULL OUTER JOIN arr a
  ON d.system = a.system AND d.station_id = a.station_id AND d.peak = a.peak
JOIN weekdays w ON w.system = coalesce(d.system, a.system)
LEFT JOIN stations s ON s.system = coalesce(d.system, a.system) AND s.station_id = coalesce(d.station_id, a.station_id);

-- ---------------------------------------------------------------------------------------------------
-- BQ3 · ride behaviour. MV: percentiles cannot be added up, so the whole-window rows (period = 'ALL')
-- are computed from the trips with GROUPING SETS, never averaged from monthly percentiles.
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_ride_behaviour
COMMENT 'Duration percentiles, round-trip share and share over 45 min by system, month, bike type and rider type (period ALL = whole window)'
AS SELECT
  system,
  coalesce(period, 'ALL')                                        AS period,
  rideable_type, member_type,
  count(*)                                                       AS trips,
  round(percentile_approx(duration_min, 0.5), 2)                 AS p50_duration_min,
  round(percentile_approx(duration_min, 0.9), 2)                 AS p90_duration_min,
  round(avg(duration_min), 2)                                    AS avg_duration_min,
  round(count_if(is_round_trip) / count(*), 4)                   AS round_trip_share,
  round(count_if(duration_min > 45) / count(*), 4)               AS share_over_45_min,
  round(count_if(start_station_id IS NULL OR end_station_id IS NULL) / count(*), 4) AS share_missing_station
FROM silver_trips
GROUP BY GROUPING SETS ((system, period, rideable_type, member_type), (system, rideable_type, member_type));

-- ---------------------------------------------------------------------------------------------------
-- BQ4 · station health. MV: joins GBFS snapshots (key = GBFS station_id) to trips (key = short_name)
-- through dim_station; only matched stations appear (match rate reported in the BQ4 analysis query).
-- Only snapshots where the station was both renting and returning count.
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_station_health
COMMENT 'Share of GBFS snapshots with no bikes / no docks (while renting and returning), with trip demand'
AS WITH snap AS (
  SELECT station_id AS gbfs_station_id,
         count(*)                                                  AS snapshots,
         avg(CASE WHEN bikes_available = 0 THEN 1.0 ELSE 0.0 END)  AS share_empty,
         avg(CASE WHEN docks_available = 0 THEN 1.0 ELSE 0.0 END)  AS share_full,
         min(snapshot_ts)                                          AS first_snapshot,
         max(snapshot_ts)                                          AS last_snapshot
  FROM silver_station_status
  WHERE is_renting = 1 AND is_returning = 1
  GROUP BY station_id
),
days AS (
  SELECT system, count(DISTINCT start_date) AS n_days FROM silver_trips GROUP BY system
),
demand AS (
  SELECT system, start_station_id AS station_id,
         max_by(start_station_name, started_at) AS station_name,
         count(*)                               AS departures_total
  FROM silver_trips
  WHERE start_station_id IS NOT NULL
  GROUP BY ALL
)
SELECT
  t.system,                                   -- taken from the trips that start at the station
  t.station_id,
  s.gbfs_station_id,
  coalesce(d.name, t.station_name)            AS station_name,
  d.capacity,
  round(t.departures_total / y.n_days, 1)     AS avg_daily_departures,
  s.snapshots, s.first_snapshot, s.last_snapshot,
  round(s.share_empty, 4)                     AS share_empty,
  round(s.share_full, 4)                      AS share_full,
  d.lat                                       AS lat,
  d.lon                                       AS lng
FROM snap s
JOIN dim_station d ON d.station_id = s.gbfs_station_id
JOIN demand t      ON t.station_id = d.short_name
JOIN days y        ON y.system = t.system;

-- ---------------------------------------------------------------------------------------------------
-- The governed object: row filter and masks are part of the definition, so every refresh keeps them.
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_trip_detail_recent (
  system            STRING,
  ride_id           STRING MASK ${a2.ops}.mask_text,
  started_at        TIMESTAMP,
  ended_at          TIMESTAMP,
  rideable_type     STRING,
  member_type       STRING,
  start_station_id  STRING,
  end_station_id    STRING,
  start_lat         DOUBLE MASK ${a2.ops}.mask_coord,
  start_lng         DOUBLE MASK ${a2.ops}.mask_coord,
  end_lat           DOUBLE MASK ${a2.ops}.mask_coord,
  end_lng           DOUBLE MASK ${a2.ops}.mask_coord,
  duration_min      DOUBLE
)
WITH ROW FILTER ${a2.ops}.rf_scope ON (system)
CLUSTER BY (started_at)
COMMENT 'Last 30 days of trips in the data, for investigations. Filtered by system, ids and coordinates masked.'
AS SELECT
  system, ride_id, started_at, ended_at, rideable_type, member_type,
  start_station_id, end_station_id, start_lat, start_lng, end_lat, end_lng, duration_min
FROM silver_trips
WHERE started_at >= (SELECT max(started_at) - INTERVAL 30 DAYS FROM silver_trips);
