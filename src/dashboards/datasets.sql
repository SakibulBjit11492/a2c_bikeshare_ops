-- src/dashboards/datasets.sql — paste each block as a Dataset in the AI/BI dashboard (Task 5.4)
-- Then: publish, export JSON -> src/dashboards/client_dashboard.lvdash.json (replace this note in the README).

-- ds_heatmap  -> Heatmap: x = start_hour, y = start_dow, color = avg_trips   (BQ1)
WITH day_counts AS (SELECT start_dow, count(DISTINCT start_date) AS n_days FROM workspace.prd_gold.gold_demand_hourly GROUP BY 1)
SELECT h.start_dow, h.start_hour, round(sum(h.trips) / d.n_days) AS avg_trips
FROM workspace.prd_gold.gold_demand_hourly h JOIN day_counts d USING (start_dow)
GROUP BY h.start_dow, h.start_hour, d.n_days;

-- ds_monthly  -> Line: x = period, y = ebike_share and avg_trips_per_day (dual axis)   (BQ1)
SELECT period,
       round(sum(trips) / count(DISTINCT start_date)) AS avg_trips_per_day,
       round(sum(ebike_trips) / sum(trips), 3)        AS ebike_share,
       round(sum(member_trips) / sum(trips), 3)       AS member_share
FROM workspace.prd_gold.gold_weather_demand GROUP BY period ORDER BY period;

-- ds_weather  -> Table / bar: rain_class vs avg index within month   (BQ1)
WITH d AS (SELECT start_date, period, sum(trips) AS trips, max(rain_class) AS rain_class
           FROM workspace.prd_gold.gold_weather_demand GROUP BY 1, 2),
     m AS (SELECT period, avg(trips) AS month_avg FROM d GROUP BY 1)
SELECT rain_class, count(*) AS days, round(avg(trips / month_avg), 3) AS demand_index_vs_month
FROM d JOIN m USING (period) GROUP BY rain_class;

-- ds_flow_map -> Point map: lat/lng, color = avg_net_flow, filter = peak   (BQ2)
SELECT peak, system, station_id, station_name, avg_net_flow, lat, lng
FROM workspace.prd_gold.gold_station_flow
QUALIFY row_number() OVER (PARTITION BY peak ORDER BY abs(avg_net_flow) DESC) <= 80;

-- ds_duration -> Grouped bar / table: p50 and p90 by bike x rider   (BQ3)
SELECT rideable_type, member_type, trips, p50_duration_min, p90_duration_min,
       round(100 * round_trip_share, 2) AS round_trip_pct, round(100 * share_over_45_min, 2) AS over_45_pct
FROM workspace.prd_gold.gold_ride_behaviour WHERE period = 'ALL' AND system = 'NYC';

-- ds_health  -> Table: busy stations often empty or full   (BQ4)
SELECT station_name, system, avg_daily_departures, round(100 * share_empty, 1) AS pct_empty,
       round(100 * share_full, 1) AS pct_full, lat, lng
FROM workspace.prd_gold.gold_station_health
QUALIFY dense_rank() OVER (ORDER BY avg_daily_departures DESC) <= 100
ORDER BY greatest(share_empty, share_full) DESC LIMIT 25;

-- ds_release -> Counter: last_certified_at (and certified_releases)
SELECT last_certified_at, certified_releases, last_periods FROM workspace.prd_ops.release_status;

-- Footer text widget:
-- "Trip data: Citi Bike public system data (Lyft Bikes and Scooters, Citi Bike Data License Agreement).
--  Weather: NOAA NCEI Climate Data Online, Central Park (USW00094728). Not endorsed by Citi Bike or Lyft."
