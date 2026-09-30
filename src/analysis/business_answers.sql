-- src/analysis/business_answers.sql — Section 7. Run against prd_gold (reconciled data).

-- =========================== BQ1 · When and how does the city ride? ===========================
-- 1a. Average trips per day by month and system
SELECT period, system, count(*) AS days, sum(trips) AS trips, round(avg(trips)) AS avg_trips_per_day
FROM workspace.prd_gold.gold_weather_demand
GROUP BY ALL ORDER BY period, system;

-- 1b. Hour-of-day x weekday heatmap (average trips per hour on that weekday)
WITH day_counts AS (
  SELECT start_dow, count(DISTINCT start_date) AS n_days FROM workspace.prd_gold.gold_demand_hourly GROUP BY 1)
SELECT h.start_dow, h.start_hour, round(sum(h.trips) / d.n_days) AS avg_trips
FROM workspace.prd_gold.gold_demand_hourly h JOIN day_counts d USING (start_dow)
GROUP BY h.start_dow, h.start_hour, d.n_days
ORDER BY CASE h.start_dow WHEN 'Mon' THEN 1 WHEN 'Tue' THEN 2 WHEN 'Wed' THEN 3 WHEN 'Thu' THEN 4
                          WHEN 'Fri' THEN 5 WHEN 'Sat' THEN 6 ELSE 7 END, h.start_hour;

-- 1c. Member vs casual and e-bike share by month
SELECT period,
       sum(trips) AS trips,
       round(sum(trips) FILTER (WHERE member_type = 'member') / sum(trips), 3)          AS member_share,
       round(sum(trips) FILTER (WHERE rideable_type = 'electric_bike') / sum(trips), 3) AS ebike_share
FROM workspace.prd_gold.gold_demand_hourly
GROUP BY period ORDER BY period;

-- 1d. Weather effect COMPARED WITHIN EACH MONTH (index = avg trips on class days / month average)
WITH d AS (
  SELECT start_date, period, sum(trips) AS trips, max(rain_class) AS rain_class, max(temp_band) AS temp_band
  FROM workspace.prd_gold.gold_weather_demand GROUP BY start_date, period),
m AS (SELECT period, avg(trips) AS month_avg FROM d GROUP BY period)
SELECT d.period, d.rain_class, count(*) AS days, round(avg(d.trips)) AS avg_trips,
       round(avg(d.trips) / m.month_avg, 3) AS index_vs_month_avg
FROM d JOIN m USING (period)
GROUP BY d.period, d.rain_class, m.month_avg ORDER BY d.period, d.rain_class;

-- 1e. Summary across months: day-weighted average of the within-month index, per rain class and temp band
WITH d AS (
  SELECT start_date, period, sum(trips) AS trips, max(rain_class) AS rain_class, max(temp_band) AS temp_band
  FROM workspace.prd_gold.gold_weather_demand GROUP BY start_date, period),
m AS (SELECT period, avg(trips) AS month_avg FROM d GROUP BY period)
SELECT 'rain' AS factor, rain_class AS class, count(*) AS days, round(avg(trips / month_avg), 3) AS avg_index
FROM d JOIN m USING (period) GROUP BY rain_class
UNION ALL
SELECT 'temperature', temp_band, count(*), round(avg(trips / month_avg), 3)
FROM d JOIN m USING (period) GROUP BY temp_band
ORDER BY factor, class;

-- =========================== BQ2 · Which stations drain, and which fill? ===========================
-- 2a. Top 20 draining (most negative net flow) per peak
SELECT * FROM (
  SELECT peak, system, station_id, station_name, avg_departures, avg_arrivals, avg_net_flow,
         row_number() OVER (PARTITION BY peak ORDER BY avg_net_flow ASC) AS rnk
  FROM workspace.prd_gold.gold_station_flow)
WHERE rnk <= 20 ORDER BY peak, rnk;

-- 2b. Top 20 filling (most positive net flow) per peak
SELECT * FROM (
  SELECT peak, system, station_id, station_name, avg_departures, avg_arrivals, avg_net_flow,
         row_number() OVER (PARTITION BY peak ORDER BY avg_net_flow DESC) AS rnk
  FROM workspace.prd_gold.gold_station_flow)
WHERE rnk <= 20 ORDER BY peak, rnk;

-- 2c. Excluded trips: weekday peak trips with no station id, by bike type (data-quality note from Silver)
SELECT rideable_type,
       count_if(start_station_id IS NULL) AS no_start_station,
       count_if(end_station_id IS NULL)   AS no_end_station,
       count(*)                           AS peak_trips,
       round(count_if(start_station_id IS NULL OR end_station_id IS NULL) / count(*), 4) AS share_excluded
FROM workspace.prd_lakehouse.silver_trips
WHERE dayofweek(started_at) BETWEEN 2 AND 6
  AND (start_hour BETWEEN 7 AND 9 OR start_hour BETWEEN 16 AND 18)
GROUP BY rideable_type;

-- =========================== BQ3 · How do people ride? ===========================
SELECT system, rideable_type, member_type, trips, p50_duration_min, p90_duration_min,
       round(100 * round_trip_share, 2) AS round_trip_pct, round(100 * share_over_45_min, 2) AS over_45_pct
FROM workspace.prd_gold.gold_ride_behaviour
WHERE period = 'ALL'
ORDER BY system, rideable_type, member_type;
-- Note: the publisher removes trips < 60 s, and our rule drops < 1 min, so the distribution is truncated
-- at 1 minute: medians are slightly higher than the "true" ride population, very short false starts are absent.

-- Monthly trend of the median (seasonality)
SELECT period, rideable_type, member_type, p50_duration_min
FROM workspace.prd_gold.gold_ride_behaviour WHERE period <> 'ALL' AND system = 'NYC' ORDER BY period, 2, 3;

-- =========================== BQ4 · Are the busiest stations the ones that run out? ===========================
-- 4a. Key match rate: GBFS stations (station_id) -> trip station ids (via short_name)
WITH gbfs AS (SELECT DISTINCT station_id FROM workspace.prd_lakehouse.silver_station_status),
     trips AS (SELECT DISTINCT start_station_id AS sid FROM workspace.prd_lakehouse.silver_trips WHERE start_station_id IS NOT NULL),
     joined AS (
       SELECT g.station_id, d.short_name, t.sid
       FROM gbfs g
       LEFT JOIN workspace.prd_lakehouse.dim_station d ON d.station_id = g.station_id
       LEFT JOIN trips t ON t.sid = d.short_name)
SELECT count(*)                                AS gbfs_stations,
       count(short_name)                       AS with_short_name,
       count(sid)                              AS matched_to_trips,
       round(count(sid) / count(*), 4)         AS match_rate
FROM joined;

-- 4b. Snapshot coverage (say this in the answer: hours, not months)
SELECT min(first_snapshot) AS from_ts, max(last_snapshot) AS to_ts, max(snapshots) AS snapshots_per_station
FROM workspace.prd_gold.gold_station_health;

-- 4c. High-demand stations (top 100 by departures) that are most often empty or full
WITH ranked AS (
  SELECT *, dense_rank() OVER (ORDER BY avg_daily_departures DESC) AS demand_rank
  FROM workspace.prd_gold.gold_station_health)
SELECT demand_rank, system, station_id, station_name, capacity, avg_daily_departures,
       round(100 * share_empty, 1) AS pct_snapshots_empty, round(100 * share_full, 1) AS pct_snapshots_full,
       round(100 * greatest(share_empty, share_full), 1) AS pct_unavailable, lat, lng
FROM ranked
WHERE demand_rank <= 100
ORDER BY greatest(share_empty, share_full) DESC, avg_daily_departures DESC
LIMIT 25;

-- 4d. Is demand correlated with running out? (one number for the client)
SELECT round(corr(avg_daily_departures, share_empty), 3) AS corr_demand_empty,
       round(corr(avg_daily_departures, share_full), 3)  AS corr_demand_full
FROM workspace.prd_gold.gold_station_health;
