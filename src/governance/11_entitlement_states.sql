-- src/governance/11_entitlement_states.sql  (Task 5.2) — same query, three entitlement states. Screenshot each.
SELECT session_user();          -- must match user_email in prd_ops.entitlements exactly

-- State 1: full access
SELECT system, count(*) AS trips, min(ride_id) AS sample_ride, avg(start_lat) AS avg_lat, max(start_lat) AS max_lat
FROM workspace.prd_gold.gold_trip_detail_recent GROUP BY ALL ORDER BY system;

-- State 2: the Jersey City partner without access to identifiers or precise coordinates
UPDATE workspace.prd_ops.entitlements SET scope_value = 'JC', can_see_sensitive = false
WHERE user_email = session_user();
SELECT system, count(*) AS trips, min(ride_id) AS sample_ride, avg(start_lat) AS avg_lat, max(start_lat) AS max_lat
FROM workspace.prd_gold.gold_trip_detail_recent GROUP BY ALL ORDER BY system;
-- expect: only JC rows, ride ids like id_3fa9..., max_lat with 3 decimals

-- State 3: back to full access (do this BEFORE any pipeline refresh or dashboard work)
UPDATE workspace.prd_ops.entitlements SET scope_value = '*', can_see_sensitive = true
WHERE user_email = session_user();
SELECT system, count(*) AS trips, min(ride_id) AS sample_ride, avg(start_lat) AS avg_lat, max(start_lat) AS max_lat
FROM workspace.prd_gold.gold_trip_detail_recent GROUP BY ALL ORDER BY system;

-- Optional: give an invited teammate the partner role
-- INSERT INTO workspace.prd_ops.entitlements VALUES ('teammate@bjitgroup.com', 'JC', false);
