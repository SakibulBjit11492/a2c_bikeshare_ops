-- src/governance/12_abac.sql  (Task 5.3)
-- 1. Create two governed tags once: Catalog -> Govern -> Governed tags -> Create
--      sensitivity    allowed values: financial, restricted, location
--      access_scope   allowed value:  partner
--    (SQL alternative if your workspace supports it:
--      CREATE GOVERNED TAG sensitivity VALUES ('financial', 'restricted', 'location');
--      CREATE GOVERNED TAG access_scope VALUES ('partner');)

-- 2. Tag every Gold object EXCEPT gold_trip_detail_recent (manual rules already; a column cannot carry both).
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_flow   ALTER COLUMN lat SET TAGS ('sensitivity' = 'location');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_flow   ALTER COLUMN lng SET TAGS ('sensitivity' = 'location');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_health ALTER COLUMN lat SET TAGS ('sensitivity' = 'location');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_health ALTER COLUMN lng SET TAGS ('sensitivity' = 'location');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_demand_hourly  ALTER COLUMN system SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_weather_demand ALTER COLUMN system SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_flow   ALTER COLUMN system SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_ride_behaviour ALTER COLUMN system SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_station_health ALTER COLUMN system SET TAGS ('access_scope' = 'partner');

-- Check the tags
SELECT table_name, column_name, tag_name, tag_value
FROM workspace.information_schema.column_tags
WHERE schema_name = 'prd_gold' ORDER BY table_name, column_name;

-- 3. One mask policy and one row-filter policy cover every tagged column in the schema
CREATE POLICY generalise_locations
ON SCHEMA workspace.prd_gold
COMMENT 'Coordinates are rounded to about 100 m unless the reader holds the sensitive entitlement'
COLUMN MASK workspace.prd_ops.mask_coord
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('sensitivity', 'location') AS c
ON COLUMN c;

CREATE POLICY partner_rows
ON SCHEMA workspace.prd_gold
COMMENT 'Partner analysts only see rows for the scope they are entitled to'
ROW FILTER workspace.prd_ops.rf_scope
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('access_scope', 'partner') AS s
USING COLUMNS (s);

SHOW POLICIES ON SCHEMA workspace.prd_gold;                     -- E15

-- 4. Test: switch to state 2 (11_entitlement_states.sql) and run these; NYC rows disappear, coords rounded.
SELECT system, count(*) FROM workspace.prd_gold.gold_demand_hourly  GROUP BY ALL;
SELECT system, station_id, lat, lng FROM workspace.prd_gold.gold_station_flow LIMIT 5;
--    Then switch back to state 3.

-- 5. Survival check: trigger a pipeline refresh (or the next build), then re-run the column_tags query above.
--    Record in the README whether the tags were still there.

-- FALLBACK if governed tags are unavailable: add to each Gold MV in gold.sql
--   WITH ROW FILTER ${a2.ops}.rf_scope ON (system)   and   lat DOUBLE MASK ${a2.ops}.mask_coord, ...
--   and explain what two ABAC policies would have saved you (one rule per schema vs N rules per table).
