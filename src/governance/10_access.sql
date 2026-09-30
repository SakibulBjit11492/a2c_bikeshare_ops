-- src/governance/10_access.sql  (Task 5.1) — run in the SQL editor, screenshot each SHOW GRANTS (E15)
-- Use your teammate's e-mail instead of `account users` if you invited one, e.g. `name@bjitgroup.com`
GRANT USE CATALOG ON CATALOG workspace TO `account users`;
GRANT USE SCHEMA ON SCHEMA workspace.prd_gold TO `account users`;
GRANT SELECT ON SCHEMA workspace.prd_gold TO `account users`;     -- inherited by current and future Gold objects
SHOW GRANTS ON SCHEMA workspace.prd_gold;                           -- screenshot: BEFORE

REVOKE SELECT ON SCHEMA workspace.prd_gold FROM `account users`;
SHOW GRANTS ON SCHEMA workspace.prd_gold;                           -- screenshot: AFTER (SELECT row is gone)
GRANT SELECT ON SCHEMA workspace.prd_gold TO `account users`;     -- restore it for the demo

-- Managed-table lifecycle
CREATE TABLE workspace.prd_ops.scratch_undrop AS SELECT * FROM workspace.prd_gold.gold_ride_behaviour;
DROP TABLE workspace.prd_ops.scratch_undrop;
SHOW TABLES DROPPED IN workspace.prd_ops;                           -- screenshot
UNDROP TABLE workspace.prd_ops.scratch_undrop;
SELECT count(*) FROM workspace.prd_ops.scratch_undrop;              -- proves it is back
DROP TABLE workspace.prd_ops.scratch_undrop;                        -- clean up
