# Databricks notebook source
# src/setup/setup_objects.py — idempotent; the setup job runs it on every target
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
OPS = f"{CAT}.{ENV}_ops"

for layer in ("landing", "lakehouse", "gold", "ops"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{ENV}_{layer}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{ENV}_landing.raw")
for folder in ["tripdata_zip", "tripdata_csv", "gbfs_status", "reference", "weather"]:
    # created up front so file-arrival triggers have a path to watch
    dbutils.fs.mkdirs(f"/Volumes/{CAT}/{ENV}_landing/raw/{folder}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.file_manifest (
    dataset STRING, landed_file STRING, unpacked_file STRING, period STRING,
    expected_rows BIGINT, registered_at TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.reconciliation (
    run_id STRING, period STRING, dataset STRING, expected_rows BIGINT, bronze_rows BIGINT,
    silver_rows BIGINT, quarantined_rows BIGINT, status STRING, checked_at TIMESTAMP)""")
spark.sql(f"CREATE TABLE IF NOT EXISTS {OPS}.release (run_id STRING, periods STRING, certified_at TIMESTAMP, notes STRING)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {OPS}.incidents (run_id STRING, reason STRING, raised_at TIMESTAMP)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {OPS}.entitlements (user_email STRING, scope_value STRING, can_see_sensitive BOOLEAN)")
# status table exists from day one so the dashboard counter never points at a missing table
spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.release_status (
    last_certified_at TIMESTAMP, certified_releases BIGINT, last_periods STRING)""")

# Give yourself full access once, so your pipeline, dashboard and queries keep working.
spark.sql(f"""
MERGE INTO {OPS}.entitlements t
USING (SELECT session_user() AS user_email) s
ON t.user_email = s.user_email
WHEN NOT MATCHED THEN INSERT (user_email, scope_value, can_see_sensitive) VALUES (s.user_email, '*', true)""")

# Governance functions used by row filters, column masks and ABAC policies (Day 5).
spark.sql(f"""
CREATE OR REPLACE FUNCTION {OPS}.rf_scope(scope STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: caller sees a row when entitled to its scope value, or to *'
RETURN EXISTS (
  SELECT 1 FROM {OPS}.entitlements e
  WHERE e.user_email = session_user() AND (e.scope_value = '*' OR e.scope_value = scope))""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {OPS}.mask_text(v STRING)
RETURNS STRING
COMMENT 'Column mask: pseudonymise identifiers for readers without the sensitive entitlement'
RETURN CASE WHEN EXISTS (
    SELECT 1 FROM {OPS}.entitlements e
    WHERE e.user_email = session_user() AND e.can_see_sensitive) THEN v
  ELSE concat('id_', substr(sha2(v, 256), 1, 12)) END""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {OPS}.mask_coord(v DOUBLE)
RETURNS DOUBLE
COMMENT 'Column mask: coordinates rounded to 3 decimals (about 100 m) for non-entitled readers'
RETURN CASE WHEN EXISTS (
    SELECT 1 FROM {OPS}.entitlements e
    WHERE e.user_email = session_user() AND e.can_see_sensitive) THEN v
  ELSE round(v, 3) END""")

display(spark.sql(f"SELECT * FROM {OPS}.entitlements"))
