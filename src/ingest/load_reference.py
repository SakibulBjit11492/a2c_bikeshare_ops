# Databricks notebook source
# src/ingest/load_reference.py — COPY INTO is idempotent: a re-run loads only files it has not seen
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
LH = f"{CAT}.{ENV}_lakehouse"
REF = f"/Volumes/{CAT}/{ENV}_landing/raw/reference"

try:
    landed = [f.name for f in dbutils.fs.ls(REF)]
except Exception:
    landed = []
if not landed:
    dbutils.notebook.exit("no reference files have landed yet")

spark.sql(f"CREATE TABLE IF NOT EXISTS {LH}.ref_station_information")   # schemaless until the first load
display(spark.sql(f"""
    COPY INTO {LH}.ref_station_information
    FROM '{REF}/'
    FILEFORMAT = JSON
    PATTERN = 'station_information_*.json'
    FORMAT_OPTIONS ('multiLine' = 'true', 'mergeSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')"""))     # one row per snapshot file; 0 new rows on a re-run
# Evidence E5: screenshot this output on the SECOND run (num_inserted_rows = 0).
