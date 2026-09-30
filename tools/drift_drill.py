# Databricks notebook source
# tools/drift_drill.py — DEV ONLY. Rehearses a vendor schema change on real rows:
# copies 1,000 rows of a landed CSV into a new file that carries one extra column.
LANDING = "/Volumes/workspace/dev_landing/raw/tripdata_csv"
first = sorted(f.name for f in dbutils.fs.ls(LANDING) if f.name.endswith(".csv") and "_drift" not in f.name)[0]
with open(f"{LANDING}/{first}") as fh:
    lines = [next(fh).rstrip("\n") for _ in range(1001)]
header, rows = lines[0], lines[1:]
drifted = [header + ',"feed_version"'] + [row + ',"v2"' for row in rows]
drill_name = first[:-4] + "_drift.csv"          # keeps the prefix and period that Silver parses
dbutils.fs.put(f"{LANDING}/{drill_name}", "\n".join(drifted) + "\n", overwrite=False)
print("landed", drill_name, "- now run the pipeline and watch Bronze")
# Afterwards (evidence E3):
#   SELECT _source_file, count(*), count(feed_version) FROM workspace.dev_lakehouse.bronze_trips GROUP BY 1;
#   SELECT count(*) FROM workspace.dev_lakehouse.bronze_trips WHERE _rescued_data IS NOT NULL;
