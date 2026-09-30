# Databricks notebook source
# src/jobs/prepare.py — unzip newly landed trip files and register their expected row counts
#
# Crash safety (answers the starter-kit TODO):
#   1. A zip counts as "known" only when its manifest rows exist. Manifest rows for a zip are written in ONE
#      MERGE at the very end, after every CSV of that zip is safely in tripdata_csv/.
#   2. CSV names are deterministic (NYC_2025-02_1.csv ...), and copies overwrite. If the run dies after some
#      copies but before the MERGE, the retry simply rewrites the same files and then registers them.
#      That is safe because the build (pipeline) task only starts after prepare succeeds, and the job has
#      max_concurrent_runs: 1, so Auto Loader never sees a half-written or half-registered zip.
#   3. The MERGE is keyed on unpacked_file, so even a double registration can never create duplicate rows.
import csv
import io
import os
import re
import shutil
import zipfile

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
RAW = f"/Volumes/{CAT}/{ENV}_landing/raw"
ZIP_DIR, CSV_DIR = f"{RAW}/tripdata_zip", f"{RAW}/tripdata_csv"
MANIFEST = f"{CAT}.{ENV}_ops.file_manifest"
PATTERN = re.compile(r"^(JC-)?(\d{4})(\d{2})-citibike-tripdata(\.csv)?\.zip$")


def count_data_rows(path: str) -> int:
    """Data rows as a CSV parser sees them (robust to quoted commas / newlines), minus the header."""
    with open(path, "r", newline="", encoding="utf-8", errors="replace") as fh:
        return sum(1 for _ in csv.reader(fh)) - 1


dbutils.fs.mkdirs(CSV_DIR)
known = {r.landed_file for r in spark.table(MANIFEST).select("landed_file").distinct().collect()}
periods, skipped = set(), []
for f in sorted(dbutils.fs.ls(ZIP_DIR), key=lambda f: f.name):
    m = PATTERN.match(f.name)
    if not m:
        skipped.append(f.name)
        continue
    if f.name in known:
        continue
    system = "jc" if m.group(1) else "nyc"
    period = f"{m.group(2)}-{m.group(3)}"
    local_zip = f"/tmp/{f.name}"
    shutil.copyfile(f"{ZIP_DIR}/{f.name}", local_zip)            # Volumes are readable as local paths
    registrations = []
    with zipfile.ZipFile(local_zip) as zf:
        members = sorted(n for n in zf.namelist()
                         if n.lower().endswith(".csv") and not n.startswith("__MACOSX")
                         and not os.path.basename(n).startswith("._"))
        for i, member in enumerate(members, 1):
            out_name = f"{system.upper()}_{period}_{i}.csv"
            local_csv = f"/tmp/{out_name}"
            with zf.open(member) as src, open(local_csv, "wb") as dst:
                shutil.copyfileobj(src, dst, 16 * 1024 * 1024)
            expected = count_data_rows(local_csv)
            dbutils.fs.cp(f"file:{local_csv}", f"{CSV_DIR}/{out_name}")   # overwrite-safe, deterministic name
            os.remove(local_csv)
            registrations.append((system, f.name, out_name, period, expected))
            print(f"{f.name} [{member}] -> {out_name}: {expected:,} rows")
    os.remove(local_zip)

    # Register the whole zip atomically (one MERGE = one Delta transaction)
    spark.createDataFrame(registrations,
                          "dataset string, landed_file string, unpacked_file string, period string, expected_rows bigint"
                          ).createOrReplaceTempView("new_files")
    spark.sql(f"""
        MERGE INTO {MANIFEST} t
        USING new_files s ON t.unpacked_file = s.unpacked_file
        WHEN MATCHED THEN UPDATE SET t.expected_rows = s.expected_rows, t.landed_file = s.landed_file
        WHEN NOT MATCHED THEN INSERT (dataset, landed_file, unpacked_file, period, expected_rows, registered_at)
             VALUES (s.dataset, s.landed_file, s.unpacked_file, s.period, s.expected_rows, current_timestamp())""")
    periods.add(period)

if skipped:
    print("ignored (name does not match the expected pattern):", skipped)
print("new periods:", sorted(periods))
dbutils.jobs.taskValues.set(key="periods", value=sorted(periods))
dbutils.jobs.taskValues.set(key="n_new", value=len(periods))
