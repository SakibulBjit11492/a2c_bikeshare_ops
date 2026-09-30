# Databricks notebook source
# src/jobs/quality_check.py — decides the if/else branch through a task value
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("max_quarantine_pct", "2.0")
CAT, ENV, RUN_ID = dbutils.widgets.get("catalog"), dbutils.widgets.get("env"), dbutils.widgets.get("run_id")
MAX_Q = float(dbutils.widgets.get("max_quarantine_pct"))
OPS, LH = f"{CAT}.{ENV}_ops", f"{CAT}.{ENV}_lakehouse"

# Check 1+2: every reconciliation row of this run is OK, and the quarantine share is under the threshold
rec = spark.table(f"{OPS}.reconciliation").where(F.col("run_id") == RUN_ID)
r = rec.agg(F.count("*").alias("checks"),
            F.sum(F.when(F.col("status") != "OK", 1).otherwise(0)).alias("mismatches"),
            F.sum("bronze_rows").alias("bronze"),
            F.sum("quarantined_rows").alias("quarantined")).first()
quarantine_pct = 100.0 * (r.quarantined or 0) / max(1, r.bronze or 0)

# Check 3 (own check): the de-duplicated Silver table really has unique, non-null business keys
dup = (spark.table(f"{LH}.silver_trips")
       .agg(F.count("*").alias("n"), F.countDistinct("ride_id").alias("d"),
            F.count_if(F.col("ride_id").isNull()).alias("nulls")).first())
keys_ok = dup.n == dup.d and dup.nulls == 0

reasons = []
if (r.checks or 0) == 0:
    reasons.append("no reconciliation rows for this run")
if (r.mismatches or 0) > 0:
    reasons.append(f"{r.mismatches} reconciliation mismatch(es)")
if quarantine_pct > MAX_Q:
    reasons.append(f"quarantine {quarantine_pct:.3f}% > {MAX_Q}%")
if not keys_ok:
    reasons.append(f"silver_trips keys not unique (rows={dup.n}, distinct={dup.d}, nulls={dup.nulls})")
passed = not reasons

print(f"checks={r.checks} mismatches={r.mismatches} quarantine={quarantine_pct:.3f}% (max {MAX_Q}%) "
      f"keys_ok={keys_ok} -> {'PASS' if passed else 'FAIL: ' + '; '.join(reasons)}")
dbutils.jobs.taskValues.set(key="gate", value="pass" if passed else "fail")
dbutils.jobs.taskValues.set(key="quarantine_pct", value=round(quarantine_pct, 3))
dbutils.jobs.taskValues.set(key="reason", value="; ".join(reasons) or "ok")
