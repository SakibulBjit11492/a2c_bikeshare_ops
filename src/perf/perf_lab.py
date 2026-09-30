# Databricks notebook source
# src/perf/perf_lab.py — run on the prd tables after the backfill. Record every number in docs/evidence.md.
# Run each measurement TWICE and record the second. After each cell: "See performance" -> query profile
# -> record number of tasks, and for part 3 the join node.
import time
from pyspark.sql import functions as F

SILVER = "workspace.prd_lakehouse.silver_trips"
ONE_PERIOD = spark.table(SILVER).where(F.col("period") == "2025-02")
results = []


def timed(label, df):
    t0 = time.time()
    df.write.format("noop").mode("overwrite").save()   # runs the whole plan, stores nothing (fallback: df.count())
    secs = time.time() - t0
    results.append((label, round(secs, 1)))
    print(f"{label:<40} {secs:7.1f}s")


# 1) Shuffle partitions: one of the few Spark confs you may set on serverless
for setting in ["auto", "8", "4000"]:
    spark.conf.set("spark.sql.shuffle.partitions", setting)
    timed(f"shuffle.partitions={setting}", spark.table(SILVER).groupBy("start_station_id", "start_date").count())
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# 2) Input split size: compare the number of scan tasks in the query profile
for size in ["128MB", "16MB"]:
    spark.conf.set("spark.sql.files.maxPartitionBytes", size)
    timed(f"maxPartitionBytes={size}", ONE_PERIOD.agg(F.sum("duration_min")))
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")   # back to the default

# 3) Join strategy: autoBroadcastJoinThreshold is not settable on serverless, so use hints
dim = spark.table("workspace.prd_lakehouse.dim_station")
bj = ONE_PERIOD.join(F.broadcast(dim), ONE_PERIOD["start_station_id"] == dim["short_name"])
smj = ONE_PERIOD.join(dim.hint("merge"), ONE_PERIOD["start_station_id"] == dim["short_name"])
bj.explain()          # expect BroadcastHashJoin
smj.explain()         # expect SortMergeJoin
timed("join broadcast hint", bj)
timed("join merge hint", smj)
display(spark.createDataFrame(results, "setting string, seconds double"))

# COMMAND ----------
# 4) Skew and spill on purpose: a window over a low-cardinality key, one period only.
#    Open "See performance" -> query profile. Look for one task doing most of the work, spill, DATA_SKEW.
timed("skewed window", ONE_PERIOD.selectExpr(
    "*", "row_number() OVER (PARTITION BY member_type ORDER BY started_at) AS rn"))
#    Fix: add a high-cardinality key so the work spreads out. Compare the profiles.
timed("fixed window", ONE_PERIOD.selectExpr(
    "*", "row_number() OVER (PARTITION BY member_type, start_date ORDER BY started_at) AS rn"))

# COMMAND ----------
# 5) Driver memory: one attempt only. Record what happens, then explain the fix (aggregate first).
try:
    rows = ONE_PERIOD.collect()
    print(f"collected {len(rows):,} rows to the driver")
except Exception as exc:
    print("failed as expected:", type(exc).__name__, str(exc)[:300])
# Fix: never collect raw rows; aggregate in Spark and collect only the small result, e.g.
display(ONE_PERIOD.groupBy("start_date").count().orderBy("start_date"))
