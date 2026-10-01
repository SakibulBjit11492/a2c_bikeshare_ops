# src/pipeline/silver.py — conform, validate, quarantine, de-duplicate
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F

LAKEHOUSE = spark.conf.get("a2.lakehouse")

RAW_DROP_RULES = {
    "ride_id_present": "ride_id IS NOT NULL",
    "ended_after_started": "ended_at > started_at",
    "duration_1min_to_24h": "duration_min BETWEEN 1 AND 1440",
    "member_type_valid": "member_type IN ('member', 'casual')",
}
DROP_RULES = {k: f"coalesce({v}, false)" for k, v in RAW_DROP_RULES.items()}   # a NULL fails the rule
WARN_RULES = {
    "period_matches_file": "period = file_period",
    "coords_in_area": "start_lat BETWEEN 40.4 AND 41.0 AND start_lng BETWEEN -74.3 AND -73.6",
    "stations_present": "start_station_id IS NOT NULL AND end_station_id IS NOT NULL",
}
ALL_DROP = " AND ".join(DROP_RULES.values())
COORDS = ["start_lat", "start_lng", "end_lat", "end_lng"]


def conform(df: DataFrame) -> DataFrame:
    return df.select(
        F.when(F.col("_source_file").startswith("JC_"), "JC").otherwise("NYC").alias("system"),
        F.col("ride_id").cast("string").alias("ride_id"),
        F.col("rideable_type").alias("rideable_type"),
        F.try_to_timestamp(F.col("started_at")).alias("started_at"),      # ANSI-safe: bad values become NULL
        F.try_to_timestamp(F.col("ended_at")).alias("ended_at"),
        F.col("start_station_id").cast("string").alias("start_station_id"),
        F.col("start_station_name").alias("start_station_name"),
        F.col("end_station_id").cast("string").alias("end_station_id"),
        F.col("end_station_name").alias("end_station_name"),
        *[F.col(c).cast("double").alias(c) for c in COORDS],
        F.col("member_casual").alias("member_type"),
        F.col("_source_file"),
    )


def add_derived(df: DataFrame) -> DataFrame:
    return (df
            .withColumn("duration_min", (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")) / 60.0)
            .withColumn("start_date", F.to_date("started_at"))
            .withColumn("start_hour", F.hour("started_at"))
            .withColumn("start_dow", F.date_format("started_at", "E"))
            .withColumn("period", F.date_format("started_at", "yyyy-MM"))
            .withColumn("file_period", F.regexp_extract("_source_file", r"_(\d{4}-\d{2})_", 1))
            .withColumn("is_round_trip", F.col("start_station_id") == F.col("end_station_id"))
            .withColumn("_row_key", F.col("ride_id")))


@dp.temporary_view()
def v_trips_conformed():
    return add_derived(conform(spark.readStream.table("bronze_trips")))


@dp.table(name="silver_trips_clean", cluster_by=["start_date"], comment="Trips that pass every drop rule")
@dp.expect_or_fail("has_source_file", "_source_file IS NOT NULL")
# --- Task 2.2 "prove fail" drill: uncomment ONE run in dev, screenshot the failed update + event log, re-comment.
# @dp.expect_or_fail("drill_no_electric_bikes", "rideable_type <> 'electric_bike'")
@dp.expect_all(WARN_RULES)
@dp.expect_all_or_drop(DROP_RULES)
def silver_trips_clean():
    return spark.readStream.table("v_trips_conformed")


@dp.table(name="silver_trips_quarantine",
          comment="Trips that failed at least one drop rule, with the names of the rules they failed")
def silver_trips_quarantine():
    failed = F.array_compact(F.array(*[F.when(~F.expr(v), F.lit(k)) for k, v in DROP_RULES.items()]))
    return (spark.readStream.table("v_trips_conformed")
            .where(f"NOT ({ALL_DROP})")
            .withColumn("failed_rules", failed)
            .withColumn("_quarantined_ts", F.current_timestamp()))


@dp.materialized_view(name="silver_trips", cluster_by=["start_date", "start_station_id"],
                      comment="Conformed trips from both systems, de-duplicated on ride_id")
def silver_trips():
    return spark.read.table("silver_trips_clean").dropDuplicates(["_row_key"])


@dp.table(name="silver_station_status", comment="One row per station per GBFS snapshot")
def silver_station_status():
    return (spark.readStream.table("bronze_station_status")
            .select(F.col("last_updated").cast("long").alias("snapshot_epoch"),
                    F.explode("data.stations").alias("s"), "_source_file")
            .select(F.timestamp_seconds("snapshot_epoch").alias("snapshot_ts"),
                    F.col("s.station_id").cast("string").alias("station_id"),
                    F.col("s.num_bikes_available").cast("int").alias("bikes_available"),
                    F.col("s.num_ebikes_available").cast("int").alias("ebikes_available"),
                    F.col("s.num_docks_available").cast("int").alias("docks_available"),
                    # Citi Bike GBFS 2.3 encodes these as 1/0; cast("int") also handles true/false
                    F.col("s.is_renting").cast("int").alias("is_renting"),
                    F.col("s.is_returning").cast("int").alias("is_returning"),
                    "_source_file"))


@dp.materialized_view(name="dim_station", comment="Latest GBFS station_information snapshot, one row per station")
def dim_station():
    ref = spark.read.table(f"{LAKEHOUSE}.ref_station_information")
    latest = ref.agg(F.max("last_updated").alias("last_updated"))
    return (ref.join(latest, "last_updated")
            .select(F.explode("data.stations").alias("s"))
            .select(F.col("s.station_id").cast("string").alias("station_id"),
                    F.col("s.short_name").cast("string").alias("short_name"),   # = trip files' station id
                    F.col("s.name").alias("name"),
                    F.col("s.lat").cast("double").alias("lat"),
                    F.col("s.lon").cast("double").alias("lon"),
                    F.col("s.capacity").cast("int").alias("capacity"),
                    F.col("s.region_id").cast("string").alias("region_id"))
            .dropDuplicates(["station_id"]))


# Bronze keeps the NOAA payload as delivered: Auto Loader lands JSON fields as STRING unless
# inferColumnTypes is set, so Silver applies an explicit schema to the `results` array here.
WEATHER_RESULTS = ("array<struct<date:string, datatype:string, station:string, "
                   "attributes:string, value:double>>")


@dp.materialized_view(name="silver_weather_daily", comment="One row per station and day; the latest fetch wins")
def silver_weather_daily():
    obs = (spark.read.table("bronze_weather")
           .select("fetched_at", F.explode(F.from_json("results", WEATHER_RESULTS)).alias("r"))
           .select("fetched_at",
                   F.to_date(F.substring("r.date", 1, 10)).alias("obs_date"),
                   F.regexp_replace("r.station", "^GHCND:", "").alias("station_id"),
                   F.col("r.datatype").alias("datatype"),
                   F.col("r.value").cast("double").alias("value")))
    # Each scheduled fetch lands a new file: keep the most recently fetched value per station, day and element.
    latest = obs.groupBy("obs_date", "station_id", "datatype").agg(F.max_by("value", "fetched_at").alias("value"))
    # Pipelines do not support .pivot(); conditional aggregation does the same job.
    return (latest.groupBy("obs_date", "station_id")
                  .agg(*[F.max(F.when(F.col("datatype") == t, F.col("value"))).alias(t.lower())
                         for t in ["PRCP", "SNOW", "TMAX", "TMIN"]]))
