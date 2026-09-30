# src/pipeline/bronze.py — raw, as delivered, plus lineage columns
# NOTE: plain .py file (no "# Databricks notebook source" header) because it is a pipeline file library.
# Before the FIRST run of a target, each watched folder needs at least one file, otherwise Auto Loader
# cannot infer a schema: tripdata_csv (prepare), gbfs_status (gbfs_poll), weather (weather_rest).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("a2.landing")      # /Volumes/<catalog>/<env>_landing/raw
# Station ids look numeric (5329.03) but are text; timestamps are parsed in Silver with try_to_timestamp
HINTS = ("ride_id STRING, start_station_id STRING, end_station_id STRING, "
         "started_at STRING, ended_at STRING")


def autoload(pattern: str, fmt: str, **options):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    for key, value in options.items():
        reader = reader.option(key, value)
    return (reader.load(f"{LANDING}/{pattern}")
            .withColumn("_source_file", F.col("_metadata.file_name"))
            .withColumn("_ingest_ts", F.current_timestamp()))


@dp.table(name="bronze_trips", comment="Trips exactly as delivered (unzipped CSV, New York and Jersey City)")
def bronze_trips():
    return autoload("tripdata_csv/*.csv", "csv", header="true",
                    **{"cloudFiles.inferColumnTypes": "true", "cloudFiles.schemaHints": HINTS})


@dp.table(name="bronze_station_status", comment="GBFS station_status snapshots, one JSON document per file")
def bronze_station_status():
    return autoload("gbfs_status/*.json", "json", multiLine="true", **{"cloudFiles.inferColumnTypes": "true"})


@dp.table(name="bronze_weather", comment="NOAA CDO responses, one JSON document per file")
def bronze_weather():
    return autoload("weather/*.json", "json", multiLine="true")
