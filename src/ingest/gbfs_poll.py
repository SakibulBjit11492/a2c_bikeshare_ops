# Databricks notebook source
# src/ingest/gbfs_poll.py — live station feed (GBFS 2.3), no key; gbfs_job runs it every 30 minutes
import datetime as dt

import requests

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
RAW = f"/Volumes/{CAT}/{ENV}_landing/raw"
AUTODISCOVERY = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"

discovery = requests.get(AUTODISCOVERY, timeout=30)
discovery.raise_for_status()
feeds = {f["name"]: f["url"] for f in discovery.json()["data"]["en"]["feeds"]}
stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

dbutils.fs.mkdirs(f"{RAW}/gbfs_status")
status = requests.get(feeds["station_status"], timeout=30)
status.raise_for_status()
dbutils.fs.put(f"{RAW}/gbfs_status/station_status_{stamp}.json", status.text, overwrite=False)

# One station_information snapshot per day is enough for the reference table
ref_dir = f"{RAW}/reference"
dbutils.fs.mkdirs(ref_dir)
if not any(f.name.startswith(f"station_information_{stamp[:8]}") for f in dbutils.fs.ls(ref_dir)):
    info = requests.get(feeds["station_information"], timeout=30)
    info.raise_for_status()
    dbutils.fs.put(f"{ref_dir}/station_information_{stamp}.json", info.text, overwrite=False)
print("snapshot", stamp)
