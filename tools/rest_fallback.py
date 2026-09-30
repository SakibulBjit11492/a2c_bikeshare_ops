#!/usr/bin/env python3
"""Laptop fallback for REST sources, used ONLY if the smoke test shows the host is blocked from serverless.

Same logic as src/ingest/gbfs_poll.py and weather_rest.py, but runs on your laptop and uploads the JSON
into the landing Volume with the SDK. The pipeline does not change: it still ingests files from the Volume.

    export NOAA_TOKEN=...            # never commit it
    python tools/rest_fallback.py weather --env dev
    python tools/rest_fallback.py gbfs --env prd --every 1800   # poll every 30 min (leave running 8h+)
"""
import argparse
import datetime as dt
import io
import json
import os
import time

import requests
from databricks.sdk import WorkspaceClient


def upload(w, path, text):
    w.files.upload(path, io.BytesIO(text.encode()), overwrite=False)
    print("uploaded", path)


def gbfs_once(w, raw):
    feeds = {f["name"]: f["url"] for f in
             requests.get("https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json", timeout=30).json()["data"]["en"]["feeds"]}
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    upload(w, f"{raw}/gbfs_status/station_status_{stamp}.json", requests.get(feeds["station_status"], timeout=30).text)
    existing = [e.name for e in w.files.list_directory_contents(f"{raw}/reference")]
    if not any(n.startswith(f"station_information_{stamp[:8]}") for n in existing):
        upload(w, f"{raw}/reference/station_information_{stamp}.json",
               requests.get(feeds["station_information"], timeout=30).text)


def weather(w, raw, start="2024-05-01", end="2025-06-30", station="GHCND:USW00094728"):
    token = os.environ["NOAA_TOKEN"]
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    while s <= e:
        w_end = min(e, dt.date(s.year, 12, 31))
        rows, offset = [], 1
        while True:
            r = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/data", headers={"token": token}, timeout=60,
                             params={"datasetid": "GHCND", "stationid": station,
                                     "datatypeid": ["PRCP", "SNOW", "TMAX", "TMIN"], "startdate": s.isoformat(),
                                     "enddate": w_end.isoformat(), "units": "metric", "limit": 1000, "offset": offset})
            r.raise_for_status()
            body = r.json() or {}
            rows += body.get("results", [])
            offset += 1000
            time.sleep(0.25)
            if offset > body.get("metadata", {}).get("resultset", {}).get("count", 0):
                break
        doc = {"station": station, "start": s.isoformat(), "end": w_end.isoformat(),
               "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(), "results": rows}
        upload(w, f"{raw}/weather/{station.replace(':', '_')}_{s:%Y%m%d}_{w_end:%Y%m%d}_{stamp}.json", json.dumps(doc))
        s = w_end + dt.timedelta(days=1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("source", choices=["gbfs", "weather"])
    p.add_argument("--env", default="dev")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--profile", default="DEFAULT")
    p.add_argument("--every", type=int, default=0, help="gbfs only: seconds between polls (0 = once)")
    a = p.parse_args()
    client = WorkspaceClient(profile=a.profile)
    root = f"/Volumes/{a.catalog}/{a.env}_landing/raw"
    if a.source == "weather":
        weather(client, root)
    else:
        while True:
            gbfs_once(client, root)
            if not a.every:
                break
            time.sleep(a.every)
