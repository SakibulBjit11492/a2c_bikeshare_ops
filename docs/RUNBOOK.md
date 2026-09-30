# Runbook — Assignment 2C, step by step

Follow in order. `☐` = do it, `✔ check` = the "Done when" test, `📸 E#` = capture evidence into docs/evidence.md.
All SQL checks live in `src/analysis/checks.sql`.

---
## Day 0 (15 min, before you start the clock)
☐ Laptop: Python 3.10+, git, and the Databricks CLI (`brew install databricks/tap/databricks` or `winget install Databricks.DatabricksCLI`, or the curl installer).
☐ `pip install requests databricks-sdk`
☐ Unzip this repo, `cd a2c_bikeshare_ops`.

---
## DAY 1 — Foundations

### 1.1 Environment and smoke test
1. ☐ Sign in to **Databricks Free Edition**. Copy the workspace URL (e.g. `https://dbc-xxxx.cloud.databricks.com`).
2. ☐ Put that URL in `databricks.yml` (both `host:` lines) and your e-mail in `notify_email`.
3. ☐ Settings → Developer → Access tokens → Generate. Then:
   ```bash
   databricks configure          # host = your URL, token = the PAT
   databricks current-user me    # ✔ check
   ```
4. ☐ **Request the NOAA token now**: https://www.ncei.noaa.gov/cdo-web/token (arrives by e-mail).
5. ☐ **Start the 10 GB download now** (separate terminal, leave it running):
   ```bash
   python tools/drop_files.py --download-only
   ```
6. ☐ Workspace → Import → `tools/00_smoke_test.py`. Run it on serverless. 📸 E1
7. ☐ Manual checks — record yes/no in README "Environment findings":
   Catalog → Govern → Governed tags (can you create one?); Settings → Identity and access → invite a user? create a service principal + secret?
8. ☐ SQL Warehouses → copy the exact warehouse name into `databricks.yml` (`lookup.warehouse`).

### 1.2 Repo and bundle
1. ☐ Create GitHub repo `a2c_bikeshare_ops` (private is fine), then:
   ```bash
   git init && git add . && git commit -m "Starter bundle"
   git branch -M main && git remote add origin https://github.com/<you>/a2c_bikeshare_ops.git && git push -u origin main
   ```
2. ☐ Deploy dev:
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run -t dev setup_job
   ```
   If deploy complains the trigger path/table does not exist: comment out the `trigger:` blocks of `build_job` and `release_job`, deploy, run `setup_job`, restore, deploy again. **Write one line in README whether you needed this.** (In dev the triggers are paused anyway.)
3. ☐ ✔ check: `SHOW SCHEMAS IN workspace LIKE 'dev_*'` → 4 schemas; `SHOW VOLUMES IN workspace.dev_landing` → `raw`; `SELECT * FROM workspace.dev_ops.entitlements` → you, `*`.
4. ☐ Settings → Linked accounts → add GitHub (PAT with repo scope). Workspace → Create → Git folder → your repo URL.

### 1.3 First data + COPY INTO
1. ☐ Once Feb 2025 has downloaded:
   ```bash
   python tools/drop_files.py --env dev --match 202502      # drops NYC + JC Feb 2025 zips
   ```
2. ☐ Run `src/ingest/gbfs_poll.py` once in the workspace (widgets: workspace / dev) — writes a status + reference snapshot.
3. ☐ Run `src/ingest/load_reference.py` (or re-run `setup_job`). **Run it a second time** → 0 rows inserted. 📸 E5
4. ☐ Run `src/jobs/prepare.py` by hand (widgets workspace/dev). ✔ `SELECT * FROM workspace.dev_ops.file_manifest` shows NYC_2025-02_1.csv, _2 ..., JC_2025-02_1.csv.
5. ☐ Fill README "Ingestion decisions" table (template already there).

### 1.4 Secrets + REST
1. ☐ When the NOAA token arrives:
   ```bash
   databricks secrets create-scope a2
   databricks secrets put-secret a2 noaa_token
   databricks secrets list-secrets a2
   ```
2. ☐ Run `src/ingest/weather_rest.py` (widgets workspace/dev). 📸 E6: the `token: [REDACTED]` line. Then **delete the print line**, commit.
3. ☐ If smoke test showed NOAA/GBFS blocked: `export NOAA_TOKEN=...; python tools/rest_fallback.py weather --env dev` and `python tools/rest_fallback.py gbfs --env dev`. Document it.
4. ☐ ✔ `git grep -i token` → only variable names / docs, no values.
5. ☐ Write Day-1 checkpoint answers in `docs/exam-notes.md`.

---
## DAY 2 — Pipeline
> Before the first pipeline run, `tripdata_csv/`, `gbfs_status/` and `weather/` must each hold ≥1 file, and `ref_station_information` must exist (all done on Day 1).

### 2.1 Bronze
1. ☐ `databricks bundle deploy -t dev && databricks bundle run -t dev lakehouse`
2. ☐ ✔ Bronze rows per `_source_file` = manifest `expected_rows` (checks.sql, Task 2.1 block). Also verify one file by hand.
3. ☐ **Drift drill**: import/run `tools/drift_drill.py` → run the pipeline → it stops with `UnknownFieldException` → run again → `feed_version` column appears (NULL for old rows). 📸 E3 (failed update, restarted update, the query at the bottom of drift_drill.py, `_rescued_data` count).

### 2.2 Silver
1. ☐ Profile Bronze (checks.sql profile block). Put numbers in README so each threshold is justified.
2. ☐ Silver is already written in `src/pipeline/silver.py`. Deploy + run.
3. ☐ ✔ clean + quarantine = bronze per file (checks.sql). Fill the rules table using the "rows affected per drop rule" query.
4. ☐ Record duplicates removed (checks.sql). The drift file's 1,000 rows are exact duplicates → removed by de-dup.
5. ☐ **Fail drill**: uncomment `drill_no_electric_bikes` in silver.py → deploy → run → update FAILS. 📸 failed update + event log entry. Re-comment, deploy, run (green).
6. ☐ Pipeline UI → silver_trips_clean → Data quality tab. 📸 E4

### 2.3 Gold
1. ☐ gold.sql is complete. Deploy + run. ✔ all 6 Gold MVs exist in `dev_gold` and have rows.
2. ☐ Run the business queries against **dev_gold** as a first draft (replace prd_ with dev_ in business_answers.sql). BQ4 will be thin until the prod GBFS polling has run.
3. ☐ Day-2 checkpoint answers.

---
## DAY 3 — Orchestrate and ship

### 3.1 Build job
1. ☐ ```bash
   python tools/drop_files.py --env dev --match JC-202503
   databricks bundle run -t dev build_job
   ```
   ✔ green run; open it in the UI, expand the for-each. 📸 E7 (DAG + green run)
2. ☐ Force the failure branch:
   ```bash
   python tools/drop_files.py --env dev --match JC-202504
   databricks bundle run -t dev build_job --params max_quarantine_pct=-1
   ```
   ✔ gate → false → raise_incident writes `dev_ops.incidents` → run FAILS → e-mail arrives. 📸 E8
3. ☐ README: explain `max_retries` / `min_retry_interval_millis` on prepare and build.

### 3.2 Triggers
☐ Fill README trigger table (template has it). Proof happens in prod (3.3 step 5).

### 3.3 CI/CD
1. ☐ **Git folder branch/PR**: in the workspace Git folder → branch `feature/gold-kpis` → edit a Gold comment or add a column (e.g. `avg_duration_min` already exists; change a COMMENT) → Commit & push → open the PR **on GitHub** → merge.
2. ☐ **Conflict**: on GitHub edit line X of `gold.sql` on `main`; in the Git folder edit the same line on another branch; pull `main` into it → resolve in the Git folder conflict UI → push. 📸 resolution.
3. ☐ **Targets**: `databricks bundle validate -t dev` vs `-t prod` (compare `schema: dev_lakehouse` vs `prd_lakehouse` with `databricks bundle validate -t prod -o json | grep schema`). The prod-only `build_job.email_notifications.on_success` in databricks.yml is your **target override**; explain variable vs override in README.
4. ☐ **GitHub Actions**: repo → Settings → Secrets and variables → Actions → add `DATABRICKS_HOST`, `DATABRICKS_TOKEN`. Open a PR → validate runs. Merge → deploy dev → deploy prod → setup_job. 📸 E10 (both runs)
   First prod deploy may hit the trigger bootstrap problem: run `databricks bundle run -t prod setup_job` from the laptop once (with triggers commented) then re-run the workflow.
5. ☐ **End-to-end in prod** (do this in the MORNING so 8 h of GBFS finish today):
   ```bash
   # gbfs_job is UNPAUSED in prod: check raw/gbfs_status in prd_landing has a snapshot after ~30 min
   databricks bundle run -t prod setup_job        # loads the station reference
   databricks bundle run -t prod weather_job      # needs the secret; same scope works for prod
   python tools/drop_files.py --env prd --match 202502
   ```
   Watch: build_job starts with trigger **File arrival** → certify → release_job starts with trigger **Table update**. 📸 E9
6. ☐ After ≥ 8 h of snapshots (~16+ files): set `gbfs_pause: PAUSED` under `targets.prod.variables` → branch → PR → merge (CI redeploys). Don't pause it in the UI.
7. ☐ ✔ prod job names have no `[dev ...]` prefix; `prd_*` schemas exist. Day-3 checkpoint answers.

---
## DAY 4 — Scale, tune, troubleshoot

### 4.1 Backfill (start FIRST THING)
☐ ```bash
python tools/drop_files.py --env prd --batch 6 --interval 1200
```
Remaining 13 months, 3 months per drop, 20 min apart. Queued runs wait (max_concurrent_runs 1). Remember: only one pipeline at a time — don't run the dev pipeline now.
✔ `SELECT count(*) FROM workspace.prd_lakehouse.silver_trips` > 50M; reconciliation all OK (checks.sql). 📸 E2. Record pipeline duration per build run (Jobs → build → runs → build task duration).
If disk/storage is tight: delete zips in `prd_landing/raw/tripdata_zip` that are already in the manifest.

### 4.2 Tuning (while backfill runs, once Feb 2025 is in prd)
☐ Import `src/perf/perf_lab.py`, run parts 1–3 twice, record the second run + tasks from "See performance". Confirm BroadcastHashJoin vs SortMergeJoin in explain(). 📸 E11 (≥ 7 rows)

### 4.3 Skew / memory / repair
1. ☐ perf_lab part 4 → query profile before/after. 📸 E12
2. ☐ perf_lab part 5 → record the error/behaviour; fix = aggregate before collect.
3. ☐ Library failure + repair:
   ```bash
   git checkout -b drill/bad-lib
   # add as first line after the header in src/jobs/reconcile_period.py:
   #   %pip install this-package-does-not-exist==0.0.1
   databricks bundle deploy -t dev
   python tools/drop_files.py --env dev --match JC-202505
   databricks bundle run -t dev build_job           # reconcile_each fails
   # remove the line, then:
   databricks bundle deploy -t dev
   ```
   Jobs UI → failed run → **Repair run**. ✔ only reconcile_each + quality_check + gate + certify re-run; prepare and build keep their result. 📸 E13. Delete the branch.
   (The dev pipeline must not overlap the prod backfill — do this between drops or after.)

### 4.4 Layout
☐ Run `src/perf/layout_lab.sql` (replace BUSY_ID with the step-0 result). Fill the table. Check predictive optimization in Catalog Explorer → workspace → Details. 📸 E14

### 4.5 Monitoring
☐ Build job → Runs (≥5): durations, slowest run and why. Pipeline → event log / data quality trend.
☐ Blocked DAG: e.g. set `max_quarantine_pct=abc` for one manual run, or temporarily point prepare at a bad notebook path → see downstream tasks shown as "Upstream failed"/"Excluded". Write notes. Day-4 checkpoint answers.

---
## DAY 5 — Govern, serve, present
1. ☐ **5.1** `src/governance/10_access.sql` in the SQL editor. 📸 E15 (grants before/after, UNDROP).
2. ☐ **5.2** `11_entitlement_states.sql` — 3 states, 3 screenshots. 📸 E15. **End in state 3.**
3. ☐ **5.3** Create governed tags (UI), run `12_abac.sql`, test in state 2, return to state 3, refresh pipeline, re-check tags. If tags unavailable → fallback noted in the file.
4. ☐ **5.4 Dashboard**: Dashboards → Create → Data: paste each block from `src/dashboards/datasets.sql` → build 6 visuals + counter + footer → Publish (embed credentials).
   Export: dashboard ⋮ menu → **Export** → download the `.lvdash.json`
   (CLI alternative: `databricks lakeview get <dashboard_id> -o json` and save its `serialized_dashboard` field).
   Save as `src/dashboards/client_dashboard.lvdash.json`; `git mv resources/dashboard.yml.day5 resources/dashboard.yml`; uncomment `refresh_dashboard` in `resources/jobs.yml`; PR → merge → CI deploys. Delete the hand-built dashboard afterwards so only the bundle-managed one remains.
   Trigger a release (drop a small file or run build_job) → ✔ release_job refreshes the dashboard.
   Catalog Explorer → prd_gold.gold_station_flow → Lineage → graph back to bronze and forward to the dashboard. 📸 E16
   Write BQ answers (SQL + result + 2–3 sentences) in README using `src/analysis/business_answers.sql`.
5. ☐ **5.5** Draw architecture (docs/architecture.png), finish README, evidence.md, demo-script.md, record the 10-minute demo.

## Quota discipline (read twice)
- Dev = Feb 2025 + 3 small JC files only. Prod = the only full run.
- Stop the dev pipeline before any prod run. Pause gbfs after 8 h. Delete perf tables after the lab.
