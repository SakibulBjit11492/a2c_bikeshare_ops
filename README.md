# a2c_bikeshare_ops — Bike-share fleet operations lakehouse (Assignment 2C)

Riverline Urban Planning (fictional) → city transport department. A Databricks lakehouse over Citi Bike trip
histories (NYC + Jersey City, May 2024 – Jun 2025, ~55M trips), the live GBFS station feed and NOAA Central Park
weather. Every trip count reconciles to the monthly source files; new months flow through automatically;
the Jersey City partner sees only JC rows; coordinates are generalised unless the reader is entitled.

> Data: Citi Bike public system data (Lyft Bikes and Scooters, Citi Bike Data License Agreement) and NOAA NCEI.
> Raw files are never redistributed. No logos. Not endorsed by Citi Bike or Lyft.

**Step-by-step build guide: [docs/RUNBOOK.md](docs/RUNBOOK.md)** · Evidence: [docs/evidence.md](docs/evidence.md) ·
Demo: [docs/demo-script.md](docs/demo-script.md) · Exam notes: [docs/exam-notes.md](docs/exam-notes.md) ·
Dashboard: `<published dashboard link>`

---
## How to run it (clone → prod)
1. Install the Databricks CLI; `databricks configure` with your workspace URL and a PAT.
2. Edit `databricks.yml`: both `host:` values, `notify_email`, the warehouse name.
3. `databricks bundle deploy -t prod && databricks bundle run -t prod setup_job`
   (first deploy only: if a trigger target doesn't exist yet, see *Bootstrap* below).
4. `databricks secrets create-scope a2 && databricks secrets put-secret a2 noaa_token`
5. `databricks bundle run -t prod weather_job`; wait for the first GBFS snapshot (gbfs_job, every 30 min).
6. Land data: `pip install requests databricks-sdk && python tools/drop_files.py --env prd --batch 6 --interval 1200`.
   Each drop fires `build_job` → pipeline → reconciliation → quality gate → certify → `release_job` → dashboard.
7. CI/CD: add `DATABRICKS_HOST` / `DATABRICKS_TOKEN` as GitHub secrets; PRs validate, merges to `main` deploy dev then prod.

**Bootstrap:** `<needed / not needed>` — on the first deploy of a target the file-arrival folder and `ops.release`
don't exist yet; deploy without the two trigger blocks, run `setup_job`, restore, redeploy.

---
## Environment findings (smoke test, Day 1)
| Check | Result | Consequence |
|---|---|---|
| s3.amazonaws.com (trip files) | reachable (HTTP 200) | Simulator still runs on laptop (design choice: it plays the upstream system) |
| gbfs.citibikenyc.com | reachable (HTTP 200) | gbfs_job and gbfs_poll.py run in the workspace |
| NOAA CDO API v2 | reachable (HTTP 400 — no token sent, host is reachable) | weather_job / weather_rest.py run in the workspace |
| NOAA Access Data Service | reachable (HTTP 200) | Fallback available if CDO has problems; not needed |
| PyPI | reachable (HTTP 200) | Control check passed |
| session_user() | sakibul.alam@bjitgroup.com | Entitlements row + row-filter functions match this exact value |
| Secrets (dbutils.secrets) | available, no scopes yet ([]) | Create scope `a2` in Task 1.4 |
| Row filters on serverless | works (only A returned) | gold_trip_detail_recent row filter + masks will work on Day 5 |
| Governed tags | `<check by hand>` | `<ABAC policies / per-table fallback>` |
| Invite a teammate | `<check by hand>` | `<teammate / account users>` for GRANT |
| Service principal + secret | `<check by hand>` | `<PAT / OAuth M2M in GitHub Actions>` |

---
## Architecture
![architecture](docs/architecture.png)

`landing (Volume raw/)` → `prepare` (unzip + manifest) → **Lakeflow declarative pipeline** (Bronze Auto Loader
streaming tables → Silver conform/expectations/quarantine/de-dup → Gold MVs) → `reconcile_each` (for-each per period)
→ `quality_check` → `gate` → `certify` (ops.release) → table-update trigger → `release_job` (status SQL + dashboard).

| Schema | Contents |
|---|---|
| `<env>_landing` | Volume `raw`: tripdata_zip, tripdata_csv, gbfs_status, reference, weather |
| `<env>_lakehouse` | bronze_trips, bronze_station_status, bronze_weather, silver_trips_clean, silver_trips_quarantine, silver_trips (MV), silver_station_status (ST), silver_weather_daily (MV), dim_station (MV), ref_station_information (COPY INTO) |
| `<env>_gold` | gold_demand_hourly, gold_weather_demand, gold_station_flow, gold_ride_behaviour, gold_station_health, gold_trip_detail_recent |
| `<env>_ops` | file_manifest, reconciliation, release, release_status, incidents, entitlements; rf_scope, mask_text, mask_coord |

No code is hard-coded to dev/prd: catalog and prefix come from bundle variables → job parameters / pipeline configuration.

---
## Ingestion decisions
| Source | Method | Why |
|---|---|---|
| Trip zips (NYC + JC) | Simulator → Volume → `prepare` unzips → **Auto Loader (CSV)** streaming table | Many large files arriving over time; incremental, exactly-once, schema hints + evolution, rescued data |
| GBFS station_information | REST notebook → Volume → **COPY INTO** | Small daily reference snapshot; idempotent SQL batch load; re-runs load 0 rows |
| GBFS station_status | REST notebook (cron 30 min) → Volume → **Auto Loader (JSON)** → streaming table | Continuous stream of new files; append-only |
| NOAA daily weather | REST notebook + **secret scope** → Volume → Auto Loader (JSON) → MV keeps latest fetch | API needs a token; restatements handled by "latest fetch wins" |
| (SaaS with managed connector) | Lakeflow Connect | Not used; Jira connector is stretch goal S1 |

---
## Data-quality rules (Silver)
Profile (dev, Feb 2025): rows `<>`, null ride_id `<>`, null start station `<>` (`<>`% — mostly e-bikes), null end coords `<>`,
member values `<member, casual>`.

| Rule | Behaviour | Threshold | Reason | Rows affected (prd) |
|---|---|---|---|---|
| has_source_file | fail | `_source_file IS NOT NULL` | Structural invariant: lineage broken → stop | 0 |
| ride_id_present | drop | not null | No key, no trip | `<>` |
| ended_after_started | drop | ended > started | Impossible trip | `<>` |
| duration_1min_to_24h | drop | 1–1440 min | Publisher already removed < 60 s; > 24 h = lost/unreturned bike, an ops question | `<>` |
| member_type_valid | drop | member/casual | Every question splits by rider type | `<>` |
| period_matches_file | warn | period = file_period | Month-end trips spill into the next file; real trips | `<>` |
| coords_in_area | warn | lat 40.4–41.0, lng −74.3…−73.6 | GPS glitches; keep but count | `<>` |
| stations_present | warn | both ids not null | Dockless e-bike trips are real trips | `<>` |

Drop rules are wrapped in `coalesce(rule, false)` so NULL fails; quarantine = exact complement (`NOT (all drop rules)`)
with a `failed_rules` array, so **clean + quarantine = bronze** per file. De-duplication on `ride_id` in the
`silver_trips` MV removed `<n>` rows (in prd: `<n>`; the drift drill's 1,000 copied rows in dev).
Fail behaviour proven once in dev (`drill_no_electric_bikes`) — see E4.

Schema evolution (E3): `feed_version` arrived → update stopped with `UnknownFieldException` → restart → column added,
NULL for earlier rows; `_rescued_data` held `<...>`.

---
## Gold objects — why each is what it is
| Object | Type | Why |
|---|---|---|
| gold_demand_hourly | MV, CLUSTER BY start_date | Aggregation must reflect de-dup and late files; date clustering prunes time-range queries |
| gold_weather_demand | MV | Join of trips and weather; a re-fetched weather value must update past days |
| gold_station_flow | MV | Full outer join of departures/arrivals and a global weekday count needs the whole data set |
| gold_ride_behaviour | MV | Percentiles aren't additive; whole-window rows (period='ALL') via GROUPING SETS |
| gold_station_health | MV | Joins snapshots → dim_station → trips on different keys; recomputed as snapshots arrive |
| gold_trip_detail_recent | MV + row filter + masks | Rolling 30-day window (rows leave the window) → not append-only; governance declared in the definition so refreshes keep it |
| (streaming tables) | Bronze, silver_trips_clean/quarantine, silver_station_status | Append-only, each input row processed once |
| (views) | v_trips_conformed (temporary) | Reusable logic, no storage, private to the pipeline |

---
## Orchestration
DAG: `prepare → has_new → build → reconcile_each (for-each, concurrency 2) → quality_check → gate → certify | raise_incident`.
Retries: `prepare` 2× after 60 s (transient Volume/IO errors, safe because prepare is idempotent); `build` 1× after 120 s
(also auto-restarts after an addNewColumns schema change). Failure e-mail on every job; prod adds success e-mail (target override).

| Job | Trigger | Why | Failure mode of the alternative |
|---|---|---|---|
| build_job | File arrival on raw/tripdata_zip/ | Files arrive when upstream is ready | Cron: runs before data (empty/partial run) or long after (stale), wastes compute |
| release_job | Table update on ops.release | Only after a certified change | Cron/file trigger: could publish uncertified or half-built data |
| weather_job | Cron daily 05:30 Dhaka | NOAA doesn't push; data is daily | File arrival: nothing to watch — no one drops files |
| gbfs_job | Cron every 30 min | Feed is pull-only | Continuous streaming isn't possible from a public REST feed |

---
## CI/CD
Git folder: branch `feature/gold-kpis`, PR #`<n>`, conflict resolved in the Git folder UI (E10).
One bundle, two targets: `env` variable → `dev_*` vs `prd_*`; prod override adds `on_success` e-mail to build_job.
GitHub Actions: PR → validate dev+prod (+ secret scan); push to main → deploy dev → deploy prod → setup_job.
Auth: PAT in GitHub secrets (Free Edition has no account-level OIDC). **For a client:** a service principal with
OIDC workload-identity federation (no stored secret), `run_as` the SP in prod, a GitHub Environment with required reviewers.

---
## Performance & troubleshooting
Backfill: `<n>` build runs, pipeline durations `<...>`, total `<...>`.

| Setting | Duration (2nd run) | Tasks | Join strategy |
|---|---|---|---|
| shuffle.partitions=auto | | | |
| shuffle.partitions=8 | | | |
| shuffle.partitions=4000 | | | |
| maxPartitionBytes=128MB | | | |
| maxPartitionBytes=16MB | | | |
| broadcast hint | | | BroadcastHashJoin |
| merge hint | | | SortMergeJoin |

`<what auto chose; why 8 was slower/faster; what you'd change on classic: autoBroadcastJoinThreshold, executor/driver memory, default.parallelism>`

Skew: `<two-sentence diagnosis>`. Driver memory: `<what happened, fix = aggregate before collect>`. Repair run: `<which tasks re-ran>`.

| Layout | numFiles | sizeInBytes | files read | bytes read | duration |
|---|---|---|---|---|---|
| PARTITIONED BY start_date | | | | | |
| CLUSTER BY (start_date, start_station_id) | | | | | |

`<which won; why over-partitioning hurts (tiny files, metadata overhead); predictive optimization runs OPTIMIZE/VACUUM/ANALYZE, picks keys with CLUSTER BY AUTO; status in this catalog: >`

Monitoring notes: `<duration trend; slowest run + cause; expectation pass-rate trend; what a blocked DAG looked like>`

---
## Access model
| Who | Grants | Row scope | Sensitive |
|---|---|---|---|
| Owner / engineer | owner | `*` | yes |
| Analysts (`account users` / teammate) | USE CATALOG, USE SCHEMA + SELECT on `prd_gold` only | per entitlements row | per row |
| Jersey City partner | same | `JC` | no → ride_id pseudonymised, coords rounded to 3 dp (~100 m) |

`gold_trip_detail_recent`: manual row filter `rf_scope(system)` + masks, declared in gold.sql.
Other Gold objects: ABAC — tags `sensitivity=location` on lat/lng, `access_scope=partner` on system; policies
`generalise_locations` (mask) and `partner_rows` (row filter) on the schema. Tags survived pipeline refresh: `<yes/no>`.
Why ABAC scales: one policy per rule per schema, applied automatically to every present and future tagged column, instead
of N ALTERs per table that someone must remember. DENY: always wins over GRANTs (incl. inherited, group, ownership);
today only MANAGE ACCESS CONTROL can be denied, needs DBR 18 LTS classic — not in Free Edition.
Managed vs external: DROP of a managed table deletes data after the retention window (UNDROP within 7 days); DROP of an
external table removes only metadata, files stay in your location; Free Edition has only managed storage.

---
## Business answers
(SQL: `src/analysis/business_answers.sql`, all on reconciled prd tables.)

**BQ1 — When and how does the city ride?** `<2–3 sentences: seasonality, commute peaks weekday 8 & 17–18 vs weekend midday, member share, e-bike share trend, rain effect within month with #days per class>`

**BQ2 — Which stations drain, and which fill?** `<top drains AM/PM, top fillers; excluded n trips without station id, mostly electric_bike>`

**BQ3 — How do people ride?** `<median/p90 by bike×rider; casual rides longer; round-trip share; >45 min share; truncated at 1 min>`

**BQ4 — Are the busiest stations the ones that run out?** `<match rate x%; snapshots cover h hours on date; top busy stations often empty/full; correlation>`

---
## What I would change for a real client
Service principal + OIDC; paid workspace with classic job clusters where tuning needs it; external location for landing;
alerting via SQL alerts/Slack; GBFS on a streaming ingest with a proper history; SCD2 station dimension; unit tests in CI.

## Three things I learned
1. `<>`
2. `<>`
3. `<>`

