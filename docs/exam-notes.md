# Exam notes — 27 checkpoint questions
> Drafts to get you started. Rewrite each in your own words and add what YOU saw in this project —
> the walkthrough reviewer will ask you to explain them.

## Day 1
1. **Same COPY INTO twice?** Second run loads 0 rows: COPY INTO tracks loaded files per target table and skips them (idempotent). `'force'='true'` reloads. *Seen: E5.*
2. **Auto Loader vs COPY INTO?** Auto Loader for many/continuous files, streaming tables, schema inference + evolution, rescued data (trip CSVs, GBFS status). COPY INTO for simple re-runnable SQL batch loads of a manageable number of files (station reference).
3. **SaaS with managed connector vs internal REST?** SaaS → Lakeflow Connect managed connector (governed incremental pipeline into UC). REST → notebook with a REST client, token from a secret scope, land raw JSON in a Volume, orchestrate with a job (weather_rest.py).
4. **Where does a token belong?** Secret scope, read with `dbutils.secrets.get`; printing shows `[REDACTED]`. Redaction is cosmetic; secret ACLs are the control.
5. **Compute choices?** Ad-hoc → interactive/serverless notebook; nightly ETL → job compute (serverless jobs/job cluster, ends with the run, cheaper); BI → (serverless) SQL warehouse. `spark.executor.memory` can't be set because serverless manages infrastructure; only 6 confs are allowed.

## Day 2
6. **expect / expect_or_drop / expect_or_fail?** Keep + record metric / remove bad rows / stop the update and roll back.
7. **ST vs MV vs view for BI?** ST: append-only, each input row once (Bronze, row-level Silver). MV: must reflect all source changes — aggregates, joins, de-dup (all Gold). View: no storage, computed at query time; temporary view is pipeline-private.
8. **addNewColumns meets a new column?** Stream fails with UnknownFieldException after merging the column into the schema; on restart it continues with the column (NULL for old rows); existing types unchanged. Prod pipelines/job retries restart automatically. *Seen: drift drill.*
9. **_rescued_data?** Values that didn't fit the schema — unknown fields, type or case mismatches — as JSON with the source file path.
10. **Why de-dup in an MV?** Global de-dup in a stream needs unbounded state or a watermark that can drop late rows silently; an MV sees the whole data set each refresh, so it's always correct.

## Day 3
11. **Unpredictable partner drops?** File-arrival trigger. Cron runs too early (empty/partial) or too late (stale) and wastes compute.
12. **Run when a certified table changes?** Table-update trigger (ANY_UPDATED / ALL_UPDATED). *release_job on ops.release.*
13. **mode: development?** Prefixes names `[dev user]`, pauses schedules/triggers, pipelines in development mode, allows concurrent runs, no deployment lock.
14. **Variable vs target override?** Variable = named value substituted into definitions, per-target values (env → dev_/prd_). Override = target-specific settings merged over the resource (prod-only on_success e-mail).
15. **Failed in task 4 of 7 — repair run re-runs?** Only unsuccessful tasks and their dependents; successful tasks keep results; uses current job settings; parameters can be overridden. *Seen: E13.*
16. **Git folder — where is the PR?** In GitHub. Git folders do clone/branch/commit/push/pull/merge/rebase/conflict resolution.

## Day 4
17. **Skew in metrics, and fix?** A few tasks much longer / many more rows than median, often spill; DATA_SKEW insight. Fix: better keys, salting, broadcast the small side, AQE skew-join splitting, pre-aggregate. *Seen: window by member_type → add start_date.*
18. **shuffle.partitions vs default.parallelism?** Partitions after DataFrame/SQL shuffles (auto on serverless) vs default partitions for RDD ops.
19. **autoBroadcastJoinThreshold?** Max size auto-broadcast in joins (10 MB default, -1 off); not settable on serverless → BROADCAST hint.
20. **Liquid Clustering / partitioning / Z-ORDER?** LC = default: incremental, changeable keys, high cardinality. Partitioning only for low-cardinality columns with ~1 GB+ partitions. Z-ORDER legacy, full OPTIMIZE rewrites. LC can't combine with either.
21. **Predictive optimization?** Automatically runs OPTIMIZE, VACUUM, ANALYZE on UC managed tables with serverless; with CLUSTER BY AUTO chooses keys.
22. **Cluster won't start / library conflict?** Cluster event log (init scripts, quotas, capacity); driver logs + task error for libraries; pin/remove the dependency. Serverless: check the environment's dependency list. *Seen: fake %pip.*

## Day 5
23. **DROP managed vs external?** Managed: UC deletes data after retention (UNDROP within 7 days). External: only metadata removed, files remain.
24. **Minimum privileges to query?** USE CATALOG + USE SCHEMA + SELECT (direct or inherited).
25. **Row filter vs column mask vs ABAC?** Filter decides rows; mask transforms a column's value; ABAC policy defined once at catalog/schema applies filters/masks to every table with matching governed tags.
26. **DENY vs GRANT?** DENY wins over all grants (inherited, group, ownership), metastore admins exempt; Beta, only MANAGE ACCESS CONTROL, not shown by SHOW GRANTS, needs DBR 18 LTS classic.
27. **REVOKE schema SELECT but user still reads?** Privilege via another path: catalog- or table-level grant, a group, or ownership. Check SHOW GRANTS at each level.
