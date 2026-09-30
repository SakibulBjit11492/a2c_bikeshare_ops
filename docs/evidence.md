# Evidence pack (E1–E16)
Paste screenshots into `docs/img/` and link them, or paste numbers/tables inline.

| # | Evidence | Where it comes from | Result / link |
|---|---|---|---|
| E1 | Smoke-test results | tools/00_smoke_test.py output | |
| E2 | Reconciliation for every period | checks.sql reconciliation query (prd), totals vs Section 3 | |
| E3 | Schema-evolution event | failed + restarted pipeline update, feed_version query, _rescued_data | |
| E4 | Expectation metrics | pipeline Data quality tab; event_log query; fail drill screenshot | |
| E5 | COPY INTO second run = 0 rows | load_reference.py output, 2nd run | |
| E6 | Secret redaction | `token: [REDACTED]` | |
| E7 | Build job DAG and green run | Jobs UI run graph incl. for-each iterations | |
| E8 | Incident branch run | run with max_quarantine_pct=-1, ops.incidents row, e-mail | |
| E9 | File-arrival and table-update triggered runs | prod build run (trigger: File arrival) + release run (Table update) | |
| E10 | GitHub Actions runs | PR validate + merge deploy; Git folder PR + conflict resolution | |
| E11 | Tuning results | perf_lab parts 1–3 table + explain() plans | |
| E12 | Skew before/after | query profiles of skewed vs fixed window | |
| E13 | Repair run | repair run showing only failed task + dependents | |
| E14 | Layout comparison | layout_lab table | |
| E15 | Governance | 3 entitlement states; SHOW GRANTS before/after REVOKE; SHOW POLICIES; UNDROP | |
| E16 | Lineage graph | Catalog Explorer lineage bronze → gold → dashboard | |
