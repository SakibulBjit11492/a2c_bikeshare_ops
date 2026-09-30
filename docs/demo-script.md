# 10-minute demo script

**0:00–2:00 · Problem and answers.** "The city needs to know where bikes pile up and where stations run dry, and when."
Open the dashboard: heatmap (commute peaks), e-bike share line, drain/fill map (AM vs PM), busy-and-empty table. One sentence per BQ.

**2:00–5:00 · How the data gets there.** Architecture diagram. Landing Volume → prepare (unzip, manifest with expected rows) →
pipeline graph (Bronze Auto Loader → Silver expectations + quarantine + de-dup → Gold MVs) → `ops.reconciliation`
(every period OK, expected = bronze = clean + quarantined) → quality gate → certify.

**5:00–7:00 · Live proof.** `python tools/drop_files.py --env prd --match <a file not yet dropped>` (keep one JC month back,
or re-run with a fresh small file) → build_job starts, trigger "File arrival" → gate → release job "Table update" → dashboard counter updates.
(If the run is slow, show the previous run and let the new one finish during Q&A.)

**7:00–9:00 · Trust.** Same query in state 1 vs state 2 (JC only, ids pseudonymised, coords rounded). SHOW POLICIES on prd_gold,
tagged columns. Lineage graph from bronze to the dashboard.

**9:00–10:00 · How it ships.** A merged PR → GitHub Actions validate → deploy dev → deploy prod. One command for the client:
`databricks bundle deploy -t <their target>`.
