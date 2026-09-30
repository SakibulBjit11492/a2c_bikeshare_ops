# Databricks notebook source
# src/jobs/raise_incident.py — the "false" branch: record why, then fail the run on purpose
dbutils.widgets.text("catalog", "workspace"); dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_id", "manual"); dbutils.widgets.text("quarantine_pct", "")
dbutils.widgets.text("reason_detail", "")
OPS = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('env')}_ops"
reason = (f"quality gate failed (quarantine {dbutils.widgets.get('quarantine_pct')}%; "
          f"{dbutils.widgets.get('reason_detail')}; see ops.reconciliation)")
spark.sql("INSERT INTO IDENTIFIER(:t) VALUES (:run_id, :reason, current_timestamp())",
          args={"t": f"{OPS}.incidents", "run_id": dbutils.widgets.get("run_id"), "reason": reason})
raise RuntimeError(reason)   # a failed run triggers the on_failure e-mail
