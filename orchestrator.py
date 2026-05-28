# Databricks notebook source
# MAGIC %md
# MAGIC # IntelliOps V2 — Orchestrator
# MAGIC
# MAGIC Runs the **scheduled** work only. The support agent itself is event-driven
# MAGIC (Slack / Databricks App) and is **not** invoked from this notebook.
# MAGIC
# MAGIC ```
# MAGIC ┌──────────┐    ┌────────────┐    ┌──────────┐
# MAGIC │ OBSERVE  │ →  │ KNOWLEDGE  │ →  │  REPORT  │
# MAGIC │ Features │    │ RAG index  │    │ Dashboard│
# MAGIC └──────────┘    └────────────┘    └──────────┘
# MAGIC ```
# MAGIC
# MAGIC Recommended cadence:
# MAGIC - Observe   : every 15 min
# MAGIC - Knowledge : weekly
# MAGIC - Report    : daily

# COMMAND ----------

# MAGIC %run ./config/config

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — Select Stages to Run

# COMMAND ----------

RUN_OBSERVE = True
RUN_KNOWLEDGE = False   # Toggle True on the weekly run
RUN_REPORT = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution

# COMMAND ----------

results = {}
start_time = datetime.now()

print(f"{'='*60}")
print(f" IntelliOps V2 — Pipeline Start: {start_time}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 1: OBSERVE — Refresh Feature Store

# COMMAND ----------

if RUN_OBSERVE:
    print("\n▶ Stage 1: OBSERVE — Refreshing feature store...")

    observe_notebooks = [
        "01_feat_cluster_utilization",
        "02_feat_job_cost_trend",
        "03_feat_job_health",
    ]

    for nb in observe_notebooks:
        try:
            dbutils.notebook.run(f"./01_observe/{nb}", timeout_seconds=600)
            results[nb] = "OK"
            print(f"  ✔ {nb}")
        except Exception as e:
            results[nb] = f"FAILED: {e}"
            print(f"  ❌ {nb}: {e}")

    print("  Stage 1 complete.")
else:
    print("\n⏭ Stage 1: OBSERVE — Skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 2: KNOWLEDGE — Refresh RAG Index

# COMMAND ----------

if RUN_KNOWLEDGE:
    print("\n▶ Stage 2: KNOWLEDGE — Rebuilding RAG index...")
    # Placeholder until 02_knowledge is implemented.
    # Expected entry point: ./02_knowledge/01_build_knowledge_index
    try:
        dbutils.notebook.run("./02_knowledge/01_build_knowledge_index", timeout_seconds=1800)
        results["knowledge_index"] = "OK"
        print("  ✔ knowledge_index")
    except Exception as e:
        results["knowledge_index"] = f"FAILED: {e}"
        print(f"  ❌ knowledge_index: {e}")

    print("  Stage 2 complete.")
else:
    print("\n⏭ Stage 2: KNOWLEDGE — Skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 3: REPORT — Refresh Dashboards

# COMMAND ----------

if RUN_REPORT:
    print("\n▶ Stage 3: REPORT — Refreshing dashboards...")

    reports = [
        "01_cost_command_center",
        "02_cluster_health_map",
        "03_job_reliability",
        "04_optimization_leaderboard",
    ]

    for report_name in reports:
        try:
            dbutils.notebook.run(f"./07_report/{report_name}", timeout_seconds=300)
            results[report_name] = "OK"
            print(f"  ✔ {report_name}")
        except Exception as e:
            results[report_name] = f"FAILED: {e}"
            print(f"  ❌ {report_name}: {e}")

    print("  Stage 3 complete.")
else:
    print("\n⏭ Stage 3: REPORT — Skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Summary

# COMMAND ----------

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()
passed = sum(1 for v in results.values() if v == "OK")
failed = sum(1 for v in results.values() if v != "OK")

print(f"\n{'='*60}")
print(f" IntelliOps V2 — Pipeline Complete")
print(f"{'='*60}")
print(f"  Duration:  {duration:.0f}s ({duration/60:.1f} min)")
print(f"  Passed:    {passed}/{len(results)}")
print(f"  Failed:    {failed}/{len(results)}")
print(f"{'='*60}")

if failed > 0:
    print("\nFailed steps:")
    for step, status in results.items():
        if status != "OK":
            print(f"  ❌ {step}: {status}")

print(f"\nNote: the support agent (03_agent) is event-driven via 06_interface")
print(f"      and is intentionally not invoked from the orchestrator.")
print(f"{'='*60}")
