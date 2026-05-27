# Databricks notebook source
# MAGIC %md
# MAGIC # IntelliOps V1 — Orchestrator
# MAGIC
# MAGIC Main entry point that runs the full **OBSERVE → PREDICT → ACT → REPORT** loop.
# MAGIC
# MAGIC Schedule this notebook as a Databricks Job (recommended: every 15–60 minutes for
# MAGIC Observe/Act, daily for Predict/Report).
# MAGIC
# MAGIC ```
# MAGIC ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
# MAGIC │ OBSERVE  │ →  │ PREDICT  │ →  │   ACT    │ →  │  REPORT  │
# MAGIC │ Features │    │ ML Models│    │ 5 Skills │    │ Dashboard│
# MAGIC └──────────┘    └──────────┘    └──────────┘    └──────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./config/config

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — Select Modules to Run

# COMMAND ----------

# Toggle individual modules (useful for partial runs / debugging)
RUN_OBSERVE = True
RUN_PREDICT = True
RUN_ACT = True
RUN_REPORT = True

# Predict models are expensive — run less frequently (e.g., daily)
# Set to False for frequent (15-min) runs
RUN_PREDICT_TRAINING = False  # Set True for daily model retraining runs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution

# COMMAND ----------

results = {}
start_time = datetime.now()

print(f"{'='*60}")
print(f" IntelliOps V1 — Pipeline Start: {start_time}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 1: OBSERVE — Refresh Feature Store

# COMMAND ----------

if RUN_OBSERVE:
    print("\n▶ Stage 1: OBSERVE — Refreshing feature store...")

    try:
        dbutils.notebook.run("./01_observe/01_feat_cluster_utilization", timeout_seconds=600)
        results["feat_cluster_utilization"] = "OK"
        print("  ✔ feat_cluster_utilization")
    except Exception as e:
        results["feat_cluster_utilization"] = f"FAILED: {e}"
        print(f"  ❌ feat_cluster_utilization: {e}")

    try:
        dbutils.notebook.run("./01_observe/02_feat_job_cost_trend", timeout_seconds=600)
        results["feat_job_cost_trend"] = "OK"
        print("  ✔ feat_job_cost_trend")
    except Exception as e:
        results["feat_job_cost_trend"] = f"FAILED: {e}"
        print(f"  ❌ feat_job_cost_trend: {e}")

    try:
        dbutils.notebook.run("./01_observe/03_feat_job_health", timeout_seconds=600)
        results["feat_job_health"] = "OK"
        print("  ✔ feat_job_health")
    except Exception as e:
        results["feat_job_health"] = f"FAILED: {e}"
        print(f"  ❌ feat_job_health: {e}")

    print("  Stage 1 complete.")
else:
    print("\n⏭ Stage 1: OBSERVE — Skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 2: PREDICT — Train/Refresh ML Models

# COMMAND ----------

if RUN_PREDICT and RUN_PREDICT_TRAINING:
    print("\n▶ Stage 2: PREDICT — Training ML models...")

    try:
        dbutils.notebook.run("./02_predict/01_cost_spike_predictor", timeout_seconds=1800)
        results["cost_spike_predictor"] = "OK"
        print("  ✔ cost_spike_predictor")
    except Exception as e:
        results["cost_spike_predictor"] = f"FAILED: {e}"
        print(f"  ❌ cost_spike_predictor: {e}")

    try:
        dbutils.notebook.run("./02_predict/02_job_failure_predictor", timeout_seconds=1200)
        results["job_failure_predictor"] = "OK"
        print("  ✔ job_failure_predictor")
    except Exception as e:
        results["job_failure_predictor"] = f"FAILED: {e}"
        print(f"  ❌ job_failure_predictor: {e}")

    print("  Stage 2 complete.")
elif RUN_PREDICT:
    print("\n⏭ Stage 2: PREDICT — Skipped (RUN_PREDICT_TRAINING=False, using existing models)")
else:
    print("\n⏭ Stage 2: PREDICT — Skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 3: ACT — Execute Agent Skills

# COMMAND ----------

if RUN_ACT:
    print("\n▶ Stage 3: ACT — Running agent skills...")

    skills = [
        ("01_cost_spike_alert", 300),
        ("02_cluster_right_sizing", 300),
        ("03_job_failure_scorer", 300),
        ("04_wasteful_compute_detector", 300),
        ("05_budget_forecast", 300),
    ]

    for skill_name, timeout in skills:
        try:
            dbutils.notebook.run(f"./03_act/{skill_name}", timeout_seconds=timeout)
            results[skill_name] = "OK"
            print(f"  ✔ {skill_name}")
        except Exception as e:
            results[skill_name] = f"FAILED: {e}"
            print(f"  ❌ {skill_name}: {e}")

    print("  Stage 3 complete.")
else:
    print("\n⏭ Stage 3: ACT — Skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 4: REPORT — Refresh Dashboards

# COMMAND ----------

if RUN_REPORT:
    print("\n▶ Stage 4: REPORT — Refreshing dashboards...")

    reports = [
        ("01_cost_command_center", 300),
        ("02_cluster_health_map", 300),
        ("03_job_reliability", 300),
        ("04_optimization_leaderboard", 300),
    ]

    for report_name, timeout in reports:
        try:
            dbutils.notebook.run(f"./04_report/{report_name}", timeout_seconds=timeout)
            results[report_name] = "OK"
            print(f"  ✔ {report_name}")
        except Exception as e:
            results[report_name] = f"FAILED: {e}"
            print(f"  ❌ {report_name}: {e}")

    print("  Stage 4 complete.")
else:
    print("\n⏭ Stage 4: REPORT — Skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Summary

# COMMAND ----------

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()
passed = sum(1 for v in results.values() if v == "OK")
failed = sum(1 for v in results.values() if v != "OK")

print(f"\n{'='*60}")
print(f" IntelliOps V1 — Pipeline Complete")
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

print(f"\nNext run: configure as Databricks Job with {FEATURE_REFRESH_INTERVAL_MINUTES}-min schedule")
print(f"{'='*60}")
