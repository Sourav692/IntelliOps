# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3: Act — Skill 3: Job Failure Risk Scorer
# MAGIC
# MAGIC **Trigger:** Any job run starts (or scheduled batch scan)
# MAGIC
# MAGIC **Agent Actions:**
# MAGIC 1. Scores failure probability based on historical rate, cluster pressure, duration anomaly
# MAGIC 2. If score > threshold: proactively alerts team before failure occurs
# MAGIC 3. After failure: classifies likely cause from run timeline patterns

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# MAGIC %run ../utils/notifications

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Risk Scores & Identify High-Risk Jobs

# COMMAND ----------

df_high_risk = spark.sql(f"""
    SELECT
        s.workspace_id,
        s.job_id,
        s.job_name,
        s.failure_risk_score,
        s.failure_rate,
        s.cluster_avg_cpu,
        s.cluster_peak_mem,
        s.total_runs
    FROM {FULL_SCHEMA}.job_failure_risk_scores s
    WHERE s.failure_risk_score > {JOB_FAILURE_RISK_THRESHOLD}
    ORDER BY s.failure_risk_score DESC
""")

high_risk_jobs = df_high_risk.collect()
print(f"High-risk jobs (score > {JOB_FAILURE_RISK_THRESHOLD}): {len(high_risk_jobs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Classify Failure Causes for Recently Failed Jobs

# COMMAND ----------

df_recent_failures = spark.sql(f"""
    SELECT
        r.workspace_id,
        r.job_id,
        j.name AS job_name,
        r.result_state,
        r.period_start_time,
        r.period_end_time,
        TIMESTAMPDIFF(SECOND, r.period_start_time, r.period_end_time) AS duration_secs
    FROM {SYS_LAKEFLOW_JOB_RUNS} r
    LEFT JOIN {SYS_LAKEFLOW_JOBS} j
        ON r.workspace_id = j.workspace_id AND r.job_id = j.job_id
    WHERE r.result_state = 'FAILED'
      AND r.period_start_time >= CURRENT_DATE - INTERVAL 3 DAYS
    ORDER BY r.period_start_time DESC
""")

recent_failures = df_recent_failures.collect()
print(f"Recent failures (last 3 days): {len(recent_failures)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Alerts & Classify Causes

# COMMAND ----------

actions = []

# ── Proactive alerts for high-risk jobs ────────────────────────────────────
for job in high_risk_jobs:
    risk_score = job["failure_risk_score"]
    j_name = job["job_name"] or f"job_{job['job_id']}"

    # Determine risk factors
    risk_factors = []
    if job["failure_rate"] and job["failure_rate"] > 0.2:
        risk_factors.append(f"high historical failure rate ({job['failure_rate']*100:.0f}%)")
    if job["cluster_avg_cpu"] and job["cluster_avg_cpu"] > 80:
        risk_factors.append(f"cluster under CPU pressure ({job['cluster_avg_cpu']:.0f}%)")
    if job["cluster_peak_mem"] and job["cluster_peak_mem"] > 90:
        risk_factors.append(f"cluster near memory limit ({job['cluster_peak_mem']:.0f}%)")

    description = (
        f"Job '{j_name}' has high failure risk (score: {risk_score:.2f}). "
        f"Risk factors: {', '.join(risk_factors) if risk_factors else 'composite risk from multiple signals'}."
    )

    action = {
        "action_id": str(uuid.uuid4()),
        "action_timestamp": datetime.now().isoformat(),
        "skill_name": "job_failure_scorer",
        "action_type": "alert",
        "workspace_id": job["workspace_id"],
        "target_id": job["job_id"],
        "target_name": j_name,
        "description": description,
        "projected_savings": 0.0,
        "status": "proposed",
        "details": json.dumps({
            "risk_score": round(risk_score, 3),
            "failure_rate": round(job["failure_rate"], 3) if job["failure_rate"] else 0,
            "risk_factors": risk_factors,
        }),
    }
    actions.append(action)

    # Send notification
    notify(
        title=f"High Failure Risk: {j_name}",
        message=description,
        severity="warning",
        details={"Risk Score": f"{risk_score:.2f}", "Total Runs": job["total_runs"]},
    )
    print(f"  ⚠️ {description}")

# ── Post-failure classification ────────────────────────────────────────────
for failure in recent_failures:
    j_name = failure["job_name"] or f"job_{failure['job_id']}"
    duration = failure["duration_secs"]

    # Get historical stats for this job
    health = spark.sql(f"""
        SELECT avg_duration_secs, stddev_duration
        FROM {TABLE_JOB_HEALTH}
        WHERE workspace_id = '{failure["workspace_id"]}' AND job_id = '{failure["job_id"]}'
    """).first()

    # Classify failure cause
    cause = "unknown"
    if health and health["avg_duration_secs"] and health["stddev_duration"]:
        threshold = health["avg_duration_secs"] + JOB_DURATION_ANOMALY_SIGMA * health["stddev_duration"]
        if duration and duration > threshold:
            cause = "timeout_or_resource_pressure"
        elif duration and duration < health["avg_duration_secs"] * 0.3:
            cause = "early_termination_or_dependency_failure"
        else:
            cause = "runtime_error"
    elif duration and duration < 60:
        cause = "configuration_or_dependency_failure"

    description = (
        f"Job '{j_name}' failed at {failure['period_start_time']}. "
        f"Likely cause: {cause.replace('_', ' ')}."
    )

    action = {
        "action_id": str(uuid.uuid4()),
        "action_timestamp": datetime.now().isoformat(),
        "skill_name": "job_failure_scorer",
        "action_type": "alert",
        "workspace_id": failure["workspace_id"],
        "target_id": failure["job_id"],
        "target_name": j_name,
        "description": description,
        "projected_savings": 0.0,
        "status": "proposed",
        "details": json.dumps({
            "failure_time": str(failure["period_start_time"]),
            "duration_secs": duration,
            "classified_cause": cause,
        }),
    }
    actions.append(action)
    print(f"  🔴 {description}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Log Actions

# COMMAND ----------

if actions:
    df_actions = spark.createDataFrame(actions)
    df_actions.write.format("delta").mode("append").saveAsTable(TABLE_AGENT_ACTIONS)
    print(f"\n✔ Logged {len(actions)} failure risk alert(s)")
else:
    print("✔ No high-risk jobs or recent failures detected.")
