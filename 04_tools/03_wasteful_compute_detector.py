# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3: Act — Skill 4: Wasteful Compute Detector
# MAGIC
# MAGIC **Trigger:** Scheduled daily scan
# MAGIC
# MAGIC **Agent Actions:**
# MAGIC 1. Finds jobs running on all-purpose compute instead of job compute (higher DBU rate)
# MAGIC 2. Identifies clusters idle for > 2 hours with no attached notebooks
# MAGIC 3. Detects clusters with 0 active tasks but still running
# MAGIC 4. Outputs ranked list: "Top 5 changes that would save $X this week"

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
# MAGIC ## Check 1: Jobs on All-Purpose Compute (Should Be Job Compute)
# MAGIC All-purpose compute has a higher DBU rate. Jobs should use dedicated job compute.

# COMMAND ----------

df_wrong_compute = spark.sql(f"""
    SELECT DISTINCT
        t.workspace_id,
        t.job_id,
        j.name                          AS job_name,
        c.cluster_id,
        c.cluster_name,
        c.cluster_source,
        COUNT(*) OVER (
            PARTITION BY t.workspace_id, t.job_id
        )                               AS recent_runs
    FROM {SYS_LAKEFLOW_JOB_TASKS} t
    JOIN {SYS_COMPUTE_CLUSTERS} c
        ON t.workspace_id = c.workspace_id
        AND t.compute_id = c.cluster_id
    LEFT JOIN {SYS_LAKEFLOW_JOBS} j
        ON t.workspace_id = j.workspace_id
        AND t.job_id = j.job_id
    WHERE c.cluster_source = 'UI'
      AND t.period_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    ORDER BY recent_runs DESC
""")

wrong_compute_jobs = df_wrong_compute.collect()
print(f"Jobs on all-purpose compute: {len(wrong_compute_jobs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2: Idle Clusters (No Activity for 2+ Hours)

# COMMAND ----------

df_idle = spark.sql(f"""
    WITH cluster_latest_activity AS (
        SELECT
            cluster_id,
            workspace_id,
            MAX(hour_window) AS last_active_hour,
            AVG(avg_cpu_pct) AS recent_cpu
        FROM {TABLE_CLUSTER_UTILIZATION}
        WHERE hour_window >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
        GROUP BY cluster_id, workspace_id
    ),
    running_clusters AS (
        SELECT cluster_id, workspace_id, cluster_name
        FROM {SYS_COMPUTE_CLUSTERS}
        WHERE delete_time IS NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY cluster_id, workspace_id
            ORDER BY change_time DESC
        ) = 1
    )
    SELECT
        rc.cluster_id,
        rc.workspace_id,
        rc.cluster_name,
        ca.last_active_hour,
        ROUND(ca.recent_cpu, 1) AS recent_cpu,
        TIMESTAMPDIFF(HOUR, ca.last_active_hour, CURRENT_TIMESTAMP) AS idle_hours
    FROM running_clusters rc
    LEFT JOIN cluster_latest_activity ca
        ON rc.cluster_id = ca.cluster_id
        AND rc.workspace_id = ca.workspace_id
    WHERE ca.recent_cpu < 5
       OR ca.last_active_hour < CURRENT_TIMESTAMP - INTERVAL {CLUSTER_IDLE_HOURS} HOURS
       OR ca.last_active_hour IS NULL
    ORDER BY idle_hours DESC
""")

idle_clusters = df_idle.collect()
print(f"Idle clusters (>{CLUSTER_IDLE_HOURS}h): {len(idle_clusters)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3: Zero-Task Running Clusters

# COMMAND ----------

df_zero_tasks = spark.sql(f"""
    WITH task_counts AS (
        SELECT
            compute_id AS cluster_id,
            workspace_id,
            COUNT(*) AS active_tasks
        FROM {SYS_LAKEFLOW_JOB_TASKS}
        WHERE period_start_time >= CURRENT_TIMESTAMP - INTERVAL 4 HOURS
          AND period_end_time IS NULL
        GROUP BY compute_id, workspace_id
    )
    SELECT
        c.cluster_id,
        c.workspace_id,
        c.cluster_name,
        COALESCE(tc.active_tasks, 0) AS active_tasks
    FROM {SYS_COMPUTE_CLUSTERS} c
    LEFT JOIN task_counts tc
        ON c.cluster_id = tc.cluster_id
        AND c.workspace_id = tc.workspace_id
    WHERE c.delete_time IS NULL
      AND COALESCE(tc.active_tasks, 0) = 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.cluster_id, c.workspace_id
        ORDER BY c.change_time DESC
    ) = 1
""")

zero_task_clusters = df_zero_tasks.collect()
print(f"Clusters with 0 active tasks: {len(zero_task_clusters)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Ranked Savings Report

# COMMAND ----------

actions = []
savings_items = []

# Wrong compute findings
for job in wrong_compute_jobs:
    j_name = job["job_name"] or f"job_{job['job_id']}"
    # All-purpose compute is ~2x the DBU rate of job compute
    est_weekly_savings = job["recent_runs"] * 5  # rough estimate
    savings_items.append({
        "category": "wrong_compute",
        "target": j_name,
        "description": f"Migrate '{j_name}' from all-purpose to job compute ({job['recent_runs']} runs/week)",
        "weekly_savings": est_weekly_savings,
        "workspace_id": job["workspace_id"],
        "target_id": job["job_id"],
    })

# Idle clusters
for cl in idle_clusters:
    c_name = cl["cluster_name"] or cl["cluster_id"]
    idle_hrs = cl["idle_hours"] or CLUSTER_IDLE_HOURS
    est_weekly_savings = idle_hrs * 0.5 * 7  # rough DBU cost
    savings_items.append({
        "category": "idle_cluster",
        "target": c_name,
        "description": f"Terminate idle cluster '{c_name}' (idle {idle_hrs}h)",
        "weekly_savings": est_weekly_savings,
        "workspace_id": cl["workspace_id"],
        "target_id": cl["cluster_id"],
    })

# Zero-task clusters
for cl in zero_task_clusters:
    c_name = cl["cluster_name"] or cl["cluster_id"]
    savings_items.append({
        "category": "zero_tasks",
        "target": c_name,
        "description": f"Cluster '{c_name}' is running with 0 active tasks — consider auto-terminate",
        "weekly_savings": 20,  # rough estimate
        "workspace_id": cl["workspace_id"],
        "target_id": cl["cluster_id"],
    })

# Rank by savings
savings_items.sort(key=lambda x: x["weekly_savings"], reverse=True)
top_5 = savings_items[:5]
total_weekly = sum(s["weekly_savings"] for s in top_5)

print(f"\n{'='*60}")
print(f" TOP 5 CHANGES — Est. ${total_weekly:.0f}/week savings")
print(f"{'='*60}")
for i, item in enumerate(top_5, 1):
    print(f"  {i}. [{item['category']}] {item['description']} (~${item['weekly_savings']:.0f}/wk)")

    action = {
        "action_id": str(uuid.uuid4()),
        "action_timestamp": datetime.now().isoformat(),
        "skill_name": "wasteful_compute_detector",
        "action_type": "recommendation",
        "workspace_id": item["workspace_id"],
        "target_id": item["target_id"],
        "target_name": item["target"],
        "description": item["description"],
        "projected_savings": round(item["weekly_savings"] * 4.3, 2),  # monthly
        "status": "proposed",
        "details": json.dumps({"category": item["category"], "weekly_savings": item["weekly_savings"]}),
    }
    actions.append(action)

print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Actions & Notify

# COMMAND ----------

if actions:
    df_actions = spark.createDataFrame(actions)
    df_actions.write.format("delta").mode("append").saveAsTable(TABLE_AGENT_ACTIONS)

    notify(
        title="Wasteful Compute Report",
        message=f"Found {len(savings_items)} optimization(s). Top 5 save ~${total_weekly:.0f}/week.",
        severity="warning",
        details={"Total items": len(savings_items), "Est. weekly savings": f"${total_weekly:.0f}"},
    )
    print(f"\n✔ Logged {len(actions)} wasteful compute finding(s)")
else:
    print("✔ No wasteful compute detected.")
