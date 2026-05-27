# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3: Act — Skill 1: Cost Spike Alert + Root Cause
# MAGIC
# MAGIC **Trigger:** `cost_growth_pct > 25%` (week-over-week spike)
# MAGIC
# MAGIC **Agent Actions:**
# MAGIC 1. Identifies which job/cluster caused the spike
# MAGIC 2. Checks if a new compute config was applied (cluster change_time)
# MAGIC 3. Checks if data volume increased (usage_quantity delta)
# MAGIC 4. Generates plain-English explanation

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Detect Cost Spikes

# COMMAND ----------

df_spikes = spark.sql(f"""
    SELECT
        workspace_id,
        job_id,
        job_name,
        usage_date,
        daily_cost_usd,
        rolling_14d_avg,
        cost_growth_pct
    FROM {TABLE_JOB_COST_TREND}
    WHERE usage_date >= CURRENT_DATE - INTERVAL 3 DAYS
      AND cost_growth_pct > {COST_SPIKE_THRESHOLD_PCT / 100}
    ORDER BY cost_growth_pct DESC
""")

spikes = df_spikes.collect()
print(f"Detected {len(spikes)} cost spike(s) in last 3 days.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Root Cause Analysis per Spike

# COMMAND ----------

actions = []

for spike in spikes:
    ws_id = spike["workspace_id"]
    j_id = spike["job_id"]
    j_name = spike["job_name"] or f"job_{j_id}"
    growth = spike["cost_growth_pct"]
    daily_cost = spike["daily_cost_usd"]

    root_causes = []

    # ── Check 1: Was the cluster config changed recently? ──────────────────
    cluster_changes = spark.sql(f"""
        SELECT
            c.cluster_id,
            c.cluster_name,
            c.change_time,
            c.driver_node_type_id,
            c.node_type_id,
            c.autoscale.min_workers,
            c.autoscale.max_workers
        FROM {SYS_COMPUTE_CLUSTERS} c
        JOIN {SYS_LAKEFLOW_JOB_TASKS} t
            ON c.workspace_id = t.workspace_id
            AND c.cluster_id = t.compute_id
        WHERE t.workspace_id = '{ws_id}'
          AND t.job_id = '{j_id}'
          AND c.change_time >= CURRENT_DATE - INTERVAL 7 DAYS
        ORDER BY c.change_time DESC
        LIMIT 5
    """).collect()

    if cluster_changes:
        latest = cluster_changes[0]
        root_causes.append(
            f"Cluster '{latest['cluster_name']}' was reconfigured on "
            f"{latest['change_time']} — node type: {latest['node_type_id']}, "
            f"workers: {latest['min_workers']}-{latest['max_workers']}"
        )

    # ── Check 2: Did data volume (DBU usage) increase? ─────────────────────
    volume_change = spark.sql(f"""
        WITH weekly AS (
            SELECT
                CASE WHEN usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
                     THEN 'this_week' ELSE 'last_week' END AS period,
                SUM(usage_quantity) AS total_dbus
            FROM {SYS_BILLING_USAGE}
            WHERE workspace_id = '{ws_id}'
              AND usage_metadata.job_id = '{j_id}'
              AND usage_date >= CURRENT_DATE - INTERVAL 14 DAYS
            GROUP BY ALL
        )
        SELECT
            MAX(CASE WHEN period = 'this_week' THEN total_dbus END) AS this_week_dbus,
            MAX(CASE WHEN period = 'last_week' THEN total_dbus END) AS last_week_dbus
        FROM weekly
    """).first()

    if volume_change and volume_change["this_week_dbus"] and volume_change["last_week_dbus"]:
        tw = volume_change["this_week_dbus"]
        lw = volume_change["last_week_dbus"]
        if lw > 0:
            dbu_growth = (tw - lw) / lw * 100
            if dbu_growth > 10:
                root_causes.append(
                    f"DBU consumption increased {dbu_growth:.0f}% "
                    f"({lw:.0f} → {tw:.0f} DBUs week-over-week)"
                )

    # ── Check 3: Is the job running on expensive all-purpose compute? ──────
    wrong_compute = spark.sql(f"""
        SELECT DISTINCT c.cluster_source
        FROM {SYS_LAKEFLOW_JOB_TASKS} t
        JOIN {SYS_COMPUTE_CLUSTERS} c
            ON t.workspace_id = c.workspace_id
            AND t.compute_id = c.cluster_id
        WHERE t.workspace_id = '{ws_id}'
          AND t.job_id = '{j_id}'
          AND c.cluster_source = 'UI'
          AND t.period_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    """).collect()

    if wrong_compute:
        root_causes.append(
            "Job is running on ALL-PURPOSE compute (higher DBU rate) — "
            "migrating to job compute would reduce cost"
        )

    # ── Build explanation ──────────────────────────────────────────────────
    if not root_causes:
        root_causes.append("No specific root cause identified — review job configuration manually")

    explanation = (
        f"Job '{j_name}' cost {growth*100:.0f}% more this week "
        f"(${daily_cost:.0f}/day). Root cause(s): "
        + "; ".join(root_causes)
    )

    action = {
        "action_id": str(uuid.uuid4()),
        "action_timestamp": datetime.now().isoformat(),
        "skill_name": "cost_spike_alert",
        "action_type": "alert",
        "workspace_id": ws_id,
        "target_id": j_id,
        "target_name": j_name,
        "description": explanation,
        "projected_savings": round(daily_cost * growth * 30, 2),  # Rough monthly impact
        "status": "proposed",
        "details": json.dumps({
            "cost_growth_pct": round(growth * 100, 1),
            "daily_cost_usd": round(daily_cost, 2),
            "root_causes": root_causes,
        }),
    }
    actions.append(action)
    print(f"\n🔴 {explanation}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Log Actions

# COMMAND ----------

if actions:
    df_actions = spark.createDataFrame(actions)
    df_actions.write.format("delta").mode("append").saveAsTable(TABLE_AGENT_ACTIONS)
    print(f"\n✔ Logged {len(actions)} cost spike alert(s) to {TABLE_AGENT_ACTIONS}")
else:
    print("✔ No cost spikes detected — no actions needed.")
