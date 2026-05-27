# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3: Act — Skill 2: Cluster Right-Sizing + Auto-Apply
# MAGIC
# MAGIC **Trigger:** `avg_cpu < 30%` for 5+ consecutive hours AND `avg_mem < 40%`
# MAGIC
# MAGIC **Agent Actions:**
# MAGIC 1. Calculates optimal `min_workers` / `max_workers` from utilization percentiles
# MAGIC 2. Checks node type vs workload profile
# MAGIC 3. Calls `POST /api/2.0/clusters/edit` with new config (after human approval for prod)
# MAGIC 4. Logs recommendation with projected savings

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# MAGIC %run ../utils/databricks_api

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Identify Over-Provisioned Clusters

# COMMAND ----------

df_overprovisioned = spark.sql(f"""
    WITH hourly_low AS (
        SELECT
            cluster_id,
            workspace_id,
            hour_window,
            avg_cpu_pct,
            avg_mem_pct,
            node_count,
            SUM(CASE
                WHEN avg_cpu_pct < {CLUSTER_CPU_LOW_PCT}
                 AND avg_mem_pct < {CLUSTER_MEM_LOW_PCT}
                THEN 1 ELSE 0
            END) OVER (
                PARTITION BY cluster_id, workspace_id
                ORDER BY hour_window
                ROWS BETWEEN {CLUSTER_LOW_UTIL_HOURS - 1} PRECEDING AND CURRENT ROW
            ) AS consecutive_low_hours
        FROM {TABLE_CLUSTER_UTILIZATION}
        WHERE hour_window >= CURRENT_TIMESTAMP - INTERVAL 48 HOURS
    )
    SELECT
        cluster_id,
        workspace_id,
        ROUND(AVG(avg_cpu_pct), 1)  AS avg_cpu,
        ROUND(AVG(avg_mem_pct), 1)  AS avg_mem,
        ROUND(AVG(node_count), 1)   AS avg_nodes,
        MAX(consecutive_low_hours)  AS max_consec_low_hours
    FROM hourly_low
    GROUP BY cluster_id, workspace_id
    HAVING MAX(consecutive_low_hours) >= {CLUSTER_LOW_UTIL_HOURS}
    ORDER BY avg_cpu ASC
""")

candidates = df_overprovisioned.collect()
print(f"Found {len(candidates)} over-provisioned cluster(s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate Optimal Config & Generate Recommendations

# COMMAND ----------

actions = []

for cluster in candidates:
    c_id = cluster["cluster_id"]
    ws_id = cluster["workspace_id"]
    avg_cpu = cluster["avg_cpu"]
    avg_mem = cluster["avg_mem"]
    avg_nodes = cluster["avg_nodes"]

    # Get current cluster config
    current_config = spark.sql(f"""
        SELECT
            cluster_id, cluster_name, node_type_id, driver_node_type_id,
            autoscale.min_workers AS current_min,
            autoscale.max_workers AS current_max,
            cluster_source
        FROM {SYS_COMPUTE_CLUSTERS}
        WHERE cluster_id = '{c_id}' AND workspace_id = '{ws_id}'
        ORDER BY change_time DESC
        LIMIT 1
    """).first()

    if not current_config:
        continue

    c_name = current_config["cluster_name"] or c_id
    current_min = current_config["current_min"] or 1
    current_max = current_config["current_max"] or int(avg_nodes)

    # Target ~60% average utilization after right-sizing
    utilization_ratio = max(avg_cpu, avg_mem) / 60.0
    optimal_max = max(1, int(current_max * utilization_ratio))
    optimal_min = max(1, optimal_max // 2)

    reduction_pct = (current_max - optimal_max) / current_max if current_max > 0 else 0
    estimated_monthly_savings = reduction_pct * current_max * 720 * 0.15

    description = (
        f"Cluster '{c_name}' is over-provisioned (CPU: {avg_cpu}%, MEM: {avg_mem}%). "
        f"Recommend: {current_min}-{current_max} → {optimal_min}-{optimal_max} workers "
        f"(~{reduction_pct*100:.0f}% reduction, est. ${estimated_monthly_savings:.0f}/mo savings)."
    )

    action = {
        "action_id": str(uuid.uuid4()),
        "action_timestamp": datetime.now().isoformat(),
        "skill_name": "cluster_right_sizing",
        "action_type": "recommendation",
        "workspace_id": ws_id,
        "target_id": c_id,
        "target_name": c_name,
        "description": description,
        "projected_savings": round(estimated_monthly_savings, 2),
        "status": "proposed",
        "details": json.dumps({
            "current_min": current_min, "current_max": current_max,
            "recommended_min": optimal_min, "recommended_max": optimal_max,
            "avg_cpu_pct": avg_cpu, "avg_mem_pct": avg_mem,
            "reduction_pct": round(reduction_pct * 100, 1),
        }),
    }

    # Auto-apply gate
    if not REQUIRE_APPROVAL_FOR_CLUSTER_EDIT:
        result = edit_cluster(c_id, min_workers=optimal_min, max_workers=optimal_max)
        if result:
            action["status"] = "applied"
            action["action_type"] = "auto_remediation"
            print(f"  ⚡ Applied: {description}")
        else:
            print(f"  ❌ Failed to apply for {c_name}")
    else:
        print(f"  ⏸ {description}")
        print(f"     → Awaiting human approval (set REQUIRE_APPROVAL_FOR_CLUSTER_EDIT=False to auto-apply)")

    actions.append(action)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Log Actions

# COMMAND ----------

if actions:
    df_actions = spark.createDataFrame(actions)
    df_actions.write.format("delta").mode("append").saveAsTable(TABLE_AGENT_ACTIONS)
    print(f"\n✔ Logged {len(actions)} right-sizing recommendation(s)")
else:
    print("✔ No over-provisioned clusters detected.")
