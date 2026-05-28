# Databricks notebook source

# MAGIC %md
# MAGIC # Module 1: Observe — Cluster Utilization Features
# MAGIC 
# MAGIC Reads `system.compute.node_timeline` and builds hourly cluster utilization features.
# MAGIC 
# MAGIC **Source:** `system.compute.node_timeline` (minute-by-minute CPU, memory, disk I/O per node)
# MAGIC **Target:** `intelliops.feature_store.feat_cluster_utilization`

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Cluster Utilization Features
# MAGIC Aggregates node-level metrics to hourly cluster-level summaries.

# COMMAND ----------

df_cluster_util = spark.sql(f"""
    SELECT
        cluster_id,
        workspace_id,
        date_trunc('hour', start_time)          AS hour_window,
        AVG(cpu_user_percent)                    AS avg_cpu_pct,
        MAX(cpu_user_percent)                    AS peak_cpu_pct,
        AVG(mem_used_percent)                    AS avg_mem_pct,
        MAX(mem_used_percent)                    AS peak_mem_pct,
        COUNT(DISTINCT instance_id)              AS node_count,
        COUNT(DISTINCT node_type)                AS node_type_count
    FROM {SYS_COMPUTE_NODE_TIMELINE}
    WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY cluster_id, workspace_id, date_trunc('hour', start_time)
""")

# Add refresh timestamp
df_cluster_util = df_cluster_util.withColumn("updated_at", F.current_timestamp())

print(f"Rows generated: {df_cluster_util.count()}")
df_cluster_util.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Feature Store

# COMMAND ----------

(
    df_cluster_util.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_CLUSTER_UTILIZATION)
)

print(f"✔ {TABLE_CLUSTER_UTILIZATION} refreshed at {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Validation — Top Under-Utilized Clusters
# MAGIC Clusters with avg CPU < 30% and avg memory < 40% over the last 7 days.

# COMMAND ----------

df_underutilized = spark.sql(f"""
    SELECT
        cluster_id,
        workspace_id,
        COUNT(*)                        AS hours_observed,
        ROUND(AVG(avg_cpu_pct), 1)      AS avg_cpu,
        ROUND(AVG(avg_mem_pct), 1)      AS avg_mem,
        ROUND(AVG(node_count), 1)       AS avg_nodes,
        ROUND(AVG(node_type_count), 1)  AS avg_node_types
    FROM {TABLE_CLUSTER_UTILIZATION}
    WHERE hour_window >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY cluster_id, workspace_id
    HAVING AVG(avg_cpu_pct) < {CLUSTER_CPU_LOW_PCT}
       AND AVG(avg_mem_pct) < {CLUSTER_MEM_LOW_PCT}
    ORDER BY avg_cpu ASC
    LIMIT 20
""")

print(f"Under-utilized clusters (CPU < {CLUSTER_CPU_LOW_PCT}%, MEM < {CLUSTER_MEM_LOW_PCT}%):")
df_underutilized.display()
