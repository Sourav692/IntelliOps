# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4: Report — Cluster Health Map
# MAGIC
# MAGIC **Audience:** Platform Engineering
# MAGIC
# MAGIC **Key Metrics:** Utilization heatmap, over-provisioned clusters, idle compute

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 1: Cluster Utilization Heatmap (Last 7 Days)

# COMMAND ----------

df_heatmap = spark.sql(f"""
    SELECT
        cluster_id,
        DATE(hour_window)                   AS day,
        HOUR(hour_window)                    AS hour_of_day,
        ROUND(AVG(avg_cpu_pct), 1)           AS avg_cpu,
        ROUND(AVG(avg_mem_pct), 1)           AS avg_mem
    FROM {TABLE_CLUSTER_UTILIZATION}
    WHERE hour_window >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY cluster_id, DATE(hour_window), HOUR(hour_window)
    ORDER BY cluster_id, day, hour_of_day
""")

print("Cluster utilization heatmap data:")
df_heatmap.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2: Over-Provisioned Clusters

# COMMAND ----------

df_over = spark.sql(f"""
    SELECT
        cu.cluster_id,
        cu.workspace_id,
        c.cluster_name,
        c.node_type_id,
        c.autoscale.min_workers             AS min_workers,
        c.autoscale.max_workers             AS max_workers,
        ROUND(AVG(cu.avg_cpu_pct), 1)        AS avg_cpu_7d,
        ROUND(AVG(cu.avg_mem_pct), 1)        AS avg_mem_7d,
        ROUND(AVG(cu.node_count), 1)         AS avg_nodes
    FROM {TABLE_CLUSTER_UTILIZATION} cu
    LEFT JOIN (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY cluster_id, workspace_id
            ORDER BY change_time DESC
        ) AS rn
        FROM {SYS_COMPUTE_CLUSTERS}
    ) c ON cu.cluster_id = c.cluster_id
        AND cu.workspace_id = c.workspace_id AND c.rn = 1
    WHERE cu.hour_window >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY cu.cluster_id, cu.workspace_id, c.cluster_name,
             c.node_type_id, c.autoscale.min_workers, c.autoscale.max_workers
    HAVING AVG(cu.avg_cpu_pct) < {CLUSTER_CPU_LOW_PCT}
       AND AVG(cu.avg_mem_pct) < {CLUSTER_MEM_LOW_PCT}
    ORDER BY avg_cpu_7d ASC
""")

print(f"Over-provisioned clusters (CPU < {CLUSTER_CPU_LOW_PCT}%, MEM < {CLUSTER_MEM_LOW_PCT}%):")
df_over.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3: Idle Compute Summary

# COMMAND ----------

df_idle_summary = spark.sql(f"""
    SELECT
        workspace_id,
        COUNT(DISTINCT cluster_id)                  AS total_clusters,
        SUM(CASE WHEN avg_cpu_pct < 5
                 THEN 1 ELSE 0 END)                AS idle_observations,
        ROUND(AVG(node_count), 1)                    AS avg_nodes_across_all,
        ROUND(SUM(CASE WHEN avg_cpu_pct < 5
            THEN node_count ELSE 0 END) * 1.0
            / NULLIF(SUM(node_count), 0) * 100, 1)  AS idle_node_pct
    FROM {TABLE_CLUSTER_UTILIZATION}
    WHERE hour_window >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY workspace_id
    ORDER BY idle_node_pct DESC
""")

print("Idle compute by workspace:")
df_idle_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 4: Cluster Size Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN autoscale.max_workers <= 2 THEN 'Small (1-2)'
# MAGIC         WHEN autoscale.max_workers <= 8 THEN 'Medium (3-8)'
# MAGIC         WHEN autoscale.max_workers <= 20 THEN 'Large (9-20)'
# MAGIC         ELSE 'XLarge (20+)'
# MAGIC     END AS cluster_size_bucket,
# MAGIC     COUNT(DISTINCT cluster_id) AS cluster_count
# MAGIC FROM system.compute.clusters
# MAGIC WHERE delete_time IS NULL
# MAGIC GROUP BY ALL
# MAGIC ORDER BY cluster_count DESC
