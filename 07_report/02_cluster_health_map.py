# Databricks notebook source
# MAGIC %md
# MAGIC # Report — Cluster Health Map
# MAGIC
# MAGIC **Audience:** Platform Engineering
# MAGIC
# MAGIC Publishes stable SQL views into `intelliops.report.*` for the cluster-health
# MAGIC dashboard tab.

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {REPORT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cluster_utilization_heatmap` — hourly CPU/mem for last 7 days

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cluster_utilization_heatmap AS
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cluster_over_provisioned` — clusters below CPU/mem thresholds

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cluster_over_provisioned AS
SELECT
        cu.cluster_id,
        cu.workspace_id,
        c.cluster_name,
        c.worker_node_type,
        c.min_autoscale_workers              AS min_workers,
        c.max_autoscale_workers              AS max_workers,
        ROUND(AVG(cu.avg_cpu_pct), 1)        AS avg_cpu_7d,
        ROUND(AVG(cu.avg_mem_pct), 1)        AS avg_mem_7d,
        ROUND(AVG(cu.node_type_count), 1)    AS avg_nodes
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
             c.worker_node_type, c.min_autoscale_workers, c.max_autoscale_workers
    HAVING AVG(cu.avg_cpu_pct) < {CLUSTER_CPU_LOW_PCT}
       AND AVG(cu.avg_mem_pct) < {CLUSTER_MEM_LOW_PCT}
    ORDER BY avg_cpu_7d ASC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cluster_idle_summary` — idle compute by workspace

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cluster_idle_summary AS
SELECT
        workspace_id,
        COUNT(DISTINCT cluster_id)                  AS total_clusters,
        SUM(CASE WHEN avg_cpu_pct < 5
                 THEN 1 ELSE 0 END)                AS idle_observations,
        ROUND(AVG(node_type_count), 1)               AS avg_nodes_across_all,
        ROUND(SUM(CASE WHEN avg_cpu_pct < 5
            THEN node_type_count ELSE 0 END) * 1.0
            / NULLIF(SUM(node_type_count), 0) * 100, 1)  AS idle_node_pct
    FROM {TABLE_CLUSTER_UTILIZATION}
    WHERE hour_window >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY workspace_id
    ORDER BY idle_node_pct DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cluster_size_distribution` — cluster count by size bucket

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cluster_size_distribution AS
SELECT
    CASE
        WHEN max_autoscale_workers <= 2 THEN 'Small (1-2)'
        WHEN max_autoscale_workers <= 8 THEN 'Medium (3-8)'
        WHEN max_autoscale_workers <= 20 THEN 'Large (9-20)'
        ELSE 'XLarge (20+)'
    END AS cluster_size_bucket,
    COUNT(DISTINCT cluster_id) AS cluster_count
FROM system.compute.clusters
WHERE delete_time IS NULL
GROUP BY ALL
ORDER BY cluster_count DESC
""")

# COMMAND ----------

print(f"Cluster Health Map views published under {REPORT_SCHEMA}.")
