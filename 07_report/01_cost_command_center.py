# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4: Report — Cost Command Center
# MAGIC
# MAGIC **Audience:** Finance / Leadership
# MAGIC
# MAGIC **Key Metrics:** Monthly spend, forecast, top 10 cost drivers, savings captured

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 1: Monthly Spend Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', u.usage_date)                  AS month,
# MAGIC     ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS total_spend_usd,
# MAGIC     COUNT(DISTINCT u.workspace_id)                      AS workspaces,
# MAGIC     COUNT(DISTINCT u.usage_metadata.job_id)             AS unique_jobs
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices p
# MAGIC     ON u.cloud = p.cloud AND u.sku_name = p.sku_name
# MAGIC WHERE u.usage_date >= CURRENT_DATE - INTERVAL 6 MONTHS
# MAGIC GROUP BY DATE_TRUNC('month', u.usage_date)
# MAGIC ORDER BY month DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2: Current Month — Spend Trajectory

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH daily AS (
# MAGIC     SELECT
# MAGIC         u.usage_date,
# MAGIC         ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS daily_spend
# MAGIC     FROM system.billing.usage u
# MAGIC     JOIN system.billing.list_prices p
# MAGIC         ON u.cloud = p.cloud AND u.sku_name = p.sku_name
# MAGIC     WHERE u.usage_date >= DATE_TRUNC('month', CURRENT_DATE)
# MAGIC     GROUP BY u.usage_date
# MAGIC )
# MAGIC SELECT
# MAGIC     usage_date,
# MAGIC     daily_spend,
# MAGIC     SUM(daily_spend) OVER (ORDER BY usage_date) AS cumulative_spend,
# MAGIC     AVG(daily_spend) OVER (ORDER BY usage_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
# MAGIC         AS rolling_7d_avg
# MAGIC FROM daily
# MAGIC ORDER BY usage_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3: Top 10 Cost Drivers (Current Month)

# COMMAND ----------

df_top_drivers = spark.sql(f"""
    SELECT
        job_name,
        workspace_id,
        ROUND(SUM(daily_cost_usd), 2)      AS mtd_cost,
        ROUND(AVG(daily_cost_usd), 2)       AS avg_daily_cost,
        ROUND(MAX(cost_growth_pct) * 100, 1) AS max_growth_pct
    FROM {TABLE_JOB_COST_TREND}
    WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY job_name, workspace_id
    ORDER BY mtd_cost DESC
    LIMIT 10
""")

print("Top 10 Cost Drivers — Current Month:")
df_top_drivers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 4: Savings Captured by IntelliOps

# COMMAND ----------

df_savings = spark.sql(f"""
    SELECT
        skill_name,
        action_type,
        COUNT(*)                                AS actions_count,
        SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) AS applied_count,
        ROUND(SUM(projected_savings), 2)         AS total_projected_savings
    FROM {TABLE_AGENT_ACTIONS}
    WHERE action_timestamp >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
    GROUP BY skill_name, action_type
    ORDER BY total_projected_savings DESC
""")

print("IntelliOps Savings — Current Month:")
df_savings.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 5: Spend by SKU / Product

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     u.sku_name,
# MAGIC     u.billing_origin_product,
# MAGIC     ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS mtd_spend,
# MAGIC     ROUND(SUM(u.usage_quantity), 0)                     AS total_dbus
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices p
# MAGIC     ON u.cloud = p.cloud AND u.sku_name = p.sku_name
# MAGIC WHERE u.usage_date >= DATE_TRUNC('month', CURRENT_DATE)
# MAGIC GROUP BY u.sku_name, u.billing_origin_product
# MAGIC ORDER BY mtd_spend DESC
# MAGIC LIMIT 15
