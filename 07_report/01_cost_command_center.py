# Databricks notebook source
# MAGIC %md
# MAGIC # Report — Cost Command Center
# MAGIC
# MAGIC **Audience:** Finance / Leadership
# MAGIC
# MAGIC Publishes stable SQL views into `intelliops.report.*`. Dashboards bind to these
# MAGIC view names so dashboard tiles stay valid even if underlying logic changes.

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {REPORT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cost_monthly_summary` — last 6 months of total spend

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cost_monthly_summary AS
SELECT
    DATE_TRUNC('month', u.usage_date)                  AS month,
    ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS total_spend_usd,
    COUNT(DISTINCT u.workspace_id)                      AS workspaces,
    COUNT(DISTINCT u.usage_metadata.job_id)             AS unique_jobs
FROM {SYS_BILLING_USAGE} u
JOIN {SYS_BILLING_PRICES} p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    -- list_prices is SCD; pick the row whose effective window covers usage_start_time.
    AND u.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_date >= CURRENT_DATE - INTERVAL 6 MONTHS
GROUP BY DATE_TRUNC('month', u.usage_date)
ORDER BY month DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cost_current_month_trajectory` — daily spend + 7-day rolling avg

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cost_current_month_trajectory AS
WITH daily AS (
    SELECT
        u.usage_date,
        ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS daily_spend
    FROM {SYS_BILLING_USAGE} u
    JOIN {SYS_BILLING_PRICES} p
        ON u.cloud = p.cloud
        AND u.sku_name = p.sku_name
        AND u.usage_start_time >= p.price_start_time
        AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    WHERE u.usage_date >= DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY u.usage_date
)
SELECT
    usage_date,
    daily_spend,
    SUM(daily_spend) OVER (ORDER BY usage_date) AS cumulative_spend,
    AVG(daily_spend) OVER (ORDER BY usage_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        AS rolling_7d_avg
FROM daily
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cost_top_drivers_mtd` — top 10 jobs by month-to-date spend

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cost_top_drivers_mtd AS
SELECT
    job_name,
    workspace_id,
    ROUND(SUM(daily_cost_usd), 2)        AS mtd_cost,
    ROUND(AVG(daily_cost_usd), 2)         AS avg_daily_cost,
    ROUND(MAX(cost_growth_pct) * 100, 1)  AS max_growth_pct
FROM {TABLE_JOB_COST_TREND}
WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY job_name, workspace_id
ORDER BY mtd_cost DESC
LIMIT 10
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cost_savings_captured` — month-to-date savings attributed to IntelliOps

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cost_savings_captured AS
SELECT
    skill_name,
    action_type,
    COUNT(*)                                            AS actions_count,
    SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) AS applied_count,
    ROUND(SUM(projected_savings), 2)                    AS total_projected_savings
FROM {TABLE_AGENT_ACTIONS}
WHERE action_timestamp >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
GROUP BY skill_name, action_type
ORDER BY total_projected_savings DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `cost_by_sku` — spend by SKU / product (current month)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.cost_by_sku AS
SELECT
    u.sku_name,
    u.billing_origin_product,
    ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS mtd_spend,
    ROUND(SUM(u.usage_quantity), 0)                     AS total_dbus
FROM {SYS_BILLING_USAGE} u
JOIN {SYS_BILLING_PRICES} p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_date >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY u.sku_name, u.billing_origin_product
ORDER BY mtd_spend DESC
LIMIT 15
""")

# COMMAND ----------

print(f"Cost Command Center views published under {REPORT_SCHEMA}.")
