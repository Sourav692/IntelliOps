# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1: Observe — Job Cost Trend Features
# MAGIC
# MAGIC Joins billing usage, list prices, and job metadata to build daily cost trends per job.
# MAGIC
# MAGIC **Sources:**
# MAGIC - `system.billing.usage` — DBU consumption per job/cluster/SKU
# MAGIC - `system.billing.list_prices` — DBU → USD conversion
# MAGIC - `system.lakeflow.jobs` — Job metadata (name, config)
# MAGIC
# MAGIC **Target:** `intelliops.feature_store.feat_job_cost_trend`

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Compute Daily Cost per Job

# COMMAND ----------

df_cost_trend = spark.sql(f"""
    WITH latest_jobs AS (
        -- system.lakeflow.jobs is SCD-2; keep only the most recent definition
        -- per (workspace_id, job_id) to prevent cartesian inflation.
        SELECT workspace_id, job_id, name AS job_name
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY workspace_id, job_id ORDER BY change_time DESC
            ) AS _rn
            FROM {SYS_LAKEFLOW_JOBS}
        ) WHERE _rn = 1
    ),
    cost_enriched AS (
        -- list_prices is also SCD-2; restrict the price join to the row whose
        -- effective window covers the usage event so USD totals stay correct.
        SELECT
            u.workspace_id,
            u.usage_metadata.job_id                     AS job_id,
            j.job_name                                  AS job_name,
            u.usage_date,
            SUM(u.usage_quantity * p.pricing.default)   AS daily_cost_usd
        FROM {SYS_BILLING_USAGE} u
        JOIN {SYS_BILLING_PRICES} p
            ON u.cloud = p.cloud
            AND u.sku_name = p.sku_name
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
        LEFT JOIN latest_jobs j
            ON u.workspace_id = j.workspace_id
            AND u.usage_metadata.job_id = j.job_id
        WHERE u.usage_metadata.job_id IS NOT NULL
          AND u.usage_date >= CURRENT_DATE - INTERVAL 60 DAYS
        GROUP BY ALL
    )
    SELECT
        workspace_id,
        job_id,
        job_name,
        usage_date,
        daily_cost_usd,

        -- Rolling 14-day average cost
        AVG(daily_cost_usd) OVER (
            PARTITION BY workspace_id, job_id
            ORDER BY usage_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS rolling_14d_avg,

        -- Week-over-week cost growth percentage
        daily_cost_usd / NULLIF(
            AVG(daily_cost_usd) OVER (
                PARTITION BY workspace_id, job_id
                ORDER BY usage_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ), 0
        ) - 1 AS cost_growth_pct

    FROM cost_enriched
    ORDER BY workspace_id, job_id, usage_date
""")

df_cost_trend = df_cost_trend.withColumn("updated_at", F.current_timestamp())
print(f"Cost trend records: {df_cost_trend.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Feature Store

# COMMAND ----------

(
    df_cost_trend.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_JOB_COST_TREND)
)

print(f"✔ {TABLE_JOB_COST_TREND} refreshed at {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Validation — Top Cost Spikes (Last 7 Days)

# COMMAND ----------

df_spikes = spark.sql(f"""
    SELECT
        workspace_id,
        job_id,
        job_name,
        usage_date,
        ROUND(daily_cost_usd, 2)    AS daily_cost,
        ROUND(rolling_14d_avg, 2)   AS avg_14d,
        ROUND(cost_growth_pct * 100, 1) AS growth_pct
    FROM {TABLE_JOB_COST_TREND}
    WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
      AND cost_growth_pct > {COST_SPIKE_THRESHOLD_PCT / 100}
    ORDER BY cost_growth_pct DESC
    LIMIT 20
""")

print(f"Jobs with cost growth > {COST_SPIKE_THRESHOLD_PCT}% in last 7 days:")
df_spikes.display()
