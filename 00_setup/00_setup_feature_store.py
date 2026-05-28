# Databricks notebook source
# MAGIC %md
# MAGIC # IntelliOps V1 — Setup Feature Store
# MAGIC Creates the Unity Catalog schema and Delta tables for the IntelliOps feature store.
# MAGIC
# MAGIC **Run this notebook once** to initialize the feature store.

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# ── Create Catalog & Schema ─────────────────────────────────────────────────────
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.models")

print(f"✔ Catalog '{CATALOG}' and schema '{SCHEMA}' ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Store Tables

# COMMAND ----------

# ── feat_cluster_utilization ────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_CLUSTER_UTILIZATION} (
    cluster_id          STRING      COMMENT 'Databricks cluster ID',
    workspace_id        STRING      COMMENT 'Workspace ID',
    hour_window         TIMESTAMP   COMMENT 'Truncated to hour',
    avg_cpu_pct         DOUBLE      COMMENT 'Average CPU user percent',
    peak_cpu_pct        DOUBLE      COMMENT 'Peak CPU user percent',
    avg_mem_pct         DOUBLE      COMMENT 'Average memory used percent',
    peak_mem_pct        DOUBLE      COMMENT 'Peak memory used percent',
    node_count          LONG        COMMENT 'Number of active nodes',
    updated_at          TIMESTAMP   COMMENT 'Feature refresh timestamp'
)
USING DELTA
COMMENT 'Hourly cluster utilization features from system.compute.node_timeline'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
print(f"✔ {TABLE_CLUSTER_UTILIZATION} created.")

# COMMAND ----------

# ── feat_job_cost_trend ─────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_JOB_COST_TREND} (
    workspace_id        STRING      COMMENT 'Workspace ID',
    job_id              STRING      COMMENT 'Databricks job ID',
    job_name            STRING      COMMENT 'Human-readable job name',
    usage_date          DATE        COMMENT 'Billing date',
    daily_cost_usd      DOUBLE      COMMENT 'Daily cost in USD',
    rolling_14d_avg     DOUBLE      COMMENT 'Rolling 14-day average cost',
    cost_growth_pct     DOUBLE      COMMENT 'Week-over-week cost growth percentage',
    updated_at          TIMESTAMP   COMMENT 'Feature refresh timestamp'
)
USING DELTA
COMMENT 'Daily job cost trend features from billing.usage + list_prices + lakeflow.jobs'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
print(f"✔ {TABLE_JOB_COST_TREND} created.")

# COMMAND ----------

# ── feat_job_health ─────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_JOB_HEALTH} (
    workspace_id        STRING      COMMENT 'Workspace ID',
    job_id              STRING      COMMENT 'Databricks job ID',
    job_name            STRING      COMMENT 'Human-readable job name',
    total_runs          LONG        COMMENT 'Total runs in lookback window',
    failed_runs         LONG        COMMENT 'Number of failed runs',
    failure_rate        DOUBLE      COMMENT 'Failure rate (failed / total)',
    avg_duration_secs   DOUBLE      COMMENT 'Average run duration in seconds',
    stddev_duration     DOUBLE      COMMENT 'Standard deviation of duration',
    max_duration_secs   DOUBLE      COMMENT 'Maximum run duration in seconds',
    updated_at          TIMESTAMP   COMMENT 'Feature refresh timestamp'
)
USING DELTA
COMMENT 'Job health features from system.lakeflow.job_run_timeline (30-day window)'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
print(f"✔ {TABLE_JOB_HEALTH} created.")

# COMMAND ----------

# ── agent_action_log ────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_AGENT_ACTIONS} (
    action_id           STRING      COMMENT 'Unique action identifier (UUID)',
    action_timestamp    TIMESTAMP   COMMENT 'When the action was taken',
    skill_name          STRING      COMMENT 'Agent skill that triggered the action',
    action_type         STRING      COMMENT 'Type: alert, recommendation, auto_remediation',
    workspace_id        STRING      COMMENT 'Target workspace ID',
    target_id           STRING      COMMENT 'Target resource ID (cluster_id, job_id)',
    target_name         STRING      COMMENT 'Human-readable target name',
    description         STRING      COMMENT 'Plain-English description of the action',
    projected_savings   DOUBLE      COMMENT 'Projected monthly savings in USD',
    status              STRING      COMMENT 'Status: proposed, approved, applied, rejected',
    details             STRING      COMMENT 'JSON blob with full action details'
)
USING DELTA
COMMENT 'Log of all IntelliOps agent actions for the optimization leaderboard'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
print(f"✔ {TABLE_AGENT_ACTIONS} created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {FULL_SCHEMA}").collect()
print(f"\n{'='*60}")
print(f" IntelliOps Feature Store — {len(tables)} tables ready")
print(f"{'='*60}")
for t in tables:
    print(f"  • {FULL_SCHEMA}.{t.tableName}")
print(f"{'='*60}")

# COMMAND ----------


