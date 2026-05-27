# Databricks notebook source
# MAGIC %md
# MAGIC # IntelliOps V1 — Configuration
# MAGIC Central configuration for all IntelliOps modules.

# COMMAND ----------

# ── Unity Catalog Settings ──────────────────────────────────────────────────────
CATALOG = "intelliops"
SCHEMA = "feature_store"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# Feature store table names
TABLE_CLUSTER_UTILIZATION = f"{FULL_SCHEMA}.feat_cluster_utilization"
TABLE_JOB_COST_TREND = f"{FULL_SCHEMA}.feat_job_cost_trend"
TABLE_JOB_HEALTH = f"{FULL_SCHEMA}.feat_job_health"
TABLE_AGENT_ACTIONS = f"{FULL_SCHEMA}.agent_action_log"

# ── System Tables ───────────────────────────────────────────────────────────────
SYS_BILLING_USAGE = "system.billing.usage"
SYS_BILLING_PRICES = "system.billing.list_prices"
SYS_COMPUTE_CLUSTERS = "system.compute.clusters"
SYS_COMPUTE_NODE_TIMELINE = "system.compute.node_timeline"
SYS_COMPUTE_NODE_TYPES = "system.compute.node_types"
SYS_LAKEFLOW_JOBS = "system.lakeflow.jobs"
SYS_LAKEFLOW_JOB_RUNS = "system.lakeflow.job_run_timeline"
SYS_LAKEFLOW_JOB_TASKS = "system.lakeflow.job_task_run_timeline"

# COMMAND ----------

# ── Cost Thresholds ─────────────────────────────────────────────────────────────
COST_SPIKE_THRESHOLD_PCT = 25        # Alert if cost growth > 25% week-over-week
COST_FORECAST_HORIZON_DAYS = 7       # Predict cost for next 7 days
COST_BUDGET_ALERT_PCT = 90           # Alert if forecast > 90% of budget

# Monthly budgets per workspace (override as needed)
WORKSPACE_BUDGETS = {
    # "workspace_id": monthly_budget_usd
    "default": 50000,
}

# COMMAND ----------

# ── Cluster Thresholds ──────────────────────────────────────────────────────────
CLUSTER_CPU_LOW_PCT = 30             # Under-utilized if avg CPU < 30%
CLUSTER_MEM_LOW_PCT = 40             # Under-utilized if avg memory < 40%
CLUSTER_LOW_UTIL_HOURS = 5           # Must be low for 5+ consecutive hours
CLUSTER_IDLE_HOURS = 2               # Idle if no tasks for 2+ hours

# COMMAND ----------

# ── Job Health Thresholds ───────────────────────────────────────────────────────
JOB_FAILURE_RISK_THRESHOLD = 0.7     # Alert if failure probability > 0.7
JOB_DURATION_ANOMALY_SIGMA = 2       # Flag if duration > mean + 2σ
JOB_HEALTH_LOOKBACK_DAYS = 30        # Look back 30 days for health metrics

# COMMAND ----------

# ── Feature Refresh ─────────────────────────────────────────────────────────────
FEATURE_REFRESH_INTERVAL_MINUTES = 15  # Micro-batch refresh cadence

# COMMAND ----------

# ── Notifications (Optional) ────────────────────────────────────────────────────
# Leave as None to disable a channel. Notifications work without either configured.
SLACK_WEBHOOK_URL = None             # Optional: Slack incoming webhook URL
TEAMS_WEBHOOK_URL = None             # Optional: Teams incoming webhook URL
NOTIFICATION_ENABLED = True

# COMMAND ----------

# ── Databricks REST API ─────────────────────────────────────────────────────────
# These are resolved at runtime from the notebook context
DATABRICKS_HOST = None               # Auto-populated: dbutils.notebook.entry_point...
DATABRICKS_TOKEN = None              # Auto-populated from notebook context

# Human-in-the-loop: require approval before modifying production clusters
REQUIRE_APPROVAL_FOR_CLUSTER_EDIT = True

# COMMAND ----------

# ── MLflow Settings ─────────────────────────────────────────────────────────────
MLFLOW_EXPERIMENT_PREFIX = "/IntelliOps"
COST_MODEL_NAME = f"{CATALOG}.models.cost_spike_predictor"
FAILURE_MODEL_NAME = f"{CATALOG}.models.job_failure_predictor"
