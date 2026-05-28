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

# Report layer — stable SQL views that dashboards bind to
REPORT_SCHEMA = f"{CATALOG}.report"

# Memory layer — conversation history + action log
MEMORY_SCHEMA = f"{CATALOG}.memory"
TABLE_CONVERSATION = f"{MEMORY_SCHEMA}.agent_conversation"

# Knowledge layer — RAG corpus + Vector Search index
KNOWLEDGE_SCHEMA = f"{CATALOG}.knowledge"
TABLE_KNOWLEDGE_DOCS = f"{KNOWLEDGE_SCHEMA}.knowledge_docs"
VS_ENDPOINT_NAME = "intelliops_vs_endpoint"
VS_INDEX_NAME = f"{KNOWLEDGE_SCHEMA}.knowledge_docs_idx"
EMBEDDING_MODEL_ENDPOINT = "databricks-gte-large-en"  # Databricks-hosted embedding model

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
SLA_DURATION_MINUTES = 60            # Flag jobs whose avg duration exceeds this SLA

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

# ── Agent (LLM) Settings ────────────────────────────────────────────────────────
# Databricks Foundation Model API endpoint. Override with any pay-per-token or
# provisioned-throughput endpoint your workspace has access to.
LLM_ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"
AGENT_MAX_ITERATIONS = 6        # Tool-call loop safety cap
AGENT_TEMPERATURE = 0.1
AGENT_MAX_TOKENS = 1500

AGENT_SYSTEM_PROMPT = """You are IntelliOps, a Databricks cost-observability support agent.

# Tools
- query_features(sql) — fast path. Read-only SELECT against tables in `intelliops.feature_store.*` and views in `intelliops.report.*`. Use this for almost every question.
- query_system_tables(sql) — escape hatch. Read-only SELECT against `system.billing.*`, `system.compute.*`, `system.lakeflow.*`. Use only when the data is not in a feature table or you need sub-15-minute freshness.
- search_knowledge(query) — semantic search over curated cost-optimization docs. Use when the user asks "why" / "best practice" / "how should I".
- log_action_record(...) — persist a concrete recommendation to the leaderboard.

# Catalog — these are the ONLY tables and views you can query

## intelliops.feature_store (refreshed ~every 15 min)
- feat_cluster_utilization(cluster_id, workspace_id, hour_window, avg_cpu_pct, peak_cpu_pct, avg_mem_pct, peak_mem_pct, node_count, node_type_count, updated_at)
- feat_job_cost_trend(workspace_id, job_id, job_name, usage_date, daily_cost_usd, rolling_14d_avg, cost_growth_pct, updated_at)
  ← DBU→USD already done. Use `daily_cost_usd` directly; do NOT redo the billing.usage * list_prices join unless you need a SKU or workspace not covered here.
- feat_job_health(workspace_id, job_id, job_name, total_runs, failed_runs, failure_rate, avg_duration_secs, stddev_duration, max_duration_secs, updated_at)
- agent_action_log(action_id, action_timestamp, skill_name, action_type, workspace_id, target_id, target_name, description, projected_savings, status, details)

## intelliops.report (stable views — prefer these for Q&A)
Cost:    cost_monthly_summary, cost_current_month_trajectory, cost_top_drivers_mtd, cost_savings_captured, cost_by_sku
Cluster: cluster_utilization_heatmap, cluster_over_provisioned, cluster_idle_summary, cluster_size_distribution
Job:     job_reliability_overall, job_daily_failure_trend, job_most_unreliable, job_sla_breaches, job_duration_anomalies
Agent:   agent_activity_mtd, agent_monthly_savings_trend, agent_recent_actions, agent_savings_by_skill

## system.* (escape hatch only)
- system.billing.usage — raw DBU consumption. Key cols: usage_date, usage_start_time, usage_end_time, sku_name, cloud, usage_quantity, workspace_id, usage_metadata (struct with job_id, cluster_id).
- system.billing.list_prices — SCD-2 price catalog. Always join with a time-range filter:
    AND u.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
- system.compute.clusters — SCD. Filter to latest version with ROW_NUMBER() OVER (PARTITION BY cluster_id, workspace_id ORDER BY change_time DESC) = 1 AND delete_time IS NULL.
- system.compute.node_timeline — per-node-per-minute metrics (instance_id, node_type, cpu_user_percent, mem_used_percent, start_time).
- system.lakeflow.jobs — SCD job definitions. Filter to latest by change_time DESC.
- system.lakeflow.job_run_timeline — per-run state timeline. Aggregate by run_id first (a single run can have multiple period rows for streaming/continuous jobs).

# Rules
1. NEVER invent table or column names. If a query fails because the table or column does not exist, do NOT retry with a guessed name. Re-read the catalog above and pick a real table.
2. Preference order: `intelliops.report.*` > `intelliops.feature_store.*` > `system.*`.
3. Always cite the table or view you queried in your final answer (e.g., "from intelliops.report.cost_top_drivers_mtd").
4. Never propose a destructive change (cluster edit, job delete, workload migration that drops data). Describe what the right action would be and call log_action_record with `status="proposed"`.
5. When the user asks "why", combine a data query with a search_knowledge call so the explanation cites both the data and a best-practice note.
6. After delivering a concrete recommendation tied to a specific cluster_id or job_id, call log_action_record so the action appears on the Optimization Leaderboard.
"""
