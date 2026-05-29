# IntelliOps — Data Catalog

Every Unity Catalog object this framework creates, what it does, and the SQL surface (columns) it exposes.

Layout:

```
intelliops/
├── feature_store/    ← pre-aggregated Delta tables (refreshed every ~15 min)
├── report/           ← stable SQL views dashboards & the agent bind to
├── memory/           ← agent conversation history
└── knowledge/        ← curated docs corpus + Vector Search index
```

---

## 1. `intelliops.feature_store` — Feature Tables

Pre-aggregated Delta tables refreshed by `01_observe/*.py`. Source of truth for dashboards and the agent's fast path. Created by `00_setup/00_setup_feature_store.py`.

### `feat_cluster_utilization`
**Functionality:** Hourly per-cluster utilization. Aggregated from `system.compute.node_timeline` (one row per node per minute). Powers the cluster-health views and the agent's right-sizing reasoning.

| Column | Type | Description |
|---|---|---|
| `cluster_id` | STRING | Databricks cluster ID |
| `workspace_id` | STRING | Workspace the cluster lives in |
| `hour_window` | TIMESTAMP | Truncated to the hour |
| `avg_cpu_pct` | DOUBLE | Average user-CPU % across nodes during the hour |
| `peak_cpu_pct` | DOUBLE | Peak user-CPU % observed during the hour |
| `avg_mem_pct` | DOUBLE | Average memory-used % across nodes |
| `peak_mem_pct` | DOUBLE | Peak memory-used % observed |
| `node_count` | LONG | Distinct active nodes during the hour (`COUNT(DISTINCT instance_id)`) |
| `node_type_count` | LONG | Distinct VM types in the cluster during the hour |
| `updated_at` | TIMESTAMP | Feature-refresh timestamp |

### `feat_job_cost_trend`
**Functionality:** Daily USD spend per job with 14-day rolling average and week-over-week growth %. Already does the DBU→USD conversion and the SCD-correct join with `system.billing.list_prices` so consumers don't have to.

| Column | Type | Description |
|---|---|---|
| `workspace_id` | STRING | Workspace ID |
| `job_id` | STRING | Databricks job ID |
| `job_name` | STRING | Latest human-readable job name |
| `usage_date` | DATE | Billing date |
| `daily_cost_usd` | DOUBLE | Daily cost in USD (DBU × list price, with SCD-correct time-windowed price join) |
| `rolling_14d_avg` | DOUBLE | 14-day rolling average of `daily_cost_usd` |
| `cost_growth_pct` | DOUBLE | Today / 7-day-trailing-average − 1. Negative means cheaper than recent baseline. |
| `updated_at` | TIMESTAMP | Feature-refresh timestamp |

### `feat_job_health`
**Functionality:** 30-day job reliability and duration profile. Aggregated by `run_id` (so streaming/multi-period runs don't inflate counts) and filtered to runs that reached a terminal state.

| Column | Type | Description |
|---|---|---|
| `workspace_id` | STRING | Workspace ID |
| `job_id` | STRING | Databricks job ID |
| `job_name` | STRING | Latest job name (from the most recent SCD row in `system.lakeflow.jobs`) |
| `total_runs` | LONG | Completed runs in the lookback window |
| `failed_runs` | LONG | Runs with `result_state = 'FAILED'` |
| `failure_rate` | DOUBLE | `failed_runs / total_runs` |
| `avg_duration_secs` | DOUBLE | Mean run duration (seconds) |
| `stddev_duration` | DOUBLE | Standard deviation of run duration |
| `max_duration_secs` | DOUBLE | Longest run in the window |
| `updated_at` | TIMESTAMP | Feature-refresh timestamp |

### `agent_action_log`
**Functionality:** Append-only audit log of every recommendation or action emitted by the agent (or by the rule-based skills in `04_tools/`). Drives the Optimization Leaderboard dashboard tab.

| Column | Type | Description |
|---|---|---|
| `action_id` | STRING | UUID, unique per row |
| `action_timestamp` | TIMESTAMP | When the action was recorded |
| `skill_name` | STRING | Which skill / agent tool produced the action |
| `action_type` | STRING | `alert` \| `recommendation` \| `auto_remediation` |
| `workspace_id` | STRING | Target workspace |
| `target_id` | STRING | Target resource ID (cluster_id or job_id) |
| `target_name` | STRING | Human-readable target name |
| `description` | STRING | Plain-English description of the action |
| `projected_savings` | DOUBLE | Estimated monthly USD savings |
| `status` | STRING | `proposed` \| `approved` \| `applied` \| `rejected` |
| `details` | STRING | JSON blob with full action context |

---

## 2. `intelliops.memory` — Agent Memory

Created by `05_memory/00_setup_memory.py`.

### `agent_conversation`
**Functionality:** Turn-level history of every agent invocation. Currently populated by the older hand-rolled agent path; the LangGraph `create_react_agent` does not log per-turn by default (callers can wrap `graph.invoke` and write here if needed).

| Column | Type | Description |
|---|---|---|
| `session_id` | STRING | Stable per-user / per-channel session ID |
| `turn` | INT | 0-indexed ordinal within the session |
| `role` | STRING | `user` \| `assistant` \| `tool` \| `system` |
| `content` | STRING | Message text (may be empty for tool calls) |
| `tool_name` | STRING | Tool invoked (when role is `assistant` or `tool`) |
| `tool_args` | STRING | JSON-encoded tool arguments |
| `tool_result` | STRING | JSON-encoded tool result (role=`tool` only) |
| `user_id` | STRING | Caller identity (Slack handle / email) |
| `ts` | TIMESTAMP | When the turn was recorded |

---

## 3. `intelliops.knowledge` — Knowledge Base

Created by `02_knowledge/00_seed_knowledge_docs.py` and `01_build_knowledge_index.py`.

### `knowledge_docs`
**Functionality:** Curated corpus of Databricks pricing notes, cost-optimization best practices, and internal runbooks. Source-of-truth for the Vector Search index. Change Data Feed enabled so the Delta-sync index can incrementally embed new rows.

| Column | Type | Description |
|---|---|---|
| `doc_id` | STRING | Stable identifier (primary key for the VS index) |
| `title` | STRING | Short human-readable title |
| `content` | STRING | Snippet body — the field that gets embedded |
| `source` | STRING | Origin label: `pricing-doc` / `runbook` / `best-practice` |
| `tags` | STRING | Comma-separated tags for filtering |
| `updated_at` | TIMESTAMP | Last-updated time |

### `knowledge_docs_idx` *(Vector Search index, not a Delta table)*
**Functionality:** Databricks Vector Search Delta-sync index over `knowledge_docs.content`. Used by the agent's `search_knowledge` tool. Re-embeds rows automatically when the source table changes; trigger a manual sync by re-running `01_build_knowledge_index.py`.

- Endpoint: `intelliops_vs_endpoint`
- Embedding model: `databricks-gte-large-en`
- Primary key: `doc_id`

---

## 4. `intelliops.report` — Dashboard / Agent Views

Stable SQL views published by `07_report/*.py`. Dashboards (the four AI/BI tabs) and the agent's `query_features` tool bind here. **Never read raw `system.*` from a dashboard tile** — always go through these views.

### Cost views (`07_report/01_cost_command_center.py`)

#### `cost_monthly_summary`
**Functionality:** Total spend per month for the last 6 months. The top-level number on the Cost Command Center dashboard.

| Column | Type | Description |
|---|---|---|
| `month` | TIMESTAMP | Month bucket (truncated to month) |
| `total_spend_usd` | DOUBLE | Sum of `usage_quantity * pricing.default` (SCD-correct price join) |
| `workspaces` | LONG | Distinct workspaces that incurred spend |
| `unique_jobs` | LONG | Distinct jobs that incurred spend |

#### `cost_current_month_trajectory`
**Functionality:** Day-by-day spend for the current month with a 7-day rolling average. Drives the "are we on pace?" tile.

| Column | Type | Description |
|---|---|---|
| `usage_date` | DATE | Day in the current month |
| `daily_spend` | DOUBLE | USD spent on that day |
| `cumulative_spend` | DOUBLE | Running total since the 1st |
| `rolling_7d_avg` | DOUBLE | 7-day trailing average daily spend |

#### `cost_top_drivers_mtd`
**Functionality:** Top 10 jobs by month-to-date USD spend. Reads from `feat_job_cost_trend` (already aggregated), not raw billing.

| Column | Type | Description |
|---|---|---|
| `job_name` | STRING | Job name |
| `workspace_id` | STRING | Workspace ID |
| `mtd_cost` | DOUBLE | Month-to-date USD |
| `avg_daily_cost` | DOUBLE | Average daily USD this month |
| `max_growth_pct` | DOUBLE | Highest single-day growth % this month |

#### `cost_savings_captured`
**Functionality:** Recommendations and applied actions logged this month, grouped by skill and action type. The "savings" panel on the dashboard.

| Column | Type | Description |
|---|---|---|
| `skill_name` | STRING | Skill that produced the action |
| `action_type` | STRING | Action class |
| `actions_count` | LONG | Total actions logged this month |
| `applied_count` | LONG | Actions whose status reached `applied` |
| `total_projected_savings` | DOUBLE | Sum of projected USD savings |

#### `cost_by_sku`
**Functionality:** Top 15 SKUs by month-to-date spend. Tells you whether spend is concentrated in jobs / all-purpose / SQL warehouses / etc.

| Column | Type | Description |
|---|---|---|
| `sku_name` | STRING | Databricks SKU |
| `billing_origin_product` | STRING | Product family the SKU belongs to |
| `mtd_spend` | DOUBLE | USD month-to-date |
| `total_dbus` | DOUBLE | Total DBUs consumed |

---

### Cluster views (`07_report/02_cluster_health_map.py`)

#### `cluster_utilization_heatmap`
**Functionality:** Hour-by-hour CPU/memory averages per cluster for the last 7 days. Drives the heatmap tile.

| Column | Type | Description |
|---|---|---|
| `cluster_id` | STRING | Cluster |
| `day` | DATE | Day in the 7-day window |
| `hour_of_day` | INT | 0–23 |
| `avg_cpu` | DOUBLE | Average CPU % for that hour |
| `avg_mem` | DOUBLE | Average memory % for that hour |

#### `cluster_over_provisioned`
**Functionality:** Clusters whose 7-day average CPU AND memory are below the thresholds in `config.py` (`CLUSTER_CPU_LOW_PCT`, `CLUSTER_MEM_LOW_PCT`). Joined with the latest version of `system.compute.clusters` for config detail.

| Column | Type | Description |
|---|---|---|
| `cluster_id` | STRING | Cluster |
| `workspace_id` | STRING | Workspace |
| `cluster_name` | STRING | Display name |
| `worker_node_type` | STRING | Worker VM type |
| `min_workers` | INT | Autoscale min |
| `max_workers` | INT | Autoscale max |
| `avg_cpu_7d` | DOUBLE | 7-day average CPU % |
| `avg_mem_7d` | DOUBLE | 7-day average memory % |
| `avg_nodes` | DOUBLE | Average active node count |

#### `cluster_idle_summary`
**Functionality:** Per-workspace summary of idle compute (CPU < 5%). The `idle_node_pct` is node-weighted, so a workspace running many idle nodes scores higher than one with a few.

| Column | Type | Description |
|---|---|---|
| `workspace_id` | STRING | Workspace |
| `total_clusters` | LONG | Distinct clusters seen in the 7-day window |
| `idle_observations` | LONG | Hour-rows where CPU < 5% |
| `avg_nodes_across_all` | DOUBLE | Average nodes per hour |
| `idle_node_pct` | DOUBLE | Percentage of node-hours spent below 5% CPU |

#### `cluster_size_distribution`
**Functionality:** Cluster count grouped by size bucket (Small / Medium / Large / XLarge) using the **latest** SCD version of each cluster, so resized clusters don't appear twice.

| Column | Type | Description |
|---|---|---|
| `cluster_size_bucket` | STRING | `Small (1-2)` \| `Medium (3-8)` \| `Large (9-20)` \| `XLarge (20+)` |
| `cluster_count` | LONG | Distinct clusters in that bucket |

---

### Job reliability views (`07_report/03_job_reliability.py`)

#### `job_reliability_overall`
**Functionality:** Platform-wide reliability rollup over the 30-day window. One row.

| Column | Type | Description |
|---|---|---|
| `total_jobs` | LONG | Distinct jobs tracked |
| `total_runs` | LONG | Total runs across all jobs |
| `total_failures` | LONG | Runs with `result_state = 'FAILED'` |
| `overall_success_rate_pct` | DOUBLE | `(1 − failed/total) * 100` |
| `avg_failure_rate_pct` | DOUBLE | Average per-job failure rate × 100 |

#### `job_daily_failure_trend`
**Functionality:** Daily run count and failure-rate % for the last 30 days. Aggregated per `run_id` first so multi-period streaming runs don't inflate counts.

| Column | Type | Description |
|---|---|---|
| `run_date` | DATE | Day the run started |
| `total_runs` | LONG | Completed runs on that day |
| `failures` | LONG | Failed runs on that day |
| `failure_rate_pct` | DOUBLE | `failures / total_runs * 100` |

#### `job_most_unreliable`
**Functionality:** Top 15 jobs by failure rate (minimum 5 runs in the window). Use to prioritize reliability work.

| Column | Type | Description |
|---|---|---|
| `job_name` | STRING | Job |
| `workspace_id` | STRING | Workspace |
| `total_runs` | LONG | Runs in the window |
| `failed_runs` | LONG | Failed runs |
| `failure_rate_pct` | DOUBLE | Failure rate × 100 |
| `avg_duration_min` | DOUBLE | Average run duration (minutes) |
| `max_duration_min` | DOUBLE | Longest run (minutes) |

#### `job_sla_breaches`
**Functionality:** Jobs whose average duration exceeds `SLA_DURATION_MINUTES` (default 60). Estimates the number of runs that breached.

| Column | Type | Description |
|---|---|---|
| `job_name` | STRING | Job |
| `workspace_id` | STRING | Workspace |
| `total_runs` | LONG | Runs in the window |
| `avg_duration_min` | DOUBLE | Average duration (minutes) |
| `max_duration_min` | DOUBLE | Longest run (minutes) |
| `est_sla_breaches` | LONG | Estimated breach count if avg exceeds SLA |

#### `job_duration_anomalies`
**Functionality:** Most recent runs whose duration exceeds the job's mean + Nσ (N = `JOB_DURATION_ANOMALY_SIGMA`). Flags jobs that are getting slower.

| Column | Type | Description |
|---|---|---|
| `job_name` | STRING | Job |
| `workspace_id` | STRING | Workspace |
| `latest_duration_min` | DOUBLE | Most recent run duration (minutes) |
| `avg_duration_min` | DOUBLE | Historical mean (minutes) |
| `threshold_min` | DOUBLE | `mean + Nσ` (minutes) |
| `z_score` | DOUBLE | `(latest − mean) / σ` |

---

### Leaderboard views (`07_report/04_optimization_leaderboard.py`)

All read from `agent_action_log`. Drive the Optimization Leaderboard dashboard tab.

#### `agent_activity_mtd`
**Functionality:** Current-month activity grouped by skill and action type.

| Column | Type | Description |
|---|---|---|
| `skill_name` | STRING | Skill that produced the action |
| `action_type` | STRING | Action class |
| `total_actions` | LONG | Total actions logged this month |
| `applied` | LONG | Count where `status = 'applied'` |
| `proposed` | LONG | Count where `status = 'proposed'` |
| `rejected` | LONG | Count where `status = 'rejected'` |
| `total_projected_savings` | DOUBLE | Sum of projected USD savings |

#### `agent_monthly_savings_trend`
**Functionality:** Per-month savings trend across the full history.

| Column | Type | Description |
|---|---|---|
| `month` | TIMESTAMP | Month bucket |
| `total_savings` | DOUBLE | Sum of projected USD savings that month |
| `total_actions` | LONG | Total actions logged |
| `applied_actions` | LONG | Actions that reached `applied` |

#### `agent_recent_actions`
**Functionality:** Last 50 actions across all skills. The activity-feed tile.

| Column | Type | Description |
|---|---|---|
| `action_timestamp` | TIMESTAMP | When recorded |
| `skill_name` | STRING | Skill |
| `action_type` | STRING | Action class |
| `target_name` | STRING | Human-readable target |
| `description` | STRING | Plain-English description |
| `savings` | DOUBLE | Projected USD savings |
| `status` | STRING | Action status |

#### `agent_savings_by_skill`
**Functionality:** Current-month savings totals per skill. Powers the "which skill is paying for itself" view.

| Column | Type | Description |
|---|---|---|
| `skill_name` | STRING | Skill |
| `total_savings` | DOUBLE | Sum of projected USD savings this month |
| `action_count` | LONG | Action count this month |

---

## 5. Object count summary

| Schema | Tables | Views | Other |
|---|---|---|---|
| `intelliops.feature_store` | 4 | 0 | — |
| `intelliops.memory` | 1 | 0 | — |
| `intelliops.knowledge` | 1 | 0 | 1 VS index |
| `intelliops.report` | 0 | 18 | — |
| **Total** | **6** | **18** | **1 VS index** |

## 6. Rules of thumb

- **For dashboards and the agent: read from `intelliops.report.*` first.** Stable contract, fast to render.
- **For ad-hoc / drill-down: read from `intelliops.feature_store.*`.** Richer columns, same freshness.
- **Hit `system.*` directly only when the data you need isn't pre-aggregated** — and always apply the SCD-correct joins (latest `change_time` for `clusters`/`jobs`; `usage_start_time` within `price_start_time`–`price_end_time` for `list_prices`).
