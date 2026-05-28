# IntelliOps V2

A **support agent for Databricks cost observability** at the job and cluster level. Answers questions like *"why did cluster X cost $400 yesterday?"* and *"which jobs are wasting the most spend this week?"* — backed by Unity Catalog system tables, pre-aggregated Delta features, and an LLM agent with tool access.

> Architecture and module responsibilities live in [`ARCHITECTURE.md`](./ARCHITECTURE.md). Treat that file as the source of truth; this README only covers **how to run it on Databricks**.

---

## 1. Prerequisites

| Requirement | Notes |
|---|---|
| Databricks workspace | Any cloud (AWS / Azure / GCP). |
| Unity Catalog | Required — features and memory live in UC. |
| System tables enabled | `system.billing.*`, `system.compute.*`, `system.lakeflow.*` must be enabled for your metastore. |
| DBR version | 14.3 LTS or newer recommended. |
| Cluster | Single-node serverless or a small all-purpose cluster (4–8 cores) for Observe / Report. |
| Permissions | `USE CATALOG` + `CREATE SCHEMA` on the target catalog; `SELECT` on `system.*`; `MANAGE` on clusters you intend to right-size. |
| Optional | Slack / Teams incoming webhook for notifications; Databricks Vector Search endpoint for the Knowledge module. |

### Enable system tables (one-time, per metastore)

```sql
-- Run as a metastore admin
ALTER METASTORE SET SYSTEM SCHEMA ENABLE billing;
ALTER METASTORE SET SYSTEM SCHEMA ENABLE compute;
ALTER METASTORE SET SYSTEM SCHEMA ENABLE lakeflow;
```

---

## 2. Import the Repo

Use **Databricks Repos / Git Folders**:

1. In the workspace UI, go to **Workspace → Repos → Add Repo**.
2. Paste the Git URL, select your branch (`main`).
3. The folder will appear under `/Workspace/Repos/<your-email>/IntelliOps`.

All paths below assume that root.

---

## 3. Configure

Open [`config/config.py`](./config/config.py) and update:

```python
CATALOG = "intelliops"            # change if you want a different UC catalog
SCHEMA   = "feature_store"        # schema for feature + memory tables

WORKSPACE_BUDGETS = {
    "<workspace_id>": 50000,      # monthly USD budgets
}

SLACK_WEBHOOK_URL = None          # optional
TEAMS_WEBHOOK_URL = None          # optional

REQUIRE_APPROVAL_FOR_CLUSTER_EDIT = True   # leave True in production
```

Thresholds (cost spike %, CPU/memory floors, idle hours, budget alert %) are also in this file. Never hardcode them elsewhere.

---

## 4. One-Time Setup

Run `00_setup/00_setup_feature_store.py` **once** to create the Delta tables.

From the workspace UI:
- Open the notebook, attach to a cluster, **Run All**.

From the CLI:
```bash
databricks workspace import-dir ./ /Workspace/Repos/<your-email>/IntelliOps --overwrite
databricks jobs submit --json '{
  "run_name": "intelliops-setup",
  "notebook_task": {"notebook_path": "/Workspace/Repos/<you>/IntelliOps/00_setup/00_setup_feature_store"},
  "new_cluster": {"spark_version": "14.3.x-scala2.12", "node_type_id": "i3.xlarge", "num_workers": 0}
}'
```

This creates: `intelliops.feature_store.{feat_cluster_utilization, feat_job_cost_trend, feat_job_health, agent_action_log}` plus memory tables.

---

## 5. Run the Scheduled Pipeline

`orchestrator.py` runs the **scheduled** stages only: **Observe → Knowledge → Report**. The support agent itself is event-driven (see §6) and is *not* invoked here.

> **What runs today:** only Observe and Report. Knowledge is **wired into the orchestrator but its notebook (`02_knowledge/01_build_knowledge_index`) does not exist yet** — that module is a placeholder (see §10). `RUN_KNOWLEDGE` therefore defaults to `False`; flip it to `True` only after the Knowledge module is implemented.

### Option A — Manual run (good for first validation)

Open `orchestrator.py`, attach to a cluster, **Run All**. Tail the output; each stage prints `✔` / `❌` per notebook.

### Option B — Scheduled Databricks Job (recommended)

Create one job with three tasks at different cadences, or three separate jobs. Example single-job spec:

```json
{
  "name": "IntelliOps",
  "tasks": [
    {
      "task_key": "observe",
      "notebook_task": {"notebook_path": "/Workspace/Repos/<you>/IntelliOps/orchestrator", "base_parameters": {"RUN_OBSERVE": "true", "RUN_KNOWLEDGE": "false", "RUN_REPORT": "false"}},
      "new_cluster": {"spark_version": "14.3.x-scala2.12", "node_type_id": "i3.xlarge", "num_workers": 1},
      "schedule": {"quartz_cron_expression": "0 */15 * * * ?", "timezone_id": "UTC"}
    },
    {
      "task_key": "report",
      "notebook_task": {"notebook_path": "/Workspace/Repos/<you>/IntelliOps/orchestrator", "base_parameters": {"RUN_OBSERVE": "false", "RUN_KNOWLEDGE": "false", "RUN_REPORT": "true"}},
      "depends_on": [{"task_key": "observe"}]
    },
    {
      "task_key": "knowledge",
      "_comment": "Enable only after 02_knowledge/01_build_knowledge_index exists.",
      "notebook_task": {"notebook_path": "/Workspace/Repos/<you>/IntelliOps/orchestrator", "base_parameters": {"RUN_OBSERVE": "false", "RUN_KNOWLEDGE": "true", "RUN_REPORT": "false"}},
      "schedule": {"quartz_cron_expression": "0 0 6 ? * SUN", "timezone_id": "UTC"}
    }
  ]
}
```

Recommended cadence:

| Stage | Cadence |
|---|---|
| Observe | every 15 min |
| Report | daily |
| Knowledge | weekly |

---

## 6. The Support Agent

The agent (in `03_agent/`) is **event-driven** — it is invoked when a user asks a question via `06_interface/` (Slack slash command or a Databricks App). It is *not* invoked from `orchestrator.py`.

> `02_knowledge/`, `03_agent/`, `05_memory/`, `06_interface/`, and `08_eval/` are placeholders in the current commit. See `ARCHITECTURE.md` §4 for what each module will own when implemented.

Once implemented, the typical flow will be:

1. User runs `/intelliops why did <cluster> cost $X yesterday?` in Slack.
2. `06_interface/` forwards to the agent endpoint.
3. `03_agent/` plans, calls tools in `04_tools/`, optionally consults the RAG index in `02_knowledge/`.
4. Answer returned to the user; tool calls and outcomes logged to `05_memory/agent_action_log`.
5. Any cluster mutation is gated by `REQUIRE_APPROVAL_FOR_CLUSTER_EDIT`.

---

## 7. Running a Single Tool / Notebook Manually

Every file under `01_observe/`, `04_tools/`, and `07_report/` is a Databricks notebook with the `# Databricks notebook source` header. To run one:

1. Open it in the workspace.
2. Attach to a cluster.
3. **Run All**.

This is useful for ad-hoc debugging (e.g., "rerun the cost trend feature for today only").

---

## 8. Dashboards

> Running the report notebooks **does not create a dashboard by itself** — they only publish stable SQL views (`intelliops.report.*`) that a dashboard binds to. Dashboards are separate workspace objects.

### 8.1 What the report notebooks do today

Each notebook in `07_report/` issues `CREATE OR REPLACE VIEW intelliops.report.<name>` so dashboard tiles have a stable contract. After the scheduled Report stage runs, you get views like:

| Tab | Views |
|---|---|
| Cost Command Center | `cost_monthly_summary`, `cost_current_month_trajectory`, `cost_top_drivers_mtd`, `cost_savings_captured`, `cost_by_sku` |
| Cluster Health Map | `cluster_utilization_heatmap`, `cluster_over_provisioned`, `cluster_idle_summary`, `cluster_size_distribution` |
| Job Reliability | `job_reliability_overall`, `job_daily_failure_trend`, `job_most_unreliable`, `job_sla_breaches`, `job_duration_anomalies` |
| Optimization Leaderboard | `agent_activity_mtd`, `agent_monthly_savings_trend`, `agent_recent_actions`, `agent_savings_by_skill` |

### 8.2 Create the dashboard (programmatic)

Run `07_report/00_create_dashboard.py` **once**:

1. Attach to any cluster (DBR 14+).
2. Fill the three notebook widgets at the top:
   - `warehouse_id` — SQL Warehouse ID (Workspace → SQL → SQL Warehouses → *Connection details*)
   - `parent_path` — where to put the dashboard (e.g. `/Workspace/Users/<your-email>`)
   - `display_name` — defaults to `IntelliOps Cost Observability`
3. **Run All**. The notebook prints the new dashboard URL.

Open the URL — you'll see one tab per view group with a table tile per view. Customize visualizations (counter / line / bar) in the UI; the dataset bindings are already correct.

### 8.3 Manual fallback

If the programmatic notebook fails (Lakeview specs are sensitive):

1. **Dashboards → Create dashboard** in the workspace.
2. For each view, add a dataset: `SELECT * FROM intelliops.report.<view>`.
3. Drop a table tile onto the canvas, point it at the dataset, repeat.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Table or view not found: system.billing.usage` | System tables not enabled | Run the `ALTER METASTORE` SQL in §1. |
| `Permission denied` writing to `intelliops.*` | Missing UC privileges | Grant `USE CATALOG`, `CREATE SCHEMA`, `MODIFY` on the target catalog. |
| Observe finishes but dashboards are empty | Report stage didn't run | Run `orchestrator.py` with `RUN_REPORT=True`. |
| Agent right-sizing call fails | `REQUIRE_APPROVAL_FOR_CLUSTER_EDIT=True` blocks mutation | Expected — approve via the interface, or set to `False` only in a dev workspace. |
| Notebook can't import `04_tools/databricks_api` as a module | Notebooks aren't on `sys.path` by default | Use `%run ./04_tools/databricks_api` from another notebook, or wrap as a wheel in a future iteration. |

---

## 10. What's Next

The current commit has the scheduled side (Observe / Report) working end-to-end and the tool implementations in `04_tools/`. The next implementation milestones:

1. **`02_knowledge/`** — Vector Search index over Databricks pricing docs + internal runbooks.
2. **`03_agent/`** — LLM orchestrator wiring tools + knowledge + memory.
3. **`05_memory/`** — Delta-backed conversation history + extended action log.
4. **`06_interface/`** — Slack slash command and a Databricks App front-end.
5. **`08_eval/`** — Golden question set for offline agent scoring.

Refer to [`ARCHITECTURE.md`](./ARCHITECTURE.md) §7 (rules) before adding new modules or data sources.
