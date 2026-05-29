# IntelliOps V2

A **support agent for Databricks cost observability** at the job and cluster level. Answers questions like *"why did cluster X cost $400 yesterday?"* and *"which jobs are wasting the most spend this week?"* — backed by Unity Catalog system tables, pre-aggregated Delta features, and an LLM agent with tool access.

> Architecture and module responsibilities live in `ARCHITECTURE.md`. Treat that file as the source of truth; this README only covers **how to run it on Databricks**.

---

## 1. Prerequisites

| Requirement | Notes |
| --- | --- |
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

Open `config/config.py` and update:

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

There are two paths. **Use the Asset Bundle** for production; `orchestrator.py` stays as a manual-run option for one-offs.

### Option A — Databricks Asset Bundle *(recommended for production)*

The repo ships a complete bundle (`databricks.yml` + `resources/intelliops_jobs.yml`) that creates **five Jobs**, each on the right schedule:

| Job | What it does | Schedule |
| --- | --- | --- |
| `intelliops_setup` | One-shot: creates catalog, schemas, all Delta tables, seeds knowledge docs | Manual trigger |
| `intelliops_observe` | Refreshes `intelliops.feature_store.*` + runs fast reconciliation | Every 15 min |
| `intelliops_report` | Republishes `intelliops.report.*` views | Daily 05:00 UTC |
| `intelliops_knowledge` | Re-embeds the RAG corpus into Vector Search | Sundays 06:00 UTC |
| `intelliops_reconcile_full` | Deep cross-check against `system.*`; fails loudly on any mismatch | Daily 07:00 UTC |

All run on serverless compute (no cluster spec needed).

**Deploy the bundle:**

```bash
# From the repo root, with the Databricks CLI configured for your workspace:

databricks bundle validate                    # sanity-check the YAML

databricks bundle deploy --target dev          # ships notebooks + jobs (schedules PAUSED)
databricks bundle run intelliops_setup --target dev    # creates UC objects

# When you're happy, deploy to prod (schedules UNPAUSED):
databricks bundle deploy --target prod
```

**Override variables per environment** (e.g. failure email, cron expressions):

```bash
databricks bundle deploy --target prod \
  --var notification_email=oncall@example.com \
  --var observe_cron='0 */10 * * * ?'
```

To pause / resume the schedule without redeploying, edit `pause_status` in `resources/intelliops_jobs.yml` and redeploy, or pause from the Jobs UI.

### Option B — Manual run via `orchestrator.py` *(useful for ad-hoc validation)*

`orchestrator.py` runs Observe → Knowledge → Report sequentially with toggle flags. Open it, attach to any cluster, **Run All**. Output shows `✔` / `❌` per notebook. The bundle deploys this notebook too, but the scheduled Jobs above call individual notebooks directly so they parallelize and isolate failures per task.

> **What runs today:** Setup is one-shot; Observe runs every 15 min and includes the fast reconciliation check; Report runs daily; Knowledge runs weekly; the full reconciliation runs daily and will hard-fail the pipeline if any 🔴 critical mismatch with `system.*` is detected (see [`doc/data_reconciliation.md`](./data_reconciliation.md)).

---

## 6. The Support Agent

The agent (`03_agent/`) is **event-driven** — invoked per question, not from the scheduled orchestrator. It's a single-file LangGraph ReAct agent built with `create_react_agent`, using `ChatDatabricks` (from `databricks-langchain`) against the Foundation Model endpoint named in `LLM_ENDPOINT_NAME`. Tools are closures over config — no globals, no module-loader gymnastics.

### 6.1 One-time setup (in addition to §4)

```text
Run:
  02_knowledge/00_seed_knowledge_docs       # populates the curated corpus
  02_knowledge/01_build_knowledge_index     # creates the Vector Search endpoint + index
  05_memory/00_setup_memory                 # creates agent_conversation table
```

### 6.2 Ask the agent

Open `03_agent/01_ask_agent.py`, set the `question` widget, **Run All**. The notebook prints:

- The agent's answer
- The session ID
- Every tool call made (`query_features`, `query_system_tables`, `search_knowledge`, `log_action_record`)
- A replay of the full conversation from `intelliops.memory.agent_conversation`

### 6.3 What the agent can do today

- Query feature tables and report views (fast path)
- Query `system.*` directly (escape hatch for fresh / un-aggregated data)
- Retrieve from the curated knowledge corpus (RAG)
- Log recommendations into `agent_action_log` so they appear on the Optimization Leaderboard

### 6.4 What it deliberately cannot do today

- Mutate clusters or jobs. The agent never calls the Clusters/Jobs REST API. Mutations remain in `04_tools/02_cluster_right_sizing.py`, gated by `REQUIRE_APPROVAL_FOR_CLUSTER_EDIT`, and triggered manually until `06_interface/` ships with an approval flow.

> Still placeholders: `06_interface/` (Slack / Databricks App front-end) and `08_eval/` (golden question set for offline scoring). See §10.

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
| --- | --- |
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

| Symptom | Likely cause | Fix |  |
| --- | --- | --- | --- |
| `Table or view not found: system.billing.usage` | System tables not enabled | Run the `ALTER METASTORE` SQL in §1. |  |
| `Permission denied` writing to `intelliops.*` | Missing UC privileges | Grant `USE CATALOG`, `CREATE SCHEMA`, `MODIFY` on the target catalog. |  |
| Observe finishes but dashboards are empty | Report stage didn't run | Run `orchestrator.py` with `RUN_REPORT=True`. |  |
| Agent right-sizing call fails | `REQUIRE_APPROVAL_FOR_CLUSTER_EDIT=True` blocks mutation | Expected — approve via the interface, or set to `False` only in a dev workspace. |  |
| Notebook can't import `04_tools/databricks_api` as a module | Notebooks aren't on `sys.path` by default | Use `%run ./04_tools/databricks_api` from another notebook, or wrap as a wheel in a future iteration. |  |

---

## 10. What's Next

The current commit has the scheduled side (Observe / Report), the tool implementations in `04_tools/`, **and** a first cut of `02_knowledge/`, `03_agent/`, and `05_memory/` working end-to-end. Remaining milestones:

1. `06_interface/` — Slack slash command and a Databricks App front-end so users don't have to open a notebook to ask the agent.
2. `08_eval/` — Golden question set + offline scoring so prompt/model changes don't silently regress.
3. **Agent-side cluster mutation flow** — wire the right-sizing tool into the agent behind a human-approval prompt (currently the agent can only describe the change, not propose+apply through the same UI).

Refer to `ARCHITECTURE.md` §7 (rules) before adding new modules or data sources.