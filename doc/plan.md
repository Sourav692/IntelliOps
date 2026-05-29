# IntelliOps V1 — Implementation Plan

## Overview
Build IntelliOps V1 as Databricks Python notebooks organized by module. System-tables-only approach — no Spark logs or pipeline event logs. The accelerator observes, predicts, acts, and reports on Databricks platform health using system tables.

## Project Structure
```
IntelliOps/
├── config/
│   └── config.py                  # Central configuration (catalog, thresholds, budgets)
├── 00_setup/
│   └── 00_setup_feature_store.py  # Create feature store Delta tables in Unity Catalog
├── 01_observe/
│   ├── 01_feat_cluster_utilization.py  # Build feat_cluster_utilization from node_timeline
│   ├── 02_feat_job_cost_trend.py       # Build feat_job_cost_trend from billing + lakeflow
│   └── 03_feat_job_health.py           # Build feat_job_health from job_run_timeline
├── 02_predict/
│   ├── 01_cost_spike_predictor.py      # Time-series cost forecasting (Prophet via MLflow)
│   └── 02_job_failure_predictor.py     # Job failure risk scoring model
├── 03_act/
│   ├── 01_cost_spike_alert.py          # Skill 1: Cost spike alert + root cause
│   ├── 02_cluster_right_sizing.py      # Skill 2: Cluster right-sizing + auto-apply
│   ├── 03_job_failure_scorer.py        # Skill 3: Job failure risk scorer
│   ├── 04_wasteful_compute_detector.py # Skill 4: Wasteful compute detector
│   └── 05_budget_forecast.py           # Skill 5: Budget forecast & guardrail
├── 04_report/
│   ├── 01_cost_command_center.py       # Genie dashboard: cost metrics
│   ├── 02_cluster_health_map.py        # Genie dashboard: cluster utilization
│   ├── 03_job_reliability.py           # Genie dashboard: failure rates, SLA
│   └── 04_optimization_leaderboard.py  # Genie dashboard: agent actions, savings
├── orchestrator.py                # Main orchestrator notebook (runs all modules)
└── utils/
    ├── databricks_api.py          # REST API helpers (clusters edit, etc.)
    └── notifications.py           # Slack/Teams notification helpers
```

## Implementation Steps

### Step 1: Config & Setup
- `config/config.py` — Central config: catalog/schema names, cost thresholds (25% spike), CPU/memory thresholds (30%/40%), idle hours (2h), budget limits, notification endpoints
- `00_setup/00_setup_feature_store.py` — DDL to create the 3 feature store Delta tables + agent action log table in Unity Catalog

### Step 2: Observe — Feature Store Notebooks (Module 1)
- `01_feat_cluster_utilization.py` — Reads `system.compute.node_timeline`, aggregates hourly avg/peak CPU, memory, node count per cluster
- `02_feat_job_cost_trend.py` — Joins `system.billing.usage` + `list_prices` + `lakeflow.jobs`, computes daily cost, rolling 14-day avg, cost growth %
- `03_feat_job_health.py` — Reads `system.lakeflow.job_run_timeline`, computes failure rates, avg/stddev/max duration per job (30-day window)

### Step 3: Predict — ML Models (Module 2)
- `01_cost_spike_predictor.py` — Prophet model on historical billing data, trained per workspace/job, logged to MLflow. Predicts next 7-day cost. Alerts if >20% over budget
- `02_job_failure_predictor.py` — Classification model scoring failure probability per job run using historical failure rate, cluster CPU pressure, duration anomaly (>mean+2σ)

### Step 4: Act — AI Agent Skills (Module 3)
- **Skill 1** (`01_cost_spike_alert.py`): Triggers on cost_growth_pct > 25%. Identifies root cause (config change, volume increase). Generates plain-English explanation
- **Skill 2** (`02_cluster_right_sizing.py`): Triggers on avg_cpu < 30% for 5+ hours. Calculates optimal workers. Calls Clusters REST API (`POST /api/2.0/clusters/edit`) with human approval gate
- **Skill 3** (`03_job_failure_scorer.py`): Scores each job with failure probability. Alerts if score > threshold. Classifies likely cause after failure
- **Skill 4** (`04_wasteful_compute_detector.py`): Daily scan for jobs on all-purpose compute, idle clusters >2h, clusters with 0 tasks. Outputs "Top 5 changes that save $X"
- **Skill 5** (`05_budget_forecast.py`): Projects end-of-month spend using linear trend. Alerts if forecast > 90% budget

### Step 5: Report — Dashboard Notebooks (Module 4)
- SQL views/queries powering 4 Genie dashboard views:
  - Cost Command Center (monthly spend, forecast, top 10 drivers, savings)
  - Cluster Health Map (utilization heatmap, over-provisioned, idle)
  - Job Reliability (failure rates, SLA breaches, duration anomalies)
  - Optimization Leaderboard (agent actions taken, $ saved, failures prevented)

### Step 6: Utils & Orchestrator
- `utils/databricks_api.py` — Wrapper for Clusters REST API (edit, terminate), with retry and error handling
- `utils/notifications.py` — Slack/Teams webhook notification helper
- `orchestrator.py` — Main entry point that runs observe → predict → act → report in sequence

## Key Design Decisions
- All notebooks use `spark.sql()` for system table queries (portable across workspaces)
- Feature store tables written to a configurable Unity Catalog schema (e.g., `intelliops.feature_store`)
- Agent action log table tracks all recommendations and actions for the optimization leaderboard
- Human-in-the-loop approval for production cluster modifications (REST API calls)
- MLflow used for model tracking (cost predictor + failure predictor)
- 15-min micro-batch cadence for feature refresh (configurable)
