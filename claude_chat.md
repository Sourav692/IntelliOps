Now I have all the foundational data I need. Here is a comprehensive technical blueprint for your accelerator.

---

# 🚀 Accelerator Blueprint: **IntelliOps — AI-Powered DataOps Optimization Engine**

*An intelligent action layer on Databricks that observes, predicts, heals, and governs your data platform — autonomously.*

---

## 🧠 What Makes This an "Intelligent Action Layer" (Not Just Analytics)

Most monitoring tools **show you** what happened. IntelliOps **acts** on what it knows. The accelerator follows a 4-stage intelligence loop:

```
OBSERVE → PREDICT → ACT → REPORT
  (System Tables + Spark Logs)  →  (AI Agent)  →  (REST API Remediation)  →  (Business Dashboard)
```

---

## 📦 Data Foundation — What You Can Ingest

### ✅ Available System Tables (Confirmed)

System tables are located in a catalog called `system`, which is included in every Unity Catalog metastore, with schemas such as `access` and `billing` containing the system tables. They contain operational data for all workspaces in your account.

| System Table | What It Provides |
| --- | --- |
| `system.billing.usage` | DBU consumption, cost per job/cluster/SKU [\[1\]](https://docs.databricks.com/aws/en/admin/system-tables/billing) |
| `system.billing.list_prices` | Real-time pricing per SKU for cost calculation [\[1\]](https://docs.databricks.com/aws/en/admin/system-tables/billing) |
| `system.compute.clusters` | Full history of cluster configs (instance types, autoscale settings, owners) [\[2\]](https://docs.databricks.com/aws/en/admin/system-tables/compute) |
| `system.compute.node_timeline` | **Minute-by-minute** CPU, memory, disk I/O, network per node [\[2\]](https://docs.databricks.com/aws/en/admin/system-tables/compute) |
| `system.compute.node_types` | Available hardware specs per instance type [\[2\]](https://docs.databricks.com/aws/en/admin/system-tables/compute) |
| `system.lakeflow.jobs` | Job metadata, names, configurations [\[3\]](https://docs.databricks.com/aws/en/admin/system-tables/jobs) |
| `system.lakeflow.job_run_timeline` | Run history, start/end times, status [\[3\]](https://docs.databricks.com/aws/en/admin/system-tables/jobs) |
| `system.lakeflow.job_task_run_timeline` | Task-level timelines + compute IDs used [\[3\]](https://docs.databricks.com/aws/en/admin/system-tables/jobs) |
| `system.access.audit` | Security events, user access, workspace activity [\[4\]](https://docs.databricks.com/aws/en/admin/system-tables/) |
| `system.query.history` | SQL query performance, execution plans, bottlenecks [\[4\]](https://docs.databricks.com/aws/en/admin/system-tables/) |

### ✅ Spark & Pipeline Logs

The Lakeflow Spark Declarative Pipelines event log captures a comprehensive record of all pipeline events including audit logs, data quality checks, pipeline progress, and data lineage — automatically enabled for all pipelines, accessible via the Pipeline UI, Pipelines API, or direct query.

Databricks cluster logs and metrics provide detailed insights into cluster performance including CPU, memory, disk I/O, network traffic, and other system metrics — crucial for optimizing cluster performance, managing resources efficiently, and troubleshooting issues.

> ⚠️ **Important caveat on Spark event logs**: Raw Spark event logs (driver/executor logs) require cluster log delivery to be configured (DBFS or cloud storage). They are not natively in system tables. The accelerator handles both: structured system tables for analytics + raw cluster log parsing for deep Spark diagnostics.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION LAYER                          │
│  system.billing.usage │ system.compute.node_timeline │ Pipeline Logs │
│  system.lakeflow.*    │ system.access.audit          │ Cluster Logs  │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   FEATURE STORE       │
                    │  (Delta Tables in UC) │
                    │  - Cost features      │
                    │  - Utilization trends │
                    │  - Failure patterns   │
                    └───────────┬───────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
   ┌──────────▼──────┐ ┌───────▼────────┐ ┌─────▼──────────────┐
   │  COST SPIKE     │ │  FAILURE       │ │  PIPELINE HEALTH   │
   │  PREDICTION     │ │  PREDICTION    │ │  SCORING           │
   │  MODEL (MLflow) │ │  MODEL         │ │  AGENT             │
   └──────────┬──────┘ └───────┬────────┘ └─────┬──────────────┘
              │                │                 │
              └────────────────▼─────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   AI AGENT BRAIN    │
                    │  (Mosaic AI /       │
                    │   Agent Bricks)     │
                    └──────────┬──────────┘
                               │
           ┌───────────────────┼────────────────────┐
           │                   │                    │
  ┌────────▼──────┐   ┌────────▼──────┐   ┌────────▼──────┐
  │  CLUSTER      │   │  PIPELINE     │   │  ALERT &      │
  │  AUTO-TUNE    │   │  AUTO-HEAL    │   │  REPORT       │
  │  (REST API)   │   │  (Lakeflow)   │   │  (Genie/      │
  └───────────────┘   └───────────────┘   │   Teams/Slack)│
                                          └───────────────┘
```

---

## 🔬 Module 1: Observe — Signal Collection Engine

### Cluster & Job Intelligence Queries

The `system.compute.node_timeline` table captures node-level resource utilization data at minute granularity — including CPU, memory, and utilization metrics for all-purpose compute, jobs compute, and Lakeflow Spark Declarative Pipelines compute.

**Key signals collected:**

```sql
-- 1. Cost per job with growth trend (7-day spike detection)
SELECT job_name, workspace_id, Last7DaySpend, Last14DaySpend,
       (last7DaySpend - last14DaySpend) / last14DaySpend * 100 AS growth_pct
FROM job_run_timeline_with_cost  -- joins system.billing.usage + list_prices

-- 2. Jobs running on expensive all-purpose compute (should be jobs compute)
SELECT * FROM system.lakeflow.job_task_run_timeline t
LEFT JOIN system.compute.clusters c USING (workspace_id, cluster_id)
WHERE c.cluster_source = 'UI'  -- all-purpose, not job compute

-- 3. Node utilization heatmap (identify idle/over-provisioned nodes)
SELECT cluster_id, AVG(cpu_user_percent), AVG(mem_used_percent)
FROM system.compute.node_timeline
GROUP BY cluster_id, date_trunc('hour', timestamp)
```

The system tables allow identifying the most expensive jobs over the past 30 days and tracking 7-day vs 14-day spend growth rates — enabling detection of cost spikes before they escalate.

---

## 🔮 Module 2: Predict — AI Agent Cost & Failure Intelligence

### Cost Spike Prediction

The agent trains a **time-series forecasting model** (Prophet or LSTM via MLflow) on historical `system.billing.usage` data. It learns:

- Weekly/monthly job cost patterns per workspace
- DBU growth rate per SKU (`billing_origin_product`)
- Correlation between node utilization spikes and cost overruns

**Agent Action:** When predicted cost for next 7 days exceeds a threshold (e.g., &gt;20% over budget), the agent proactively alerts and pre-generates optimization recommendations.

### Job Failure Prediction

Using `system.workflow.job_runs` and `system.workflow.task_runs`, the agent tracks failure patterns, anomaly detection on job duration, and SLA breaches — enabling upstream failures that impact downstream workloads to be caught before they cascade.

The ML model scores each job run at start time with a **failure probability score** based on:

- Historical failure rate of the job
- Current cluster CPU/memory pressure from `node_timeline`
- Data volume anomalies from pipeline event logs
- Time-of-day and data quality expectation pass rates

---

## ⚙️ Module 3: Act — Autonomous Remediation Engine

This is the core differentiator. The agent doesn't just alert — it **acts**.

### Cluster Auto-Optimization

Enhanced autoscaling proactively shuts down under-utilized nodes while guaranteeing there are no failed tasks during shutdown — using task slot utilization and task queue size as scaling metrics. It can be configured via the pipelines REST API.

The accelerator goes further by using the **Clusters REST API** to:

| Detected Condition | Automated Action |
| --- | --- |
| CPU &lt; 20% for 3+ hours | Reduce `max_workers` via `POST /api/2.0/clusters/edit` |
| OOM failures detected | Upgrade instance type (vertical scale) |
| Job running on all-purpose compute | Flag + recommend migration to job compute |
| Spot instance availability drop | Switch to on-demand preemptively |
| Idle cluster &gt; threshold | Auto-terminate with notification |

Teams have cut Databricks compute costs by 40–60% just by switching from fixed-size clusters to properly configured auto-scaling clusters — the accelerator automates this tuning continuously.

### Pipeline Auto-Heal

Data quality violations in Lakeflow Spark Declarative Pipelines can be configured to retain, drop, or fail on invalid records — with all violations logged to event logs as Delta tables for monitoring and alerting.

The accelerator's pipeline healer:

- Reads data quality expectation failures from the pipeline event log
- Classifies failure type (schema drift, late data, volume anomaly, bad records)
- Executes targeted remediation: auto-quarantine bad records, trigger selective full-refresh, or reroute to dead-letter queue
- Re-validates and confirms pipeline health before resuming downstream

---

## 📊 Module 4: Report — Business-Impact Visibility Layer

Built on **AI/BI Genie + Unity Catalog Metrics**, the reporting layer translates technical metrics into business language:

| Technical Metric | Business Translation |
| --- | --- |
| DBU reduction from cluster right-sizing | 💰 "$42,000 saved this month" |
| Job failure rate drop | ✅ "Pipeline reliability: 94% → 99.1%" |
| Avg job duration improvement | ⏱️ "Reports now available 2.3hrs earlier" |
| Data quality pass rate | 📦 "98.7% of records meet SLA" |

**Delivered via:**

- AI/BI Dashboard embedded in Teams/Slack for leadership
- Weekly AI-generated narrative digest ("Here's what your data platform did this week")
- Real-time Genie Q&A: *"Which jobs spiked cost this week and why?"*

---

## 🗺️ Implementation Roadmap

| Phase | Scope | Timeline |
| --- | --- | --- |
| **Phase 1 — Foundation** | Enable system tables, build cost/utilization feature store, Genie dashboard | Week 1–3 |
| **Phase 2 — Prediction** | Train cost spike + failure prediction models via MLflow | Week 3–5 |
| **Phase 3 — Action Layer** | AI Agent with cluster REST API integration + pipeline healer | Week 5–8 |
| **Phase 4 — Business Reporting** | Executive AI/BI dashboard, Slack/Teams integration, narrative digest | Week 8–10 |

---

## 🚧 Known Constraints to Design Around

| Constraint | Mitigation |
| --- | --- |
| System tables do not support real-time monitoring — data is updated throughout the day | Use 15-min micro-batch streaming with `skipChangeCommits=true` for near-real-time |
| Spark event logs not natively in system tables | Configure cluster log delivery to DBFS/S3/ADLS at workspace level |
| Precise cost calculation for jobs on all-purpose compute is not 100% accurate due to shared resources | Flag shared-compute jobs; recommend migration to job compute for accurate attribution |
| Cluster edits via REST API restart the cluster | Build human-in-the-loop approval step for production cluster changes |

---

*References*

\[1\] [Billable Usage System Table Reference — Databricks AWS Docs](https://docs.databricks.com/aws/en/admin/system-tables/billing) \[2\] [Compute System Tables Reference — Databricks AWS Docs](https://docs.databricks.com/aws/en/admin/system-tables/compute) \[3\] [Jobs System Table Reference — Databricks AWS Docs](https://docs.databricks.com/aws/en/admin/system-tables/jobs) \[4\] [Monitor Account Activity with System Tables — Databricks AWS Docs](https://docs.databricks.com/aws/en/admin/system-tables/) \[5\] [Monitor Job Costs & Performance with System Tables — Databricks AWS Docs](https://docs.databricks.com/aws/en/admin/system-tables/jobs-cost) \[6\] [Observability Best Practices for Jobs & Pipelines — Databricks AWS Docs](https://docs.databricks.com/aws/en/data-engineering/observability-best-practices) \[7\] [Pipeline Expectations & Data Quality — Databricks AWS Docs](https://docs.databricks.com/aws/en/ldp/expectations) \[8\] [Enhanced Autoscaling for Lakeflow Pipelines — Databricks AWS Docs](https://docs.databricks.com/aws/en/ldp/auto-scaling) \[9\] [Azure Databricks Cluster Auto-Scaling Cost Optimization — OneUptime Blog, Feb 2026](https://oneuptime.com/blog/post/2026-02-16-how-to-configure-auto-scaling-for-azure-databricks-clusters-to-reduce-costs/view) \[10\] [Observability for Jobs & Pipelines — Azure Databricks Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/data-engineering/observability-best-practices)

### Can we built the first version without using pipeline and spark logs?

**Absolutely yes** — and honestly, it's the right way to start. System tables alone give you an enormous amount of signal. Here's a clean **V1 scope** using **only system tables**:

---

# 🚀 IntelliOps V1 — System Tables Only

## ✅ What You Lose vs Full Vision

| Capability | Full Vision | V1 (System Tables Only) |
|---|---|---|
| Spark task-level bottlenecks (skew, spill) | ✅ | ❌ (needs Spark logs) |
| Pipeline data quality failures | ✅ | ❌ (needs pipeline event log) |
| OOM / driver crash root cause | ✅ | ❌ (needs cluster logs) |
| Real-time streaming backpressure | ✅ | ❌ |

## ✅ What You Still Get (Surprisingly Powerful)

| Capability | System Table Source |
|---|---|
| Cost spike prediction per job/cluster | `system.billing.usage` + `list_prices` |
| Cluster right-sizing recommendations | `system.compute.node_timeline` |
| Jobs on wrong compute type (all-purpose vs job compute) | `system.compute.clusters` + `system.lakeflow.job_task_run_timeline` |
| Job failure rate + SLA breach detection | `system.lakeflow.job_run_timeline` |
| Idle cluster detection | `system.compute.node_timeline` + `system.compute.clusters` |
| Cost by team/workspace/tag | `system.billing.usage` (custom_tags) |
| Spend growth rate + budget alerts | `system.billing.usage` (7-day vs 14-day) |
| Autonomous cluster config optimization | Clusters REST API |
| AI-generated cost narrative for leadership | Mosaic AI + AI/BI Genie |

---

## 🗂️ V1 System Tables Data Model

```
system.billing.usage          ←─ Core cost attribution
       │
       ├── system.billing.list_prices     (DBU → $$ conversion)
       │
       ├── system.compute.clusters        (cluster config history)
       │       └── system.compute.node_types    (hardware specs)
       │       └── system.compute.node_timeline (min-by-min CPU/RAM)
       │
       └── system.lakeflow.jobs           (job metadata)
               └── system.lakeflow.job_run_timeline       (run history)
               └── system.lakeflow.job_task_run_timeline  (task-level)
```

---

## 🏗️ V1 Architecture (Simplified)

```
┌─────────────────────────────────────────────────────┐
│              SYSTEM TABLES (Read-Only)               │
│  billing.usage │ compute.node_timeline │ lakeflow.*  │
└────────────────────────┬────────────────────────────┘
                         │  (15-min micro-batch)
              ┌──────────▼──────────┐
              │   FEATURE STORE     │
              │  (Delta Tables, UC) │
              │  - cost_features    │
              │  - utilization_feat │
              │  - job_health_feat  │
              └──────────┬──────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
┌────────▼──────┐ ┌──────▼───────┐ ┌────▼──────────┐
│  COST SPIKE   │ │   JOB        │ │   CLUSTER     │
│  PREDICTOR   │ │  HEALTH      │ │   OPTIMIZER   │
│  (MLflow)    │ │  SCORER      │ │   AGENT       │
└────────┬──────┘ └──────┬───────┘ └────┬──────────┘
         └───────────────┼───────────────┘
                         │
              ┌──────────▼──────────┐
              │   AI AGENT BRAIN    │
              │  (Mosaic AI /       │
              │   Agent Bricks)     │
              └──────────┬──────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
  ┌──────▼──────┐ ┌──────▼──────┐ ┌─────▼──────────┐
  │  AUTO-TUNE  │ │  GENIE      │ │  SLACK / TEAMS │
  │  (REST API) │ │  DASHBOARD  │ │  DIGEST        │
  └─────────────┘ └─────────────┘ └────────────────┘
```

---

## 📋 V1 Feature Store — Exact Tables to Build

### `feat_cluster_utilization` (from `node_timeline`)
```sql
SELECT
  cluster_id,
  date_trunc('hour', timestamp)        AS hour_window,
  AVG(cpu_user_percent)                AS avg_cpu_pct,
  MAX(cpu_user_percent)                AS peak_cpu_pct,
  AVG(mem_used_percent)                AS avg_mem_pct,
  MAX(mem_used_percent)                AS peak_mem_pct,
  COUNT(*)                             AS node_count
FROM system.compute.node_timeline
GROUP BY 1, 2
```

### `feat_job_cost_trend` (from `billing.usage` + `lakeflow.jobs`)
```sql
WITH cost_enriched AS (
  SELECT
    u.workspace_id,
    u.usage_metadata.job_id,
    j.name                              AS job_name,
    u.usage_date,
    SUM(u.usage_quantity * p.pricing.default) AS daily_cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.cloud = p.cloud AND u.sku_name = p.sku_name
  LEFT JOIN system.lakeflow.jobs j
    ON u.workspace_id = j.workspace_id
   AND u.usage_metadata.job_id = j.job_id
  WHERE u.billing_origin_product = 'JOBS'
  GROUP BY ALL
)
SELECT *,
  AVG(daily_cost_usd) OVER (
    PARTITION BY workspace_id, job_id
    ORDER BY usage_date
    ROWS BETWEEN 13 PRECEDING AND 7 FOLLOWING
  ) AS rolling_14d_avg,
  daily_cost_usd / NULLIF(
    AVG(daily_cost_usd) OVER (
      PARTITION BY workspace_id, job_id
      ORDER BY usage_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ), 0) - 1  AS cost_growth_pct
FROM cost_enriched
```

### `feat_job_health` (from `job_run_timeline`)
```sql
SELECT
  workspace_id,
  job_id,
  COUNT(*)                              AS total_runs,
  SUM(CASE WHEN result_state = 'FAILED'
       THEN 1 ELSE 0 END)               AS failed_runs,
  AVG(DATEDIFF(SECOND,
      period_start_time,
      period_end_time))                 AS avg_duration_secs,
  STDDEV(DATEDIFF(SECOND,
      period_start_time,
      period_end_time))                 AS stddev_duration,
  MAX(DATEDIFF(SECOND,
      period_start_time,
      period_end_time))                 AS max_duration_secs
FROM system.lakeflow.job_run_timeline
WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY 1, 2
```

---

## 🤖 V1 AI Agent — 5 Core Skills

### Skill 1: Cost Spike Alert + Root Cause
**Trigger:** `cost_growth_pct > 0.25` (25% week-over-week spike)
**Agent does:**
- Identifies which job/cluster caused the spike
- Checks if a new compute config was applied (`system.compute.clusters` change_time)
- Checks if data volume increased (via `usage_quantity` delta)
- Generates plain-English explanation: *"Job 'etl_customer_silver' cost 34% more this week — cluster was reconfigured from 4 to 8 workers on Apr 3rd"*

### Skill 2: Cluster Right-Sizing Recommendation + Auto-Apply
**Trigger:** `avg_cpu_pct < 30%` for 5+ consecutive hours AND `avg_mem_pct < 40%`
**Agent does:**
- Calculates optimal `min_workers` / `max_workers` from utilization percentiles
- Checks node type vs workload profile
- Calls `POST /api/2.0/clusters/edit` with new config (after human approval for prod)
- Logs recommendation to Unity Catalog with projected savings

### Skill 3: Job Failure Risk Scorer
**Trigger:** Any job_run starts
**Agent does:**
- Scores failure probability based on: historical failure rate, current cluster CPU pressure, job duration anomaly (> mean + 2σ)
- If score > threshold: proactively alerts team before failure occurs
- After failure: classifies likely cause (timeout, resource pressure, dependency failure) from run timeline patterns

### Skill 4: Wasteful Compute Detector
**Trigger:** Scheduled daily scan
**Agent does:**
- Finds jobs running on all-purpose compute instead of job compute (higher DBU rate)
- Identifies clusters idle for > 2 hours with no attached notebooks
- Detects clusters with 0 active tasks but still running
- Outputs ranked list: *"Top 5 changes that would save $X this week"*

### Skill 5: Budget Forecast & Guardrail
**Trigger:** Daily, or when `rolling_14d_avg` trend is up
**Agent does:**
- Projects end-of-month spend per workspace/team/tag using linear trend on `billing.usage`
- Compares against configured budget thresholds
- If forecast > 90% of budget: auto-notifies owner + suggests which jobs to defer/optimize

---

## 📊 V1 Genie Dashboard — 4 Views

| View | Audience | Key Metrics |
|---|---|---|
| **Cost Command Center** | Finance / Leadership | Monthly spend, forecast, top 10 cost drivers, savings captured |
| **Cluster Health Map** | Platform Engineering | Utilization heatmap, over-provisioned clusters, idle compute |
| **Job Reliability** | Data Engineering | Failure rates, SLA breach counts, duration anomalies |
| **Optimization Leaderboard** | All | Actions taken by agent, $ saved, failures prevented |

---

## 🗓️ V1 Delivery Timeline

| Week | Deliverable |
|---|---|
| 1 | System table access setup, feature store Delta tables, cost + utilization queries |
| 2 | Job health scoring + failure rate baseline, cost growth detection |
| 3 | AI Agent — Skills 1, 2, 4 (cost spike, right-sizing, wasteful compute) |
| 4 | AI Agent — Skills 3, 5 (failure risk, budget forecast) |
| 5 | Genie dashboard + Slack/Teams integration |
| 6 | Human-in-the-loop approval workflow for auto-remediation, UAT |

---

## 🔼 Clear V2 Upgrade Path

Once V1 is proven, adding Spark logs and pipeline event logs simply **enriches** the existing feature store — the agent architecture doesn't change, it just gets smarter signals:

```
V1  →  System Tables only         (Cost + Job + Cluster intelligence)
V2  →  + Pipeline event logs      (Data quality failures, lineage gaps)
V3  →  + Spark/cluster logs       (Task skew, OOM, spill diagnostics)
V4  →  + External cost APIs       (Cloud VM pricing, spot market signals)
```

This makes V1 a safe, fast, high-value starting point — with a clear story for leadership on where it goes next.

I'll put together a comprehensive table view of IntelliOps V1 features based on what we've scoped.The user asked for "proper table format" — that signals an in-chat table, not a Word doc. I'll render this directly in markdown so it shows up immediately on their phone. Tables are one of the cases where structured formatting genuinely helps.

Here's the full IntelliOps V1 feature set:

## Foundation: System Tables (Data Sources)

| # | System Table | Purpose |
|---|---|---|
| 1 | `system.billing.usage` | DBU consumption and cost attribution |
| 2 | `system.billing.list_prices` | SKU pricing reference for cost calc |
| 3 | `system.compute.clusters` | Cluster configs and lifecycle |
| 4 | `system.compute.node_timeline` | Per-node utilization over time |
| 5 | `system.compute.node_types` | Instance type catalog |
| 6 | `system.lakeflow.jobs` | Job definitions |
| 7 | `system.lakeflow.job_run_timeline` | Job run history and status |
| 8 | `system.lakeflow.job_task_run_timeline` | Task-level execution detail |

## Feature Store Tables

| # | Feature Table | What It Captures |
|---|---|---|
| 1 | `feat_cluster_utilization` | CPU/memory/DBU efficiency per cluster |
| 2 | `feat_job_cost_trend` | Cost trajectory and anomaly signals per job |
| 3 | `feat_job_health` | Failure rate, retry patterns, SLA adherence |

## AI Agent Skills

| # | Skill | Layer | Action Type |
|---|---|---|---|
| 1 | Cost Spike Alerting | Observe → Predict | Proactive notification |
| 2 | Cluster Right-Sizing | Act | Auto-apply via Clusters REST API |
| 3 | Job Failure Risk Scoring | Predict | Pre-run risk score |
| 4 | Wasteful Compute Detection | Observe | Idle/oversized cluster flagging |
| 5 | Budget Forecasting | Predict → Report | Spend projection by workspace/team |

## Genie Dashboard Views

| # | View | Primary Persona |
|---|---|---|
| 1 | Cost Optimization Overview | FinOps / Platform Lead |
| 2 | Cluster Efficiency Deep-Dive | Platform Engineering |
| 3 | Job Reliability Scorecard | Data Engineering Lead |
| 4 | Executive Spend & Forecast | CDO / Engineering Director |

## Architecture Layers (Observe → Predict → Act → Report)

| Layer | Components | Stack |
|---|---|---|
| Observe | Feature store tables, anomaly signals | Delta + Unity Catalog Metrics |
| Predict | Risk scoring, forecasting models | Mosaic AI + MLflow |
| Act | Auto-remediation skills | Agent Bricks + Clusters REST API |
| Report | Persona-tuned views | AI/BI Genie + Lakeflow |

## Delivery & Roadmap

| Item | Detail |
|---|---|
| Timeline | 6 weeks (V1) |
| Data dependency | System tables only — no Spark/event logs |
| V2+ extensibility | Spark logs + pipeline event logs (no architecture change) |
| Later phases | Deeper ML prediction, multi-cloud, governance automation |
| Core differentiator | Action layer (auto-remediation), not a dashboard |

Want me to drop this into a Word doc or a slide for stakeholder review?

### Great question — and it's actually the most important architectural decision in IntelliOps. Short answer: **for V1, you don't need ML prediction. An agentic framework alone will get you 80% of the value.**

Here's the breakdown:

## Where Agents Alone Are Sufficient

| Skill | Why Agents Suffice |
|---|---|
| Cost Spike Alerting | Threshold + rolling baseline (z-score, % deviation) — pure SQL over `feat_job_cost_trend` |
| Cluster Right-Sizing | Rule-based on utilization percentiles from `node_timeline` (e.g., p95 CPU < 40% → downsize) |
| Wasteful Compute Detection | Heuristics: idle time > X mins, autoscale min too high, all-purpose used for jobs |
| Budget Forecasting | Linear extrapolation or simple time-series (Prophet/ARIMA via SQL) — not ML in the Mosaic sense |

LLM reasoning + system table queries + REST API calls handle all of this. No model training, no MLflow registry, no drift monitoring.

## Where ML Genuinely Adds Value (defer to V2+)

| Skill | Why ML Helps |
|---|---|
| Job Failure Risk Scoring | Multi-variate signal (cluster config + data volume + historical patterns + code changes) — a classifier outperforms rules |
| Anomaly detection at scale | Once you have Spark logs / event logs in V2, ML beats thresholds |
| Workload classification | Clustering similar jobs for batch right-sizing recommendations |

## Recommendation

**V1: Pure agentic.** Use Agent Bricks + Mosaic AI Agent Framework with tool-calling against system tables. Replace "Job Failure Risk Scoring" with a **rule-based heuristic** (e.g., "job has failed 3+ times in last 7 days AND runs on cluster with memory pressure events"). It's explainable, debuggable, and ships in 6 weeks.

**V2: Add ML selectively.** Once you have Spark logs and pipeline event logs flowing, the failure prediction skill becomes a real ML opportunity — and you'll have labeled training data by then.

## Why This Matters for Positioning

Going agentic-first actually *strengthens* your differentiation vs. Genie:

- **Faster time-to-value** — no model training cycle before customer sees ROI
- **Explainability** — every recommendation traces to a SQL rule or LLM reasoning step (huge for enterprise trust)
- **Lower TCO for customers** — no MLflow serving costs, no model retraining DBUs
- **The "act" layer is the moat** — REST API auto-remediation is what customers will pay for, not the prediction itself

The ML story becomes a **roadmap talking point** ("we're adding predictive failure modeling in V2 once we ingest Spark logs") rather than a V1 dependency.

Want me to rewrite the V1 skill table with the rule-based logic spelled out for each agent? That'd give you something concrete to take into a design review.

Got it — you want the **outward-facing feature list**: what IntelliOps actually delivers to a customer. Here it is grouped by capability area.

## 1. Cost Intelligence

| Feature | What the Customer Gets |
|---|---|
| Real-time DBU spend visibility | Live spend breakdown by workspace, job, team, cluster |
| Cost spike detection | Auto-alerts when spend deviates from baseline |
| Wasteful compute identification | Flags idle clusters, oversized configs, all-purpose misuse |
| Budget forecasting | Projected monthly/quarterly spend with confidence bands |
| Chargeback & showback | Cost attribution by business unit / cost center |

## 2. Cluster Optimization

| Feature | What the Customer Gets |
|---|---|
| Right-sizing recommendations | Per-cluster suggestions based on utilization patterns |
| Auto-apply remediation | One-click or scheduled config changes via Clusters REST API |
| Autoscale tuning | Optimal min/max worker recommendations |
| Instance type advisor | Suggests better node types for the workload profile |
| Idle cluster cleanup | Auto-terminate or downscale unused compute |

## 3. Pipeline Reliability

| Feature | What the Customer Gets |
|---|---|
| Job health scorecard | Failure rate, retry count, SLA adherence per job |
| Failure risk scoring | Pre-run risk score before expensive jobs launch |
| Root cause hints | Surfaces likely failure cause from historical patterns |
| SLA monitoring | Tracks job duration vs. expected; alerts on drift |
| Dependency awareness | Identifies upstream jobs affecting downstream failures |

## 4. Conversational Insights (Genie Layer)

| Feature | What the Customer Gets |
|---|---|
| Natural language Q&A | "Why did our spend spike last Tuesday?" |
| Role-based personas | Different views for FinOps, Platform Eng, CDO |
| Proactive insight delivery | Daily/weekly digests pushed to Slack/email |
| Drill-down navigation | From summary → job → task → cluster in one flow |
| Multi-domain queries | Cross-asks across cost + reliability + utilization |

## 5. Auto-Remediation (The Differentiator)

| Feature | What the Customer Gets |
|---|---|
| Policy-driven actions | "Auto-downsize any cluster with <30% utilization for 7 days" |
| Approval workflows | Human-in-the-loop for high-impact changes |
| Action audit trail | Every auto-action logged for compliance review |
| Rollback capability | Revert any agent-applied change |
| Guardrails | Configurable limits on what agents can modify |

## 6. Governance & Trust

| Feature | What the Customer Gets |
|---|---|
| Unity Catalog native | All data, models, decisions governed centrally |
| Explainable recommendations | Every suggestion traces to a rule or signal |
| RBAC-aware insights | Users only see what they're entitled to |
| Compliance reporting | Audit logs for SOC 2 / regulatory needs |

## 7. Extensibility

| Feature | What the Customer Gets |
|---|---|
| System-tables-first | Works day 1 with zero pipeline log dependency |
| Plug-in Spark logs (V2) | Deeper insights without redesign |
| Custom skill SDK | Customers add their own agent skills |
| API-first | Integrate with ServiceNow, Slack, PagerDuty, JIRA |

## The IntelliOps Value Pitch (One-Liner Per Persona)

| Persona | What They Get |
|---|---|
| **CDO / CTO** | Predictable Databricks spend + reliable data products |
| **Platform Lead** | Auto-optimized clusters without manual tuning |
| **FinOps** | Real-time cost visibility + chargeback |
| **Data Engineering Lead** | Fewer pipeline failures, faster RCA |
| **Workspace Admin** | Policy-driven governance at scale |

Want me to turn this into a one-pager you can use for internal pitches or customer conversations?