# Databricks notebook source
# MAGIC %md
# MAGIC # Knowledge — Seed Corpus
# MAGIC
# MAGIC Populates `intelliops.knowledge.knowledge_docs` with a starter corpus of
# MAGIC Databricks cost-observability snippets. Extend this table later by appending
# MAGIC additional rows (e.g. internal runbooks, post-incident notes).
# MAGIC
# MAGIC The Vector Search index in `01_build_knowledge_index` reads from this table.

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

from datetime import datetime, timezone

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {KNOWLEDGE_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_KNOWLEDGE_DOCS} (
    doc_id      STRING      COMMENT 'Stable identifier for the snippet',
    title       STRING      COMMENT 'Short human-readable title',
    content     STRING      COMMENT 'The snippet body — embedded by the VS index',
    source      STRING      COMMENT 'Origin label (pricing-doc, runbook, best-practice)',
    tags        STRING      COMMENT 'Comma-separated tags',
    updated_at  TIMESTAMP   COMMENT 'Last updated time'
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
# Change Data Feed is required for a Delta Sync vector index.

# COMMAND ----------

SEED = [
    (
        "dbu-basics",
        "DBUs and how they convert to dollars",
        "A Databricks Unit (DBU) is a normalized unit of processing capability "
        "consumed per second. `system.billing.usage` reports DBUs by SKU; "
        "`system.billing.list_prices` maps each SKU to a USD price. The IntelliOps "
        "`feat_job_cost_trend` feature already performs this join so downstream "
        "tools do not need to recompute it.",
        "best-practice",
        "billing,dbu,pricing",
    ),
    (
        "job-vs-allpurpose",
        "Jobs compute vs all-purpose compute pricing",
        "All-purpose (interactive) compute is roughly 2–3× more expensive per DBU "
        "than jobs compute for the same workload. Scheduled production jobs should "
        "run on a Jobs cluster (created per run) or on a Job-compatible cluster, "
        "not on a shared all-purpose cluster. The wasteful-compute detector flags "
        "jobs running on all-purpose clusters as a high-value optimization.",
        "best-practice",
        "compute,jobs,cost-optimization",
    ),
    (
        "photon-enablement",
        "When Photon pays off",
        "Photon typically delivers 2–4× faster execution on SQL and DataFrame "
        "workloads heavy on aggregations, joins, and string processing. Photon "
        "adds a per-DBU multiplier (commonly ~2×), so it only saves money when the "
        "wall-clock speedup exceeds the DBU multiplier. Enable for ETL/SQL heavy "
        "workloads; skip for very small or UDF-heavy jobs.",
        "best-practice",
        "photon,performance,cost-optimization",
    ),
    (
        "autoscale-min-workers",
        "Autoscaling: setting min_workers correctly",
        "A high min_workers value defeats the purpose of autoscaling — the cluster "
        "always pays for at least that many nodes even when idle. For bursty "
        "workloads set min_workers as low as 1 (or 2 for HA). The cluster "
        "right-sizing tool proposes the lowest min_workers consistent with "
        "observed peak utilization over the last 7 days.",
        "runbook",
        "autoscale,cluster,right-sizing",
    ),
    (
        "idle-auto-termination",
        "Auto-termination prevents idle waste",
        "Always set `autotermination_minutes` on all-purpose clusters (recommended "
        "10–30 minutes). Without it, an idle cluster bills indefinitely. The "
        "wasteful-compute detector flags clusters with no tasks for 2+ hours; "
        "fixing auto-termination is usually the highest-ROI single change.",
        "runbook",
        "idle,auto-termination,waste",
    ),
    (
        "spot-instances",
        "Spot instances for fault-tolerant jobs",
        "Spot (preemptible) instances cost 60–90% less than on-demand for the same "
        "node type. Use them for jobs that tolerate restarts: ETL re-runs, ad-hoc "
        "analysis, training jobs with checkpointing. Avoid for low-latency "
        "interactive workloads and for the Spark driver node.",
        "best-practice",
        "spot,nodes,cost-optimization",
    ),
    (
        "cluster-pools",
        "Cluster pools reduce startup latency, not cost",
        "Pools pre-warm idle VMs so jobs start faster, but those idle VMs still "
        "incur cloud-provider cost (no DBU charge). Pools save money only if "
        "they replace many separate cluster startups; otherwise they add a "
        "background idle cost. Audit pool utilization in `cluster_idle_summary`.",
        "best-practice",
        "pools,latency,cost-optimization",
    ),
    (
        "right-sizing-rule",
        "IntelliOps right-sizing rule",
        "A cluster is flagged as over-provisioned when avg CPU < 30% AND avg "
        "memory < 40% for 5+ consecutive hours over a 7-day window. The proposed "
        "worker count is the smallest count whose theoretical peak capacity still "
        "covers the observed P95 utilization with a 25% headroom.",
        "runbook",
        "right-sizing,cluster,utilization",
    ),
    (
        "cost-spike-causes",
        "Common causes of a 25%+ cost spike",
        "1) Cluster config change — node type or worker count increased. "
        "2) Data volume increased — input table size doubled. "
        "3) Photon turned off, removing acceleration. "
        "4) Job moved from jobs compute to all-purpose. "
        "5) Schedule increased frequency (e.g., from daily to hourly). "
        "The cost-spike alert tool checks (1)–(4) automatically; (5) requires "
        "looking at run_count from job_run_timeline.",
        "runbook",
        "cost-spike,root-cause,billing",
    ),
    (
        "budget-forecast-method",
        "How the budget forecast works",
        "The budget tool fits a simple linear projection to month-to-date daily "
        "spend, multiplies by remaining days, and adds to MTD actual. When the "
        "projection exceeds 90% of `WORKSPACE_BUDGETS[<workspace_id>]`, an alert "
        "fires. This is intentionally simple — for seasonal workloads consider "
        "using `cost_current_month_trajectory.rolling_7d_avg` as the base rate.",
        "runbook",
        "budget,forecast,alerts",
    ),
    (
        "approval-gate",
        "Why cluster edits are gated",
        "When `REQUIRE_APPROVAL_FOR_CLUSTER_EDIT=True` (default), the agent will "
        "never mutate a production cluster. It returns the proposed new "
        "configuration and a diff; a human must approve via the interface "
        "before any REST API write happens. This is the only safety rail "
        "between the agent and production compute.",
        "runbook",
        "safety,approval,cluster",
    ),
    (
        "tagging-cost-allocation",
        "Tag-based cost allocation",
        "Apply consistent tags (team, project, env) to clusters and jobs. "
        "`system.billing.usage` joins on `usage_metadata.cluster_id` and you can "
        "group by tag to produce per-team chargeback. Without tags, only "
        "workspace-level granularity is available.",
        "best-practice",
        "tagging,chargeback,allocation",
    ),
]

now = datetime.now(timezone.utc)
df = spark.createDataFrame(
    [(d[0], d[1], d[2], d[3], d[4], now) for d in SEED],
    "doc_id string, title string, content string, source string, tags string, updated_at timestamp",
)

# Idempotent insert — replace any existing rows with the same doc_id
df.createOrReplaceTempView("_seed_docs")
spark.sql(f"""
MERGE INTO {TABLE_KNOWLEDGE_DOCS} t
USING _seed_docs s
ON t.doc_id = s.doc_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE_KNOWLEDGE_DOCS}").first()["n"]
print(f"✔ {TABLE_KNOWLEDGE_DOCS} seeded — {count} rows total.")
