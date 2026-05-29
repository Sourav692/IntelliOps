# Data Reconciliation Plan — `intelliops.feature_store`

> The four tables in `intelliops.feature_store` are the source of truth for every view in `intelliops.report.*` and for the agent's `query_features` tool. If they drift, every downstream artifact lies in the same direction. This document is the contract for "the feature store is correct."

## How to use this document

1. **After every change** to an observe notebook, run `01_observe/99_reconcile_feature_store.py`. Each check below is encoded there as a SQL assertion with a tolerance.
2. **Daily as a scheduled job**, run the same notebook. Persist results to `intelliops.feature_store.reconciliation_log` and alert on any FAIL.
3. **Before debugging any "the numbers look wrong"** report, run the relevant section here first — most of the time the bug is upstream, not in the dashboard.

Severity legend (used in the notebook output):

- 🔴 **Critical** — silently corrupts USD totals or run counts. Treat as a P1.
- 🟠 **High** — produces misleading metrics but the magnitude is bounded.
- 🟡 **Medium** — schema / freshness issue; numbers might still be usable.

---

## 1. `feat_cluster_utilization` checks

Upstream: `system.compute.node_timeline` (per-node per-minute).

### C1 · Row count sanity 🟠

For the last 24 hours, the feature row count should equal the count of distinct `(cluster_id, hour_window)` in the upstream:

```sql
WITH src AS (
  SELECT cluster_id, date_trunc('hour', start_time) AS hour_window
  FROM system.compute.node_timeline
  WHERE start_time >= CURRENT_DATE - INTERVAL 1 DAY
  GROUP BY cluster_id, date_trunc('hour', start_time)
),
fea AS (
  SELECT cluster_id, hour_window
  FROM intelliops.feature_store.feat_cluster_utilization
  WHERE hour_window >= CURRENT_DATE - INTERVAL 1 DAY
)
SELECT (SELECT COUNT(*) FROM src) AS src_rows,
       (SELECT COUNT(*) FROM fea) AS fea_rows;
```

PASS when `abs(src_rows - fea_rows) / src_rows < 0.02`.

### C2 · Value bounds 🟠

```sql
SELECT
  COUNT_IF(avg_cpu_pct  NOT BETWEEN 0 AND 100)  AS bad_avg_cpu,
  COUNT_IF(peak_cpu_pct NOT BETWEEN 0 AND 100)  AS bad_peak_cpu,
  COUNT_IF(avg_mem_pct  NOT BETWEEN 0 AND 100)  AS bad_avg_mem,
  COUNT_IF(peak_mem_pct NOT BETWEEN 0 AND 100)  AS bad_peak_mem,
  COUNT_IF(node_count < 0)                      AS bad_node_count,
  COUNT_IF(node_type_count > node_count)        AS bad_type_gt_node
FROM intelliops.feature_store.feat_cluster_utilization
WHERE hour_window >= CURRENT_DATE - INTERVAL 7 DAYS;
```

All counters must be `0`. `node_type_count > node_count` would indicate the columns are swapped.

### C3 · Freshness 🟡

```sql
SELECT
  MAX(hour_window)  AS latest_window,
  MAX(updated_at)   AS latest_refresh,
  TIMESTAMPDIFF(MINUTE, MAX(updated_at), CURRENT_TIMESTAMP) AS minutes_since_refresh
FROM intelliops.feature_store.feat_cluster_utilization;
```

PASS when `minutes_since_refresh < 60` (assuming a 15-min refresh; raise threshold for different cadences).

### C4 · No NULL keys 🟠

```sql
SELECT
  COUNT_IF(cluster_id IS NULL)    AS null_cluster_id,
  COUNT_IF(workspace_id IS NULL)  AS null_workspace_id,
  COUNT_IF(hour_window IS NULL)   AS null_hour_window
FROM intelliops.feature_store.feat_cluster_utilization;
```

All must be `0`.

### C5 · Per-cluster spot check 🟠

Pick the largest cluster by recent activity. Recompute its 24-hour averages straight from `node_timeline` and compare to the feature row.

```sql
WITH tgt AS (
  SELECT cluster_id FROM intelliops.feature_store.feat_cluster_utilization
  WHERE hour_window >= CURRENT_DATE - INTERVAL 1 DAY
  GROUP BY cluster_id ORDER BY SUM(node_count) DESC LIMIT 1
),
src AS (
  SELECT
    n.cluster_id,
    AVG(n.cpu_user_percent) AS src_avg_cpu,
    AVG(n.mem_used_percent) AS src_avg_mem,
    COUNT(DISTINCT n.instance_id) AS src_node_count
  FROM system.compute.node_timeline n JOIN tgt USING (cluster_id)
  WHERE n.start_time >= CURRENT_DATE - INTERVAL 1 DAY
  GROUP BY n.cluster_id
),
fea AS (
  SELECT
    cluster_id,
    AVG(avg_cpu_pct) AS fea_avg_cpu,
    AVG(avg_mem_pct) AS fea_avg_mem,
    MAX(node_count)  AS fea_node_count
  FROM intelliops.feature_store.feat_cluster_utilization
  WHERE hour_window >= CURRENT_DATE - INTERVAL 1 DAY
  GROUP BY cluster_id
)
SELECT * FROM src JOIN fea USING (cluster_id);
```

PASS when `|src_avg_cpu - fea_avg_cpu| < 2` and `|src_avg_mem - fea_avg_mem| < 2`. The feature is hourly-averaged before this query averages it again, so a small jitter is normal.

---

## 2. `feat_job_cost_trend` checks

Upstream: `system.billing.usage` + `system.billing.list_prices` + `system.lakeflow.jobs`.

### J1 · Dollar reconciliation 🔴

The single most important check. For the last full day, total feature USD must equal total source USD computed with the same SCD-correct joins:

```sql
WITH src AS (
  SELECT SUM(u.usage_quantity * p.pricing.default) AS src_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
  WHERE u.usage_metadata.job_id IS NOT NULL
    AND u.usage_date = CURRENT_DATE - 1
),
fea AS (
  SELECT SUM(daily_cost_usd) AS fea_usd
  FROM intelliops.feature_store.feat_job_cost_trend
  WHERE usage_date = CURRENT_DATE - 1
)
SELECT src_usd, fea_usd,
       ABS(src_usd - fea_usd) / NULLIF(src_usd, 0) AS rel_delta
FROM src CROSS JOIN fea;
```

PASS when `rel_delta < 0.005` (0.5%). Larger means either the SCD price join is back, or the feature is filtering job-attributable usage differently than source.

### J2 · No SCD cartesian 🔴

For each `(workspace_id, job_id, usage_date)`, exactly one row must exist:

```sql
SELECT MAX(c) AS worst_day_dupes
FROM (
  SELECT workspace_id, job_id, usage_date, COUNT(*) AS c
  FROM intelliops.feature_store.feat_job_cost_trend
  GROUP BY workspace_id, job_id, usage_date
);
```

PASS when `worst_day_dupes = 1`. Anything ≥ 2 means the upstream `jobs` SCD filter or the `list_prices` time-range filter regressed.

### J3 · Value bounds 🟠

```sql
SELECT
  COUNT_IF(daily_cost_usd < 0) AS negative_cost,
  COUNT_IF(rolling_14d_avg < 0) AS negative_avg
FROM intelliops.feature_store.feat_job_cost_trend;
```

Both must be `0`.

### J4 · Freshness 🟡

```sql
SELECT
  MAX(usage_date) AS latest_date,
  DATEDIFF(CURRENT_DATE, MAX(usage_date)) AS days_behind
FROM intelliops.feature_store.feat_job_cost_trend;
```

PASS when `days_behind <= 2`. Billing publishes hourly with ~1 hour lag, so yesterday should always be present.

### J5 · Growth rate sanity 🟠

For jobs with a stable schedule and similar daily cost, `cost_growth_pct` should be small. Catch wildly wrong growth values:

```sql
SELECT COUNT_IF(ABS(cost_growth_pct) > 10) AS extreme_growth_rows
FROM intelliops.feature_store.feat_job_cost_trend
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND rolling_14d_avg > 0;
```

PASS when `extreme_growth_rows = 0`. (A 1000% growth value almost always means the trailing window had a single near-zero day; the data isn't wrong but worth investigating.)

---

## 3. `feat_job_health` checks

Upstream: `system.lakeflow.job_run_timeline` + `system.lakeflow.jobs`.

### H1 · One row per job 🟠

```sql
SELECT MAX(c) AS worst_dupes
FROM (
  SELECT workspace_id, job_id, COUNT(*) AS c
  FROM intelliops.feature_store.feat_job_health
  GROUP BY workspace_id, job_id
);
```

Must be `1`. > 1 means the SCD filter on `system.lakeflow.jobs` regressed.

### H2 · Total runs reconciliation 🔴

`SUM(total_runs)` must equal the count of distinct, completed runs in the upstream timeline (collapsed by `run_id`):

```sql
WITH src AS (
  SELECT COUNT(*) AS src_runs
  FROM (
    SELECT workspace_id, job_id, run_id,
           MAX(CASE WHEN result_state IS NOT NULL THEN result_state END) AS rs
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY workspace_id, job_id, run_id
  )
  WHERE rs IS NOT NULL
),
fea AS (
  SELECT SUM(total_runs) AS fea_runs FROM intelliops.feature_store.feat_job_health
)
SELECT src_runs, fea_runs, ABS(src_runs - fea_runs) AS abs_delta
FROM src CROSS JOIN fea;
```

PASS when `abs_delta = 0`. Off-by-one is fine if a run completed between the two queries; anything > a few hundred is a regression.

### H3 · Failure count reconciliation 🔴

Same shape, filtered to `result_state = 'FAILED'`:

```sql
WITH src AS (
  SELECT COUNT(*) AS src_failed
  FROM (
    SELECT workspace_id, job_id, run_id,
           MAX(CASE WHEN result_state IS NOT NULL THEN result_state END) AS rs
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY workspace_id, job_id, run_id
  )
  WHERE rs = 'FAILED'
),
fea AS (
  SELECT SUM(failed_runs) AS fea_failed FROM intelliops.feature_store.feat_job_health
)
SELECT src_failed, fea_failed FROM src CROSS JOIN fea;
```

PASS when they're equal (or off-by-one).

### H4 · Bounds 🟠

```sql
SELECT
  COUNT_IF(failure_rate NOT BETWEEN 0 AND 1)        AS bad_failure_rate,
  COUNT_IF(total_runs < failed_runs)                 AS bad_total_lt_failed,
  COUNT_IF(avg_duration_secs < 0)                    AS bad_avg_duration,
  COUNT_IF(max_duration_secs < avg_duration_secs)    AS bad_max_lt_avg,
  COUNT_IF(stddev_duration < 0)                      AS bad_stddev
FROM intelliops.feature_store.feat_job_health;
```

All must be `0`.

### H5 · Duration plausibility 🟠

Catch the streaming-job pollution that hit us before — most batch jobs finish in under a day:

```sql
SELECT job_name, ROUND(avg_duration_secs / 3600, 1) AS avg_hours
FROM intelliops.feature_store.feat_job_health
WHERE avg_duration_secs > 86400          -- > 24 hours
ORDER BY avg_duration_secs DESC
LIMIT 20;
```

Inspect the result. Streaming / continuous jobs legitimately have multi-day "durations" if not filtered; everything else here is suspicious.

### H6 · Freshness 🟡

```sql
SELECT TIMESTAMPDIFF(MINUTE, MAX(updated_at), CURRENT_TIMESTAMP) AS minutes_since_refresh
FROM intelliops.feature_store.feat_job_health;
```

PASS when `< 60`.

---

## 4. `agent_action_log` checks

Internally written by the agent and the rule-based skills. No upstream — schema/integrity checks only.

### A1 · Allowed status values 🟠

```sql
SELECT DISTINCT status FROM intelliops.feature_store.agent_action_log;
```

Must be a subset of `{proposed, approved, applied, rejected}`.

### A2 · Bounds 🟠

```sql
SELECT
  COUNT_IF(projected_savings < 0)                       AS negative_savings,
  COUNT_IF(action_timestamp > CURRENT_TIMESTAMP)        AS future_action,
  COUNT_IF(action_id IS NULL OR target_id IS NULL)      AS null_keys
FROM intelliops.feature_store.agent_action_log;
```

All must be `0`.

### A3 · JSON `details` parses 🟡

```sql
SELECT COUNT_IF(details IS NOT NULL AND TRY_PARSE_JSON(details) IS NULL) AS bad_json
FROM intelliops.feature_store.agent_action_log;
```

Must be `0`.

---

## 5. Per-entity spot checks (S1–S3) — the UI-equivalent reconciliation

Aggregate checks (J1, H2, H3) catch *global* drift but can miss per-job or per-cluster bugs. Example: the EBC-agent-sales_pulse incident showed inflated counts for **one specific job** while total counts across the whole platform looked plausible. The aggregate USD check would have passed; the per-job comparison wouldn't.

These spot checks pick the **top N most-active** jobs and clusters and compare each one against `system.*` side-by-side — the exact comparison a human would do staring at the Databricks Jobs UI:

> *"The UI says EBC agent — sales_pulse ran 28,800 times this month with a 90s average. My feature says 227,055 runs with 854 min average. Where's the bug?"*

### S1 · Top-5 job spot check 🔴

For the top 5 jobs by month-to-date cost, compare every health metric against the upstream:

| Metric | Source (live) | Feature | Tolerance |
|---|---|---|---|
| `total_runs` | `COUNT(*)` of distinct `run_id` in `job_run_timeline` (terminal state) | `feat_job_health.total_runs` | ±2% |
| `failed_runs` | Same, filtered to `result_state = 'FAILED'` | `feat_job_health.failed_runs` | ±5% or ±1 |
| `avg_duration_secs` | `AVG(run_end - run_start)` per `run_id` | `feat_job_health.avg_duration_secs` | ±30 s |

Any divergence on a specific job here means the SCD or multi-period bug came back for that job — the aggregate may still look right because most jobs are fine.

### S2 · Top-5 cluster spot check 🟠

For the top 5 clusters by node-hours in the last 24h, compare the latest hour:

| Metric | Source (live) | Feature | Tolerance |
|---|---|---|---|
| `node_count` | `COUNT(DISTINCT instance_id)` in `node_timeline` for the same hour | `feat_cluster_utilization.node_count` | ±1 node |
| `avg_cpu_pct` | `AVG(cpu_user_percent)` in `node_timeline` for the same hour | `feat_cluster_utilization.avg_cpu_pct` | ±3 pp |

### S3 · Top-5 per-job cost spot check 🔴

For the top 5 jobs by MTD cost, compare yesterday's `daily_cost_usd` against a freshly computed sum from `system.billing.*` (with the SCD-correct time-windowed price join):

| Metric | Source (live) | Feature | Tolerance |
|---|---|---|---|
| `daily_cost_usd` (yesterday) | `SUM(usage_quantity * pricing.default)` with time-window price join | `feat_job_cost_trend.daily_cost_usd` | ±1% relative |

This is the single most useful check when validating "the dashboard says job X cost $Y yesterday — is that right?" — exactly the workflow that surfaced the SCD cartesian bug in the first place.

### Drill-down on a single entity

The notebook accepts two optional widget values for ad-hoc validation:

- `spot_job_id` — when set, S1 and S3 run only against that job_id.
- `spot_cluster_id` — when set, S2 runs only against that cluster_id.

Use these whenever a stakeholder reports "the dashboard shows X for job/cluster Z, but the UI shows Y — which is right?":

```text
Open 01_observe/99_reconcile_feature_store
Set spot_job_id = <the suspect job_id>
Run All
→ side-by-side table shows source vs feature with PASS/FAIL per metric
```

The side-by-side output names the offending metric so the bug is localized in seconds rather than minutes of digging.

---

## 6. Cross-table checks (require all four to be in sync)

### X1 · Top cost driver is alive 🟠

The top job in `cost_top_drivers_mtd` should also have a row in `feat_job_health` (i.e., it has actually run, not a stale entry):

```sql
WITH top_job AS (
  SELECT job_name, workspace_id
  FROM intelliops.report.cost_top_drivers_mtd LIMIT 1
)
SELECT t.job_name,
       (SELECT total_runs FROM intelliops.feature_store.feat_job_health h
        WHERE h.job_name = t.job_name AND h.workspace_id = t.workspace_id) AS runs_30d
FROM top_job t;
```

PASS when `runs_30d > 0`.

### X2 · Right-sizing candidates show real utilization 🟡

For every cluster in `cluster_over_provisioned`, the underlying utilization must be < threshold:

```sql
SELECT op.cluster_id, op.avg_cpu_7d, op.avg_mem_7d
FROM intelliops.report.cluster_over_provisioned op
WHERE op.avg_cpu_7d >= 30 OR op.avg_mem_7d >= 40;
```

PASS when zero rows are returned.

---

## 6. Cadence

| Check group | Recommended frequency | Reason |
|---|---|---|
| C2, C3, C4, J2, J3, J4, H1, H4, H6, A* | Every Observe run (15 min) | Cheap, fast; catch schema/freshness drift immediately. |
| C1, C5, J1, J5, H2, H3, H5, **S1, S2, S3**, X1, X2 | Daily | Some require full-table scans of `system.*`; running every 15 min is wasteful. |
| `spot_job_id` / `spot_cluster_id` drill-down | On-demand | Whenever a stakeholder reports a UI/dashboard mismatch for a specific resource. |

The notebook `01_observe/99_reconcile_feature_store.py` supports both modes via a `mode` widget (`fast` or `full`). Daily, wire the `full` mode into the orchestrator after Observe completes.

## 7. Failure response

When a check fails:

1. **Don't trust dashboards or the agent until it's resolved.** Both read from the same tables.
2. **Re-read the relevant section of [`ARCHITECTURE.md`](./ARCHITECTURE.md) §7 (rules)** — every check here exists because we got bitten by a real bug; the fix usually means applying the SCD or multi-period collapse pattern that was missed.
3. **Re-run the affected observe notebook** with `overwriteSchema=true` after the fix lands — feature tables are rebuilt in place so a corrected logic immediately replaces the bad data.
4. **Record the incident** as a row in a `reconciliation_incidents` log (future work — currently a TODO).
