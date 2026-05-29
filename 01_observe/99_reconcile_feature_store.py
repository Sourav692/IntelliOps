# Databricks notebook source
# MAGIC %md
# MAGIC # Observe — Feature Store Reconciliation
# MAGIC
# MAGIC Runs every check defined in `doc/data_reconciliation.md` and prints
# MAGIC PASS / FAIL per item. Designed to be wired into the orchestrator after the
# MAGIC Observe stage completes — if any 🔴 critical check fails, the orchestrator
# MAGIC should stop and alert.
# MAGIC
# MAGIC **Mode widget:**
# MAGIC - `fast` — cheap checks only (bounds, freshness, no-NULL keys, dedup). Safe to run every 15 min.
# MAGIC - `full` — adds reconciliation against `system.*` (USD totals, run counts, spot checks). Run daily.

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

dbutils.widgets.dropdown("mode", "full", ["fast", "full"], "Reconciliation mode")
dbutils.widgets.text("spot_job_id", "", "Optional: spot-check this job_id")
dbutils.widgets.text("spot_cluster_id", "", "Optional: spot-check this cluster_id")
MODE = dbutils.widgets.get("mode")
SPOT_JOB_ID = dbutils.widgets.get("spot_job_id").strip() or None
SPOT_CLUSTER_ID = dbutils.widgets.get("spot_cluster_id").strip() or None

# COMMAND ----------

# Severity constants used in the result tuples below.
CRIT, HIGH, MED = "🔴 critical", "🟠 high", "🟡 medium"

results: list[tuple[str, str, str, str, str]] = []  # (id, severity, status, summary, detail)


def _record(check_id: str, severity: str, status: str, summary: str, detail: str = "") -> None:
    results.append((check_id, severity, status, summary, detail))
    icon = "✔" if status == "PASS" else ("✘" if status == "FAIL" else "—")
    print(f"  [{icon}] {check_id:<5} {severity}  {summary}")
    if detail and status != "PASS":
        print(f"         └─ {detail}")


def _scalar(sql: str):
    row = spark.sql(sql).first()
    return None if row is None else row[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## `feat_cluster_utilization` checks

# COMMAND ----------

print("─" * 70)
print("feat_cluster_utilization")
print("─" * 70)

# C2 · Bounds
bad = spark.sql(f"""
    SELECT
      COUNT_IF(avg_cpu_pct  NOT BETWEEN 0 AND 100)  AS bad_avg_cpu,
      COUNT_IF(peak_cpu_pct NOT BETWEEN 0 AND 100)  AS bad_peak_cpu,
      COUNT_IF(avg_mem_pct  NOT BETWEEN 0 AND 100)  AS bad_avg_mem,
      COUNT_IF(peak_mem_pct NOT BETWEEN 0 AND 100)  AS bad_peak_mem,
      COUNT_IF(node_count < 0)                      AS bad_node_count,
      COUNT_IF(node_type_count > node_count)        AS bad_type_gt_node
    FROM {TABLE_CLUSTER_UTILIZATION}
    WHERE hour_window >= CURRENT_DATE - INTERVAL 7 DAYS
""").first().asDict()
total_bad = sum(bad.values())
_record("C2", HIGH, "PASS" if total_bad == 0 else "FAIL",
        f"value bounds", f"out-of-range counters: {bad}" if total_bad else "")

# C3 · Freshness
mins = _scalar(f"""
    SELECT TIMESTAMPDIFF(MINUTE, MAX(updated_at), CURRENT_TIMESTAMP)
    FROM {TABLE_CLUSTER_UTILIZATION}
""")
_record("C3", MED, "PASS" if mins is not None and mins < 60 else "FAIL",
        f"freshness ({mins} min since refresh)",
        "feature is stale — observe job may have failed" if mins is None or mins >= 60 else "")

# C4 · No NULL keys
nulls = spark.sql(f"""
    SELECT
      COUNT_IF(cluster_id IS NULL)    AS null_cluster,
      COUNT_IF(workspace_id IS NULL)  AS null_ws,
      COUNT_IF(hour_window IS NULL)   AS null_hour
    FROM {TABLE_CLUSTER_UTILIZATION}
""").first().asDict()
_record("C4", HIGH, "PASS" if sum(nulls.values()) == 0 else "FAIL",
        "no NULL keys", f"nulls: {nulls}" if sum(nulls.values()) else "")

if MODE == "full":
    # C1 · Row count sanity
    src = _scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT cluster_id, date_trunc('hour', start_time)
            FROM {SYS_COMPUTE_NODE_TIMELINE}
            WHERE start_time >= CURRENT_DATE - INTERVAL 1 DAY
            GROUP BY cluster_id, date_trunc('hour', start_time)
        )
    """)
    fea = _scalar(f"""
        SELECT COUNT(*) FROM {TABLE_CLUSTER_UTILIZATION}
        WHERE hour_window >= CURRENT_DATE - INTERVAL 1 DAY
    """)
    delta = abs((src or 0) - (fea or 0)) / max(src or 1, 1)
    _record("C1", HIGH, "PASS" if delta < 0.02 else "FAIL",
            f"row count (src={src:,}, fea={fea:,}, delta={delta:.2%})",
            "feature row count diverges from upstream by > 2%" if delta >= 0.02 else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `feat_job_cost_trend` checks

# COMMAND ----------

print("─" * 70)
print("feat_job_cost_trend")
print("─" * 70)

# J2 · No SCD cartesian
worst = _scalar(f"""
    SELECT MAX(c) FROM (
        SELECT workspace_id, job_id, usage_date, COUNT(*) AS c
        FROM {TABLE_JOB_COST_TREND}
        GROUP BY workspace_id, job_id, usage_date
    )
""") or 0
_record("J2", CRIT, "PASS" if worst <= 1 else "FAIL",
        f"no SCD cartesian (worst {worst} dupes)",
        "CRITICAL — list_prices or jobs SCD filter regressed" if worst > 1 else "")

# J3 · Value bounds
negs = spark.sql(f"""
    SELECT
      COUNT_IF(daily_cost_usd < 0)  AS neg_cost,
      COUNT_IF(rolling_14d_avg < 0) AS neg_avg
    FROM {TABLE_JOB_COST_TREND}
""").first().asDict()
_record("J3", HIGH, "PASS" if sum(negs.values()) == 0 else "FAIL",
        "value bounds", f"negative values: {negs}" if sum(negs.values()) else "")

# J4 · Freshness
days_behind = _scalar(f"""
    SELECT DATEDIFF(CURRENT_DATE, MAX(usage_date))
    FROM {TABLE_JOB_COST_TREND}
""")
_record("J4", MED, "PASS" if days_behind is not None and days_behind <= 2 else "FAIL",
        f"freshness ({days_behind} days behind)",
        "billing aggregation is lagging" if days_behind is None or days_behind > 2 else "")

if MODE == "full":
    # J1 · Dollar reconciliation
    row = spark.sql(f"""
        WITH src AS (
          SELECT SUM(u.usage_quantity * p.pricing.default) AS src_usd
          FROM {SYS_BILLING_USAGE} u
          JOIN {SYS_BILLING_PRICES} p
            ON u.cloud = p.cloud
            AND u.sku_name = p.sku_name
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
          WHERE u.usage_metadata.job_id IS NOT NULL
            AND u.usage_date = CURRENT_DATE - 1
        ),
        fea AS (
          SELECT SUM(daily_cost_usd) AS fea_usd
          FROM {TABLE_JOB_COST_TREND}
          WHERE usage_date = CURRENT_DATE - 1
        )
        SELECT src_usd, fea_usd FROM src CROSS JOIN fea
    """).first()
    src_usd = float(row["src_usd"] or 0)
    fea_usd = float(row["fea_usd"] or 0)
    rel = abs(src_usd - fea_usd) / max(src_usd, 1.0)
    _record("J1", CRIT, "PASS" if rel < 0.005 else "FAIL",
            f"USD reconciliation (src=${src_usd:,.2f}, fea=${fea_usd:,.2f}, delta={rel:.2%})",
            "CRITICAL — USD totals diverge from system.billing.* by > 0.5%" if rel >= 0.005 else "")

    # J5 · Growth rate sanity
    extreme = _scalar(f"""
        SELECT COUNT_IF(ABS(cost_growth_pct) > 10)
        FROM {TABLE_JOB_COST_TREND}
        WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
          AND rolling_14d_avg > 0
    """) or 0
    _record("J5", HIGH, "PASS" if extreme == 0 else "FAIL",
            f"growth rate sanity ({extreme} rows with |growth| > 1000%)",
            "investigate jobs with near-zero baselines" if extreme else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `feat_job_health` checks

# COMMAND ----------

print("─" * 70)
print("feat_job_health")
print("─" * 70)

# H1 · One row per job
worst = _scalar(f"""
    SELECT MAX(c) FROM (
        SELECT workspace_id, job_id, COUNT(*) AS c
        FROM {TABLE_JOB_HEALTH}
        GROUP BY workspace_id, job_id
    )
""") or 0
_record("H1", HIGH, "PASS" if worst <= 1 else "FAIL",
        f"one row per job (worst {worst} dupes)",
        "jobs SCD filter regressed" if worst > 1 else "")

# H4 · Bounds
bad = spark.sql(f"""
    SELECT
      COUNT_IF(failure_rate NOT BETWEEN 0 AND 1)        AS bad_fr,
      COUNT_IF(total_runs < failed_runs)                 AS bad_total_lt_failed,
      COUNT_IF(avg_duration_secs < 0)                    AS bad_avg_dur,
      COUNT_IF(max_duration_secs < avg_duration_secs)    AS bad_max_lt_avg,
      COUNT_IF(stddev_duration < 0)                      AS bad_stddev
    FROM {TABLE_JOB_HEALTH}
""").first().asDict()
_record("H4", HIGH, "PASS" if sum(bad.values()) == 0 else "FAIL",
        "value bounds", f"violations: {bad}" if sum(bad.values()) else "")

# H6 · Freshness
mins = _scalar(f"""
    SELECT TIMESTAMPDIFF(MINUTE, MAX(updated_at), CURRENT_TIMESTAMP)
    FROM {TABLE_JOB_HEALTH}
""")
_record("H6", MED, "PASS" if mins is not None and mins < 60 else "FAIL",
        f"freshness ({mins} min since refresh)")

if MODE == "full":
    # H2 · Total runs reconciliation
    row = spark.sql(f"""
        WITH src AS (
          SELECT COUNT(*) AS src_runs FROM (
            SELECT workspace_id, job_id, run_id,
                   MAX(CASE WHEN result_state IS NOT NULL THEN result_state END) AS rs
            FROM {SYS_LAKEFLOW_JOB_RUNS}
            WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
            GROUP BY workspace_id, job_id, run_id
          ) WHERE rs IS NOT NULL
        ),
        fea AS (SELECT SUM(total_runs) AS fea_runs FROM {TABLE_JOB_HEALTH})
        SELECT src_runs, fea_runs FROM src CROSS JOIN fea
    """).first()
    src_runs = int(row["src_runs"] or 0)
    fea_runs = int(row["fea_runs"] or 0)
    rel = abs(src_runs - fea_runs) / max(src_runs, 1)
    _record("H2", CRIT, "PASS" if rel < 0.001 else "FAIL",
            f"total runs reconciliation (src={src_runs:,}, fea={fea_runs:,}, delta={rel:.3%})",
            "CRITICAL — multi-period collapse or SCD filter regressed" if rel >= 0.001 else "")

    # H3 · Failure count reconciliation
    row = spark.sql(f"""
        WITH src AS (
          SELECT COUNT(*) AS src_failed FROM (
            SELECT workspace_id, job_id, run_id,
                   MAX(CASE WHEN result_state IS NOT NULL THEN result_state END) AS rs
            FROM {SYS_LAKEFLOW_JOB_RUNS}
            WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
            GROUP BY workspace_id, job_id, run_id
          ) WHERE rs = 'FAILED'
        ),
        fea AS (SELECT SUM(failed_runs) AS fea_failed FROM {TABLE_JOB_HEALTH})
        SELECT src_failed, fea_failed FROM src CROSS JOIN fea
    """).first()
    src_f = int(row["src_failed"] or 0)
    fea_f = int(row["fea_failed"] or 0)
    delta = abs(src_f - fea_f)
    _record("H3", CRIT, "PASS" if delta <= 1 else "FAIL",
            f"failure count reconciliation (src={src_f:,}, fea={fea_f:,}, delta={delta})",
            "CRITICAL — failure aggregation diverged" if delta > 1 else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `agent_action_log` checks

# COMMAND ----------

print("─" * 70)
print("agent_action_log")
print("─" * 70)

# A1 · Allowed status values
unknown_statuses = spark.sql(f"""
    SELECT DISTINCT status FROM {TABLE_AGENT_ACTIONS}
    WHERE status NOT IN ('proposed', 'approved', 'applied', 'rejected') OR status IS NULL
""").collect()
_record("A1", HIGH, "PASS" if not unknown_statuses else "FAIL",
        "allowed status values",
        f"unexpected: {[r['status'] for r in unknown_statuses]}" if unknown_statuses else "")

# A2 · Bounds
bad = spark.sql(f"""
    SELECT
      COUNT_IF(projected_savings < 0)                  AS neg_savings,
      COUNT_IF(action_timestamp > CURRENT_TIMESTAMP)   AS future_action,
      COUNT_IF(action_id IS NULL OR target_id IS NULL) AS null_keys
    FROM {TABLE_AGENT_ACTIONS}
""").first().asDict()
_record("A2", HIGH, "PASS" if sum(bad.values()) == 0 else "FAIL",
        "value bounds", f"violations: {bad}" if sum(bad.values()) else "")

# A3 · JSON details parses
bad_json = _scalar(f"""
    SELECT COUNT_IF(details IS NOT NULL AND TRY_PARSE_JSON(details) IS NULL)
    FROM {TABLE_AGENT_ACTIONS}
""") or 0
_record("A3", MED, "PASS" if bad_json == 0 else "FAIL",
        f"JSON details parses ({bad_json} rows have unparseable JSON)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-entity spot checks (S1–S3)
# MAGIC
# MAGIC The aggregate checks above catch global drift but can miss per-entity bugs
# MAGIC (e.g. a single job with the SCD cartesian still applied to it). These checks
# MAGIC pick the most-active jobs and clusters and compare each one against `system.*`
# MAGIC the way a human reconciling against the Databricks UI would.
# MAGIC
# MAGIC Runs in `full` mode only, or whenever a specific job_id / cluster_id is
# MAGIC supplied via the widgets at the top.

# COMMAND ----------

# Tolerances for per-entity comparisons.
RUN_COUNT_TOL = 0.02       # ±2% on run counts
DURATION_TOL_SECS = 30     # ±30s on average run duration
CPU_TOL_PCT = 3            # ±3 percentage points on average CPU
NODE_COUNT_TOL = 1         # ±1 node


def _flag(delta_ok: bool) -> str:
    return "✔" if delta_ok else "✘"


run_spot_checks = (MODE == "full") or SPOT_JOB_ID or SPOT_CLUSTER_ID

if run_spot_checks:
    print("─" * 70)
    print("per-entity spot checks")
    print("─" * 70)

# COMMAND ----------

# ── S1 · Job spot check ──────────────────────────────────────────────────────
# Top 5 jobs by month-to-date cost. For each: total_runs, failed_runs, avg
# duration → both from feat_job_health and from system.lakeflow.job_run_timeline
# (collapsed per run_id). Side-by-side with deltas.

if run_spot_checks:
    job_filter = ""
    if SPOT_JOB_ID:
        job_filter = f"AND job_id = '{SPOT_JOB_ID}'"

    spot_df = spark.sql(f"""
        WITH top_jobs AS (
            SELECT job_id, workspace_id, MAX(job_name) AS job_name
            FROM {TABLE_JOB_COST_TREND}
            WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE)
              {job_filter}
            GROUP BY job_id, workspace_id
            ORDER BY SUM(daily_cost_usd) DESC
            LIMIT { '1' if SPOT_JOB_ID else '5' }
        ),
        src AS (
            SELECT
                workspace_id, job_id,
                COUNT(*) AS src_runs,
                SUM(CASE WHEN rs = 'FAILED' THEN 1 ELSE 0 END) AS src_failed,
                ROUND(AVG(duration_secs), 0) AS src_avg_secs
            FROM (
                SELECT
                    workspace_id, job_id, run_id,
                    MIN(period_start_time) AS run_start,
                    MAX(period_end_time)   AS run_end,
                    MAX(CASE WHEN result_state IS NOT NULL THEN result_state END) AS rs
                FROM {SYS_LAKEFLOW_JOB_RUNS}
                WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
                GROUP BY workspace_id, job_id, run_id
            )
            JOIN top_jobs USING (workspace_id, job_id)
            WHERE rs IS NOT NULL AND run_end >= run_start
            CROSS JOIN LATERAL (
                SELECT TIMESTAMPDIFF(SECOND, run_start, run_end) AS duration_secs
            )
            GROUP BY workspace_id, job_id
        ),
        fea AS (
            SELECT workspace_id, job_id, total_runs, failed_runs, avg_duration_secs
            FROM {TABLE_JOB_HEALTH}
        )
        SELECT
            t.job_name,
            t.job_id,
            COALESCE(s.src_runs, 0)                AS src_runs,
            COALESCE(f.total_runs, 0)              AS fea_runs,
            COALESCE(s.src_failed, 0)              AS src_failed,
            COALESCE(f.failed_runs, 0)             AS fea_failed,
            CAST(COALESCE(s.src_avg_secs, 0) AS BIGINT) AS src_avg_secs,
            CAST(COALESCE(f.avg_duration_secs, 0) AS BIGINT) AS fea_avg_secs
        FROM top_jobs t
        LEFT JOIN src s USING (workspace_id, job_id)
        LEFT JOIN fea f USING (workspace_id, job_id)
        ORDER BY src_runs DESC
    """).collect()

    print("\nS1 · Top jobs — feature vs source")
    print(f"  {'job_name':<40} {'src/fea runs':>16}  {'src/fea fail':>14}  {'src/fea avg':>16}  status")
    all_pass = True
    for r in spot_df:
        run_delta_ok = abs(r["src_runs"] - r["fea_runs"]) / max(r["src_runs"], 1) < RUN_COUNT_TOL
        fail_delta_ok = abs(r["src_failed"] - r["fea_failed"]) <= max(1, r["src_failed"] * 0.05)
        dur_delta_ok = abs(r["src_avg_secs"] - r["fea_avg_secs"]) < DURATION_TOL_SECS
        row_ok = run_delta_ok and fail_delta_ok and dur_delta_ok
        all_pass = all_pass and row_ok
        name = (r["job_name"] or r["job_id"])[:40]
        print(
            f"  {name:<40} "
            f"{r['src_runs']:>7,}/{r['fea_runs']:<7,} {_flag(run_delta_ok)}  "
            f"{r['src_failed']:>6,}/{r['fea_failed']:<6,} {_flag(fail_delta_ok)}  "
            f"{r['src_avg_secs']:>6,}s/{r['fea_avg_secs']:<6,}s {_flag(dur_delta_ok)}  "
            f"{'PASS' if row_ok else 'FAIL'}"
        )
    detail = "" if all_pass else "one or more top jobs diverge from system tables — investigate the offenders above"
    _record("S1", CRIT, "PASS" if all_pass else "FAIL",
            f"top-5 job spot check vs system.lakeflow.job_run_timeline",
            detail)

# COMMAND ----------

# ── S2 · Cluster spot check ──────────────────────────────────────────────────
# Top 5 clusters by hour-count in the last 24h. For each: node_count and
# avg_cpu_pct — both from feat_cluster_utilization (latest hour) and from
# system.compute.node_timeline computed live.

if run_spot_checks:
    cluster_filter = ""
    if SPOT_CLUSTER_ID:
        cluster_filter = f"AND cluster_id = '{SPOT_CLUSTER_ID}'"

    spot_clusters = spark.sql(f"""
        WITH top_clusters AS (
            SELECT cluster_id, workspace_id, MAX(hour_window) AS latest_hour
            FROM {TABLE_CLUSTER_UTILIZATION}
            WHERE hour_window >= CURRENT_DATE - INTERVAL 1 DAY
              {cluster_filter}
            GROUP BY cluster_id, workspace_id
            ORDER BY SUM(node_count) DESC
            LIMIT { '1' if SPOT_CLUSTER_ID else '5' }
        ),
        src AS (
            SELECT
                n.cluster_id,
                n.workspace_id,
                date_trunc('hour', n.start_time) AS hour_window,
                COUNT(DISTINCT n.instance_id)    AS src_nodes,
                ROUND(AVG(n.cpu_user_percent), 1) AS src_avg_cpu
            FROM {SYS_COMPUTE_NODE_TIMELINE} n
            JOIN top_clusters t
              ON n.cluster_id = t.cluster_id
             AND n.workspace_id = t.workspace_id
             AND date_trunc('hour', n.start_time) = t.latest_hour
            GROUP BY n.cluster_id, n.workspace_id, date_trunc('hour', n.start_time)
        ),
        fea AS (
            SELECT cluster_id, workspace_id, hour_window,
                   node_count AS fea_nodes,
                   ROUND(avg_cpu_pct, 1) AS fea_avg_cpu
            FROM {TABLE_CLUSTER_UTILIZATION}
        )
        SELECT
            t.cluster_id,
            t.latest_hour,
            COALESCE(s.src_nodes, 0)   AS src_nodes,
            COALESCE(f.fea_nodes, 0)   AS fea_nodes,
            COALESCE(s.src_avg_cpu, 0) AS src_avg_cpu,
            COALESCE(f.fea_avg_cpu, 0) AS fea_avg_cpu
        FROM top_clusters t
        LEFT JOIN src s ON s.cluster_id = t.cluster_id AND s.workspace_id = t.workspace_id
        LEFT JOIN fea f ON f.cluster_id = t.cluster_id AND f.workspace_id = t.workspace_id AND f.hour_window = t.latest_hour
        ORDER BY src_nodes DESC
    """).collect()

    print("\nS2 · Top clusters — feature vs source (latest hour)")
    print(f"  {'cluster_id':<40} {'latest_hour':<20} {'src/fea nodes':>16} {'src/fea cpu':>16}  status")
    all_pass = True
    for r in spot_clusters:
        node_ok = abs(int(r["src_nodes"]) - int(r["fea_nodes"])) <= NODE_COUNT_TOL
        cpu_ok = abs(float(r["src_avg_cpu"]) - float(r["fea_avg_cpu"])) < CPU_TOL_PCT
        row_ok = node_ok and cpu_ok
        all_pass = all_pass and row_ok
        cid = (r["cluster_id"] or "")[:40]
        print(
            f"  {cid:<40} "
            f"{str(r['latest_hour']):<20} "
            f"{r['src_nodes']:>6,}/{r['fea_nodes']:<6,} {_flag(node_ok)}  "
            f"{r['src_avg_cpu']:>6.1f}/{r['fea_avg_cpu']:<6.1f} {_flag(cpu_ok)}  "
            f"{'PASS' if row_ok else 'FAIL'}"
        )
    detail = "" if all_pass else "one or more top clusters diverge from system tables — investigate the offenders above"
    _record("S2", HIGH, "PASS" if all_pass else "FAIL",
            f"top-5 cluster spot check vs system.compute.node_timeline",
            detail)

# COMMAND ----------

# ── S3 · Per-job cost spot check ─────────────────────────────────────────────
# For the top 5 jobs by MTD cost: compare yesterday's feat_job_cost_trend
# daily_cost_usd against a freshly computed sum from system.billing.* with the
# SCD-correct price join.

if run_spot_checks:
    job_filter = ""
    if SPOT_JOB_ID:
        job_filter = f"AND job_id = '{SPOT_JOB_ID}'"

    cost_spot = spark.sql(f"""
        WITH top_jobs AS (
            SELECT job_id, workspace_id, MAX(job_name) AS job_name
            FROM {TABLE_JOB_COST_TREND}
            WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE)
              {job_filter}
            GROUP BY job_id, workspace_id
            ORDER BY SUM(daily_cost_usd) DESC
            LIMIT { '1' if SPOT_JOB_ID else '5' }
        ),
        src AS (
            SELECT
                u.usage_metadata.job_id        AS job_id,
                u.workspace_id,
                ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS src_usd
            FROM {SYS_BILLING_USAGE} u
            JOIN {SYS_BILLING_PRICES} p
              ON u.cloud = p.cloud
             AND u.sku_name = p.sku_name
             AND u.usage_start_time >= p.price_start_time
             AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE u.usage_date = CURRENT_DATE - 1
              AND u.usage_metadata.job_id IN (SELECT job_id FROM top_jobs)
            GROUP BY u.usage_metadata.job_id, u.workspace_id
        ),
        fea AS (
            SELECT job_id, workspace_id, ROUND(daily_cost_usd, 2) AS fea_usd
            FROM {TABLE_JOB_COST_TREND}
            WHERE usage_date = CURRENT_DATE - 1
        )
        SELECT
            t.job_name,
            t.job_id,
            COALESCE(s.src_usd, 0) AS src_usd,
            COALESCE(f.fea_usd, 0) AS fea_usd
        FROM top_jobs t
        LEFT JOIN src s USING (job_id, workspace_id)
        LEFT JOIN fea f USING (job_id, workspace_id)
        ORDER BY src_usd DESC
    """).collect()

    print("\nS3 · Top jobs — yesterday's cost feature vs source")
    print(f"  {'job_name':<40} {'src_usd':>14} {'fea_usd':>14}  {'rel delta':>10}  status")
    all_pass = True
    for r in cost_spot:
        src_v = float(r["src_usd"]) or 0.0
        fea_v = float(r["fea_usd"]) or 0.0
        rel = abs(src_v - fea_v) / max(src_v, 1.0)
        row_ok = rel < 0.01    # 1% per-job tolerance
        all_pass = all_pass and row_ok
        name = (r["job_name"] or r["job_id"])[:40]
        print(
            f"  {name:<40} "
            f"${src_v:>12,.2f}  ${fea_v:>12,.2f}  {rel:>9.2%}  "
            f"{_flag(row_ok)} {'PASS' if row_ok else 'FAIL'}"
        )
    detail = "" if all_pass else "one or more top jobs' cost values diverge by > 1% — list_prices SCD or job filter likely regressed for that job"
    _record("S3", CRIT, "PASS" if all_pass else "FAIL",
            "top-5 job cost spot check vs system.billing.*",
            detail)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
total = len(results)
passed = sum(1 for r in results if r[2] == "PASS")
failed = total - passed
crit_failed = sum(1 for r in results if r[2] == "FAIL" and r[1] == CRIT)

print(f" Reconciliation summary — mode={MODE}")
print(f" {passed}/{total} passed, {failed} failed ({crit_failed} critical)")
print("=" * 70)

if failed > 0:
    print("\nFAILED checks (in order):")
    for cid, sev, status, summary, detail in results:
        if status != "PASS":
            print(f"  [{cid}] {sev}  {summary}")
            if detail:
                print(f"         {detail}")

if crit_failed > 0:
    raise RuntimeError(
        f"{crit_failed} critical reconciliation check(s) failed — "
        f"do not trust intelliops.* downstream until resolved. "
        f"See doc/data_reconciliation.md."
    )
