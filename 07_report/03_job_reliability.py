# Databricks notebook source
# MAGIC %md
# MAGIC # Report — Job Reliability
# MAGIC
# MAGIC **Audience:** Data Engineering
# MAGIC
# MAGIC Publishes stable SQL views into `intelliops.report.*` for the job-reliability
# MAGIC dashboard tab.

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {REPORT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `job_reliability_overall` — platform-wide reliability score

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.job_reliability_overall AS
SELECT
    COUNT(*)                                            AS total_jobs,
    SUM(total_runs)                                     AS total_runs,
    SUM(failed_runs)                                    AS total_failures,
    ROUND((1 - SUM(failed_runs) * 1.0
        / NULLIF(SUM(total_runs), 0)) * 100, 2)        AS overall_success_rate_pct,
    ROUND(AVG(failure_rate) * 100, 2)                   AS avg_failure_rate_pct
FROM {TABLE_JOB_HEALTH}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `job_daily_failure_trend` — daily failure rate, last 30 days

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.job_daily_failure_trend AS
WITH per_run AS (
    -- One row per run, dated by when the run started.
    SELECT
        workspace_id, job_id, run_id,
        DATE(MIN(period_start_time))                                  AS run_date,
        MAX(CASE WHEN result_state IS NOT NULL THEN result_state END) AS result_state
    FROM {SYS_LAKEFLOW_JOB_RUNS}
    WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY workspace_id, job_id, run_id
)
SELECT
    run_date,
    COUNT(*)                                                AS total_runs,
    SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failures,
    ROUND(SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 2)                                      AS failure_rate_pct
FROM per_run
WHERE result_state IS NOT NULL    -- Exclude still-in-flight runs
GROUP BY run_date
ORDER BY run_date
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `job_most_unreliable` — top 15 jobs by failure rate

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.job_most_unreliable AS
SELECT
    job_name,
    workspace_id,
    total_runs,
    failed_runs,
    ROUND(failure_rate * 100, 1)        AS failure_rate_pct,
    ROUND(avg_duration_secs / 60, 1)     AS avg_duration_min,
    ROUND(max_duration_secs / 60, 1)     AS max_duration_min
FROM {TABLE_JOB_HEALTH}
WHERE total_runs >= 5
ORDER BY failure_rate DESC
LIMIT 15
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `job_sla_breaches` — jobs exceeding the configured SLA duration

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.job_sla_breaches AS
SELECT
    job_name,
    workspace_id,
    total_runs,
    ROUND(avg_duration_secs / 60, 1)        AS avg_duration_min,
    ROUND(max_duration_secs / 60, 1)         AS max_duration_min,
    ROUND(
        CASE WHEN avg_duration_secs > {SLA_DURATION_MINUTES * 60}
             THEN 1 ELSE 0 END * total_runs, 0
    )                                         AS est_sla_breaches
FROM {TABLE_JOB_HEALTH}
WHERE avg_duration_secs > {SLA_DURATION_MINUTES * 60}
ORDER BY avg_duration_secs DESC
LIMIT 15
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `job_duration_anomalies` — latest runs exceeding mean + Nσ

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.job_duration_anomalies AS
WITH per_run AS (
    -- Collapse multi-period timeline rows to one row per actual run.
    SELECT
        workspace_id, job_id, run_id,
        MIN(period_start_time)                                        AS run_start,
        MAX(period_end_time)                                          AS run_end,
        MAX(CASE WHEN result_state IS NOT NULL THEN result_state END) AS result_state
    FROM {SYS_LAKEFLOW_JOB_RUNS}
    WHERE period_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY workspace_id, job_id, run_id
),
latest_runs AS (
    SELECT
        workspace_id, job_id,
        TIMESTAMPDIFF(SECOND, run_start, run_end) AS duration_secs,
        run_start,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, job_id ORDER BY run_start DESC
        ) AS rn
    FROM per_run
    WHERE result_state IS NOT NULL
      AND run_end IS NOT NULL
      AND run_end >= run_start
)
SELECT
    h.job_name,
    h.workspace_id,
    ROUND(lr.duration_secs / 60, 1)                    AS latest_duration_min,
    ROUND(h.avg_duration_secs / 60, 1)                  AS avg_duration_min,
    ROUND((h.avg_duration_secs + {JOB_DURATION_ANOMALY_SIGMA}
        * h.stddev_duration) / 60, 1)                   AS threshold_min,
    ROUND((lr.duration_secs - h.avg_duration_secs)
        / NULLIF(h.stddev_duration, 0), 1)              AS z_score
FROM {TABLE_JOB_HEALTH} h
JOIN latest_runs lr
    ON h.workspace_id = lr.workspace_id AND h.job_id = lr.job_id AND lr.rn = 1
WHERE lr.duration_secs > h.avg_duration_secs + {JOB_DURATION_ANOMALY_SIGMA} * h.stddev_duration
  AND h.stddev_duration > 0
ORDER BY z_score DESC
LIMIT 15
""")

# COMMAND ----------

print(f"Job Reliability views published under {REPORT_SCHEMA}.")
