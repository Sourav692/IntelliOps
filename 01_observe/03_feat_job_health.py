# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1: Observe — Job Health Features
# MAGIC
# MAGIC Computes job reliability metrics from run history.
# MAGIC
# MAGIC **Source:** `system.lakeflow.job_run_timeline` — Run history with start/end times and status
# MAGIC **Target:** `intelliops.feature_store.feat_job_health`

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Job Health Features
# MAGIC Failure rates, duration statistics, and anomaly indicators per job (30-day window).

# COMMAND ----------

df_job_health = spark.sql(f"""
    WITH latest_jobs AS (
        -- One row per (workspace_id, job_id): the most recent job definition.
        -- system.lakeflow.jobs is an SCD-2 table; joining without this filter
        -- multiplies every run by the number of historical job versions.
        SELECT workspace_id, job_id, name AS job_name
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY workspace_id, job_id ORDER BY change_time DESC
            ) AS _rn
            FROM {SYS_LAKEFLOW_JOBS}
        )
        WHERE _rn = 1
    ),
    per_run AS (
        -- Collapse multi-period rows (streaming / state-transition timelines)
        -- into one row per run_id with a single start/end and final state.
        SELECT
            workspace_id,
            job_id,
            run_id,
            MIN(period_start_time)                                          AS run_start,
            MAX(period_end_time)                                            AS run_end,
            MAX(CASE WHEN result_state IS NOT NULL THEN result_state END)   AS result_state
        FROM {SYS_LAKEFLOW_JOB_RUNS}
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {JOB_HEALTH_LOOKBACK_DAYS} DAYS
        GROUP BY workspace_id, job_id, run_id
    ),
    completed_runs AS (
        -- Only runs that actually finished — drop in-flight / queued-only entries
        -- that would otherwise inflate avg/max duration.
        SELECT
            workspace_id, job_id, run_id, result_state,
            TIMESTAMPDIFF(SECOND, run_start, run_end) AS duration_secs
        FROM per_run
        WHERE result_state IS NOT NULL
          AND run_end IS NOT NULL
          AND run_end >= run_start
    ),
    run_stats AS (
        SELECT
            cr.workspace_id,
            cr.job_id,
            j.job_name,
            COUNT(*)                                                    AS total_runs,
            SUM(CASE WHEN cr.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
            AVG(cr.duration_secs)                                       AS avg_duration_secs,
            STDDEV(cr.duration_secs)                                    AS stddev_duration,
            MAX(cr.duration_secs)                                       AS max_duration_secs
        FROM completed_runs cr
        LEFT JOIN latest_jobs j
            ON cr.workspace_id = j.workspace_id AND cr.job_id = j.job_id
        GROUP BY cr.workspace_id, cr.job_id, j.job_name
    )
    SELECT
        *,
        ROUND(failed_runs * 1.0 / NULLIF(total_runs, 0), 4) AS failure_rate
    FROM run_stats
""")

df_job_health = df_job_health.withColumn("updated_at", F.current_timestamp())

print(f"Jobs tracked: {df_job_health.count()}")
df_job_health.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Feature Store

# COMMAND ----------

(
    df_job_health.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_JOB_HEALTH)
)

print(f"✔ {TABLE_JOB_HEALTH} refreshed at {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Validation — Most Unreliable Jobs

# COMMAND ----------

df_unreliable = spark.sql(f"""
    SELECT
        workspace_id,
        job_id,
        job_name,
        total_runs,
        failed_runs,
        ROUND(failure_rate * 100, 1)    AS failure_rate_pct,
        ROUND(avg_duration_secs, 0)     AS avg_duration_s,
        ROUND(stddev_duration, 0)       AS stddev_s,
        ROUND(max_duration_secs, 0)     AS max_duration_s
    FROM {TABLE_JOB_HEALTH}
    WHERE total_runs >= 5
    ORDER BY failure_rate DESC
    LIMIT 20
""")

print("Top 20 most unreliable jobs (min 5 runs):")
df_unreliable.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duration Anomalies
# MAGIC Jobs whose latest run duration exceeded mean + 2σ.

# COMMAND ----------

df_anomalies = spark.sql(f"""
    WITH per_run AS (
        SELECT
            workspace_id,
            job_id,
            run_id,
            MIN(period_start_time)                                        AS run_start,
            MAX(period_end_time)                                          AS run_end,
            MAX(CASE WHEN result_state IS NOT NULL THEN result_state END) AS result_state
        FROM {SYS_LAKEFLOW_JOB_RUNS}
        WHERE period_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY workspace_id, job_id, run_id
    ),
    latest_runs AS (
        SELECT
            workspace_id,
            job_id,
            TIMESTAMPDIFF(SECOND, run_start, run_end) AS duration_secs,
            ROW_NUMBER() OVER (
                PARTITION BY workspace_id, job_id
                ORDER BY run_start DESC
            ) AS rn
        FROM per_run
        WHERE result_state IS NOT NULL
          AND run_end IS NOT NULL
          AND run_end >= run_start
    )
    SELECT
        h.workspace_id,
        h.job_id,
        h.job_name,
        lr.duration_secs                                AS latest_duration_s,
        ROUND(h.avg_duration_secs, 0)                   AS avg_s,
        ROUND(h.avg_duration_secs + {JOB_DURATION_ANOMALY_SIGMA} * h.stddev_duration, 0) AS threshold_s
    FROM {TABLE_JOB_HEALTH} h
    JOIN latest_runs lr
        ON h.workspace_id = lr.workspace_id
        AND h.job_id = lr.job_id
        AND lr.rn = 1
    WHERE lr.duration_secs > h.avg_duration_secs + {JOB_DURATION_ANOMALY_SIGMA} * h.stddev_duration
      AND h.stddev_duration > 0
    ORDER BY (lr.duration_secs - h.avg_duration_secs) / h.stddev_duration DESC
    LIMIT 20
""")

print(f"Jobs with duration anomalies (> mean + {JOB_DURATION_ANOMALY_SIGMA}σ):")
df_anomalies.display()
