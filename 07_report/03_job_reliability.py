# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4: Report — Job Reliability
# MAGIC
# MAGIC **Audience:** Data Engineering
# MAGIC
# MAGIC **Key Metrics:** Failure rates, SLA breach counts, duration anomalies

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 1: Overall Reliability Score

# COMMAND ----------

df_reliability = spark.sql(f"""
    SELECT
        COUNT(*)                                            AS total_jobs,
        SUM(total_runs)                                     AS total_runs,
        SUM(failed_runs)                                    AS total_failures,
        ROUND((1 - SUM(failed_runs) * 1.0
            / NULLIF(SUM(total_runs), 0)) * 100, 2)        AS overall_success_rate_pct,
        ROUND(AVG(failure_rate) * 100, 2)                   AS avg_failure_rate_pct
    FROM {TABLE_JOB_HEALTH}
""")

print("Platform reliability (last 30 days):")
df_reliability.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2: Daily Failure Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE(period_start_time)                             AS run_date,
# MAGIC     COUNT(*)                                            AS total_runs,
# MAGIC     SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failures,
# MAGIC     ROUND(SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) * 100.0
# MAGIC         / COUNT(*), 2)                                  AS failure_rate_pct
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC GROUP BY DATE(period_start_time)
# MAGIC ORDER BY run_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3: Most Unreliable Jobs

# COMMAND ----------

df_unreliable = spark.sql(f"""
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

print("Most unreliable jobs (min 5 runs, last 30 days):")
df_unreliable.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 4: SLA Breach Detection
# MAGIC Jobs whose average duration exceeds a configurable SLA threshold.

# COMMAND ----------

# Define SLA thresholds (can be moved to config)
SLA_DURATION_MINUTES = 60  # Default: jobs should complete within 60 min

df_sla = spark.sql(f"""
    SELECT
        job_name,
        workspace_id,
        total_runs,
        ROUND(avg_duration_secs / 60, 1)        AS avg_duration_min,
        ROUND(max_duration_secs / 60, 1)         AS max_duration_min,
        -- How many runs exceeded the SLA?
        ROUND(
            CASE WHEN avg_duration_secs > {SLA_DURATION_MINUTES * 60}
                 THEN 1 ELSE 0 END * total_runs, 0
        )                                         AS est_sla_breaches
    FROM {TABLE_JOB_HEALTH}
    WHERE avg_duration_secs > {SLA_DURATION_MINUTES * 60}
    ORDER BY avg_duration_secs DESC
    LIMIT 15
""")

print(f"Jobs breaching {SLA_DURATION_MINUTES}-min SLA:")
df_sla.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 5: Duration Anomalies (Last 7 Days)

# COMMAND ----------

df_anomalies = spark.sql(f"""
    WITH latest_runs AS (
        SELECT
            workspace_id, job_id,
            TIMESTAMPDIFF(SECOND, period_start_time, period_end_time) AS duration_secs,
            period_start_time,
            ROW_NUMBER() OVER (
                PARTITION BY workspace_id, job_id ORDER BY period_start_time DESC
            ) AS rn
        FROM {SYS_LAKEFLOW_JOB_RUNS}
        WHERE period_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
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

print(f"Duration anomalies (> mean + {JOB_DURATION_ANOMALY_SIGMA}σ):")
df_anomalies.display()
