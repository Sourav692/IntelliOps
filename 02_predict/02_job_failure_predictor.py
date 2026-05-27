# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2: Predict — Job Failure Risk Predictor
# MAGIC
# MAGIC Scores each job with a failure probability based on historical patterns
# MAGIC and current cluster pressure.
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `intelliops.feature_store.feat_job_health` — Historical failure rates
# MAGIC - `intelliops.feature_store.feat_cluster_utilization` — Current cluster pressure
# MAGIC - `system.lakeflow.job_task_run_timeline` — Task-to-cluster mapping
# MAGIC
# MAGIC **Output:** Failure risk scores per job, logged to MLflow

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from datetime import datetime

# COMMAND ----------

# ── MLflow Experiment Setup ─────────────────────────────────────────────────────
experiment_path = f"{MLFLOW_EXPERIMENT_PREFIX}/job_failure_predictor"
mlflow.set_experiment(experiment_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Training Features
# MAGIC Combine job health metrics with cluster utilization pressure signals.

# COMMAND ----------

df_features = spark.sql(f"""
    WITH job_cluster_map AS (
        -- Map jobs to their most recent cluster
        SELECT
            workspace_id,
            job_id,
            compute_id AS cluster_id,
            ROW_NUMBER() OVER (
                PARTITION BY workspace_id, job_id
                ORDER BY period_start_time DESC
            ) AS rn
        FROM {SYS_LAKEFLOW_JOB_TASKS}
        WHERE period_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    ),
    cluster_pressure AS (
        -- Recent cluster pressure (last 24h)
        SELECT
            cluster_id,
            workspace_id,
            AVG(avg_cpu_pct)    AS recent_avg_cpu,
            MAX(peak_cpu_pct)   AS recent_peak_cpu,
            AVG(avg_mem_pct)    AS recent_avg_mem,
            MAX(peak_mem_pct)   AS recent_peak_mem
        FROM {TABLE_CLUSTER_UTILIZATION}
        WHERE hour_window >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
        GROUP BY cluster_id, workspace_id
    )
    SELECT
        h.workspace_id,
        h.job_id,
        h.job_name,
        h.total_runs,
        h.failed_runs,
        h.failure_rate,
        h.avg_duration_secs,
        h.stddev_duration,
        h.max_duration_secs,
        -- Duration variability ratio
        COALESCE(h.stddev_duration / NULLIF(h.avg_duration_secs, 0), 0)
            AS duration_cv,
        -- Cluster pressure signals
        COALESCE(cp.recent_avg_cpu, 50)     AS cluster_avg_cpu,
        COALESCE(cp.recent_peak_cpu, 50)    AS cluster_peak_cpu,
        COALESCE(cp.recent_avg_mem, 50)     AS cluster_avg_mem,
        COALESCE(cp.recent_peak_mem, 50)    AS cluster_peak_mem,
        -- Label: 1 if failure rate > 0 in recent history
        CASE WHEN h.failure_rate > 0 THEN 1 ELSE 0 END AS has_failures
    FROM {TABLE_JOB_HEALTH} h
    LEFT JOIN job_cluster_map jcm
        ON h.workspace_id = jcm.workspace_id
        AND h.job_id = jcm.job_id
        AND jcm.rn = 1
    LEFT JOIN cluster_pressure cp
        ON jcm.cluster_id = cp.cluster_id
        AND jcm.workspace_id = cp.workspace_id
    WHERE h.total_runs >= 3
""")

print(f"Training samples: {df_features.count()}")
df_features.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Train Failure Risk Model
# MAGIC Gradient Boosted Classifier to predict failure probability.

# COMMAND ----------

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_score, recall_score

pdf = df_features.toPandas()

feature_cols = [
    "failure_rate", "total_runs", "avg_duration_secs", "stddev_duration",
    "duration_cv", "cluster_avg_cpu", "cluster_peak_cpu",
    "cluster_avg_mem", "cluster_peak_mem",
]

X = pdf[feature_cols].fillna(0)
y = pdf["has_failures"]

if len(y.unique()) < 2:
    print("⚠️ Insufficient label diversity — all jobs have same failure status.")
    print("   Falling back to heuristic scoring.")
    USE_HEURISTIC = True
else:
    USE_HEURISTIC = False
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    with mlflow.start_run(run_name="failure_risk_model"):
        model = GradientBoostingClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            random_state=42,
        )
        model.fit(X_train, y_train)

        # Evaluate
        y_prob = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_prob)
        prec = precision_score(y_test, (y_prob > 0.5).astype(int), zero_division=0)
        rec = recall_score(y_test, (y_prob > 0.5).astype(int), zero_division=0)

        mlflow.log_metrics({"auc": auc, "precision": prec, "recall": rec})
        mlflow.log_params({"n_estimators": 100, "max_depth": 4, "features": str(feature_cols)})
        mlflow.sklearn.log_model(model, artifact_path="failure_model")

        print(f"Model trained — AUC: {auc:.3f}, Precision: {prec:.3f}, Recall: {rec:.3f}")

        # Feature importance
        importance = sorted(
            zip(feature_cols, model.feature_importances_),
            key=lambda x: x[1], reverse=True
        )
        print("\nFeature importance:")
        for feat, imp in importance:
            print(f"  {feat:30s} {imp:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Score All Jobs

# COMMAND ----------

if USE_HEURISTIC:
    # Heuristic: combine failure_rate + cluster pressure + duration variability
    pdf["failure_risk_score"] = (
        pdf["failure_rate"] * 0.5 +
        (pdf["cluster_peak_cpu"] / 100) * 0.2 +
        (pdf["cluster_peak_mem"] / 100) * 0.15 +
        pdf["duration_cv"].clip(0, 1) * 0.15
    ).clip(0, 1)
    print("Using heuristic scoring (insufficient training data for ML model).")
else:
    pdf["failure_risk_score"] = model.predict_proba(X)[:, 1]
    print("Using ML model scoring.")

# COMMAND ----------

# ── Save risk scores ────────────────────────────────────────────────────────────
df_scores = spark.createDataFrame(
    pdf[["workspace_id", "job_id", "job_name", "failure_risk_score", "failure_rate",
         "cluster_avg_cpu", "cluster_peak_mem", "total_runs"]]
)
df_scores = df_scores.withColumn("scored_at", __import__("pyspark.sql.functions", fromlist=["current_timestamp"]).current_timestamp())

(
    df_scores.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{FULL_SCHEMA}.job_failure_risk_scores")
)

print(f"\n✔ Saved risk scores to {FULL_SCHEMA}.job_failure_risk_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## High-Risk Jobs

# COMMAND ----------

df_high_risk = spark.sql(f"""
    SELECT
        workspace_id,
        job_id,
        job_name,
        ROUND(failure_risk_score, 3)    AS risk_score,
        ROUND(failure_rate * 100, 1)    AS failure_rate_pct,
        total_runs
    FROM {FULL_SCHEMA}.job_failure_risk_scores
    WHERE failure_risk_score > {JOB_FAILURE_RISK_THRESHOLD}
    ORDER BY failure_risk_score DESC
    LIMIT 20
""")

print(f"High-risk jobs (score > {JOB_FAILURE_RISK_THRESHOLD}):")
df_high_risk.display()
