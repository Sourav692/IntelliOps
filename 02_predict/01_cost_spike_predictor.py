# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2: Predict — Cost Spike Predictor
# MAGIC
# MAGIC Trains a time-series forecasting model on historical billing data to predict
# MAGIC cost spikes for the next 7 days per workspace/job.
# MAGIC
# MAGIC **Approach:** Prophet model per top-N cost-driving jobs, tracked via MLflow.
# MAGIC
# MAGIC **Input:** `intelliops.feature_store.feat_job_cost_trend`
# MAGIC **Output:** Predictions logged to MLflow + cost forecast table

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

import mlflow
import mlflow.prophet
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# COMMAND ----------

# ── MLflow Experiment Setup ─────────────────────────────────────────────────────
experiment_path = f"{MLFLOW_EXPERIMENT_PREFIX}/cost_spike_predictor"
mlflow.set_experiment(experiment_path)
print(f"MLflow experiment: {experiment_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare Training Data
# MAGIC Aggregate daily cost per workspace for Prophet (requires `ds` and `y` columns).

# COMMAND ----------

df_cost = spark.sql(f"""
    SELECT
        workspace_id,
        job_id,
        job_name,
        usage_date          AS ds,
        daily_cost_usd      AS y
    FROM {TABLE_JOB_COST_TREND}
    WHERE daily_cost_usd IS NOT NULL
      AND usage_date >= CURRENT_DATE - INTERVAL 60 DAYS
    ORDER BY workspace_id, job_id, usage_date
""")

# Identify top cost-driving jobs to model (top 20 by total spend)
top_jobs = spark.sql(f"""
    SELECT workspace_id, job_id, job_name, SUM(daily_cost_usd) AS total_cost
    FROM {TABLE_JOB_COST_TREND}
    WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY workspace_id, job_id, job_name
    ORDER BY total_cost DESC
    LIMIT 20
""").collect()

print(f"Training models for {len(top_jobs)} top cost-driving jobs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Train Prophet Model per Job

# COMMAND ----------

from prophet import Prophet

forecast_results = []

for row in top_jobs:
    ws_id = row["workspace_id"]
    j_id = row["job_id"]
    j_name = row["job_name"] or f"job_{j_id}"

    # Filter data for this job
    pdf = (
        df_cost
        .filter(f"workspace_id = '{ws_id}' AND job_id = '{j_id}'")
        .select("ds", "y")
        .toPandas()
    )

    if len(pdf) < 14:
        print(f"  ⏭ Skipping {j_name} — only {len(pdf)} days of data (need 14+)")
        continue

    pdf["ds"] = pd.to_datetime(pdf["ds"])

    with mlflow.start_run(run_name=f"cost_pred_{j_name[:40]}"):
        mlflow.log_params({
            "workspace_id": ws_id,
            "job_id": j_id,
            "job_name": j_name,
            "training_days": len(pdf),
            "forecast_horizon": COST_FORECAST_HORIZON_DAYS,
        })

        # Train Prophet
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=False,
            changepoint_prior_scale=0.05,
        )
        model.fit(pdf)

        # Forecast next N days
        future = model.make_future_dataframe(periods=COST_FORECAST_HORIZON_DAYS)
        forecast = model.predict(future)

        # Extract predictions for future dates only
        future_forecast = forecast[forecast["ds"] > pdf["ds"].max()][
            ["ds", "yhat", "yhat_lower", "yhat_upper"]
        ]
        total_predicted = future_forecast["yhat"].sum()
        total_historical_avg = pdf["y"].tail(7).sum()

        predicted_growth = (
            (total_predicted - total_historical_avg) / total_historical_avg * 100
            if total_historical_avg > 0 else 0
        )

        mlflow.log_metrics({
            "predicted_7d_cost": total_predicted,
            "historical_7d_avg_cost": total_historical_avg,
            "predicted_growth_pct": predicted_growth,
        })

        # Log model
        mlflow.prophet.log_model(model, artifact_path="prophet_model")

        # Collect results
        forecast_results.append({
            "workspace_id": ws_id,
            "job_id": j_id,
            "job_name": j_name,
            "predicted_7d_cost": round(total_predicted, 2),
            "historical_7d_avg": round(total_historical_avg, 2),
            "predicted_growth_pct": round(predicted_growth, 1),
            "is_spike": predicted_growth > COST_SPIKE_THRESHOLD_PCT,
        })

        status = "🔴 SPIKE" if predicted_growth > COST_SPIKE_THRESHOLD_PCT else "🟢 OK"
        print(f"  {status} {j_name}: predicted ${total_predicted:.0f} vs avg ${total_historical_avg:.0f} ({predicted_growth:+.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save Forecast Results

# COMMAND ----------

if forecast_results:
    df_forecasts = spark.createDataFrame(forecast_results)
    (
        df_forecasts.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{FULL_SCHEMA}.cost_forecasts")
    )
    print(f"\n✔ Saved {len(forecast_results)} forecasts to {FULL_SCHEMA}.cost_forecasts")

    # Show spike alerts
    spikes = [r for r in forecast_results if r["is_spike"]]
    if spikes:
        print(f"\n⚠️  {len(spikes)} cost spike(s) predicted:")
        for s in spikes:
            print(f"   • {s['job_name']}: +{s['predicted_growth_pct']}% (${s['predicted_7d_cost']})")
    else:
        print("\n✔ No cost spikes predicted for next 7 days.")
else:
    print("No forecasts generated — insufficient data.")
