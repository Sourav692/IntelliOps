# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3: Act — Skill 5: Budget Forecast & Guardrail
# MAGIC
# MAGIC **Trigger:** Daily, or when rolling_14d_avg trend is rising
# MAGIC
# MAGIC **Agent Actions:**
# MAGIC 1. Projects end-of-month spend per workspace/team/tag using linear trend
# MAGIC 2. Compares against configured budget thresholds
# MAGIC 3. If forecast > 90% of budget: auto-notifies owner + suggests optimization

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# MAGIC %run ../utils/notifications

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Calculate Current Month Spend & Project End-of-Month

# COMMAND ----------

df_monthly_projection = spark.sql(f"""
    WITH daily_spend AS (
        SELECT
            u.workspace_id,
            u.usage_date,
            SUM(u.usage_quantity * p.pricing.default) AS daily_cost_usd
        FROM {SYS_BILLING_USAGE} u
        JOIN {SYS_BILLING_PRICES} p
            ON u.cloud = p.cloud AND u.sku_name = p.sku_name
        WHERE u.usage_date >= DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY u.workspace_id, u.usage_date
    ),
    workspace_stats AS (
        SELECT
            workspace_id,
            SUM(daily_cost_usd)                         AS mtd_spend,
            AVG(daily_cost_usd)                         AS avg_daily_spend,
            COUNT(DISTINCT usage_date)                   AS days_elapsed,
            DAY(LAST_DAY(CURRENT_DATE))                  AS days_in_month
        FROM daily_spend
        GROUP BY workspace_id
    )
    SELECT
        workspace_id,
        ROUND(mtd_spend, 2)                             AS mtd_spend,
        ROUND(avg_daily_spend, 2)                       AS avg_daily_spend,
        days_elapsed,
        days_in_month,
        ROUND(avg_daily_spend * days_in_month, 2)        AS projected_eom_spend,
        -- Linear trend projection
        ROUND(mtd_spend + avg_daily_spend * (days_in_month - days_elapsed), 2)
                                                         AS linear_projection
    FROM workspace_stats
    ORDER BY linear_projection DESC
""")

projections = df_monthly_projection.collect()
print(f"Workspace projections for {datetime.now().strftime('%B %Y')}:")
df_monthly_projection.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Spend by Team / Custom Tag

# COMMAND ----------

df_spend_by_tag = spark.sql(f"""
    SELECT
        u.workspace_id,
        u.custom_tags['team']           AS team_tag,
        ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS mtd_spend
    FROM {SYS_BILLING_USAGE} u
    JOIN {SYS_BILLING_PRICES} p
        ON u.cloud = p.cloud AND u.sku_name = p.sku_name
    WHERE u.usage_date >= DATE_TRUNC('month', CURRENT_DATE)
      AND u.custom_tags['team'] IS NOT NULL
    GROUP BY u.workspace_id, u.custom_tags['team']
    ORDER BY mtd_spend DESC
""")

print("Spend by team tag:")
df_spend_by_tag.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Budget Guardrail — Alert if Forecast > 90% Budget

# COMMAND ----------

actions = []

for proj in projections:
    ws_id = proj["workspace_id"]
    linear_proj = proj["linear_projection"]
    mtd = proj["mtd_spend"]
    daily_avg = proj["avg_daily_spend"]

    # Get budget for this workspace
    budget = WORKSPACE_BUDGETS.get(ws_id, WORKSPACE_BUDGETS.get("default", 50000))
    budget_pct = (linear_proj / budget * 100) if budget > 0 else 0

    if budget_pct >= COST_BUDGET_ALERT_PCT:
        severity = "critical" if budget_pct > 100 else "warning"

        # Find top cost drivers to suggest optimizations
        top_jobs = spark.sql(f"""
            SELECT job_name, ROUND(SUM(daily_cost_usd), 2) AS mtd_cost
            FROM {TABLE_JOB_COST_TREND}
            WHERE workspace_id = '{ws_id}'
              AND usage_date >= DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY job_name
            ORDER BY mtd_cost DESC
            LIMIT 3
        """).collect()

        top_drivers = ", ".join(
            f"{r['job_name']} (${r['mtd_cost']})" for r in top_jobs
        ) or "N/A"

        description = (
            f"Workspace {ws_id}: projected EOM spend ${linear_proj:,.0f} "
            f"({budget_pct:.0f}% of ${budget:,.0f} budget). "
            f"MTD: ${mtd:,.0f}, avg ${daily_avg:,.0f}/day. "
            f"Top drivers: {top_drivers}."
        )

        action = {
            "action_id": str(uuid.uuid4()),
            "action_timestamp": datetime.now().isoformat(),
            "skill_name": "budget_forecast",
            "action_type": "alert",
            "workspace_id": ws_id,
            "target_id": ws_id,
            "target_name": f"Workspace {ws_id}",
            "description": description,
            "projected_savings": 0.0,
            "status": "proposed",
            "details": json.dumps({
                "mtd_spend": mtd,
                "projected_eom": linear_proj,
                "budget": budget,
                "budget_pct": round(budget_pct, 1),
                "top_drivers": [{"name": r["job_name"], "cost": r["mtd_cost"]} for r in top_jobs],
            }),
        }
        actions.append(action)

        # Notify
        notify(
            title=f"Budget Alert: {budget_pct:.0f}% of Budget",
            message=description,
            severity=severity,
            details={
                "Workspace": ws_id,
                "MTD Spend": f"${mtd:,.0f}",
                "Projected": f"${linear_proj:,.0f}",
                "Budget": f"${budget:,.0f}",
            },
        )
        print(f"  {'🔴' if severity == 'critical' else '🟡'} {description}")

    else:
        print(f"  🟢 Workspace {ws_id}: ${linear_proj:,.0f} projected ({budget_pct:.0f}% of budget) — OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Log Actions

# COMMAND ----------

if actions:
    df_actions = spark.createDataFrame(actions)
    df_actions.write.format("delta").mode("append").saveAsTable(TABLE_AGENT_ACTIONS)
    print(f"\n✔ Logged {len(actions)} budget alert(s)")
else:
    print("✔ All workspaces within budget.")
