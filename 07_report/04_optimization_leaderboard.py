# Databricks notebook source
# MAGIC %md
# MAGIC # Report — Optimization Leaderboard
# MAGIC
# MAGIC **Audience:** All stakeholders
# MAGIC
# MAGIC Publishes stable SQL views into `intelliops.report.*` for the leaderboard
# MAGIC dashboard tab. Prints a plain-English monthly digest at the end so the
# MAGIC orchestrator run log doubles as an exec summary.

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {REPORT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `agent_activity_mtd` — current-month activity by skill / action type

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.agent_activity_mtd AS
SELECT
    skill_name,
    action_type,
    COUNT(*)                                                AS total_actions,
    SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END)    AS applied,
    SUM(CASE WHEN status = 'proposed' THEN 1 ELSE 0 END)    AS proposed,
    SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END)    AS rejected,
    ROUND(SUM(projected_savings), 2)                        AS total_projected_savings
FROM {TABLE_AGENT_ACTIONS}
WHERE action_timestamp >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
GROUP BY skill_name, action_type
ORDER BY total_projected_savings DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `agent_monthly_savings_trend` — savings per month, all history

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.agent_monthly_savings_trend AS
SELECT
    DATE_TRUNC('month', action_timestamp)               AS month,
    ROUND(SUM(projected_savings), 2)                    AS total_savings,
    COUNT(*)                                            AS total_actions,
    SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) AS applied_actions
FROM {TABLE_AGENT_ACTIONS}
GROUP BY DATE_TRUNC('month', action_timestamp)
ORDER BY month DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `agent_recent_actions` — last 50 actions

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.agent_recent_actions AS
SELECT
    action_timestamp,
    skill_name,
    action_type,
    target_name,
    description,
    ROUND(projected_savings, 2)     AS savings,
    status
FROM {TABLE_AGENT_ACTIONS}
ORDER BY action_timestamp DESC
LIMIT 50
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View: `agent_savings_by_skill` — current-month savings per skill

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {REPORT_SCHEMA}.agent_savings_by_skill AS
SELECT
    skill_name,
    ROUND(SUM(projected_savings), 2)        AS total_savings,
    COUNT(*)                                AS action_count
FROM {TABLE_AGENT_ACTIONS}
WHERE action_timestamp >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
GROUP BY skill_name
ORDER BY total_savings DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly executive digest (printed to run log, not a view)

# COMMAND ----------

stats = spark.sql(f"""
    SELECT
        COUNT(*)                                            AS total_actions,
        SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) AS applied,
        ROUND(SUM(projected_savings), 0)                    AS total_savings,
        COUNT(DISTINCT target_id)                            AS unique_targets,
        COUNT(DISTINCT skill_name)                          AS skills_used
    FROM {TABLE_AGENT_ACTIONS}
    WHERE action_timestamp >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
""").first()

if stats and stats["total_actions"] and stats["total_actions"] > 0:
    print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  IntelliOps Monthly Digest — {datetime.now().strftime('%B %Y')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  This month, IntelliOps recorded {stats['total_actions']} actions
  across {stats['unique_targets']} resources using {stats['skills_used']} skills.

  {stats['applied']} action(s) were applied.

  Projected savings: ${(stats['total_savings'] or 0):,.0f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
else:
    print("No agent actions recorded this month yet.")

# COMMAND ----------

print(f"Optimization Leaderboard views published under {REPORT_SCHEMA}.")
