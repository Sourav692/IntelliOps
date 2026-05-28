# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4: Report — Optimization Leaderboard
# MAGIC
# MAGIC **Audience:** All stakeholders
# MAGIC
# MAGIC **Key Metrics:** Actions taken by agent, $ saved, failures prevented

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 1: Agent Activity Summary (Current Month)

# COMMAND ----------

df_summary = spark.sql(f"""
    SELECT
        skill_name,
        action_type,
        COUNT(*)                                                AS total_actions,
        SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END)   AS applied,
        SUM(CASE WHEN status = 'proposed' THEN 1 ELSE 0 END)   AS proposed,
        SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END)   AS rejected,
        ROUND(SUM(projected_savings), 2)                        AS total_projected_savings
    FROM {TABLE_AGENT_ACTIONS}
    WHERE action_timestamp >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
    GROUP BY skill_name, action_type
    ORDER BY total_projected_savings DESC
""")

print("Agent activity — current month:")
df_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2: Total Savings Captured

# COMMAND ----------

df_total_savings = spark.sql(f"""
    SELECT
        DATE_TRUNC('month', action_timestamp)               AS month,
        ROUND(SUM(projected_savings), 2)                    AS total_savings,
        COUNT(*)                                            AS total_actions,
        SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END) AS applied_actions
    FROM {TABLE_AGENT_ACTIONS}
    GROUP BY DATE_TRUNC('month', action_timestamp)
    ORDER BY month DESC
""")

print("Monthly savings trend:")
df_total_savings.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3: Recent Actions Timeline

# COMMAND ----------

df_timeline = spark.sql(f"""
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

print("Recent agent actions:")
df_timeline.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 4: Savings by Skill (Pie Chart Data)

# COMMAND ----------

df_by_skill = spark.sql(f"""
    SELECT
        skill_name,
        ROUND(SUM(projected_savings), 2)        AS total_savings,
        COUNT(*)                                AS action_count
    FROM {TABLE_AGENT_ACTIONS}
    WHERE action_timestamp >= DATE_TRUNC('month', CURRENT_TIMESTAMP)
    GROUP BY skill_name
    ORDER BY total_savings DESC
""")

print("Savings breakdown by skill:")
df_by_skill.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 5: Executive Narrative Digest

# COMMAND ----------

# Generate a plain-English summary of this month's agent activity
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

if stats and stats["total_actions"] > 0:
    narrative = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  IntelliOps Monthly Digest — {datetime.now().strftime('%B %Y')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  This month, IntelliOps took {stats['total_actions']} actions
  across {stats['unique_targets']} resources using {stats['skills_used']} skills.

  {stats['applied']} action(s) were automatically applied.

  Projected savings: ${stats['total_savings']:,.0f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    print(narrative)
else:
    print("No agent actions recorded this month yet.")
