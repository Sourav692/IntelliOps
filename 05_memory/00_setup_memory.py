# Databricks notebook source
# MAGIC %md
# MAGIC # Memory — Setup
# MAGIC
# MAGIC Creates the conversation-history Delta table. The action log already lives
# MAGIC under `intelliops.feature_store.agent_action_log` (created by `00_setup`); the
# MAGIC memory module reads from it but does not own it.

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MEMORY_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_CONVERSATION} (
    session_id      STRING      COMMENT 'Stable per-user / per-channel session ID',
    turn            INT         COMMENT 'Ordinal within the session',
    role            STRING      COMMENT 'user | assistant | tool | system',
    content         STRING      COMMENT 'Message text (may be empty for tool calls)',
    tool_name       STRING      COMMENT 'Tool invoked, if role=assistant or role=tool',
    tool_args       STRING      COMMENT 'JSON-encoded tool arguments',
    tool_result     STRING      COMMENT 'JSON-encoded tool result, if role=tool',
    user_id         STRING      COMMENT 'Caller identity (Slack handle / email)',
    ts              TIMESTAMP   COMMENT 'When the turn was recorded'
)
USING DELTA
COMMENT 'Per-session conversation history for the IntelliOps support agent'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

print(f"✔ {TABLE_CONVERSATION} ready.")
print(f"  Action log (already owned by feature_store): {TABLE_AGENT_ACTIONS}")
