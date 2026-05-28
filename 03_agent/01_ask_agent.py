# Databricks notebook source
# MAGIC %md
# MAGIC # Agent — Ask
# MAGIC
# MAGIC Interactive entry point for the IntelliOps support agent. Used for
# MAGIC development and ad-hoc queries; the production interface (`06_interface/`)
# MAGIC will call `agent.ask()` from a Slack bot or Databricks App.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - `00_setup/00_setup_feature_store` has been run (action log table exists).
# MAGIC - `05_memory/00_setup_memory` has been run (conversation table exists).
# MAGIC - `02_knowledge/00_seed_knowledge_docs` + `01_build_knowledge_index` have run.
# MAGIC - `LLM_ENDPOINT_NAME` in config points to a Foundation Model endpoint your
# MAGIC   workspace has access to.

# COMMAND ----------

# MAGIC %pip install -q langgraph langchain-core databricks-langchain databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

import importlib.util, os, sys

# Load the agent module, injecting config globals it needs.
here = os.getcwd()
agent_path = os.path.join(here, "agent.py") if os.path.exists("agent.py") \
             else "/Workspace" + dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0] + "/agent.py"

spec = importlib.util.spec_from_file_location("ops_agent", agent_path)
agent_mod = importlib.util.module_from_spec(spec)
for k in (
    "LLM_ENDPOINT_NAME", "AGENT_MAX_ITERATIONS", "AGENT_TEMPERATURE",
    "AGENT_MAX_TOKENS", "AGENT_SYSTEM_PROMPT",
    "TABLE_AGENT_ACTIONS", "TABLE_CONVERSATION",
    "VS_ENDPOINT_NAME", "VS_INDEX_NAME",
):
    setattr(agent_mod, k, globals()[k])
sys.modules["ops_agent"] = agent_mod
spec.loader.exec_module(agent_mod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ask a question

# COMMAND ----------

dbutils.widgets.text("question", "Which jobs are wasting the most spend this week?", "Question")
QUESTION = dbutils.widgets.get("question")

# COMMAND ----------

result = agent_mod.ask(QUESTION, user_id="notebook")

print("━" * 60)
print(f"Q: {QUESTION}")
print("━" * 60)
print(result["answer"])
print("━" * 60)
print(f"Session     : {result['session_id']}")
print(f"Iterations  : {result['iterations']}")
print(f"Tool calls  : {len(result['tool_calls'])}")
for i, tc in enumerate(result["tool_calls"], 1):
    print(f"  [{i}] {tc['name']}({tc['args'][:120]}…)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replay the conversation

# COMMAND ----------

display(spark.sql(f"""
    SELECT turn, role, tool_name,
           SUBSTRING(content, 1, 200) AS content_preview,
           ts
    FROM {TABLE_CONVERSATION}
    WHERE session_id = '{result["session_id"]}'
    ORDER BY turn
"""))
