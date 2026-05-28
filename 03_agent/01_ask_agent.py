# Databricks notebook source
# MAGIC %md
# MAGIC # Agent — Ask
# MAGIC
# MAGIC Builds a LangGraph ReAct agent from `agent.build_graph(...)` and asks it
# MAGIC one question. The graph is the public surface — no wrapper class.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - `00_setup/00_setup_feature_store` has been run (action log table exists).
# MAGIC - `02_knowledge/00_seed_knowledge_docs` + `01_build_knowledge_index` have run.
# MAGIC - `LLM_ENDPOINT_NAME` in config points to a Foundation Model endpoint your
# MAGIC   workspace has access to.

# COMMAND ----------

# MAGIC %pip install -q langgraph langchain-core databricks-langchain databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

import json, os, sys

# Make the agent module importable. In Databricks Repos / Git Folders the
# notebook's parent directory is on sys.path automatically; this line is a
# safety net for other layouts.
notebook_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path.insert(0, f"/Workspace{notebook_dir}")

from agent import build_graph

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the graph

# COMMAND ----------

graph = build_graph(
    llm_endpoint=LLM_ENDPOINT_NAME,
    system_prompt=AGENT_SYSTEM_PROMPT,
    vs_endpoint=VS_ENDPOINT_NAME,
    vs_index=VS_INDEX_NAME,
    table_actions=TABLE_AGENT_ACTIONS,
    temperature=AGENT_TEMPERATURE,
    max_tokens=AGENT_MAX_TOKENS,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ask a question

# COMMAND ----------

dbutils.widgets.text("question", "Which jobs are wasting the most spend this week?", "Question")
QUESTION = dbutils.widgets.get("question")

# COMMAND ----------

result = graph.invoke(
    {"messages": [("user", QUESTION)]},
    config={"recursion_limit": AGENT_MAX_ITERATIONS * 2 + 5},
)

print("━" * 60)
print(f"Q: {QUESTION}")
print("━" * 60)
print(result["messages"][-1].content)
print("━" * 60)

# Show the full message trace (system / user / assistant / tool turns)
for m in result["messages"]:
    print(f"[{m.type:>9}] {(m.content or '')[:200]}")
    if getattr(m, "tool_calls", None):
        for tc in m.tool_calls:
            print(f"           → call {tc['name']}({json.dumps(tc.get('args', {}))[:120]})")
