# Databricks notebook source
# MAGIC %md
# MAGIC # Knowledge — Build Vector Search Index
# MAGIC
# MAGIC Creates (or refreshes) a Databricks Vector Search **Delta-sync** index over
# MAGIC `intelliops.knowledge.knowledge_docs`. The index re-embeds rows automatically
# MAGIC when the source table changes (Change Data Feed must be enabled — handled by
# MAGIC `00_seed_knowledge_docs`).
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - A Vector Search endpoint named `VS_ENDPOINT_NAME` (config.py). This notebook
# MAGIC   will create it if missing — Standard endpoints are pay-per-query.
# MAGIC - The embedding model endpoint `EMBEDDING_MODEL_ENDPOINT` (default:
# MAGIC   `databricks-gte-large-en`) is enabled in your workspace.
# MAGIC
# MAGIC Re-run after appending new rows to the docs table; the index will catch up
# MAGIC asynchronously (typically within a few minutes).

# COMMAND ----------

# MAGIC %pip install -q databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure the endpoint exists

# COMMAND ----------

# DBTITLE 1,Cell 6
from datetime import timedelta

existing_endpoints = {e["name"] for e in vsc.list_endpoints().get("endpoints", [])}
if VS_ENDPOINT_NAME not in existing_endpoints:
    print(f"Creating Vector Search endpoint '{VS_ENDPOINT_NAME}' (this can take ~5 min)...")
    vsc.create_endpoint(name=VS_ENDPOINT_NAME, endpoint_type="STANDARD")
    vsc.wait_for_endpoint(name=VS_ENDPOINT_NAME, timeout=timedelta(seconds=600))
print(f"✔ Endpoint '{VS_ENDPOINT_NAME}' ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or refresh the Delta-sync index

# COMMAND ----------

existing_indexes = {
    i["name"] for i in vsc.list_indexes(name=VS_ENDPOINT_NAME).get("vector_indexes", [])
}

if VS_INDEX_NAME not in existing_indexes:
    print(f"Creating Delta-sync index '{VS_INDEX_NAME}'...")
    vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT_NAME,
        index_name=VS_INDEX_NAME,
        source_table_name=TABLE_KNOWLEDGE_DOCS,
        pipeline_type="TRIGGERED",       # Manual refresh from this notebook
        primary_key="doc_id",
        embedding_source_column="content",
        embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
    )
    print("  Index creation kicked off — first sync runs in the background.")
else:
    print(f"Index '{VS_INDEX_NAME}' already exists — triggering a sync...")
    vsc.get_index(VS_ENDPOINT_NAME, VS_INDEX_NAME).sync()

print(f"✔ Index '{VS_INDEX_NAME}' is being populated.")
print("  Allow a few minutes for the initial embedding pass to complete before querying.")

# COMMAND ----------


