# Databricks notebook source
# MAGIC %md
# MAGIC # Utils — Databricks REST API Helpers
# MAGIC
# MAGIC Wrappers for Clusters REST API operations with retry and error handling.
# MAGIC Requires notebook context for host/token resolution.

# COMMAND ----------

import requests
import json
import time

# COMMAND ----------

def get_databricks_context():
    """Resolve Databricks host and token from the notebook context."""
    host = spark.conf.get("spark.databricks.workspaceUrl", None)
    if not host:
        host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    return host.rstrip("/"), token

# COMMAND ----------

def _api_request(method, endpoint, payload=None, retries=2):
    """Generic Databricks REST API request with retry."""
    host, token = get_databricks_context()
    url = f"{host}{endpoint}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    for attempt in range(retries + 1):
        try:
            resp = requests.request(method, url, headers=headers, json=payload, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 2 ** attempt
                print(f"  Rate limited, retrying in {wait}s...")
                time.sleep(wait)
                continue
            else:
                print(f"  API error {resp.status_code}: {resp.text}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"  Request failed: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None

# COMMAND ----------

def get_cluster(cluster_id):
    """Get current cluster configuration."""
    return _api_request("GET", f"/api/2.0/clusters/get?cluster_id={cluster_id}")

# COMMAND ----------

def edit_cluster(cluster_id, min_workers=None, max_workers=None, node_type_id=None):
    """
    Edit cluster autoscale or node type configuration.
    Note: This will restart the cluster.
    """
    current = get_cluster(cluster_id)
    if not current:
        print(f"  Could not fetch cluster {cluster_id}")
        return None

    payload = {
        "cluster_id": cluster_id,
        "cluster_name": current.get("cluster_name"),
        "spark_version": current.get("spark_version"),
        "node_type_id": node_type_id or current.get("node_type_id"),
    }

    if min_workers is not None and max_workers is not None:
        payload["autoscale"] = {
            "min_workers": min_workers,
            "max_workers": max_workers,
        }
    elif "autoscale" in current:
        payload["autoscale"] = current["autoscale"]
    elif "num_workers" in current:
        payload["num_workers"] = current["num_workers"]

    print(f"  Editing cluster {cluster_id}: {json.dumps(payload, indent=2)}")
    return _api_request("POST", "/api/2.0/clusters/edit", payload)

# COMMAND ----------

def terminate_cluster(cluster_id):
    """Terminate an idle cluster."""
    return _api_request("POST", "/api/2.0/clusters/delete", {"cluster_id": cluster_id})

# COMMAND ----------

def list_clusters(state_filter=None):
    """List all clusters, optionally filtered by state (RUNNING, TERMINATED, etc.)."""
    result = _api_request("GET", "/api/2.0/clusters/list")
    if not result:
        return []
    clusters = result.get("clusters", [])
    if state_filter:
        clusters = [c for c in clusters if c.get("state") == state_filter]
    return clusters
