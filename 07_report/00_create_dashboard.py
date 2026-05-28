# Databricks notebook source
# MAGIC %md
# MAGIC # Report — Create Lakeview Dashboard
# MAGIC
# MAGIC Programmatically creates an AI/BI (Lakeview) dashboard with **one tab per
# MAGIC view group** in `intelliops.report.*`. Each tile is a simple table widget
# MAGIC over one view. After creation, customize visualizations in the UI.
# MAGIC
# MAGIC **Run this once** (or whenever you add a new view). The notebook is idempotent
# MAGIC by display name — if a dashboard with the same name exists in `PARENT_PATH`,
# MAGIC the new one will appear alongside it (Databricks does not enforce uniqueness).
# MAGIC
# MAGIC **Pre-requisites:**
# MAGIC 1. The four report notebooks have been run, so views exist under `intelliops.report.*`.
# MAGIC 2. A running SQL Warehouse you can use as the dashboard's query backend.
# MAGIC 3. `databricks-sdk` installed on the cluster (it ships with DBR 14+).

# COMMAND ----------

# MAGIC %run ../config/config

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.text("parent_path", "", "Workspace parent path (e.g. /Workspace/Users/you@co.com)")
dbutils.widgets.text("display_name", "IntelliOps Cost Observability", "Dashboard display name")

WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
PARENT_PATH = dbutils.widgets.get("parent_path").strip()
DISPLAY_NAME = dbutils.widgets.get("display_name").strip()

if not WAREHOUSE_ID:
    raise ValueError("warehouse_id is required. Find it in SQL → SQL Warehouses → <name> → Connection details.")
if not PARENT_PATH:
    raise ValueError("parent_path is required (e.g. /Workspace/Users/<your-email>).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard structure — one page per view group

# COMMAND ----------

PAGES = {
    "Cost Command Center": [
        "cost_monthly_summary",
        "cost_current_month_trajectory",
        "cost_top_drivers_mtd",
        "cost_savings_captured",
        "cost_by_sku",
    ],
    "Cluster Health Map": [
        "cluster_utilization_heatmap",
        "cluster_over_provisioned",
        "cluster_idle_summary",
        "cluster_size_distribution",
    ],
    "Job Reliability": [
        "job_reliability_overall",
        "job_daily_failure_trend",
        "job_most_unreliable",
        "job_sla_breaches",
        "job_duration_anomalies",
    ],
    "Optimization Leaderboard": [
        "agent_activity_mtd",
        "agent_monthly_savings_trend",
        "agent_recent_actions",
        "agent_savings_by_skill",
    ],
}

GRID_COLS = 12       # Lakeview canvas is 12 columns wide
TILE_W = 6
TILE_H = 6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the serialized dashboard
# MAGIC
# MAGIC Schema columns are discovered at build time from each view, so the JSON
# MAGIC matches whatever the report notebooks publish.

# COMMAND ----------

def slug(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name).lower().strip("_")

def title_case(name: str) -> str:
    return name.replace("_", " ").title()

def table_widget(view_name, columns, widget_id, dataset_id, x, y):
    return {
        "widget": {
            "name": widget_id,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset_id,
                    "fields": [{"name": c, "expression": f"`{c}`"} for c in columns],
                    "disaggregated": True,
                },
            }],
            "spec": {
                "version": 1,
                "widgetType": "table",
                "encodings": {
                    "columns": [
                        {"fieldName": c, "displayName": title_case(c)}
                        for c in columns
                    ],
                },
                "frame": {"showTitle": True, "title": title_case(view_name)},
            },
        },
        "position": {"x": x, "y": y, "width": TILE_W, "height": TILE_H},
    }

dashboard = {"datasets": [], "pages": []}
missing_views = []

for page_name, views in PAGES.items():
    page = {
        "name": slug(page_name),
        "displayName": page_name,
        "layout": [],
    }
    for i, view in enumerate(views):
        fqn = f"{REPORT_SCHEMA}.{view}"
        try:
            columns = spark.table(fqn).columns
        except Exception:
            missing_views.append(fqn)
            continue

        dataset_id = f"ds_{slug(view)}"
        dashboard["datasets"].append({
            "name": dataset_id,
            "displayName": view,
            "queryLines": [f"SELECT * FROM {fqn}"],
        })

        x = 0 if (i % 2) == 0 else TILE_W
        y = (i // 2) * TILE_H
        page["layout"].append(
            table_widget(view, columns, f"w_{slug(view)}", dataset_id, x, y)
        )

    dashboard["pages"].append(page)

if missing_views:
    print("⚠ The following views are missing — run the 4 report notebooks first:")
    for v in missing_views:
        print(f"   - {v}")
    print("Continuing with the views that do exist.")

serialized = json.dumps(dashboard)
print(f"Built dashboard with {len(dashboard['datasets'])} datasets across {len(dashboard['pages'])} pages.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the dashboard via the Databricks SDK

# COMMAND ----------

w = WorkspaceClient()

created = w.lakeview.create(
    display_name=DISPLAY_NAME,
    parent_path=PARENT_PATH,
    warehouse_id=WAREHOUSE_ID,
    serialized_dashboard=serialized,
)

print(f"✔ Dashboard created.")
print(f"  dashboard_id : {created.dashboard_id}")
print(f"  path         : {created.path}")
print(f"  Open it at   : {spark.conf.get('spark.databricks.workspaceUrl', 'https://<workspace>')}/dashboards/{created.dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## If this notebook fails — manual fallback
# MAGIC
# MAGIC 1. In the workspace go to **Dashboards → Create dashboard**.
# MAGIC 2. Add a dataset for each view:
# MAGIC    `SELECT * FROM intelliops.report.<view_name>`
# MAGIC 3. Drag a table widget onto the canvas, point it at the dataset.
# MAGIC 4. Group views into the four tabs listed in the `PAGES` dict above.
# MAGIC
# MAGIC Programmatic Lakeview creation is sensitive to the exact widget spec; if the
# MAGIC dashboard imports but renders blank tiles, open each tile in the UI and pick
# MAGIC a visualization (table / counter / line chart / bar chart) — the dataset
# MAGIC binding will already be correct.
