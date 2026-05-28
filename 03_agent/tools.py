"""Tool implementations + JSON schemas exposed to the LLM.

Each tool is a pure function that returns a JSON-serializable value. The agent
loop is responsible for calling them, formatting the result back into the chat,
and logging the call to memory.

Constants (`TABLE_*`, `REPORT_SCHEMA`, `MEMORY_SCHEMA`, etc.) are expected to
be in the module's globals via `%run ../config/config` before import.
"""

from __future__ import annotations

import json
from typing import Any


# ── Tool definitions exposed to the LLM (OpenAI tool-call schema) ────────────
TOOL_SCHEMAS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "query_features",
            "description": (
                "Fast path. Run a read-only SQL query against the pre-aggregated "
                "IntelliOps feature tables (intelliops.feature_store.*) or the "
                "stable report views (intelliops.report.*). Use this for the "
                "common questions: cost by job, utilization by cluster, failure "
                "rates, savings captured. Returns up to 50 rows."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": (
                            "A single SELECT statement. Must read from "
                            "intelliops.feature_store.* or intelliops.report.* only."
                        ),
                    }
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_system_tables",
            "description": (
                "Escape hatch. Run a read-only SQL query against system.* tables "
                "directly. Use this only when (a) the requested data is not "
                "pre-aggregated in a feature table, or (b) freshness within the "
                "last ~15 minutes is required. Returns up to 50 rows."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": (
                            "A single SELECT statement reading from system.billing.*, "
                            "system.compute.*, or system.lakeflow.* only."
                        ),
                    }
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_knowledge",
            "description": (
                "Semantic search over IntelliOps's curated knowledge corpus "
                "(pricing notes, cost-optimization best practices, internal runbooks). "
                "Use when the user asks 'why', 'best practice', 'how should I', or "
                "needs context the data tables can't provide."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural-language query."},
                    "num_results": {"type": "integer", "default": 4},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "log_action_record",
            "description": (
                "Persist a recommendation or finding to the agent action log so it "
                "appears on the Optimization Leaderboard. Call this when you have "
                "delivered a concrete recommendation with a target resource."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "skill_name":        {"type": "string"},
                    "action_type":       {"type": "string", "description": "alert | recommendation"},
                    "target_id":         {"type": "string"},
                    "target_name":       {"type": "string"},
                    "description":       {"type": "string"},
                    "projected_savings": {"type": "number", "default": 0},
                    "status":            {"type": "string", "default": "proposed"},
                    "workspace_id":      {"type": "string"},
                },
                "required": ["skill_name", "action_type", "target_id", "target_name", "description"],
            },
        },
    },
]


# ── Tool implementations ─────────────────────────────────────────────────────

def _spark():
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession()


def _run_select(sql: str, allowed_prefixes: tuple[str, ...], limit: int = 50) -> dict:
    """Guarded SELECT runner. Refuses anything that isn't a single SELECT or that
    touches a disallowed namespace."""
    s = sql.strip().rstrip(";").strip()
    head = s.lstrip("(").lstrip().lower()
    if not (head.startswith("select") or head.startswith("with ")):
        return {"error": "Only SELECT / WITH queries are allowed."}
    lower = s.lower()
    # Reject anything not pointing at an allowed namespace.
    if not any(p in lower for p in allowed_prefixes):
        return {"error": f"Query must read from one of: {allowed_prefixes}"}
    # Reject anything that looks like a mutation.
    for bad in ("insert ", "update ", "delete ", "drop ", "alter ", "merge ", "create "):
        if bad in lower:
            return {"error": f"Disallowed keyword: {bad.strip()}"}
    # Wrap with limit if the user didn't.
    if " limit " not in lower:
        s = f"SELECT * FROM ({s}) _q LIMIT {limit}"
    try:
        rows = _spark().sql(s).collect()
        return {
            "row_count": len(rows),
            "rows": [r.asDict() for r in rows[:limit]],
        }
    except Exception as e:
        return {"error": str(e)}


def query_features(sql: str) -> dict:
    return _run_select(
        sql,
        allowed_prefixes=("intelliops.feature_store.", "intelliops.report."),
    )


def query_system_tables(sql: str) -> dict:
    return _run_select(
        sql,
        allowed_prefixes=("system.billing.", "system.compute.", "system.lakeflow."),
    )


def search_knowledge(query: str, num_results: int = 4) -> dict:
    # Import here so the agent module can be loaded even if knowledge is misconfigured.
    import importlib.util, os, sys
    knowledge_path = os.path.join(os.path.dirname(__file__), "..", "02_knowledge", "knowledge.py")
    spec = importlib.util.spec_from_file_location("ops_knowledge", knowledge_path)
    if spec is None or spec.loader is None:
        return {"error": "knowledge module not found"}
    mod = importlib.util.module_from_spec(spec)
    # Bring config constants into the knowledge module's namespace.
    mod.VS_ENDPOINT_NAME = VS_ENDPOINT_NAME       # noqa: F821
    mod.VS_INDEX_NAME = VS_INDEX_NAME             # noqa: F821
    sys.modules["ops_knowledge"] = mod
    spec.loader.exec_module(mod)
    results = mod.search_knowledge(query, num_results=num_results)
    return {"results": results}


def log_action_record(
    skill_name: str,
    action_type: str,
    target_id: str,
    target_name: str,
    description: str,
    projected_savings: float = 0.0,
    status: str = "proposed",
    workspace_id: str | None = None,
) -> dict:
    # Lazy-load the memory module the same way as knowledge.
    import importlib.util, os, sys
    mem_path = os.path.join(os.path.dirname(__file__), "..", "05_memory", "memory.py")
    spec = importlib.util.spec_from_file_location("ops_memory", mem_path)
    if spec is None or spec.loader is None:
        return {"error": "memory module not found"}
    mod = importlib.util.module_from_spec(spec)
    mod.TABLE_AGENT_ACTIONS = TABLE_AGENT_ACTIONS  # noqa: F821
    mod.TABLE_CONVERSATION = TABLE_CONVERSATION    # noqa: F821
    sys.modules["ops_memory"] = mod
    spec.loader.exec_module(mod)

    action_id = mod.log_action_record(
        skill_name=skill_name,
        action_type=action_type,
        target_id=target_id,
        target_name=target_name,
        description=description,
        projected_savings=projected_savings,
        status=status,
        workspace_id=workspace_id,
    )
    return {"action_id": action_id, "status": status}


# ── Dispatcher used by the agent loop ────────────────────────────────────────

DISPATCH = {
    "query_features": query_features,
    "query_system_tables": query_system_tables,
    "search_knowledge": search_knowledge,
    "log_action_record": log_action_record,
}


def call_tool(name: str, args_json: str) -> Any:
    if name not in DISPATCH:
        return {"error": f"unknown tool '{name}'"}
    try:
        args = json.loads(args_json) if args_json else {}
    except json.JSONDecodeError as e:
        return {"error": f"bad arguments JSON: {e}"}
    return DISPATCH[name](**args)
