"""Tool implementations used by the LangGraph agent.

Each function is a pure callable that returns a JSON-serializable dict. The
agent (in `agent.py`) wraps these with LangChain's `@tool` decorator so the
LLM can call them through the LangGraph state machine.

Constants (`TABLE_AGENT_ACTIONS`, `TABLE_CONVERSATION`, `VS_ENDPOINT_NAME`,
`VS_INDEX_NAME`) are expected to be in the module's globals via
`%run ../config/config` (or injected by `agent.py`'s loader) before use.
"""

from __future__ import annotations

import json


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


