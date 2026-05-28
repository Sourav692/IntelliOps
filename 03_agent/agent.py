"""IntelliOps support agent — minimal LangGraph implementation.

One file. Tools are defined inline inside `build_graph(...)` so they close over
the config the caller passes in (no global injection, no dynamic module loaders).
Returns a compiled LangGraph graph the caller invokes directly.

Usage from a Databricks notebook (after `%run ../config/config`):

    from agent import build_graph

    graph = build_graph(
        llm_endpoint=LLM_ENDPOINT_NAME,
        system_prompt=AGENT_SYSTEM_PROMPT,
        vs_endpoint=VS_ENDPOINT_NAME,
        vs_index=VS_INDEX_NAME,
        table_actions=TABLE_AGENT_ACTIONS,
    )

    result = graph.invoke({"messages": [("user", "Why is cluster X expensive?")]})
    print(result["messages"][-1].content)
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from databricks_langchain import ChatDatabricks


# ── SQL guard (shared by the two query tools) ────────────────────────────────


def _spark():
    # getActiveSession() is thread-local and returns None if the tool runs on a
    # thread other than the notebook's main thread (LangGraph executes tools off
    # the main thread). builder.getOrCreate() is idempotent in Databricks — it
    # returns the existing JVM-backed session.
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _run_select(sql: str, allowed_prefixes: tuple[str, ...], limit: int = 50) -> dict:
    """Refuses anything that isn't a single SELECT/WITH or that touches a namespace
    outside `allowed_prefixes`."""
    s = sql.strip().rstrip(";").strip()
    head = s.lstrip("(").lstrip().lower()
    if not (head.startswith("select") or head.startswith("with ")):
        return {"error": "Only SELECT / WITH queries are allowed."}
    lower = s.lower()
    if not any(p in lower for p in allowed_prefixes):
        return {"error": f"Query must read from one of: {allowed_prefixes}"}
    for bad in ("insert ", "update ", "delete ", "drop ", "alter ", "merge ", "create "):
        if bad in lower:
            return {"error": f"Disallowed keyword: {bad.strip()}"}
    if " limit " not in lower:
        s = f"SELECT * FROM ({s}) _q LIMIT {limit}"
    try:
        rows = _spark().sql(s).collect()
        return {"row_count": len(rows), "rows": [r.asDict() for r in rows[:limit]]}
    except Exception as e:
        return {"error": str(e)}


# ── Graph factory ────────────────────────────────────────────────────────────


def build_graph(
    llm_endpoint: str,
    system_prompt: str,
    vs_endpoint: str | None = None,
    vs_index: str | None = None,
    table_actions: str | None = None,
    temperature: float = 0.1,
    max_tokens: int = 1500,
):
    """Build a LangGraph ReAct agent over the four IntelliOps tools.

    Knowledge search and action logging are no-ops if their config is not passed,
    so the agent still works in a minimal setup.
    """

    @tool
    def query_features(sql: str) -> dict:
        """Fast path. Read-only SELECT against intelliops.feature_store.* or
        intelliops.report.*. Use for the common questions."""
        return _run_select(
            sql, ("intelliops.feature_store.", "intelliops.report.")
        )

    @tool
    def query_system_tables(sql: str) -> dict:
        """Escape hatch. Read-only SELECT against system.billing.*, system.compute.*,
        or system.lakeflow.*. Use only when the data is not pre-aggregated or
        sub-15-minute freshness is required."""
        return _run_select(
            sql, ("system.billing.", "system.compute.", "system.lakeflow.")
        )

    @tool
    def search_knowledge(query: str, num_results: int = 4) -> dict:
        """Semantic search over IntelliOps's curated knowledge corpus (pricing
        notes, cost-optimization best practices, internal runbooks)."""
        if not vs_endpoint or not vs_index:
            return {"error": "knowledge index not configured"}
        try:
            from databricks.vector_search.client import VectorSearchClient
            vsc = VectorSearchClient(disable_notice=True)
            resp = vsc.get_index(vs_endpoint, vs_index).similarity_search(
                query_text=query,
                columns=["doc_id", "title", "content", "source", "tags"],
                num_results=int(num_results),
            )
            cols = [c["name"] for c in resp.get("manifest", {}).get("columns", [])]
            data = resp.get("result", {}).get("data_array", []) or []
            return {"results": [dict(zip(cols, row)) for row in data]}
        except Exception as e:
            return {"error": str(e)}

    @tool
    def log_action_record(
        skill_name: str,
        action_type: str,
        target_id: str,
        target_name: str,
        description: str,
        projected_savings: float = 0.0,
        status: str = "proposed",
        workspace_id: str = "",
    ) -> dict:
        """Persist a recommendation to the agent action log so it appears on the
        Optimization Leaderboard."""
        if not table_actions:
            return {"error": "action log not configured"}
        action_id = str(uuid.uuid4())
        df = _spark().createDataFrame(
            [(
                action_id,
                datetime.now(timezone.utc),
                skill_name,
                action_type,
                workspace_id or None,
                target_id,
                target_name,
                description,
                float(projected_savings or 0.0),
                status,
                json.dumps({}),
            )],
            "action_id string, action_timestamp timestamp, skill_name string, "
            "action_type string, workspace_id string, target_id string, "
            "target_name string, description string, projected_savings double, "
            "status string, details string",
        )
        df.write.mode("append").saveAsTable(table_actions)
        return {"action_id": action_id, "status": status}

    llm = ChatDatabricks(
        endpoint=llm_endpoint,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    return create_react_agent(
        llm,
        tools=[query_features, query_system_tables, search_knowledge, log_action_record],
        prompt=system_prompt,
    )
