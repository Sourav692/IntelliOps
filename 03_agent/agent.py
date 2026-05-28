"""IntelliOps support agent — LangGraph implementation.

State machine:

    ┌──────────┐         tool_calls          ┌──────────┐
    │   llm    │ ─────────────────────────►  │  tools   │
    │  (node)  │ ◄─────────────────────────  │  (node)  │
    └──────────┘         tool results        └──────────┘
         │
         │ no tool_calls
         ▼
        END

Each `llm` invocation increments an iteration counter and bails out at
`AGENT_MAX_ITERATIONS`. Every assistant turn and every tool call is appended to
`intelliops.memory.agent_conversation` for auditability.

Expected globals from `%run ../config/config`:
  LLM_ENDPOINT_NAME, AGENT_MAX_ITERATIONS, AGENT_TEMPERATURE, AGENT_MAX_TOKENS,
  AGENT_SYSTEM_PROMPT, TABLE_CONVERSATION, TABLE_AGENT_ACTIONS,
  VS_ENDPOINT_NAME, VS_INDEX_NAME.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import uuid
from typing import Annotated, Sequence, TypedDict

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from databricks_langchain import ChatDatabricks


# ── Module loaders ────────────────────────────────────────────────────────────
# tools.py and 05_memory/memory.py rely on config constants being present in
# their module globals (Databricks `%run` pattern). We re-create that by loading
# them as files and injecting the constants this module already has.


def _load_module(path: str, alias: str, injected_globals: dict) -> object:
    spec = importlib.util.spec_from_file_location(alias, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {path}")
    mod = importlib.util.module_from_spec(spec)
    for k, v in injected_globals.items():
        setattr(mod, k, v)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


def _here() -> str:
    return os.path.dirname(__file__)


def _injected() -> dict:
    return {
        k: globals()[k]
        for k in (
            "TABLE_AGENT_ACTIONS",
            "TABLE_CONVERSATION",
            "VS_ENDPOINT_NAME",
            "VS_INDEX_NAME",
        )
        if k in globals()
    }


# ── Tool wrappers ────────────────────────────────────────────────────────────


def _build_tools():
    tools_mod = _load_module(os.path.join(_here(), "tools.py"), "ops_agent_tools", _injected())

    @tool
    def query_features(sql: str) -> dict:
        """Fast path. Run a read-only SELECT against the pre-aggregated IntelliOps
        feature tables (intelliops.feature_store.*) or the stable report views
        (intelliops.report.*). Use this for the common case."""
        return tools_mod.query_features(sql)

    @tool
    def query_system_tables(sql: str) -> dict:
        """Escape hatch. Read-only SELECT against system.billing.*, system.compute.*,
        or system.lakeflow.*. Use only when the data is not pre-aggregated or when
        freshness within the last ~15 minutes is required."""
        return tools_mod.query_system_tables(sql)

    @tool
    def search_knowledge(query: str, num_results: int = 4) -> dict:
        """Semantic search over IntelliOps's curated knowledge corpus (pricing
        notes, cost-optimization best practices, internal runbooks). Use when the
        user asks 'why' / 'best practice' / 'how should I'."""
        return tools_mod.search_knowledge(query, num_results)

    @tool
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
        """Persist a recommendation to the agent action log so it appears on the
        Optimization Leaderboard. Call when you have delivered a concrete
        recommendation tied to a specific Databricks resource."""
        return tools_mod.log_action_record(
            skill_name=skill_name,
            action_type=action_type,
            target_id=target_id,
            target_name=target_name,
            description=description,
            projected_savings=projected_savings,
            status=status,
            workspace_id=workspace_id,
        )

    return [query_features, query_system_tables, search_knowledge, log_action_record]


# ── State ────────────────────────────────────────────────────────────────────


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    session_id: str
    user_id: str | None
    iteration: int
    tool_calls_made: list[dict]


# ── Graph build (lazy, cached) ───────────────────────────────────────────────

_graph = None
_memory = None


def _get_memory():
    global _memory
    if _memory is None:
        _memory = _load_module(
            os.path.join(_here(), "..", "05_memory", "memory.py"),
            "ops_agent_memory",
            _injected(),
        )
    return _memory


def _build_graph():
    tools_list = _build_tools()
    tools_by_name = {t.name: t for t in tools_list}

    llm = ChatDatabricks(
        endpoint=LLM_ENDPOINT_NAME,            # noqa: F821
        temperature=AGENT_TEMPERATURE,          # noqa: F821
        max_tokens=AGENT_MAX_TOKENS,            # noqa: F821
    ).bind_tools(tools_list)

    def llm_node(state: AgentState) -> dict:
        if state["iteration"] >= AGENT_MAX_ITERATIONS:  # noqa: F821
            stop_msg = AIMessage(
                content="Stopped — reached the tool-call iteration cap."
            )
            return {"messages": [stop_msg]}

        response = llm.invoke(state["messages"])
        _get_memory().log_turn(
            state["session_id"],
            "assistant",
            response.content or "",
            user_id=state.get("user_id"),
        )
        return {"messages": [response], "iteration": state["iteration"] + 1}

    def tools_node(state: AgentState) -> dict:
        last = state["messages"][-1]
        if not isinstance(last, AIMessage) or not last.tool_calls:
            return {}

        memory = _get_memory()
        outputs: list[ToolMessage] = []
        records: list[dict] = []

        for tc in last.tool_calls:
            name = tc["name"]
            args = tc.get("args") or {}
            fn = tools_by_name.get(name)
            if fn is None:
                result = {"error": f"unknown tool '{name}'"}
            else:
                try:
                    result = fn.invoke(args)
                except Exception as e:
                    result = {"error": str(e)}

            outputs.append(
                ToolMessage(
                    content=json.dumps(result, default=str)[:8000],
                    name=name,
                    tool_call_id=tc["id"],
                )
            )
            records.append({"name": name, "args": json.dumps(args), "result": result})

            memory.log_turn(
                state["session_id"],
                "tool",
                content="",
                tool_name=name,
                tool_args=args,
                tool_result=result,
                user_id=state.get("user_id"),
            )

        return {
            "messages": outputs,
            "tool_calls_made": state.get("tool_calls_made", []) + records,
        }

    def should_continue(state: AgentState) -> str:
        last = state["messages"][-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "tools"
        return END

    graph = StateGraph(AgentState)
    graph.add_node("llm", llm_node)
    graph.add_node("tools", tools_node)
    graph.add_edge(START, "llm")
    graph.add_conditional_edges("llm", should_continue, {"tools": "tools", END: END})
    graph.add_edge("tools", "llm")
    return graph.compile()


# ── Public API (unchanged signature) ─────────────────────────────────────────


def ask(
    question: str,
    user_id: str | None = None,
    session_id: str | None = None,
) -> dict:
    """Run one user question through the LangGraph agent.

    Returns: {session_id, answer, tool_calls, iterations}.
    """
    global _graph
    if _graph is None:
        _graph = _build_graph()

    memory = _get_memory()
    session_id = session_id or str(uuid.uuid4())
    memory.log_turn(session_id, "user", question, user_id=user_id)

    initial: AgentState = {
        "messages": [
            SystemMessage(content=AGENT_SYSTEM_PROMPT),  # noqa: F821
            HumanMessage(content=question),
        ],
        "session_id": session_id,
        "user_id": user_id,
        "iteration": 0,
        "tool_calls_made": [],
    }

    # LangGraph's own recursion guard. AGENT_MAX_ITERATIONS bounds llm-node calls;
    # double-plus-buffer covers the alternating tools node and the final llm step.
    final = _graph.invoke(
        initial,
        config={"recursion_limit": AGENT_MAX_ITERATIONS * 2 + 5},  # noqa: F821
    )

    last = final["messages"][-1]
    answer = getattr(last, "content", str(last))

    return {
        "session_id": session_id,
        "answer": answer or "",
        "tool_calls": final.get("tool_calls_made", []),
        "iterations": final.get("iteration", 0),
    }
