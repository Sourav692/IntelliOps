"""IntelliOps support agent — single-question tool-calling loop.

Uses the Databricks Foundation Model Serving endpoint (OpenAI-compatible). Each
question runs as its own session; every turn is appended to
`intelliops.memory.agent_conversation`.

Expected globals from `%run ../config/config`:
  LLM_ENDPOINT_NAME, AGENT_MAX_ITERATIONS, AGENT_TEMPERATURE, AGENT_MAX_TOKENS,
  AGENT_SYSTEM_PROMPT, TABLE_CONVERSATION, TABLE_AGENT_ACTIONS,
  VS_ENDPOINT_NAME, VS_INDEX_NAME.
"""

from __future__ import annotations

import json
import os
import uuid
import importlib.util
import sys
from typing import Any


def _load_tools_module():
    """Load the tools.py module sitting next to this file, injecting config globals."""
    here = os.path.dirname(__file__)
    spec = importlib.util.spec_from_file_location("ops_agent_tools", os.path.join(here, "tools.py"))
    if spec is None or spec.loader is None:
        raise RuntimeError("could not locate 03_agent/tools.py")
    mod = importlib.util.module_from_spec(spec)
    # Inject config constants tools.py needs.
    for k in (
        "TABLE_AGENT_ACTIONS",
        "TABLE_CONVERSATION",
        "VS_ENDPOINT_NAME",
        "VS_INDEX_NAME",
    ):
        if k in globals():
            setattr(mod, k, globals()[k])
    sys.modules["ops_agent_tools"] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_memory_module():
    here = os.path.dirname(__file__)
    mem_path = os.path.join(here, "..", "05_memory", "memory.py")
    spec = importlib.util.spec_from_file_location("ops_agent_memory", mem_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not locate 05_memory/memory.py")
    mod = importlib.util.module_from_spec(spec)
    for k in ("TABLE_CONVERSATION", "TABLE_AGENT_ACTIONS"):
        if k in globals():
            setattr(mod, k, globals()[k])
    sys.modules["ops_agent_memory"] = mod
    spec.loader.exec_module(mod)
    return mod


def _openai_client():
    """OpenAI-compatible client pointing at the Databricks serving endpoints."""
    from openai import OpenAI
    host = (
        os.environ.get("DATABRICKS_HOST")
        or "https://" + dbutils.notebook.entry_point.getDbutils()  # type: ignore  # noqa: F821
            .notebook().getContext().tags().apply("browserHostName")
    )
    token = (
        os.environ.get("DATABRICKS_TOKEN")
        or dbutils.notebook.entry_point.getDbutils()  # type: ignore  # noqa: F821
            .notebook().getContext().apiToken().get()
    )
    return OpenAI(api_key=token, base_url=f"{host.rstrip('/')}/serving-endpoints")


def ask(
    question: str,
    user_id: str | None = None,
    session_id: str | None = None,
) -> dict:
    """Run one user question through the agent loop.

    Returns:
      {
        "session_id": str,
        "answer": str,
        "tool_calls": list[dict],
        "iterations": int,
      }
    """
    tools_mod = _load_tools_module()
    mem = _load_memory_module()
    client = _openai_client()

    session_id = session_id or str(uuid.uuid4())

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": AGENT_SYSTEM_PROMPT},  # noqa: F821
        {"role": "user", "content": question},
    ]
    mem.log_turn(session_id, "user", question, user_id=user_id)

    tool_calls_made: list[dict] = []
    iterations = 0

    while iterations < AGENT_MAX_ITERATIONS:  # noqa: F821
        iterations += 1
        resp = client.chat.completions.create(
            model=LLM_ENDPOINT_NAME,  # noqa: F821
            messages=messages,
            tools=tools_mod.TOOL_SCHEMAS,
            tool_choice="auto",
            temperature=AGENT_TEMPERATURE,    # noqa: F821
            max_tokens=AGENT_MAX_TOKENS,      # noqa: F821
        )
        msg = resp.choices[0].message
        # Reflect the assistant turn back into the running messages list.
        assistant_turn: dict[str, Any] = {"role": "assistant", "content": msg.content or ""}
        if msg.tool_calls:
            assistant_turn["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                }
                for tc in msg.tool_calls
            ]
        messages.append(assistant_turn)

        if not msg.tool_calls:
            # Final answer.
            mem.log_turn(session_id, "assistant", msg.content or "", user_id=user_id)
            return {
                "session_id": session_id,
                "answer": msg.content or "",
                "tool_calls": tool_calls_made,
                "iterations": iterations,
            }

        # Execute every requested tool call, append results, loop.
        for tc in msg.tool_calls:
            name = tc.function.name
            args_json = tc.function.arguments or "{}"
            result = tools_mod.call_tool(name, args_json)
            tool_calls_made.append({"name": name, "args": args_json, "result": result})

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "name": name,
                "content": json.dumps(result, default=str)[:8000],  # cap context bloat
            })
            mem.log_turn(
                session_id,
                "tool",
                content="",
                tool_name=name,
                tool_args=json.loads(args_json) if args_json else {},
                tool_result=result,
                user_id=user_id,
            )

    # Loop cap hit — return whatever we have.
    return {
        "session_id": session_id,
        "answer": "Stopped before completing — reached the tool-call iteration cap.",
        "tool_calls": tool_calls_made,
        "iterations": iterations,
    }
