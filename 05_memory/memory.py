"""Memory helpers — conversation history and action log.

Importable from any notebook that has already executed `%run ../config/config`,
so the module references config constants as globals (Databricks pattern).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any


def _spark():
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession()


def _next_turn(session_id: str) -> int:
    row = _spark().sql(
        f"SELECT COALESCE(MAX(turn), -1) + 1 AS next_turn "
        f"FROM {TABLE_CONVERSATION} WHERE session_id = '{session_id}'"  # noqa: F821
    ).first()
    return int(row["next_turn"]) if row else 0


def log_turn(
    session_id: str,
    role: str,
    content: str = "",
    tool_name: str | None = None,
    tool_args: dict | None = None,
    tool_result: Any = None,
    user_id: str | None = None,
) -> int:
    """Append a single turn to the conversation table. Returns the assigned turn index."""
    turn = _next_turn(session_id)
    row = [(
        session_id,
        turn,
        role,
        content or "",
        tool_name,
        json.dumps(tool_args) if tool_args is not None else None,
        json.dumps(tool_result, default=str) if tool_result is not None else None,
        user_id,
        datetime.now(timezone.utc),
    )]
    df = _spark().createDataFrame(
        row,
        "session_id string, turn int, role string, content string, "
        "tool_name string, tool_args string, tool_result string, "
        "user_id string, ts timestamp",
    )
    df.write.mode("append").saveAsTable(TABLE_CONVERSATION)  # noqa: F821
    return turn


def get_conversation(session_id: str, limit: int = 20) -> list[dict]:
    """Most recent `limit` turns for a session, oldest first."""
    rows = _spark().sql(f"""
        SELECT * FROM (
          SELECT * FROM {TABLE_CONVERSATION}
          WHERE session_id = '{session_id}'
          ORDER BY turn DESC
          LIMIT {int(limit)}
        ) ORDER BY turn ASC
    """).collect()  # noqa: F821
    return [r.asDict() for r in rows]


def log_action_record(
    skill_name: str,
    action_type: str,
    target_id: str,
    target_name: str,
    description: str,
    projected_savings: float = 0.0,
    status: str = "proposed",
    workspace_id: str | None = None,
    details: dict | None = None,
) -> str:
    """Append an action to the leaderboard log. Returns the generated action_id."""
    action_id = str(uuid.uuid4())
    row = [(
        action_id,
        datetime.now(timezone.utc),
        skill_name,
        action_type,
        workspace_id,
        target_id,
        target_name,
        description,
        float(projected_savings or 0.0),
        status,
        json.dumps(details or {}, default=str),
    )]
    df = _spark().createDataFrame(
        row,
        "action_id string, action_timestamp timestamp, skill_name string, "
        "action_type string, workspace_id string, target_id string, "
        "target_name string, description string, projected_savings double, "
        "status string, details string",
    )
    df.write.mode("append").saveAsTable(TABLE_AGENT_ACTIONS)  # noqa: F821
    return action_id


def get_recent_actions(
    target_id: str | None = None,
    skill_name: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Recent actions, optionally filtered by target or skill."""
    where = []
    if target_id:
        where.append(f"target_id = '{target_id}'")
    if skill_name:
        where.append(f"skill_name = '{skill_name}'")
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = _spark().sql(f"""
        SELECT * FROM {TABLE_AGENT_ACTIONS}
        {where_clause}
        ORDER BY action_timestamp DESC
        LIMIT {int(limit)}
    """).collect()  # noqa: F821
    return [r.asDict() for r in rows]
