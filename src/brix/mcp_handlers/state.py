"""State and agent context handler module."""
from __future__ import annotations

from brix.db import BrixDB


async def _handle_save_agent_context(arguments: dict) -> dict:
    """Save agent session context to DB (V6-10)."""
    session_id = arguments.get("session_id", "").strip()
    summary = arguments.get("summary", "").strip()
    if not session_id:
        return {"error": "session_id is required"}
    if not summary:
        return {"error": "summary is required"}

    active_pipeline = arguments.get("active_pipeline")
    last_run_id = arguments.get("last_run_id")
    pending_decisions = arguments.get("pending_decisions", [])
    if not isinstance(pending_decisions, list):
        pending_decisions = []

    db = BrixDB()
    db.save_agent_context(
        session_id=session_id,
        summary=summary,
        active_pipeline=active_pipeline or None,
        last_run_id=last_run_id or None,
        pending_decisions=pending_decisions,
    )
    ctx = db.restore_agent_context(session_id)
    return {
        "saved": True,
        "session_id": session_id,
        "updated_at": ctx["updated_at"] if ctx else None,
    }


async def _handle_restore_agent_context(arguments: dict) -> dict:
    """Restore agent session context from DB (V6-10)."""
    session_id = arguments.get("session_id", "").strip()
    if not session_id:
        return {"error": "session_id is required"}

    db = BrixDB()
    ctx = db.restore_agent_context(session_id)
    if ctx is None:
        return {"found": False, "session_id": session_id}
    return {
        "found": True,
        "session_id": ctx["session_id"],
        "summary": ctx["summary"],
        "active_pipeline": ctx["active_pipeline"],
        "last_run_id": ctx["last_run_id"],
        "pending_decisions": ctx["pending_decisions"],
        "updated_at": ctx["updated_at"],
    }


async def _handle_state_set(arguments: dict) -> dict:
    """Set a key in the shared blackboard (V6-12)."""
    key = arguments.get("key", "").strip()
    if not key:
        return {"error": "key is required"}
    if "value" not in arguments:
        return {"error": "value is required"}

    db = BrixDB()
    db.state_set(key, arguments["value"])
    entries = db.state_list(prefix=None)
    updated_at = next((e["updated_at"] for e in entries if e["key"] == key), None)
    return {"key": key, "updated_at": updated_at}


async def _handle_state_get(arguments: dict) -> dict:
    """Get a value from the shared blackboard (V6-12)."""
    key = arguments.get("key", "").strip()
    if not key:
        return {"error": "key is required"}

    db = BrixDB()
    result = db.state_get(key)
    if result is None:
        return {"key": key, "found": False}
    # Get updated_at too
    entries = db.state_list(prefix=None)
    entry = next((e for e in entries if e["key"] == key), None)
    return {
        "key": key,
        "found": True,
        "value": result,
        "updated_at": entry["updated_at"] if entry else None,
    }


async def _handle_state_list(arguments: dict) -> dict:
    """List all entries in the shared blackboard (V6-12)."""
    prefix = arguments.get("prefix") or None
    if prefix:
        prefix = prefix.strip() or None

    db = BrixDB()
    entries = db.state_list(prefix=prefix)
    return {"entries": entries, "count": len(entries)}


async def _handle_state_delete(arguments: dict) -> dict:
    """Delete a key from the shared blackboard (V6-12)."""
    key = arguments.get("key", "").strip()
    if not key:
        return {"error": "key is required"}

    db = BrixDB()
    deleted = db.state_delete(key)
    return {"deleted": deleted, "key": key}
