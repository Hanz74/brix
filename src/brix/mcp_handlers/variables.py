"""Managed Variables and Persistent Data Store handler module (T-BRIX-DB-13)."""
from __future__ import annotations

from brix.db import BrixDB


# ------------------------------------------------------------------
# Managed Variables
# ------------------------------------------------------------------


async def _handle_set_variable(arguments: dict) -> dict:
    """Create or update a managed variable."""
    name = arguments.get("name", "").strip()
    if not name:
        return {"error": "name is required"}
    if "value" not in arguments:
        return {"error": "value is required"}
    value = str(arguments["value"])
    description = arguments.get("description", "")
    secret = bool(arguments.get("secret", False))

    db = BrixDB()
    db.variable_set(name, value, description, secret=secret)
    return {"set": True, "name": name, "secret": secret}


async def _handle_get_variable(arguments: dict) -> dict:
    """Get a managed variable by name.

    For secret variables the value is NOT returned — only metadata.
    """
    name = arguments.get("name", "").strip()
    if not name:
        return {"error": "name is required"}

    db = BrixDB()
    raw = db.variable_get_raw(name)
    if raw is None:
        return {"found": False, "name": name, "value": ""}
    if raw.get("secret"):
        # Do not expose the plaintext value via MCP
        return {
            "found": True,
            "name": name,
            "value": "",
            "secret": True,
            "note": "Secret variable — value not returned via MCP for security.",
        }
    return {"found": True, "name": name, "value": raw["value"], "secret": False}


async def _handle_list_variables(arguments: dict) -> dict:
    """List all managed variables. Secret values shown as '***SECRET***'."""
    db = BrixDB()
    variables = db.variable_list()
    return {"variables": variables, "count": len(variables)}


async def _handle_delete_variable(arguments: dict) -> dict:
    """Delete a managed variable by name."""
    name = arguments.get("name", "").strip()
    if not name:
        return {"error": "name is required"}

    db = BrixDB()
    deleted = db.variable_delete(name)
    return {"deleted": deleted, "name": name}


# ------------------------------------------------------------------
# Persistent Data Store
# ------------------------------------------------------------------


async def _handle_store_set(arguments: dict) -> dict:
    """Create or update a persistent store entry."""
    key = arguments.get("key", "").strip()
    if not key:
        return {"error": "key is required"}
    if "value" not in arguments:
        return {"error": "value is required"}
    value = str(arguments["value"])
    pipeline_name = arguments.get("pipeline_name", "")

    db = BrixDB()
    db.store_set(key, value, pipeline_name)
    return {"set": True, "key": key}


async def _handle_store_get(arguments: dict) -> dict:
    """Get a persistent store entry by key."""
    key = arguments.get("key", "").strip()
    if not key:
        return {"error": "key is required"}

    db = BrixDB()
    value = db.store_get(key)
    if value is None:
        return {"found": False, "key": key, "value": ""}
    return {"found": True, "key": key, "value": value}


async def _handle_store_list(arguments: dict) -> dict:
    """List persistent store entries, optionally filtered by pipeline_name."""
    pipeline_name = arguments.get("pipeline_name") or None
    if pipeline_name:
        pipeline_name = pipeline_name.strip() or None

    db = BrixDB()
    entries = db.store_list(pipeline_name=pipeline_name)
    return {"entries": entries, "count": len(entries)}


async def _handle_store_delete(arguments: dict) -> dict:
    """Delete a persistent store entry by key."""
    key = arguments.get("key", "").strip()
    if not key:
        return {"error": "key is required"}

    db = BrixDB()
    deleted = db.store_delete(key)
    return {"deleted": deleted, "key": key}
