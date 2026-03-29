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

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    db = BrixDB()
    db.variable_set(
        name, value, description, secret=secret,
        project=org_project, tags=org_tags, group_name=org_group,
    )

    result: dict = {"set": True, "name": name, "secret": secret}
    if org_project is not None:
        result["project"] = org_project
    if org_tags is not None:
        result["tags"] = org_tags
    if org_group is not None:
        result["group"] = org_group

    # Org enforcement warnings
    warnings: list[str] = []
    if org_project is None:
        warnings.append(
            "MISSING PROJECT: Bitte 'project' angeben (z.B. 'buddy', 'cody', 'utility')."
        )
    if org_tags is None:
        warnings.append(
            "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['config', 'secret'])."
        )
    if warnings:
        result["warnings"] = warnings
    return result


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

    # T-BRIX-ORG-01: include org fields
    org_fields = {
        "project": raw.get("project", "") or "",
        "tags": raw.get("tags", []),
        "group": raw.get("group_name", "") or "",
    }

    if raw.get("secret"):
        # Do not expose the plaintext value via MCP
        return {
            "found": True,
            "name": name,
            "value": "",
            "secret": True,
            "note": "Secret variable — value not returned via MCP for security.",
            **org_fields,
        }
    return {"found": True, "name": name, "value": raw["value"], "secret": False, **org_fields}


async def _handle_list_variables(arguments: dict) -> dict:
    """List all managed variables. Secret values shown as '***SECRET***'.

    Supports optional project/tags/group filter.
    """
    # T-BRIX-ORG-01: project/tags/group filter
    filter_project = arguments.get("project") or None
    filter_tags = arguments.get("tags") or None
    filter_group = arguments.get("group") or None

    db = BrixDB()
    variables = db.variable_list(
        project=filter_project,
        group_name=filter_group,
        tags=filter_tags,
    )
    # Normalize org fields in response
    for v in variables:
        v.setdefault("project", "")
        v.setdefault("group", v.pop("group_name", "") or "")
        if "group_name" in v:
            v["group"] = v.pop("group_name")
    result: dict = {"variables": variables, "count": len(variables)}
    if filter_project or filter_tags or filter_group:
        result["filter"] = {
            "project": filter_project,
            "tags": filter_tags,
            "group": filter_group,
        }
    return result


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
