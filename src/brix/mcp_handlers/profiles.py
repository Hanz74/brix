"""Profile / Mixin MCP handlers — T-BRIX-DB-23."""
from __future__ import annotations

import json as _json


async def _handle_create_profile(arguments: dict) -> dict:
    """Create or update a named profile (config defaults for steps)."""
    from brix.db import BrixDB

    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    config = arguments.get("config")
    if config is None:
        return {"success": False, "error": "Parameter 'config' is required"}
    if isinstance(config, str):
        try:
            config = _json.loads(config)
        except _json.JSONDecodeError as exc:
            return {"success": False, "error": f"Invalid JSON in 'config': {exc}"}
    if not isinstance(config, dict):
        return {"success": False, "error": "Parameter 'config' must be a JSON object"}

    description = arguments.get("description", "")

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    try:
        db = BrixDB()
        profile = db.profile_set(
            name, config, description,
            project=org_project, tags=org_tags, group_name=org_group,
        )

        # Org enforcement warnings
        warnings: list[str] = []
        if org_project is None:
            warnings.append(
                "MISSING PROJECT: Bitte 'project' angeben (z.B. 'buddy', 'cody', 'utility')."
            )
        if not description:
            warnings.append(
                "MISSING DESCRIPTION: Bitte 'description' angeben."
            )
        if org_tags is None:
            warnings.append(
                "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['config', 'defaults'])."
            )

        result: dict = {"success": True, **profile}
        if warnings:
            result["warnings"] = warnings
        return result
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_get_profile(arguments: dict) -> dict:
    """Get a profile by name."""
    from brix.db import BrixDB

    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    try:
        db = BrixDB()
        profile = db.profile_get(name)
        if profile is None:
            return {"success": False, "error": f"Profile '{name}' not found"}
        return {"success": True, **profile}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_list_profiles(arguments: dict) -> dict:
    """List all profiles."""
    from brix.db import BrixDB

    try:
        db = BrixDB()
        profiles = db.profile_list()
        return {"success": True, "count": len(profiles), "profiles": profiles}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_update_profile(arguments: dict) -> dict:
    """Update an existing profile's config, description, and/or org fields."""
    from brix.db import BrixDB

    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    try:
        db = BrixDB()
        existing = db.profile_get(name)
        if existing is None:
            return {"success": False, "error": f"Profile '{name}' not found"}

        config = arguments.get("config", existing["config"])
        if isinstance(config, str):
            try:
                config = _json.loads(config)
            except _json.JSONDecodeError as exc:
                return {"success": False, "error": f"Invalid JSON in 'config': {exc}"}

        description = arguments.get("description", existing.get("description", ""))
        profile = db.profile_set(
            name, config, description,
            project=org_project, tags=org_tags, group_name=org_group,
        )
        return {"success": True, **profile}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_delete_profile(arguments: dict) -> dict:
    """Delete a profile by name."""
    from brix.db import BrixDB

    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    try:
        db = BrixDB()
        deleted = db.profile_delete(name)
        if not deleted:
            return {"success": False, "error": f"Profile '{name}' not found"}
        return {"success": True, "deleted": name}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_search_profiles(arguments: dict) -> dict:
    """Search profiles by name or description substring."""
    from brix.db import BrixDB

    query = arguments.get("query", "").strip()
    if not query:
        return {"success": False, "error": "Parameter 'query' is required"}

    try:
        db = BrixDB()
        all_profiles = db.profile_list()
        q_lower = query.lower()
        matches = [
            p for p in all_profiles
            if q_lower in p.get("name", "").lower()
            or q_lower in p.get("description", "").lower()
        ]
        return {"success": True, "query": query, "profiles": matches, "total": len(matches)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
