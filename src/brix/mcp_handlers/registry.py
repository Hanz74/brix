"""Knowledge registry handler module."""
from __future__ import annotations

from brix.db import BrixDB as _BrixDB


def _get_db() -> "_BrixDB":
    """Return a BrixDB instance, respecting monkeypatching on brix.mcp_server.BrixDB.

    Looks up BrixDB via brix.mcp_server at call-time so that
    monkeypatch.setattr('brix.mcp_server.BrixDB', ...) in tests takes effect
    even though this handler lives in a sub-module.
    """
    import sys as _sys
    mcp_mod = _sys.modules.get("brix.mcp_server")
    cls = getattr(mcp_mod, "BrixDB", None) if mcp_mod is not None else None
    if cls is None:
        cls = _BrixDB
    return cls()


async def _handle_registry_add(arguments: dict) -> dict:
    """Add a new entry to a knowledge registry."""
    registry_type = arguments.get("registry_type", "").strip()
    name = arguments.get("name", "").strip()
    content = arguments.get("content")
    tags = arguments.get("tags", [])
    description = arguments.get("description", "")

    # T-BRIX-ORG-01: project/group support
    org_project = arguments.get("project") or None
    org_group = arguments.get("group") or None

    if not registry_type:
        return {"success": False, "error": "Parameter 'registry_type' is required"}
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}
    if content is None:
        return {"success": False, "error": "Parameter 'content' is required"}

    try:
        db = _get_db()
        entry_id = db.registry_add(
            registry_type=registry_type,
            name=name,
            content=content,
            tags=tags if isinstance(tags, list) else [],
            description=description,
            project=org_project,
            group_name=org_group,
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
        if not tags:
            warnings.append(
                "HINT: 'tags' helfen bei der Kategorisierung."
            )

        result: dict = {
            "success": True,
            "id": entry_id,
            "name": name,
            "registry_type": registry_type,
        }
        if org_project is not None:
            result["project"] = org_project
        if org_group is not None:
            result["group"] = org_group
        if warnings:
            result["warnings"] = warnings
        return result
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        import sqlite3 as _sqlite3
        if isinstance(exc, _sqlite3.IntegrityError):
            return {
                "success": False,
                "error": f"An entry named '{name}' already exists in '{registry_type}'. Use registry_update to modify it.",
            }
        return {"success": False, "error": str(exc)}


async def _handle_registry_get(arguments: dict) -> dict:
    """Get a registry entry by name or id."""
    registry_type = arguments.get("registry_type", "").strip()
    name_or_id = arguments.get("name_or_id", "").strip()

    if not registry_type:
        return {"success": False, "error": "Parameter 'registry_type' is required"}
    if not name_or_id:
        return {"success": False, "error": "Parameter 'name_or_id' is required"}

    try:
        db = _get_db()
        entry = db.registry_get(registry_type, name_or_id)
        if entry is None:
            return {
                "success": False,
                "error": f"Entry '{name_or_id}' not found in registry '{registry_type}'",
            }
        # T-BRIX-ORG-01: normalize org field names for response
        if "group_name" in entry:
            entry["group"] = entry.pop("group_name")
        return {"success": True, "entry": entry}
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_registry_list(arguments: dict) -> dict:
    """List all entries in a registry, optionally filtered by tag."""
    registry_type = arguments.get("registry_type", "").strip()
    tag_filter = arguments.get("tag_filter")

    if not registry_type:
        return {"success": False, "error": "Parameter 'registry_type' is required"}

    try:
        db = _get_db()
        entries = db.registry_list(registry_type, tag_filter=tag_filter)
        # T-BRIX-ORG-01: normalize org field names for response
        for entry in entries:
            if "group_name" in entry:
                entry["group"] = entry.pop("group_name")
        return {
            "success": True,
            "registry_type": registry_type,
            "count": len(entries),
            "tag_filter": tag_filter,
            "entries": entries,
        }
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_registry_update(arguments: dict) -> dict:
    """Update an existing registry entry (partial update)."""
    registry_type = arguments.get("registry_type", "").strip()
    name_or_id = arguments.get("name_or_id", "").strip()
    content = arguments.get("content")
    tags = arguments.get("tags")
    description = arguments.get("description")

    # T-BRIX-ORG-01: project/group support
    org_project = arguments.get("project") or None
    org_group = arguments.get("group") or None

    if not registry_type:
        return {"success": False, "error": "Parameter 'registry_type' is required"}
    if not name_or_id:
        return {"success": False, "error": "Parameter 'name_or_id' is required"}
    if content is None and tags is None and description is None and org_project is None and org_group is None:
        return {"success": False, "error": "At least one of 'content', 'tags', 'description', 'project', or 'group' must be provided"}

    try:
        db = _get_db()
        updated = db.registry_update(
            registry_type=registry_type,
            name_or_id=name_or_id,
            content=content,
            tags=tags,
            description=description,
            project=org_project,
            group_name=org_group,
        )
        if updated is None:
            return {
                "success": False,
                "error": f"Entry '{name_or_id}' not found in registry '{registry_type}'",
            }
        # T-BRIX-ORG-01: normalize org field names for response
        if "group_name" in updated:
            updated["group"] = updated.pop("group_name")
        return {"success": True, "entry": updated}
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_registry_delete(arguments: dict) -> dict:
    """Delete a registry entry by name or id."""
    registry_type = arguments.get("registry_type", "").strip()
    name_or_id = arguments.get("name_or_id", "").strip()

    if not registry_type:
        return {"success": False, "error": "Parameter 'registry_type' is required"}
    if not name_or_id:
        return {"success": False, "error": "Parameter 'name_or_id' is required"}

    try:
        db = _get_db()
        deleted = db.registry_delete(registry_type, name_or_id)
        if not deleted:
            return {
                "success": False,
                "error": f"Entry '{name_or_id}' not found in registry '{registry_type}'",
            }
        return {"success": True, "deleted": name_or_id, "registry_type": registry_type}
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_registry_search(arguments: dict) -> dict:
    """Search across all or specific registries by keyword."""
    query = arguments.get("query", "").strip()
    registry_types = arguments.get("registry_types")

    if not query:
        return {"success": False, "error": "Parameter 'query' is required"}

    try:
        db = _get_db()
        results = db.registry_search(
            query=query,
            registry_types=registry_types if isinstance(registry_types, list) else None,
        )
        return {
            "success": True,
            "query": query,
            "count": len(results),
            "results": results,
        }
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
