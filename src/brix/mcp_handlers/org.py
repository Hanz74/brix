"""Org Registry handler — CRUD for known projects, tags, and groups (T-BRIX-ORG-02)."""
from __future__ import annotations


async def _handle_org(arguments: dict) -> dict:
    """Consolidated CRUD for org definitions: projects, tags, groups.

    Actions:
      create — add a new project/tag/group definition
      list   — list all or filtered by type
      delete — remove a definition
      seed   — seed built-in defaults (idempotent)

    Parameters:
      action      str  — 'create' | 'list' | 'delete' | 'seed'
      type        str  — 'project' | 'tag' | 'group'  (required for create/delete)
      name        str  — required for create/delete
      description str  — optional for create
      pipelines   list — optional for group: list of pipeline names in this group
    """
    from brix.db import BrixDB

    action = (arguments.get("action") or "list").lower().strip()
    entry_type = (arguments.get("type") or "").lower().strip()
    name = (arguments.get("name") or "").strip()
    description = arguments.get("description") or ""
    db = BrixDB()

    if action == "seed":
        try:
            db.org_registry_seed_defaults()
            entries = db.org_registry_list()
            return {
                "success": True,
                "action": "seed",
                "seeded": len(entries),
                "message": "Default projects, tags, and groups have been seeded.",
            }
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    if action == "list":
        try:
            if entry_type and entry_type not in ("project", "tag", "group"):
                return {
                    "success": False,
                    "error": f"Unknown type '{entry_type}'. Valid types: project, tag, group",
                }
            rows = db.org_registry_list(entry_type=entry_type or None)
            # Group by type for readability
            grouped: dict[str, list] = {}
            for r in rows:
                et = r["entry_type"]
                grouped.setdefault(et, []).append({
                    "name": r["name"],
                    "description": r["description"],
                    "metadata": r["metadata"],
                })
            return {
                "success": True,
                "action": "list",
                "entries": rows,
                "grouped": grouped,
                "total": len(rows),
            }
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    if action == "create":
        if entry_type not in ("project", "tag", "group"):
            return {
                "success": False,
                "error": "Parameter 'type' must be 'project', 'tag', or 'group'",
            }
        if not name:
            return {"success": False, "error": "Parameter 'name' is required for create"}
        metadata: dict = {}
        if entry_type == "group":
            pipelines = arguments.get("pipelines") or []
            if pipelines:
                metadata["pipelines"] = pipelines
        try:
            eid = db.org_registry_upsert(entry_type, name, description, metadata)
            return {
                "success": True,
                "action": "create",
                "type": entry_type,
                "name": name,
                "id": eid,
            }
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    if action == "delete":
        if entry_type not in ("project", "tag", "group"):
            return {
                "success": False,
                "error": "Parameter 'type' must be 'project', 'tag', or 'group'",
            }
        if not name:
            return {"success": False, "error": "Parameter 'name' is required for delete"}
        try:
            deleted = db.org_registry_delete(entry_type, name)
            if deleted:
                return {"success": True, "action": "delete", "type": entry_type, "name": name}
            return {"success": False, "error": f"Entry '{entry_type}/{name}' not found"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    return {
        "success": False,
        "error": f"Unknown action '{action}'. Valid actions: create, list, delete, seed",
    }
