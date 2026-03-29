"""Connection management MCP handlers — T-BRIX-DB-05b."""
from __future__ import annotations

import sqlite3 as _sqlite3


async def _handle_connection_add(arguments: dict) -> dict:
    """Register a named DB connection (DSN encrypted via CredentialStore)."""
    from brix.db import BrixDB
    from brix.connections import ConnectionManager, SUPPORTED_DRIVERS

    name = arguments.get("name", "").strip()
    dsn = arguments.get("dsn", "").strip()
    driver = arguments.get("driver", "postgresql").strip()
    description = arguments.get("description", "")
    env_var = arguments.get("env_var") or None

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}
    if not dsn:
        return {"success": False, "error": "Parameter 'dsn' is required"}
    if driver not in SUPPORTED_DRIVERS:
        return {
            "success": False,
            "error": f"Unsupported driver '{driver}'. Supported: {', '.join(SUPPORTED_DRIVERS)}",
        }

    try:
        db = BrixDB()
        manager = ConnectionManager(db)
        meta = manager.register(
            name, dsn, driver=driver, description=description, env_var=env_var,
            project=org_project, tags=org_tags, group_name=org_group,
        )

        # Org enforcement warnings
        warnings: list[str] = []
        if org_project is None:
            warnings.append(
                "MISSING PROJECT: Bitte 'project' angeben (z.B. 'buddy', 'cody', 'utility')."
            )
        if org_tags is None:
            warnings.append(
                "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['database', 'postgres'])."
            )

        result: dict = {
            "success": True,
            **meta,
            "note": "DSN is encrypted and stored in CredentialStore. It will NOT be shown.",
        }
        if warnings:
            result["warnings"] = warnings
        return result
    except _sqlite3.IntegrityError:
        return {
            "success": False,
            "error": f"A connection named '{name}' already exists. Delete it first or use a different name.",
        }
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_connection_list(arguments: dict) -> dict:
    """List all registered connections (metadata only — no DSN)."""
    from brix.db import BrixDB
    from brix.connections import ConnectionManager

    try:
        db = BrixDB()
        manager = ConnectionManager(db)
        items = manager.list()
        return {
            "success": True,
            "count": len(items),
            "connections": items,
            "note": "DSNs are encrypted and never shown.",
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_connection_test(arguments: dict) -> dict:
    """Test a named connection (ping/connect)."""
    from brix.db import BrixDB
    from brix.connections import ConnectionManager

    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    try:
        db = BrixDB()
        manager = ConnectionManager(db)
        result = manager.test(name)
        return result
    except Exception as exc:
        return {"success": False, "name": name, "error": str(exc)}


async def _handle_connection_delete(arguments: dict) -> dict:
    """Delete a named connection and its encrypted credential."""
    from brix.db import BrixDB
    from brix.connections import ConnectionManager

    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    try:
        db = BrixDB()
        manager = ConnectionManager(db)
        deleted = manager.delete(name)
        if deleted:
            return {"success": True, "deleted": name, "note": "Connection and its encrypted DSN credential have been removed."}
        return {"success": False, "error": f"Connection '{name}' not found"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
