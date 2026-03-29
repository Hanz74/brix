"""Alert handler module."""
from __future__ import annotations


async def _handle_alert_add(arguments: dict) -> dict:
    """Add a new alert rule."""
    from brix.alerting import AlertManager
    from brix.db import BrixDB
    name = arguments.get("name", "")
    condition = arguments.get("condition", "")
    channel = arguments.get("channel", "")
    config = arguments.get("config") or {}
    description = arguments.get("description", "")

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    if not condition:
        return {"success": False, "error": "Parameter 'condition' is required."}
    if not channel:
        return {"success": False, "error": "Parameter 'channel' is required."}

    # Use DB directly to pass org fields
    db = BrixDB()
    try:
        row = db.alert_rule_add(
            name=name, condition=condition, channel=channel, config=config,
            project=org_project, tags=org_tags, group_name=org_group,
        )
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

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
            "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['alert', 'cost'])."
        )

    result: dict = {
        "success": True,
        "rule": {
            "id": row["id"],
            "name": row["name"],
            "condition": row["condition"],
            "channel": row["channel"],
            "config": row["config"],
            "enabled": row["enabled"],
            "created_at": row["created_at"],
            "project": row.get("project", ""),
            "tags": row.get("tags", []),
            "group": row.get("group_name", ""),
        },
    }
    if warnings:
        result["warnings"] = warnings
    return result


async def _handle_alert_list(arguments: dict) -> dict:
    """List all alert rules."""
    from brix.db import BrixDB
    db = BrixDB()
    rows = db.alert_rule_list()
    return {
        "rules": [
            {
                "id": r["id"],
                "name": r["name"],
                "condition": r["condition"],
                "channel": r["channel"],
                "config": r["config"],
                "enabled": r["enabled"],
                "created_at": r["created_at"],
                "project": r.get("project", ""),
                "tags": r.get("tags", []),
                "group": r.get("group_name", ""),
            }
            for r in rows
        ],
        "total": len(rows),
    }


async def _handle_alert_update(arguments: dict) -> dict:
    """Update an existing alert rule."""
    from brix.db import BrixDB
    rule_id = arguments.get("id", "").strip()
    if not rule_id:
        return {"success": False, "error": "Parameter 'id' is required."}

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    db = BrixDB()
    try:
        updated = db.alert_rule_update(
            rule_id=rule_id,
            name=arguments.get("name"),
            condition=arguments.get("condition"),
            channel=arguments.get("channel"),
            config=arguments.get("config"),
            enabled=arguments.get("enabled"),
            project=org_project,
            tags=org_tags,
            group_name=org_group,
        )
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    if updated is None:
        return {"success": False, "error": f"Alert rule '{rule_id}' not found."}

    return {
        "success": True,
        "rule": {
            "id": updated["id"],
            "name": updated["name"],
            "condition": updated["condition"],
            "channel": updated["channel"],
            "config": updated["config"],
            "enabled": updated["enabled"],
            "created_at": updated["created_at"],
            "project": updated.get("project", ""),
            "tags": updated.get("tags", []),
            "group": updated.get("group_name", ""),
        },
    }


async def _handle_alert_delete(arguments: dict) -> dict:
    """Delete an alert rule by ID."""
    from brix.alerting import AlertManager
    rule_id = arguments.get("id", "")
    if not rule_id:
        return {"success": False, "error": "Parameter 'id' is required."}

    mgr = AlertManager()
    deleted = mgr.delete_rule(rule_id)
    if not deleted:
        return {"success": False, "error": f"Alert rule '{rule_id}' not found."}
    return {"success": True, "id": rule_id}


async def _handle_alert_history(arguments: dict) -> dict:
    """Return recent alert history."""
    from brix.alerting import AlertManager
    limit = int(arguments.get("limit", 20))
    mgr = AlertManager()
    history = mgr.get_alert_history(limit=limit)
    return {"history": history, "total": len(history)}
