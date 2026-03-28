"""Alert handler module."""
from __future__ import annotations


async def _handle_alert_add(arguments: dict) -> dict:
    """Add a new alert rule."""
    from brix.alerting import AlertManager
    name = arguments.get("name", "")
    condition = arguments.get("condition", "")
    channel = arguments.get("channel", "")
    config = arguments.get("config") or {}

    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    if not condition:
        return {"success": False, "error": "Parameter 'condition' is required."}
    if not channel:
        return {"success": False, "error": "Parameter 'channel' is required."}

    mgr = AlertManager()
    try:
        rule = mgr.add_rule(name=name, condition=condition, channel=channel, config=config)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    return {
        "success": True,
        "rule": {
            "id": rule.id,
            "name": rule.name,
            "condition": rule.condition,
            "channel": rule.channel,
            "config": rule.config,
            "enabled": rule.enabled,
            "created_at": rule.created_at,
        },
    }


async def _handle_alert_list(arguments: dict) -> dict:
    """List all alert rules."""
    from brix.alerting import AlertManager
    mgr = AlertManager()
    rules = mgr.list_rules()
    return {
        "rules": [
            {
                "id": r.id,
                "name": r.name,
                "condition": r.condition,
                "channel": r.channel,
                "config": r.config,
                "enabled": r.enabled,
                "created_at": r.created_at,
            }
            for r in rules
        ],
        "total": len(rules),
    }


async def _handle_alert_update(arguments: dict) -> dict:
    """Update an existing alert rule."""
    from brix.alerting import AlertManager
    rule_id = arguments.get("id", "").strip()
    if not rule_id:
        return {"success": False, "error": "Parameter 'id' is required."}

    mgr = AlertManager()
    try:
        updated = mgr.update_rule(
            rule_id=rule_id,
            name=arguments.get("name"),
            condition=arguments.get("condition"),
            channel=arguments.get("channel"),
            config=arguments.get("config"),
            enabled=arguments.get("enabled"),
        )
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    if updated is None:
        return {"success": False, "error": f"Alert rule '{rule_id}' not found."}

    return {
        "success": True,
        "rule": {
            "id": updated.id,
            "name": updated.name,
            "condition": updated.condition,
            "channel": updated.channel,
            "config": updated.config,
            "enabled": updated.enabled,
            "created_at": updated.created_at,
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
