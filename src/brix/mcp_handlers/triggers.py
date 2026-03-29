"""Trigger and scheduler handler module."""
from __future__ import annotations

# In-process scheduler state (per-MCP-server-process)
_scheduler_task: "asyncio.Task | None" = None
_scheduler_running: bool = False


async def _handle_trigger_add(arguments: dict) -> dict:
    """Add a new trigger."""
    from brix.triggers.store import TriggerStore
    name = arguments.get("name", "").strip()
    trigger_type = arguments.get("type", "").strip()
    pipeline = arguments.get("pipeline", "").strip()

    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    if not trigger_type:
        return {"success": False, "error": "Parameter 'type' is required."}
    if not pipeline:
        return {"success": False, "error": "Parameter 'pipeline' is required."}

    config = arguments.get("config") or {}
    enabled = arguments.get("enabled", True)

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    store = TriggerStore()
    try:
        trigger = store.add(
            name=name,
            type=trigger_type,
            pipeline=pipeline,
            config=config,
            enabled=bool(enabled),
            project=org_project,
            tags=org_tags,
            group_name=org_group,
        )
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    # Org enforcement warnings
    warnings: list[str] = []
    if org_project is None:
        warnings.append(
            "MISSING PROJECT: Bitte 'project' angeben (z.B. 'buddy', 'cody', 'utility')."
        )
    if org_tags is None:
        warnings.append(
            "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['email', 'trigger'])."
        )

    result: dict = {"success": True, "trigger": trigger}
    if warnings:
        result["warnings"] = warnings
    return result


async def _handle_trigger_list(arguments: dict) -> dict:
    """List all triggers."""
    from brix.triggers.store import TriggerStore
    store = TriggerStore()
    triggers = store.list_all()
    return {"triggers": triggers, "total": len(triggers)}


async def _handle_trigger_get(arguments: dict) -> dict:
    """Get a trigger by name."""
    from brix.triggers.store import TriggerStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    store = TriggerStore()
    trigger = store.get(name)
    if trigger is None:
        return {"success": False, "error": f"Trigger '{name}' not found."}
    return {"success": True, "trigger": trigger}


async def _handle_trigger_update(arguments: dict) -> dict:
    """Update a trigger's config, enabled state, pipeline, or org fields."""
    from brix.triggers.store import TriggerStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    store = TriggerStore()
    updated = store.update(
        name=name,
        config=arguments.get("config"),
        enabled=arguments.get("enabled"),
        pipeline=arguments.get("pipeline"),
        project=org_project,
        tags=org_tags,
        group_name=org_group,
    )
    if updated is None:
        return {"success": False, "error": f"Trigger '{name}' not found."}
    return {"success": True, "trigger": updated}


async def _handle_trigger_delete(arguments: dict) -> dict:
    """Delete a trigger by name."""
    from brix.triggers.store import TriggerStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    store = TriggerStore()
    deleted = store.delete(name)
    if not deleted:
        return {"success": False, "error": f"Trigger '{name}' not found."}
    return {"success": True, "name": name}


async def _handle_trigger_test(arguments: dict) -> dict:
    """Manually fire a trigger once."""
    from brix.triggers.store import TriggerStore
    from brix.triggers.models import TriggerConfig
    from brix.triggers.state import TriggerState
    from brix.triggers.runners import TRIGGER_RUNNERS

    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    store = TriggerStore()
    trigger_data = store.get(name)
    if trigger_data is None:
        return {"success": False, "error": f"Trigger '{name}' not found."}

    # Build TriggerConfig from stored data
    config = trigger_data.get("config", {})
    tc = TriggerConfig(
        id=trigger_data["id"],
        type=trigger_data["type"],
        pipeline=trigger_data["pipeline"],
        enabled=trigger_data.get("enabled", True),
        filter=config if trigger_data["type"] in ("mail", "pipeline_done") else {},
        path=config.get("path"),
        pattern=config.get("pattern"),
        url=config.get("url"),
        headers=config.get("headers", {}),
        hash_field=config.get("hash_field"),
        status=config.get("status"),
        pipeline_target=config.get("pipeline"),
        interval=config.get("interval", "5m"),
    )

    state = TriggerState()
    runner_class = TRIGGER_RUNNERS.get(tc.type)
    if runner_class is None:
        return {"success": False, "error": f"Unknown trigger type '{tc.type}'."}

    runner = runner_class(tc, state)
    try:
        events = await runner.poll()
        new_events = runner.dedupe(events)
        results = []
        for event in new_events:
            run_result = await runner.fire(event)
            results.append({
                "event": event,
                "run_id": run_result.run_id if run_result else None,
                "success": run_result.success if run_result else False,
            })
        # Update last_fired_at in store
        if results:
            store.record_fired(
                name,
                run_id=results[-1].get("run_id"),
                status="success" if results[-1].get("success") else "failure",
            )
        return {
            "success": True,
            "events_found": len(events),
            "events_fired": len(new_events),
            "results": results,
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_scheduler_status(arguments: dict) -> dict:
    """Return scheduler status."""
    from brix.triggers.store import TriggerStore
    store = TriggerStore()
    triggers = store.list_all()
    enabled = [t for t in triggers if t.get("enabled")]
    return {
        "running": _scheduler_running,
        "trigger_count": len(triggers),
        "enabled_count": len(enabled),
        "note": (
            "The Brix scheduler runs in-process. "
            "Use brix__scheduler_start/stop to control it in the current MCP server process."
        ),
    }


async def _auto_start_scheduler_if_needed() -> None:
    """Auto-start the scheduler on server startup if enabled triggers exist (T-BRIX-V6-BUG-01)."""
    import logging
    logger = logging.getLogger(__name__)
    try:
        from brix.triggers.store import TriggerStore
        store = TriggerStore()
        triggers = store.list_all()
        enabled = [t for t in triggers if t.get("enabled")]
        if enabled:
            logger.info(
                "Auto-starting scheduler: %d enabled trigger(s) found.", len(enabled)
            )
            await _handle_scheduler_start({})
    except Exception as exc:  # noqa: BLE001
        logger.warning("Auto-start scheduler failed: %s", exc)


async def _handle_scheduler_start(arguments: dict) -> dict:
    """Start the in-process trigger scheduler."""
    global _scheduler_task, _scheduler_running

    from brix.triggers.store import TriggerStore
    store = TriggerStore()
    triggers = store.list_all()
    enabled = [t for t in triggers if t.get("enabled")]

    if _scheduler_running:
        return {
            "success": True,
            "status": "already_running",
            "enabled_triggers": len(enabled),
        }

    if not enabled:
        return {
            "success": False,
            "status": "no_enabled_triggers",
            "error": "No enabled triggers configured. Add triggers with brix__trigger_add first.",
        }

    _scheduler_running = True

    return {
        "success": True,
        "status": "started",
        "enabled_triggers": len(enabled),
        "note": (
            "Scheduler started in background. "
            "Use brix__scheduler_status to check status."
        ),
    }


async def _handle_scheduler_stop(arguments: dict) -> dict:
    """Stop the in-process trigger scheduler."""
    global _scheduler_running

    if not _scheduler_running:
        return {"success": True, "status": "already_stopped"}

    _scheduler_running = False
    return {"success": True, "status": "stopped"}


async def _handle_trigger_group_add(arguments: dict) -> dict:
    """Add a new trigger group."""
    from brix.triggers.store import TriggerGroupStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    triggers = arguments.get("triggers", [])
    if not isinstance(triggers, list):
        return {"success": False, "error": "Parameter 'triggers' must be a list of trigger names."}
    description = arguments.get("description", "")
    enabled = arguments.get("enabled", True)

    # T-BRIX-ORG-01: project/tags/group support
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    store = TriggerGroupStore()
    try:
        group = store.add(
            name=name,
            triggers=triggers,
            description=description,
            enabled=bool(enabled),
            project=org_project,
            tags=org_tags,
            group_name=org_group,
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
            "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['trigger', 'group'])."
        )

    result: dict = {"success": True, "group": group}
    if warnings:
        result["warnings"] = warnings
    return result


async def _handle_trigger_group_list(arguments: dict) -> dict:
    """List all trigger groups."""
    from brix.triggers.store import TriggerGroupStore
    store = TriggerGroupStore()
    groups = store.list_all()
    return {"groups": groups, "total": len(groups)}


async def _handle_trigger_group_delete(arguments: dict) -> dict:
    """Delete a trigger group by name."""
    from brix.triggers.store import TriggerGroupStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    store = TriggerGroupStore()
    deleted = store.delete(name)
    if not deleted:
        return {"success": False, "error": f"Trigger group '{name}' not found."}
    return {"success": True, "name": name}


async def _handle_trigger_group_get(arguments: dict) -> dict:
    """Get a trigger group by name including its triggers."""
    from brix.triggers.store import TriggerGroupStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    store = TriggerGroupStore()
    group = store.get(name)
    if group is None:
        return {"success": False, "error": f"Trigger group '{name}' not found."}
    return {"success": True, "group": group}


async def _handle_trigger_group_update(arguments: dict) -> dict:
    """Update a trigger group: name, description, project/tags/group."""
    from brix.triggers.store import TriggerGroupStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    store = TriggerGroupStore()
    updated = store.update(
        name=name,
        triggers=arguments.get("triggers"),
        description=arguments.get("description"),
        enabled=arguments.get("enabled"),
        project=org_project,
        tags=org_tags,
        group_name=org_group,
    )
    if updated is None:
        return {"success": False, "error": f"Trigger group '{name}' not found."}

    warnings: list[str] = []
    if org_project is None and not updated.get("project"):
        warnings.append(
            "MISSING PROJECT: Bitte 'project' angeben (z.B. 'buddy', 'cody', 'utility')."
        )

    result: dict = {"success": True, "group": updated}
    if warnings:
        result["warnings"] = warnings
    return result


async def _handle_search_trigger_groups(arguments: dict) -> dict:
    """Search trigger groups by name or description substring."""
    from brix.triggers.store import TriggerGroupStore
    query = arguments.get("query", "").strip()
    if not query:
        return {"success": False, "error": "Parameter 'query' is required."}
    store = TriggerGroupStore()
    all_groups = store.list_all()
    q_lower = query.lower()
    matches = [
        g for g in all_groups
        if q_lower in g.get("name", "").lower()
        or q_lower in g.get("description", "").lower()
    ]
    return {"success": True, "query": query, "groups": matches, "total": len(matches)}


async def _handle_search_triggers(arguments: dict) -> dict:
    """Search triggers by name, pipeline, or type substring."""
    from brix.triggers.store import TriggerStore
    query = arguments.get("query", "").strip()
    if not query:
        return {"success": False, "error": "Parameter 'query' is required."}
    store = TriggerStore()
    all_triggers = store.list_all()
    q_lower = query.lower()
    matches = [
        t for t in all_triggers
        if q_lower in t.get("name", "").lower()
        or q_lower in t.get("pipeline", "").lower()
        or q_lower in t.get("type", "").lower()
    ]
    return {"success": True, "query": query, "triggers": matches, "total": len(matches)}


async def _handle_trigger_group_start(arguments: dict) -> dict:
    """Enable all triggers in a group."""
    from brix.triggers.store import TriggerGroupStore, TriggerStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    group_store = TriggerGroupStore()
    group = group_store.get(name)
    if group is None:
        return {"success": False, "error": f"Trigger group '{name}' not found."}

    trigger_store = TriggerStore()
    enabled_triggers = []
    not_found = []
    for trigger_name in group["triggers"]:
        result = trigger_store.update(trigger_name, enabled=True)
        if result is None:
            not_found.append(trigger_name)
        else:
            enabled_triggers.append(trigger_name)

    # Mark group as enabled
    group_store.update(name, enabled=True)

    return {
        "success": True,
        "group": name,
        "enabled": enabled_triggers,
        "not_found": not_found,
    }


async def _handle_trigger_group_stop(arguments: dict) -> dict:
    """Disable all triggers in a group."""
    from brix.triggers.store import TriggerGroupStore, TriggerStore
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    group_store = TriggerGroupStore()
    group = group_store.get(name)
    if group is None:
        return {"success": False, "error": f"Trigger group '{name}' not found."}

    trigger_store = TriggerStore()
    disabled_triggers = []
    not_found = []
    for trigger_name in group["triggers"]:
        result = trigger_store.update(trigger_name, enabled=False)
        if result is None:
            not_found.append(trigger_name)
        else:
            disabled_triggers.append(trigger_name)

    # Mark group as disabled
    group_store.update(name, enabled=False)

    return {
        "success": True,
        "group": name,
        "disabled": disabled_triggers,
        "not_found": not_found,
    }
