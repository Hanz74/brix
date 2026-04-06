"""Trigger and scheduler handler module.

T-BRIX-SCHED-02: Scheduler start/stop/status now controls TriggerService
(DB-backed triggers) instead of the old YAML-based BrixScheduler.
"""
from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

# In-process scheduler state (per-MCP-server-process)
_scheduler_task: "asyncio.Task | None" = None
_scheduler_running: bool = False


def _on_scheduler_done(task: asyncio.Task) -> None:
    """Handle scheduler task termination."""
    global _scheduler_task, _scheduler_running

    if task.cancelled():
        if task is _scheduler_task:
            _scheduler_task = None
        return

    exc = task.exception()
    if exc is not None:
        logger.error("Trigger scheduler task crashed: %s", exc)
        if task is _scheduler_task:
            _scheduler_task = None
        if _scheduler_running:
            asyncio.create_task(_restart_scheduler_after_delay(), name="brix-trigger-service-restart")
        return

    if task is _scheduler_task:
        _scheduler_task = None
    _scheduler_running = False


def _start_scheduler_task() -> asyncio.Task:
    """Create and register the background TriggerService task."""
    global _scheduler_task

    from brix.triggers.service import TriggerService

    svc = TriggerService()
    _scheduler_task = asyncio.create_task(svc.start(), name="brix-trigger-service")
    _scheduler_task.add_done_callback(_on_scheduler_done)
    return _scheduler_task


async def _restart_scheduler_after_delay(delay_seconds: float = 10.0) -> None:
    """Restart the scheduler after a crash if it is still meant to run."""
    global _scheduler_task

    await asyncio.sleep(delay_seconds)
    if not _scheduler_running:
        return
    if _scheduler_task is not None and not _scheduler_task.done():
        return
    _start_scheduler_task()


# ---------------------------------------------------------------------------
# T-BRIX-SCHED-02: schedules.yaml -> DB trigger migration
# ---------------------------------------------------------------------------

def interval_to_cron(interval_str: str) -> str:
    """Convert an interval string (e.g. '6h', '30m', 'daily') to a cron expression.

    Supported formats: Nh, Nm, Nd, 'daily', 'hourly'.
    """
    s = interval_str.strip().lower()
    if s == "daily":
        return "0 0 * * *"
    if s == "hourly":
        return "0 * * * *"
    try:
        if s.endswith("h"):
            hours = int(float(s[:-1]))
            if hours <= 0:
                return "0 * * * *"
            if hours >= 24:
                return "0 0 * * *"
            return f"0 */{hours} * * *"
        if s.endswith("m"):
            minutes = int(float(s[:-1]))
            if minutes <= 0:
                return "* * * * *"
            if minutes >= 60:
                hours = minutes // 60
                return f"0 */{hours} * * *" if hours < 24 else "0 0 * * *"
            return f"*/{minutes} * * * *"
        if s.endswith("d"):
            return "0 0 * * *"
    except (ValueError, TypeError):
        pass
    # Default: daily
    return "0 0 * * *"


def migrate_schedules_yaml() -> list[dict]:
    """Migrate schedules.yaml entries to DB triggers (idempotent).

    Returns a list of dicts describing what was migrated.
    """
    from pathlib import Path

    schedules_path = Path.home() / ".brix" / "schedules.yaml"
    if not schedules_path.exists():
        return []

    import yaml
    with open(schedules_path) as f:
        data = yaml.safe_load(f) or {}

    schedules = data.get("schedules", [])
    if not schedules:
        return []

    from brix.triggers.store import TriggerStore
    store = TriggerStore()
    results = []

    for sched in schedules:
        name = sched.get("name", "")
        if not name:
            continue

        # Skip if trigger with this name already exists
        existing = store.get(name)
        if existing is not None:
            results.append({"name": name, "action": "skipped", "reason": "already exists"})
            continue

        pipeline = sched.get("pipeline", "")
        interval = sched.get("interval", "24h")
        cron_expr = interval_to_cron(interval)
        enabled = sched.get("enabled", True)
        project = sched.get("project") or None
        tags = sched.get("tags") or None
        group = sched.get("group") or None
        description = sched.get("description", "")
        params = sched.get("params") or {}

        config = {
            "cron": cron_expr,
            "params": params,
            "migrated_from": "schedules.yaml",
            "original_interval": interval,
        }

        try:
            trigger = store.add(
                name=name,
                type="schedule",
                pipeline=pipeline,
                config=config,
                enabled=bool(enabled),
                project=project,
                tags=tags,
                group_name=group,
            )
            # Set description via update (add() doesn't support it directly)
            if description:
                store.update(name, description=description)
            results.append({"name": name, "action": "created", "cron": cron_expr, "pipeline": pipeline})
            logger.info("Migrated schedule '%s' -> DB trigger (cron=%s, pipeline=%s)", name, cron_expr, pipeline)
        except Exception as exc:
            results.append({"name": name, "action": "error", "error": str(exc)})
            logger.warning("Failed to migrate schedule '%s': %s", name, exc)

    return results


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
    return {"success": True, "triggers": triggers, "count": len(triggers)}


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
        description=arguments.get("description"),
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
    # T-BRIX-BUG-18: Report actual task liveness, not just the flag.
    task_alive = _scheduler_task is not None and not _scheduler_task.done()
    return {
        "running": _scheduler_running and task_alive,
        "trigger_count": len(triggers),
        "enabled_count": len(enabled),
        "note": (
            "The Brix scheduler runs in-process independently of MCP sessions. "
            "Use brix__scheduler_start/stop to control it."
        ),
    }


async def _auto_start_scheduler_if_needed() -> None:
    """Auto-start the scheduler on server startup if enabled triggers exist (T-BRIX-V6-BUG-01).

    T-BRIX-SCHED-02: Also migrates schedules.yaml to DB triggers on first run.
    """
    try:
        # Migrate schedules.yaml -> DB triggers (idempotent)
        migration_results = migrate_schedules_yaml()
        if migration_results:
            created = [r for r in migration_results if r.get("action") == "created"]
            if created:
                logger.info(
                    "Migrated %d schedule(s) from schedules.yaml to DB triggers.",
                    len(created),
                )

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

    # Already running with a live task — nothing to do.
    if _scheduler_running and _scheduler_task is not None and not _scheduler_task.done():
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

    # T-BRIX-BUG-18: Actually create a background asyncio task for the
    # TriggerService so triggers poll independently of MCP sessions.
    _start_scheduler_task()

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
    global _scheduler_running, _scheduler_task

    if not _scheduler_running and (_scheduler_task is None or _scheduler_task.done()):
        return {"success": True, "status": "already_stopped"}

    _scheduler_running = False

    # T-BRIX-BUG-18: Cancel the actual background task.
    if _scheduler_task is not None and not _scheduler_task.done():
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
        _scheduler_task = None

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
    return {"success": True, "groups": groups, "count": len(groups)}


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
    return {"success": True, "query": query, "groups": matches, "count": len(matches)}


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
    return {"success": True, "query": query, "triggers": matches, "count": len(matches)}


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
