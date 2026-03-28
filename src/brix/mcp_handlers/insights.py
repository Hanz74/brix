"""Insights and diagnostics handler module."""
from __future__ import annotations

import json

from brix.db import BrixDB
from brix.history import RunHistory
from brix.mcp_handlers._shared import (
    _re_module_name,
    _pipeline_dir,
)
from brix.pipeline_store import PipelineStore


async def _handle_diagnose_run(arguments: dict) -> dict:
    """Diagnose a failed run — structured error analysis with fix suggestions."""
    run_id = arguments.get("run_id", "").strip()
    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required."}

    history = RunHistory()
    run = history.get_run(run_id)
    if run is None:
        return {"success": False, "error": f"Run '{run_id}' not found in history."}

    steps_json = run.get("steps_data")
    if not steps_json:
        return {
            "success": True,
            "run_id": run_id,
            "pipeline": run.get("pipeline", ""),
            "diagnoses": [],
            "message": "No step data recorded for this run.",
        }

    try:
        steps = json.loads(steps_json)
    except (json.JSONDecodeError, TypeError):
        return {"success": False, "error": "Could not parse steps_data for this run."}

    from brix.history import _error_hint

    pipeline_name = run.get("pipeline", "")
    store = PipelineStore(pipelines_dir=_pipeline_dir())

    # Try to load pipeline YAML for context
    pipeline_context: dict = {}
    for name_candidate in [pipeline_name, pipeline_name.replace("-", "_")]:
        try:
            raw = store.load_raw(name_candidate)
            # Build a step_id → step dict for fast lookup
            pipeline_context = {
                s.get("id", ""): s
                for s in raw.get("steps", [])
                if isinstance(s, dict)
            }
            break
        except (FileNotFoundError, Exception):
            pass

    diagnoses = []
    for step_id, data in steps.items():
        if data.get("status") != "error":
            continue

        err_msg = data.get("error_message") or data.get("errors") or "unknown error"
        if not isinstance(err_msg, str):
            err_msg = str(err_msg)

        hint = _error_hint(step_id, err_msg)

        # Determine fix suggestion
        fix_suggestion: "str | None" = None
        if "ModuleNotFoundError" in err_msg:
            m = _re_module_name(err_msg)
            fix_suggestion = (
                f"Call brix__auto_fix_step(run_id='{run_id}', step_id='{step_id}') "
                f"to install the missing module{' ' + repr(m) if m else ''}."
            )
        elif "UndefinedError" in err_msg or "is undefined" in err_msg:
            fix_suggestion = (
                f"Call brix__auto_fix_step(run_id='{run_id}', step_id='{step_id}') "
                "to add | default('') to the undefined Jinja2 reference."
            )
        elif "Timeout" in err_msg:
            fix_suggestion = (
                f"Call brix__auto_fix_step(run_id='{run_id}', step_id='{step_id}') "
                "to double the step timeout."
            )

        step_ctx = pipeline_context.get(step_id) or {}

        diagnoses.append({
            "step_id": step_id,
            "error": err_msg,
            "hint": hint,
            "fix_suggestion": fix_suggestion,
            "pipeline_context": step_ctx,
        })

    return {
        "success": True,
        "run_id": run_id,
        "pipeline": pipeline_name,
        "diagnoses": diagnoses,
        "total_failed_steps": len(diagnoses),
    }


async def _handle_get_insights(arguments: dict) -> dict:
    """Return analytical insights: slow steps, failure patterns, dead helpers."""
    db = BrixDB()

    insights: dict = {
        "slow_steps": [],
        "failure_patterns": [],
        "dead_helpers": [],
    }

    with db._connect() as conn:
        # slow_steps: per-pipeline/step combos with avg_duration > 3x median
        rows = conn.execute(
            "SELECT pipeline, steps_data FROM runs "
            "WHERE finished_at IS NOT NULL AND steps_data IS NOT NULL"
        ).fetchall()

        # Accumulate durations per (pipeline, step_id)
        step_durations: dict[tuple[str, str], list[float]] = {}
        for pipeline, steps_json in rows:
            try:
                steps = json.loads(steps_json)
            except (json.JSONDecodeError, TypeError):
                continue
            for step_id, data in steps.items():
                dur = data.get("duration")
                if dur is not None:
                    try:
                        key = (pipeline, step_id)
                        step_durations.setdefault(key, []).append(float(dur))
                    except (TypeError, ValueError):
                        pass

        if step_durations:
            all_avgs = []
            step_avg_map: dict[tuple[str, str], float] = {}
            for key, durs in step_durations.items():
                avg = sum(durs) / len(durs)
                step_avg_map[key] = avg
                all_avgs.append(avg)

            # Median of all per-step averages
            sorted_avgs = sorted(all_avgs)
            n = len(sorted_avgs)
            median = (
                sorted_avgs[n // 2]
                if n % 2
                else (sorted_avgs[n // 2 - 1] + sorted_avgs[n // 2]) / 2
            )
            threshold = median * 3

            for (pipeline, step_id), avg in step_avg_map.items():
                if avg > threshold:
                    insights["slow_steps"].append({
                        "pipeline": pipeline,
                        "step_id": step_id,
                        "avg_duration": round(avg, 2),
                        "median_duration": round(median, 2),
                        "ratio": round(avg / median, 1) if median > 0 else None,
                    })

            insights["slow_steps"].sort(key=lambda x: x["avg_duration"], reverse=True)

        # failure_patterns: common errors grouped by pipeline
        fail_rows = conn.execute(
            "SELECT pipeline, steps_data FROM runs "
            "WHERE finished_at IS NOT NULL AND success=0 AND steps_data IS NOT NULL"
        ).fetchall()

        from collections import Counter
        error_counter: Counter = Counter()
        for pipeline, steps_json in fail_rows:
            try:
                steps = json.loads(steps_json)
            except (json.JSONDecodeError, TypeError):
                continue
            for step_id, data in steps.items():
                if data.get("status") != "error":
                    continue
                err = data.get("error_message") or data.get("errors") or ""
                if not isinstance(err, str):
                    err = str(err)
                # Use first 80 chars as pattern key
                pattern = err[:80].strip()
                if pattern:
                    error_counter[(pipeline, pattern)] += 1

        for (pipeline, pattern), count in error_counter.most_common(20):
            insights["failure_patterns"].append({
                "pipeline": pipeline,
                "error_pattern": pattern,
                "occurrences": count,
            })

        # dead_helpers: helpers in registry not referenced by any pipeline
        all_helpers = conn.execute(
            "SELECT h.id, h.name FROM helpers h "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM pipeline_helpers ph WHERE ph.helper_id = h.id"
            ")"
        ).fetchall()

        for helper_id, helper_name in all_helpers:
            insights["dead_helpers"].append({
                "id": helper_id,
                "name": helper_name,
            })

    return {"success": True, **insights}


async def _handle_get_proactive_suggestions(arguments: dict) -> dict:
    """Return actionable suggestions combining insights, alert history, and dependency checks."""
    # Gather insights
    insights_result = await _handle_get_insights({})

    suggestions: list[dict] = []

    # --- Suggestions from slow steps ---
    for slow in insights_result.get("slow_steps", []):
        suggestions.append({
            "type": "performance",
            "message": (
                f"Step '{slow['step_id']}' in pipeline '{slow['pipeline']}' "
                f"is {slow['ratio']}x slower than average "
                f"(avg {slow['avg_duration']}s vs median {slow['median_duration']}s). "
                "Consider adding caching, reducing payload size, or optimising the step logic."
            ),
            "action_tool": "brix__get_step",
            "action_params": {
                "pipeline_name": slow["pipeline"],
                "step_id": slow["step_id"],
            },
        })

    # --- Suggestions from failure patterns ---
    for fp in insights_result.get("failure_patterns", []):
        if fp["occurrences"] >= 2:
            suggestions.append({
                "type": "reliability",
                "message": (
                    f"Pipeline '{fp['pipeline']}' has failed {fp['occurrences']} times "
                    f"with error: \"{fp['error_pattern']}\". "
                    "Add on_error=retry or investigate the root cause."
                ),
                "action_tool": "brix__get_run_errors",
                "action_params": {
                    "pipeline": fp["pipeline"],
                    "last": 3,
                },
            })

    # --- Suggestions from dead helpers ---
    for dead in insights_result.get("dead_helpers", []):
        suggestions.append({
            "type": "cleanup",
            "message": (
                f"Helper '{dead['name']}' is registered but not used in any pipeline. "
                "Consider removing it to keep the registry clean."
            ),
            "action_tool": "brix__delete_helper",
            "action_params": {"name": dead["name"]},
        })

    # --- Suggestions from recent alert history ---
    try:
        from brix.alerting import AlertManager
        mgr = AlertManager()
        recent_alerts = mgr.get_alert_history(limit=10)
        alert_pipelines: dict[str, int] = {}
        for alert in recent_alerts:
            p = alert.get("pipeline") or ""
            if p:
                alert_pipelines[p] = alert_pipelines.get(p, 0) + 1
        for pipeline, count in alert_pipelines.items():
            if count >= 3:
                suggestions.append({
                    "type": "alert",
                    "message": (
                        f"Pipeline '{pipeline}' triggered {count} alerts recently. "
                        "Review alert rules or investigate repeated failures."
                    ),
                    "action_tool": "brix__alert_history",
                    "action_params": {"limit": 20},
                })
    except Exception:
        pass  # Alerting is optional — don't fail the whole call

    # --- Dependency check: helpers with requirements not installed ---
    try:
        from brix.deps import check_requirements
        from brix.helper_registry import HelperRegistry
        reg = HelperRegistry()
        for entry in reg.list_all():
            reqs = entry.requirements or []
            if not reqs:
                continue
            missing = check_requirements(reqs)
            if missing:
                suggestions.append({
                    "type": "dependency",
                    "message": (
                        f"Helper '{entry.name}' requires packages that are not installed: "
                        f"{missing}. Run auto_fix or install manually."
                    ),
                    "action_tool": "brix__get_helper",
                    "action_params": {"name": entry.name},
                })
    except Exception:
        pass  # Don't fail if registry is empty/inaccessible

    return {
        "success": True,
        "suggestions": suggestions,
        "total": len(suggestions),
    }


async def _handle_get_timeline(arguments: dict) -> dict:
    """Get chronological step timeline for a run (T-BRIX-V7-07) — re-exported here for insights."""
    # This is the canonical implementation; runs.py also imports this via the shared pattern
    run_id = arguments.get("run_id", "")
    if not run_id:
        return {"success": False, "error": "run_id is required."}

    db = BrixDB()
    timeline = db.get_run_timeline(run_id)

    if not timeline:
        run = db.get_run(run_id)
        if run is None:
            return {"success": False, "error": f"Run '{run_id}' not found."}
        return {
            "success": True,
            "run_id": run_id,
            "timeline": [],
            "total_steps": 0,
            "note": "No step data available for this run.",
        }

    total_duration = sum(s.get("duration", 0.0) for s in timeline)
    return {
        "success": True,
        "run_id": run_id,
        "timeline": timeline,
        "total_steps": len(timeline),
        "total_duration": round(total_duration, 3),
    }


async def _handle_check_resource(arguments: dict) -> dict:
    """Check lock status of a resource (V6-11)."""
    resource_id = arguments.get("resource_id", "").strip()
    if not resource_id:
        return {"error": "resource_id is required"}

    db = BrixDB()
    return db.check_resource(resource_id)


async def _handle_claim_resource(arguments: dict) -> dict:
    """Acquire a distributed lock on a named resource (V6-11)."""
    resource_id = arguments.get("resource_id", "").strip()
    run_id = arguments.get("run_id", "").strip()
    if not resource_id:
        return {"error": "resource_id is required"}
    if not run_id:
        return {"error": "run_id is required"}
    ttl_minutes = int(arguments.get("ttl_minutes") or 30)

    db = BrixDB()
    return db.claim_resource(resource_id=resource_id, run_id=run_id, ttl_minutes=ttl_minutes)


async def _handle_release_resource(arguments: dict) -> dict:
    """Release a lock on a resource (V6-11)."""
    resource_id = arguments.get("resource_id", "").strip()
    if not resource_id:
        return {"error": "resource_id is required"}

    db = BrixDB()
    released = db.release_resource(resource_id)
    return {"released": released, "resource_id": resource_id}


async def _handle_db_status(arguments: dict) -> dict:
    """Return DB schema version, applied/pending migrations, and DB size (T-BRIX-DB-27)."""
    from brix.migrations import get_migration_status
    db = BrixDB()
    status = get_migration_status(db)
    return {"success": True, **status}
