"""Run execution and history handler module."""
from __future__ import annotations

import json

import yaml

from brix.mcp_handlers._shared import (
    _loader,
    _validator,
    _audit_db,
    _extract_source,
    _source_summary,
    _load_pipeline_yaml,
    _background_runs,
    _ensure_watchdog,
    _pipeline_dir,
)
from brix.config import config
from brix.engine import PipelineEngine
from brix.mcp_pool import McpConnectionPool
from brix.history import RunHistory
from brix.pipeline_store import PipelineStore
from brix.db import BrixDB


async def _handle_run_pipeline(arguments: dict) -> dict:
    """Execute a pipeline and return results with dual-layer error schema."""
    import asyncio
    import uuid as _uuid_mod

    name = arguments.get("pipeline_id", "")
    user_input = arguments.get("input", {})
    source = _extract_source(arguments)

    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": {
                "code": "PIPELINE_NOT_FOUND",
                "message": str(exc),
                "step_id": None,
                "recoverable": False,
                "agent_actions": ["list_pipelines", "create_pipeline"],
                "resume_command": None,
            },
        }

    try:
        pipeline_yaml = yaml.dump(data)
        pipeline = _loader.load_from_string(pipeline_yaml)
    except Exception as exc:
        return {
            "success": False,
            "error": {
                "code": "PIPELINE_PARSE_ERROR",
                "message": str(exc),
                "step_id": None,
                "recoverable": True,
                "agent_actions": ["validate_pipeline", "fix_pipeline_yaml"],
                "resume_command": f"brix__validate_pipeline({{\"pipeline_id\": \"{name}\"}})",
            },
        }

    # Apply test_mode flag if requested (T-BRIX-DB-24)
    if arguments.get("test_mode"):
        pipeline.test_mode = True

    # Warn about unknown input parameters (V2-20)
    user_params = arguments.get("input", {}) or {}
    defined_params = set(pipeline.input.keys())
    unknown_params = set(user_params.keys()) - defined_params
    warnings: list[str] = []
    if unknown_params:
        warnings.append(
            f"Unknown input parameters (ignored): {', '.join(sorted(unknown_params))}"
        )

    # Validate required input parameters are present (T-BRIX-V4-21)
    input_validation = _validator.validate_input_params(pipeline, user_params)
    if not input_validation.is_valid:
        missing = [
            e.replace("Missing required input parameter: ", "").strip("'")
            for e in input_validation.errors
        ]
        return {
            "success": False,
            "error": {
                "code": "MISSING_REQUIRED_PARAMS",
                "message": f"Required input parameters not provided: {', '.join(missing)}",
                "missing_params": missing,
                "step_id": None,
                "recoverable": True,
                "agent_actions": ["provide_missing_params", "check_pipeline_input_schema"],
                "resume_command": None,
            },
        }

    async_mode = arguments.get("async", False)
    resume_run_id = arguments.get("resume_run_id")
    dry_run_steps: "list[str] | None" = arguments.get("dry_run_steps") or None

    # Validate resume_run_id if provided
    if resume_run_id:
        from brix.context import WORKDIR_BASE
        resume_workdir = WORKDIR_BASE / resume_run_id
        if not resume_workdir.exists():
            return {
                "success": False,
                "error": {
                    "code": "RESUME_RUN_NOT_FOUND",
                    "message": f"No workdir found for run_id '{resume_run_id}'. Cannot resume.",
                    "step_id": None,
                    "recoverable": False,
                    "agent_actions": ["list_run_history", "run_pipeline_fresh"],
                    "resume_command": None,
                },
            }

    if async_mode:
        # Pre-generate run_id so we can return it immediately
        pre_run_id = resume_run_id or f"run-{_uuid_mod.uuid4().hex[:12]}"

        bg_pool = McpConnectionPool()
        await bg_pool.__aenter__()

        async def _run_in_background() -> None:
            try:
                engine = PipelineEngine()
                await engine.run(pipeline, user_input, run_id=pre_run_id, mcp_pool=bg_pool, dry_run_steps=dry_run_steps)
            except Exception:
                pass  # engine.run() already records failure in history
            finally:
                await bg_pool.__aexit__(None, None, None)
                _background_runs.pop(pre_run_id, None)

        task = asyncio.create_task(_run_in_background())
        _background_runs[pre_run_id] = task
        _ensure_watchdog()  # Start the auto-kill watchdog if not already running (T-BRIX-V6-03)

        return {
            "success": True,
            "run_id": pre_run_id,
            "status": "running",
            "pipeline": name,
            "resumed": resume_run_id is not None,
            "message": (
                f"Pipeline '{name}' started in background. "
                f"Poll with brix__get_run_status(run_id='{pre_run_id}')"
            ),
            "warnings": warnings,
        }

    # Wire up MCP progress notifications for synchronous runs (T-BRIX-V4-BUG-02).
    engine = PipelineEngine()
    try:
        from mcp.server.lowlevel.server import request_ctx
        _ctx = request_ctx.get(None)
        if _ctx is not None and _ctx.meta is not None and _ctx.meta.progressToken is not None:
            from brix.progress import McpProgressReporter
            engine.progress = McpProgressReporter(
                session=_ctx.session,
                progress_token=_ctx.meta.progressToken,
            )
    except Exception:
        pass  # Never break execution because of progress wiring

    try:
        result = await engine.run(pipeline, user_input, run_id=resume_run_id, dry_run_steps=dry_run_steps)
    except Exception as exc:
        return {
            "success": False,
            "error": {
                "code": "ENGINE_ERROR",
                "message": str(exc),
                "step_id": None,
                "recoverable": True,
                "agent_actions": ["retry_pipeline", "validate_pipeline"],
                "resume_command": f"brix__run_pipeline({{\"pipeline_id\": \"{name}\"}})",
            },
        }

    # T-BRIX-V6-01: store source in triggered_by and write audit entry
    if source:
        triggered_by_value = json.dumps(source)
        try:
            with _audit_db._connect() as _conn:
                _conn.execute(
                    "UPDATE runs SET triggered_by=? WHERE run_id=?",
                    (triggered_by_value, result.run_id),
                )
        except Exception:
            pass  # Non-fatal — run record still valid
    _audit_db.write_audit_entry(
        tool="brix__run_pipeline",
        source=source,
        arguments_summary=_source_summary(source, pipeline=name, run_id=result.run_id),
    )

    # T-BRIX-V6-05: Claude Code channel push — send MCP notification to the calling session
    if source:
        try:
            from mcp.server.lowlevel.server import request_ctx
            from mcp.types import JSONRPCNotification
            _req_ctx = request_ctx.get(None)
            if _req_ctx is not None:
                _status = "success" if result.success else "failed"
                _items = sum(
                    s.items for s in result.steps.values() if s.items is not None
                )
                _content = (
                    f"Pipeline '{name}' {_status}: "
                    f"{_items} items in {round(result.duration, 2)}s"
                )
                _notif = JSONRPCNotification(
                    jsonrpc="2.0",
                    method="notifications/claude/channel",
                    params={
                        "content": _content,
                        "meta": {
                            "run_id": result.run_id,
                            "pipeline": name,
                            "status": _status,
                            "duration": round(result.duration, 2),
                        },
                    },
                )
                await _req_ctx.session.send_notification(_notif)
        except Exception:
            pass  # Channel push failure must never affect pipeline result

    # T-BRIX-V6-06: Mattermost webhook notification
    try:
        _mm = pipeline.notify.mattermost
        if _mm.enabled and _mm.webhook_url:
            import urllib.request as _urllib_req
            _mm_status = "success" if result.success else "failed"
            _mm_msg = (
                f"[Brix] Pipeline **{name}** {_mm_status} "
                f"(run_id: {result.run_id}, {round(result.duration, 2)}s)"
            )
            _mm_payload = json.dumps({"text": _mm_msg}).encode()
            _mm_req = _urllib_req.Request(
                _mm.webhook_url,
                data=_mm_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with _urllib_req.urlopen(_mm_req, timeout=config.MATTERMOST_WEBHOOK_TIMEOUT):
                pass
    except Exception:
        pass  # Mattermost failure must never affect pipeline result

    if result.success:
        resp: dict = {
            "success": True,
            "run_id": result.run_id,
            "pipeline": name,
            "resumed": resume_run_id is not None,
            "duration": round(result.duration, 2),
            "steps": {
                step_id: {
                    "status": s.status,
                    "duration": round(s.duration, 2),
                    "items": s.items,
                    "errors": s.errors,
                }
                for step_id, s in result.steps.items()
            },
            "result": result.result,
            "warnings": warnings,
        }
        if result.deprecation_warnings:
            resp["deprecation_warnings"] = result.deprecation_warnings
        return resp
    else:
        # Find the first failed step for the error report
        failed_step = next(
            (sid for sid, s in result.steps.items() if s.status == "error"),
            None,
        )
        resp = {
            "success": False,
            "run_id": result.run_id,
            "pipeline": name,
            "resumed": resume_run_id is not None,
            "duration": round(result.duration, 2),
            "steps": {
                step_id: {
                    "status": s.status,
                    "duration": round(s.duration, 2),
                    "items": s.items,
                    "errors": s.errors,
                }
                for step_id, s in result.steps.items()
            },
            "error": {
                "code": "STEP_FAILED",
                "message": f"Pipeline failed at step: {failed_step or 'unknown'}",
                "step_id": failed_step,
                "recoverable": True,
                "agent_actions": ["retry_step", "skip_step", "abort_pipeline"],
                "resume_command": (
                    f"brix__run_pipeline({{\"pipeline_id\": \"{name}\", "
                    f"\"resume_run_id\": \"{result.run_id}\"}})"
                    if failed_step else None
                ),
            },
            "warnings": warnings,
        }
        if result.deprecation_warnings:
            resp["deprecation_warnings"] = result.deprecation_warnings
        return resp


async def _handle_get_run_status(arguments: dict) -> dict:
    """Get the status of a specific run by run_id."""
    import json as _json
    import time as _time

    run_id = arguments.get("run_id", "")

    # 1. Try live run.json from workdir (running or recently finished)
    from brix.context import WORKDIR_BASE
    run_json_path = WORKDIR_BASE / run_id / "run.json"
    if run_json_path.exists():
        try:
            with open(run_json_path) as f:
                live = _json.load(f)
            if live.get("status") == "running":
                # Hang detection: no heartbeat for >5 minutes
                heartbeat = live.get("last_heartbeat", 0)
                age = _time.time() - heartbeat if heartbeat else 0
                is_suspected_hang = age > 300
                live["suspected_hang"] = is_suspected_hang
                if is_suspected_hang:
                    live["hint"] = (
                        "Run appears to be hung (no heartbeat for >5 minutes). "
                        "Check container logs or use brix__cancel_run to abort."
                    )
                # Inject per-step intra-step progress (T-BRIX-V4-BUG-05)
                step_progress_path = WORKDIR_BASE / run_id / "step_progress.json"
                if step_progress_path.exists():
                    try:
                        with open(step_progress_path) as spf:
                            raw_sp = _json.load(spf)
                        enriched: dict = {}
                        for sid, sp in raw_sp.items():
                            processed = sp.get("processed", 0)
                            total_items = sp.get("total", 0)
                            pct = round(processed / total_items * 100, 1) if total_items > 0 else 0.0
                            entry: dict = {
                                "processed": processed,
                                "total": total_items,
                                "percent": pct,
                            }
                            if sp.get("eta_seconds") is not None:
                                entry["eta_seconds"] = sp["eta_seconds"]
                            if sp.get("message"):
                                entry["message"] = sp["message"]
                            enriched[sid] = entry
                        live["step_progress"] = enriched
                    except (OSError, ValueError):
                        pass
                # Inject live_progress from DB (T-BRIX-DB-14)
                try:
                    _db = BrixDB()
                    _live_progress = _db.get_step_progress(run_id)
                    if _live_progress:
                        live["live_progress"] = _live_progress
                except Exception:
                    pass
                return {"success": True, "source": "live", **live}
        except (OSError, ValueError):
            pass

    # 2. Fallback to SQLite history (completed runs)
    history = RunHistory()
    run = history.get_run(run_id)

    if run is None:
        return {
            "success": False,
            "error": f"Run '{run_id}' not found in history.",
        }

    # SQLite stores success as 0/1 integer — normalise to bool
    run_data = dict(run)
    if "success" in run_data:
        run_data["success"] = bool(run_data["success"])

    # Attach the actual pipeline output (T-BRIX-V4-BUG-10)
    result_output, truncated = history.get_result(run_id)
    if truncated:
        run_data["result"] = "(truncated, use get_run_log for full output)"
    elif result_output is not None:
        run_data["result"] = result_output
    else:
        run_data["result"] = None
        run_data["result_hint"] = (
            "Result is empty. Set persist_output: true on steps to capture outputs."
        )

    # Hint: if run failed, suggest get_run_errors for detailed error info
    if not run_data.get("success"):
        run_data["hint"] = "Use get_run_errors(run_id) for detailed error info with auto-hints"

    # Attach deprecation warnings for this pipeline (T-BRIX-DB-05d)
    try:
        _pipeline_name = run_data.get("pipeline", "")
        if _pipeline_name:
            _dep_db = BrixDB()
            _dep_all = _dep_db.get_deprecated_usage()
            _dep_for_pipeline = [
                f"Step '{e['step_id']}': '{e['old_type']}' → '{e['new_type']}'"
                for e in _dep_all
                if e.get("pipeline_name") == _pipeline_name
            ]
            if _dep_for_pipeline:
                run_data["deprecation_warnings"] = _dep_for_pipeline
    except Exception:
        pass  # Never break get_run_status over tracking

    # Attach final step progress from DB (T-BRIX-DB-14)
    try:
        _prog_db = BrixDB()
        _final_progress = _prog_db.get_step_progress(run_id)
        if _final_progress:
            run_data["live_progress"] = _final_progress
    except Exception:
        pass  # Never break get_run_status over progress

    return {
        "success": True,
        "source": "history",
        **run_data,
    }


async def _handle_diff_runs(arguments: dict) -> dict:
    """Compare two runs side by side (T-BRIX-V7-05)."""
    import json as _json

    run_id_a = arguments.get("run_id_a", "")
    run_id_b = arguments.get("run_id_b", "")

    if not run_id_a or not run_id_b:
        return {"success": False, "error": "Both run_id_a and run_id_b are required."}
    if run_id_a == run_id_b:
        return {"success": False, "error": "run_id_a and run_id_b must be different runs."}

    db = BrixDB()
    run_a = db.get_run(run_id_a)
    run_b = db.get_run(run_id_b)

    missing = []
    if run_a is None:
        missing.append(run_id_a)
    if run_b is None:
        missing.append(run_id_b)
    if missing:
        return {"success": False, "error": f"Run(s) not found: {', '.join(missing)}"}

    def _parse_json_field(raw) -> object:
        if raw is None:
            return None
        if isinstance(raw, (dict, list)):
            return raw
        try:
            return _json.loads(raw)
        except (ValueError, TypeError):
            return raw

    input_a = _parse_json_field(run_a.get("input_data"))
    input_b = _parse_json_field(run_b.get("input_data"))

    input_diff: dict = {}
    all_input_keys = set()
    if isinstance(input_a, dict):
        all_input_keys.update(input_a.keys())
    if isinstance(input_b, dict):
        all_input_keys.update(input_b.keys())
    for k in sorted(all_input_keys):
        va = input_a.get(k) if isinstance(input_a, dict) else None
        vb = input_b.get(k) if isinstance(input_b, dict) else None
        if va != vb:
            input_diff[k] = {"a": va, "b": vb}

    if input_a != input_b and not input_diff:
        input_diff["_value"] = {"a": input_a, "b": input_b}

    ver_a = run_a.get("version")
    ver_b = run_b.get("version")
    version_diff: "dict | None" = None
    if ver_a != ver_b:
        version_diff = {"a": ver_a, "b": ver_b}

    steps_a = _parse_json_field(run_a.get("steps_data")) or {}
    steps_b = _parse_json_field(run_b.get("steps_data")) or {}

    step_diffs: list[dict] = []
    all_step_ids = sorted(set(list(steps_a.keys()) + list(steps_b.keys())))

    # Load persisted step outputs (from step_outputs table) for both runs
    so_a_list = db.get_step_outputs(run_id_a)
    so_b_list = db.get_step_outputs(run_id_b)
    so_a = {row["step_id"]: row for row in so_a_list}
    so_b = {row["step_id"]: row for row in so_b_list}

    for step_id in all_step_ids:
        step_entry_a = steps_a.get(step_id, {})
        step_entry_b = steps_b.get(step_id, {})
        status_a = step_entry_a.get("status") if step_entry_a else None
        status_b = step_entry_b.get("status") if step_entry_b else None
        err_a = step_entry_a.get("error_message") if step_entry_a else None
        err_b = step_entry_b.get("error_message") if step_entry_b else None
        dur_a = step_entry_a.get("duration") if step_entry_a else None
        dur_b = step_entry_b.get("duration") if step_entry_b else None

        # Output diff (only when step_outputs are available for both)
        output_diff: "dict | None" = None
        if step_id in so_a and step_id in so_b:
            out_a = so_a[step_id].get("output")
            out_b = so_b[step_id].get("output")
            if out_a != out_b:
                output_diff = {"a": out_a, "b": out_b}

        changed = (
            status_a != status_b
            or err_a != err_b
            or output_diff is not None
        )
        if changed:
            entry: dict = {
                "step_id": step_id,
                "a_status": status_a,
                "b_status": status_b,
            }
            if dur_a is not None or dur_b is not None:
                entry["a_duration"] = dur_a
                entry["b_duration"] = dur_b
            if err_a or err_b:
                entry["a_error"] = err_a
                entry["b_error"] = err_b
            if output_diff is not None:
                entry["output_diff"] = output_diff
            step_diffs.append(entry)

    env_a = _parse_json_field(run_a.get("environment_json"))
    env_b = _parse_json_field(run_b.get("environment_json"))
    environment_diff: dict = {}
    if isinstance(env_a, dict) and isinstance(env_b, dict):
        pv_a = env_a.get("python_version")
        pv_b = env_b.get("python_version")
        if pv_a != pv_b:
            environment_diff["python_version"] = {"a": pv_a, "b": pv_b}
        srv_a = set(env_a.get("mcp_servers") or [])
        srv_b = set(env_b.get("mcp_servers") or [])
        if srv_a != srv_b:
            environment_diff["mcp_servers"] = {
                "added": sorted(srv_b - srv_a),
                "removed": sorted(srv_a - srv_b),
            }
        pkgs_a = set(env_a.get("installed_packages") or [])
        pkgs_b = set(env_b.get("installed_packages") or [])
        if pkgs_a != pkgs_b:
            environment_diff["installed_packages"] = {
                "added": sorted(pkgs_b - pkgs_a),
                "removed": sorted(pkgs_a - pkgs_b),
            }
    elif env_a != env_b:
        environment_diff["_snapshot"] = {
            "a": "present" if env_a else "missing",
            "b": "present" if env_b else "missing",
        }

    return {
        "success": True,
        "run_id_a": run_id_a,
        "run_id_b": run_id_b,
        "pipeline": run_a.get("pipeline"),
        "identical": (
            not input_diff
            and version_diff is None
            and not step_diffs
            and not environment_diff
        ),
        "input_diff": input_diff,
        "version_diff": version_diff,
        "step_diffs": step_diffs,
        "environment_diff": environment_diff if environment_diff else None,
        "summary": {
            "a_success": bool(run_a.get("success")),
            "b_success": bool(run_b.get("success")),
            "changed_steps": len(step_diffs),
            "has_input_diff": bool(input_diff),
            "has_version_diff": version_diff is not None,
            "has_env_diff": bool(environment_diff),
        },
    }


async def _handle_get_run_errors(arguments: dict) -> dict:
    """Get error details for a specific run or last N failed runs of a pipeline."""
    run_id = arguments.get("run_id")
    pipeline_name = arguments.get("pipeline")
    last = int(arguments.get("last", 1))

    history = RunHistory()
    errors = history.get_run_errors(run_id=run_id, pipeline=pipeline_name, last=last)

    return {
        "success": True,
        "errors": errors,
        "total": len(errors),
    }


async def _handle_get_run_log(arguments: dict) -> dict:
    """Get the full step-by-step execution log for a run."""
    run_id = arguments.get("run_id", "")

    history = RunHistory()
    log = history.get_run_log(run_id)

    if not log:
        # Check if run exists at all
        run = history.get_run(run_id)
        if run is None:
            return {
                "success": False,
                "error": f"Run '{run_id}' not found in history.",
            }

    return {
        "success": True,
        "run_id": run_id,
        "steps": log,
        "total_steps": len(log),
    }


async def _handle_get_run_history(arguments: dict) -> dict:
    """Get recent run history."""
    limit = int(arguments.get("limit", 10))
    pipeline_name = arguments.get("pipeline_name")

    history = RunHistory()
    runs = history.get_recent(limit=limit)

    if pipeline_name:
        runs = [r for r in runs if r.get("pipeline") == pipeline_name]

    return {
        "success": True,
        "runs": runs,
        "total": len(runs),
    }


async def _handle_get_timeline(arguments: dict) -> dict:
    """Get chronological step timeline for a run (T-BRIX-V7-07)."""
    run_id = arguments.get("run_id", "")
    if not run_id:
        return {"success": False, "error": "run_id is required."}

    db = BrixDB()
    timeline = db.get_run_timeline(run_id)

    if not timeline:
        # Check if the run exists at all
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


async def _handle_run_annotate(arguments: dict) -> dict:
    """Attach notes to a run."""
    run_id = arguments.get("run_id", "").strip()
    notes = arguments.get("notes", "")

    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required."}
    if notes == "":
        return {"success": False, "error": "Parameter 'notes' is required."}

    history = RunHistory()
    updated = history.annotate(run_id, notes)
    if not updated:
        return {"success": False, "error": f"Run '{run_id}' not found in history."}
    return {"success": True, "run_id": run_id, "notes": notes}


async def _handle_run_search(arguments: dict) -> dict:
    """Filter run history by pipeline, status, and/or time range."""
    pipeline = arguments.get("pipeline") or None
    status = arguments.get("status") or None
    since = arguments.get("since") or None
    until = arguments.get("until") or None
    limit = int(arguments.get("limit", 50))

    if status and status not in ("success", "failure", "running"):
        return {
            "success": False,
            "error": f"Invalid status '{status}'. Must be 'success', 'failure', or 'running'.",
        }

    history = RunHistory()
    runs = history.search(pipeline=pipeline, status=status, since=since, until=until, limit=limit)

    # Strip heavy fields from search results to keep payload lean
    results = [
        {
            "run_id": r.get("run_id"),
            "pipeline": r.get("pipeline"),
            "version": r.get("version"),
            "started_at": r.get("started_at"),
            "finished_at": r.get("finished_at"),
            "duration": r.get("duration"),
            "success": r.get("success"),
            "triggered_by": r.get("triggered_by"),
            "notes": r.get("notes"),
        }
        for r in runs
    ]

    return {
        "success": True,
        "total": len(results),
        "runs": results,
    }


async def _handle_delete_run(arguments: dict) -> dict:
    """Delete a single run from SQLite run history."""
    run_id = arguments.get("run_id", "").strip()

    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}

    history = RunHistory()
    deleted = history.delete_run(run_id)

    if deleted:
        return {"success": True, "deleted_run_id": run_id}
    return {"success": False, "error": f"Run '{run_id}' not found in history"}


async def _handle_cancel_run(arguments: dict) -> dict:
    """Cancel a running pipeline (T-BRIX-V6-BUG-03)."""
    import time as _time_mod

    run_id = arguments.get("run_id", "").strip()
    reason = arguments.get("reason", "") or ""

    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}

    from brix.context import WORKDIR_BASE

    run_workdir = WORKDIR_BASE / run_id

    # Write the sentinel file — engine and helpers poll this path
    sentinel = run_workdir / "cancel_requested.json"
    try:
        run_workdir.mkdir(parents=True, exist_ok=True)
        sentinel.write_text(
            json.dumps({"reason": reason, "requested_at": _time_mod.time()})
        )
    except OSError as exc:
        return {
            "success": False,
            "error": f"Could not write cancel sentinel for run '{run_id}': {exc}",
        }

    # Cancel the asyncio.Task if this is an active background run
    task_cancelled = False
    task = _background_runs.get(run_id)
    if task is not None and not task.done():
        task.cancel()
        _background_runs.pop(run_id, None)
        task_cancelled = True

    # Update history (mark as cancelled)
    history = RunHistory()
    history.cancel_run(run_id, reason=reason, cancelled_by="user")

    # Audit log
    source = _extract_source(arguments)
    _audit_db.write_audit_entry(
        tool="brix__cancel_run",
        source=source,
        arguments_summary=_source_summary(source, run_id=run_id, reason=reason),
    )

    return {
        "success": True,
        "cancelled": True,
        "run_id": run_id,
        "reason": reason,
        "task_cancelled": task_cancelled,
        "sentinel_written": True,
    }


async def _handle_resume_run(arguments: dict) -> dict:
    """Resume a previously failed or cancelled run from the last checkpoint."""
    run_id = arguments.get("run_id", "").strip()
    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}

    # Delegate to run_pipeline with resume_run_id
    return await _handle_run_pipeline({
        **arguments,
        "pipeline_id": arguments.get("pipeline_id", ""),
        "resume_run_id": run_id,
    })


async def _handle_inspect_context(arguments: dict) -> dict:
    """Inspect the execution context of a specific run step."""
    run_id = arguments.get("run_id", "").strip()
    step_id = arguments.get("step_id", "").strip()

    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}

    from brix.context import WORKDIR_BASE
    run_dir = WORKDIR_BASE / run_id

    if not run_dir.exists():
        return {"success": False, "error": f"Run '{run_id}' workdir not found"}

    # Read step context if available
    context_file = run_dir / "context.json"
    if context_file.exists():
        try:
            import json as _json
            ctx = _json.loads(context_file.read_text())
            if step_id:
                step_ctx = ctx.get("steps", {}).get(step_id)
                if step_ctx is None:
                    return {"success": False, "error": f"Step '{step_id}' context not found"}
                return {"success": True, "run_id": run_id, "step_id": step_id, "context": step_ctx}
            return {"success": True, "run_id": run_id, "context": ctx}
        except Exception as exc:
            return {"success": False, "error": f"Could not read context: {exc}"}

    return {"success": False, "error": f"No context data available for run '{run_id}'"}


async def _handle_replay_step(arguments: dict) -> dict:
    """Replay a single step from a previous run using its saved context."""
    run_id = arguments.get("run_id", "").strip()
    step_id = arguments.get("step_id", "").strip()

    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}
    if not step_id:
        return {"success": False, "error": "Parameter 'step_id' is required"}

    history = RunHistory()
    run = history.get_run(run_id)
    if run is None:
        return {"success": False, "error": f"Run '{run_id}' not found in history"}

    pipeline_name = run.get("pipeline", "")
    if not pipeline_name:
        return {"success": False, "error": "Could not determine pipeline name from run"}

    # Load pipeline and find step
    try:
        import yaml as _yaml
        data = _load_pipeline_yaml(pipeline_name)
        pipeline = _loader.load_from_string(_yaml.dump(data))
    except Exception as exc:
        return {"success": False, "error": f"Could not load pipeline: {exc}"}

    step_def = None
    from brix.mcp_handlers._shared import _find_step_recursive
    step_def = _find_step_recursive(data.get("steps", []), step_id)
    if step_def is None:
        return {"success": False, "error": f"Step '{step_id}' not found in pipeline '{pipeline_name}'"}

    return {
        "success": True,
        "message": f"Step '{step_id}' replay is not yet implemented. Use run_pipeline with resume_run_id instead.",
        "hint": f"brix__run_pipeline(pipeline_id='{pipeline_name}', resume_run_id='{run_id}')",
    }


async def _handle_get_step_data(arguments: dict) -> dict:
    """Return persisted execution data for a step (T-BRIX-DB-07)."""
    run_id = arguments.get("run_id", "").strip()
    step_id = arguments.get("step_id", "").strip()

    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}
    if not step_id:
        return {"success": False, "error": "Parameter 'step_id' is required"}

    db = BrixDB()
    step_executions = db.get_step_executions(run_id=run_id, step_id=step_id)
    foreach_items = db.get_foreach_items(run_id=run_id, step_id=step_id)

    if not step_executions and not foreach_items:
        return {
            "success": False,
            "error": (
                f"No execution data found for step '{step_id}' in run '{run_id}'. "
                "Data is only persisted when persist_output=true or BRIX_DEBUG is set."
            ),
        }

    # Return latest execution record for this step
    execution = step_executions[-1] if step_executions else None

    return {
        "success": True,
        "run_id": run_id,
        "step_id": step_id,
        "execution": execution,
        "foreach_items": foreach_items,
        "foreach_item_count": len(foreach_items),
    }
