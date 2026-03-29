"""Step and brick handler module."""
from __future__ import annotations

from brix.mcp_handlers._shared import (
    _registry,
    _audit_db,
    _extract_source,
    _source_summary,
    _load_pipeline_yaml,
    _save_pipeline_yaml,
    _validate_pipeline_dict,
    _find_step_recursive,
    _pipeline_dir,
    record_schema_consultation,
    was_schema_consulted,
)
from brix.bricks.types import is_compatible, suggest_converter
from brix.pipeline_store import PipelineStore
from brix.engine import LEGACY_ALIASES


async def _handle_list_bricks(arguments: dict) -> dict:
    """List all available bricks, optionally filtered by category."""
    category = arguments.get("category")

    if category:
        bricks = _registry.list_by_category(category)
    else:
        bricks = _registry.list_all()

    # T-BRIX-ORG-01: enrich with org fields from DB
    _org_map: dict = {}
    try:
        from brix.db import BrixDB as _BrixDB
        import json as _list_json
        _ldb = _BrixDB()
        _lconn = _ldb._connect()
        for row in _lconn.execute(
            "SELECT name, namespace, org_tags, project, group_name FROM brick_definitions"
        ).fetchall():
            raw_tags = row[2]
            try:
                _tags = _list_json.loads(raw_tags) if raw_tags else []
            except (ValueError, TypeError):
                _tags = []
            _org_map[row[0]] = {
                "namespace": row[1] or "",
                "tags": _tags,
                "project": row[3] or "",
                "group": row[4] or "",
            }
        _lconn.close()
    except Exception:
        pass

    return {
        "bricks": [
            {
                "name": b.name,
                "type": b.type,
                "description": b.description,
                "when_to_use": b.when_to_use,
                "category": b.category,
                **_org_map.get(b.name, {"namespace": "", "tags": [], "project": "", "group": ""}),
            }
            for b in bricks
        ],
        "total": len(bricks),
        "categories": _registry.get_categories(),
    }


async def _handle_search_bricks(arguments: dict) -> dict:
    """Search bricks by keyword."""
    query = arguments.get("query", "")
    category = arguments.get("category")

    results = _registry.search(query, category=category)

    return {
        "query": query,
        "results": [
            {
                "name": b.name,
                "type": b.type,
                "description": b.description,
                "when_to_use": b.when_to_use,
                "category": b.category,
            }
            for b in results
        ],
        "total": len(results),
    }


async def _handle_get_brick_schema(arguments: dict) -> dict:
    """Get full schema for a specific brick."""
    name = arguments.get("brick_name", "")
    brick = _registry.get(name)

    if not brick:
        return {
            "success": False,
            "error": f"Brick '{name}' not found. Use brix__list_bricks to see available bricks.",
        }

    # Track that this schema was consulted (T-BRIX-V8-09)
    source = _extract_source(arguments)
    record_schema_consultation(source, name)

    # Build compatible_with list: bricks whose output_type is compatible with this brick's input_type
    compatible_with: list[str] = []
    if brick.input_type:
        for other in _registry.list_all():
            if other.name == name:
                continue
            if other.output_type and is_compatible(other.output_type, brick.input_type):
                compatible_with.append(other.name)

    return {
        "name": brick.name,
        "type": brick.type,
        "description": brick.description,
        "when_to_use": brick.when_to_use,
        "category": brick.category,
        "input_type": brick.input_type,
        "output_type": brick.output_type,
        "input_description": brick.input_description,
        "output_description": brick.output_description,
        "config_schema": brick.to_json_schema(),
        "compatible_with": compatible_with,
    }


async def _handle_add_step(arguments: dict) -> dict:
    """Add a step to an existing pipeline.

    Accepts either ``brick`` (registry lookup) or ``type`` (direct flow-control
    step type).  Exactly one of the two must be provided.  Any extra keyword
    arguments beyond the standard set (params, on_error, parallel, position)
    are forwarded verbatim as step-level config — this lets callers pass
    flow-control fields like ``until``, ``max_iterations``, ``sequence``,
    ``choices``, ``values``, ``when``, ``message``, etc.
    """
    name = arguments.get("pipeline_name", arguments.get("pipeline_id", ""))
    step_id = arguments.get("step_id", "")
    brick = arguments.get("brick", "")
    direct_type = arguments.get("type", "")

    # Validate: exactly one of brick / type must be supplied
    if brick and direct_type:
        return {
            "success": False,
            "error": "Provide either 'brick' or 'type', not both.",
        }
    if not brick and not direct_type:
        return {
            "success": False,
            "error": "Either 'brick' or 'type' must be provided.",
        }

    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {"success": False, "error": str(exc)}

    if brick:
        # Map brick name to type via registry (original behaviour)
        brick_def = _registry.get(brick)
        step_type = brick_def.type if brick_def else "cli"  # safe fallback
    else:
        # Flow-control step — use the type directly, no registry lookup
        step_type = direct_type

    # Known "envelope" keys that are NOT passed through as step config
    _ENVELOPE_KEYS = {
        "pipeline_name", "pipeline_id", "step_id", "brick", "type",
        "params", "on_error", "parallel", "position",
    }

    # Build step dict — start with id + type
    step: dict = {"id": step_id, "type": step_type}
    if arguments.get("params"):
        step["params"] = arguments["params"]
    if arguments.get("on_error"):
        step["on_error"] = arguments["on_error"]
    if arguments.get("parallel"):
        step["parallel"] = arguments["parallel"]

    # Forward any extra flow-control / step-config keys verbatim
    for key, value in arguments.items():
        if key not in _ENVELOPE_KEYS and key not in step:
            step[key] = value

    # Insert at position or append
    steps: list = data.get("steps", [])
    position = arguments.get("position", "")
    if position and position.startswith("after:"):
        after_id = position[len("after:"):]
        idx = next((i for i, s in enumerate(steps) if s.get("id") == after_id), None)
        if idx is not None:
            steps.insert(idx + 1, step)
        else:
            steps.append(step)
    else:
        steps.append(step)

    data["steps"] = steps

    # Auto-bump version (minor for structural change)
    from brix.mcp_handlers.pipelines import _bump_version
    old_version = data.get("version", "1.0.0")
    data["version"] = _bump_version(old_version, "minor")

    # Validate and save
    validation = _validate_pipeline_dict(data)
    _save_pipeline_yaml(name, data)

    source = _extract_source(arguments)
    _audit_db.write_audit_entry(
        tool="brix__add_step",
        source=source,
        arguments_summary=_source_summary(source, pipeline=name, step=step_id),
    )

    result: dict = {
        "success": True,
        "pipeline_id": name,
        "step_count": len(steps),
        "validated": validation["valid"],
        "validation": validation,
    }

    # Collect warnings into a list (multiple sources)
    step_warnings: list[str] = []

    # Legacy step-type warning — stored in a separate key to avoid interfering with
    # existing warning/warnings semantics (T-BRIX-DB-05d)
    _new_type = LEGACY_ALIASES.get(step_type)
    _deprecation_warnings: list[str] = []
    if _new_type:
        _deprecation_warnings.append(
            f"DEPRECATION WARNING: Step '{step_id}' uses legacy type '{step_type}'. "
            f"Use '{_new_type}' instead."
        )

    # Compositor-Mode warning: warn if the pipeline has compositor_mode enabled
    # and the new step uses python/cli (T-BRIX-V8-07)
    is_compositor = bool(data.get("compositor_mode", False))
    allow_code_val = data.get("allow_code", True)
    if is_compositor and not allow_code_val and step_type in ("python", "cli"):
        step_warnings.append(
            f"COMPOSITOR-MODE WARNING: Step '{step_id}' has type '{step_type}' which is "
            "blocked at runtime (compositor_mode=true, allow_code=false). "
            "Use built-in bricks / mcp_call steps or set allow_code: true on the pipeline."
        )

    # Schema-consultation warning (T-BRIX-V8-09):
    # Warn if the step refers to a known brick and get_brick_schema was not called first.
    if brick and brick_def:
        source = _extract_source(arguments)
        if not was_schema_consulted(source, brick):
            step_warnings.append(
                f"HINWEIS: get_brick_schema wurde für '{brick}' nicht aufgerufen. "
                "Empfehlung: Erst Schema prüfen, dann Step hinzufügen."
            )

    # Type compatibility check with adjacent steps (T-BRIX-V8-09):
    # Find the new step's index in the steps list and check neighbours.
    new_step_index = next(
        (i for i, s in enumerate(steps) if s.get("id") == step_id), None
    )
    if new_step_index is not None and brick and brick_def:
        new_out = brick_def.output_type or ""
        new_in = brick_def.input_type or ""

        # Check compatibility with previous step
        if new_step_index > 0:
            prev_step = steps[new_step_index - 1]
            prev_brick_name = prev_step.get("brick", "")
            prev_brick_def = _registry.get(prev_brick_name) if prev_brick_name else None
            # Also try to look up by step type
            if not prev_brick_def:
                prev_type = prev_step.get("type", "")
                prev_brick_def = next(
                    (b for b in _registry.list_all() if b.type == prev_type), None
                )
            if prev_brick_def and prev_brick_def.output_type and new_in:
                if not is_compatible(prev_brick_def.output_type, new_in):
                    converter = suggest_converter(prev_brick_def.output_type, new_in)
                    conv_hint = f" Erwäge Converter: '{converter}'." if converter else ""
                    step_warnings.append(
                        f"TYP-INKOMPATIBILITÄT: Step '{prev_step.get('id', '?')}' "
                        f"liefert '{prev_brick_def.output_type}', "
                        f"aber '{step_id}' erwartet '{new_in}'.{conv_hint}"
                    )

        # Check compatibility with next step
        if new_step_index < len(steps) - 1:
            next_step = steps[new_step_index + 1]
            next_brick_name = next_step.get("brick", "")
            next_brick_def = _registry.get(next_brick_name) if next_brick_name else None
            if not next_brick_def:
                next_type = next_step.get("type", "")
                next_brick_def = next(
                    (b for b in _registry.list_all() if b.type == next_type), None
                )
            if next_brick_def and new_out and next_brick_def.input_type:
                if not is_compatible(new_out, next_brick_def.input_type):
                    converter = suggest_converter(new_out, next_brick_def.input_type)
                    conv_hint = f" Erwäge Converter: '{converter}'." if converter else ""
                    step_warnings.append(
                        f"TYP-INKOMPATIBILITÄT: Step '{step_id}' "
                        f"liefert '{new_out}', "
                        f"aber '{next_step.get('id', '?')}' erwartet '{next_brick_def.input_type}'.{conv_hint}"
                    )

    # Attach warnings to result
    if len(step_warnings) == 1:
        result["warning"] = step_warnings[0]
    elif step_warnings:
        result["warnings"] = step_warnings

    # Attach deprecation warnings separately (T-BRIX-DB-05d)
    if _deprecation_warnings:
        result["deprecation_warnings"] = _deprecation_warnings

    return result


async def _handle_remove_step(arguments: dict) -> dict:
    """Remove a step from a pipeline."""
    name = arguments.get("pipeline_name", arguments.get("pipeline_id", ""))
    step_id = arguments.get("step_id", "")
    source = _extract_source(arguments)

    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {"success": False, "error": str(exc)}

    steps: list = data.get("steps", [])
    original_count = len(steps)
    steps = [s for s in steps if s.get("id") != step_id]

    if len(steps) == original_count:
        return {
            "success": False,
            "error": f"Step '{step_id}' not found in pipeline '{name}'.",
        }

    data["steps"] = steps

    # Auto-bump version (minor for structural change)
    from brix.mcp_handlers.pipelines import _bump_version
    old_version = data.get("version", "1.0.0")
    data["version"] = _bump_version(old_version, "minor")

    _save_pipeline_yaml(name, data)

    _audit_db.write_audit_entry(
        tool="brix__remove_step",
        source=source,
        arguments_summary=_source_summary(source, pipeline=name, step=step_id),
    )

    return {
        "success": True,
        "pipeline_id": name,
        "removed_step": step_id,
        "step_count": len(steps),
    }


async def _handle_update_step(arguments: dict) -> dict:
    """Update individual parameters of an existing pipeline step."""
    name = arguments.get("pipeline_name", "")
    step_id = arguments.get("step_id", "")
    updates = arguments.get("updates", {})
    source = _extract_source(arguments)

    store = PipelineStore(pipelines_dir=_pipeline_dir())
    try:
        raw = store.load_raw(name)
    except FileNotFoundError:
        return {"success": False, "error": f"Pipeline '{name}' not found"}

    # Find step -- search top-level first, then recurse into nested containers
    steps = raw.get("steps", [])
    target = _find_step_recursive(steps, step_id)

    if not target:
        return {"success": False, "error": f"Step '{step_id}' not found in pipeline '{name}'"}

    # Apply updates (id cannot be changed)
    for key, value in updates.items():
        if key == "id":
            continue
        target[key] = value

    # Auto-bump version (patch for config change)
    from brix.mcp_handlers.pipelines import _bump_version
    old_version = raw.get("version", "1.0.0")
    raw["version"] = _bump_version(old_version, "patch")

    # Save
    store.save(raw, name)

    _audit_db.write_audit_entry(
        tool="brix__update_step",
        source=source,
        arguments_summary=_source_summary(source, pipeline=name, step=step_id),
    )

    # Validate
    try:
        store.load(name)  # Validates the updated pipeline
        return {
            "success": True,
            "step_id": step_id,
            "updated_fields": list(updates.keys()),
            "validated": True,
        }
    except Exception as e:
        return {
            "success": True,
            "step_id": step_id,
            "updated_fields": list(updates.keys()),
            "validated": False,
            "validation_error": str(e),
        }


async def _handle_get_step(arguments: dict) -> dict:
    """Return a single pipeline step by ID (searches recursively)."""
    pipeline_name = arguments.get("pipeline_name", "").strip()
    step_id = arguments.get("step_id", "").strip()

    if not pipeline_name:
        return {"success": False, "error": "Parameter 'pipeline_name' is required"}
    if not step_id:
        return {"success": False, "error": "Parameter 'step_id' is required"}

    store = PipelineStore(pipelines_dir=_pipeline_dir())
    try:
        raw = store.load_raw(pipeline_name)
    except FileNotFoundError:
        return {"success": False, "error": f"Pipeline '{pipeline_name}' not found"}

    steps = raw.get("steps", [])
    step = _find_step_recursive(steps, step_id)

    if step is None:
        return {
            "success": False,
            "error": f"Step '{step_id}' not found in pipeline '{pipeline_name}'",
        }

    return {
        "success": True,
        "pipeline_name": pipeline_name,
        "step_id": step_id,
        "step": step,
    }


async def _handle_auto_fix_step(arguments: dict) -> dict:
    """Attempt to automatically fix a failing step."""
    import json
    from brix.history import RunHistory
    from brix.mcp_handlers._shared import _re_module_name, _find_step_recursive

    run_id = arguments.get("run_id", "").strip()
    step_id = arguments.get("step_id", "").strip()

    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required."}
    if not step_id:
        return {"success": False, "error": "Parameter 'step_id' is required."}

    history = RunHistory()
    run = history.get_run(run_id)
    if run is None:
        return {"success": False, "error": f"Run '{run_id}' not found in history."}

    steps_json = run.get("steps_data")
    if not steps_json:
        return {"success": False, "error": f"No step data recorded for run '{run_id}'."}

    try:
        steps = json.loads(steps_json)
    except (json.JSONDecodeError, TypeError):
        return {"success": False, "error": "Could not parse steps_data for this run."}

    step_data = steps.get(step_id)
    if step_data is None:
        return {"success": False, "error": f"Step '{step_id}' not found in run '{run_id}'."}

    if step_data.get("status") != "error":
        return {
            "success": False,
            "error": f"Step '{step_id}' did not fail (status={step_data.get('status')!r}).",
        }

    err_msg = step_data.get("error_message") or step_data.get("errors") or ""
    if not isinstance(err_msg, str):
        err_msg = str(err_msg)

    pipeline_name = run.get("pipeline", "")
    store = PipelineStore(pipelines_dir=_pipeline_dir())

    # --- Fix 1: ModuleNotFoundError → pip install ---
    if "ModuleNotFoundError" in err_msg:
        module = _re_module_name(err_msg)
        if module:
            from brix.deps import install_requirements
            ok = install_requirements([module])
            if ok:
                return {
                    "fixed": True,
                    "action": f"installed {module}",
                    "rerun_hint": (
                        f"brix__run_pipeline(pipeline_id='{pipeline_name}') — "
                        f"module '{module}' is now installed."
                    ),
                }
            return {
                "fixed": False,
                "action": f"pip install {module} failed",
                "rerun_hint": f"Add '{module}' to requirements.txt and rebuild the container.",
            }
        return {
            "fixed": False,
            "action": "could not parse module name from error",
            "rerun_hint": "Inspect the error message manually and install the missing module.",
        }

    # --- Fix 2: UndefinedError / is undefined → add | default('') ---
    if "UndefinedError" in err_msg or "is undefined" in err_msg:
        # Locate the step in the pipeline YAML and patch string values
        try:
            raw = store.load_raw(pipeline_name)
        except FileNotFoundError:
            return {
                "fixed": False,
                "action": "pipeline not found in store",
                "rerun_hint": f"Pipeline '{pipeline_name}' could not be loaded for patching.",
            }

        steps_list = raw.get("steps", [])
        target = _find_step_recursive(steps_list, step_id)
        if target is None:
            return {
                "fixed": False,
                "action": "step not found in pipeline YAML",
                "rerun_hint": "Manually add | default('') to undefined Jinja2 references.",
            }

        # Patch all string values (recursively) that contain {{ ... }} without | default
        import re as _re2

        def _patch_jinja(obj: object) -> list[str]:
            """Recursively patch Jinja2 expressions missing | default. Returns patched keys."""
            changed: list[str] = []
            if isinstance(obj, dict):
                for k, v in list(obj.items()):
                    if isinstance(v, str) and "{{" in v and "default" not in v:
                        obj[k] = _re2.sub(r"\{\{\s*([^}]+?)\s*\}\}", r"{{ \1 | default('') }}", v)
                        changed.append(k)
                    else:
                        changed.extend(_patch_jinja(v))
            elif isinstance(obj, list):
                for item in obj:
                    changed.extend(_patch_jinja(item))
            return changed

        patched_keys = _patch_jinja(target)

        if patched_keys:
            store.save(raw, pipeline_name)
            return {
                "fixed": True,
                "action": f"added | default('') to fields: {patched_keys}",
                "rerun_hint": (
                    f"brix__run_pipeline(pipeline_id='{pipeline_name}') — "
                    "undefined Jinja2 variables now have a default fallback."
                ),
            }
        return {
            "fixed": False,
            "action": "no patchable Jinja2 expressions found in step",
            "rerun_hint": "Manually add | default('') to the undefined reference.",
        }

    # --- Fix 3: Timeout → double the timeout value ---
    if "Timeout" in err_msg:
        try:
            raw = store.load_raw(pipeline_name)
        except FileNotFoundError:
            return {
                "fixed": False,
                "action": "pipeline not found in store",
                "rerun_hint": f"Pipeline '{pipeline_name}' could not be loaded for patching.",
            }

        steps_list = raw.get("steps", [])
        target = _find_step_recursive(steps_list, step_id)
        if target is None:
            return {
                "fixed": False,
                "action": "step not found in pipeline YAML",
                "rerun_hint": "Manually increase the timeout value for this step.",
            }

        old_timeout = target.get("timeout")
        try:
            new_timeout = int(old_timeout) * 2 if old_timeout is not None else 120
        except (TypeError, ValueError):
            new_timeout = 120

        target["timeout"] = new_timeout
        store.save(raw, pipeline_name)

        return {
            "fixed": True,
            "action": f"timeout doubled from {old_timeout} to {new_timeout}",
            "rerun_hint": (
                f"brix__run_pipeline(pipeline_id='{pipeline_name}') — "
                f"timeout for step '{step_id}' is now {new_timeout}s."
            ),
        }

    return {
        "fixed": False,
        "action": "no automatic fix available for this error type",
        "rerun_hint": "Review the error message and apply a manual fix.",
    }
