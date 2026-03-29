"""Pipeline handler module — CRUD, versioning, search, template."""
from __future__ import annotations

import uuid as _uuid_mod
from pathlib import Path

import yaml


def _bump_version(current: str, bump: str = "patch") -> str:
    """Bump a semver string. bump='patch'|'minor'|'major'."""
    try:
        parts = current.split(".")
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])
    except (IndexError, ValueError):
        return "1.0.1"
    if bump == "major":
        return f"{major + 1}.0.0"
    elif bump == "minor":
        return f"{major}.{minor + 1}.0"
    else:
        return f"{major}.{minor}.{patch + 1}"

from brix.mcp_handlers._shared import (
    _audit_db,
    _registry,
    _extract_source,
    _source_summary,
    _load_pipeline_yaml,
    _save_pipeline_yaml,
    _validate_pipeline_dict,
    _pipeline_dir,
    _pipeline_path,
    _find_similar_pipelines,
    _scan_pipelines_for_sub_pipeline,
    _now_iso_helper,
)
from brix.bricks.types import is_compatible, suggest_converter
from brix.pipeline_store import PipelineStore
from brix.history import RunHistory
from brix.config import config
from brix.engine import LEGACY_ALIASES


def _resolve_brick_def(step: dict):
    """Resolve a BrickSchema for a given step dict, or None if not found.

    Tries: step['brick'] by name, then step['type'] matched to a brick's type field.
    """
    brick_name = step.get("brick", "")
    if brick_name:
        return _registry.get(brick_name)
    step_type = step.get("type", "")
    if step_type:
        return next((b for b in _registry.list_all() if b.type == step_type), None)
    return None


def _check_step_type_compatibility(steps: list, warnings: list) -> None:
    """Inspect step pairs for output/input type incompatibilities.

    Appends warning strings to `warnings` for each incompatible pair found.
    Only warns; never raises.
    """
    for i in range(len(steps) - 1):
        step_a = steps[i]
        step_b = steps[i + 1]
        if not isinstance(step_a, dict) or not isinstance(step_b, dict):
            continue
        brick_a = _resolve_brick_def(step_a)
        brick_b = _resolve_brick_def(step_b)
        if not brick_a or not brick_b:
            continue
        out_type = brick_a.output_type or ""
        in_type = brick_b.input_type or ""
        if not out_type or not in_type:
            continue
        if not is_compatible(out_type, in_type):
            converter = suggest_converter(out_type, in_type)
            conv_hint = f" Erwäge Converter: '{converter}'." if converter else ""
            warnings.append(
                f"TYP-INKOMPATIBILITÄT: Step '{step_a.get('id', '?')}' liefert '{out_type}', "
                f"aber Step '{step_b.get('id', '?')}' erwartet '{in_type}'.{conv_hint}"
            )


async def _handle_create_pipeline(arguments: dict) -> dict:
    """Create a new pipeline, optionally with inline steps."""
    name = arguments.get("name", "")
    if not name:
        return {"success": False, "error": "Pipeline 'name' is required."}

    source = _extract_source(arguments)
    description = arguments.get("description", "")
    version = arguments.get("version", "1.0.0")
    steps_raw = arguments.get("steps", [])
    input_schema = arguments.get("input_schema", {})
    requirements = arguments.get("requirements", [])
    credentials = arguments.get("credentials")
    error_handling = arguments.get("error_handling")
    groups = arguments.get("groups")
    output = arguments.get("output")
    compositor_mode = arguments.get("compositor_mode")
    allow_code = arguments.get("allow_code")
    # Project organisation (T-BRIX-ORG-01)
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None

    # Build pipeline dict
    pipeline_data: dict = {
        "name": name,
        "version": version,
        "steps": steps_raw or [],
    }
    if description:
        pipeline_data["description"] = description
    if input_schema:
        pipeline_data["input"] = input_schema
    if requirements:
        pipeline_data["requirements"] = requirements
    if credentials is not None:
        pipeline_data["credentials"] = credentials
    if error_handling is not None:
        pipeline_data["error_handling"] = error_handling
    if groups is not None:
        pipeline_data["groups"] = groups
    if output is not None:
        pipeline_data["output"] = output
    if compositor_mode is not None:
        pipeline_data["compositor_mode"] = bool(compositor_mode)
    if allow_code is not None:
        pipeline_data["allow_code"] = bool(allow_code)

    # Assign stable UUID (preserve existing if pipeline already exists)
    try:
        existing_raw = _load_pipeline_yaml(name)
        pipeline_id = existing_raw.get("id") or str(_uuid_mod.uuid4())
    except FileNotFoundError:
        pipeline_id = str(_uuid_mod.uuid4())
    pipeline_data["id"] = pipeline_id

    # Duplicate detection: find similar existing pipelines
    pipeline_warnings: list[str] = []
    similar_pipelines = _find_similar_pipelines(name, description)
    for match in similar_pipelines:
        pipeline_warnings.append(
            f"WARNING: Ähnliche Pipeline '{match['name']}' existiert bereits "
            f"({match['reason']}). Prüfe ob du die bestehende nutzen oder erweitern kannst."
        )

    # Compositor-Mode warning: flag python/cli steps when compositor_mode is set (T-BRIX-V8-07)
    is_compositor = bool(pipeline_data.get("compositor_mode", False))
    allow_code_explicit = pipeline_data.get("allow_code", True)
    if is_compositor and not allow_code_explicit:
        blocked_types = {"python", "cli"}
        code_step_ids = [
            s.get("id", "?")
            for s in (steps_raw or [])
            if isinstance(s, dict) and s.get("type") in blocked_types
        ]
        if code_step_ids:
            pipeline_warnings.append(
                f"COMPOSITOR-MODE WARNING: Steps {code_step_ids} use python/cli which are "
                "blocked at runtime. Use built-in bricks / mcp_call or set allow_code: true."
            )

    # Type compatibility check across inline steps (T-BRIX-V8-09)
    _check_step_type_compatibility(steps_raw or [], pipeline_warnings)

    # Legacy step-type warning (T-BRIX-DB-05d)
    for step in (steps_raw or []):
        if not isinstance(step, dict):
            continue
        step_type = step.get("type", "")
        new_type = LEGACY_ALIASES.get(step_type)
        if new_type:
            pipeline_warnings.append(
                f"DEPRECATION WARNING: Step '{step.get('id', '?')}' uses legacy type "
                f"'{step_type}'. Use '{new_type}' instead."
            )

    # Validate
    validation = _validate_pipeline_dict(pipeline_data)

    # Save regardless (agent can fix errors via add_step / validate)
    _save_pipeline_yaml(name, pipeline_data)

    # Update project/tags/group_name in DB (T-BRIX-ORG-01)
    if org_project is not None or org_tags is not None or org_group is not None:
        try:
            from brix.db import BrixDB as _BrixDB
            _org_db = _BrixDB()
            _org_db.upsert_pipeline(
                name=name,
                path=str(_pipeline_path(name)),
                project=org_project,
                tags=org_tags,
                group_name=org_group,
            )
        except Exception:
            pass  # Non-fatal — org fields are metadata only

    # Audit log
    _audit_db.write_audit_entry(
        tool="brix__create_pipeline",
        source=source,
        arguments_summary=_source_summary(source, pipeline=name),
    )

    # Org enforcement warnings (project/tags mandatory hints)
    if org_project is None:
        pipeline_warnings.append(
            "MISSING PROJECT: Bitte 'project' angeben (z.B. 'buddy', 'cody', 'utility')."
        )
    if not description:
        pipeline_warnings.append(
            "MISSING DESCRIPTION: Bitte 'description' angeben."
        )
    if org_tags is None:
        pipeline_warnings.append(
            "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['email', 'import'])."
        )

    result: dict = {
        "success": True,
        "pipeline_id": name,
        "id": pipeline_id,
        "pipeline_path": str(_pipeline_path(name)),
        "step_count": len(steps_raw or []),
        "validated": validation["valid"],
        "validation": validation,
    }
    if org_project is not None:
        result["project"] = org_project
    if pipeline_warnings:
        result["warnings"] = pipeline_warnings
    return result


async def _handle_get_pipeline(arguments: dict) -> dict:
    """Get pipeline definition by name."""
    name = arguments.get("pipeline_id", "")
    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {"success": False, "error": str(exc)}

    steps = data.get("steps", [])
    result: dict = {
        "name": data.get("name", name),
        "version": data.get("version", "1.0.0"),
        "description": data.get("description", ""),
        "step_count": len(steps),
        "steps": steps,
        "input": data.get("input", {}),
        "credentials": data.get("credentials", {}),
        "output": data.get("output", {}),
        "requirements": data.get("requirements", []),
        "pipeline_path": str(_pipeline_path(name)),
    }
    if data.get("id"):
        result["id"] = data["id"]
    if data.get("created_at"):
        result["created_at"] = data["created_at"]
    if data.get("updated_at"):
        result["updated_at"] = data["updated_at"]

    # Add org fields from DB (T-BRIX-ORG-01)
    try:
        from brix.db import BrixDB as _OrgDB
        _odb = _OrgDB()
        _oconn = _odb._connect()
        _orow = _oconn.execute(
            "SELECT project, tags, group_name FROM pipelines WHERE name = ?", (name,)
        ).fetchone()
        if _orow:
            result["project"] = _orow[0] or ""
            import json as _json
            try:
                result["tags"] = _json.loads(_orow[1]) if _orow[1] else []
            except (ValueError, TypeError):
                result["tags"] = []
            result["group"] = _orow[2] or ""
        _oconn.close()
    except Exception:
        pass

    return result


async def _handle_update_pipeline(arguments: dict) -> dict:
    """Update pipeline metadata without touching steps.

    Supports: input_schema, version, description, requirements,
    credentials, error_handling, groups, output.
    """
    from brix.system_pipelines import is_system_pipeline

    name = arguments.get("name", "")
    if not name:
        return {"success": False, "error": "Pipeline 'name' is required."}

    # _system/ pipelines can be updated but emit a warning
    _system_warning: str | None = None
    if is_system_pipeline(name):
        _system_warning = (
            f"Warnung: '{name}' ist eine System-Pipeline. "
            "Änderungen können das Systemverhalten beeinflussen."
        )

    source = _extract_source(arguments)
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    try:
        raw = store.load_raw(name)
    except FileNotFoundError:
        return {"success": False, "error": f"Pipeline '{name}' not found."}

    changed_fields: list[str] = []

    if "input_schema" in arguments and arguments["input_schema"] is not None:
        raw["input"] = arguments["input_schema"]
        changed_fields.append("input_schema")

    if "version" in arguments and arguments["version"] is not None:
        raw["version"] = arguments["version"]
        changed_fields.append("version")

    if "description" in arguments and arguments["description"] is not None:
        raw["description"] = arguments["description"]
        changed_fields.append("description")

    if "requirements" in arguments and arguments["requirements"] is not None:
        raw["requirements"] = arguments["requirements"]
        changed_fields.append("requirements")

    if "credentials" in arguments and arguments["credentials"] is not None:
        raw["credentials"] = arguments["credentials"]
        changed_fields.append("credentials")

    if "error_handling" in arguments and arguments["error_handling"] is not None:
        raw["error_handling"] = arguments["error_handling"]
        changed_fields.append("error_handling")

    if "groups" in arguments and arguments["groups"] is not None:
        raw["groups"] = arguments["groups"]
        changed_fields.append("groups")

    if "output" in arguments and arguments["output"] is not None:
        raw["output"] = arguments["output"]
        changed_fields.append("output")

    # Project organisation (T-BRIX-ORG-01)
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None
    has_org_update = (org_project is not None or org_tags is not None or org_group is not None)

    if not changed_fields and not has_org_update:
        return {
            "success": True,
            "pipeline_name": name,
            "changed_fields": [],
            "message": "No fields provided — pipeline unchanged.",
        }

    if changed_fields:
        # Auto-bump version (patch for config changes, unless version was explicitly set)
        if "version" not in changed_fields:
            old_version = raw.get("version", "1.0.0")
            raw["version"] = _bump_version(old_version, "patch")
            changed_fields.append("version (auto-bump)")
        store.save(raw, name)

    # Update project/tags/group_name in DB (T-BRIX-ORG-01)
    if has_org_update:
        try:
            from brix.db import BrixDB as _BrixDB
            _org_db = _BrixDB()
            _org_db.upsert_pipeline(
                name=name,
                path=str(_pipeline_path(name)),
                project=org_project,
                tags=org_tags,
                group_name=org_group,
            )
            if org_project is not None:
                changed_fields.append("project")
            if org_tags is not None:
                changed_fields.append("tags")
            if org_group is not None:
                changed_fields.append("group")
        except Exception:
            pass  # Non-fatal

    # Validate after save (only if YAML was changed)
    validated = True
    validation_error = None
    if changed_fields:
        try:
            store.load(name)
        except Exception as exc:
            validated = False
            validation_error = str(exc)

    _audit_db.write_audit_entry(
        tool="brix__update_pipeline",
        source=source,
        arguments_summary=_source_summary(
            source, pipeline=name, fields=",".join(changed_fields)
        ),
    )

    result: dict = {
        "success": True,
        "pipeline_name": name,
        "changed_fields": changed_fields,
        "validated": validated,
    }
    if validation_error:
        result["validation_error"] = validation_error
    if _system_warning:
        result["warning"] = _system_warning
        result["system_pipeline"] = True
    return result


async def _handle_delete_pipeline(arguments: dict) -> dict:
    """Delete a pipeline YAML. Warns if run history exists unless force=true."""
    from brix.system_pipelines import is_system_pipeline

    name = arguments.get("name", "").strip()
    force = bool(arguments.get("force", False))
    source = _extract_source(arguments)

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    # _system/ pipelines are protected from deletion
    if is_system_pipeline(name):
        return {
            "success": False,
            "error": f"System-Pipelines können nicht gelöscht werden: '{name}'",
            "system_pipeline": True,
        }

    store = PipelineStore(pipelines_dir=_pipeline_dir())
    if not store.exists(name):
        return {"success": False, "error": f"Pipeline '{name}' not found"}

    # Check for run history
    if not force:
        history = RunHistory()
        runs = history.get_recent(limit=config.HISTORY_LIST_LIMIT)
        matching = [r for r in runs if r.get("pipeline") == name]
        if matching:
            return {
                "success": False,
                "warning": (
                    f"Pipeline '{name}' has {len(matching)} run(s) in history. "
                    "Use force=true to delete anyway."
                ),
                "run_count": len(matching),
            }

    # Find the actual path before deleting
    deleted_path: str = ""
    for search_dir in store.search_paths:
        for ext in [".yaml", ".yml"]:
            candidate = Path(search_dir) / f"{name}{ext}"
            if candidate.exists():
                deleted_path = str(candidate)
                break
        if deleted_path:
            break

    deleted = store.delete(name)
    if deleted:
        # Cleanup referential data
        try:
            from brix.db import BrixDB as _CleanDB
            _cdb = _CleanDB()
            _cconn = _cdb._connect()
            _cconn.execute("DELETE FROM deprecated_usage WHERE pipeline_name = ?", (name,))
            _cconn.commit()
            _cconn.close()
        except Exception:
            pass
        _audit_db.write_audit_entry(
            tool="brix__delete_pipeline",
            source=source,
            arguments_summary=_source_summary(source, pipeline=name),
        )
        return {
            "success": True,
            "deleted_pipeline": name,
            "deleted_path": deleted_path,
        }
    return {"success": False, "error": f"Could not delete pipeline '{name}'"}


async def _handle_rename_pipeline(arguments: dict) -> dict:
    """Rename a pipeline: file + YAML name field + db index. UUID is preserved."""
    old_name = arguments.get("old_name", "").strip()
    new_name = arguments.get("new_name", "").strip()

    if not old_name:
        return {"success": False, "error": "Parameter 'old_name' is required"}
    if not new_name:
        return {"success": False, "error": "Parameter 'new_name' is required"}
    if old_name == new_name:
        return {"success": False, "error": "old_name and new_name must be different"}

    store = PipelineStore(pipelines_dir=_pipeline_dir())

    if not store.exists(old_name):
        return {"success": False, "error": f"Pipeline '{old_name}' not found"}
    if store.exists(new_name):
        return {"success": False, "error": f"Pipeline '{new_name}' already exists"}

    # Load raw data (preserves all fields)
    try:
        data = store.load_raw(old_name)
    except FileNotFoundError:
        return {"success": False, "error": f"Pipeline '{old_name}' not found"}

    # Locate old YAML file (may be in search paths, not necessarily pipelines_dir)
    old_path: "Path | None" = None
    for search_dir in store.search_paths:
        for ext in [".yaml", ".yml"]:
            candidate = Path(search_dir) / f"{old_name}{ext}"
            if candidate.exists():
                old_path = candidate
                break
        if old_path:
            break

    if old_path is None:
        return {"success": False, "error": f"Could not locate YAML file for '{old_name}'"}

    # Update the name field inside the data
    data["name"] = new_name

    # Save under new name (this also writes db index and archives a version)
    store.save(data, name=new_name)

    # Delete old file + db entry
    # Remove old YAML directly (store.delete only looks in pipelines_dir)
    try:
        old_path.unlink()
    except OSError as exc:
        return {
            "success": False,
            "error": f"New file saved as '{new_name}' but could not remove old file: {exc}",
        }
    # Remove old db entry
    store._db.delete_pipeline(old_name)

    # Warn if other pipelines reference the old name as a sub-pipeline
    affected = _scan_pipelines_for_sub_pipeline(old_name)

    result: dict = {
        "success": True,
        "old_name": old_name,
        "new_name": new_name,
    }
    if affected:
        result["warning"] = (
            f"The following pipelines reference '{old_name}' as a sub-pipeline and may need updating: "
            + ", ".join(affected)
        )
        result["affected_pipelines"] = affected

    return result


async def _handle_validate_pipeline(arguments: dict) -> dict:
    """Validate a pipeline without running it."""
    name = arguments.get("pipeline_id", "")
    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {"success": False, "error": str(exc)}

    validation = _validate_pipeline_dict(data)
    return {
        "success": True,
        "pipeline_id": name,
        "valid": validation["valid"],
        "errors": validation["errors"],
        "warnings": validation["warnings"],
        "checks": validation["checks"],
    }


async def _handle_list_pipelines(arguments: dict) -> dict:
    """List all pipeline YAML files, with optional project/tags/group filter."""
    directory = arguments.get("directory")
    # T-BRIX-ORG-01: project/tags/group filter
    filter_project = arguments.get("project") or None
    filter_tags = arguments.get("tags") or None
    filter_group = arguments.get("group") or None
    has_org_filter = (filter_project is not None or filter_tags is not None or filter_group is not None)

    if directory:
        # Explicit directory: scan that single directory only
        search_dir = Path(directory)
        pipelines = []
        if search_dir.exists():
            for yaml_file in sorted(search_dir.glob("*.yaml")):
                try:
                    with open(yaml_file) as f:
                        data = yaml.safe_load(f) or {}
                    steps = data.get("steps", [])
                    pipelines.append({
                        "name": data.get("name", yaml_file.stem),
                        "version": data.get("version", ""),
                        "description": data.get("description", ""),
                        "step_count": len(steps),
                        "file": str(yaml_file),
                    })
                except Exception as exc:
                    pipelines.append({
                        "name": yaml_file.stem,
                        "error": str(exc),
                        "file": str(yaml_file),
                    })
        return {
            "success": True,
            "pipelines": pipelines,
            "total": len(pipelines),
            "directory": str(search_dir),
        }
    elif has_org_filter:
        # Use DB-filtered query when org filters are present (T-BRIX-ORG-01)
        try:
            from brix.db import BrixDB as _BrixDB
            _org_db = _BrixDB()
            db_rows = _org_db.list_pipelines(
                project=filter_project,
                group_name=filter_group,
                tags=filter_tags,
            )
            pipelines = [
                {
                    "name": p["name"],
                    "version": p.get("version", ""),
                    "description": p.get("description", ""),
                    "step_count": p.get("steps", 0),
                    "file": p.get("path", ""),
                    "project": p.get("project", ""),
                    "tags": p.get("tags", []),
                    "group": p.get("group_name", ""),
                }
                for p in db_rows
            ]
        except Exception:
            pipelines = []
        return {
            "success": True,
            "pipelines": pipelines,
            "total": len(pipelines),
            "directory": "multi-path",
            "filter": {
                "project": filter_project,
                "tags": filter_tags,
                "group": filter_group,
            },
        }
    else:
        # No directory specified: use PipelineStore with current PIPELINE_DIR
        # (respects monkeypatching) and multi-path search
        store = PipelineStore(pipelines_dir=_pipeline_dir())
        all_pipelines = store.list_all()
        # Normalise field names to match the explicit-dir branch
        # Enrich with org fields from DB
        _org_map: dict = {}
        try:
            from brix.db import BrixDB as _ListDB
            import json as _list_json
            _ldb = _ListDB()
            _lconn = _ldb._connect()
            for row in _lconn.execute("SELECT name, project, tags, group_name FROM pipelines").fetchall():
                try:
                    _tags = _list_json.loads(row[1 + 1]) if row[1 + 1] else []
                except (ValueError, TypeError):
                    _tags = []
                _org_map[row[0]] = {"project": row[1] or "", "tags": _tags, "group": row[3] or ""}
            _lconn.close()
        except Exception:
            pass

        pipelines = [
            {
                "name": p["name"],
                "version": p.get("version", ""),
                "description": p.get("description", ""),
                "step_count": p.get("steps", 0),
                "file": p.get("path", ""),
                **_org_map.get(p["name"], {"project": "", "tags": [], "group": ""}),
            }
            for p in all_pipelines
        ]
        result_list: dict = {
            "success": True,
            "pipelines": pipelines,
            "total": len(pipelines),
            "directory": "multi-path",
        }
        # Hint if any pipelines lack a project
        no_project_count = sum(1 for p in pipelines if not p.get("project"))
        if no_project_count > 0:
            result_list["hint"] = (
                f"{no_project_count} pipeline(s) haben kein Projekt. "
                "Nutze update_pipeline(project=...) um sie zuzuordnen."
            )
        return result_list


async def _handle_search_pipelines(arguments: dict) -> dict:
    """Search pipelines by name/description substring."""
    query = arguments.get("query", "").strip()
    if not query:
        return {"success": False, "error": "Parameter 'query' is required."}

    store = PipelineStore(pipelines_dir=_pipeline_dir())
    all_pipelines = store.list_all()
    q = query.lower()
    matches = [
        p for p in all_pipelines
        if q in p.get("name", "").lower() or q in p.get("description", "").lower()
    ]
    return {
        "success": True,
        "query": query,
        "results": matches,
        "total": len(matches),
    }


async def _handle_get_versions(arguments: dict) -> dict:
    """List archived versions for a pipeline or helper."""
    from brix.db import BrixDB
    obj_type = arguments.get("type", "").strip()
    name = arguments.get("name", "").strip()

    if obj_type not in ("pipeline", "helper"):
        return {"success": False, "error": "Parameter 'type' must be 'pipeline' or 'helper'"}
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    db = BrixDB()
    rows = db.get_object_versions(obj_type, name)
    versions = []
    for row in rows:
        content_raw = row.get("content", "")
        versions.append({
            "version_id": row["version_id"],
            "created_at": row["created_at"],
            "size": len(content_raw),
        })

    return {
        "success": True,
        "type": obj_type,
        "name": name,
        "versions": versions,
        "count": len(versions),
    }


def _get_current_content_str(obj_type: str, name: str) -> str:
    """Return the current live content of a pipeline or helper as a string."""
    from brix.helper_registry import HelperRegistry
    import json

    if obj_type == "pipeline":
        try:
            data = _load_pipeline_yaml(name)
            return yaml.dump(data, default_flow_style=False, allow_unicode=True)
        except FileNotFoundError:
            return ""
    else:
        registry = HelperRegistry()
        entry = registry.get(name)
        if entry is None:
            return ""
        script_path = Path(entry.script) if entry.script else None
        if script_path and script_path.exists():
            try:
                return script_path.read_text(encoding="utf-8")
            except Exception:
                return ""
        return ""


def _version_content_str(version_record: dict, obj_type: str) -> str:
    """Extract a human-readable string from a version record's JSON content."""
    import json
    try:
        raw = json.loads(version_record["content"])
    except Exception:
        return version_record.get("content", "")

    if obj_type == "pipeline":
        # raw is the pipeline dict — dump as YAML for readability
        return yaml.dump(raw, default_flow_style=False, allow_unicode=True)
    else:
        # raw is {"code": "...", "meta": {...}}
        return raw.get("code", "")


async def _handle_rollback(arguments: dict) -> dict:
    """Restore a pipeline or helper to a previously archived version."""
    import json
    from brix.db import BrixDB
    from brix.helper_registry import HelperRegistry
    from brix.mcp_handlers._shared import _managed_helper_dir

    obj_type = arguments.get("type", "").strip()
    name = arguments.get("name", "").strip()
    version_id = arguments.get("version_id", "").strip()

    if obj_type not in ("pipeline", "helper"):
        return {"success": False, "error": "Parameter 'type' must be 'pipeline' or 'helper'"}
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}
    if not version_id:
        return {"success": False, "error": "Parameter 'version_id' is required"}

    db = BrixDB()
    version_record = db.get_object_version(version_id)
    if version_record is None:
        return {"success": False, "error": f"Version '{version_id}' not found"}

    if version_record.get("type") != obj_type or version_record.get("name") != name:
        return {
            "success": False,
            "error": (
                f"Version '{version_id}' belongs to {version_record.get('type')}/{version_record.get('name')}, "
                f"not {obj_type}/{name}"
            ),
        }

    try:
        raw = json.loads(version_record["content"])
    except Exception as exc:
        return {"success": False, "error": f"Failed to parse archived content: {exc}"}

    if obj_type == "pipeline":
        # raw is the pipeline dict — save it back via PipelineStore
        try:
            store = PipelineStore(pipelines_dir=_pipeline_dir())
            store.save(raw, name)
        except Exception as exc:
            return {"success": False, "error": f"Failed to restore pipeline: {exc}"}
        return {
            "success": True,
            "type": obj_type,
            "name": name,
            "version_id": version_id,
            "restored_at": raw.get("updated_at", ""),
        }
    else:
        # raw is {"code": "...", "meta": {...}}
        code = raw.get("code", "")
        meta = raw.get("meta", {})
        helpers_dir = _managed_helper_dir()
        script_path = helpers_dir / f"{name}.py"
        try:
            script_path.write_text(code, encoding="utf-8")
        except OSError as exc:
            return {"success": False, "error": f"Could not write helper file: {exc}"}

        registry = HelperRegistry()
        existing = registry.get(name)
        if existing is None:
            # Re-register from archived meta
            registry.register(
                name=name,
                script=str(script_path),
                description=meta.get("description", ""),
                requirements=meta.get("requirements", []),
                input_schema=meta.get("input_schema", {}),
                output_schema=meta.get("output_schema", {}),
            )
        else:
            registry.update(
                name,
                script=str(script_path),
                description=meta.get("description", existing.description),
                requirements=meta.get("requirements", existing.requirements),
                input_schema=meta.get("input_schema", existing.input_schema),
                output_schema=meta.get("output_schema", existing.output_schema),
            )
        return {
            "success": True,
            "type": obj_type,
            "name": name,
            "version_id": version_id,
            "script_path": str(script_path),
        }


async def _handle_diff_versions(arguments: dict) -> dict:
    """Return a unified diff between two archived versions (or 'current')."""
    import difflib as _difflib
    from brix.db import BrixDB

    obj_type = arguments.get("type", "").strip()
    name = arguments.get("name", "").strip()
    vid_a = arguments.get("version_id_a", "").strip()
    vid_b = arguments.get("version_id_b", "").strip()

    if obj_type not in ("pipeline", "helper"):
        return {"success": False, "error": "Parameter 'type' must be 'pipeline' or 'helper'"}
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}
    if not vid_a or not vid_b:
        return {"success": False, "error": "Both 'version_id_a' and 'version_id_b' are required"}

    db = BrixDB()

    def _resolve(vid: str) -> "tuple[str, str | None]":
        """Return (content_str, error_msg). 'current' resolves to live file."""
        if vid == "current":
            return _get_current_content_str(obj_type, name), None
        record = db.get_object_version(vid)
        if record is None:
            return "", f"Version '{vid}' not found"
        return _version_content_str(record, obj_type), None

    content_a, err_a = _resolve(vid_a)
    if err_a:
        return {"success": False, "error": err_a}
    content_b, err_b = _resolve(vid_b)
    if err_b:
        return {"success": False, "error": err_b}

    lines_a = content_a.splitlines(keepends=True)
    lines_b = content_b.splitlines(keepends=True)
    diff_lines = list(_difflib.unified_diff(
        lines_a, lines_b,
        fromfile=f"{name} ({vid_a})",
        tofile=f"{name} ({vid_b})",
    ))
    diff_str = "".join(diff_lines) or "(no differences)"

    return {
        "success": True,
        "type": obj_type,
        "name": name,
        "version_id_a": vid_a,
        "version_id_b": vid_b,
        "diff": diff_str,
        "changed": bool(diff_lines),
    }


async def _handle_get_template(arguments: dict) -> dict:
    """Return a pipeline template matching the goal, or list all templates."""
    from brix.templates.catalog import get_template, list_templates

    goal = arguments.get("goal", "")
    if not goal:
        # Return all templates
        return {"templates": list_templates()}

    template = get_template(goal)
    if template:
        return {
            "name": template["name"],
            "description": template["description"],
            "customization_points": template["customization_points"],
            "pipeline": template["pipeline"],
        }

    return {"error": f"No template found for: {goal}", "available": list_templates()}


async def _handle_test_pipeline(arguments: dict) -> dict:
    """Run a pipeline with mock data (brix test as MCP tool)."""
    from brix.testing import TestFixture, PipelineTestRunner

    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    # Resolve pipeline path
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    if not store.exists(name):
        return {"success": False, "error": f"Pipeline '{name}' not found"}

    pipeline_path: "str | None" = None
    for search_dir in store.search_paths:
        for ext in [".yaml", ".yml"]:
            candidate = Path(search_dir) / f"{name}{ext}"
            if candidate.exists():
                pipeline_path = str(candidate)
                break
        if pipeline_path:
            break

    if not pipeline_path:
        return {"success": False, "error": f"Could not locate YAML file for '{name}'"}

    fx = TestFixture(
        pipeline_path=pipeline_path,
        input_data=arguments.get("input") or {},
        mocks=arguments.get("mocks") or {},
        assertions=arguments.get("assertions") or {},
    )

    runner = PipelineTestRunner()
    try:
        test_result = await runner.run_test(fx)
    except Exception as exc:
        return {"success": False, "error": f"Test run failed: {exc}"}

    summary = test_result["summary"]
    assertion_details = [
        {
            "step_id": a.step_id,
            "assertion": a.assertion,
            "passed": a.passed,
            "message": a.message,
        }
        for a in test_result["assertions"]
    ]

    run_result = test_result["run_result"]
    step_details = {
        step_id: {
            "status": s.status,
            "duration": round(s.duration, 2),
            "items": s.items,
            "errors": s.errors,
        }
        for step_id, s in run_result.steps.items()
    }

    return {
        "success": test_result["success"],
        "pipeline": name,
        "summary": summary,
        "steps": step_details,
        "assertions": assertion_details,
    }
