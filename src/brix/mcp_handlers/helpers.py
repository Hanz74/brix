"""Helper registry handler module."""
from __future__ import annotations

import hashlib
from pathlib import Path

from brix.mcp_handlers._shared import (
    _audit_db,
    _extract_source,
    _source_summary,
    _make_helper_dict,
    _validate_python_code,
    _code_line_count,
    _find_similar_helpers,
    _scan_pipelines_for_helper,
    _now_iso_helper,
)
from brix.helper_registry import HelperRegistry
from brix.helper_governance import assess_helper_governance, governance_warnings
from brix.metadata_enforcement import (
    apply_metadata_result,
    assess_metadata_enforcement,
    blocking_metadata_response,
    extract_supplemental_metadata,
)


_HELPER_CACHE_DIR = Path("/tmp/brix-helpers")


def _validate_helper_metadata(arguments: dict, *, require_all: bool) -> str | None:
    """Validate helper metadata shape without forcing schemas upfront."""
    description = arguments.get("description")
    input_schema = arguments.get("input_schema")
    output_schema = arguments.get("output_schema")

    if "description" in arguments and description is not None and not isinstance(description, str):
        return "Parameter 'description' must be a string"
    if "input_schema" in arguments:
        if not isinstance(input_schema, dict):
            return "Parameter 'input_schema' must be an object"
    if "output_schema" in arguments:
        if not isinstance(output_schema, dict):
            return "Parameter 'output_schema' must be an object"
    return None


def _delete_helper_cache_files(name: str, content_hash: str | None = None) -> list[str]:
    """Delete cached helper files from /tmp."""
    if content_hash:
        candidates = [_HELPER_CACHE_DIR / f"{name}_{content_hash}.py"]
    else:
        candidates = list(_HELPER_CACHE_DIR.glob(f"{name}_*.py"))

    deleted: list[str] = []
    for path in candidates:
        if not path.exists():
            continue
        try:
            path.unlink()
            deleted.append(str(path))
        except OSError:
            continue
    return deleted


def _scan_db_pipelines_for_helper(helper_name: str) -> list[str]:
    """Return DB-backed pipeline names whose step rows reference the helper."""
    try:
        from brix.db import BrixDB as _BrixDB

        return _BrixDB().find_pipelines_referencing_helper(helper_name)
    except Exception:
        return []


async def _handle_create_helper(arguments: dict) -> dict:
    """Create a new Python helper script with inline code and register it."""
    name = arguments.get("name", "").strip()
    code = arguments.get("code", "")
    description = arguments.get("description", "")
    source = _extract_source(arguments)
    supplemental_metadata = extract_supplemental_metadata(arguments)

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}
    if not code:
        return {"success": False, "error": "Parameter 'code' is required"}
    metadata_error = _validate_helper_metadata(arguments, require_all=True)
    if metadata_error:
        return {"success": False, "error": metadata_error}

    # Validate Python syntax
    syntax_error = _validate_python_code(code)
    if syntax_error:
        return {"success": False, "error": f"Invalid Python code: {syntax_error}"}

    # Collect warnings (duplicate detection + linting)
    warnings: list[str] = []

    # Duplicate detection: find similar existing helpers
    similar = _find_similar_helpers(name, description)
    for match in similar:
        warnings.append(
            f"WARNING: Ähnlicher Helper '{match['name']}' existiert bereits "
            f"({match['reason']}). Prüfe ob du den bestehenden nutzen oder erweitern kannst."
        )

    # Linting: warn if code exceeds 200 lines
    line_count = _code_line_count(code)
    if line_count > 200:
        warnings.append(
            f"WARNING: Helper hat {line_count} Zeilen. Erwäge Aufteilen in kleinere Helper "
            f"oder nutze Brix-Pipelines statt großer Python-Scripts."
        )

    registry = HelperRegistry()
    content_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
    entry = registry.register(
        name=name,
        script="",
        description=description,
        requirements=arguments.get("requirements") or [],
        input_schema=arguments.get("input_schema"),
        output_schema=arguments.get("output_schema"),
        code=code,
        imports=arguments.get("imports") or [],
    )

    # Update project/tags/group_name in DB (T-BRIX-ORG-01)
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None
    governance_input = {
        "description": description,
        "input_schema": arguments.get("input_schema"),
        "output_schema": arguments.get("output_schema"),
        "project": org_project,
        "tags": org_tags,
        "reason_not_a_brick": arguments.get("reason_not_a_brick", ""),
        "brick_candidate_ref": arguments.get("brick_candidate_ref", ""),
    }
    governance = assess_helper_governance(governance_input)
    metadata_assessment = assess_metadata_enforcement(
        "helper",
        base_data={
            "description": description,
            "project": org_project,
            "governance_status": governance.status,
            "reason_not_a_brick": governance.reason_not_a_brick,
            "brick_candidate_ref": governance.brick_candidate_ref,
        },
        incoming_metadata=supplemental_metadata,
        operation="create",
    )
    try:
        from brix.db import BrixDB as _BrixDB
        _org_db = _BrixDB()
        _org_db.upsert_helper(
            name=name,
            script_path="",
            description=description,
            requirements=arguments.get("requirements") or [],
            input_schema=arguments.get("input_schema"),
            output_schema=arguments.get("output_schema"),
            code=code,
            content_hash=content_hash,
            project=org_project,
            tags=org_tags,
            group_name=org_group,
            reason_not_a_brick=governance.reason_not_a_brick,
            brick_candidate_ref=governance.brick_candidate_ref,
            governance_status=governance.status,
        )
        if metadata_assessment.stored_metadata:
            _org_db.entity_metadata_upsert("helper", name, **metadata_assessment.stored_metadata)
    except Exception:
        pass  # Non-fatal

    _audit_db.write_audit_entry(
        tool="brix__create_helper",
        source=source,
        arguments_summary=_source_summary(source, helper=name),
    )
    # Org enforcement warnings (project/tags mandatory hints)
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
            "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['email', 'import'])."
        )
    warnings.extend(governance_warnings(governance))

    result: dict = {
        "success": True,
        "action": "created",
        "helper": _make_helper_dict(entry),
    }
    result["content_hash"] = content_hash
    if org_project is not None:
        result["project"] = org_project
    result["governance"] = governance.as_dict()
    if warnings:
        result["warnings"] = warnings
    return apply_metadata_result(result, metadata_assessment)


async def _handle_register_helper(arguments: dict) -> dict:
    """Register or overwrite a helper in the Brix helper registry."""
    registry = HelperRegistry()

    name = arguments.get("name", "").strip()
    script = arguments.get("script", "").strip()
    source = _extract_source(arguments)

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}
    if not script:
        return {"success": False, "error": "Parameter 'script' is required"}

    governance = assess_helper_governance(
        {
            "description": arguments.get("description", ""),
            "input_schema": arguments.get("input_schema") or {},
            "output_schema": arguments.get("output_schema") or {},
            "project": arguments.get("project") or None,
            "tags": arguments.get("tags") or None,
            "reason_not_a_brick": arguments.get("reason_not_a_brick", ""),
            "brick_candidate_ref": arguments.get("brick_candidate_ref", ""),
        }
    )

    entry = registry.register(
        name=name,
        script=script,
        description=arguments.get("description", ""),
        requirements=arguments.get("requirements") or [],
        input_schema=arguments.get("input_schema") or {},
        output_schema=arguments.get("output_schema") or {},
    )
    # T-BRIX-BUG-01: Persist org-fields (project/tags/group) to DB
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None
    try:
        from brix.db import BrixDB as _BrixDB
        _org_db = _BrixDB()
        _org_db.upsert_helper(
            name=name,
            script_path=script,
            description=arguments.get("description", ""),
            requirements=arguments.get("requirements") or [],
            input_schema=arguments.get("input_schema") or {},
            output_schema=arguments.get("output_schema") or {},
            project=org_project,
            tags=org_tags,
            group_name=org_group,
            reason_not_a_brick=governance.reason_not_a_brick,
            brick_candidate_ref=governance.brick_candidate_ref,
            governance_status=governance.status,
        )
    except Exception:
        pass  # Non-fatal: governance metadata not saved but helper is registered
    _audit_db.write_audit_entry(
        tool="brix__register_helper",
        source=source,
        arguments_summary=_source_summary(source, helper=name),
    )
    warnings = governance_warnings(governance)
    result = {
        "success": True,
        "action": "registered",
        "helper": _make_helper_dict(entry),
        "governance": governance.as_dict(),
    }
    if warnings:
        result["warnings"] = warnings
    return result


async def _handle_list_helpers(arguments: dict) -> dict:
    """List all registered helpers, with optional project/tags/group filter."""
    include_inventory = bool(arguments.get("include_inventory", False))
    include_reuse_candidates = bool(arguments.get("include_reuse_candidates", False))
    # T-BRIX-ORG-01: project/tags/group filter
    filter_project = arguments.get("project") or None
    filter_tags = arguments.get("tags") or None
    filter_group = arguments.get("group") or None
    has_org_filter = (filter_project is not None or filter_tags is not None or filter_group is not None)

    if has_org_filter:
        try:
            from brix.db import BrixDB as _BrixDB
            _org_db = _BrixDB()
            db_rows = _org_db.list_helpers(
                project=filter_project,
                group_name=filter_group,
                tags=filter_tags,
            )
            helpers = [
                {
                    "name": h["name"],
                    "description": h.get("description", ""),
                    "script": h.get("script_path", ""),
                    "project": h.get("project", ""),
                    "tags": h.get("tags", []),
                    "group": h.get("group_name", ""),
                    "created_at": h.get("created_at", ""),
                    "updated_at": h.get("updated_at", ""),
                    "reason_not_a_brick": h.get("reason_not_a_brick", ""),
                    "brick_candidate_ref": h.get("brick_candidate_ref", ""),
                    "governance_status": h.get("governance_status", "draft"),
                }
                for h in db_rows
            ]
        except Exception:
            helpers = []
        result = {
            "success": True,
            "helpers": helpers,
            "count": len(helpers),
            "filter": {
                "project": filter_project,
                "tags": filter_tags,
                "group": filter_group,
            },
        }
        if include_inventory:
            try:
                from brix.helper_inventory import build_helper_inventory, filter_helper_inventory

                inventory_model = build_helper_inventory(_org_db)
                inventory = filter_helper_inventory(
                    inventory_model,
                    {helper["name"] for helper in helpers},
                ).as_dict()
                by_name = {item["name"]: item for item in inventory["helpers"]}
                for helper in helpers:
                    helper["inventory"] = by_name.get(helper["name"], {})
                result["inventory_summary"] = inventory["summary"]
                result["clusters"] = inventory["clusters"]
            except Exception:
                result["inventory_warning"] = "Helper inventory classification could not be loaded."
        if include_reuse_candidates:
            try:
                from brix.brick_candidate_detector import detect_brick_candidates, filter_brick_candidate_report
                from brix.helper_inventory import build_helper_inventory, filter_helper_inventory

                helper_names = {helper["name"] for helper in helpers}
                scoped_inventory = filter_helper_inventory(build_helper_inventory(_org_db), helper_names)
                pipeline_names = {
                    pipeline
                    for item in scoped_inventory.items
                    for pipeline in item.used_by_pipelines
                }
                result["reuse_candidates"] = filter_brick_candidate_report(
                    detect_brick_candidates(_org_db),
                    helper_names=helper_names,
                    pipeline_names=pipeline_names,
                ).as_dict()
                result["reuse_candidate_scope"] = "filtered_helpers"
            except Exception:
                result["reuse_candidate_warning"] = "Brick-candidate detection could not be loaded."
        return result

    registry = HelperRegistry()
    entries = registry.list_all()
    helpers_list = [_make_helper_dict(e) for e in entries]
    if include_inventory:
        try:
            from brix.helper_inventory import build_helper_inventory

            inventory = build_helper_inventory().as_dict()
            by_name = {item["name"]: item for item in inventory["helpers"]}
            for helper in helpers_list:
                helper["inventory"] = by_name.get(helper["name"], {})
        except Exception:
            inventory = {"summary": {}, "clusters": []}
    result_h: dict = {
        "success": True,
        "helpers": helpers_list,
        "count": len(helpers_list),
    }
    if include_inventory:
        result_h["inventory_summary"] = inventory["summary"]
        result_h["clusters"] = inventory["clusters"]
    if include_reuse_candidates:
        try:
            from brix.brick_candidate_detector import detect_brick_candidates

            result_h["reuse_candidates"] = detect_brick_candidates().as_dict()
        except Exception:
            result_h["reuse_candidate_warning"] = "Brick-candidate detection could not be loaded."
    # Hint if any helpers lack a project
    no_project_count = sum(1 for h in helpers_list if not h.get("project"))
    if no_project_count > 0:
        result_h["hint"] = (
            f"{no_project_count} helper(s) haben kein Projekt. "
            "Nutze update_helper(project=...) um sie zuzuordnen."
        )
    return result_h


async def _handle_get_helper(arguments: dict) -> dict:
    """Get a single helper by name."""
    registry = HelperRegistry()

    name = arguments.get("name", "")
    entry = registry.get(name)
    if entry is None:
        return {
            "success": False,
            "error": f"Helper '{name}' not found in registry",
        }
    helper = _make_helper_dict(entry)
    code = entry.code or ""
    helper.pop("script", None)
    helper["legacy_script_path"] = entry.script
    helper["source"] = "db"
    helper["has_code"] = bool(code)
    helper["code_length"] = len(code)
    helper["code_preview"] = code[:200]
    return {
        "success": True,
        "helper": helper,
    }


async def _handle_search_helpers(arguments: dict) -> dict:
    """Search helpers by keyword."""
    registry = HelperRegistry()

    query = arguments.get("query", "")
    results = registry.search(query)
    return {
        "success": True,
        "query": query,
        "helpers": [_make_helper_dict(e) for e in results],
        "count": len(results),
    }


async def _handle_update_helper(arguments: dict) -> dict:
    """Update or remove a helper from the registry."""
    registry = HelperRegistry()

    name = arguments.get("name", "")
    action = arguments.get("action", "update")
    source = _extract_source(arguments)
    supplemental_metadata = extract_supplemental_metadata(arguments)

    if action == "remove":
        removed = registry.remove(name)
        if removed:
            _audit_db.write_audit_entry(
                tool="brix__update_helper",
                source=source,
                arguments_summary=_source_summary(source, helper=name, action="remove"),
            )
            return {"success": True, "action": "removed", "name": name}
        return {"success": False, "error": f"Helper '{name}' not found in registry"}

    code = arguments.get("code")
    update_warnings: list[str] = []
    if code is not None:
        syntax_error = _validate_python_code(code)
        if syntax_error:
            return {"success": False, "error": f"Invalid Python code: {syntax_error}"}

        # Linting: warn if code exceeds 200 lines
        line_count = _code_line_count(code)
        if line_count > 200:
            update_warnings.append(
                f"WARNING: Helper hat {line_count} Zeilen. Erwäge Aufteilen in kleinere Helper "
                f"oder nutze Brix-Pipelines statt großer Python-Scripts."
            )
    metadata_error = _validate_helper_metadata(arguments, require_all=False)
    if metadata_error:
        return {"success": False, "error": metadata_error}

    # Update path
    update_fields: dict = {}
    for field_name in (
        "description", "requirements", "input_schema", "output_schema", "code", "imports",
        "reason_not_a_brick", "brick_candidate_ref",
    ):
        if field_name in arguments:
            update_fields[field_name] = arguments[field_name]

    # T-BRIX-ORG-01: project/tags/group update
    org_project = arguments.get("project") or None
    org_tags = arguments.get("tags") or None
    org_group = arguments.get("group") or None
    has_governance_update = (
        "reason_not_a_brick" in arguments
        or "brick_candidate_ref" in arguments
        or "description" in arguments
        or "input_schema" in arguments
        or "output_schema" in arguments
        or org_project is not None
        or org_tags is not None
    )
    has_org_update = (org_project is not None or org_tags is not None or org_group is not None)
    has_metadata_update = bool(supplemental_metadata)

    if not update_fields and not has_org_update and not has_metadata_update:
        return {
            "success": False,
            "error": (
                "No fields to update. Provide at least one of: code, script, description, requirements, "
                "input_schema, output_schema, project, tags, group, owner, purpose, source_intent_id, "
                "lifecycle_stage, status, usage_scope, version_relevance, linked_topic, replacement_plan, "
                "expiry_condition, reason_not_a_brick, brick_candidate_ref"
            ),
        }

    entry = None
    if update_fields:
        try:
            entry = registry.update(name, **update_fields)
        except KeyError:
            return {"success": False, "error": f"Helper '{name}' not found in registry"}

    # Update project/tags/group_name in DB
    # T-BRIX-BUG-02: Read existing DB row to preserve fields not being updated
    metadata_assessment = None
    if has_org_update or has_governance_update or has_metadata_update:
        try:
            from brix.db import BrixDB as _BrixDB
            _org_db = _BrixDB()
            existing_reg = registry.get(name)
            existing_db = _org_db.get_helper(name) if existing_reg else None
            governance = assess_helper_governance(
                {
                    "description": update_fields.get(
                        "description",
                        existing_db.get("description", "") if existing_db else "",
                    ),
                    "input_schema": update_fields.get(
                        "input_schema",
                        existing_db.get("input_schema") if existing_db else {},
                    ),
                    "output_schema": update_fields.get(
                        "output_schema",
                        existing_db.get("output_schema") if existing_db else {},
                    ),
                    "project": org_project if org_project is not None else existing_db.get("project", "") if existing_db else "",
                    "tags": org_tags if org_tags is not None else existing_db.get("tags", []) if existing_db else [],
                    "reason_not_a_brick": update_fields.get(
                        "reason_not_a_brick",
                        existing_db.get("reason_not_a_brick", "") if existing_db else "",
                    ),
                    "brick_candidate_ref": update_fields.get(
                        "brick_candidate_ref",
                        existing_db.get("brick_candidate_ref", "") if existing_db else "",
                    ),
                }
            )
            metadata_assessment = assess_metadata_enforcement(
                "helper",
                base_data={
                    "description": update_fields.get(
                        "description",
                        existing_db.get("description", "") if existing_db else "",
                    ),
                    "project": org_project if org_project is not None else existing_db.get("project", "") if existing_db else "",
                    "governance_status": governance.status,
                    "reason_not_a_brick": governance.reason_not_a_brick,
                    "brick_candidate_ref": governance.brick_candidate_ref,
                },
                incoming_metadata=supplemental_metadata,
                existing_data=existing_db or {},
                existing_metadata=_org_db.entity_metadata_get("helper", name) or {},
                operation="update",
            )
            if metadata_assessment.blocking:
                return blocking_metadata_response(metadata_assessment)
            _org_db.upsert_helper(
                name=name,
                script_path=(existing_db.get("script_path") if existing_db else "") or "",
                description=update_fields.get(
                    "description",
                    existing_db.get("description", "") if existing_db else "",
                ),
                requirements=update_fields.get("requirements", existing_db.get("requirements") if existing_db else []),
                input_schema=update_fields.get("input_schema", existing_db.get("input_schema") if existing_db else {}),
                output_schema=update_fields.get("output_schema", existing_db.get("output_schema") if existing_db else {}),
                code=update_fields.get("code", existing_db.get("code") if existing_db else None),
                content_hash=(
                    hashlib.sha256(update_fields["code"].encode("utf-8")).hexdigest()
                    if "code" in update_fields
                    else existing_db.get("content_hash", "") if existing_db else ""
                ),
                project=org_project,
                tags=org_tags,
                group_name=org_group,
                reason_not_a_brick=governance.reason_not_a_brick,
                brick_candidate_ref=governance.brick_candidate_ref,
                governance_status=governance.status,
            )
            if metadata_assessment.stored_metadata:
                _org_db.entity_metadata_upsert("helper", name, **metadata_assessment.stored_metadata)
            update_warnings.extend(governance_warnings(governance))
        except Exception:
            pass  # Non-fatal

    if entry is None:
        # Only org-fields were updated, no registry update happened
        existing = registry.get(name)
        if existing is None:
            return {"success": False, "error": f"Helper '{name}' not found in registry"}
        entry = existing

    updated_fields_list = list(update_fields.keys())
    if org_project is not None:
        updated_fields_list.append("project")
    if org_tags is not None:
        updated_fields_list.append("tags")
    if org_group is not None:
        updated_fields_list.append("group")
    if "reason_not_a_brick" in update_fields:
        updated_fields_list.append("reason_not_a_brick")
    if "brick_candidate_ref" in update_fields:
        updated_fields_list.append("brick_candidate_ref")

    _audit_db.write_audit_entry(
        tool="brix__update_helper",
        source=source,
        arguments_summary=_source_summary(
            source, helper=name, fields=",".join(updated_fields_list)
        ),
    )

    result = {
        "success": True,
        "action": "updated",
        "updated_fields": updated_fields_list,
        "helper": _make_helper_dict(entry),
    }
    if update_warnings:
        result["warnings"] = update_warnings
    return apply_metadata_result(result, metadata_assessment) if metadata_assessment else result


async def _handle_delete_helper(arguments: dict) -> dict:
    """Delete a helper from the registry, with pipeline-scan safety check."""
    registry = HelperRegistry()

    name = arguments.get("name", "").strip()
    force = bool(arguments.get("force", False))
    source = _extract_source(arguments)

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    entry = registry.get(name)
    if entry is None:
        return {"success": False, "error": f"Helper '{name}' not found in registry"}

    # Scan DB-backed step rows first; keep the YAML scan as a fallback for any
    # stale filesystem-only pipeline definitions.
    affected_pipelines = sorted({
        *_scan_db_pipelines_for_helper(name),
        *_scan_pipelines_for_helper(name),
    })

    if affected_pipelines and not force:
        return {
            "success": False,
            "warning": (
                f"Helper '{name}' is referenced in {len(affected_pipelines)} pipeline(s). "
                "Use force=true to delete anyway."
            ),
            "affected_pipelines": affected_pipelines,
        }

    content_hash = getattr(entry, "content_hash", "") or None
    registry.remove(name)
    deleted_cache_files = _delete_helper_cache_files(name, content_hash)

    _audit_db.write_audit_entry(
        tool="brix__delete_helper",
        source=source,
        arguments_summary=_source_summary(source, helper=name),
    )

    result: dict = {
        "success": True,
        "deleted_helper": name,
        "affected_pipelines": affected_pipelines,
    }
    if deleted_cache_files:
        result["deleted_cache_files"] = deleted_cache_files

    return result


async def _handle_rename_helper(arguments: dict) -> dict:
    """Rename a helper: script file + registry name + db index. UUID is preserved."""
    old_name = arguments.get("old_name", "").strip()
    new_name = arguments.get("new_name", "").strip()

    if not old_name:
        return {"success": False, "error": "Parameter 'old_name' is required"}
    if not new_name:
        return {"success": False, "error": "Parameter 'new_name' is required"}
    if old_name == new_name:
        return {"success": False, "error": "old_name and new_name must be different"}

    registry = HelperRegistry()

    old_entry = registry.get(old_name)
    if old_entry is None:
        return {"success": False, "error": f"Helper '{old_name}' not found in registry"}

    if registry.get(new_name) is not None:
        return {"success": False, "error": f"Helper '{new_name}' already exists in registry"}

    # Rename script file if it exists in managed storage
    old_script_path = Path(old_entry.script) if old_entry.script else None
    new_script_path: str = old_entry.script  # default: unchanged

    if old_script_path and old_script_path.exists():
        # Try to rename within the same directory
        new_file = old_script_path.parent / f"{new_name}.py"
        try:
            old_script_path.rename(new_file)
            new_script_path = str(new_file)
        except OSError as exc:
            return {"success": False, "error": f"Could not rename script file: {exc}"}

    # Register under new name (preserves UUID and timestamps)
    now = _now_iso_helper()
    all_data = registry._load()
    old_raw = all_data.get(old_name, {})

    new_raw = dict(old_raw)
    new_raw["name"] = new_name
    new_raw["script"] = new_script_path
    new_raw["updated_at"] = now

    # Write new entry, remove old
    all_data[new_name] = new_raw
    del all_data[old_name]
    registry._save(all_data)

    # Keep DB index in sync: delete old entry first (frees the UUID PRIMARY KEY),
    # then insert new entry with the same UUID
    registry._db.delete_helper(old_name)
    registry._db.upsert_helper(
        name=new_name,
        script_path=new_script_path,
        description=old_raw.get("description", ""),
        requirements=old_raw.get("requirements", []),
        input_schema=old_raw.get("input_schema", {}),
        output_schema=old_raw.get("output_schema", {}),
        helper_id=old_raw.get("id"),
        code=old_raw.get("code", ""),
        content_hash=old_raw.get("content_hash", ""),
        imports=old_raw.get("imports", []),
        project=old_raw.get("project", ""),
        tags=old_raw.get("tags", []),
        group_name=old_raw.get("group_name", ""),
    )

    # Warn if pipelines reference the old helper name
    affected = _scan_pipelines_for_helper(old_name)

    result: dict = {
        "success": True,
        "old_name": old_name,
        "new_name": new_name,
        "script": new_script_path,
    }
    if affected:
        result["warning"] = (
            f"The following pipelines reference helper '{old_name}' and may need updating: "
            + ", ".join(affected)
        )
        result["affected_pipelines"] = affected

    return result
