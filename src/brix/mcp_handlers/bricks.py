"""Custom Brick CRUD MCP handlers — T-BRIX-DB-20."""
from __future__ import annotations

import json

# Module-level imports allow patching in tests
from brix.db import BrixDB
from brix.bricks.registry import _row_to_brick


def _get_valid_runners() -> set[str]:
    """Return the set of valid runner names from discover_runners()."""
    from brix.runners.base import discover_runners
    return set(discover_runners().keys())


def _scan_pipelines_for_brick(brick_name: str) -> list[str]:
    """Return a list of pipeline names that use the given brick as a step.type."""
    from brix.mcp_handlers._shared import _pipeline_dir
    from brix.pipeline_store import PipelineStore
    import yaml

    store = PipelineStore(pipelines_dir=_pipeline_dir())
    affected: list[str] = []
    for info in store.list_all():
        try:
            raw = store.load_raw(info["name"])
        except Exception:
            continue
        yaml_text = yaml.dump(raw)
        if brick_name in yaml_text:
            affected.append(info["name"])
    return affected


def _scan_pipelines_for_connection(connection_name: str) -> list[str]:
    """Return a list of pipeline names that reference the given connection name."""
    from brix.mcp_handlers._shared import _pipeline_dir
    from brix.pipeline_store import PipelineStore
    import yaml

    store = PipelineStore(pipelines_dir=_pipeline_dir())
    affected: list[str] = []
    for info in store.list_all():
        try:
            raw = store.load_raw(info["name"])
        except Exception:
            continue
        yaml_text = yaml.dump(raw)
        if connection_name in yaml_text:
            affected.append(info["name"])
    return affected


def _get_triggers_for_pipeline(pipeline_name: str) -> list[str]:
    """Return a list of trigger names that point to the given pipeline."""
    from brix.db import BrixDB
    db = BrixDB()
    try:
        triggers = db.list_triggers()
        return [t["name"] for t in triggers if t.get("pipeline") == pipeline_name]
    except Exception:
        return []


def check_references(entity_type: str, entity_name: str) -> list[str]:
    """Check references to an entity. Returns list of referencing entity descriptions.

    entity_type: "pipeline" | "connection" | "brick"
    Returns list of human-readable reference strings (e.g. "trigger: my-trigger").
    """
    refs: list[str] = []
    if entity_type == "pipeline":
        triggers = _get_triggers_for_pipeline(entity_name)
        for t in triggers:
            refs.append(f"trigger: {t}")
    elif entity_type == "connection":
        pipelines = _scan_pipelines_for_connection(entity_name)
        for p in pipelines:
            refs.append(f"pipeline: {p}")
    elif entity_type == "brick":
        pipelines = _scan_pipelines_for_brick(entity_name)
        for p in pipelines:
            refs.append(f"pipeline: {p}")
    return refs


async def _handle_create_brick(arguments: dict) -> dict:
    """Create a custom brick definition and persist it to brick_definitions DB."""
    from brix.mcp_handlers._shared import _audit_db, _extract_source, _source_summary

    name = arguments.get("name", "").strip()
    runner = arguments.get("runner", "").strip()
    config_defaults = arguments.get("config_defaults", {})
    input_type = arguments.get("input_type", "*")
    output_type = arguments.get("output_type", "*")
    description = arguments.get("description", "")
    aliases = arguments.get("aliases", [])
    when_to_use = arguments.get("when_to_use", "")
    when_NOT_to_use = arguments.get("when_NOT_to_use", "")
    namespace = arguments.get("namespace", "")
    category = arguments.get("category", "custom")
    source = _extract_source(arguments)

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}
    if not runner:
        return {"success": False, "error": "Parameter 'runner' is required"}
    if not description:
        return {"success": False, "error": "Parameter 'description' is required"}

    # Validate runner
    valid_runners = _get_valid_runners()
    if runner not in valid_runners:
        return {
            "success": False,
            "error": f"Unknown runner '{runner}'. Valid runners: {sorted(valid_runners)}",
        }

    # Check if brick already exists
    db = BrixDB()
    existing = db.brick_definitions_get(name)
    if existing is not None:
        return {
            "success": False,
            "error": f"Brick '{name}' already exists. Use brix__update_brick to modify it.",
        }

    # T-BRIX-ORG-01: tags support
    org_tags = arguments.get("tags") or None

    # Build record
    record = {
        "name": name,
        "runner": runner,
        "namespace": namespace,
        "category": category,
        "description": description,
        "when_to_use": when_to_use,
        "when_NOT_to_use": when_NOT_to_use,
        "aliases": aliases if isinstance(aliases, list) else [],
        "input_type": input_type or "*",
        "output_type": output_type or "*",
        "config_schema": config_defaults if isinstance(config_defaults, dict) else {},
        "examples": [],
        "related_connector": "",
        "system": False,
    }
    if org_tags is not None:
        record["org_tags"] = org_tags if isinstance(org_tags, list) else []

    db.brick_definitions_upsert(record)

    # Refresh registry
    import brix.mcp_handlers._shared as _shared_mod
    row = db.brick_definitions_get(name)
    if row:
        try:
            brick = _row_to_brick(row)
            _shared_mod._registry.register(brick)
        except Exception:
            pass

    _audit_db.write_audit_entry(
        tool="brix__create_brick",
        source=source,
        arguments_summary=_source_summary(source, brick=name),
    )

    # Org enforcement warnings
    warnings: list[str] = []
    if not namespace:
        warnings.append(
            "MISSING PROJECT: Bitte 'namespace' angeben (z.B. 'buddy', 'cody', 'utility')."
        )
    if org_tags is None:
        warnings.append(
            "HINT: 'tags' helfen bei der Kategorisierung (z.B. tags=['email', 'transform'])."
        )

    result: dict = {
        "success": True,
        "created_brick": name,
        "runner": runner,
        "namespace": namespace,
        "category": category,
        "note": "Custom brick created and registered in BrickRegistry.",
    }
    if org_tags is not None:
        result["tags"] = org_tags
    if warnings:
        result["warnings"] = warnings
    return result


async def _handle_update_brick(arguments: dict) -> dict:
    """Update a custom brick definition. System bricks cannot be modified."""
    from brix.mcp_handlers._shared import _audit_db, _extract_source, _source_summary

    name = arguments.get("name", "").strip()
    source = _extract_source(arguments)

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    db = BrixDB()
    existing = db.brick_definitions_get(name)
    if existing is None:
        return {"success": False, "error": f"Brick '{name}' not found"}

    if bool(existing.get("system", False)):
        return {
            "success": False,
            "error": f"Brick '{name}' is a system brick and cannot be modified.",
        }

    # Validate runner if provided
    runner = arguments.get("runner")
    if runner is not None:
        runner = runner.strip()
        valid_runners = _get_valid_runners()
        if runner not in valid_runners:
            return {
                "success": False,
                "error": f"Unknown runner '{runner}'. Valid runners: {sorted(valid_runners)}",
            }

    # T-BRIX-ORG-01: tags support
    org_tags = arguments.get("tags") or None

    # Merge updates into existing record
    aliases_raw = existing.get("aliases", "[]")
    if isinstance(aliases_raw, str):
        aliases_raw = json.loads(aliases_raw)

    config_schema_raw = existing.get("config_schema", "{}")
    if isinstance(config_schema_raw, str):
        config_schema_raw = json.loads(config_schema_raw)

    record = {
        "name": name,
        "runner": runner if runner is not None else existing.get("runner", ""),
        "namespace": arguments.get("namespace", existing.get("namespace", "")),
        "category": arguments.get("category", existing.get("category", "custom")),
        "description": arguments.get("description", existing.get("description", "")),
        "when_to_use": arguments.get("when_to_use", existing.get("when_to_use", "")),
        "when_NOT_to_use": arguments.get("when_NOT_to_use", existing.get("when_NOT_to_use", "")),
        "aliases": arguments.get("aliases", aliases_raw),
        "input_type": arguments.get("input_type", existing.get("input_type", "*")),
        "output_type": arguments.get("output_type", existing.get("output_type", "*")),
        "config_schema": arguments.get("config_defaults", config_schema_raw),
        "examples": [],
        "related_connector": existing.get("related_connector", ""),
        "system": False,
    }
    if org_tags is not None:
        record["org_tags"] = org_tags if isinstance(org_tags, list) else []

    db.brick_definitions_upsert(record)

    # Refresh registry
    import brix.mcp_handlers._shared as _shared_mod
    row = db.brick_definitions_get(name)
    if row:
        try:
            brick = _row_to_brick(row)
            _shared_mod._registry.register(brick)
        except Exception:
            pass

    _audit_db.write_audit_entry(
        tool="brix__update_brick",
        source=source,
        arguments_summary=_source_summary(source, brick=name),
    )

    result_upd: dict = {
        "success": True,
        "updated_brick": name,
        "note": "Custom brick updated in DB and BrickRegistry refreshed.",
    }
    if org_tags is not None:
        result_upd["tags"] = org_tags
    return result_upd


async def _handle_delete_brick(arguments: dict) -> dict:
    """Delete a custom brick. System bricks cannot be deleted.

    If the brick is referenced in pipelines and force=false, returns an error
    listing all referencing pipelines. With force=true, deletes anyway.
    """
    from brix.mcp_handlers._shared import _audit_db, _extract_source, _source_summary

    name = arguments.get("name", "").strip()
    force = bool(arguments.get("force", False))
    source = _extract_source(arguments)

    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    db = BrixDB()
    existing = db.brick_definitions_get(name)
    if existing is None:
        return {"success": False, "error": f"Brick '{name}' not found"}

    if bool(existing.get("system", False)):
        return {
            "success": False,
            "error": f"System-Bricks sind nicht löschbar. Brick '{name}' ist ein System-Brick.",
        }

    # Reference check
    if not force:
        refs = check_references("brick", name)
        if refs:
            return {
                "success": False,
                "error": (
                    f"Brick '{name}' wird in {len(refs)} Pipeline(s) verwendet. "
                    "Entferne diese Referenzen zuerst oder nutze force=true."
                ),
                "references": refs,
            }

    # Delete from DB
    deleted = db.brick_definitions_delete(name)
    if not deleted:
        return {"success": False, "error": f"Could not delete brick '{name}'"}

    # Remove from registry
    import brix.mcp_handlers._shared as _shared_mod
    try:
        _shared_mod._registry.unregister(name)
    except ValueError:
        pass  # Already handles system check, but we checked above

    _audit_db.write_audit_entry(
        tool="brix__delete_brick",
        source=source,
        arguments_summary=_source_summary(source, brick=name),
    )

    return {
        "success": True,
        "deleted_brick": name,
        "note": "Custom brick deleted from DB and removed from BrickRegistry.",
    }
