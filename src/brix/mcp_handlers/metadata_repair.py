"""Metadata repair and reuse decision handlers."""
from __future__ import annotations

from typing import Any

from brix.db import BrixDB
from brix.metadata_enforcement import assess_metadata_enforcement
from brix.metadata_policy import REQUIRED_METADATA_MATRIX, is_active_like
from brix.mcp_handlers._shared import _audit_db, _extract_source, _source_summary
from brix.reuse_enforcement import (
    blocking_reuse_response,
    persist_reuse_review,
    assess_reuse_for_creation,
)


_SUPPORTED_COMPONENT_TYPES: frozenset[str] = frozenset(REQUIRED_METADATA_MATRIX)
_REUSE_COMPONENT_TYPES: frozenset[str] = frozenset({"pipeline", "brick", "helper"})
_SUPPLEMENTAL_FIELDS: frozenset[str] = frozenset(
    {
        "owner",
        "purpose",
        "source_intent_id",
        "lifecycle_stage",
        "status",
        "usage_scope",
        "version_relevance",
        "linked_topic",
        "replacement_plan",
        "expiry_condition",
    }
)


async def _handle_get_missing_metadata(arguments: dict) -> dict:
    entity_type = str(arguments.get("entity_type") or "").strip().lower()
    entity_id = str(arguments.get("entity_id") or "").strip()
    if not entity_type or not entity_id:
        return {"success": False, "error": "Parameters 'entity_type' and 'entity_id' are required"}

    try:
        resolved = _resolve_component(entity_type, entity_id)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    assessment = assess_metadata_enforcement(
        entity_type,
        base_data=resolved["base_data"],
        existing_metadata=resolved["metadata"],
        operation="update",
    )
    missing_fields = [violation.field for violation in assessment.violations]
    result = {
        "success": True,
        "entity_type": entity_type,
        "entity_id": resolved["entity_id"],
        "entity": resolved["entity"],
        "metadata": resolved["metadata"],
        "active_like": is_active_like(entity_type, assessment.merged_data),
        "missing_fields": missing_fields,
        "missing_count": len(missing_fields),
        "metadata_policy": assessment.as_dict(),
        "repair_prompts": list(assessment.repair_prompts),
        "next_actions": [
            "Use brix__repair_component_metadata to fill the missing fields."
        ],
    }
    if entity_type in _REUSE_COMPONENT_TYPES:
        result["next_actions"].append(
            "If this component was newly introduced or heavily modified, record the review with brix__record_reuse_decision."
        )
    return result


async def _handle_repair_component_metadata(arguments: dict) -> dict:
    entity_type = str(arguments.get("entity_type") or "").strip().lower()
    entity_id = str(arguments.get("entity_id") or "").strip()
    if not entity_type or not entity_id:
        return {"success": False, "error": "Parameters 'entity_type' and 'entity_id' are required"}

    try:
        resolved = _resolve_component(entity_type, entity_id)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    source = _extract_source(arguments)
    updates = _extract_repair_updates(arguments)
    if not updates:
        return {"success": False, "error": "No repair fields were provided"}

    result = await _dispatch_repair(entity_type, resolved["entity_name"], updates)
    if result.get("success"):
        _audit_db.write_audit_entry(
            tool="brix__repair_component_metadata",
            source=source,
            arguments_summary=_source_summary(source, entity=f"{entity_type}:{entity_id}"),
        )
    return result


async def _handle_record_reuse_decision(arguments: dict) -> dict:
    entity_type = str(arguments.get("entity_type") or "").strip().lower()
    entity_id = str(arguments.get("entity_id") or "").strip()
    if entity_type not in _REUSE_COMPONENT_TYPES:
        valid = ", ".join(sorted(_REUSE_COMPONENT_TYPES))
        return {"success": False, "error": f"entity_type must be one of: {valid}"}
    if not entity_id:
        return {"success": False, "error": "Parameter 'entity_id' is required"}

    try:
        resolved = _resolve_component(entity_type, entity_id)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    reviewed_components = arguments.get("reviewed_components")
    if reviewed_components is None:
        reviewed_components = arguments.get("reuse_reviewed_components")
    if not isinstance(reviewed_components, list):
        reviewed_components = [reviewed_components] if reviewed_components else []

    project = str(arguments.get("project") or resolved["base_data"].get("project") or "").strip()
    owner = str(arguments.get("owner") or resolved["metadata"].get("owner") or resolved["base_data"].get("owner") or "").strip()
    description = " ".join(
        part
        for part in (
            str(arguments.get("description") or "").strip(),
            str(resolved["base_data"].get("description") or "").strip(),
            str(resolved["metadata"].get("purpose") or "").strip(),
        )
        if part
    ).strip()

    assessment = assess_reuse_for_creation(
        entity_type=entity_type,
        entity_name=resolved["entity_name"],
        description=description,
        project=project,
        owner=owner,
        decision_outcome=str(arguments.get("decision_outcome") or arguments.get("reuse_decision_outcome") or "").strip(),
        rationale=str(arguments.get("rationale") or arguments.get("reuse_rationale") or "").strip(),
        reviewed_components=reviewed_components,
    )
    if assessment.blocking:
        return blocking_reuse_response(assessment)

    db = BrixDB()
    review = persist_reuse_review(db=db, assessment=assessment, project=project, owner=owner)
    source = _extract_source(arguments)
    _audit_db.write_audit_entry(
        tool="brix__record_reuse_decision",
        source=source,
        arguments_summary=_source_summary(source, entity=f"{entity_type}:{entity_id}", outcome=assessment.decision_outcome),
    )
    return {
        "success": True,
        "entity_type": entity_type,
        "entity_id": resolved["entity_id"],
        "reuse_review": assessment.as_dict(),
        "review_entity": review,
        "context": db.knowledge_context("reuse", review["id"]),
    }


def _resolve_component(entity_type: str, entity_id: str) -> dict[str, Any]:
    if entity_type not in _SUPPORTED_COMPONENT_TYPES:
        valid = ", ".join(sorted(_SUPPORTED_COMPONENT_TYPES))
        raise ValueError(f"Unknown entity_type '{entity_type}'. Valid types: {valid}")

    db = BrixDB()
    metadata = db.entity_metadata_get(entity_type, entity_id) or {}

    if entity_type == "pipeline":
        entity = db.get_pipeline(entity_id)
        if entity is None:
            raise ValueError(f"pipeline '{entity_id}' not found")
        return {
            "entity_type": entity_type,
            "entity_id": str(entity.get("id") or entity.get("name") or entity_id),
            "entity_name": str(entity.get("name") or entity_id),
            "entity": entity,
            "metadata": metadata,
            "base_data": {
                "project": entity.get("project", ""),
                "description": entity.get("description", ""),
            },
        }

    if entity_type == "brick":
        entity = db.brick_definitions_get(entity_id)
        if entity is None:
            raise ValueError(f"brick '{entity_id}' not found")
        return {
            "entity_type": entity_type,
            "entity_id": str(entity.get("name") or entity_id),
            "entity_name": str(entity.get("name") or entity_id),
            "entity": entity,
            "metadata": metadata,
            "base_data": {
                "description": entity.get("description", ""),
                "input_type": entity.get("input_type", ""),
                "output_type": entity.get("output_type", ""),
                "when_NOT_to_use": entity.get("when_NOT_to_use", ""),
                "examples": _json_field(entity.get("examples")),
                "project": entity.get("project", ""),
            },
        }

    if entity_type == "helper":
        entity = db.get_helper(entity_id)
        if entity is None:
            raise ValueError(f"helper '{entity_id}' not found")
        return {
            "entity_type": entity_type,
            "entity_id": str(entity.get("id") or entity_id),
            "entity_name": str(entity.get("name") or entity_id),
            "entity": entity,
            "metadata": metadata,
            "base_data": {
                "description": entity.get("description", ""),
                "project": entity.get("project", ""),
                "reason_not_a_brick": entity.get("reason_not_a_brick", ""),
                "brick_candidate_ref": entity.get("brick_candidate_ref", ""),
                "governance_status": entity.get("governance_status", ""),
            },
        }

    if entity_type == "connection":
        from brix.connections import ConnectionManager

        entity = next((item for item in ConnectionManager(db).list() if item.get("name") == entity_id), None)
        if entity is None:
            raise ValueError(f"connection '{entity_id}' not found")
        return {
            "entity_type": entity_type,
            "entity_id": str(entity.get("name") or entity_id),
            "entity_name": str(entity.get("name") or entity_id),
            "entity": entity,
            "metadata": metadata,
            "base_data": {
                "description": entity.get("description", ""),
                "project": entity.get("project", ""),
            },
        }

    if entity_type == "help_topic":
        entity = db.help_topics_get(entity_id)
        if entity is None:
            raise ValueError(f"help_topic '{entity_id}' not found")
        return {
            "entity_type": entity_type,
            "entity_id": str(entity.get("name") or entity_id),
            "entity_name": str(entity.get("name") or entity_id),
            "entity": entity,
            "metadata": metadata,
            "base_data": {
                "title": entity.get("title", ""),
            },
        }

    entity = db.knowledge_entity_get(entity_id)
    if entity is None or entity.get("entity_type") != entity_type:
        raise ValueError(f"{entity_type} '{entity_id}' not found")
    if not metadata:
        metadata = db.entity_metadata_get(entity_type, entity.get("name") or entity_id) or {}
    return {
        "entity_type": entity_type,
        "entity_id": str(entity.get("id") or entity_id),
        "entity_name": str(entity.get("name") or entity_id),
        "entity": entity,
        "metadata": metadata,
        "base_data": {
            "title": entity.get("title", ""),
            "project": entity.get("project", ""),
            "owner": entity.get("owner", ""),
            "rationale": entity.get("rationale", ""),
            "lifecycle_stage": entity.get("lifecycle_stage", ""),
            "status": entity.get("status", ""),
        },
    }


def _extract_repair_updates(arguments: dict) -> dict[str, Any]:
    allowed = {
        "title",
        "description",
        "project",
        "owner",
        "purpose",
        "source_intent_id",
        "lifecycle_stage",
        "status",
        "usage_scope",
        "version_relevance",
        "linked_topic",
        "replacement_plan",
        "expiry_condition",
        "reason_not_a_brick",
        "brick_candidate_ref",
        "input_type",
        "output_type",
        "when_NOT_to_use",
        "examples",
        "category",
        "content",
        "raw_text",
        "summary",
        "rationale",
        "tags",
    }
    updates: dict[str, Any] = {}
    for field in allowed:
        if field in arguments:
            updates[field] = arguments[field]
    return updates


async def _dispatch_repair(entity_type: str, entity_id: str, updates: dict[str, Any]) -> dict:
    if entity_type == "pipeline":
        from brix.mcp_handlers.pipelines import _handle_update_pipeline

        payload = {"name": entity_id, **updates}
        return await _handle_update_pipeline(payload)
    if entity_type == "brick":
        from brix.mcp_handlers.bricks import _handle_update_brick

        payload = {"name": entity_id, **updates}
        return await _handle_update_brick(payload)
    if entity_type == "helper":
        from brix.mcp_handlers.helpers import _handle_update_helper

        payload = {"name": entity_id, **updates}
        return await _handle_update_helper(payload)
    if entity_type == "connection":
        from brix.mcp_handlers.connections import _handle_update_connection

        payload = {"name": entity_id, **updates}
        return await _handle_update_connection(payload)
    if entity_type == "help_topic":
        from brix.mcp_handlers.dbfirst_crud import _handle_help_topic_update

        payload = {"name": entity_id, **updates}
        return await _handle_help_topic_update(payload)

    db = BrixDB()
    entity = db.knowledge_entity_get(entity_id)
    if entity is None or entity.get("entity_type") != entity_type:
        return {"success": False, "error": f"{entity_type} '{entity_id}' not found"}

    base_updates = {
        key: value
        for key, value in updates.items()
        if key in {"title", "raw_text", "summary", "rationale", "lifecycle_stage", "status", "owner", "project", "tags", "content"}
    }
    metadata_updates = {
        key: value
        for key, value in updates.items()
        if key in _SUPPLEMENTAL_FIELDS
    }
    merged_base = {
        "title": base_updates.get("title", entity.get("title", "")),
        "project": base_updates.get("project", entity.get("project", "")),
        "owner": base_updates.get("owner", entity.get("owner", "")),
        "rationale": base_updates.get("rationale", entity.get("rationale", "")),
        "lifecycle_stage": base_updates.get("lifecycle_stage", entity.get("lifecycle_stage", "")),
        "status": base_updates.get("status", entity.get("status", "")),
    }
    assessment = assess_metadata_enforcement(
        entity_type,
        base_data=merged_base,
        incoming_metadata=metadata_updates,
        existing_data=entity,
        existing_metadata=db.entity_metadata_get(entity_type, entity.get("name") or entity_id) or {},
        operation="update",
    )
    if assessment.blocking:
        return {
            "success": False,
            "error": (
                "Required metadata is incomplete for an active/governed entity. "
                "Complete the missing fields or downgrade it to draft first."
            ),
            "metadata_policy": assessment.as_dict(),
            "repair_prompts": list(assessment.repair_prompts),
        }

    updated = db.knowledge_entity_update(entity["id"], **base_updates)
    if metadata_updates:
        db.entity_metadata_upsert(entity_type, updated["name"], **metadata_updates)
    refreshed = _resolve_component(entity_type, updated["id"])
    final_assessment = assess_metadata_enforcement(
        entity_type,
        base_data=refreshed["base_data"],
        existing_metadata=refreshed["metadata"],
        operation="update",
    )
    return {
        "success": True,
        "entity": refreshed["entity"],
        "metadata": refreshed["metadata"],
        "metadata_policy": final_assessment.as_dict(),
        "repair_prompts": list(final_assessment.repair_prompts),
    }


def _json_field(value: Any) -> Any:
    if isinstance(value, str):
        import json

        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value
