"""Knowledge entity handlers for intent/decision CRUD."""
from __future__ import annotations

from brix.db import BrixDB
from brix.mcp_handlers._shared import _audit_db, _extract_source, _source_summary


def _normalize_links(raw_links: object) -> list[dict]:
    if not isinstance(raw_links, list):
        return []
    normalized: list[dict] = []
    for item in raw_links:
        if not isinstance(item, dict):
            continue
        normalized.append(item)
    return normalized


async def _handle_intent(arguments: dict) -> dict:
    """CRUD handler for intent knowledge entities."""
    return await _handle_knowledge_entity(arguments, entity_type="intent", tool_name="brix__intent")


async def _handle_decision(arguments: dict) -> dict:
    """CRUD handler for decision knowledge entities."""
    return await _handle_knowledge_entity(arguments, entity_type="decision", tool_name="brix__decision")


def _validate_required_fields(entity_type: str, action: str, arguments: dict) -> str | None:
    if action == "add":
        if not str(arguments.get("name") or "").strip():
            return "Parameter 'name' is required"
        if not str(arguments.get("title") or "").strip():
            return "Parameter 'title' is required"
        if entity_type == "decision" and not str(arguments.get("rationale") or "").strip():
            return "Parameter 'rationale' is required for decisions"
    if action in {"get", "update", "delete"} and not str(arguments.get("name_or_id") or "").strip():
        return "Parameter 'name_or_id' is required"
    return None


def _link_entity(db: BrixDB, entity_type: str, entity_id: str, links: list[dict]) -> list[dict]:
    created: list[dict] = []
    for link in links:
        relation_type = str(link.get("relation_type") or "").strip()
        target_entity_type = str(link.get("target_entity_type") or "").strip()
        target_entity_id = str(link.get("target_entity_id") or "").strip()
        if not relation_type or not target_entity_type or not target_entity_id:
            continue
        created.append(
            db.knowledge_link_add(
                entity_type,
                entity_id,
                relation_type,
                target_entity_type,
                target_entity_id,
                metadata=link.get("metadata") or {},
            )
        )
    return created


async def _handle_knowledge_entity(arguments: dict, *, entity_type: str, tool_name: str) -> dict:
    action = str(arguments.get("action") or "").strip().lower()
    if action not in {"add", "get", "list", "update", "delete"}:
        return {
            "success": False,
            "error": f"Unknown action '{action}'. Valid actions: add, get, list, update, delete.",
        }

    validation_error = _validate_required_fields(entity_type, action, arguments)
    if validation_error:
        return {"success": False, "error": validation_error}

    source = _extract_source(arguments)
    db = BrixDB()

    if action == "add":
        entry = db.knowledge_entity_add(
            entity_type,
            str(arguments.get("name") or "").strip(),
            str(arguments.get("title") or "").strip(),
            raw_text=str(arguments.get("raw_text") or ""),
            summary=str(arguments.get("summary") or ""),
            rationale=str(arguments.get("rationale") or ""),
            lifecycle_stage=str(arguments.get("lifecycle_stage") or "draft"),
            status=str(arguments.get("status") or ""),
            owner=str(arguments.get("owner") or ""),
            project=str(arguments.get("project") or ""),
            tags=arguments.get("tags") if isinstance(arguments.get("tags"), list) else [],
            content=arguments.get("content") or {},
        )
        links = _link_entity(db, entity_type, entry["id"], _normalize_links(arguments.get("links")))
        _audit_db.write_audit_entry(
            tool=tool_name,
            source=source,
            arguments_summary=_source_summary(source, action=action, entity=entry["name"]),
        )
        return {"success": True, "entry": entry, "links": links}

    if action == "get":
        name_or_id = str(arguments.get("name_or_id") or "").strip()
        entry = db.knowledge_entity_get(name_or_id)
        if entry is None or entry.get("entity_type") != entity_type:
            return {"success": False, "error": f"{entity_type} '{name_or_id}' not found"}
        return {"success": True, "entry": entry}

    if action == "list":
        entries = db.knowledge_entity_list(
            entity_type=entity_type,
            project=arguments.get("project"),
            lifecycle_stage=arguments.get("lifecycle_stage"),
            tag_filter=arguments.get("tag_filter"),
        )
        return {"success": True, "entries": entries, "count": len(entries)}

    if action == "update":
        name_or_id = str(arguments.get("name_or_id") or "").strip()
        entry = db.knowledge_entity_update(
            name_or_id,
            title=arguments.get("title"),
            raw_text=arguments.get("raw_text"),
            summary=arguments.get("summary"),
            rationale=arguments.get("rationale"),
            lifecycle_stage=arguments.get("lifecycle_stage"),
            status=arguments.get("status"),
            owner=arguments.get("owner"),
            project=arguments.get("project"),
            tags=arguments.get("tags") if isinstance(arguments.get("tags"), list) else None,
            content=arguments.get("content") if "content" in arguments else None,
        )
        if entry is None or entry.get("entity_type") != entity_type:
            return {"success": False, "error": f"{entity_type} '{name_or_id}' not found"}
        _audit_db.write_audit_entry(
            tool=tool_name,
            source=source,
            arguments_summary=_source_summary(source, action=action, entity=entry["name"]),
        )
        return {"success": True, "entry": entry}

    name_or_id = str(arguments.get("name_or_id") or "").strip()
    entry = db.knowledge_entity_get(name_or_id)
    if entry is None or entry.get("entity_type") != entity_type:
        return {"success": False, "error": f"{entity_type} '{name_or_id}' not found"}
    deleted = db.knowledge_entity_delete(entry["id"])
    if not deleted:
        return {"success": False, "error": f"{entity_type} '{name_or_id}' could not be deleted"}
    _audit_db.write_audit_entry(
        tool=tool_name,
        source=source,
        arguments_summary=_source_summary(source, action=action, entity=entry["name"]),
    )
    return {"success": True, "deleted": entry["name"], "id": entry["id"]}
