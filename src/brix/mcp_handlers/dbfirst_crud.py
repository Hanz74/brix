"""T-BRIX-DBF-04: MCP CRUD handlers for Tool-Schemas, Help-Topics, Keywords, Type-Compatibility.

Consolidated action-based handlers following the brix__trigger pattern.
"""
from __future__ import annotations

from brix.metadata_enforcement import (
    apply_metadata_result,
    assess_metadata_enforcement,
    blocking_metadata_response,
    extract_supplemental_metadata,
)

import json


def _get_db():
    """Lazy import to avoid circular imports."""
    from brix.db import BrixDB
    return BrixDB()


# ------------------------------------------------------------------
# brix__tool_schema — action: add/get/list/update/delete
# ------------------------------------------------------------------

async def _handle_tool_schema_add(arguments: dict) -> dict:
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    description = arguments.get("description", "")
    input_schema = arguments.get("input_schema", {})
    if isinstance(input_schema, str):
        try:
            input_schema = json.loads(input_schema)
        except json.JSONDecodeError:
            return {"success": False, "error": "input_schema must be valid JSON."}
    db = _get_db()
    db.mcp_tool_schemas_upsert({
        "name": name,
        "description": description,
        "input_schema": input_schema,
    })
    record = db.mcp_tool_schemas_get(name)
    return {"success": True, "tool_schema": record}


async def _handle_tool_schema_get(arguments: dict) -> dict:
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    db = _get_db()
    record = db.mcp_tool_schemas_get(name)
    if record is None:
        return {"success": False, "error": f"Tool schema '{name}' not found."}
    # Parse input_schema from JSON string
    if isinstance(record.get("input_schema"), str):
        try:
            record["input_schema"] = json.loads(record["input_schema"])
        except (json.JSONDecodeError, TypeError):
            pass
    return {"success": True, "tool_schema": record}


async def _handle_tool_schema_list(arguments: dict) -> dict:
    db = _get_db()
    schemas = db.mcp_tool_schemas_list()
    for s in schemas:
        if isinstance(s.get("input_schema"), str):
            try:
                s["input_schema"] = json.loads(s["input_schema"])
            except (json.JSONDecodeError, TypeError):
                pass
    return {"success": True, "tool_schemas": schemas, "count": len(schemas)}


async def _handle_tool_schema_update(arguments: dict) -> dict:
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    db = _get_db()
    existing = db.mcp_tool_schemas_get(name)
    if existing is None:
        return {"success": False, "error": f"Tool schema '{name}' not found."}
    # Merge updates
    record = {"name": name}
    if "description" in arguments:
        record["description"] = arguments["description"]
    else:
        record["description"] = existing.get("description", "")
    if "input_schema" in arguments:
        input_schema = arguments["input_schema"]
        if isinstance(input_schema, str):
            try:
                input_schema = json.loads(input_schema)
            except json.JSONDecodeError:
                return {"success": False, "error": "input_schema must be valid JSON."}
        record["input_schema"] = input_schema
    else:
        raw = existing.get("input_schema", "{}")
        record["input_schema"] = json.loads(raw) if isinstance(raw, str) else raw
    db.mcp_tool_schemas_upsert(record)
    updated = db.mcp_tool_schemas_get(name)
    if isinstance(updated.get("input_schema"), str):
        try:
            updated["input_schema"] = json.loads(updated["input_schema"])
        except (json.JSONDecodeError, TypeError):
            pass
    return {"success": True, "tool_schema": updated}


async def _handle_tool_schema_delete(arguments: dict) -> dict:
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    db = _get_db()
    deleted = db.mcp_tool_schemas_delete(name)
    if not deleted:
        return {"success": False, "error": f"Tool schema '{name}' not found."}
    return {"success": True, "name": name}


async def _handle_tool_schema(arguments: dict) -> dict:
    """Dispatcher for brix__tool_schema — routes to individual handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_tool_schema_add(arguments)
    elif action == "get":
        return await _handle_tool_schema_get(arguments)
    elif action == "list":
        return await _handle_tool_schema_list(arguments)
    elif action == "update":
        return await _handle_tool_schema_update(arguments)
    elif action == "delete":
        return await _handle_tool_schema_delete(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, get, list, update, delete."}


# ------------------------------------------------------------------
# brix__help_topic — action: add/get/list/update/delete
# ------------------------------------------------------------------

async def _handle_help_topic_add(arguments: dict) -> dict:
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    title = arguments.get("title", name)
    content = arguments.get("content", "")
    category = arguments.get("category", "")
    db = _get_db()
    metadata_assessment = assess_metadata_enforcement(
        "help_topic",
        base_data={"title": title},
        incoming_metadata=extract_supplemental_metadata(arguments),
        operation="create",
    )
    db.help_topics_upsert({
        "name": name,
        "title": title,
        "content": content,
        "category": category,
    })
    if metadata_assessment.stored_metadata:
        db.entity_metadata_upsert("help_topic", name, **metadata_assessment.stored_metadata)
    record = db.help_topics_get(name)
    return apply_metadata_result({"success": True, "help_topic": record}, metadata_assessment)


async def _handle_help_topic_get(arguments: dict) -> dict:
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    db = _get_db()
    record = db.help_topics_get(name)
    if record is None:
        return {"success": False, "error": f"Help topic '{name}' not found."}
    return {"success": True, "help_topic": record}


async def _handle_help_topic_list(arguments: dict) -> dict:
    category = arguments.get("category")
    db = _get_db()
    topics = db.help_topics_list(category=category)
    return {"success": True, "help_topics": topics, "count": len(topics)}


async def _handle_help_topic_update(arguments: dict) -> dict:
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    db = _get_db()
    existing = db.help_topics_get(name)
    if existing is None:
        return {"success": False, "error": f"Help topic '{name}' not found."}
    metadata_assessment = assess_metadata_enforcement(
        "help_topic",
        base_data={"title": arguments.get("title", existing.get("title", name))},
        incoming_metadata=extract_supplemental_metadata(arguments),
        existing_data=existing,
        existing_metadata=db.entity_metadata_get("help_topic", name) or {},
        operation="update",
    )
    if metadata_assessment.blocking:
        return blocking_metadata_response(metadata_assessment)
    record = {
        "name": name,
        "title": arguments.get("title", existing.get("title", name)),
        "content": arguments.get("content", existing.get("content", "")),
        "category": arguments.get("category", existing.get("category", "")),
    }
    db.help_topics_upsert(record)
    if metadata_assessment.stored_metadata:
        db.entity_metadata_upsert("help_topic", name, **metadata_assessment.stored_metadata)
    updated = db.help_topics_get(name)
    return apply_metadata_result({"success": True, "help_topic": updated}, metadata_assessment)


async def _handle_help_topic_delete(arguments: dict) -> dict:
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    db = _get_db()
    deleted = db.help_topics_delete(name)
    if not deleted:
        return {"success": False, "error": f"Help topic '{name}' not found."}
    return {"success": True, "name": name}


async def _handle_help_topic(arguments: dict) -> dict:
    """Dispatcher for brix__help_topic — routes to individual handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_help_topic_add(arguments)
    elif action == "get":
        return await _handle_help_topic_get(arguments)
    elif action == "list":
        return await _handle_help_topic_list(arguments)
    elif action == "update":
        return await _handle_help_topic_update(arguments)
    elif action == "delete":
        return await _handle_help_topic_delete(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, get, list, update, delete."}


# ------------------------------------------------------------------
# brix__keyword — action: add/list/delete
# ------------------------------------------------------------------

async def _handle_keyword_add(arguments: dict) -> dict:
    keyword = arguments.get("keyword", "").strip()
    category = arguments.get("category", "").strip()
    if not keyword:
        return {"success": False, "error": "Parameter 'keyword' is required."}
    if not category:
        return {"success": False, "error": "Parameter 'category' is required."}
    language = arguments.get("language", "de")
    mapped_to = arguments.get("mapped_to", "")
    db = _get_db()
    db.keyword_taxonomies_upsert(
        category=category,
        keyword=keyword,
        language=language,
        mapped_to=mapped_to,
    )
    return {"success": True, "keyword": keyword, "category": category, "language": language, "mapped_to": mapped_to}


async def _handle_keyword_list(arguments: dict) -> dict:
    category = arguments.get("category")
    db = _get_db()
    keywords = db.keyword_taxonomies_list(category=category)
    return {"success": True, "keywords": keywords, "count": len(keywords)}


async def _handle_keyword_delete(arguments: dict) -> dict:
    keyword = arguments.get("keyword", "").strip()
    category = arguments.get("category", "").strip()
    if not keyword:
        return {"success": False, "error": "Parameter 'keyword' is required."}
    if not category:
        return {"success": False, "error": "Parameter 'category' is required."}
    db = _get_db()
    deleted = db.keyword_taxonomies_delete(category=category, keyword=keyword)
    if not deleted:
        return {"success": False, "error": f"Keyword '{keyword}' in category '{category}' not found."}
    return {"success": True, "keyword": keyword, "category": category}


async def _handle_keyword(arguments: dict) -> dict:
    """Dispatcher for brix__keyword — routes to individual handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_keyword_add(arguments)
    elif action == "list":
        return await _handle_keyword_list(arguments)
    elif action == "delete":
        return await _handle_keyword_delete(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, list, delete."}


# ------------------------------------------------------------------
# brix__type_compat — action: add/list/delete
# ------------------------------------------------------------------

async def _handle_type_compat_add(arguments: dict) -> dict:
    source_type = arguments.get("source_type", "").strip()
    target_type = arguments.get("target_type", "").strip()
    if not source_type:
        return {"success": False, "error": "Parameter 'source_type' is required."}
    if not target_type:
        return {"success": False, "error": "Parameter 'target_type' is required."}
    db = _get_db()
    db.type_compatibility_upsert(output_type=source_type, compatible_input=target_type)
    return {"success": True, "source_type": source_type, "target_type": target_type}


async def _handle_type_compat_list(arguments: dict) -> dict:
    source_type = arguments.get("source_type")
    db = _get_db()
    entries = db.type_compatibility_list()
    if source_type:
        entries = [e for e in entries if e["output_type"] == source_type]
    return {"success": True, "type_compatibilities": entries, "count": len(entries)}


async def _handle_type_compat_delete(arguments: dict) -> dict:
    source_type = arguments.get("source_type", "").strip()
    target_type = arguments.get("target_type", "").strip()
    if not source_type:
        return {"success": False, "error": "Parameter 'source_type' is required."}
    if not target_type:
        return {"success": False, "error": "Parameter 'target_type' is required."}
    db = _get_db()
    deleted = db.type_compatibility_delete(output_type=source_type, compatible_input=target_type)
    if not deleted:
        return {"success": False, "error": f"Type compatibility '{source_type}' -> '{target_type}' not found."}
    return {"success": True, "source_type": source_type, "target_type": target_type}


async def _handle_type_compat(arguments: dict) -> dict:
    """Dispatcher for brix__type_compat — routes to individual handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_type_compat_add(arguments)
    elif action == "list":
        return await _handle_type_compat_list(arguments)
    elif action == "delete":
        return await _handle_type_compat_delete(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, list, delete."}
