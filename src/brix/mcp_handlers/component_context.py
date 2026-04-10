"""Component context and relationship inspection handlers."""
from __future__ import annotations

from brix.db import BrixDB
from brix.knowledge_graph import query_component_relationships


async def _handle_get_component_context(arguments: dict) -> dict:
    entity_type = str(arguments.get("entity_type") or "").strip()
    entity_id = str(arguments.get("entity_id") or "").strip()
    if not entity_type or not entity_id:
        return {"success": False, "error": "Parameters 'entity_type' and 'entity_id' are required"}

    db = BrixDB()
    try:
        context = db.knowledge_context(entity_type, entity_id)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    return {"success": True, **context}


async def _handle_get_related_components(arguments: dict) -> dict:
    entity_type = str(arguments.get("entity_type") or "").strip()
    entity_id = str(arguments.get("entity_id") or "").strip()
    if not entity_type or not entity_id:
        return {"success": False, "error": "Parameters 'entity_type' and 'entity_id' are required"}

    depth = arguments.get("depth", 1)
    relation_types = arguments.get("relation_types")
    if not isinstance(depth, int) or depth < 1:
        depth = 1
    if not isinstance(relation_types, list):
        relation_types = None

    try:
        graph = query_component_relationships(
            entity_type,
            entity_id,
            depth=depth,
            relation_types=relation_types,
            project=arguments.get("project"),
        )
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    return {"success": True, **graph}
