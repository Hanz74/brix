from __future__ import annotations

import json

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


@pytest.fixture
def db(tmp_path, monkeypatch):
    import brix.db as db_mod
    import brix.mcp_handlers.component_context as context_mod
    import brix.knowledge_graph as graph_mod

    database = BrixDB(db_path=tmp_path / "component_context.db")
    run_pending_migrations(database)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", database.db_path)
    monkeypatch.setattr(context_mod, "BrixDB", lambda: database)
    monkeypatch.setattr(graph_mod, "BrixDB", lambda: database)
    return database


def _seed_graph(db: BrixDB) -> None:
    intent = db.knowledge_entity_add("intent", "hmk-context-intent", "HMK context intent", raw_text="Original ask")
    decision = db.knowledge_entity_add(
        "decision",
        "hmk-context-decision",
        "HMK context decision",
        rationale="Adopt reusable persistence bricks.",
    )
    db.upsert_pipeline(name="buddy-hmk-context", path="/tmp/buddy-hmk-context.yaml", project="buddy")
    db.knowledge_link_add("decision", decision["id"], "implements", "intent", intent["id"])
    db.knowledge_link_add("decision", decision["id"], "documents", "pipeline", "buddy-hmk-context")


@pytest.mark.asyncio
async def test_get_component_context_returns_related_entities(db):
    from brix.mcp_handlers.component_context import _handle_get_component_context

    _seed_graph(db)
    result = await _handle_get_component_context({"entity_type": "decision", "entity_id": "hmk-context-decision"})

    assert result["success"] is True
    assert result["entity"]["name"] == "hmk-context-decision"
    assert any(item["relation_type"] == "implements" and item["entity_type"] == "intent" for item in result["related"])


@pytest.mark.asyncio
async def test_get_related_components_returns_graph_neighbors(db):
    from brix.mcp_handlers.component_context import _handle_get_related_components

    _seed_graph(db)
    result = await _handle_get_related_components(
        {"entity_type": "decision", "entity_id": "hmk-context-decision", "depth": 2}
    )

    assert result["success"] is True
    assert result["start"]["name"] == "hmk-context-decision"
    assert any(node["entity_type"] == "intent" for node in result["neighbors"])
    assert any(edge["relation_type"] == "documents" for edge in result["edges"])


def test_migration_registers_component_context_tool_schemas(db):
    get_schema = db.mcp_tool_schemas_get("brix__get_component_context")
    related_schema = db.mcp_tool_schemas_get("brix__get_related_components")

    assert get_schema is not None
    assert related_schema is not None
    get_input = get_schema["input_schema"]
    related_input = related_schema["input_schema"]
    if isinstance(get_input, str):
        get_input = json.loads(get_input)
    if isinstance(related_input, str):
        related_input = json.loads(related_input)
    assert "entity_type" in get_input["properties"]
    assert "depth" in related_input["properties"]
