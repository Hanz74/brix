from __future__ import annotations

import json

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


@pytest.fixture
def db(tmp_path, monkeypatch):
    import brix.db as db_mod
    import brix.mcp_handlers.knowledge as knowledge_mod
    import brix.mcp_handlers._shared as shared_mod

    database = BrixDB(db_path=tmp_path / "knowledge_handlers.db")
    run_pending_migrations(database)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", database.db_path)
    monkeypatch.setattr(knowledge_mod, "BrixDB", lambda: database)
    monkeypatch.setattr(shared_mod, "_audit_db", database)
    return database


@pytest.mark.asyncio
async def test_intent_crud_roundtrip(db):
    from brix.mcp_handlers.knowledge import _handle_intent

    add_result = await _handle_intent(
        {
            "action": "add",
            "name": "hmk-stability-intent",
            "title": "Stabilize HMK extraction without workaround growth",
            "raw_text": "I need HMK stable without new workaround loops.",
            "summary": "Capture the original HMK stability request.",
            "owner": "team-brix",
            "project": "buddy",
            "tags": ["hmk", "stability"],
        }
    )
    assert add_result["success"] is True
    assert add_result["entry"]["entity_type"] == "intent"

    get_result = await _handle_intent({"action": "get", "name_or_id": "hmk-stability-intent"})
    assert get_result["success"] is True
    assert get_result["entry"]["title"] == "Stabilize HMK extraction without workaround growth"

    list_result = await _handle_intent({"action": "list", "project": "buddy"})
    assert list_result["count"] == 1

    update_result = await _handle_intent(
        {
            "action": "update",
            "name_or_id": "hmk-stability-intent",
            "summary": "Updated summary",
            "lifecycle_stage": "active",
        }
    )
    assert update_result["success"] is True
    assert update_result["entry"]["summary"] == "Updated summary"
    assert update_result["entry"]["lifecycle_stage"] == "active"

    delete_result = await _handle_intent({"action": "delete", "name_or_id": "hmk-stability-intent"})
    assert delete_result["success"] is True


@pytest.mark.asyncio
async def test_decision_add_requires_rationale(db):
    from brix.mcp_handlers.knowledge import _handle_decision

    result = await _handle_decision(
        {
            "action": "add",
            "name": "hmk-no-rationale",
            "title": "Decision without rationale",
        }
    )
    assert result["success"] is False
    assert "rationale" in result["error"]


@pytest.mark.asyncio
async def test_decision_add_can_link_to_intent(db):
    from brix.mcp_handlers.knowledge import _handle_intent, _handle_decision

    await _handle_intent(
        {
            "action": "add",
            "name": "hmk-anchor-intent",
            "title": "Anchor intent",
            "raw_text": "Original HMK ask",
        }
    )
    result = await _handle_decision(
        {
            "action": "add",
            "name": "hmk-architecture-decision",
            "title": "Adopt brick-first HMK persistence",
            "rationale": "We need reusable persistence contracts instead of pipeline-local SQL.",
            "links": [
                {
                    "relation_type": "implements",
                    "target_entity_type": "intent",
                    "target_entity_id": "hmk-anchor-intent",
                }
            ],
        }
    )
    assert result["success"] is True
    assert len(result["links"]) == 1
    context = db.knowledge_context("decision", "hmk-architecture-decision")
    assert any(item["relation_type"] == "implements" and item["entity_type"] == "intent" for item in context["related"])


def test_migration_registers_intent_decision_tool_schemas(db):
    intent_schema = db.mcp_tool_schemas_get("brix__intent")
    decision_schema = db.mcp_tool_schemas_get("brix__decision")

    assert intent_schema is not None
    assert decision_schema is not None
    intent_input = intent_schema["input_schema"]
    decision_input = decision_schema["input_schema"]
    if isinstance(intent_input, str):
        intent_input = json.loads(intent_input)
    if isinstance(decision_input, str):
        decision_input = json.loads(decision_input)
    assert "action" in intent_input["properties"]
    assert "links" in decision_input["properties"]
