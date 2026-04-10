from __future__ import annotations

import json

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


@pytest.fixture
def db(tmp_path):
    database = BrixDB(db_path=tmp_path / "metadata_repair.db")
    run_pending_migrations(database)
    return database


@pytest.fixture
def patch_db(tmp_path, monkeypatch, db):
    import brix.db as db_mod
    import brix.helper_registry as helper_registry_mod
    import brix.brick_candidate_detector as detector_mod

    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", db.db_path)
    monkeypatch.setattr(helper_registry_mod, "BrixDB", lambda: db)
    monkeypatch.setattr(detector_mod, "BrixDB", lambda: db)
    return db


@pytest.mark.asyncio
async def test_get_missing_metadata_reports_pipeline_gaps(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_handlers.metadata_repair as mr
    import brix.mcp_server as mcp_server

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)
    monkeypatch.setattr(mr, "BrixDB", lambda: patch_db)

    create_result = await ph._handle_create_pipeline(
        {
            "name": "metadata-gap-pipeline",
            "project": "buddy",
            "description": "Pipeline with intentionally incomplete metadata.",
            "steps": [],
        }
    )
    assert create_result["success"] is True

    result = await mr._handle_get_missing_metadata(
        {
            "entity_type": "pipeline",
            "entity_id": "metadata-gap-pipeline",
        }
    )

    assert result["success"] is True
    assert "owner" in result["missing_fields"]
    assert "purpose" in result["missing_fields"]
    assert "source_intent_id" in result["missing_fields"]
    assert result["repair_prompts"]


@pytest.mark.asyncio
async def test_repair_component_metadata_dispatches_pipeline_update(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_handlers.metadata_repair as mr
    import brix.mcp_server as mcp_server

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)
    monkeypatch.setattr(mr, "BrixDB", lambda: patch_db)

    await ph._handle_create_pipeline(
        {
            "name": "metadata-repair-pipeline",
            "project": "buddy",
            "description": "Pipeline to repair metadata for active promotion.",
            "steps": [],
        }
    )

    result = await mr._handle_repair_component_metadata(
        {
            "entity_type": "pipeline",
            "entity_id": "metadata-repair-pipeline",
            "owner": "team-brix",
            "purpose": "Prove repair tooling can complete missing metadata.",
            "source_intent_id": "intent-123",
            "lifecycle_stage": "active",
        }
    )

    assert result["success"] is True
    follow_up = await mr._handle_get_missing_metadata(
        {
            "entity_type": "pipeline",
            "entity_id": "metadata-repair-pipeline",
        }
    )
    assert follow_up["success"] is True
    assert follow_up["missing_fields"] == []


@pytest.mark.asyncio
async def test_record_reuse_decision_persists_review_for_existing_brick(patch_db, monkeypatch):
    import brix.mcp_handlers.bricks as bh
    import brix.mcp_handlers.metadata_repair as mr

    monkeypatch.setattr(bh, "BrixDB", lambda: patch_db)
    monkeypatch.setattr(mr, "BrixDB", lambda: patch_db)

    create_result = await bh._handle_create_brick(
        {
            "name": "metadata.reuse.brick",
            "runner": "python",
            "description": "Reusable metadata-aware persistence brick.",
            "when_NOT_to_use": "Do not use for one-off vendor scripts.",
            "input_type": "object",
            "output_type": "object",
            "owner": "team-brix",
            "examples": [{"input": {"doc_id": 1}, "output": {"status": "ok"}}],
        }
    )
    assert create_result["success"] is True

    result = await mr._handle_record_reuse_decision(
        {
            "entity_type": "brick",
            "entity_id": "metadata.reuse.brick",
            "decision_outcome": "new_component_justified",
            "rationale": "No equivalent brick with the same persistence contract exists.",
            "owner": "team-brix",
        }
    )

    assert result["success"] is True
    assert result["reuse_review"]["decision_outcome"] == "new_component_justified"
    review = patch_db.knowledge_entity_get("reuse-brick-metadata-reuse-brick")
    assert review is not None
    context = patch_db.knowledge_context("reuse", review["id"])
    assert any(item["entity_type"] == "brick" and item["relation_type"] == "documents" for item in context["related"])


def test_migration_registers_metadata_repair_tool_schemas(db):
    for tool_name in (
        "brix__get_missing_metadata",
        "brix__repair_component_metadata",
        "brix__record_reuse_decision",
    ):
        row = db.mcp_tool_schemas_get(tool_name)
        assert row is not None
        input_schema = row["input_schema"]
        if isinstance(input_schema, str):
            input_schema = json.loads(input_schema)
        assert "entity_type" in input_schema["properties"]
        assert "entity_id" in input_schema["properties"]
