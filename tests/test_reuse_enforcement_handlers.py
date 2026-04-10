from __future__ import annotations

import json

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


@pytest.fixture
def db(tmp_path):
    database = BrixDB(db_path=tmp_path / "reuse_enforcement.db")
    run_pending_migrations(database)
    return database


@pytest.fixture
def patch_db(monkeypatch, db):
    import brix.db as db_mod
    import brix.helper_registry as helper_registry_mod
    import brix.brick_candidate_detector as detector_mod

    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", db.db_path)
    monkeypatch.setattr(helper_registry_mod, "BrixDB", lambda: db)
    monkeypatch.setattr(detector_mod, "BrixDB", lambda: db)
    return db


@pytest.mark.asyncio
async def test_create_pipeline_auto_records_reuse_review_when_no_match(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_server as mcp_server

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)

    result = await ph._handle_create_pipeline(
        {
            "name": "distinct-reuse-pipeline",
            "description": "A unique pipeline description that should not resemble existing fixtures.",
            "steps": [],
        }
    )

    assert result["success"] is True
    assert result["reuse_review"]["decision_outcome"] == "new_component_justified"
    review = patch_db.knowledge_entity_get("reuse-pipeline-distinct-reuse-pipeline")
    assert review is not None
    assert review["status"] == "new_component_justified"
    context = patch_db.knowledge_context("reuse", review["id"])
    assert any(item["entity_type"] == "pipeline" and item["relation_type"] == "documents" for item in context["related"])


@pytest.mark.asyncio
async def test_create_pipeline_blocks_when_similar_pipeline_exists_without_explicit_reuse_decision(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_server as mcp_server

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)

    await ph._handle_create_pipeline(
        {
            "name": "hmk-reuse-source",
            "description": "Download HMK files and persist structured extraction results.",
            "steps": [],
        }
    )

    result = await ph._handle_create_pipeline(
        {
            "name": "hmk-reuse-variant",
            "description": "Download HMK files and persist structured extraction results.",
            "steps": [],
        }
    )

    assert result["success"] is False
    assert result["reuse_review"]["blocking"] is True
    assert result["reuse_review"]["similar_components"]


@pytest.mark.asyncio
async def test_create_pipeline_with_explicit_reuse_decision_persists_review_links(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_server as mcp_server

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)

    await ph._handle_create_pipeline(
        {
            "name": "hmk-existing",
            "description": "Existing HMK orchestrator for reuse review.",
            "steps": [],
        }
    )

    result = await ph._handle_create_pipeline(
        {
            "name": "hmk-explicit-reuse",
            "description": "Existing HMK orchestrator for reuse review.",
            "reuse_decision_outcome": "modified_existing_component",
            "reuse_rationale": "The existing orchestrator was reviewed and extended into a new guarded variant.",
            "reuse_reviewed_components": ["pipeline:hmk-existing"],
            "steps": [],
        }
    )

    assert result["success"] is True
    review = patch_db.knowledge_entity_get("reuse-pipeline-hmk-explicit-reuse")
    assert review is not None
    context = patch_db.knowledge_context("reuse", review["id"])
    relations = {(item["relation_type"], item["entity_type"], item["entity_id"]) for item in context["related"]}
    assert ("documents", "pipeline", patch_db.get_pipeline("hmk-explicit-reuse")["id"]) in relations or any(
        rel[0] == "documents" and rel[1] == "pipeline" for rel in relations
    )
    assert any(rel[0] == "compared_against" and rel[1] == "pipeline" for rel in relations)


@pytest.mark.asyncio
async def test_create_brick_blocks_when_similar_brick_exists_without_explicit_reuse_decision(patch_db, monkeypatch):
    import brix.mcp_handlers.bricks as bh

    monkeypatch.setattr(bh, "BrixDB", lambda: patch_db)

    first = await bh._handle_create_brick(
        {
            "name": "hmk.persist_result",
            "runner": "python",
            "description": "Persist HMK extraction results with status tracking.",
            "when_NOT_to_use": "Do not use for one-off scripts.",
            "input_type": "object",
            "output_type": "object",
        }
    )
    assert first["success"] is True

    result = await bh._handle_create_brick(
        {
            "name": "hmk.persist_results",
            "runner": "python",
            "description": "Persist HMK extraction results with status tracking.",
            "when_NOT_to_use": "Do not use for one-off scripts.",
            "input_type": "object",
            "output_type": "object",
        }
    )

    assert result["success"] is False
    assert result["reuse_review"]["blocking"] is True
    assert result["reuse_review"]["similar_components"]


@pytest.mark.asyncio
async def test_create_brick_with_explicit_reuse_review_persists_knowledge_entity(patch_db, monkeypatch):
    import brix.mcp_handlers.bricks as bh

    monkeypatch.setattr(bh, "BrixDB", lambda: patch_db)

    result = await bh._handle_create_brick(
        {
            "name": "hmk.document.persist_result",
            "runner": "python",
            "description": "Persist HMK extraction results with normalized contracts.",
            "when_NOT_to_use": "Do not use for generic raw file writes.",
            "input_type": "object",
            "output_type": "object",
            "reuse_decision_outcome": "new_component_justified",
            "reuse_rationale": "No comparable stable brick exists with the same contract and HMK-specific persistence semantics.",
            "examples": [{"input": {"doc_id": 1}, "output": {"status": "ok"}}],
        }
    )

    assert result["success"] is True
    review = patch_db.knowledge_entity_get("reuse-brick-hmk-document-persist-result")
    assert review is not None
    assert review["status"] == "new_component_justified"


def test_migrations_patch_creation_tool_schemas_with_reuse_fields(db):
    pipeline_schema = db.mcp_tool_schemas_get("brix__create_pipeline")
    brick_schema = db.mcp_tool_schemas_get("brix__create_brick")

    for schema_row in (pipeline_schema, brick_schema):
        input_schema = schema_row["input_schema"]
        if isinstance(input_schema, str):
            input_schema = json.loads(input_schema)
        props = input_schema["properties"]
        assert "reuse_decision_outcome" in props
        assert "reuse_rationale" in props
        assert "reuse_reviewed_components" in props
