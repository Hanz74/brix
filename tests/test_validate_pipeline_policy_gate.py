from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


@pytest.fixture
def db(tmp_path):
    database = BrixDB(db_path=tmp_path / "validate_policy.db")
    run_pending_migrations(database)
    return database


@pytest.fixture
def patch_db(tmp_path, monkeypatch, db):
    import brix.db as db_mod

    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", db.db_path)
    return db


@pytest.mark.asyncio
async def test_validate_pipeline_surfaces_metadata_policy_guidance(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_server as mcp_server

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)

    create_result = await ph._handle_create_pipeline(
        {
            "name": "validate-policy-metadata",
            "project": "buddy",
            "description": "Pipeline missing governance metadata.",
            "steps": [],
        }
    )
    assert create_result["success"] is True

    result = await ph._handle_validate_pipeline({"pipeline_id": "validate-policy-metadata"})

    assert result["success"] is True
    assert result["policy_checks"]["metadata"]["missing_fields"]
    assert "owner" in result["policy_checks"]["metadata"]["missing_fields"]
    assert any("brix__get_missing_metadata" in action for action in result["next_actions"])


@pytest.mark.asyncio
async def test_validate_pipeline_blocks_when_reuse_review_is_missing(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_server as mcp_server
    from brix.pipeline_store import PipelineStore

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)

    existing = await ph._handle_create_pipeline(
        {
            "name": "reuse-existing-pipeline",
            "description": "Download HMK files and persist extraction results.",
            "steps": [],
            "reuse_decision_outcome": "new_component_justified",
            "reuse_rationale": "Seed baseline pipeline.",
        }
    )
    assert existing["success"] is True

    store = PipelineStore(pipelines_dir=tmp_path, db=patch_db)
    store.save(
        {
            "name": "reuse-variant-pipeline",
            "version": "1.0.0",
            "description": "Download HMK files and persist extraction results.",
            "steps": [],
        },
        "reuse-variant-pipeline",
    )
    patch_db.upsert_pipeline(
        name="reuse-variant-pipeline",
        path=str(tmp_path / "reuse-variant-pipeline.yaml"),
        project="buddy",
    )

    result = await ph._handle_validate_pipeline({"pipeline_id": "reuse-variant-pipeline"})

    assert result["success"] is True
    assert result["valid"] is False
    assert result["policy_checks"]["reuse"]["blocking"] is True
    assert result["policy_checks"]["reuse"]["similar_components"]
    assert any("brix__record_reuse_decision" in action for action in result["next_actions"])
