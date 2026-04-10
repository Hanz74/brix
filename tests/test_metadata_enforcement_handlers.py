from __future__ import annotations

import json

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


@pytest.fixture
def db(tmp_path):
    database = BrixDB(db_path=tmp_path / "metadata_enforcement.db")
    run_pending_migrations(database)
    return database


@pytest.fixture
def patch_db(tmp_path, monkeypatch, db):
    import brix.db as db_mod

    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", db.db_path)
    return db


def test_entity_metadata_roundtrip(db):
    row = db.entity_metadata_upsert(
        "pipeline",
        "demo-pipeline",
        owner="team-brix",
        purpose="Validate metadata roundtrip",
        lifecycle_stage="draft",
    )

    assert row["entity_type"] == "pipeline"
    assert row["entity_ref"] == "demo-pipeline"
    assert row["owner"] == "team-brix"
    assert db.entity_metadata_get("pipeline", "demo-pipeline")["purpose"] == "Validate metadata roundtrip"


@pytest.mark.asyncio
async def test_create_pipeline_persists_draft_metadata_and_repair_prompts(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)

    result = await ph._handle_create_pipeline(
        {
            "name": "metadata-pipeline",
            "project": "buddy",
            "description": "Pipeline with intentionally incomplete governance metadata.",
            "steps": [],
        }
    )

    assert result["success"] is True
    assert result["metadata_policy"]["draft_enforced"] is True
    assert result["repair_prompts"]
    stored = patch_db.entity_metadata_get("pipeline", "metadata-pipeline")
    assert stored["lifecycle_stage"] == "draft"


@pytest.mark.asyncio
async def test_create_pipeline_persists_supplemental_metadata_without_org_fields(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)

    result = await ph._handle_create_pipeline(
        {
            "name": "metadata-pipeline-no-org",
            "description": "Pipeline with metadata but no explicit org assignment.",
            "owner": "team-brix",
            "purpose": "Validate supplemental metadata persistence.",
            "steps": [],
        }
    )

    assert result["success"] is True
    stored = patch_db.entity_metadata_get("pipeline", "metadata-pipeline-no-org")
    assert stored["owner"] == "team-brix"
    assert stored["purpose"] == "Validate supplemental metadata persistence."


@pytest.mark.asyncio
async def test_update_pipeline_blocks_active_promotion_without_required_metadata(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.pipelines as ph

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)

    await ph._handle_create_pipeline(
        {
            "name": "promotion-pipeline",
            "project": "buddy",
            "description": "Pipeline missing owner, purpose, and source intent.",
            "steps": [],
        }
    )

    result = await ph._handle_update_pipeline(
        {
            "name": "promotion-pipeline",
            "lifecycle_stage": "active",
        }
    )

    assert result["success"] is False
    assert "metadata_policy" in result
    assert "owner" in result["metadata_policy"]["blocking_fields"]


@pytest.mark.asyncio
async def test_create_helper_preserves_governance_but_still_emits_draft_metadata_guidance(
    tmp_path,
    monkeypatch,
    patch_db,
):
    import brix.mcp_handlers.helpers as hh

    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    result = await hh._handle_create_helper(
        {
            "name": "metadata_helper",
            "code": "def run(data): return data",
            "description": "Vendor-specific helper that otherwise looks governed.",
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
            "project": "buddy",
            "tags": ["extract"],
            "reason_not_a_brick": "Temporary adapter until the shared brick lands.",
        }
    )

    assert result["success"] is True
    assert result["metadata_policy"]["draft_enforced"] is True
    assert patch_db.get_helper("metadata_helper")["governance_status"] == "governed"
    assert patch_db.entity_metadata_get("helper", "metadata_helper")["lifecycle_stage"] == "draft"


@pytest.mark.asyncio
async def test_update_helper_blocks_governed_helper_without_owner_metadata(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.helpers as hh

    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    await hh._handle_create_helper(
        {
            "name": "metadata_helper_update",
            "code": "def run(data): return data",
            "description": "Governed helper that still lacks owner metadata.",
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
            "project": "buddy",
            "tags": ["extract"],
            "reason_not_a_brick": "Temporary adapter until a shared brick exists.",
        }
    )

    result = await hh._handle_update_helper({"name": "metadata_helper_update", "description": "Still governed"})

    assert result["success"] is False
    assert "owner" in result["metadata_policy"]["blocking_fields"]


@pytest.mark.asyncio
async def test_update_brick_blocks_stable_status_without_required_metadata(patch_db, monkeypatch):
    import brix.mcp_handlers.bricks as bh

    monkeypatch.setattr(bh, "BrixDB", lambda: patch_db)

    create_result = await bh._handle_create_brick(
        {
            "name": "metadata.brick",
            "runner": "python",
            "description": "Brick missing owner and examples on purpose.",
            "when_NOT_to_use": "Do not use for vendor-specific one-off hacks.",
            "input_type": "object",
            "output_type": "object",
        }
    )
    assert create_result["success"] is True

    update_result = await bh._handle_update_brick(
        {
            "name": "metadata.brick",
            "status": "stable",
        }
    )

    assert update_result["success"] is False
    assert "owner" in update_result["metadata_policy"]["blocking_fields"]


@pytest.mark.asyncio
async def test_update_brick_persists_examples_used_for_metadata_enforcement(patch_db, monkeypatch):
    import brix.mcp_handlers.bricks as bh

    monkeypatch.setattr(bh, "BrixDB", lambda: patch_db)

    await bh._handle_create_brick(
        {
            "name": "examples.brick",
            "runner": "python",
            "description": "Brick that will be promoted once examples are stored.",
            "when_NOT_to_use": "Do not use for one-off vendor logic.",
            "input_type": "object",
            "output_type": "object",
        }
    )

    update_result = await bh._handle_update_brick(
        {
            "name": "examples.brick",
            "owner": "team-brix",
            "examples": [{"input": {"x": 1}, "output": {"x": 1}}],
            "status": "stable",
        }
    )

    assert update_result["success"] is True
    stored = patch_db.brick_definitions_get("examples.brick")
    assert json.loads(stored["examples"]) == [{"input": {"x": 1}, "output": {"x": 1}}]


@pytest.mark.asyncio
async def test_connection_add_persists_supplemental_metadata(patch_db):
    from brix.mcp_handlers.connections import _handle_connection_add

    result = await _handle_connection_add(
        {
            "name": "metadata_conn",
            "dsn": ":memory:",
            "driver": "sqlite",
            "description": "Connection with explicit owner metadata.",
            "project": "buddy",
            "owner": "team-data",
            "usage_scope": "hmk extraction only",
        }
    )

    assert result["success"] is True
    stored = patch_db.entity_metadata_get("connection", "metadata_conn")
    assert stored["owner"] == "team-data"
    assert stored["usage_scope"] == "hmk extraction only"


@pytest.mark.asyncio
async def test_help_topic_update_blocks_stable_status_without_metadata(patch_db):
    from brix.mcp_handlers.dbfirst_crud import _handle_help_topic_add, _handle_help_topic_update

    add_result = await _handle_help_topic_add(
        {
            "name": "metadata-help",
            "title": "Metadata help topic",
            "content": "Document the policy.",
        }
    )
    assert add_result["success"] is True

    update_result = await _handle_help_topic_update(
        {
            "name": "metadata-help",
            "status": "stable",
        }
    )

    assert update_result["success"] is False
    assert "owner" in update_result["metadata_policy"]["blocking_fields"]
