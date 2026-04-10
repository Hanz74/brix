from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


@pytest.fixture
def db(tmp_path):
    database = BrixDB(db_path=tmp_path / "tips_guidance.db")
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
async def test_get_tips_surfaces_metadata_guidance_for_pipeline_gap(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.help as help_mod
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_server as mcp_server

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)
    monkeypatch.setattr(help_mod._registry, "list_all", lambda: [])

    create_result = await ph._handle_create_pipeline(
        {
            "name": "tips-guidance-pipeline",
            "project": "buddy",
            "description": "Pipeline with unresolved metadata gaps.",
            "steps": [],
        }
    )
    assert create_result["success"] is True

    result = await help_mod._handle_get_tips({})

    assert "guidance" in result
    assert any(
        item["entity_type"] == "pipeline" and item["entity_id"] == "tips-guidance-pipeline"
        for item in result["guidance"]["metadata_gaps"]
    )
    tips_text = "\n".join(result["tips"])
    assert "AUTHORING GUIDANCE" in tips_text
    assert "tips-guidance-pipeline" in tips_text
    assert "brix__get_missing_metadata" in tips_text
    assert "brix__repair_component_metadata" in tips_text


@pytest.mark.asyncio
async def test_get_tips_surfaces_reuse_guidance_for_helper_brick_candidate(tmp_path, monkeypatch, patch_db):
    import brix.mcp_handlers.help as help_mod
    import brix.mcp_handlers.helpers as hh

    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)
    monkeypatch.setattr(help_mod._registry, "list_all", lambda: [])

    create_result = await hh._handle_create_helper(
        {
            "name": "persist_invoice_rows",
            "code": "def run(data):\n    return data\n",
            "description": "Persist invoice rows into the database with stable normalization.",
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
            "project": "buddy",
            "tags": ["persistence"],
            "reason_not_a_brick": "Temporary adapter until the shared persistence brick is fully adopted.",
        }
    )
    assert create_result["success"] is True

    result = await help_mod._handle_get_tips({})

    assert any(item["helper"] == "persist_invoice_rows" for item in result["guidance"]["reuse_candidates"])
    tips_text = "\n".join(result["tips"])
    assert "persist_invoice_rows" in tips_text
    assert "record_reuse_decision" in tips_text
