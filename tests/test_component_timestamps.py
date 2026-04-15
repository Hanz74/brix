from __future__ import annotations

import asyncio

import pytest

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "component_timestamps.db")


@pytest.fixture
def patch_db(monkeypatch, db):
    import brix.db as db_mod

    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", db.db_path)
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    return db


@pytest.fixture
def patch_pipeline_dir(tmp_path, monkeypatch, db):
    monkeypatch.setattr("brix.mcp_handlers._shared._pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
    monkeypatch.setattr(
        "brix.mcp_handlers.pipelines.PipelineStore",
        lambda pipelines_dir=None: PipelineStore(
            pipelines_dir=tmp_path,
            search_paths=[tmp_path],
            db=db,
        ),
    )
    return tmp_path


@pytest.mark.asyncio
async def test_pipeline_list_and_search_include_timestamps(patch_db, patch_pipeline_dir):
    from brix.mcp_handlers.pipelines import (
        _handle_create_pipeline,
        _handle_list_pipelines,
        _handle_search_pipelines,
    )

    await _handle_create_pipeline(
        {
            "name": "timestamp-pipeline",
            "description": "Invoice search target",
            "steps": [],
        }
    )

    listed = await _handle_list_pipelines({})
    pipeline = next(item for item in listed["pipelines"] if item["name"] == "timestamp-pipeline")
    assert pipeline["created_at"]
    assert pipeline["updated_at"]

    searched = await _handle_search_pipelines({"query": "invoice"})
    match = next(item for item in searched["pipelines"] if item["name"] == "timestamp-pipeline")
    assert match["created_at"]
    assert match["updated_at"]


def test_filtered_helper_list_includes_timestamps(monkeypatch, tmp_path, patch_db) -> None:
    from brix.helper_registry import HelperRegistry
    from brix.mcp_handlers.helpers import _handle_list_helpers

    registry = HelperRegistry(db=patch_db)
    monkeypatch.setattr("brix.mcp_handlers._shared._managed_helper_dir", lambda: tmp_path / "helpers")
    (tmp_path / "helpers").mkdir(exist_ok=True)

    registry.register(
        name="timestamp_helper",
        description="Timestamped helper",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        project="buddy",
        tags=["utility"],
    )

    result = asyncio.run(_handle_list_helpers({"project": "buddy"}))
    helper = next(item for item in result["helpers"] if item["name"] == "timestamp_helper")
    assert helper["created_at"]
    assert helper["updated_at"]


@pytest.mark.asyncio
async def test_brick_handlers_include_timestamps() -> None:
    from brix.mcp_handlers.steps import (
        _handle_get_brick_schema,
        _handle_list_bricks,
        _handle_search_bricks,
    )

    listed = await _handle_list_bricks({})
    brick = next(item for item in listed["bricks"] if item["name"] == "db.exec")
    assert brick["created_at"]
    assert brick["updated_at"]

    searched = await _handle_search_bricks({"query": "db.exec"})
    match = next(item for item in searched["bricks"] if item["name"] == "db.exec")
    assert match["created_at"]
    assert match["updated_at"]

    schema = await _handle_get_brick_schema({"brick_name": "db.exec"})
    assert schema["created_at"]
    assert schema["updated_at"]
