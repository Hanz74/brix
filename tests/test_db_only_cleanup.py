from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore
from brix.startup_sync import run_startup_sync


@pytest.fixture
def db():
    return BrixDB()


@pytest.fixture
def store(tmp_path, db):
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db)


@pytest.fixture
def handlers():
    from brix.mcp_server import (
        _handle_add_step,
        _handle_create_pipeline,
        _handle_remove_step,
        _handle_update_step,
    )

    return {
        "add_step": _handle_add_step,
        "create_pipeline": _handle_create_pipeline,
        "remove_step": _handle_remove_step,
        "update_step": _handle_update_step,
    }


def _patch_pipeline_dir(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)


def _pipeline_row(db: BrixDB, name: str) -> dict:
    row = db.get_pipeline(name)
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_pipeline_crud_does_not_create_yaml_files(tmp_path, monkeypatch, db, handlers):
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "db-only-cleanup",
            "steps": [{"id": "first", "type": "flow.set", "values": {"value": 1}}],
        }
    )
    await handlers["add_step"](
        {
            "pipeline_name": "db-only-cleanup",
            "step_id": "fetch",
            "type": "http.request",
            "url": "https://example.com",
        }
    )
    await handlers["update_step"](
        {
            "pipeline_name": "db-only-cleanup",
            "step_id": "fetch",
            "updates": {"url": "https://example.org"},
        }
    )
    await handlers["remove_step"](
        {"pipeline_name": "db-only-cleanup", "step_id": "first"}
    )

    row = _pipeline_row(db, "db-only-cleanup")
    assert row["migration_status"] == "v71_complete"
    assert list(tmp_path.rglob("*.yaml")) == []


def test_startup_sync_works_without_pipeline_disk_scan(monkeypatch, db):
    pipeline_id = db.upsert_pipeline(
        name="startup-cleanup",
        path="/virtual/startup-cleanup.yaml",
    )
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET yaml_content=? WHERE id=?",
            (
                """
name: startup-cleanup
steps:
  - id: migrated
    type: flow.set
    values:
      ok: true
""".strip(),
                pipeline_id,
            ),
        )

    def _boom():
        raise AssertionError("pipeline disk scan should not run")

    monkeypatch.setattr("brix.startup_sync._scan_pipeline_files", _boom)
    summary = run_startup_sync(db)
    row = _pipeline_row(db, "startup-cleanup")

    assert summary["pipelines_imported"] == 0
    assert summary["pipelines_normalized"] == 1
    assert summary["descriptions_backfilled"] == 0
    assert row["migration_status"] == "v71_complete"


def test_refresh_pipeline_deps_prefers_step_rows_over_yaml_content(store, db):
    db.upsert_helper("row_helper", "/tmp/row_helper.py")
    db.upsert_helper("yaml_helper", "/tmp/yaml_helper.py")

    store.save(
        {
            "name": "deps-cleanup",
            "steps": [{"id": "call-helper", "type": "script.python", "helper": "row_helper"}],
        }
    )

    row = _pipeline_row(db, "deps-cleanup")
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET yaml_content=? WHERE id=?",
            (
                "name: deps-cleanup\nsteps:\n  - id: old\n    type: script.python\n    helper: yaml_helper\n",
                row["id"],
            ),
        )

    db.refresh_pipeline_deps("deps-cleanup")

    helpers = db.get_pipeline_helpers("deps-cleanup")
    assert [item["name"] for item in helpers] == ["row_helper"]
