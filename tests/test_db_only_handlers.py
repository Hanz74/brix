from __future__ import annotations

import json

import pytest

from brix.credential_store import CredentialStore
from brix.db import BrixDB
from brix.engine import PipelineEngine
from brix.models import Pipeline


@pytest.fixture
def db():
    return BrixDB()


def _patch_pipeline_dir(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)


def _handlers():
    from brix.mcp_server import (
        _handle_add_step,
        _handle_create_pipeline,
        _handle_get_pipeline,
        _handle_get_step,
        _handle_remove_step,
        _handle_update_step,
    )

    return {
        "add_step": _handle_add_step,
        "create_pipeline": _handle_create_pipeline,
        "get_pipeline": _handle_get_pipeline,
        "get_step": _handle_get_step,
        "remove_step": _handle_remove_step,
        "update_step": _handle_update_step,
    }


def _pipeline_row(db: BrixDB, name: str) -> dict:
    row = db.get_pipeline(name)
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_update_step_timeout_none_stays_null_on_reload(tmp_path, monkeypatch, db):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "timeout-null-pipeline",
            "steps": [
                {
                    "id": "fetch",
                    "type": "http.request",
                    "method": "POST",
                    "body": {"hello": "world"},
                    "timeout": "45s",
                }
            ],
        }
    )

    result = await handlers["update_step"](
        {
            "pipeline_name": "timeout-null-pipeline",
            "step_id": "fetch",
            "updates": {"timeout": None},
        }
    )

    row = _pipeline_row(db, "timeout-null-pipeline")
    reloaded = await handlers["get_pipeline"]({"pipeline_id": "timeout-null-pipeline"})
    step = db.get_step_by_id(row["id"], "fetch")

    assert result["success"] is True
    assert step is not None
    assert step["timeout"] is None
    assert reloaded["steps"][0]["timeout"] is None


@pytest.mark.asyncio
async def test_update_step_only_changes_requested_field(tmp_path, monkeypatch, db):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "partial-update-pipeline",
            "steps": [
                {
                    "id": "fetch",
                    "type": "http.request",
                    "method": "POST",
                    "headers": {"Authorization": "Bearer token"},
                    "body": {"hello": "world"},
                    "timeout": "45s",
                }
            ],
        }
    )

    await handlers["update_step"](
        {
            "pipeline_name": "partial-update-pipeline",
            "step_id": "fetch",
            "updates": {"timeout": None},
        }
    )

    row = _pipeline_row(db, "partial-update-pipeline")
    step = db.get_step_by_id(row["id"], "fetch")

    assert step is not None
    assert step["timeout"] is None
    assert step["method"] == "POST"
    assert step["headers"] == {"Authorization": "Bearer token"}
    assert step["body"] == {"hello": "world"}


@pytest.mark.asyncio
async def test_add_step_creates_pipeline_step_row_with_correct_step_order(tmp_path, monkeypatch, db):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "add-step-pipeline",
            "steps": [
                {"id": "first", "type": "flow.set", "values": {"value": 1}},
                {"id": "third", "type": "flow.set", "values": {"value": 3}},
            ],
        }
    )

    result = await handlers["add_step"](
        {
            "pipeline_name": "add-step-pipeline",
            "step_id": "second",
            "type": "flow.set",
            "values": {"value": 2},
            "position": "after:first",
        }
    )

    row = _pipeline_row(db, "add-step-pipeline")
    with db._connect() as conn:
        positions = conn.execute(
            "SELECT step_key, position FROM pipeline_step WHERE pipeline_id=? ORDER BY position ASC",
            (row["id"],),
        ).fetchall()

    assert result["success"] is True
    assert [(item[0], item[1]) for item in positions] == [
        ("first", 0),
        ("second", 1),
        ("third", 2),
    ]


@pytest.mark.asyncio
async def test_remove_step_deletes_row_and_reorders_remaining_steps(tmp_path, monkeypatch, db):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "remove-step-pipeline",
            "steps": [
                {"id": "first", "type": "flow.set", "values": {"value": 1}},
                {"id": "second", "type": "flow.set", "values": {"value": 2}},
                {"id": "third", "type": "flow.set", "values": {"value": 3}},
            ],
        }
    )

    result = await handlers["remove_step"](
        {"pipeline_name": "remove-step-pipeline", "step_id": "second"}
    )

    row = _pipeline_row(db, "remove-step-pipeline")
    with db._connect() as conn:
        positions = conn.execute(
            "SELECT step_key, position FROM pipeline_step WHERE pipeline_id=? ORDER BY position ASC",
            (row["id"],),
        ).fetchall()

    assert result["success"] is True
    assert db.get_step_by_id(row["id"], "second") is None
    assert [(item[0], item[1]) for item in positions] == [
        ("first", 0),
        ("third", 1),
    ]


@pytest.mark.asyncio
async def test_get_step_returns_data_from_db_row(tmp_path, monkeypatch, db):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "get-step-pipeline",
            "steps": [
                {
                    "id": "fetch",
                    "type": "http.request",
                    "method": "POST",
                    "body": {"hello": "db"},
                    "timeout": "30s",
                }
            ],
        }
    )

    row = _pipeline_row(db, "get-step-pipeline")
    db.update_step_row(row["id"], "fetch", {"timeout": None})

    result = await handlers["get_step"](
        {"pipeline_name": "get-step-pipeline", "step_id": "fetch"}
    )

    assert result["success"] is True
    assert result["step"]["id"] == "fetch"
    assert result["step"]["body"] == {"hello": "db"}
    assert result["step"]["timeout"] is None


@pytest.mark.asyncio
async def test_create_pipeline_with_three_steps_creates_three_pipeline_step_rows(
    tmp_path, monkeypatch, db
):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    result = await handlers["create_pipeline"](
        {
            "name": "three-step-pipeline",
            "steps": [
                {"id": "s1", "type": "flow.set", "values": {"a": 1}},
                {"id": "s2", "type": "flow.set", "values": {"b": 2}},
                {"id": "s3", "type": "flow.set", "values": {"c": 3}},
            ],
        }
    )

    row = _pipeline_row(db, "three-step-pipeline")
    steps = db.get_steps(row["id"])

    assert result["success"] is True
    assert row["migration_status"] == "v71_complete"
    assert [step["id"] for step in steps] == ["s1", "s2", "s3"]


@pytest.mark.asyncio
async def test_step_level_credentials_are_injected_into_helper_env(tmp_path, monkeypatch):
    script_path = tmp_path / "print_env.py"
    script_path.write_text(
        "import json\n"
        "import os\n"
        "print(json.dumps({'api_token': os.environ.get('API_TOKEN', '')}))\n",
        encoding="utf-8",
    )

    cred_store = CredentialStore(db_path=tmp_path / "credentials.db")
    cred_id = cred_store.add("api-token", "api-key", "secret-token")
    monkeypatch.setattr("brix.engine.CredentialStore", lambda: cred_store)
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")

    pipeline = Pipeline.model_validate(
        {
            "name": "step-credential-pipeline",
            "steps": [
                {
                    "id": "show-env",
                    "type": "python",
                    "script": str(script_path),
                }
            ],
        }
    )
    pipeline.steps[0].__dict__["credentials"] = {"API_TOKEN": cred_id}

    result = await PipelineEngine().run(pipeline)

    assert result.success is True
    assert result.result == {"api_token": "secret-token"}
