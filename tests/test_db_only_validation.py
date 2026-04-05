from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore


@pytest.fixture
def db():
    return BrixDB()


@pytest.fixture
def handlers():
    from brix.mcp_server import (
        _handle_add_step,
        _handle_create_pipeline,
        _handle_remove_step,
        _handle_search_pipelines,
        _handle_update_step,
    )

    return {
        "add_step": _handle_add_step,
        "create_pipeline": _handle_create_pipeline,
        "remove_step": _handle_remove_step,
        "search_pipelines": _handle_search_pipelines,
        "update_step": _handle_update_step,
    }


@pytest.fixture
def store(tmp_path, db):
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db)


def _patch_pipeline_dir(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)


def _pipeline_row(db: BrixDB, name: str) -> dict:
    row = db.get_pipeline(name)
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_create_pipeline_creates_step_rows(tmp_path, monkeypatch, db, handlers):
    _patch_pipeline_dir(monkeypatch, tmp_path)

    result = await handlers["create_pipeline"](
        {
            "name": "db-only-create-rows",
            "steps": [
                {"id": "first", "type": "flow.set", "values": {"value": 1}},
                {"id": "second", "type": "flow.set", "values": {"value": 2}},
            ],
        }
    )

    row = _pipeline_row(db, "db-only-create-rows")
    steps = db.get_steps(row["id"])

    assert result["success"] is True
    assert [step["id"] for step in steps] == ["first", "second"]


@pytest.mark.asyncio
async def test_add_step_creates_step_row(tmp_path, monkeypatch, db, handlers):
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"]({"name": "db-only-add-step", "steps": []})
    result = await handlers["add_step"](
        {
            "pipeline_name": "db-only-add-step",
            "step_id": "fetch",
            "type": "http.request",
            "url": "https://example.com",
        }
    )

    row = _pipeline_row(db, "db-only-add-step")
    step = db.get_step_by_id(row["id"], "fetch")

    assert result["success"] is True
    assert step is not None
    assert step["url"] == "https://example.com"


@pytest.mark.asyncio
async def test_remove_step_deletes_row(tmp_path, monkeypatch, db, handlers):
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "db-only-remove-step",
            "steps": [{"id": "gone", "type": "flow.set", "values": {"value": 1}}],
        }
    )

    result = await handlers["remove_step"](
        {"pipeline_name": "db-only-remove-step", "step_id": "gone"}
    )

    row = _pipeline_row(db, "db-only-remove-step")

    assert result["success"] is True
    assert db.get_step_by_id(row["id"], "gone") is None


@pytest.mark.asyncio
async def test_update_step_updates_db_row(tmp_path, monkeypatch, db, handlers):
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "db-only-update-step",
            "steps": [{"id": "fetch", "type": "http.request", "url": "https://old.example.com"}],
        }
    )

    result = await handlers["update_step"](
        {
            "pipeline_name": "db-only-update-step",
            "step_id": "fetch",
            "updates": {"url": "https://new.example.com", "timeout": "30s"},
        }
    )

    row = _pipeline_row(db, "db-only-update-step")
    step = db.get_step_by_id(row["id"], "fetch")

    assert result["success"] is True
    assert step is not None
    assert step["url"] == "https://new.example.com"
    assert step["timeout"] == "30s"


def test_pipeline_credential_table_populated(store, db):
    store.save(
        {
            "name": "db-only-credentials",
            "steps": [],
            "credentials": {
                "api_key": "API_KEY",
                "oauth": {"env": "OAUTH_TOKEN", "refresh": {"type": "oauth2"}},
            },
        }
    )

    row = _pipeline_row(db, "db-only-credentials")
    credentials = db.get_pipeline_credentials(row["id"])

    assert credentials == [
        {"pipeline_id": row["id"], "name": "api_key", "env": "API_KEY", "refresh": None},
        {
            "pipeline_id": row["id"],
            "name": "oauth",
            "env": "OAUTH_TOKEN",
            "refresh": {"type": "oauth2"},
        },
    ]


def test_pipeline_input_table_populated(store, db):
    store.save(
        {
            "name": "db-only-inputs",
            "steps": [],
            "input": {
                "folder": {"type": "string", "default": "Inbox", "description": "Folder name"},
                "limit": {"type": "integer", "default": 25},
            },
        }
    )

    row = _pipeline_row(db, "db-only-inputs")
    inputs = db.get_pipeline_inputs(row["id"])

    assert inputs == [
        {
            "pipeline_id": row["id"],
            "name": "folder",
            "type": "string",
            "default": "Inbox",
            "description": "Folder name",
        },
        {
            "pipeline_id": row["id"],
            "name": "limit",
            "type": "integer",
            "default": 25,
            "description": None,
        },
    ]


def test_step_order_preserved_roundtrip(store):
    store.save(
        {
            "name": "db-only-step-order",
            "steps": [
                {"id": "first", "type": "flow.set", "values": {"value": 1}},
                {"id": "second", "type": "flow.set", "values": {"value": 2}},
                {"id": "third", "type": "flow.set", "values": {"value": 3}},
            ],
        }
    )

    raw = store.load_raw("db-only-step-order")

    assert [step["id"] for step in raw["steps"]] == ["first", "second", "third"]


def test_nested_steps_stored_as_json(store, db):
    store.save(
        {
            "name": "db-only-nested",
            "steps": [
                {
                    "id": "repeat-step",
                    "type": "repeat",
                    "sequence": [{"id": "inner-repeat", "type": "flow.set", "values": {"ok": True}}],
                },
                {
                    "id": "choose-step",
                    "type": "choose",
                    "choices": [{"when": "true", "steps": [{"id": "branch", "type": "flow.set"}]}],
                    "default_steps": [{"id": "fallback", "type": "flow.set"}],
                },
                {
                    "id": "parallel-step",
                    "type": "parallel",
                    "sub_steps": [{"id": "worker", "type": "flow.set", "values": {"n": 1}}],
                },
            ],
        }
    )

    row = _pipeline_row(db, "db-only-nested")
    repeat_step = db.get_step_by_id(row["id"], "repeat-step")
    choose_step = db.get_step_by_id(row["id"], "choose-step")
    parallel_step = db.get_step_by_id(row["id"], "parallel-step")

    assert repeat_step is not None
    assert repeat_step["sequence"][0]["id"] == "inner-repeat"
    assert choose_step is not None
    assert choose_step["choices"][0]["steps"][0]["id"] == "branch"
    assert choose_step["default_steps"][0]["id"] == "fallback"
    assert parallel_step is not None
    assert parallel_step["sub_steps"][0]["id"] == "worker"


@pytest.mark.asyncio
async def test_search_pipelines_from_db(tmp_path, monkeypatch, store, handlers):
    _patch_pipeline_dir(monkeypatch, tmp_path)

    store.save(
        {
            "name": "db-search-target",
            "description": "Search me from database rows",
            "steps": [],
        }
    )

    result = await handlers["search_pipelines"]({"query": "search"})

    assert result["success"] is True
    assert any(p["name"] == "db-search-target" for p in result["pipelines"])


def test_refresh_pipeline_deps_from_step_rows(store, db):
    db.upsert_helper("helper_dep", "/tmp/helper_dep.py")
    store.save(
        {
            "name": "db-only-refresh-deps",
            "steps": [{"id": "call-helper", "type": "script.python", "helper": "helper_dep"}],
        }
    )

    db.refresh_pipeline_deps("db-only-refresh-deps")

    helpers = db.get_pipeline_helpers("db-only-refresh-deps")

    assert [item["name"] for item in helpers] == ["helper_dep"]


@pytest.mark.asyncio
async def test_config_mapping_in_step_row(tmp_path, monkeypatch, db, handlers):
    _patch_pipeline_dir(monkeypatch, tmp_path)

    result = await handlers["create_pipeline"](
        {
            "name": "db-only-config-map",
            "steps": [
                {
                    "id": "fetch",
                    "type": "http.request",
                    "config": {"url": "https://example.com", "method": "GET"},
                }
            ],
        }
    )

    row = _pipeline_row(db, "db-only-config-map")
    step = db.get_step_by_id(row["id"], "fetch")

    assert result["success"] is True
    assert step is not None
    assert step["params"] == {"url": "https://example.com", "method": "GET"}
    assert step["config"] is None


@pytest.mark.asyncio
async def test_promoted_field_in_step_column(tmp_path, monkeypatch, db, handlers):
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"]({"name": "db-only-promoted", "steps": []})
    result = await handlers["add_step"](
        {
            "pipeline_name": "db-only-promoted",
            "step_id": "use-helper",
            "type": "script.python",
            "params": {"helper": "my_helper", "custom": "value"},
        }
    )

    row = _pipeline_row(db, "db-only-promoted")
    step = db.get_step_by_id(row["id"], "use-helper")

    assert result["success"] is True
    assert step is not None
    assert step["helper"] == "my_helper"
    assert step["params"] == {"custom": "value"}
