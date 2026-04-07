"""Tests for preserving step config in ``config_json`` for MCP pipeline CRUD."""

from __future__ import annotations

import json

import pytest

from brix.db import BrixDB
from brix.mcp_handlers._shared import _normalize_step_config, _normalize_steps
from brix.mcp_server import _handle_add_step, _handle_create_pipeline


class TestNormalizeStepConfig:
    """The shared normalizer must not rewrite config into params."""

    def test_config_preserved(self):
        step = {"id": "s1", "type": "db.query", "config": {"query": "SELECT 1"}}
        result = _normalize_step_config(step)
        assert result["config"] == {"query": "SELECT 1"}
        assert "params" not in result

    def test_params_preserved_when_both(self):
        step = {
            "id": "s1",
            "type": "db.query",
            "params": {"limit": 5},
            "config": {"query": "SELECT 1"},
        }
        result = _normalize_step_config(step)
        assert result["params"] == {"limit": 5}
        assert result["config"] == {"query": "SELECT 1"}

    def test_non_dict_passthrough(self):
        assert _normalize_step_config("not a dict") == "not a dict"


class TestNormalizeSteps:
    def test_preserves_config_on_all_steps(self):
        steps = [
            {"id": "a", "type": "db.query", "config": {"query": "SELECT 1"}},
            {"id": "b", "type": "http.request", "params": {"url": "https://example.com"}},
        ]
        _normalize_steps(steps)
        assert steps[0]["config"] == {"query": "SELECT 1"}
        assert "params" not in steps[0]
        assert steps[1]["params"] == {"url": "https://example.com"}


@pytest.mark.asyncio
async def test_create_pipeline_persists_config_in_config_json(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)

    result = await _handle_create_pipeline(
        {
            "name": "config-json-create",
            "steps": [
                {
                    "id": "fetch",
                    "type": "db.query",
                    "config": {"connection": "db", "query": "SELECT 1"},
                }
            ],
        }
    )

    assert result["success"] is True

    db = BrixDB()
    pipeline = db.get_pipeline("config-json-create")
    assert pipeline is not None
    with db._connect() as conn:
        row = conn.execute(
            """
            SELECT config_json, params_json
            FROM pipeline_step
            WHERE pipeline_id=? AND step_key=?
            """,
            (pipeline["id"], "fetch"),
        ).fetchone()

    assert row is not None
    assert json.loads(row[0]) == {"connection": "db", "query": "SELECT 1"}
    assert row[1] in (None, "", "{}")


@pytest.mark.asyncio
async def test_add_step_persists_config_in_config_json(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)

    await _handle_create_pipeline({"name": "config-json-add", "steps": []})
    result = await _handle_add_step(
        {
            "pipeline_name": "config-json-add",
            "step_id": "fetch",
            "type": "db.query",
            "config": {"connection": "db", "query": "SELECT 1"},
        }
    )

    assert result["success"] is True

    db = BrixDB()
    pipeline = db.get_pipeline("config-json-add")
    assert pipeline is not None
    with db._connect() as conn:
        row = conn.execute(
            """
            SELECT config_json, params_json
            FROM pipeline_step
            WHERE pipeline_id=? AND step_key=?
            """,
            (pipeline["id"], "fetch"),
        ).fetchone()

    assert row is not None
    assert json.loads(row[0]) == {"connection": "db", "query": "SELECT 1"}
    assert row[1] in (None, "", "{}")
