from __future__ import annotations

import pytest


def _patch_pipeline_dir(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)


def _handlers():
    from brix.mcp_server import _HANDLERS, _handle_create_pipeline

    return {
        "create_pipeline": _handle_create_pipeline,
        "diagnose_step": _HANDLERS["brix__diagnose_step"],
    }


@pytest.mark.asyncio
async def test_diagnose_step_renders_jinja2_params_correctly(tmp_path, monkeypatch):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "diagnose-render-pipeline",
            "steps": [
                {
                    "id": "render_me",
                    "type": "script.python",
                    "script": "run.py",
                    "params": {
                        "name": "{{ input.name }}",
                        "count": "{{ input.count }}",
                        "payload": {"tags": "{{ input.tags }}"},
                    },
                    "when": "{{ input.enabled }}",
                    "foreach": "{{ input.tags }}",
                }
            ],
        }
    )

    result = await handlers["diagnose_step"](
        {
            "pipeline_id": "diagnose-render-pipeline",
            "step_id": "render_me",
            "input": {
                "name": "Ada",
                "count": 3,
                "enabled": True,
                "tags": ["x", "y"],
            },
        }
    )

    assert result["success"] is True
    assert result["rendered_params"]["name"] == "Ada"
    assert result["rendered_params"]["count"] == 3
    assert result["rendered_params"]["payload"]["tags"] == ["x", "y"]
    assert result["rendered_when"] is True
    assert result["rendered_foreach"] == ["x", "y"]


@pytest.mark.asyncio
async def test_diagnose_step_detects_config_vs_params_misplacement(tmp_path, monkeypatch):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "diagnose-placement-pipeline",
            "steps": [
                {
                    "id": "query_db",
                    "type": "db.query",
                    "params": {
                        "connection": "analytics",
                        "query": "SELECT 1",
                    },
                }
            ],
        }
    )

    result = await handlers["diagnose_step"](
        {
            "pipeline_id": "diagnose-placement-pipeline",
            "step_id": "query_db",
        }
    )

    assert result["success"] is True
    assert result["config_vs_params"]["connection"] == "in params ⚠ should be in config"


@pytest.mark.asyncio
async def test_diagnose_step_shows_schema_issues(tmp_path, monkeypatch):
    handlers = _handlers()
    _patch_pipeline_dir(monkeypatch, tmp_path)

    await handlers["create_pipeline"](
        {
            "name": "diagnose-schema-pipeline",
            "steps": [
                {
                    "id": "schema_step",
                    "type": "script.python",
                    "script": "run.py",
                    "params": {"unexpected": "value"},
                }
            ],
        }
    )

    monkeypatch.setattr(
        "brix.validator.PipelineValidator._resolve_step_schema",
        lambda self, step: {
            "type": "object",
            "properties": {
                "script": {"type": "string"},
                "helper": {"type": "string"},
            },
            "required": ["script", "helper"],
        },
    )

    result = await handlers["diagnose_step"](
        {
            "pipeline_id": "diagnose-schema-pipeline",
            "step_id": "schema_step",
        }
    )

    assert result["success"] is True
    assert result["schema_check"]["required"] == ["script", "helper"]
    assert result["schema_check"]["missing"] == ["helper"]
    assert "unexpected" in result["schema_check"]["extra"]
