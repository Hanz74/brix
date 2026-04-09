from __future__ import annotations

import pytest

from brix.mcp_server import _HANDLERS


def _handlers():
    return {
        "create_pipeline": _HANDLERS["brix__create_pipeline"],
        "add_step": _HANDLERS["brix__add_step"],
        "materialize_step": _HANDLERS["brix__materialize_step"],
        "inspect_effective_pipeline": _HANDLERS["brix__inspect_effective_pipeline"],
    }


@pytest.mark.asyncio
async def test_materialize_step_exposes_raw_vs_effective_shape(tmp_path, monkeypatch):
    from brix import mcp_server
    from brix.mcp_handlers import _shared

    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)
    monkeypatch.setattr(_shared, "PIPELINE_DIR", tmp_path)

    handlers = _handlers()
    await handlers["create_pipeline"]({"name": "materialize-one", "steps": []})
    await handlers["add_step"](
        {
            "pipeline_name": "materialize-one",
            "step_id": "child",
            "type": "flow.pipeline",
            "config": {"pipeline": "real-child"},
        }
    )

    result = await handlers["materialize_step"](
        {"pipeline_id": "materialize-one", "step_id": "child"}
    )

    assert result["success"] is True
    assert result["raw"]["step"]["config"]["pipeline"] == "real-child"
    assert result["effective"]["type"] == "flow.pipeline"
    assert result["effective"]["step_fields"]["pipeline"] == "real-child"
    assert result["effective"]["promoted_fields"]["pipeline"]["source"] == "config.pipeline"
    assert result["control_flow"]["should_execute"] is True


@pytest.mark.asyncio
async def test_inspect_effective_pipeline_returns_nested_steps(tmp_path, monkeypatch):
    from brix import mcp_server
    from brix.mcp_handlers import _shared

    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)
    monkeypatch.setattr(_shared, "PIPELINE_DIR", tmp_path)

    handlers = _handlers()
    await handlers["create_pipeline"](
        {
            "name": "materialize-all",
            "steps": [
                {
                    "id": "chooser",
                    "type": "flow.choose",
                    "choices": [
                        {
                            "when": "{{ input.use_branch }}",
                            "steps": [
                                {"id": "branch-step", "type": "pipeline", "pipeline": "child-one"},
                            ],
                        }
                    ],
                }
            ],
        }
    )

    result = await handlers["inspect_effective_pipeline"]({"pipeline_id": "materialize-all"})

    assert result["success"] is True
    assert result["step_count"] == 2
    branch = next(step for step in result["steps"] if step["step_id"] == "branch-step")
    assert branch["effective"]["type"] == "flow.pipeline"
    assert branch["effective"]["dependency_refs"]["pipeline"] == "child-one"


@pytest.mark.asyncio
async def test_materialize_step_includes_control_flow_preview(tmp_path, monkeypatch):
    from brix import mcp_server
    from brix.mcp_handlers import _shared

    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)
    monkeypatch.setattr(_shared, "PIPELINE_DIR", tmp_path)

    handlers = _handlers()
    await handlers["create_pipeline"]({"name": "materialize-flow", "steps": []})
    await handlers["add_step"](
        {
            "pipeline_name": "materialize-flow",
            "step_id": "conditional",
            "type": "flow.set",
            "when": "{{ input.enabled }}",
            "foreach": "{{ input.items }}",
            "params": {"value": "x"},
        }
    )

    result = await handlers["materialize_step"](
        {
            "pipeline_id": "materialize-flow",
            "step_id": "conditional",
            "input": {"enabled": True, "items": ["a", "b"]},
        }
    )

    assert result["success"] is True
    assert result["control_flow"]["rendered_when"] is True
    assert result["control_flow"]["rendered_foreach"] == ["a", "b"]
    assert result["control_flow"]["should_execute"] is True
