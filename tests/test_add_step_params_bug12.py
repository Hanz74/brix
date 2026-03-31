"""Tests for add_step storing params as top-level attributes (T-BRIX-BUG-12).

When MCP callers pass 'params' to add_step, known Step model fields
(helper, foreach, on_error, etc.) must be promoted to top-level step
attributes in the YAML. Only unknown keys remain nested under 'params'.

Additionally, when 'params' arrives as a JSON string it must be parsed
into a dict before processing.
"""
import json
import pytest

from brix.mcp_server import (
    _handle_create_pipeline,
    _handle_add_step,
    _handle_get_pipeline,
    _handle_get_step,
)


@pytest.fixture(autouse=True)
def _setup_pipeline_dir(tmp_path, monkeypatch):
    """Point PIPELINE_DIR at a temp directory for all tests."""
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)


class TestAddStepParamsAsTopLevel:
    """T-BRIX-BUG-12: params dict entries that are Step model fields
    should be stored as top-level YAML attributes."""

    @pytest.mark.asyncio
    async def test_helper_promoted_from_params(self):
        """'helper' inside params becomes a top-level step attribute."""
        await _handle_create_pipeline({"name": "bug12-helper"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-helper",
            "step_id": "extract",
            "brick": "run_python",
            "params": {"helper": "my_helper", "custom_key": "custom_val"},
        })
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "bug12-helper"})
        step = pipeline["steps"][0]
        # helper should be top-level, not nested under params
        assert step.get("helper") == "my_helper"
        # custom_key is not a Step model field, so it stays in params
        assert step.get("params", {}).get("custom_key") == "custom_val"
        # helper should NOT be in params
        assert "helper" not in step.get("params", {})

    @pytest.mark.asyncio
    async def test_foreach_promoted_from_params(self):
        """'foreach' inside params becomes a top-level step attribute."""
        await _handle_create_pipeline({"name": "bug12-foreach"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-foreach",
            "step_id": "loop",
            "type": "script.python",
            "params": {"foreach": "{{ steps.prev.output }}", "script": "print('hi')"},
        })
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "bug12-foreach"})
        step = pipeline["steps"][0]
        assert step.get("foreach") == "{{ steps.prev.output }}"
        assert step.get("script") == "print('hi')"

    @pytest.mark.asyncio
    async def test_on_error_promoted_from_params(self):
        """'on_error' inside params becomes a top-level step attribute."""
        await _handle_create_pipeline({"name": "bug12-onerror"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-onerror",
            "step_id": "risky",
            "type": "script.python",
            "params": {"on_error": "continue", "script": "1/0"},
        })
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "bug12-onerror"})
        step = pipeline["steps"][0]
        assert step.get("on_error") == "continue"

    @pytest.mark.asyncio
    async def test_all_params_are_model_fields(self):
        """When all params keys are Step model fields, no 'params' key remains."""
        await _handle_create_pipeline({"name": "bug12-all-model"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-all-model",
            "step_id": "s1",
            "type": "script.python",
            "params": {"helper": "my_helper", "when": "{{ input.go }}"},
        })
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "bug12-all-model"})
        step = pipeline["steps"][0]
        assert step.get("helper") == "my_helper"
        assert step.get("when") == "{{ input.go }}"
        # No leftover params key (or empty)
        assert not step.get("params")


class TestAddStepParamsJsonString:
    """T-BRIX-BUG-12: params passed as a JSON string must be parsed."""

    @pytest.mark.asyncio
    async def test_params_json_string_parsed(self):
        """JSON string params are parsed and fields promoted."""
        await _handle_create_pipeline({"name": "bug12-json"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-json",
            "step_id": "s1",
            "type": "script.python",
            "params": '{"helper": "from_json", "extra": 42}',
        })
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "bug12-json"})
        step = pipeline["steps"][0]
        assert step.get("helper") == "from_json"
        assert step.get("params", {}).get("extra") == 42
        # params should NOT be a string
        assert not isinstance(step.get("params"), str)

    @pytest.mark.asyncio
    async def test_params_invalid_json_string(self):
        """Invalid JSON string params are stored as _raw fallback."""
        await _handle_create_pipeline({"name": "bug12-bad-json"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-bad-json",
            "step_id": "s1",
            "type": "script.python",
            "params": "not valid json {",
        })
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "bug12-bad-json"})
        step = pipeline["steps"][0]
        assert step.get("params", {}).get("_raw") == "not valid json {"


class TestAddStepNestedFields:
    """T-BRIX-BUG-12: nested flow-control fields work correctly via params."""

    @pytest.mark.asyncio
    async def test_foreach_and_concurrency(self):
        """foreach + concurrency in params are promoted correctly."""
        await _handle_create_pipeline({"name": "bug12-nested"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-nested",
            "step_id": "batch",
            "type": "llm.batch",
            "params": {
                "foreach": "{{ steps.data.output }}",
                "concurrency": 5,
                "flat_output": True,
            },
        })
        assert result["success"] is True

        step_result = await _handle_get_step({
            "pipeline_name": "bug12-nested",
            "step_id": "batch",
        })
        assert step_result["success"] is True
        step = step_result["step"]
        assert step.get("foreach") == "{{ steps.data.output }}"
        assert step.get("concurrency") == 5
        assert step.get("flat_output") is True
        # No leftover in params
        assert not step.get("params")

    @pytest.mark.asyncio
    async def test_mixed_model_and_custom_keys(self):
        """Mix of Step model fields and custom keys in params."""
        await _handle_create_pipeline({"name": "bug12-mixed"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-mixed",
            "step_id": "s1",
            "type": "http.request",
            "params": {
                "url": "https://api.example.com",
                "method": "POST",
                "headers": {"Authorization": "Bearer xxx"},
                "custom_setting": "value",
            },
        })
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "bug12-mixed"})
        step = pipeline["steps"][0]
        assert step.get("url") == "https://api.example.com"
        assert step.get("method") == "POST"
        assert step.get("headers") == {"Authorization": "Bearer xxx"}
        assert step.get("params", {}).get("custom_setting") == "value"

    @pytest.mark.asyncio
    async def test_direct_top_level_params_still_work(self):
        """Existing behaviour: params dict with only custom keys stays as params."""
        await _handle_create_pipeline({"name": "bug12-custom-only"})
        result = await _handle_add_step({
            "pipeline_id": "bug12-custom-only",
            "step_id": "s1",
            "brick": "run_cli",
            "params": {"args": ["echo", "hello"]},
        })
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "bug12-custom-only"})
        step = pipeline["steps"][0]
        # args IS a Step model field, so it should be promoted
        assert step.get("args") == ["echo", "hello"]
