"""Tests for config → params mapping in create_pipeline and add_step (T-BRIX-BUG-11).

When MCP callers pass 'config' on a step dict (meaning generic params), the
YAML must store the value under 'params' so the Engine can read it.  The only
exception is the specialist runner where 'config' is a legitimate field
(ExtractionRules).
"""
import pytest
import yaml

from brix.mcp_server import (
    _handle_create_pipeline,
    _handle_add_step,
)
from brix.mcp_handlers._shared import (
    _normalize_step_config,
    _normalize_steps,
    _pipeline_dir,
)
from brix.pipeline_store import PipelineStore


# ---------------------------------------------------------------------------
# Unit tests for the normalize helpers
# ---------------------------------------------------------------------------

class TestNormalizeStepConfig:
    """Unit tests for _normalize_step_config."""

    def test_config_mapped_to_params(self):
        step = {"id": "s1", "type": "http.request", "config": {"url": "http://x"}}
        result = _normalize_step_config(step)
        assert "params" in result
        assert result["params"] == {"url": "http://x"}
        assert "config" not in result

    def test_params_preserved_when_both(self):
        """If both config and params exist, params wins and config stays."""
        step = {
            "id": "s1",
            "type": "flow.transform",
            "params": {"a": 1},
            "config": {"b": 2},
        }
        result = _normalize_step_config(step)
        assert result["params"] == {"a": 1}
        assert result["config"] == {"b": 2}

    def test_specialist_keeps_config(self):
        """Specialist steps keep 'config' as-is (it's ExtractionRules)."""
        step = {
            "id": "s1",
            "type": "extract.specialist",
            "config": {"input_field": "text", "extract": []},
        }
        result = _normalize_step_config(step)
        assert "config" in result
        assert "params" not in result

    def test_specialist_legacy_type_keeps_config(self):
        step = {
            "id": "s1",
            "type": "specialist",
            "config": {"input_field": "text", "extract": []},
        }
        result = _normalize_step_config(step)
        assert "config" in result
        assert "params" not in result

    def test_params_only_unchanged(self):
        step = {"id": "s1", "type": "mcp.call", "params": {"tool": "x"}}
        result = _normalize_step_config(step)
        assert result["params"] == {"tool": "x"}
        assert "config" not in result

    def test_no_config_no_params(self):
        step = {"id": "s1", "type": "flow.set", "values": {"x": 1}}
        result = _normalize_step_config(step)
        assert "params" not in result
        assert "config" not in result

    def test_non_dict_passthrough(self):
        assert _normalize_step_config("not a dict") == "not a dict"


class TestNormalizeSteps:
    """Unit tests for _normalize_steps (list version)."""

    def test_normalizes_all_steps(self):
        steps = [
            {"id": "a", "type": "http.request", "config": {"url": "http://a"}},
            {"id": "b", "type": "flow.filter", "config": {"expr": "true"}},
            {"id": "c", "type": "flow.set", "params": {"x": 1}},
        ]
        _normalize_steps(steps)
        assert steps[0]["params"] == {"url": "http://a"}
        assert "config" not in steps[0]
        assert steps[1]["params"] == {"expr": "true"}
        assert "config" not in steps[1]
        assert steps[2]["params"] == {"x": 1}


# ---------------------------------------------------------------------------
# Integration tests via MCP handlers
# ---------------------------------------------------------------------------

class TestCreatePipelineConfigMapping:
    """create_pipeline must store 'config' as 'params' in YAML."""

    @pytest.mark.asyncio
    async def test_config_stored_as_params(self, tmp_path, monkeypatch):
        """Steps passed with 'config' end up as 'params' in saved YAML."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        steps = [
            {
                "id": "fetch",
                "type": "http.request",
                "config": {"url": "https://example.com", "method": "GET"},
            }
        ]
        result = await _handle_create_pipeline({"name": "test-config-map", "steps": steps})
        assert result["success"] is True

        # Read the saved YAML and verify
        store = PipelineStore(pipelines_dir=tmp_path)
        raw = store.load_raw("test-config-map")
        saved_step = raw["steps"][0]
        assert saved_step.get("params") == {"url": "https://example.com", "method": "GET"}
        assert saved_step.get("config") in (None, {})

    @pytest.mark.asyncio
    async def test_params_still_works(self, tmp_path, monkeypatch):
        """Steps passed with 'params' continue to work as before."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        steps = [
            {
                "id": "fetch",
                "type": "http.request",
                "params": {"url": "https://example.com"},
            }
        ]
        result = await _handle_create_pipeline({"name": "test-params-ok", "steps": steps})
        assert result["success"] is True

        store = PipelineStore(pipelines_dir=tmp_path)
        raw = store.load_raw("test-params-ok")
        saved_step = raw["steps"][0]
        assert saved_step.get("params") == {"url": "https://example.com"}
        assert saved_step.get("config") in (None, {})

    @pytest.mark.asyncio
    async def test_specialist_config_preserved(self, tmp_path, monkeypatch):
        """Specialist steps keep 'config' (not remapped to params)."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        specialist_config = {
            "input_field": "text",
            "extract": [{"name": "amount", "method": "regex", "pattern": r"\d+"}],
        }
        steps = [
            {
                "id": "extract",
                "type": "extract.specialist",
                "config": specialist_config,
            }
        ]
        result = await _handle_create_pipeline({"name": "test-specialist", "steps": steps})
        assert result["success"] is True

        store = PipelineStore(pipelines_dir=tmp_path)
        raw = store.load_raw("test-specialist")
        saved_step = raw["steps"][0]
        assert saved_step.get("config") == specialist_config
        assert saved_step.get("params") in (None, {})


class TestAddStepConfigMapping:
    """add_step must normalize 'config' → 'params' for non-specialist steps."""

    @pytest.mark.asyncio
    async def test_add_step_with_config(self, tmp_path, monkeypatch):
        """Adding a step via add_step with 'config' stores 'params'."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)

        # Create a base pipeline first
        await _handle_create_pipeline({
            "name": "test-add-step",
            "steps": [{"id": "init", "type": "flow.set", "values": {"x": 1}}],
        })

        # Add a step using 'config' instead of 'params'
        result = await _handle_add_step({
            "pipeline_name": "test-add-step",
            "step_id": "call_api",
            "type": "http.request",
            "config": {"url": "https://api.example.com"},
        })
        assert result["success"] is True

        store = PipelineStore(pipelines_dir=tmp_path)
        raw = store.load_raw("test-add-step")
        added_step = next(s for s in raw["steps"] if s["id"] == "call_api")
        assert added_step.get("params") == {"url": "https://api.example.com"}
        assert added_step.get("config") in (None, {})
