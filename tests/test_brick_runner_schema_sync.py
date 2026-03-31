"""Tests for T-BRIX-BUG-13: Runner config_schema fallback in get_brick_schema.

Verifies that get_brick_schema returns non-empty config_schema for system
bricks that delegate to runners with config_schema() methods, even when the
brick definition itself has an empty config_schema.
"""

import pytest

from brix.mcp_handlers.steps import _get_runner_config_schema, _handle_get_brick_schema


# ---------------------------------------------------------------------------
# Unit tests for _get_runner_config_schema helper
# ---------------------------------------------------------------------------


class TestGetRunnerConfigSchema:
    """Tests for the runner config schema fallback helper."""

    @pytest.mark.parametrize(
        "runner_name",
        ["llm_batch", "python", "cli", "db_query", "http", "filter", "transform",
         "validate", "specialist", "db_upsert", "markitdown", "aggregate"],
    )
    def test_known_runners_return_schema(self, runner_name):
        schema = _get_runner_config_schema(runner_name)
        assert schema is not None, f"Runner '{runner_name}' should return a config schema"
        assert isinstance(schema, dict)
        assert "properties" in schema or "type" in schema

    def test_unknown_runner_returns_none(self):
        schema = _get_runner_config_schema("nonexistent_runner_xyz")
        assert schema is None

    def test_schema_has_properties(self):
        schema = _get_runner_config_schema("llm_batch")
        assert schema is not None
        assert schema.get("type") == "object"
        props = schema.get("properties", {})
        assert "model" in props
        assert "system_prompt" in props
        assert "user_template" in props

    def test_http_runner_schema(self):
        schema = _get_runner_config_schema("http")
        assert schema is not None
        props = schema.get("properties", {})
        assert "url" in props
        assert "method" in props

    def test_python_runner_schema(self):
        schema = _get_runner_config_schema("python")
        assert schema is not None
        props = schema.get("properties", {})
        assert "script" in props

    def test_db_query_runner_schema(self):
        schema = _get_runner_config_schema("db_query")
        assert schema is not None
        props = schema.get("properties", {})
        assert "query" in props
        assert "connection" in props


# ---------------------------------------------------------------------------
# Integration tests for _handle_get_brick_schema
# ---------------------------------------------------------------------------


class TestHandleGetBrickSchemaFallback:
    """Tests that get_brick_schema returns runner schemas for system bricks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "brick_name,expected_props",
        [
            ("llm.batch", ["model", "system_prompt", "user_template"]),
            ("script.python", ["script"]),
            ("script.cli", ["args"]),
            ("http.request", ["url"]),
        ],
    )
    async def test_system_bricks_have_config_schema(self, brick_name, expected_props):
        result = await _handle_get_brick_schema({"brick_name": brick_name})
        assert result.get("name") == brick_name, f"Brick '{brick_name}' not found"
        config = result.get("config_schema", {})
        props = config.get("properties", {})
        for prop in expected_props:
            assert prop in props, (
                f"Brick '{brick_name}' config_schema should have property '{prop}', "
                f"got properties: {list(props.keys())}"
            )

    @pytest.mark.asyncio
    async def test_brick_with_own_schema_not_overridden(self):
        """Bricks that already have a config_schema should not be overridden."""
        # db.query has its own config_schema in builtins.py
        result = await _handle_get_brick_schema({"brick_name": "db.query"})
        assert result.get("name") == "db.query"
        config = result.get("config_schema", {})
        props = config.get("properties", {})
        # Should have properties from its own definition
        assert len(props) > 0

    @pytest.mark.asyncio
    async def test_nonexistent_brick_returns_error(self):
        result = await _handle_get_brick_schema({"brick_name": "nonexistent.brick"})
        assert result.get("success") is False
        assert "not found" in result.get("error", "").lower()
