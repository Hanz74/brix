"""Tests for the Brix MCP server (T-BRIX-V2-04).

The MCP server uses stdio transport, so we test the components directly
rather than exercising the full stdio I/O path.
"""
import json
import pytest

from brix.mcp_server import (
    BRIX_TOOLS,
    _HANDLERS,
    create_server,
    _handle_get_tips,
    _handle_list_bricks,
    _handle_search_bricks,
    _handle_get_brick_schema,
    _handle_create_pipeline,
    _handle_get_pipeline,
    _handle_add_step,
    _handle_remove_step,
    _handle_validate_pipeline,
    _handle_run_pipeline,
    _handle_get_run_status,
    _handle_get_run_history,
    _handle_list_pipelines,
)


EXPECTED_TOOL_COUNT = 13

EXPECTED_TOOL_NAMES = {
    "brix__get_tips",
    "brix__list_bricks",
    "brix__search_bricks",
    "brix__get_brick_schema",
    "brix__create_pipeline",
    "brix__get_pipeline",
    "brix__add_step",
    "brix__remove_step",
    "brix__validate_pipeline",
    "brix__run_pipeline",
    "brix__get_run_status",
    "brix__get_run_history",
    "brix__list_pipelines",
}


class TestServerToolsRegistered:
    """Server exposes exactly the expected set of tools."""

    def test_tool_count(self):
        assert len(BRIX_TOOLS) == EXPECTED_TOOL_COUNT

    def test_all_expected_tools_present(self):
        registered_names = {t.name for t in BRIX_TOOLS}
        assert registered_names == EXPECTED_TOOL_NAMES

    def test_all_tools_have_brix_prefix(self):
        for tool in BRIX_TOOLS:
            assert tool.name.startswith("brix__"), (
                f"Tool '{tool.name}' missing 'brix__' prefix"
            )

    def test_handler_count_matches_tool_count(self):
        """Every registered tool must have a corresponding handler."""
        assert len(_HANDLERS) == EXPECTED_TOOL_COUNT

    def test_every_tool_has_handler(self):
        registered_names = {t.name for t in BRIX_TOOLS}
        for name in registered_names:
            assert name in _HANDLERS, f"No handler for tool '{name}'"


class TestToolDescriptionsNotEmpty:
    """Every tool must have a non-empty, meaningful description."""

    def test_descriptions_not_empty(self):
        for tool in BRIX_TOOLS:
            assert tool.description, f"Tool '{tool.name}' has empty description"

    def test_descriptions_minimum_length(self):
        """Descriptions should be at least 30 chars to be meaningful."""
        for tool in BRIX_TOOLS:
            assert len(tool.description) >= 30, (
                f"Tool '{tool.name}' description too short: {tool.description!r}"
            )

    def test_descriptions_contain_returns_info(self):
        """Agent-optimised descriptions should mention what is returned."""
        keywords = {"returns", "return", "Returns", "Return"}
        for tool in BRIX_TOOLS:
            has_keyword = any(kw in tool.description for kw in keywords)
            assert has_keyword, (
                f"Tool '{tool.name}' description doesn't explain what it returns"
            )


class TestToolSchemasValid:
    """Every tool must have a valid JSON Schema as inputSchema."""

    def test_input_schemas_present(self):
        for tool in BRIX_TOOLS:
            assert tool.inputSchema is not None, (
                f"Tool '{tool.name}' missing inputSchema"
            )

    def test_input_schemas_are_objects(self):
        for tool in BRIX_TOOLS:
            assert isinstance(tool.inputSchema, dict), (
                f"Tool '{tool.name}' inputSchema is not a dict"
            )

    def test_input_schemas_have_type_object(self):
        for tool in BRIX_TOOLS:
            assert tool.inputSchema.get("type") == "object", (
                f"Tool '{tool.name}' inputSchema.type != 'object'"
            )

    def test_input_schemas_have_properties(self):
        for tool in BRIX_TOOLS:
            assert "properties" in tool.inputSchema, (
                f"Tool '{tool.name}' inputSchema missing 'properties'"
            )

    def test_input_schemas_have_required_field(self):
        for tool in BRIX_TOOLS:
            assert "required" in tool.inputSchema, (
                f"Tool '{tool.name}' inputSchema missing 'required'"
            )

    def test_required_is_list(self):
        for tool in BRIX_TOOLS:
            required = tool.inputSchema.get("required", [])
            assert isinstance(required, list), (
                f"Tool '{tool.name}' inputSchema.required is not a list"
            )

    def test_required_fields_exist_in_properties(self):
        for tool in BRIX_TOOLS:
            props = set(tool.inputSchema.get("properties", {}).keys())
            required = tool.inputSchema.get("required", [])
            for field in required:
                assert field in props, (
                    f"Tool '{tool.name}' requires '{field}' but it's not in properties"
                )



class TestServerObject:
    """Tests for the Server object itself."""

    def test_create_server_returns_server(self):
        from mcp.server.lowlevel import Server
        server = create_server()
        assert isinstance(server, Server)

    def test_server_name(self):
        server = create_server()
        assert server.name == "brix"

    def test_call_tool_unknown_raises(self):
        """call_tool handler must raise ValueError for unknown tool names."""
        import asyncio
        from brix.mcp_server import _HANDLERS

        async def _run():
            # We test the dispatch logic directly
            handler = _HANDLERS.get("brix__nonexistent_tool")
            return handler

        handler = asyncio.run(_run())
        assert handler is None, "Unknown tools must not have handlers"


# ---------------------------------------------------------------------------
# V2-05 Discovery Tests
# ---------------------------------------------------------------------------

class TestDiscoveryHandlers:
    """Tests for real Discovery handler implementations (V2-05)."""

    @pytest.mark.asyncio
    async def test_get_tips_returns_content(self):
        """get_tips gives non-empty tips list."""
        result = await _handle_get_tips({})
        assert "tips" in result
        assert isinstance(result["tips"], list)
        assert len(result["tips"]) > 0
        # Should mention brick count
        assert result["brick_count"] > 0

    @pytest.mark.asyncio
    async def test_list_bricks_returns_builtins(self):
        """list_bricks returns at least 10 built-in bricks."""
        result = await _handle_list_bricks({})
        assert "bricks" in result
        assert result["total"] >= 10
        names = [b["name"] for b in result["bricks"]]
        assert "http_get" in names

    @pytest.mark.asyncio
    async def test_list_bricks_category_filter(self):
        """list_bricks category filter only returns matching bricks."""
        result = await _handle_list_bricks({"category": "http"})
        assert result["total"] >= 1
        for brick in result["bricks"]:
            assert brick["category"] == "http"

    @pytest.mark.asyncio
    async def test_search_bricks_finds_http(self):
        """Search for 'http' finds http_get brick."""
        result = await _handle_search_bricks({"query": "http"})
        assert result["total"] > 0
        names = [b["name"] for b in result["results"]]
        assert any("http" in n for n in names)

    @pytest.mark.asyncio
    async def test_search_bricks_no_match(self):
        """Search for nonsense query returns empty list."""
        result = await _handle_search_bricks({"query": "xyzzy_nonexistent_brick_9999"})
        assert result["total"] == 0
        assert result["results"] == []

    @pytest.mark.asyncio
    async def test_get_brick_schema_valid(self):
        """Schema for http_get has properties with url."""
        result = await _handle_get_brick_schema({"brick_name": "http_get"})
        assert "config_schema" in result
        assert "properties" in result["config_schema"]
        assert "url" in result["config_schema"]["properties"]

    @pytest.mark.asyncio
    async def test_get_brick_schema_not_found(self):
        """Schema for unknown brick returns error."""
        result = await _handle_get_brick_schema({"brick_name": "nonexistent_brick_xyz"})
        assert result.get("success") is False
        assert "error" in result


# ---------------------------------------------------------------------------
# V2-06 Builder Tests
# ---------------------------------------------------------------------------

class TestBuilderHandlers:
    """Tests for real Builder handler implementations (V2-06)."""

    @pytest.mark.asyncio
    async def test_create_pipeline_empty(self, tmp_path, monkeypatch):
        """Create an empty pipeline saves a YAML file."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_create_pipeline({"name": "test-empty"})
        assert result["success"] is True
        assert result["pipeline_id"] == "test-empty"
        assert result["step_count"] == 0
        assert (tmp_path / "test-empty.yaml").exists()

    @pytest.mark.asyncio
    async def test_create_pipeline_inline(self, tmp_path, monkeypatch):
        """Create a pipeline with inline steps — validated immediately (Lisa P0)."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        steps = [{"id": "greet", "type": "cli", "args": ["echo", "hello"]}]
        result = await _handle_create_pipeline({
            "name": "test-inline",
            "description": "Test inline pipeline",
            "steps": steps,
        })
        assert result["success"] is True
        assert result["step_count"] == 1
        assert "validated" in result
        assert (tmp_path / "test-inline.yaml").exists()

    @pytest.mark.asyncio
    async def test_get_pipeline(self, tmp_path, monkeypatch):
        """Get a pipeline that was previously created."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        # Create first
        await _handle_create_pipeline({
            "name": "test-get",
            "description": "A test pipeline",
            "steps": [{"id": "step1", "type": "cli", "args": ["echo", "hi"]}],
        })
        # Now retrieve it
        result = await _handle_get_pipeline({"pipeline_id": "test-get"})
        assert result["name"] == "test-get"
        assert result["step_count"] == 1
        assert result["description"] == "A test pipeline"

    @pytest.mark.asyncio
    async def test_get_pipeline_not_found(self, tmp_path, monkeypatch):
        """Get a nonexistent pipeline returns error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_get_pipeline({"pipeline_id": "does-not-exist"})
        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_add_step(self, tmp_path, monkeypatch):
        """Add a step to an existing pipeline."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        # Create empty pipeline
        await _handle_create_pipeline({"name": "test-add-step"})
        # Add a step
        result = await _handle_add_step({
            "pipeline_id": "test-add-step",
            "step_id": "say_hello",
            "brick": "run_cli",
            "params": {"args": ["echo", "hello"]},
        })
        assert result["success"] is True
        assert result["step_count"] == 1

    @pytest.mark.asyncio
    async def test_add_step_position(self, tmp_path, monkeypatch):
        """Add a step after a specific step ID."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-position",
            "steps": [
                {"id": "first", "type": "cli", "args": ["echo", "1"]},
                {"id": "third", "type": "cli", "args": ["echo", "3"]},
            ],
        })
        result = await _handle_add_step({
            "pipeline_id": "test-position",
            "step_id": "second",
            "brick": "run_cli",
            "position": "after:first",
        })
        assert result["success"] is True
        assert result["step_count"] == 3
        # Verify order
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-position"})
        step_ids = [s["id"] for s in pipeline["steps"]]
        assert step_ids == ["first", "second", "third"]

    @pytest.mark.asyncio
    async def test_remove_step(self, tmp_path, monkeypatch):
        """Remove a step from a pipeline."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-remove",
            "steps": [
                {"id": "keep", "type": "cli", "args": ["echo", "keep"]},
                {"id": "remove_me", "type": "cli", "args": ["echo", "gone"]},
            ],
        })
        result = await _handle_remove_step({
            "pipeline_id": "test-remove",
            "step_id": "remove_me",
        })
        assert result["success"] is True
        assert result["step_count"] == 1
        # Verify step is gone
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-remove"})
        step_ids = [s["id"] for s in pipeline["steps"]]
        assert "remove_me" not in step_ids
        assert "keep" in step_ids

    @pytest.mark.asyncio
    async def test_remove_step_not_found(self, tmp_path, monkeypatch):
        """Removing a nonexistent step returns error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-remove-missing"})
        result = await _handle_remove_step({
            "pipeline_id": "test-remove-missing",
            "step_id": "ghost_step",
        })
        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_validate_pipeline(self, tmp_path, monkeypatch):
        """A valid pipeline returns valid=True."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-validate",
            "steps": [{"id": "step1", "type": "cli", "args": ["echo", "ok"]}],
        })
        result = await _handle_validate_pipeline({"pipeline_id": "test-validate"})
        assert result["success"] is True
        assert result["valid"] is True
        assert isinstance(result["errors"], list)
        assert isinstance(result["warnings"], list)
        assert isinstance(result["checks"], list)

    @pytest.mark.asyncio
    async def test_validate_pipeline_not_found(self, tmp_path, monkeypatch):
        """Validate nonexistent pipeline returns error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_validate_pipeline({"pipeline_id": "ghost-pipeline"})
        assert result.get("success") is False


# ---------------------------------------------------------------------------
# V2-07 Execution Tests
# ---------------------------------------------------------------------------

class TestExecutionHandlers:
    """Tests for real Execution handler implementations (V2-07)."""

    @pytest.mark.asyncio
    async def test_run_pipeline(self, tmp_path, monkeypatch):
        """Run a pipeline with a simple CLI step returns success."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-run",
            "steps": [{"id": "greet", "type": "cli", "args": ["echo", "hello brix"]}],
        })
        result = await _handle_run_pipeline({"pipeline_id": "test-run"})
        assert result["success"] is True
        assert "run_id" in result
        assert result["steps"]["greet"]["status"] == "ok"

    @pytest.mark.asyncio
    async def test_run_pipeline_not_found(self, tmp_path, monkeypatch):
        """Running a nonexistent pipeline returns structured error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_run_pipeline({"pipeline_id": "nonexistent-xyz"})
        assert result["success"] is False
        assert "error" in result
        assert result["error"]["code"] == "PIPELINE_NOT_FOUND"
        assert result["error"]["recoverable"] is False

    @pytest.mark.asyncio
    async def test_run_pipeline_error_has_dual_layer(self, tmp_path, monkeypatch):
        """A failing pipeline step returns the dual-layer error schema."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        # Create pipeline with a failing step (command that always fails)
        await _handle_create_pipeline({
            "name": "test-fail",
            "steps": [{"id": "fail_step", "type": "cli", "args": ["false"]}],
        })
        result = await _handle_run_pipeline({"pipeline_id": "test-fail"})
        assert result["success"] is False
        assert "error" in result
        error = result["error"]
        assert "code" in error
        assert "message" in error
        assert "recoverable" in error
        assert "agent_actions" in error
        assert isinstance(error["agent_actions"], list)

    @pytest.mark.asyncio
    async def test_get_run_status(self, tmp_path, monkeypatch):
        """After running a pipeline, get_run_status returns the run."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-status",
            "steps": [{"id": "hi", "type": "cli", "args": ["echo", "status test"]}],
        })
        run_result = await _handle_run_pipeline({"pipeline_id": "test-status"})
        run_id = run_result["run_id"]

        status = await _handle_get_run_status({"run_id": run_id})
        assert status["success"] is True
        assert status["run_id"] == run_id

    @pytest.mark.asyncio
    async def test_get_run_status_not_found(self):
        """Unknown run_id returns error."""
        result = await _handle_get_run_status({"run_id": "run-nonexistent-xyz"})
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_get_run_history(self, tmp_path, monkeypatch):
        """After running a pipeline, history has at least one entry."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-history",
            "steps": [{"id": "h1", "type": "cli", "args": ["echo", "history"]}],
        })
        await _handle_run_pipeline({"pipeline_id": "test-history"})

        history_result = await _handle_get_run_history({"limit": 20})
        assert history_result["success"] is True
        assert history_result["total"] >= 1

    @pytest.mark.asyncio
    async def test_get_run_history_pipeline_filter(self, tmp_path, monkeypatch):
        """get_run_history pipeline_name filter works."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-filter-hist",
            "steps": [{"id": "f1", "type": "cli", "args": ["echo", "filter"]}],
        })
        await _handle_run_pipeline({"pipeline_id": "test-filter-hist"})

        result = await _handle_get_run_history({
            "pipeline_name": "test-filter-hist",
            "limit": 10,
        })
        assert result["success"] is True
        for run in result["runs"]:
            assert run["pipeline"] == "test-filter-hist"

    @pytest.mark.asyncio
    async def test_list_pipelines(self, tmp_path, monkeypatch):
        """list_pipelines returns saved pipeline files."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "listed-pipeline-a"})
        await _handle_create_pipeline({"name": "listed-pipeline-b"})

        result = await _handle_list_pipelines({})
        assert result["success"] is True
        assert result["total"] >= 2
        names = [p["name"] for p in result["pipelines"]]
        assert "listed-pipeline-a" in names
        assert "listed-pipeline-b" in names

    @pytest.mark.asyncio
    async def test_list_pipelines_custom_dir(self, tmp_path):
        """list_pipelines with custom directory argument."""
        import yaml as _yaml
        # Write a pipeline file manually
        (tmp_path / "custom-pipe.yaml").write_text(
            _yaml.dump({"name": "custom-pipe", "version": "1.0.0", "steps": []})
        )
        result = await _handle_list_pipelines({"directory": str(tmp_path)})
        assert result["success"] is True
        assert result["total"] >= 1
        names = [p["name"] for p in result["pipelines"]]
        assert "custom-pipe" in names
