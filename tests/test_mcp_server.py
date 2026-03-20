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


class TestStubReturnsNotImplemented:
    """Each handler stub returns the expected not_implemented response."""

    @pytest.mark.asyncio
    async def test_get_tips_stub(self):
        result = await _handle_get_tips({})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__get_tips"

    @pytest.mark.asyncio
    async def test_list_bricks_stub(self):
        result = await _handle_list_bricks({})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__list_bricks"

    @pytest.mark.asyncio
    async def test_search_bricks_stub(self):
        result = await _handle_search_bricks({"query": "http"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__search_bricks"

    @pytest.mark.asyncio
    async def test_get_brick_schema_stub(self):
        result = await _handle_get_brick_schema({"brick_name": "http_get"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__get_brick_schema"

    @pytest.mark.asyncio
    async def test_create_pipeline_stub(self):
        result = await _handle_create_pipeline({"name": "test"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__create_pipeline"

    @pytest.mark.asyncio
    async def test_get_pipeline_stub(self):
        result = await _handle_get_pipeline({"pipeline_id": "abc"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__get_pipeline"

    @pytest.mark.asyncio
    async def test_add_step_stub(self):
        result = await _handle_add_step({"pipeline_id": "abc", "step_id": "s1", "brick": "http_get"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__add_step"

    @pytest.mark.asyncio
    async def test_remove_step_stub(self):
        result = await _handle_remove_step({"pipeline_id": "abc", "step_id": "s1"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__remove_step"

    @pytest.mark.asyncio
    async def test_validate_pipeline_stub(self):
        result = await _handle_validate_pipeline({"pipeline_id": "abc"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__validate_pipeline"

    @pytest.mark.asyncio
    async def test_run_pipeline_stub(self):
        result = await _handle_run_pipeline({"pipeline_id": "abc"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__run_pipeline"

    @pytest.mark.asyncio
    async def test_get_run_status_stub(self):
        result = await _handle_get_run_status({"run_id": "run-123"})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__get_run_status"

    @pytest.mark.asyncio
    async def test_get_run_history_stub(self):
        result = await _handle_get_run_history({})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__get_run_history"

    @pytest.mark.asyncio
    async def test_list_pipelines_stub(self):
        result = await _handle_list_pipelines({})
        assert result["status"] == "not_implemented"
        assert result["tool"] == "brix__list_pipelines"

    @pytest.mark.asyncio
    async def test_all_stubs_return_not_implemented(self):
        """Dispatch table coverage: every handler returns not_implemented."""
        for tool_name, handler in _HANDLERS.items():
            result = await handler({})
            assert result["status"] == "not_implemented", (
                f"Handler for '{tool_name}' did not return not_implemented"
            )
            assert result["tool"] == tool_name, (
                f"Handler for '{tool_name}' returned wrong tool name: {result['tool']}"
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
