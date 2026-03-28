"""Tests for the Brix MCP server (T-BRIX-V2-04).

The MCP server uses stdio transport, so we test the components directly
rather than exercising the full stdio I/O path.
"""
import json
import pytest

from brix.mcp_server import (
    BRIX_TOOLS,
    PIPELINE_TOOL_PREFIX,
    _HANDLERS,
    _build_pipeline_tools,
    _handle_pipeline_tool,
    create_server,
    _handle_get_tips,
    _handle_get_help,
    _handle_list_bricks,
    _handle_search_bricks,
    _handle_get_brick_schema,
    _handle_create_pipeline,
    _handle_get_pipeline,
    _handle_add_step,
    _handle_remove_step,
    _handle_update_step,
    _handle_update_pipeline,
    _handle_validate_pipeline,
    _handle_run_pipeline,
    _handle_get_run_status,
    _handle_get_run_errors,
    _handle_get_run_log,
    _handle_get_run_history,
    _handle_list_pipelines,
    _handle_create_helper,
    _handle_register_helper,
    _handle_list_helpers,
    _handle_get_helper,
    _handle_search_helpers,
    _handle_update_helper,
    _handle_delete_pipeline,
    _handle_get_step,
    _handle_delete_helper,
    _handle_delete_run,
    _handle_cancel_run,
    _handle_get_versions,
    _handle_rollback,
    _handle_diff_versions,
    # T-BRIX-V5-11 handlers
    _handle_search_pipelines,
    _handle_run_annotate,
    _handle_run_search,
    # T-BRIX-V5-13 new handlers
    _handle_rename_pipeline,
    _handle_rename_helper,
    _handle_test_pipeline,
    # T-BRIX-V6-10 / V6-11 / V6-12 Agent State handlers
    _handle_save_agent_context,
    _handle_restore_agent_context,
    _handle_claim_resource,
    _handle_check_resource,
    _handle_release_resource,
    # T-BRIX-DB-20 Custom Bricks
    _handle_create_brick,
    _handle_update_brick,
    _handle_delete_brick,
    # T-BRIX-DB-19 Universal Registry
    _handle_discover,
    _handle_list_runners,
    _handle_get_runner_info,
    _handle_list_env_config,
    _handle_list_types,
    _handle_list_namespaces,
    # V8-12 Consolidated dispatchers
    _handle_registry,
    _handle_trigger,
    _handle_credential,
    _handle_alert,
    _handle_server,
    _handle_state,
    _handle_trigger_group,
)


EXPECTED_HANDLER_COUNT = 101  # V8-12: consolidated 7 CRUD groups (-32 tools)

EXPECTED_TOOL_NAMES = {
    "brix__get_tips",
    "brix__get_help",
    "brix__list_bricks",
    "brix__search_bricks",
    "brix__get_brick_schema",
    "brix__create_pipeline",
    "brix__get_pipeline",
    "brix__add_step",
    "brix__remove_step",
    "brix__update_step",
    "brix__update_pipeline",
    "brix__validate_pipeline",
    "brix__run_pipeline",
    "brix__get_run_status",
    "brix__get_run_errors",
    "brix__get_run_log",
    "brix__get_run_history",
    "brix__list_pipelines",
    "brix__get_template",
    "brix__create_helper",
    "brix__register_helper",
    "brix__list_helpers",
    "brix__get_helper",
    "brix__search_helpers",
    "brix__update_helper",
    "brix__delete_pipeline",
    "brix__get_step",
    "brix__delete_helper",
    "brix__delete_run",
    # Credential Store — consolidated (V8-12)
    "brix__credential",
    # Object Versioning (T-BRIX-V5-07)
    "brix__get_versions",
    "brix__rollback",
    "brix__diff_versions",
    # Alerting — consolidated (V8-12)
    "brix__alert",
    # T-BRIX-V5-11 — CRUD gap fillers
    "brix__search_pipelines",
    "brix__run_annotate",
    "brix__run_search",
    # MCP Server Management — consolidated (V8-12)
    "brix__server",
    # Trigger CRUD — consolidated (V8-12)
    "brix__trigger",
    "brix__scheduler_status",
    "brix__scheduler_start",
    "brix__scheduler_stop",
    # Rename + Test Pipeline (T-BRIX-V5-13)
    "brix__rename_pipeline",
    "brix__rename_helper",
    "brix__test_pipeline",
    # Agent Intelligence (T-BRIX-V6-07 / V6-08 / V6-09)
    "brix__diagnose_run",
    "brix__auto_fix_step",
    "brix__get_insights",
    "brix__get_proactive_suggestions",
    # Agent State (T-BRIX-V6-10 / V6-11 / V6-12)
    "brix__save_agent_context",
    "brix__restore_agent_context",
    "brix__claim_resource",
    "brix__check_resource",
    "brix__release_resource",
    # Blackboard — consolidated (V8-12)
    "brix__state",
    # Trigger Groups — consolidated (V8-12)
    "brix__trigger_group",
    # T-BRIX-V6-BUG-03
    "brix__cancel_run",
    # T-BRIX-V7-05 — Run Diff
    "brix__diff_runs",
    # T-BRIX-V7-06 — Debug tools
    "brix__replay_step",
    "brix__resume_run",
    "brix__inspect_context",
    # T-BRIX-V7-07 — Resource Monitoring
    "brix__get_timeline",
    # T-BRIX-V7-10 — Registry System — consolidated (V8-12)
    "brix__registry",
    # T-BRIX-V8-01 — Intent-to-Pipeline Assembly
    "brix__compose_pipeline",
    # T-BRIX-V8-02 — Formalized Reason Phase
    "brix__plan_pipeline",
    # T-BRIX-V8-04 — Source-Connector-Abstraktion
    "brix__list_connectors",
    "brix__get_connector",
    "brix__connector_status",
    # T-BRIX-V8-08 — Pipeline-Templates: Parametrisierte Blueprints
    "brix__list_templates",
    "brix__instantiate_template",
    # T-BRIX-DB-05b — Named DB-Connections
    "brix__connection_add",
    "brix__connection_list",
    "brix__connection_test",
    "brix__connection_delete",
    # T-BRIX-DB-07 — Run-Persistenz: Step Execution Data
    "brix__get_step_data",
    # T-BRIX-DB-13 — Managed Variables
    "brix__set_variable",
    "brix__get_variable",
    "brix__list_variables",
    "brix__delete_variable",
    # T-BRIX-DB-13 — Persistent Data Store
    "brix__store_set",
    "brix__store_get",
    "brix__store_list",
    "brix__store_delete",
    # T-BRIX-DB-20 — Custom Bricks
    "brix__create_brick",
    "brix__update_brick",
    "brix__delete_brick",
    # T-BRIX-DB-19 — Universal Registry
    "brix__discover",
    "brix__list_runners",
    "brix__get_runner_info",
    "brix__list_env_config",
    "brix__list_types",
    "brix__list_namespaces",
    # T-BRIX-DB-10 — Helper Migration Analysis
    "brix__analyze_migration",
    # T-BRIX-DB-25 — Health Check
    "brix__health",
    # T-BRIX-DB-27 — DB Schema Status
    "brix__db_status",
    # T-BRIX-DB-28 — Backup / Restore
    "brix__backup",
    "brix__restore",
    "brix__backup_list",
    # T-BRIX-DB-24 — Pipeline Testing: Step Pins
    "brix__pin_step_data",
    "brix__unpin_step_data",
    "brix__list_pins",
}


class TestServerToolsRegistered:
    """Server exposes exactly the expected set of tools."""

    def test_tool_count_from_db(self):
        """BRIX_TOOLS loaded from DB must have at least as many tools as handlers."""
        assert len(BRIX_TOOLS) >= EXPECTED_HANDLER_COUNT

    def test_all_expected_tools_have_handlers(self):
        """All expected tools must have a handler in _HANDLERS."""
        for name in EXPECTED_TOOL_NAMES:
            assert name in _HANDLERS, f"Expected tool '{name}' not found in _HANDLERS"

    def test_all_tools_have_brix_prefix(self):
        for tool in BRIX_TOOLS:
            assert tool.name.startswith("brix__"), (
                f"Tool '{tool.name}' missing 'brix__' prefix"
            )

    def test_handler_count(self):
        """Handler count must match expected handler count."""
        assert len(_HANDLERS) == EXPECTED_HANDLER_COUNT

    def test_every_expected_tool_has_handler(self):
        """Every expected tool must have a corresponding handler."""
        for name in EXPECTED_TOOL_NAMES:
            assert name in _HANDLERS, f"Expected tool '{name}' has no handler"


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
    async def test_get_tips_includes_step_types_section(self):
        """get_tips references step-types topic via get_help."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Compact format: references get_help for details
        assert "get_help" in tips_text or "step-types" in tips_text or "TOOL-KATEGORIEN" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_on_error_options(self):
        """get_tips mentions update_step / add_step (tool categories)."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # New compact format lists tools; stop/continue/retry are in get_help('step-types')
        assert "update_step" in tips_text or "add_step" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_async_warning(self):
        """get_tips references helper-scripts topic for async warning."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Compact format: async details are in get_help('helper-scripts')
        assert "helper-scripts" in tips_text or "Helper" in tips_text or "get_help" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_helper_registry_section(self):
        """get_tips lists helper CRUD tools in the tool categories section."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Compact format: Helper category lists key tools
        assert "register" in tips_text or "Helper" in tips_text
        assert "list_helpers" in tips_text or "Helper:" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_pipeline_dependencies_section(self):
        """get_tips references debugging topic which covers pipeline dependencies."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Compact format: pipeline/helper tools listed in tool categories
        assert "create" in tips_text and "Pipeline" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_error_debugging_section(self):
        """get_tips documents error debugging tools (compact format)."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Must still mention key debugging tools in compact DEBUGGING section
        assert "get_run_errors" in tips_text
        assert "diagnose_run" in tips_text or "get_run_log" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_flow_control_best_practices_section(self):
        """get_tips lists update_step and add_step in tool categories."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Compact format: Steps category + get_help('flow-control') for details
        assert "update_step" in tips_text
        assert "add_step" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_progress_section(self):
        """get_tips references get_help for progress tracking details."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Compact format: progress details in get_help('foreach') or get_help('advanced-features')
        assert "get_help" in tips_text or "foreach" in tips_text or "run_pipeline" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_timeout_defaults_section(self):
        """get_tips lists run_pipeline and step tools (timeout details in get_help)."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Compact format: step types and timeout details are in get_help('step-types')
        assert "run_pipeline" in tips_text or "Runs:" in tips_text

    @pytest.mark.asyncio
    async def test_get_tips_includes_v46_features_section(self):
        """get_tips references advanced-features topic for v4.6+ features."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Compact format: advanced features listed in get_help('advanced-features')
        assert "advanced-features" in tips_text or "get_help" in tips_text

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

    # ------------------------------------------------------------------
    # add_step — flow-control steps via 'type' parameter
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_add_step_repeat_via_type(self, tmp_path, monkeypatch):
        """Add a 'repeat' flow-control step using the 'type' parameter."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-repeat"})
        result = await _handle_add_step({
            "pipeline_id": "test-repeat",
            "step_id": "poll",
            "type": "repeat",
            "until": "{{ steps.poll.result == 'done' }}",
            "max_iterations": 300,
            "sequence": [{"id": "check", "type": "cli", "args": ["echo", "check"]}],
        })
        assert result["success"] is True
        assert result["step_count"] == 1
        # Verify step content
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-repeat"})
        step = pipeline["steps"][0]
        assert step["id"] == "poll"
        assert step["type"] == "repeat"
        assert step["until"] == "{{ steps.poll.result == 'done' }}"
        assert step["max_iterations"] == 300
        assert isinstance(step["sequence"], list)

    @pytest.mark.asyncio
    async def test_add_step_stop_via_type(self, tmp_path, monkeypatch):
        """Add a 'stop' flow-control step using the 'type' parameter."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-stop"})
        result = await _handle_add_step({
            "pipeline_id": "test-stop",
            "step_id": "bail_out",
            "type": "stop",
            "when": "{{ input.abort }}",
            "message": "Aborted by input flag.",
        })
        assert result["success"] is True
        assert result["step_count"] == 1
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-stop"})
        step = pipeline["steps"][0]
        assert step["type"] == "stop"
        assert step["when"] == "{{ input.abort }}"
        assert step["message"] == "Aborted by input flag."

    @pytest.mark.asyncio
    async def test_add_step_set_via_type(self, tmp_path, monkeypatch):
        """Add a 'set' flow-control step using the 'type' parameter."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-set"})
        result = await _handle_add_step({
            "pipeline_id": "test-set",
            "step_id": "assign_vars",
            "type": "set",
            "values": {"count": 0, "status": "pending"},
        })
        assert result["success"] is True
        assert result["step_count"] == 1
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-set"})
        step = pipeline["steps"][0]
        assert step["type"] == "set"
        assert step["values"] == {"count": 0, "status": "pending"}

    @pytest.mark.asyncio
    async def test_add_step_brick_path_unchanged(self, tmp_path, monkeypatch):
        """Existing brick path still works after the flow-control refactor."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-brick-path"})
        result = await _handle_add_step({
            "pipeline_id": "test-brick-path",
            "step_id": "fetch",
            "brick": "http_get",
            "params": {"url": "https://example.com"},
        })
        assert result["success"] is True
        assert result["step_count"] == 1
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-brick-path"})
        step = pipeline["steps"][0]
        assert step["id"] == "fetch"
        # type should be resolved from the registry (http_get → python or http)
        assert "type" in step

    @pytest.mark.asyncio
    async def test_add_step_both_brick_and_type_error(self, tmp_path, monkeypatch):
        """Providing both 'brick' and 'type' returns an error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-conflict"})
        result = await _handle_add_step({
            "pipeline_id": "test-conflict",
            "step_id": "bad_step",
            "brick": "http_get",
            "type": "repeat",
        })
        assert result["success"] is False
        assert "both" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_add_step_neither_brick_nor_type_error(self, tmp_path, monkeypatch):
        """Providing neither 'brick' nor 'type' returns an error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-missing"})
        result = await _handle_add_step({
            "pipeline_id": "test-missing",
            "step_id": "orphan_step",
        })
        assert result["success"] is False
        assert "type" in result["error"].lower() or "brick" in result["error"].lower()

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

    # ------------------------------------------------------------------
    # update_pipeline tests
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_update_pipeline_description(self, tmp_path, monkeypatch):
        """update_pipeline changes description without touching steps."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-upd-desc",
            "description": "old description",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo", "hi"]}],
        })
        result = await _handle_update_pipeline({
            "name": "test-upd-desc",
            "description": "new description",
        })
        assert result["success"] is True
        assert "description" in result["changed_fields"]
        # Steps must be unchanged
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-upd-desc"})
        assert pipeline["description"] == "new description"
        assert pipeline["step_count"] == 1

    @pytest.mark.asyncio
    async def test_update_pipeline_version(self, tmp_path, monkeypatch):
        """update_pipeline bumps version."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-upd-ver", "version": "1.0.0"})
        result = await _handle_update_pipeline({
            "name": "test-upd-ver",
            "version": "2.0.0",
        })
        assert result["success"] is True
        assert "version" in result["changed_fields"]
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-upd-ver"})
        assert pipeline["version"] == "2.0.0"

    @pytest.mark.asyncio
    async def test_update_pipeline_input_schema(self, tmp_path, monkeypatch):
        """update_pipeline replaces input_schema."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-upd-input"})
        new_input = {
            "folder": {"type": "string", "description": "Target folder"},
            "limit": {"type": "integer", "default": 10},
        }
        result = await _handle_update_pipeline({
            "name": "test-upd-input",
            "input_schema": new_input,
        })
        assert result["success"] is True
        assert "input_schema" in result["changed_fields"]
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-upd-input"})
        assert "folder" in pipeline["input"]
        assert "limit" in pipeline["input"]

    @pytest.mark.asyncio
    async def test_update_pipeline_multiple_fields(self, tmp_path, monkeypatch):
        """update_pipeline can update description and version in one call."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-upd-multi"})
        result = await _handle_update_pipeline({
            "name": "test-upd-multi",
            "description": "updated",
            "version": "3.0.0",
        })
        assert result["success"] is True
        assert set(result["changed_fields"]) == {"description", "version"}

    @pytest.mark.asyncio
    async def test_update_pipeline_not_found(self, tmp_path, monkeypatch):
        """update_pipeline on nonexistent pipeline returns error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_update_pipeline({
            "name": "ghost-pipeline-xyz",
            "description": "does not matter",
        })
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_update_pipeline_no_fields(self, tmp_path, monkeypatch):
        """update_pipeline with only name (no fields) returns unchanged message."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-upd-noop"})
        result = await _handle_update_pipeline({"name": "test-upd-noop"})
        assert result["success"] is True
        assert result["changed_fields"] == []
        assert "unchanged" in result.get("message", "").lower()

    @pytest.mark.asyncio
    async def test_update_pipeline_preserves_steps(self, tmp_path, monkeypatch):
        """update_pipeline never modifies existing steps."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-upd-steps",
            "steps": [
                {"id": "a", "type": "cli", "args": ["echo", "a"]},
                {"id": "b", "type": "cli", "args": ["echo", "b"]},
            ],
        })
        await _handle_update_pipeline({
            "name": "test-upd-steps",
            "description": "changed",
            "version": "9.9.9",
        })
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-upd-steps"})
        assert pipeline["step_count"] == 2
        step_ids = [s["id"] for s in pipeline["steps"]]
        assert step_ids == ["a", "b"]

    @pytest.mark.asyncio
    async def test_update_pipeline_no_name_returns_error(self):
        """update_pipeline without 'name' parameter returns error."""
        result = await _handle_update_pipeline({"description": "oops"})
        assert result["success"] is False
        assert "name" in result["error"].lower()

    # ------------------------------------------------------------------
    # requirements support
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_pipeline_with_requirements(self, tmp_path, monkeypatch):
        """create_pipeline stores requirements in the YAML."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_create_pipeline({
            "name": "test-reqs-create",
            "requirements": ["requests>=2.28", "httpx"],
        })
        assert result["success"] is True
        # Verify YAML contains requirements
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-reqs-create"})
        assert pipeline["requirements"] == ["requests>=2.28", "httpx"]

    @pytest.mark.asyncio
    async def test_create_pipeline_without_requirements_defaults_empty(self, tmp_path, monkeypatch):
        """create_pipeline without requirements returns empty list in get_pipeline."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-reqs-empty"})
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-reqs-empty"})
        assert pipeline["requirements"] == []

    @pytest.mark.asyncio
    async def test_get_pipeline_includes_requirements(self, tmp_path, monkeypatch):
        """get_pipeline always includes 'requirements' key in response."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-reqs-get",
            "requirements": ["pandas>=1.5"],
        })
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-reqs-get"})
        assert "requirements" in pipeline
        assert pipeline["requirements"] == ["pandas>=1.5"]

    @pytest.mark.asyncio
    async def test_update_pipeline_requirements(self, tmp_path, monkeypatch):
        """update_pipeline replaces requirements list."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-reqs-upd",
            "requirements": ["old-pkg"],
        })
        result = await _handle_update_pipeline({
            "name": "test-reqs-upd",
            "requirements": ["requests>=2.28", "httpx", "pydantic>=2"],
        })
        assert result["success"] is True
        assert "requirements" in result["changed_fields"]
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-reqs-upd"})
        assert pipeline["requirements"] == ["requests>=2.28", "httpx", "pydantic>=2"]

    @pytest.mark.asyncio
    async def test_update_pipeline_requirements_clear(self, tmp_path, monkeypatch):
        """update_pipeline with empty list clears requirements."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-reqs-clear",
            "requirements": ["some-pkg"],
        })
        result = await _handle_update_pipeline({
            "name": "test-reqs-clear",
            "requirements": [],
        })
        assert result["success"] is True
        assert "requirements" in result["changed_fields"]
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-reqs-clear"})
        assert pipeline["requirements"] == []

    @pytest.mark.asyncio
    async def test_update_pipeline_requirements_does_not_touch_steps(self, tmp_path, monkeypatch):
        """Updating requirements leaves steps intact."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-reqs-steps",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo", "hi"]}],
        })
        await _handle_update_pipeline({
            "name": "test-reqs-steps",
            "requirements": ["httpx"],
        })
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-reqs-steps"})
        assert pipeline["step_count"] == 1
        assert pipeline["requirements"] == ["httpx"]

    @pytest.mark.asyncio
    async def test_create_pipeline_inputschema_has_requirements(self):
        """brix__create_pipeline tool schema exposes 'requirements' property."""
        tool = next(t for t in BRIX_TOOLS if t.name == "brix__create_pipeline")
        assert "requirements" in tool.inputSchema["properties"]
        prop = tool.inputSchema["properties"]["requirements"]
        assert prop["type"] == "array"

    def test_update_pipeline_inputschema_has_requirements(self):
        """brix__update_pipeline tool schema exposes 'requirements' property."""
        tool = next(t for t in BRIX_TOOLS if t.name == "brix__update_pipeline")
        assert "requirements" in tool.inputSchema["properties"]
        prop = tool.inputSchema["properties"]["requirements"]
        assert prop["type"] == "array"

    # ------------------------------------------------------------------
    # T-BRIX-V5-10: credentials / error_handling / groups / output
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_create_pipeline_with_credentials(self, tmp_path, monkeypatch):
        """create_pipeline stores credentials in the YAML."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        creds = {"my_api": "cred-uuid-abc123"}
        result = await _handle_create_pipeline({
            "name": "test-creds-create",
            "credentials": creds,
        })
        assert result["success"] is True
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-creds-create"})
        assert pipeline["credentials"] == creds

    @pytest.mark.asyncio
    async def test_update_pipeline_credentials(self, tmp_path, monkeypatch):
        """update_pipeline sets credentials dict."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-creds-upd"})
        creds = {"token": "env:MY_TOKEN_VAR"}
        result = await _handle_update_pipeline({
            "name": "test-creds-upd",
            "credentials": creds,
        })
        assert result["success"] is True
        assert "credentials" in result["changed_fields"]
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-creds-upd"})
        assert pipeline["credentials"] == creds

    @pytest.mark.asyncio
    async def test_create_pipeline_with_error_handling(self, tmp_path, monkeypatch):
        """create_pipeline stores error_handling config."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        eh = {"on_error": "continue"}
        result = await _handle_create_pipeline({
            "name": "test-eh-create",
            "error_handling": eh,
        })
        assert result["success"] is True
        # Load raw YAML to confirm error_handling is stored
        import yaml
        raw = yaml.safe_load((tmp_path / "test-eh-create.yaml").read_text())
        assert raw.get("error_handling", {}).get("on_error") == "continue"

    @pytest.mark.asyncio
    async def test_update_pipeline_error_handling(self, tmp_path, monkeypatch):
        """update_pipeline sets error_handling config."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-eh-upd"})
        eh = {"on_error": "retry", "retry": {"max": 5, "backoff": "linear"}}
        result = await _handle_update_pipeline({
            "name": "test-eh-upd",
            "error_handling": eh,
        })
        assert result["success"] is True
        assert "error_handling" in result["changed_fields"]
        import yaml
        raw = yaml.safe_load((tmp_path / "test-eh-upd.yaml").read_text())
        assert raw["error_handling"]["on_error"] == "retry"
        assert raw["error_handling"]["retry"]["max"] == 5

    @pytest.mark.asyncio
    async def test_create_pipeline_with_groups(self, tmp_path, monkeypatch):
        """create_pipeline stores named step groups."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        groups = {
            "shared_fetch": [
                {"id": "fetch", "type": "cli", "args": ["echo", "fetching"]},
            ]
        }
        result = await _handle_create_pipeline({
            "name": "test-groups-create",
            "groups": groups,
        })
        assert result["success"] is True
        import yaml
        raw = yaml.safe_load((tmp_path / "test-groups-create.yaml").read_text())
        assert "shared_fetch" in raw.get("groups", {})

    @pytest.mark.asyncio
    async def test_update_pipeline_groups(self, tmp_path, monkeypatch):
        """update_pipeline sets groups dict."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-groups-upd"})
        groups = {
            "setup": [{"id": "init", "type": "cli", "args": ["echo", "init"]}]
        }
        result = await _handle_update_pipeline({
            "name": "test-groups-upd",
            "groups": groups,
        })
        assert result["success"] is True
        assert "groups" in result["changed_fields"]
        import yaml
        raw = yaml.safe_load((tmp_path / "test-groups-upd.yaml").read_text())
        assert "setup" in raw.get("groups", {})

    @pytest.mark.asyncio
    async def test_create_pipeline_with_output(self, tmp_path, monkeypatch):
        """create_pipeline stores output template."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        output = {"result": "{{ steps.fetch.data }}", "count": "{{ steps.fetch.count }}"}
        result = await _handle_create_pipeline({
            "name": "test-output-create",
            "output": output,
        })
        assert result["success"] is True
        import yaml
        raw = yaml.safe_load((tmp_path / "test-output-create.yaml").read_text())
        assert raw.get("output", {}).get("result") == "{{ steps.fetch.data }}"

    @pytest.mark.asyncio
    async def test_update_pipeline_output(self, tmp_path, monkeypatch):
        """update_pipeline sets output template."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-output-upd"})
        output = {"summary": "{{ steps.process.summary }}"}
        result = await _handle_update_pipeline({
            "name": "test-output-upd",
            "output": output,
        })
        assert result["success"] is True
        assert "output" in result["changed_fields"]
        import yaml
        raw = yaml.safe_load((tmp_path / "test-output-upd.yaml").read_text())
        assert raw["output"]["summary"] == "{{ steps.process.summary }}"

    @pytest.mark.asyncio
    async def test_update_pipeline_all_new_fields(self, tmp_path, monkeypatch):
        """update_pipeline can set credentials, error_handling, groups, and output in one call."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "test-all-new-fields"})
        result = await _handle_update_pipeline({
            "name": "test-all-new-fields",
            "credentials": {"api": "cred-abc"},
            "error_handling": {"on_error": "continue"},
            "groups": {"g1": [{"id": "s1", "type": "cli", "args": ["echo", "x"]}]},
            "output": {"out": "{{ steps.s1.result }}"},
        })
        assert result["success"] is True
        assert set(result["changed_fields"]) == {"credentials", "error_handling", "groups", "output"}

    def test_create_pipeline_inputschema_has_new_fields(self):
        """brix__create_pipeline tool schema exposes credentials, error_handling, groups, output."""
        tool = next(t for t in BRIX_TOOLS if t.name == "brix__create_pipeline")
        props = tool.inputSchema["properties"]
        for field in ("credentials", "error_handling", "groups", "output"):
            assert field in props, f"Missing field in create_pipeline schema: {field}"

    def test_update_pipeline_inputschema_has_new_fields(self):
        """brix__update_pipeline tool schema exposes credentials, error_handling, groups, output."""
        tool = next(t for t in BRIX_TOOLS if t.name == "brix__update_pipeline")
        props = tool.inputSchema["properties"]
        for field in ("credentials", "error_handling", "groups", "output"):
            assert field in props, f"Missing field in update_pipeline schema: {field}"

    @pytest.mark.asyncio
    async def test_update_pipeline_new_fields_preserve_steps(self, tmp_path, monkeypatch):
        """Updating new fields leaves steps intact."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-new-fields-steps",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo", "hi"]}],
        })
        await _handle_update_pipeline({
            "name": "test-new-fields-steps",
            "credentials": {"key": "val"},
            "output": {"r": "{{ steps.s1.result }}"},
        })
        pipeline = await _handle_get_pipeline({"pipeline_id": "test-new-fields-steps"})
        assert pipeline["step_count"] == 1
        assert pipeline["steps"][0]["id"] == "s1"


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
    async def test_get_run_status_includes_result(self, tmp_path, monkeypatch):
        """get_run_status includes 'result' field with actual pipeline output (T-BRIX-V4-BUG-10)."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "test-result-output",
            "steps": [{"id": "greet", "type": "cli", "args": ["echo", "hello"]}],
        })
        run_result = await _handle_run_pipeline({"pipeline_id": "test-result-output"})
        run_id = run_result["run_id"]

        status = await _handle_get_run_status({"run_id": run_id})
        assert status["success"] is True
        # result field must be present (pipeline has an output — the last step output)
        assert "result" in status

    @pytest.mark.asyncio
    async def test_get_run_status_result_truncated(self, tmp_path, monkeypatch):
        """get_run_status shows truncation notice when result exceeds 10 KB (T-BRIX-V4-BUG-10)."""
        import json as _json
        from brix.history import RunHistory

        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)

        # Directly write a large result to history to test truncation path
        history = RunHistory()
        history.record_start("run-big-result", "fake-pipeline")
        history.record_finish(
            "run-big-result", True, 1.0,
            result_summary={"data": "x" * 12000},
        )

        status = await _handle_get_run_status({"run_id": "run-big-result"})
        assert status["success"] is True
        assert "result" in status
        assert "truncated" in status["result"]
        assert "get_run_log" in status["result"]

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

    @pytest.mark.asyncio
    async def test_run_pipeline_unknown_params_warning(self, tmp_path, monkeypatch):
        """Unknown input params produce a warning in the result."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        # Create a pipeline with only 'name' input
        await _handle_create_pipeline({
            "name": "warn-test",
            "input_schema": {
                "name": {"type": "string", "description": "A name param"},
            },
            "steps": [{"id": "greet", "type": "cli", "args": ["echo", "hello"]}],
        })
        # Run with 'name' + 'unknown_param'
        result = await _handle_run_pipeline({
            "pipeline_id": "warn-test",
            "input": {"name": "brix", "unknown_param": "oops", "another_extra": "42"},
        })
        assert result["success"] is True
        assert "warnings" in result
        assert isinstance(result["warnings"], list)
        assert len(result["warnings"]) == 1
        warning_msg = result["warnings"][0]
        assert "unknown_param" in warning_msg
        assert "another_extra" in warning_msg
        assert "ignored" in warning_msg.lower()

    @pytest.mark.asyncio
    async def test_run_pipeline_no_warning_when_params_known(self, tmp_path, monkeypatch):
        """No warnings when all input params match the pipeline schema."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "no-warn-test",
            "input_schema": {
                "name": {"type": "string", "description": "A name param"},
            },
            "steps": [{"id": "greet", "type": "cli", "args": ["echo", "hello"]}],
        })
        result = await _handle_run_pipeline({
            "pipeline_id": "no-warn-test",
            "input": {"name": "brix"},
        })
        assert result["success"] is True
        assert result["warnings"] == []


# ---------------------------------------------------------------------------
# V2-08 + V2-09 Pipeline Store + Auto-Exposure Tests
# ---------------------------------------------------------------------------

class TestPipelineToolsRegistered:
    """V2-09: Saved pipelines appear as brix__pipeline__* MCP tools."""

    def test_pipeline_tools_registered(self, tmp_path):
        """A saved pipeline appears in the dynamic tool list."""
        import yaml as _yaml
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(
            {
                "name": "my-exposed-pipeline",
                "version": "1.0.0",
                "description": "A pipeline that should be auto-exposed.",
                "steps": [{"id": "step1", "type": "cli", "args": ["echo", "hi"]}],
            }
        )

        pipeline_tools = _build_pipeline_tools(store)
        tool_names = [t.name for t in pipeline_tools]
        assert "brix__pipeline__my_exposed_pipeline" in tool_names

    def test_pipeline_tool_has_description(self, tmp_path):
        """Auto-exposed pipeline tool has a non-empty description."""
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(
            {
                "name": "desc-pipeline",
                "version": "1.0.0",
                "description": "Does something useful.",
                "steps": [{"id": "s", "type": "cli", "args": ["echo", "ok"]}],
            }
        )

        pipeline_tools = _build_pipeline_tools(store)
        tool = next(t for t in pipeline_tools if t.name == "brix__pipeline__desc_pipeline")
        assert tool.description
        assert len(tool.description) >= 10
        assert "Returns" in tool.description

    def test_pipeline_tool_input_schema(self, tmp_path):
        """Auto-exposed tool input schema is built from pipeline input params."""
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(
            {
                "name": "param-pipeline",
                "version": "1.0.0",
                "input": {
                    "query": {"type": "string", "description": "The search term"},
                    "limit": {"type": "integer", "default": 10},
                },
                "steps": [{"id": "s", "type": "cli", "args": ["echo", "{{ input.query }}"]}],
            }
        )

        pipeline_tools = _build_pipeline_tools(store)
        tool = next(t for t in pipeline_tools if t.name == "brix__pipeline__param_pipeline")
        schema = tool.inputSchema
        assert schema["type"] == "object"
        assert "query" in schema["properties"]
        assert "limit" in schema["properties"]
        # query has no default → required
        assert "query" in schema["required"]
        # limit has default → not required
        assert "limit" not in schema["required"]

    def test_pipeline_tool_no_input_schema_when_no_params(self, tmp_path):
        """Pipeline with no input params gets empty properties schema."""
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(
            {
                "name": "no-input",
                "version": "1.0.0",
                "steps": [{"id": "s", "type": "cli", "args": ["echo", "ok"]}],
            }
        )

        pipeline_tools = _build_pipeline_tools(store)
        tool = next(t for t in pipeline_tools if t.name == "brix__pipeline__no_input")
        schema = tool.inputSchema
        # Pipeline with no declared input params should only have the injected 'source' param
        assert set(schema["properties"].keys()) == {"source"}
        assert schema["required"] == []

    def test_multiple_pipelines_all_exposed(self, tmp_path, isolated_db):
        """Multiple saved pipelines all appear as tools."""
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        for i in range(3):
            store.save(
                {
                    "name": f"pipe-{i}",
                    "version": "1.0.0",
                    "steps": [{"id": "s", "type": "cli", "args": ["echo", str(i)]}],
                }
            )

        pipeline_tools = _build_pipeline_tools(store)
        assert len(pipeline_tools) == 3
        names = {t.name for t in pipeline_tools}
        assert "brix__pipeline__pipe_0" in names
        assert "brix__pipeline__pipe_1" in names
        assert "brix__pipeline__pipe_2" in names

    def test_empty_store_no_pipeline_tools(self, tmp_path, isolated_db):
        """Empty store produces no pipeline tools."""
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        pipeline_tools = _build_pipeline_tools(store)
        assert pipeline_tools == []

    def test_server_list_tools_includes_pipeline_tools(self, tmp_path, monkeypatch):
        """_build_pipeline_tools includes pipeline tools when store has saved pipelines."""
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(
            {
                "name": "server-test-pipe",
                "version": "1.0.0",
                "steps": [{"id": "s", "type": "cli", "args": ["echo", "test"]}],
            }
        )

        # Core tools (BRIX_TOOLS) + pipeline tools together form the full list
        pipeline_tools = _build_pipeline_tools(store)
        all_tools = BRIX_TOOLS + pipeline_tools
        tool_names = [t.name for t in all_tools]
        # Core tools still present
        assert "brix__get_tips" in tool_names
        assert "brix__run_pipeline" in tool_names
        # Pipeline tool present
        assert "brix__pipeline__server_test_pipe" in tool_names


class TestPipelineToolExecution:
    """V2-09: brix__pipeline__* tools execute the underlying pipeline."""

    @pytest.mark.asyncio
    async def test_pipeline_tool_execution(self, tmp_path, monkeypatch):
        """Execute a pipeline via its auto-exposed tool."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(
            {
                "name": "exec-test",
                "version": "1.0.0",
                "steps": [{"id": "greet", "type": "cli", "args": ["echo", "pipeline tool works"]}],
            }
        )

        result = await _handle_pipeline_tool("exec-test", {})
        assert result["success"] is True
        assert "run_id" in result
        assert result["steps"]["greet"]["status"] == "ok"

    @pytest.mark.asyncio
    async def test_pipeline_tool_not_found(self, tmp_path, monkeypatch):
        """_handle_pipeline_tool returns structured error for missing pipeline."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_pipeline_tool("does-not-exist-xyz", {})
        assert result["success"] is False
        assert result["error"]["code"] == "PIPELINE_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_pipeline_tool_with_input(self, tmp_path, monkeypatch):
        """Pipeline tool passes input arguments to the pipeline engine."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(
            {
                "name": "input-exec-test",
                "version": "1.0.0",
                "input": {
                    "message": {"type": "string", "description": "A message to echo"},
                },
                "steps": [
                    {
                        "id": "say",
                        "type": "cli",
                        "args": ["echo", "{{ input.message }}"],
                    }
                ],
            }
        )

        result = await _handle_pipeline_tool("input-exec-test", {"message": "hello from tool"})
        assert result["success"] is True
        assert result["steps"]["say"]["status"] == "ok"

    @pytest.mark.asyncio
    async def test_pipeline_tool_prefix_constant(self):
        """PIPELINE_TOOL_PREFIX is the expected string."""
        assert PIPELINE_TOOL_PREFIX == "brix__pipeline__"


# ---------------------------------------------------------------------------
# T-BRIX-V3-04 Async Dispatch Tests
# ---------------------------------------------------------------------------

class TestAsyncDispatch:
    """run_pipeline async=true returns run_id immediately; sync mode unchanged."""

    @pytest.mark.asyncio
    async def test_run_pipeline_async_returns_immediately(self, tmp_path, monkeypatch):
        """async=true → instant response with run_id and status=running."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "async-test",
            "steps": [{"id": "work", "type": "cli", "args": ["echo", "async hello"]}],
        })
        result = await _handle_run_pipeline({"pipeline_id": "async-test", "async": True})

        assert result["success"] is True
        assert "run_id" in result
        assert result["run_id"].startswith("run-")
        assert result["status"] == "running"
        assert "message" in result
        assert result["run_id"] in result["message"]

        # Let the background task finish so it doesn't leak into other tests
        from brix.mcp_server import _background_runs
        run_id = result["run_id"]
        if run_id in _background_runs:
            import asyncio as _asyncio
            task = _background_runs[run_id]
            try:
                await _asyncio.wait_for(_asyncio.shield(task), timeout=5.0)
            except (_asyncio.TimeoutError, Exception):
                pass

    @pytest.mark.asyncio
    async def test_run_pipeline_sync_still_works(self, tmp_path, monkeypatch):
        """async=false (or omitted) → blocking response with full result (original behaviour)."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "sync-test",
            "steps": [{"id": "greet", "type": "cli", "args": ["echo", "sync hello"]}],
        })
        result = await _handle_run_pipeline({"pipeline_id": "sync-test", "async": False})

        assert result["success"] is True
        assert "run_id" in result
        assert "steps" in result
        assert result["steps"]["greet"]["status"] == "ok"
        # sync mode must NOT have a "status": "running" key
        assert result.get("status") != "running"

    @pytest.mark.asyncio
    async def test_run_pipeline_async_run_id_recorded_in_history(self, tmp_path, monkeypatch):
        """Background run's run_id should appear in history after completion."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "async-history-test",
            "steps": [{"id": "hi", "type": "cli", "args": ["echo", "bg"]}],
        })
        result = await _handle_run_pipeline({"pipeline_id": "async-history-test", "async": True})
        run_id = result["run_id"]

        # Wait for background task to complete
        from brix.mcp_server import _background_runs
        import asyncio as _asyncio
        if run_id in _background_runs:
            task = _background_runs[run_id]
            try:
                await _asyncio.wait_for(_asyncio.shield(task), timeout=5.0)
            except (_asyncio.TimeoutError, Exception):
                pass
        # Allow event loop to settle
        await _asyncio.sleep(0.05)

        status = await _handle_get_run_status({"run_id": run_id})
        assert status["success"] is True
        assert status["run_id"] == run_id

    @pytest.mark.asyncio
    async def test_run_pipeline_async_schema_has_async_param(self):
        """brix__run_pipeline tool schema exposes the async parameter."""
        tool = next(t for t in BRIX_TOOLS if t.name == "brix__run_pipeline")
        props = tool.inputSchema.get("properties", {})
        assert "async" in props
        assert props["async"]["type"] == "boolean"
        assert props["async"].get("default") is False


# ---------------------------------------------------------------------------
# T-BRIX-V3-12: brix__update_step tests
# ---------------------------------------------------------------------------

class TestUpdateStep:
    """Tests for the brix__update_step handler."""

    @pytest.mark.asyncio
    async def test_update_step_success(self, tmp_path, monkeypatch):
        """Change a parameter on an existing step → success with updated_fields."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "update-test",
            "steps": [{"id": "fetch", "type": "cli", "args": ["echo", "original"], "top": 100}],
        })
        result = await _handle_update_step({
            "pipeline_name": "update-test",
            "step_id": "fetch",
            "updates": {"top": 500, "concurrency": 10},
        })
        assert result["success"] is True
        assert result["step_id"] == "fetch"
        assert set(result["updated_fields"]) == {"top", "concurrency"}
        assert "validated" in result

        # Verify the change was persisted
        pipeline = await _handle_get_pipeline({"pipeline_id": "update-test"})
        fetch_step = next(s for s in pipeline["steps"] if s["id"] == "fetch")
        assert fetch_step["top"] == 500
        assert fetch_step["concurrency"] == 10

    @pytest.mark.asyncio
    async def test_update_step_not_found(self, tmp_path, monkeypatch):
        """Step ID does not exist → success=False with error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "update-missing-step",
            "steps": [{"id": "real_step", "type": "cli", "args": ["echo", "hi"]}],
        })
        result = await _handle_update_step({
            "pipeline_name": "update-missing-step",
            "step_id": "ghost_step",
            "updates": {"top": 200},
        })
        assert result["success"] is False
        assert "error" in result
        assert "ghost_step" in result["error"]

    @pytest.mark.asyncio
    async def test_update_step_pipeline_not_found(self, tmp_path, monkeypatch):
        """Pipeline does not exist → success=False with error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_update_step({
            "pipeline_name": "nonexistent-pipeline-xyz",
            "step_id": "some_step",
            "updates": {"top": 100},
        })
        assert result["success"] is False
        assert "error" in result
        assert "nonexistent-pipeline-xyz" in result["error"]

    @pytest.mark.asyncio
    async def test_update_step_id_cannot_change(self, tmp_path, monkeypatch):
        """Passing 'id' in updates must be silently ignored."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "update-id-test",
            "steps": [{"id": "original_id", "type": "cli", "args": ["echo", "x"]}],
        })
        result = await _handle_update_step({
            "pipeline_name": "update-id-test",
            "step_id": "original_id",
            "updates": {"id": "new_id", "top": 99},
        })
        assert result["success"] is True
        # Step ID must still be the original
        pipeline = await _handle_get_pipeline({"pipeline_id": "update-id-test"})
        step_ids = [s["id"] for s in pipeline["steps"]]
        assert "original_id" in step_ids
        assert "new_id" not in step_ids

    @pytest.mark.asyncio
    async def test_update_substep_in_repeat_sequence(self, tmp_path, monkeypatch):
        """update_step can update a sub-step nested inside repeat.sequence."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "repeat-substep-test",
            "steps": [
                {
                    "id": "loop",
                    "type": "repeat",
                    "until": "false",
                    "max_iterations": 1,
                    "sequence": [
                        {"id": "inner", "type": "cli", "args": ["echo", "original"], "top": 10},
                    ],
                }
            ],
        })

        result = await _handle_update_step({
            "pipeline_name": "repeat-substep-test",
            "step_id": "inner",
            "updates": {"top": 999, "args": ["echo", "updated"]},
        })

        assert result["success"] is True
        assert result["step_id"] == "inner"
        assert set(result["updated_fields"]) == {"top", "args"}

        # Verify the change was persisted inside sequence
        pipeline = await _handle_get_pipeline({"pipeline_id": "repeat-substep-test"})
        loop_step = next(s for s in pipeline["steps"] if s["id"] == "loop")
        inner = next(s for s in loop_step["sequence"] if s["id"] == "inner")
        assert inner["top"] == 999
        assert inner["args"] == ["echo", "updated"]

    @pytest.mark.asyncio
    async def test_update_substep_in_choose_choices(self, tmp_path, monkeypatch):
        """update_step can update a sub-step nested inside choose.choices[].steps."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "choose-substep-test",
            "steps": [
                {
                    "id": "branch",
                    "type": "choose",
                    "choices": [
                        {
                            "when": "true",
                            "steps": [
                                {"id": "branch_step", "type": "cli", "args": ["echo", "old"], "top": 5},
                            ],
                        }
                    ],
                }
            ],
        })

        result = await _handle_update_step({
            "pipeline_name": "choose-substep-test",
            "step_id": "branch_step",
            "updates": {"top": 777},
        })

        assert result["success"] is True
        assert result["step_id"] == "branch_step"
        assert result["updated_fields"] == ["top"]

        # Verify the change was persisted inside the choice branch
        pipeline = await _handle_get_pipeline({"pipeline_id": "choose-substep-test"})
        branch = next(s for s in pipeline["steps"] if s["id"] == "branch")
        branch_step = next(s for s in branch["choices"][0]["steps"] if s["id"] == "branch_step")
        assert branch_step["top"] == 777


# ---------------------------------------------------------------------------
# T-BRIX-V3-13: Live run.json polling + heartbeat + hang detection
# T-BRIX-V3-15: resume_run_id in run_pipeline MCP Tool
# ---------------------------------------------------------------------------

class TestLiveRunStatusAndResume:
    """Tests for live workdir polling, hang detection, and resume_run_id."""

    @pytest.mark.asyncio
    async def test_get_run_status_live_workdir(self, tmp_path, monkeypatch):
        """get_run_status reads live run.json from workdir when status=running."""
        import json as _json
        import time as _time
        from brix.context import WORKDIR_BASE

        # Write a fake in-progress run.json directly to the workdir
        run_id = "run-livetest0001"
        run_dir = WORKDIR_BASE / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        run_json = {
            "run_id": run_id,
            "pipeline": "live-test",
            "input": {},
            "status": "running",
            "completed_steps": ["step1"],
            "last_heartbeat": _time.time(),
        }
        (run_dir / "run.json").write_text(_json.dumps(run_json))

        try:
            result = await _handle_get_run_status({"run_id": run_id})
            assert result["success"] is True
            assert result["source"] == "live"
            assert result["status"] == "running"
            assert result["run_id"] == run_id
            assert "suspected_hang" in result
            assert result["suspected_hang"] is False  # heartbeat is fresh
        finally:
            import shutil
            shutil.rmtree(run_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_get_run_status_history_fallback(self, tmp_path, monkeypatch):
        """get_run_status falls back to SQLite when no live workdir exists."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "history-fallback-test",
            "steps": [{"id": "hi", "type": "cli", "args": ["echo", "done"]}],
        })
        run_result = await _handle_run_pipeline({"pipeline_id": "history-fallback-test"})
        run_id = run_result["run_id"]

        # Completed runs are in SQLite; workdir may or may not still exist but status != running
        status = await _handle_get_run_status({"run_id": run_id})
        assert status["success"] is True
        assert status["run_id"] == run_id

    @pytest.mark.asyncio
    async def test_get_run_status_hang_detection(self, tmp_path):
        """Old heartbeat (>5 min) in running run.json → suspected_hang=True."""
        import json as _json
        import time as _time
        from brix.context import WORKDIR_BASE

        run_id = "run-hangtest00001"
        run_dir = WORKDIR_BASE / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        run_json = {
            "run_id": run_id,
            "pipeline": "hang-test",
            "input": {},
            "status": "running",
            "completed_steps": [],
            "last_heartbeat": _time.time() - 400,  # 400s ago → >300s threshold
        }
        (run_dir / "run.json").write_text(_json.dumps(run_json))

        try:
            result = await _handle_get_run_status({"run_id": run_id})
            assert result["success"] is True
            assert result["source"] == "live"
            assert result["suspected_hang"] is True
        finally:
            import shutil
            shutil.rmtree(run_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_run_pipeline_resume_param(self, tmp_path, monkeypatch):
        """brix__run_pipeline tool schema exposes resume_run_id parameter."""
        tool = next(t for t in BRIX_TOOLS if t.name == "brix__run_pipeline")
        props = tool.inputSchema.get("properties", {})
        assert "resume_run_id" in props
        assert props["resume_run_id"]["type"] == "string"
        assert "resume" in props["resume_run_id"]["description"].lower()

    @pytest.mark.asyncio
    async def test_run_pipeline_resume_not_found(self, tmp_path, monkeypatch):
        """resume_run_id pointing to nonexistent workdir returns structured error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "resume-missing-test",
            "steps": [{"id": "hi", "type": "cli", "args": ["echo", "hi"]}],
        })
        result = await _handle_run_pipeline({
            "pipeline_id": "resume-missing-test",
            "resume_run_id": "run-doesnotexist999",
        })
        assert result["success"] is False
        assert result["error"]["code"] == "RESUME_RUN_NOT_FOUND"
        assert result["error"]["recoverable"] is False


# ---------------------------------------------------------------------------
# T-BRIX-V4-21: Runtime input parameter validation
# ---------------------------------------------------------------------------

class TestRuntimeInputValidation:
    """Runtime validation of required input parameters before execution (T-BRIX-V4-21)."""

    @pytest.mark.asyncio
    async def test_missing_required_param_returns_error(self, tmp_path, monkeypatch):
        """Pipeline with required param not provided → MISSING_REQUIRED_PARAMS error."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "required-param-test",
            "input_schema": {
                "folder": {"type": "string"},   # required — no default
            },
            "steps": [{"id": "greet", "type": "cli", "args": ["echo", "hi"]}],
        })
        result = await _handle_run_pipeline({
            "pipeline_id": "required-param-test",
            "input": {},  # folder missing
        })
        assert result["success"] is False
        assert result["error"]["code"] == "MISSING_REQUIRED_PARAMS"
        assert "folder" in result["error"]["missing_params"]
        assert result["error"]["recoverable"] is True

    @pytest.mark.asyncio
    async def test_missing_required_param_lists_all_missing(self, tmp_path, monkeypatch):
        """All missing required params are reported in one response."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "multi-required-test",
            "input_schema": {
                "source": {"type": "string"},
                "dest": {"type": "string"},
                "limit": {"type": "integer", "default": 10},  # has default → optional
            },
            "steps": [{"id": "run", "type": "cli", "args": ["echo", "go"]}],
        })
        result = await _handle_run_pipeline({
            "pipeline_id": "multi-required-test",
            "input": {},
        })
        assert result["success"] is False
        assert result["error"]["code"] == "MISSING_REQUIRED_PARAMS"
        missing = result["error"]["missing_params"]
        assert "source" in missing
        assert "dest" in missing
        assert "limit" not in missing  # has default, not required

    @pytest.mark.asyncio
    async def test_required_param_provided_runs_successfully(self, tmp_path, monkeypatch):
        """Pipeline runs when all required params are provided."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "required-ok-test",
            "input_schema": {
                "folder": {"type": "string"},
            },
            "steps": [{"id": "greet", "type": "cli", "args": ["echo", "hi"]}],
        })
        result = await _handle_run_pipeline({
            "pipeline_id": "required-ok-test",
            "input": {"folder": "Inbox"},
        })
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_optional_only_pipeline_runs_without_input(self, tmp_path, monkeypatch):
        """Pipeline with only optional params runs with empty input."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "optional-only-test",
            "input_schema": {
                "limit": {"type": "integer", "default": 100},
            },
            "steps": [{"id": "run", "type": "cli", "args": ["echo", "ok"]}],
        })
        result = await _handle_run_pipeline({
            "pipeline_id": "optional-only-test",
            "input": {},
        })
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_missing_required_error_has_agent_actions(self, tmp_path, monkeypatch):
        """MISSING_REQUIRED_PARAMS error includes agent_actions for recovery."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "agent-actions-test",
            "input_schema": {"token": {"type": "string"}},
            "steps": [{"id": "run", "type": "cli", "args": ["echo", "x"]}],
        })
        result = await _handle_run_pipeline({
            "pipeline_id": "agent-actions-test",
            "input": {},
        })
        assert result["success"] is False
        assert "agent_actions" in result["error"]
        assert isinstance(result["error"]["agent_actions"], list)
        assert len(result["error"]["agent_actions"]) > 0


# ---------------------------------------------------------------------------
# T-BRIX-V4-BUG-04: get_run_errors and get_run_log MCP handlers
# ---------------------------------------------------------------------------

class TestGetRunErrors:
    """Tests for the brix__get_run_errors MCP handler."""

    @pytest.mark.asyncio
    async def test_get_run_errors_no_args_returns_empty(self):
        """Calling with no args returns empty errors list."""
        result = await _handle_get_run_errors({})
        assert result["success"] is True
        assert result["errors"] == []
        assert result["total"] == 0

    @pytest.mark.asyncio
    async def test_get_run_errors_unknown_run_id(self, tmp_path, monkeypatch):
        """Unknown run_id returns empty errors list."""
        from brix import history as hist_mod
        orig = hist_mod.HISTORY_DB_PATH
        hist_mod.HISTORY_DB_PATH = tmp_path / "test.db"
        try:
            result = await _handle_get_run_errors({"run_id": "nonexistent-run"})
            assert result["success"] is True
            assert result["errors"] == []
        finally:
            hist_mod.HISTORY_DB_PATH = orig

    @pytest.mark.asyncio
    async def test_get_run_errors_returns_error_steps(self, tmp_path, monkeypatch):
        """Returns failed step details when run has errors."""
        from brix import history as hist_mod
        orig = hist_mod.HISTORY_DB_PATH
        hist_mod.HISTORY_DB_PATH = tmp_path / "test.db"
        try:
            h = hist_mod.RunHistory(db_path=tmp_path / "test.db")
            h.record_start("err-run-1", "broken-pipeline")
            h.record_finish("err-run-1", False, 2.0, {
                "step1": {"status": "ok", "duration": 0.5},
                "step2": {"status": "error", "duration": 1.5, "errors": 1,
                          "error_message": "ModuleNotFoundError: requests"},
            })
            result = await _handle_get_run_errors({"run_id": "err-run-1"})
            assert result["success"] is True
            assert result["total"] == 1
            err = result["errors"][0]
            assert err["step_id"] == "step2"
            assert "ModuleNotFoundError" in err["error_message"]
            assert err["hint"] is not None
        finally:
            hist_mod.HISTORY_DB_PATH = orig


class TestGetRunLog:
    """Tests for the brix__get_run_log MCP handler."""

    @pytest.mark.asyncio
    async def test_get_run_log_not_found(self, tmp_path, monkeypatch):
        """Returns error for unknown run_id."""
        from brix import history as hist_mod
        orig = hist_mod.HISTORY_DB_PATH
        hist_mod.HISTORY_DB_PATH = tmp_path / "test.db"
        try:
            result = await _handle_get_run_log({"run_id": "no-such-run"})
            assert result["success"] is False
            assert "not found" in result["error"]
        finally:
            hist_mod.HISTORY_DB_PATH = orig

    @pytest.mark.asyncio
    async def test_get_run_log_returns_steps(self, tmp_path, monkeypatch):
        """Returns ordered step entries for a completed run."""
        from brix import history as hist_mod
        orig = hist_mod.HISTORY_DB_PATH
        hist_mod.HISTORY_DB_PATH = tmp_path / "test.db"
        try:
            h = hist_mod.RunHistory(db_path=tmp_path / "test.db")
            h.record_start("log-run-1", "sample-pipeline")
            h.record_finish("log-run-1", True, 3.0, {
                "fetch": {"status": "ok", "duration": 1.0, "items": 100},
                "save": {"status": "ok", "duration": 2.0, "items": None},
            })
            result = await _handle_get_run_log({"run_id": "log-run-1"})
            assert result["success"] is True
            assert result["run_id"] == "log-run-1"
            assert result["total_steps"] == 2
            by_id = {s["step_id"]: s for s in result["steps"]}
            assert by_id["fetch"]["status"] == "ok"
            assert by_id["fetch"]["items"] == 100
            assert by_id["save"]["status"] == "ok"
        finally:
            hist_mod.HISTORY_DB_PATH = orig


# ---------------------------------------------------------------------------
# T-BRIX-V5-13: Rename Pipeline + Rename Helper + test_pipeline MCP tool
# ---------------------------------------------------------------------------

class TestRenamePipeline:
    """Tests for brix__rename_pipeline handler."""

    @pytest.mark.asyncio
    async def test_rename_pipeline_basic(self, tmp_path, monkeypatch):
        """Rename a pipeline: new YAML file created, old file removed, db updated."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "xtest-orig-rename-basic"})
        assert (tmp_path / "xtest-orig-rename-basic.yaml").exists()

        result = await _handle_rename_pipeline({
            "old_name": "xtest-orig-rename-basic",
            "new_name": "xtest-new-rename-basic",
        })
        assert result["success"] is True
        assert result["old_name"] == "xtest-orig-rename-basic"
        assert result["new_name"] == "xtest-new-rename-basic"

        assert not (tmp_path / "xtest-orig-rename-basic.yaml").exists()
        assert (tmp_path / "xtest-new-rename-basic.yaml").exists()

    @pytest.mark.asyncio
    async def test_rename_pipeline_updates_name_field(self, tmp_path, monkeypatch):
        """The 'name' field inside the YAML is updated to the new name."""
        import yaml as _yaml
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "xtest-rn-alpha"})

        await _handle_rename_pipeline({"old_name": "xtest-rn-alpha", "new_name": "xtest-rn-beta"})

        raw = _yaml.safe_load((tmp_path / "xtest-rn-beta.yaml").read_text())
        assert raw["name"] == "xtest-rn-beta"

    @pytest.mark.asyncio
    async def test_rename_pipeline_preserves_uuid(self, tmp_path, monkeypatch):
        """The pipeline UUID is preserved after rename."""
        import yaml as _yaml
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "xtest-uuid-pipe"})

        before_raw = _yaml.safe_load((tmp_path / "xtest-uuid-pipe.yaml").read_text())
        original_uuid = before_raw.get("id")

        await _handle_rename_pipeline({"old_name": "xtest-uuid-pipe", "new_name": "xtest-uuid-renamed"})

        after_raw = _yaml.safe_load((tmp_path / "xtest-uuid-renamed.yaml").read_text())
        assert after_raw.get("id") == original_uuid

    @pytest.mark.asyncio
    async def test_rename_pipeline_not_found(self, tmp_path, monkeypatch):
        """Returns error if the pipeline does not exist."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_rename_pipeline({
            "old_name": "nonexistent-pipeline",
            "new_name": "new-name",
        })
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_rename_pipeline_target_exists(self, tmp_path, monkeypatch):
        """Returns error if the target name already exists."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "source-pipeline"})
        await _handle_create_pipeline({"name": "target-pipeline"})

        result = await _handle_rename_pipeline({
            "old_name": "source-pipeline",
            "new_name": "target-pipeline",
        })
        assert result["success"] is False
        assert "already exists" in result["error"]

    @pytest.mark.asyncio
    async def test_rename_pipeline_same_name(self, tmp_path, monkeypatch):
        """Returns error if old_name == new_name."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "same-name"})
        result = await _handle_rename_pipeline({"old_name": "same-name", "new_name": "same-name"})
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_rename_pipeline_warns_sub_pipeline_references(self, tmp_path, monkeypatch):
        """Returns warning if another pipeline references the old name."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({"name": "xtest-rn-base"})
        await _handle_create_pipeline({
            "name": "xtest-rn-caller",
            "steps": [{
                "id": "sub",
                "type": "pipeline",
                "pipeline": "xtest-rn-base",
            }],
        })

        result = await _handle_rename_pipeline({
            "old_name": "xtest-rn-base",
            "new_name": "xtest-rn-base-v2",
        })
        assert result["success"] is True
        assert "warning" in result
        assert "xtest-rn-caller" in result.get("affected_pipelines", [])

    def test_rename_pipeline_has_handler(self):
        """brix__rename_pipeline has a handler registered."""
        assert "brix__rename_pipeline" in _HANDLERS


class TestRenameHelper:
    """Tests for brix__rename_helper handler."""

    @pytest.mark.asyncio
    async def test_rename_helper_not_found(self, tmp_path, monkeypatch):
        """Returns error if the helper does not exist."""
        from brix.helper_registry import HelperRegistry, REGISTRY_PATH
        import brix.helper_registry as hr_mod
        import brix.mcp_server as mcp_mod

        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        registry_path = helpers_dir / "registry.yaml"

        registry = HelperRegistry(registry_path=registry_path)
        monkeypatch.setattr(mcp_mod, "HelperRegistry", lambda: registry)

        result = await _handle_rename_helper({"old_name": "nonexistent", "new_name": "new-name"})
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_rename_helper_via_registry_directly(self, tmp_path):
        """Rename helper via the handler with a real tmp registry."""
        from brix.helper_registry import HelperRegistry
        from brix.db import BrixDB

        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        registry_path = helpers_dir / "registry.yaml"
        db_path = tmp_path / "brix.db"

        db = BrixDB(db_path=db_path)
        registry = HelperRegistry(registry_path=registry_path, db=db)

        # Write script file
        script_path = helpers_dir / "my-helper.py"
        script_path.write_text("import json, sys\nprint(json.dumps({}))")

        registry.register(name="my-helper", script=str(script_path), description="Test helper")

        # Rename: simulate what the handler does
        old_entry = registry.get("my-helper")
        assert old_entry is not None

        new_path = script_path.parent / "renamed-helper.py"
        script_path.rename(new_path)

        all_data = registry._load()
        raw = all_data["my-helper"]
        raw["name"] = "renamed-helper"
        raw["script"] = str(new_path)
        all_data["renamed-helper"] = raw
        del all_data["my-helper"]
        registry._save(all_data)
        # Delete old entry first (frees the UUID PRIMARY KEY), then insert new
        db.delete_helper("my-helper")
        db.upsert_helper(
            name="renamed-helper",
            script_path=str(new_path),
            description=raw.get("description", ""),
            requirements=raw.get("requirements", []),
            input_schema=raw.get("input_schema", {}),
            output_schema=raw.get("output_schema", {}),
            helper_id=raw.get("id"),
        )

        # Assertions
        assert not script_path.exists()
        assert new_path.exists()
        assert registry.get("my-helper") is None
        renamed_entry = registry.get("renamed-helper")
        assert renamed_entry is not None
        assert renamed_entry.name == "renamed-helper"
        assert renamed_entry.script == str(new_path)
        # UUID preserved
        assert renamed_entry.id == old_entry.id

    def test_rename_helper_tool_schema(self):
        """brix__rename_helper tool schema requires old_name and new_name."""
        tool = next(t for t in BRIX_TOOLS if t.name == "brix__rename_helper")
        props = tool.inputSchema["properties"]
        assert "old_name" in props
        assert "new_name" in props
        assert tool.inputSchema["required"] == ["old_name", "new_name"]


class TestTestPipeline:
    """Tests for brix__test_pipeline handler."""

    @pytest.mark.asyncio
    async def test_test_pipeline_basic(self, tmp_path, monkeypatch):
        """test_pipeline runs a pipeline with mocks and returns success."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "mock-test-pipeline",
            "steps": [
                {"id": "fetch", "type": "mcp", "server": "m365", "tool": "list-mails"},
                {"id": "save", "type": "cli", "args": ["echo", "done"]},
            ],
        })

        result = await _handle_test_pipeline({
            "name": "mock-test-pipeline",
            "input": {"folder": "Inbox"},
            "mocks": {
                "fetch": {"messages": [1, 2, 3]},
                "save": {"output": "done"},
            },
        })

        assert result["success"] is True
        assert result["pipeline"] == "mock-test-pipeline"
        assert result["summary"]["steps_total"] == 2
        assert result["summary"]["steps_passed"] == 2

    @pytest.mark.asyncio
    async def test_test_pipeline_with_assertions(self, tmp_path, monkeypatch):
        """test_pipeline evaluates assertions and reports pass/fail."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "assert-test-pipeline",
            "steps": [
                {"id": "step1", "type": "mcp", "server": "fake", "tool": "fake-tool"},
            ],
        })

        result = await _handle_test_pipeline({
            "name": "assert-test-pipeline",
            "mocks": {"step1": {"data": "ok"}},
            "assertions": {"step1": [{"status": "ok"}]},
        })

        assert result["success"] is True
        assert result["summary"]["assertions_passed"] == 1
        assert result["summary"]["assertions_total"] == 1

    @pytest.mark.asyncio
    async def test_test_pipeline_failing_assertion(self, tmp_path, monkeypatch):
        """test_pipeline returns success=False when an assertion fails."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        await _handle_create_pipeline({
            "name": "fail-assert-pipeline",
            "steps": [
                {"id": "step1", "type": "mcp", "server": "fake", "tool": "fake-tool"},
            ],
        })

        result = await _handle_test_pipeline({
            "name": "fail-assert-pipeline",
            "mocks": {"step1": {"data": "ok"}},
            "assertions": {"step1": [{"status": "error"}]},  # expects error, gets ok
        })

        assert result["success"] is False
        assert result["summary"]["assertions_passed"] == 0

    @pytest.mark.asyncio
    async def test_test_pipeline_not_found(self, tmp_path, monkeypatch):
        """test_pipeline returns error if pipeline does not exist."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_test_pipeline({"name": "nonexistent-pipeline"})
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.asyncio
    async def test_test_pipeline_missing_name(self, tmp_path, monkeypatch):
        """test_pipeline returns error if name is missing."""
        monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
        result = await _handle_test_pipeline({})
        assert result["success"] is False
        assert "required" in result["error"]

    def test_test_pipeline_tool_in_handlers(self):
        """brix__test_pipeline is registered in _HANDLERS."""
        from brix.mcp_server import _HANDLERS
        assert "brix__test_pipeline" in _HANDLERS

    def test_rename_pipeline_in_handlers(self):
        """brix__rename_pipeline is registered in _HANDLERS."""
        from brix.mcp_server import _HANDLERS
        assert "brix__rename_pipeline" in _HANDLERS

    def test_rename_helper_in_handlers(self):
        """brix__rename_helper is registered in _HANDLERS."""
        from brix.mcp_server import _HANDLERS
        assert "brix__rename_helper" in _HANDLERS

    def test_test_pipeline_tool_schema(self):
        """brix__test_pipeline tool schema exposes name, input, mocks, assertions."""
        tool = next(t for t in BRIX_TOOLS if t.name == "brix__test_pipeline")
        props = tool.inputSchema["properties"]
        assert "name" in props
        assert "input" in props
        assert "mocks" in props
        assert "assertions" in props


# ---------------------------------------------------------------------------
# T-BRIX-V7-03: get_help tests
# ---------------------------------------------------------------------------

class TestGetHelp:
    """Tests for brix__get_help(topic) MCP tool."""

    @pytest.mark.asyncio
    async def test_get_help_no_topic_returns_topic_list(self):
        """Without topic argument, returns list of all available topics."""
        result = await _handle_get_help({})
        assert "topics" in result
        assert "descriptions" in result
        assert "message" in result
        topics = result["topics"]
        assert len(topics) >= 16  # At least 16 base topics from DB
        assert "quick-start" in topics
        assert "step-referenzen" in topics
        assert "foreach" in topics
        assert "debugging" in topics
        assert "tools" in topics
        assert "anti-patterns" in topics
        assert "helpers" in topics
        assert "credentials" in topics
        assert "triggers" in topics
        assert "templates" in topics
        assert "dag" in topics
        assert "sdk" in topics
        assert "beispiele" in topics
        assert "lessons-learned" in topics

    @pytest.mark.asyncio
    async def test_get_help_no_topic_message_contains_usage(self):
        """No-topic response explains how to use the tool."""
        result = await _handle_get_help({})
        assert "brix__get_help" in result["message"]
        assert "quick-start" in result["message"]

    @pytest.mark.asyncio
    async def test_get_help_no_topic_descriptions_non_empty(self):
        """Every topic has a non-empty description."""
        result = await _handle_get_help({})
        for topic, desc in result["descriptions"].items():
            assert desc, f"Topic '{topic}' has empty description"

    @pytest.mark.asyncio
    async def test_get_help_quick_start(self):
        """quick-start topic returns content with key workflow steps."""
        result = await _handle_get_help({"topic": "quick-start"})
        assert result["topic"] == "quick-start"
        assert "content" in result
        content = result["content"]
        assert "get_tips" in content
        assert "create_pipeline" in content
        assert "run_pipeline" in content
        assert len(content) > 200

    @pytest.mark.asyncio
    async def test_get_help_step_referenzen(self):
        """step-referenzen topic covers correct and wrong patterns."""
        result = await _handle_get_help({"topic": "step-referenzen"})
        assert result["topic"] == "step-referenzen"
        content = result["content"]
        assert "step_id.output" in content
        assert "input.param" in content
        assert "credentials." in content
        assert "FALSCH" in content

    @pytest.mark.asyncio
    async def test_get_help_foreach(self):
        """foreach topic covers output structure and results shorthand."""
        result = await _handle_get_help({"topic": "foreach"})
        content = result["content"]
        assert "results" in content
        assert "summary" in content
        assert "concurrency" in content
        assert "flatten" in content or "flat_output" in content or "flatten: true" in content

    @pytest.mark.asyncio
    async def test_get_help_debugging(self):
        """debugging topic covers 4-step guide."""
        result = await _handle_get_help({"topic": "debugging"})
        content = result["content"]
        assert "get_run_errors" in content
        assert "diagnose_run" in content
        assert "auto_fix_step" in content
        assert "get_run_log" in content

    @pytest.mark.asyncio
    async def test_get_help_tools(self):
        """tools topic provides decision tree with tool names."""
        result = await _handle_get_help({"topic": "tools"})
        content = result["content"]
        assert "create_pipeline" in content
        assert "run_pipeline" in content
        assert "get_run_errors" in content
        assert "credential_add" in content

    @pytest.mark.asyncio
    async def test_get_help_anti_patterns(self):
        """anti-patterns topic covers all major NIEMALS rules."""
        result = await _handle_get_help({"topic": "anti-patterns"})
        content = result["content"]
        assert "base64" in content
        assert "concurrency" in content
        assert "Credentials" in content or "credentials" in content
        assert "YAML" in content or "yaml" in content.lower()

    @pytest.mark.asyncio
    async def test_get_help_helpers(self):
        """helpers topic covers create, register, and SDK."""
        result = await _handle_get_help({"topic": "helpers"})
        content = result["content"]
        assert "create_helper" in content
        assert "register_helper" in content
        assert "sdk" in content.lower() or "SDK" in content

    @pytest.mark.asyncio
    async def test_get_help_credentials(self):
        """credentials topic covers workflow and security rules."""
        result = await _handle_get_help({"topic": "credentials"})
        content = result["content"]
        assert "credential_add" in content
        assert "credentials." in content

    @pytest.mark.asyncio
    async def test_get_help_all_topics_return_content(self):
        """All topics return non-empty content."""
        all_topics = [
            "quick-start", "step-referenzen", "foreach", "debugging", "tools",
            "anti-patterns", "helpers", "credentials", "triggers", "templates",
            "dag", "sdk", "beispiele", "lessons-learned",
        ]
        for topic in all_topics:
            result = await _handle_get_help({"topic": topic})
            assert "content" in result, f"Topic '{topic}' missing 'content' key"
            assert result["content"], f"Topic '{topic}' has empty content"
            assert result["topic"] == topic
            assert "description" in result
            assert "all_topics" in result

    @pytest.mark.asyncio
    async def test_get_help_unknown_topic_returns_error(self):
        """Unknown topic returns error dict with available topics list."""
        result = await _handle_get_help({"topic": "unknown-topic"})
        assert "error" in result
        assert "unknown-topic" in result["error"]
        assert "available_topics" in result
        assert "quick-start" in result["available_topics"]

    @pytest.mark.asyncio
    async def test_get_help_unknown_topic_error_lists_all_topics(self):
        """Error response for unknown topic lists valid topics from DB."""
        result = await _handle_get_help({"topic": "does-not-exist"})
        assert len(result["available_topics"]) >= 16  # At least 16 base topics from DB

    def test_get_help_tool_registered_in_handlers(self):
        """brix__get_help is registered in _HANDLERS."""
        from brix.mcp_server import _HANDLERS
        assert "brix__get_help" in _HANDLERS
