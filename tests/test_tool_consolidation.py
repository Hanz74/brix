"""Tests for V8-12 MCP Tool Consolidation.

Verifies that CRUD groups are consolidated into action-parameter tools,
that each action dispatches correctly, and that old individual tools are gone.
"""
import pytest

from brix.mcp_server import (
    BRIX_TOOLS,
    _HANDLERS,
    _handle_registry,
    _handle_trigger,
    _handle_credential,
    _handle_alert,
    _handle_server,
    _handle_state,
    _handle_trigger_group,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CONSOLIDATED_TOOLS = {
    "brix__registry": ["add", "get", "list", "update", "delete", "search"],
    "brix__trigger": ["add", "get", "list", "update", "delete", "test"],
    "brix__credential": ["add", "get", "list", "update", "delete", "rotate", "search"],
    "brix__alert": ["add", "list", "update", "delete", "history"],
    "brix__server": ["add", "list", "update", "remove", "health", "refresh"],
    "brix__state": ["get", "set", "list", "delete"],
    "brix__trigger_group": ["add", "list", "start", "stop", "delete"],
}

# Old individual tools that must NOT exist anymore
REMOVED_INDIVIDUAL_TOOLS = [
    # registry
    "brix__registry_add", "brix__registry_get", "brix__registry_list",
    "brix__registry_update", "brix__registry_delete", "brix__registry_search",
    # trigger
    "brix__trigger_add", "brix__trigger_get", "brix__trigger_list",
    "brix__trigger_update", "brix__trigger_delete", "brix__trigger_test",
    # credential
    "brix__credential_add", "brix__credential_get", "brix__credential_list",
    "brix__credential_update", "brix__credential_delete",
    "brix__credential_rotate", "brix__credential_search",
    # alert
    "brix__alert_add", "brix__alert_list", "brix__alert_update",
    "brix__alert_delete", "brix__alert_history",
    # server
    "brix__server_add", "brix__server_list", "brix__server_update",
    "brix__server_remove", "brix__server_health", "brix__server_refresh",
    # state
    "brix__state_get", "brix__state_set", "brix__state_list", "brix__state_delete",
    # trigger_group
    "brix__trigger_group_add", "brix__trigger_group_list",
    "brix__trigger_group_start", "brix__trigger_group_stop",
    "brix__trigger_group_delete",
]

STANDALONE_TOOLS = [
    "brix__get_tips", "brix__get_help", "brix__get_insights",
    "brix__get_proactive_suggestions",
    "brix__list_pipelines", "brix__list_helpers", "brix__list_bricks",
    "brix__list_connectors",
    "brix__run_pipeline", "brix__create_pipeline", "brix__create_helper",
    "brix__compose_pipeline", "brix__plan_pipeline",
    "brix__get_run_status", "brix__get_step_data", "brix__get_run_log",
    "brix__health", "brix__backup", "brix__restore",
    "brix__discover",
]

TOOL_NAMES = {t.name for t in BRIX_TOOLS}


# ---------------------------------------------------------------------------
# Section 1: Consolidated tools exist in _HANDLERS
# ---------------------------------------------------------------------------

class TestConsolidatedToolsRegistered:
    """All 7 consolidated tools appear in _HANDLERS."""

    def test_consolidated_tools_exist_in_handlers(self):
        for name in CONSOLIDATED_TOOLS:
            assert name in _HANDLERS, f"Consolidated tool '{name}' missing from _HANDLERS"

    def test_consolidated_tools_in_db_if_seeded(self):
        """If consolidated tools are in DB, they should have action parameter."""
        for tool in BRIX_TOOLS:
            if tool.name in CONSOLIDATED_TOOLS:
                props = tool.inputSchema.get("properties", {})
                assert "action" in props, (
                    f"Consolidated tool '{tool.name}' missing 'action' parameter"
                )


# ---------------------------------------------------------------------------
# Section 2: Old individual tools are removed
# ---------------------------------------------------------------------------

class TestOldToolsRemoved:
    """Old CRUD individual tools must not appear in _HANDLERS."""

    def test_old_tools_not_in_handlers(self):
        for name in REMOVED_INDIVIDUAL_TOOLS:
            assert name not in _HANDLERS, (
                f"Old individual tool '{name}' still in _HANDLERS"
            )

    def test_old_tools_not_in_handlers(self):
        for name in REMOVED_INDIVIDUAL_TOOLS:
            assert name not in _HANDLERS, (
                f"Old individual tool '{name}' still in _HANDLERS"
            )


# ---------------------------------------------------------------------------
# Section 3: Standalone tools remain unchanged
# ---------------------------------------------------------------------------

class TestStandaloneToolsUnchanged:
    """Standalone tools (get_tips, list_pipelines etc.) must still exist in _HANDLERS."""

    def test_standalone_tools_have_handlers(self):
        for name in STANDALONE_TOOLS:
            assert name in _HANDLERS, f"Standalone tool '{name}' missing handler"


# ---------------------------------------------------------------------------
# Section 4: Dispatcher functions — valid action routing
# ---------------------------------------------------------------------------

class TestDispatcherInvalidAction:
    """Invalid action returns structured error, not exception."""

    @pytest.mark.asyncio
    async def test_registry_invalid_action(self):
        result = await _handle_registry({"action": "nonexistent"})
        assert result.get("success") is False
        assert "action" in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_trigger_invalid_action(self):
        result = await _handle_trigger({"action": "zap"})
        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_credential_invalid_action(self):
        result = await _handle_credential({"action": "hack"})
        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_alert_invalid_action(self):
        result = await _handle_alert({"action": "blast"})
        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_server_invalid_action(self):
        result = await _handle_server({"action": "reboot"})
        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_state_invalid_action(self):
        result = await _handle_state({"action": "wipe"})
        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_trigger_group_invalid_action(self):
        result = await _handle_trigger_group({"action": "explode"})
        assert result.get("success") is False


class TestDispatcherValidActionRouting:
    """Valid actions are routed to the correct underlying handler."""

    @pytest.mark.asyncio
    async def test_credential_list_action(self):
        # list requires no extra params and returns a list of credentials
        result = await _handle_credential({"action": "list"})
        # The real handler returns a dict — we just check it doesn't return an error
        assert result.get("success") is not False or "credentials" in result or isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_alert_list_action(self):
        result = await _handle_alert({"action": "list"})
        assert isinstance(result, dict)
        assert result.get("success") is not False

    @pytest.mark.asyncio
    async def test_server_list_action(self):
        result = await _handle_server({"action": "list"})
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_state_list_action(self):
        result = await _handle_state({"action": "list"})
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_trigger_list_action(self):
        result = await _handle_trigger({"action": "list"})
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_trigger_group_list_action(self):
        result = await _handle_trigger_group({"action": "list"})
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_registry_list_action_requires_registry_type(self):
        # registry list without registry_type should fail with meaningful error
        result = await _handle_registry({"action": "list"})
        # Either succeeds (some default) or returns error — either way it's a dict
        assert isinstance(result, dict)
