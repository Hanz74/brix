"""Tests for brix.mcp_pool — McpConnectionPool.

These tests do NOT connect to real MCP servers.  They verify:
- Pool construction and initial state.
- Error paths that don't require a live connection.
- close_all / __aexit__ do not crash when pool is empty or not entered.
- Timeout propagation and McpError handling (INBOX-264).
"""

import pytest
import yaml
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

from mcp import McpError
from mcp.shared.exceptions import ErrorData

from brix.mcp_pool import McpConnectionPool, DEFAULT_POOL_CALL_TIMEOUT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_servers_yaml(tmp_path: Path, servers: dict) -> Path:
    """Write a servers.yaml with the given server entries and return the path."""
    config_path = tmp_path / "servers.yaml"
    config_path.write_text(yaml.dump({"servers": servers}))
    return config_path


# ---------------------------------------------------------------------------
# test_pool_init
# ---------------------------------------------------------------------------


def test_pool_init(tmp_path: Path):
    """Pool can be instantiated with a custom config path."""
    config_path = tmp_path / "servers.yaml"
    pool = McpConnectionPool(config_path=config_path)

    assert pool._config_path == config_path
    assert pool._group is None


def test_pool_init_default_config():
    """Pool instantiated without arguments uses SERVERS_CONFIG_PATH."""
    from brix.runners.mcp import SERVERS_CONFIG_PATH

    pool = McpConnectionPool()
    assert pool._config_path == SERVERS_CONFIG_PATH


# ---------------------------------------------------------------------------
# test_pool_connected_servers_empty
# ---------------------------------------------------------------------------


def test_pool_connected_servers_empty(tmp_path: Path):
    """Freshly created pool has no connected servers."""
    pool = McpConnectionPool(config_path=tmp_path / "servers.yaml")
    assert pool.connected_servers == []


# ---------------------------------------------------------------------------
# test_pool_call_tool_no_config — server not configured → error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_call_tool_no_servers_yaml(tmp_path: Path):
    """call_tool returns success=False when servers.yaml does not exist."""
    pool = McpConnectionPool(config_path=tmp_path / "nonexistent.yaml")

    # Provide a minimal fake group so the pool considers itself "entered"
    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("m365", "list-mail-messages", {})

    assert result["success"] is False
    assert "servers.yaml" in result["error"] or "No servers.yaml" in result["error"]
    assert "duration" in result


@pytest.mark.asyncio
async def test_pool_call_tool_server_not_in_config(tmp_path: Path):
    """call_tool returns success=False when server name is not in servers.yaml."""
    config_path = _write_servers_yaml(tmp_path, {"other": {"command": "node"}})
    pool = McpConnectionPool(config_path=config_path)

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("m365", "list-mail-messages", {})

    assert result["success"] is False
    assert "m365" in result["error"]
    assert "duration" in result


# ---------------------------------------------------------------------------
# test_pool_close_all — close_all without crash
# ---------------------------------------------------------------------------


def test_pool_close_all_never_entered():
    """close_all on a pool that was never entered does not raise."""
    pool = McpConnectionPool()
    import asyncio

    # Should not raise
    asyncio.get_event_loop().run_until_complete(pool.close_all())
    assert pool.connected_servers == []


@pytest.mark.asyncio
async def test_pool_close_all_after_enter(tmp_path: Path):
    """close_all after entering the pool closes the group and clears state."""
    pool = McpConnectionPool(config_path=tmp_path / "servers.yaml")

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        await pool.__aenter__()
        assert pool._group is not None

        await pool.close_all()

    assert pool._group is None
    assert pool.connected_servers == []
    fake_group.__aexit__.assert_called_once()


# ---------------------------------------------------------------------------
# test_pool_get_session_without_context_manager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_get_session_without_context_manager(tmp_path: Path):
    """get_session raises RuntimeError when pool was not entered."""
    pool = McpConnectionPool(config_path=tmp_path / "servers.yaml")

    with pytest.raises(RuntimeError, match="async context manager"):
        await pool.get_session("m365")


@pytest.mark.asyncio
async def test_pool_call_tool_without_context_manager(tmp_path: Path):
    """call_tool returns success=False when pool was not entered."""
    pool = McpConnectionPool(config_path=tmp_path / "servers.yaml")

    result = await pool.call_tool("m365", "list-mail-messages", {})

    assert result["success"] is False
    assert "context manager" in result["error"]
    assert result["duration"] == 0.0


# ---------------------------------------------------------------------------
# test_pool_caches_session — same session returned on second call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_caches_session(tmp_path: Path):
    """A second call to get_session for the same server returns the same object."""
    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    fake_session = MagicMock(name="session")
    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            session_a = await pool.get_session("fake")
            session_b = await pool.get_session("fake")

    assert session_a is session_b
    # connect_to_server must only be called once
    assert fake_group.connect_to_server.call_count == 1


# ---------------------------------------------------------------------------
# test_pool_call_tool_success — happy path with mocked session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_call_tool_success(tmp_path: Path):
    """call_tool returns success=True with parsed JSON data on a happy path."""
    import json

    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    # Simulate a successful tool result with text content
    text_block = MagicMock()
    text_block.text = json.dumps({"items": [1, 2, 3]})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("fake", "my_tool", {"arg": "val"})

    assert result["success"] is True
    assert result["data"] == {"items": [1, 2, 3]}
    assert result["duration"] >= 0.0


@pytest.mark.asyncio
async def test_pool_call_tool_tool_error(tmp_path: Path):
    """call_tool returns success=False when result.isError is True."""
    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = "something went wrong"
    tool_result = MagicMock()
    tool_result.isError = True
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("fake", "my_tool", {})

    assert result["success"] is False
    assert "something went wrong" in result["error"]


@pytest.mark.asyncio
async def test_pool_call_tool_evicts_on_connection_error(tmp_path: Path):
    """On unexpected exception during call_tool, the session is evicted."""
    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=RuntimeError("connection lost"))

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("fake", "my_tool", {})
            # Session must be evicted after failure
            assert "fake" not in pool.connected_servers

    assert result["success"] is False
    assert "connection lost" in result["error"]


@pytest.mark.asyncio
async def test_pool_call_tool_structured_content(tmp_path: Path):
    """call_tool prefers structuredContent over text when available."""
    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = {"key": "value"}
    tool_result.content = []

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("fake", "my_tool", {})

    assert result["success"] is True
    assert result["data"] == {"key": "value"}


# ---------------------------------------------------------------------------
# Connection pooling integration (T-BRIX-V3-01)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_reuses_session_across_multiple_calls(tmp_path: Path):
    """Multiple call_tool calls to the same server reuse a single session."""
    import json

    config_path = _write_servers_yaml(
        tmp_path, {"svc": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = json.dumps({"ok": True})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("svc", "tool_a", {})
            await pool.call_tool("svc", "tool_b", {})
            await pool.call_tool("svc", "tool_a", {"x": 1})

    # Session was established only once despite three calls
    assert fake_group.connect_to_server.call_count == 1
    # But call_tool on the session was invoked three times
    assert fake_session.call_tool.call_count == 3


@pytest.mark.asyncio
async def test_pool_separate_sessions_per_server(tmp_path: Path):
    """Different server names get separate sessions."""
    import json

    config_path = _write_servers_yaml(
        tmp_path,
        {
            "svc_a": {"command": "echo", "args": ["a"]},
            "svc_b": {"command": "echo", "args": ["b"]},
        },
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = json.dumps({"ok": True})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    session_a = AsyncMock()
    session_a.call_tool = AsyncMock(return_value=tool_result)
    session_b = AsyncMock()
    session_b.call_tool = AsyncMock(return_value=tool_result)

    # Return different session objects for each connect call
    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(side_effect=[session_a, session_b])

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("svc_a", "tool", {})
            await pool.call_tool("svc_b", "tool", {})
            await pool.call_tool("svc_a", "tool", {})  # second call — reuses session_a

            # Both servers must be connected while pool is still open
            assert "svc_a" in pool.connected_servers
            assert "svc_b" in pool.connected_servers

    # Two distinct connections were opened
    assert fake_group.connect_to_server.call_count == 2

    # svc_a session called twice, svc_b once
    assert session_a.call_tool.call_count == 2
    assert session_b.call_tool.call_count == 1


@pytest.mark.asyncio
async def test_pool_context_manager_clears_state(tmp_path: Path):
    """After exiting the context manager the pool state is fully reset."""
    config_path = _write_servers_yaml(
        tmp_path, {"svc": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    fake_session = AsyncMock()
    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            # Manually inject a fake session to simulate a previous connection
            pool._server_sessions["svc"] = fake_session

    # After __aexit__ all state must be cleared
    assert pool._group is None
    assert pool.connected_servers == []
    assert pool._locks == {}
    fake_group.__aexit__.assert_called_once()


# ---------------------------------------------------------------------------
# INBOX-264: Timeout propagation and McpError handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_call_tool_passes_timeout_to_sdk(tmp_path: Path):
    """call_tool forwards read_timeout_seconds to session.call_tool (INBOX-264).

    Without this, a stalled MCP server (e.g., stuck on schema validation)
    would cause the run to hang indefinitely.
    """
    import json

    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = json.dumps({"ok": True})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("fake", "my_tool", {"x": 1}, timeout=30.0)

    assert result["success"] is True
    # Verify the SDK call_tool received read_timeout_seconds
    fake_session.call_tool.assert_called_once_with(
        "my_tool",
        {"x": 1},
        read_timeout_seconds=timedelta(seconds=30.0),
    )


@pytest.mark.asyncio
async def test_pool_call_tool_uses_default_timeout(tmp_path: Path):
    """call_tool uses DEFAULT_POOL_CALL_TIMEOUT when no explicit timeout given."""
    import json

    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = json.dumps({"ok": True})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("fake", "my_tool", {})

    fake_session.call_tool.assert_called_once_with(
        "my_tool",
        {},
        read_timeout_seconds=timedelta(seconds=DEFAULT_POOL_CALL_TIMEOUT),
    )


@pytest.mark.asyncio
async def test_pool_call_tool_mcp_error_no_session_eviction(tmp_path: Path):
    """McpError (e.g. schema validation failure) does NOT evict the session.

    INBOX-264: the session is still valid after a protocol-level error —
    only transport/connection errors should trigger eviction.
    """
    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    error_data = ErrorData(code=-32602, message="Invalid params: 'items' must be array")
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=McpError(error_data))

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("fake", "my_tool", {"items": "bad"})
            # Session must NOT be evicted — it's a protocol error, not a broken pipe
            assert "fake" in pool.connected_servers

    assert result["success"] is False
    assert "Invalid params" in result["error"]
    assert "MCP error:" in result["error"]
    assert "duration" in result


@pytest.mark.asyncio
async def test_pool_call_tool_mcp_timeout_error_no_eviction(tmp_path: Path):
    """McpError raised on timeout does NOT evict the session (INBOX-264).

    The SDK raises McpError (not asyncio.TimeoutError) when the per-call
    read_timeout_seconds fires.  The session is still usable afterward.
    """
    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    timeout_error = ErrorData(code=408, message="Timed out while waiting for response")
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=McpError(timeout_error))

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("fake", "slow_tool", {}, timeout=1.0)
            # Session must NOT be evicted on timeout — it may recover
            assert "fake" in pool.connected_servers

    assert result["success"] is False
    assert "Timed out" in result["error"]
    assert result["duration"] >= 0.0


@pytest.mark.asyncio
async def test_pool_call_tool_transport_error_evicts_session(tmp_path: Path):
    """Non-McpError exceptions (transport errors) still evict the session."""
    config_path = _write_servers_yaml(
        tmp_path, {"fake": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=OSError("broken pipe"))

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            result = await pool.call_tool("fake", "my_tool", {})
            # Transport error — session must be evicted
            assert "fake" not in pool.connected_servers

    assert result["success"] is False
    assert "broken pipe" in result["error"]


# ---------------------------------------------------------------------------
# T-BRIX-V6-25: MCP Connection Pool Health Tracking
# ---------------------------------------------------------------------------


def test_pool_get_health_empty_initially(tmp_path: Path):
    """A newly created pool has no health data."""
    pool = McpConnectionPool(config_path=tmp_path / "servers.yaml")
    assert pool.get_health() == {}


@pytest.mark.asyncio
async def test_pool_health_recorded_on_success(tmp_path: Path):
    """get_health() returns stats after a successful call_tool."""
    import json

    config_path = _write_servers_yaml(
        tmp_path, {"svc": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = json.dumps({"ok": True})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)
    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("svc", "tool_a", {})
            await pool.call_tool("svc", "tool_b", {})

            health = pool.get_health()

    assert "svc" in health
    entry = health["svc"]
    assert entry["call_count"] == 2
    assert entry["error_count"] == 0
    assert entry["avg_latency_ms"] >= 0.0
    assert entry["last_contact_at"]  # ISO timestamp is non-empty


@pytest.mark.asyncio
async def test_pool_health_error_count_incremented(tmp_path: Path):
    """error_count increments on tool-level errors (isError=True)."""
    config_path = _write_servers_yaml(
        tmp_path, {"svc": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = "something broke"
    tool_result = MagicMock()
    tool_result.isError = True
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)
    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("svc", "bad_tool", {})

            health = pool.get_health()

    assert health["svc"]["call_count"] == 1
    assert health["svc"]["error_count"] == 1


@pytest.mark.asyncio
async def test_pool_health_error_count_on_mcp_error(tmp_path: Path):
    """error_count increments on McpError (protocol-level errors)."""
    config_path = _write_servers_yaml(
        tmp_path, {"svc": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    from mcp.shared.exceptions import ErrorData
    error_data = ErrorData(code=-32602, message="Bad params")
    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=McpError(error_data))

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("svc", "tool", {})

            health = pool.get_health()

    assert health["svc"]["call_count"] == 1
    assert health["svc"]["error_count"] == 1


@pytest.mark.asyncio
async def test_pool_health_error_count_on_transport_error(tmp_path: Path):
    """error_count increments on transport-level exceptions (OSError etc.)."""
    config_path = _write_servers_yaml(
        tmp_path, {"svc": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(side_effect=OSError("connection reset"))

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("svc", "tool", {})

            health = pool.get_health()

    assert health["svc"]["call_count"] == 1
    assert health["svc"]["error_count"] == 1


@pytest.mark.asyncio
async def test_pool_health_avg_latency_computed(tmp_path: Path):
    """avg_latency_ms is computed across all calls (success + errors)."""
    import json

    config_path = _write_servers_yaml(
        tmp_path, {"svc": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = json.dumps({"ok": True})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)
    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            for _ in range(5):
                await pool.call_tool("svc", "tool", {})

            health = pool.get_health()

    assert health["svc"]["call_count"] == 5
    # avg_latency_ms must be a non-negative float
    assert isinstance(health["svc"]["avg_latency_ms"], float)
    assert health["svc"]["avg_latency_ms"] >= 0.0


@pytest.mark.asyncio
async def test_pool_health_multiple_servers_tracked_independently(tmp_path: Path):
    """Health stats are tracked per server independently."""
    import json

    config_path = _write_servers_yaml(
        tmp_path,
        {
            "alpha": {"command": "echo", "args": []},
            "beta": {"command": "echo", "args": []},
        },
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = json.dumps({"ok": True})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    session_alpha = AsyncMock()
    session_alpha.call_tool = AsyncMock(return_value=tool_result)
    session_beta = AsyncMock()
    session_beta.call_tool = AsyncMock(return_value=tool_result)

    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(side_effect=[session_alpha, session_beta])

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("alpha", "tool", {})
            await pool.call_tool("alpha", "tool", {})
            await pool.call_tool("beta", "tool", {})

            health = pool.get_health()

    assert health["alpha"]["call_count"] == 2
    assert health["beta"]["call_count"] == 1


@pytest.mark.asyncio
async def test_pool_health_cleared_after_aexit(tmp_path: Path):
    """Health data is cleared when the pool exits the context manager."""
    import json

    config_path = _write_servers_yaml(
        tmp_path, {"svc": {"command": "echo", "args": []}}
    )
    pool = McpConnectionPool(config_path=config_path)

    text_block = MagicMock()
    text_block.text = json.dumps({"ok": True})
    tool_result = MagicMock()
    tool_result.isError = False
    tool_result.structuredContent = None
    tool_result.content = [text_block]

    fake_session = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=tool_result)
    fake_group = AsyncMock()
    fake_group.__aenter__ = AsyncMock(return_value=fake_group)
    fake_group.__aexit__ = AsyncMock(return_value=None)
    fake_group.connect_to_server = AsyncMock(return_value=fake_session)

    with patch("brix.mcp_pool.ClientSessionGroup", return_value=fake_group):
        async with pool:
            await pool.call_tool("svc", "tool", {})
            assert "svc" in pool.get_health()

    # After exit, health must be cleared
    assert pool.get_health() == {}
