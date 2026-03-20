"""Tests for brix.mcp_pool — McpConnectionPool.

These tests do NOT connect to real MCP servers.  They verify:
- Pool construction and initial state.
- Error paths that don't require a live connection.
- close_all / __aexit__ do not crash when pool is empty or not entered.
"""

import pytest
import yaml
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from brix.mcp_pool import McpConnectionPool


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
