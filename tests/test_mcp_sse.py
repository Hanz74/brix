"""Tests for SSE transport support in McpRunner and McpConnectionPool.

These tests do NOT connect to real MCP servers.  They verify:
- ServerConfig accepts transport=sse and transport=stdio (default).
- McpRunner uses sse_client when transport=sse.
- McpRunner uses stdio_client when transport=stdio (existing behaviour unchanged).
- McpRunner returns a clear error when SSE server has no url.
- McpConnectionPool._connect uses SseServerParameters for SSE servers.
- McpConnectionPool._connect raises ValueError when SSE server has no url.
"""

import pytest
import yaml
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import timedelta

from brix.models import ServerConfig
from brix.runners.mcp import McpRunner, load_server_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_servers_yaml(tmp_path: Path, servers: dict) -> Path:
    config_path = tmp_path / "servers.yaml"
    config_path.write_text(yaml.dump({"servers": servers}))
    return config_path


class _Step:
    """Minimal step stand-in."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# ServerConfig field tests
# ---------------------------------------------------------------------------


def test_server_config_default_transport():
    """ServerConfig defaults to stdio transport."""
    sc = ServerConfig(name="my-server", command="node")
    assert sc.transport == "stdio"
    assert sc.url == ""


def test_server_config_sse_transport():
    """ServerConfig accepts transport=sse and stores url."""
    sc = ServerConfig(
        name="my-sse-server",
        transport="sse",
        url="http://localhost:8080/sse",
    )
    assert sc.transport == "sse"
    assert sc.url == "http://localhost:8080/sse"
    # command is empty for SSE servers
    assert sc.command == ""


def test_server_config_stdio_with_url():
    """ServerConfig allows url on stdio servers (ignored by runner)."""
    sc = ServerConfig(name="s", command="python3", transport="stdio", url="http://x")
    assert sc.transport == "stdio"
    assert sc.url == "http://x"


def test_load_server_config_sse(tmp_path: Path):
    """load_server_config loads SSE server entries correctly."""
    config_path = _write_servers_yaml(
        tmp_path,
        {
            "my-sse": {
                "transport": "sse",
                "url": "http://127.0.0.1:9000/sse",
            }
        },
    )
    sc = load_server_config("my-sse", config_path)
    assert sc.transport == "sse"
    assert sc.url == "http://127.0.0.1:9000/sse"
    assert sc.command == ""


# ---------------------------------------------------------------------------
# McpRunner — SSE path (mocked)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_runner_uses_sse_client_for_sse_server(tmp_path: Path):
    """McpRunner calls sse_client (not stdio_client) when transport=sse."""
    config_path = _write_servers_yaml(
        tmp_path,
        {"sse-srv": {"transport": "sse", "url": "http://localhost:8099/sse"}},
    )

    runner = McpRunner(servers_config_path=config_path)
    step = _Step(server="sse-srv", tool="my_tool", params={}, timeout=None)

    # Build a fake tool result
    fake_content = MagicMock()
    fake_content.text = '{"ok": true}'
    fake_result = MagicMock()
    fake_result.isError = False
    fake_result.structuredContent = None
    fake_result.content = [fake_content]

    fake_session = AsyncMock()
    fake_session.initialize = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=fake_result)
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    fake_streams = (MagicMock(), MagicMock())
    fake_sse_ctx = AsyncMock()
    fake_sse_ctx.__aenter__ = AsyncMock(return_value=fake_streams)
    fake_sse_ctx.__aexit__ = AsyncMock(return_value=False)

    with patch("brix.runners.mcp.ClientSession", return_value=fake_session), \
         patch("brix.runners.mcp.sse_client", return_value=fake_sse_ctx) as mock_sse, \
         patch("brix.runners.mcp.stdio_client") as mock_stdio, \
         patch("brix.runners.mcp._MCP_AVAILABLE", True):
        result = await runner.execute(step, context=None)

    assert result["success"] is True
    mock_sse.assert_called_once_with(url="http://localhost:8099/sse")
    mock_stdio.assert_not_called()


@pytest.mark.asyncio
async def test_mcp_runner_uses_stdio_client_for_stdio_server(tmp_path: Path):
    """McpRunner calls stdio_client (not sse_client) when transport=stdio."""
    config_path = _write_servers_yaml(
        tmp_path,
        {"stdio-srv": {"command": "python3", "args": ["-m", "myserver"]}},
    )

    runner = McpRunner(servers_config_path=config_path)
    step = _Step(server="stdio-srv", tool="ping", params={}, timeout=None)

    fake_content = MagicMock()
    fake_content.text = "pong"
    fake_result = MagicMock()
    fake_result.isError = False
    fake_result.structuredContent = None
    fake_result.content = [fake_content]

    fake_session = AsyncMock()
    fake_session.initialize = AsyncMock()
    fake_session.call_tool = AsyncMock(return_value=fake_result)
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    fake_streams = (MagicMock(), MagicMock())
    fake_stdio_ctx = AsyncMock()
    fake_stdio_ctx.__aenter__ = AsyncMock(return_value=fake_streams)
    fake_stdio_ctx.__aexit__ = AsyncMock(return_value=False)

    with patch("brix.runners.mcp.ClientSession", return_value=fake_session), \
         patch("brix.runners.mcp.stdio_client", return_value=fake_stdio_ctx) as mock_stdio, \
         patch("brix.runners.mcp.sse_client") as mock_sse, \
         patch("brix.runners.mcp._MCP_AVAILABLE", True):
        result = await runner.execute(step, context=None)

    assert result["success"] is True
    mock_stdio.assert_called_once()
    mock_sse.assert_not_called()


@pytest.mark.asyncio
async def test_mcp_runner_sse_missing_url_returns_error(tmp_path: Path):
    """McpRunner returns error when SSE server has no url configured."""
    config_path = _write_servers_yaml(
        tmp_path,
        {"sse-nourl": {"transport": "sse", "url": ""}},
    )

    runner = McpRunner(servers_config_path=config_path)
    step = _Step(server="sse-nourl", tool="anything", params={}, timeout=None)

    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "url" in result["error"].lower() or "sse" in result["error"].lower()


# ---------------------------------------------------------------------------
# McpConnectionPool — SSE path (mocked)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_connect_uses_sse_params_for_sse_server(tmp_path: Path):
    """McpConnectionPool._connect passes SseServerParameters when transport=sse."""
    from brix.mcp_pool import McpConnectionPool

    config_path = _write_servers_yaml(
        tmp_path,
        {"sse-pool": {"transport": "sse", "url": "http://sse-host:7777/sse"}},
    )

    pool = McpConnectionPool(config_path=config_path)

    fake_session = AsyncMock()

    mock_group = AsyncMock()
    mock_group.connect_to_server = AsyncMock(return_value=fake_session)
    pool._group = mock_group

    from mcp.client.session_group import SseServerParameters

    session = await pool._connect("sse-pool")

    assert session is fake_session
    mock_group.connect_to_server.assert_called_once()
    passed_params = mock_group.connect_to_server.call_args[0][0]
    assert isinstance(passed_params, SseServerParameters)
    assert passed_params.url == "http://sse-host:7777/sse"


@pytest.mark.asyncio
async def test_pool_connect_uses_stdio_params_for_stdio_server(tmp_path: Path):
    """McpConnectionPool._connect passes StdioServerParameters when transport=stdio."""
    from brix.mcp_pool import McpConnectionPool
    from mcp import StdioServerParameters

    config_path = _write_servers_yaml(
        tmp_path,
        {"stdio-pool": {"command": "node", "args": ["/app.js"]}},
    )

    pool = McpConnectionPool(config_path=config_path)

    fake_session = AsyncMock()
    mock_group = AsyncMock()
    mock_group.connect_to_server = AsyncMock(return_value=fake_session)
    pool._group = mock_group

    session = await pool._connect("stdio-pool")

    assert session is fake_session
    passed_params = mock_group.connect_to_server.call_args[0][0]
    assert isinstance(passed_params, StdioServerParameters)
    assert passed_params.command == "node"


@pytest.mark.asyncio
async def test_pool_connect_sse_missing_url_raises(tmp_path: Path):
    """McpConnectionPool._connect raises ValueError when SSE url is empty."""
    from brix.mcp_pool import McpConnectionPool

    config_path = _write_servers_yaml(
        tmp_path,
        {"sse-bad": {"transport": "sse", "url": ""}},
    )

    pool = McpConnectionPool(config_path=config_path)
    pool._group = AsyncMock()

    with pytest.raises(ValueError, match="url"):
        await pool._connect("sse-bad")
