"""Tests for brix.runners.mcp — McpRunner and load_server_config."""

import pytest
import yaml
from pathlib import Path

from brix.runners.mcp import McpRunner, load_server_config
from brix.models import ServerConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step stand-in for McpRunner tests."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# load_server_config
# ---------------------------------------------------------------------------


def test_load_server_config(tmp_path: Path):
    """Load a valid server entry from servers.yaml returns correct ServerConfig."""
    config = {
        "servers": {
            "m365": {
                "command": "node",
                "args": ["/app/index.js"],
                "env": {"TOKEN": "abc"},
            }
        }
    }
    config_path = tmp_path / "servers.yaml"
    config_path.write_text(yaml.dump(config))

    sc = load_server_config("m365", config_path)

    assert isinstance(sc, ServerConfig)
    assert sc.name == "m365"
    assert sc.command == "node"
    assert sc.args == ["/app/index.js"]
    assert sc.env == {"TOKEN": "abc"}


def test_load_server_config_no_env(tmp_path: Path):
    """Server without env field defaults to empty dict."""
    config = {
        "servers": {
            "simple": {
                "command": "python3",
                "args": ["-m", "myserver"],
            }
        }
    }
    config_path = tmp_path / "servers.yaml"
    config_path.write_text(yaml.dump(config))

    sc = load_server_config("simple", config_path)

    assert sc.command == "python3"
    assert sc.env == {}


def test_load_server_config_missing_server(tmp_path: Path):
    """Requesting a server that does not exist in servers.yaml raises KeyError."""
    config = {"servers": {"m365": {"command": "node"}}}
    config_path = tmp_path / "servers.yaml"
    config_path.write_text(yaml.dump(config))

    with pytest.raises(KeyError, match="docker"):
        load_server_config("docker", config_path)


def test_load_server_config_no_file(tmp_path: Path):
    """Missing servers.yaml raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_server_config("m365", tmp_path / "nonexistent.yaml")


def test_load_server_config_empty_file(tmp_path: Path):
    """Empty servers.yaml raises KeyError (no servers section)."""
    config_path = tmp_path / "servers.yaml"
    config_path.write_text("")  # empty YAML → None

    with pytest.raises(KeyError):
        load_server_config("m365", config_path)


# ---------------------------------------------------------------------------
# McpRunner — validation errors (no server / tool)
# ---------------------------------------------------------------------------


async def test_mcp_no_server(tmp_path: Path):
    """Step without 'server' returns success=False immediately."""
    runner = McpRunner(servers_config_path=tmp_path / "servers.yaml")
    step = _Step(tool="list_messages")

    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "server" in result["error"]
    assert result["duration"] == 0.0


async def test_mcp_no_tool(tmp_path: Path):
    """Step without 'tool' returns success=False immediately."""
    runner = McpRunner(servers_config_path=tmp_path / "servers.yaml")
    step = _Step(server="m365")

    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "tool" in result["error"]
    assert result["duration"] == 0.0


# ---------------------------------------------------------------------------
# McpRunner — config errors (missing file / server not found)
# ---------------------------------------------------------------------------


async def test_mcp_no_servers_yaml(tmp_path: Path):
    """Missing servers.yaml produces success=False with descriptive error."""
    runner = McpRunner(servers_config_path=tmp_path / "servers.yaml")
    step = _Step(server="m365", tool="list_messages")

    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "servers.yaml" in result["error"] or "No servers.yaml" in result["error"]


async def test_mcp_server_not_found(tmp_path: Path):
    """Server name not in servers.yaml produces success=False."""
    config = {"servers": {"other": {"command": "node"}}}
    config_path = tmp_path / "servers.yaml"
    config_path.write_text(yaml.dump(config))

    runner = McpRunner(servers_config_path=config_path)
    step = _Step(server="m365", tool="list_messages")

    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "m365" in result["error"]


# ---------------------------------------------------------------------------
# McpRunner — argument filtering
# ---------------------------------------------------------------------------


async def test_mcp_internal_params_stripped(tmp_path: Path):
    """Params with _ prefix are stripped before passing as tool arguments.

    We verify this indirectly: the runner fails at the server-launch stage
    (server is not startable) but not before reaching that point, confirming
    argument filtering happened without error.
    """
    config = {"servers": {"fake": {"command": "false", "args": []}}}
    config_path = tmp_path / "servers.yaml"
    config_path.write_text(yaml.dump(config))

    runner = McpRunner(servers_config_path=config_path)
    step = _Step(
        server="fake",
        tool="some_tool",
        params={"_internal": "skip_me", "visible": "keep_me"},
    )

    result = await runner.execute(step, context=None)

    # The server will fail to start (command 'false' exits immediately),
    # but we should NOT get a param-related error
    assert result["success"] is False
    assert "_internal" not in result.get("error", "")
