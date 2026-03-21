"""Tests for brix.runners.mcp — McpRunner and load_server_config."""

import pytest
import yaml
from pathlib import Path

from brix.runners.mcp import McpRunner, load_server_config
from brix.models import ServerConfig
from brix.cache import SchemaCache


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


# ---------------------------------------------------------------------------
# McpRunner — _validate_params_against_schema
# ---------------------------------------------------------------------------


def _make_cache_with_schema(tmp_path: Path, server_name: str, tool_name: str, properties: dict) -> SchemaCache:
    """Create a SchemaCache populated with a single tool schema."""
    cache = SchemaCache(cache_dir=tmp_path / "cache")
    tools = [
        {
            "name": tool_name,
            "inputSchema": {
                "type": "object",
                "properties": properties,
            },
        }
    ]
    cache.save_tools(server_name, tools)
    return cache


def test_validate_params_string_instead_of_array(tmp_path: Path):
    """String param where array is expected produces a type-mismatch warning."""
    cache = _make_cache_with_schema(
        tmp_path, "myserver", "my_tool", {"tags": {"type": "array"}}
    )
    runner = McpRunner(schema_cache=cache)

    warnings = runner._validate_params_against_schema(
        "myserver", "my_tool", {"tags": "not-an-array"}
    )

    assert len(warnings) == 1
    assert "tags" in warnings[0]
    assert "array" in warnings[0]
    assert "str" in warnings[0]


def test_validate_params_correct_types(tmp_path: Path):
    """Correct parameter types produce no warnings."""
    cache = _make_cache_with_schema(
        tmp_path,
        "myserver",
        "my_tool",
        {
            "name": {"type": "string"},
            "count": {"type": "integer"},
            "active": {"type": "boolean"},
            "tags": {"type": "array"},
            "meta": {"type": "object"},
        },
    )
    runner = McpRunner(schema_cache=cache)

    warnings = runner._validate_params_against_schema(
        "myserver",
        "my_tool",
        {
            "name": "hello",
            "count": 3,
            "active": True,
            "tags": ["a", "b"],
            "meta": {"key": "val"},
        },
    )

    assert warnings == []


def test_validate_params_no_cache(tmp_path: Path):
    """When no cached schema exists validation is skipped and returns no warnings."""
    cache = SchemaCache(cache_dir=tmp_path / "empty_cache")
    runner = McpRunner(schema_cache=cache)

    warnings = runner._validate_params_against_schema(
        "unknown_server", "unknown_tool", {"tags": "not-an-array"}
    )

    assert warnings == []


def test_validate_params_unknown_tool(tmp_path: Path):
    """Params for a tool not in cache produce no warnings (graceful skip)."""
    cache = _make_cache_with_schema(
        tmp_path, "myserver", "other_tool", {"tags": {"type": "array"}}
    )
    runner = McpRunner(schema_cache=cache)

    warnings = runner._validate_params_against_schema(
        "myserver", "my_tool", {"tags": "not-an-array"}
    )

    assert warnings == []


def test_validate_params_extra_params_ignored(tmp_path: Path):
    """Extra params not in the schema produce no warnings."""
    cache = _make_cache_with_schema(
        tmp_path, "myserver", "my_tool", {"name": {"type": "string"}}
    )
    runner = McpRunner(schema_cache=cache)

    warnings = runner._validate_params_against_schema(
        "myserver", "my_tool", {"name": "ok", "extra_param": 42}
    )

    assert warnings == []


def test_validate_params_number_accepts_int(tmp_path: Path):
    """JSON 'number' type accepts both int and float values."""
    cache = _make_cache_with_schema(
        tmp_path, "myserver", "my_tool", {"score": {"type": "number"}}
    )
    runner = McpRunner(schema_cache=cache)

    assert runner._validate_params_against_schema("myserver", "my_tool", {"score": 5}) == []
    assert runner._validate_params_against_schema("myserver", "my_tool", {"score": 3.14}) == []


async def test_execute_returns_error_on_type_mismatch(tmp_path: Path):
    """execute() returns success=False with helpful error when params fail type check."""
    cache = _make_cache_with_schema(
        tmp_path, "fake", "my_tool", {"items": {"type": "array"}}
    )
    config = {"servers": {"fake": {"command": "false", "args": []}}}
    config_path = tmp_path / "servers.yaml"
    config_path.write_text(yaml.dump(config))

    runner = McpRunner(servers_config_path=config_path, schema_cache=cache)
    step = _Step(server="fake", tool="my_tool", params={"items": "not-a-list"})

    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "Parameter type mismatch" in result["error"]
    assert "items" in result["error"]
    assert "warnings" in result
    assert len(result["warnings"]) == 1
