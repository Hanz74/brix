"""Tests for MCP runner unwrap_json feature.

Covers _unwrap_json_strings helper and McpRunner.execute integration
with the unwrap_json ServerConfig flag.
"""

import json
import pytest
import yaml
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from brix.runners.mcp import McpRunner, _unwrap_json_strings
from brix.models import ServerConfig


# ---------------------------------------------------------------------------
# Unit tests for _unwrap_json_strings
# ---------------------------------------------------------------------------


def test_unwrap_single_level():
    """A dict with one JSON-encoded string value is unwrapped one level."""
    inner = {"id": 1, "name": "Alice"}
    obj = {"result": json.dumps(inner)}
    result = _unwrap_json_strings(obj)
    assert result == {"result": inner}


def test_unwrap_double_wrapped():
    """Doubly-nested JSON strings (Cody pattern) are fully resolved."""
    innermost = {"tasks": [1, 2, 3]}
    middle = {"result": json.dumps(innermost)}
    outer = {"result": json.dumps(middle)}
    result = _unwrap_json_strings(outer)
    # After two levels of unwrapping the innermost dict should be present
    assert result["result"]["result"] == innermost


def test_unwrap_non_json_strings_unchanged():
    """Plain string values that are not valid JSON remain unchanged."""
    obj = {"message": "hello world", "status": "ok"}
    result = _unwrap_json_strings(obj)
    assert result == obj


def test_unwrap_non_string_values_unchanged():
    """Non-string values (int, list, bool, None) are passed through untouched."""
    obj = {"count": 42, "items": [1, 2], "active": True, "nothing": None}
    result = _unwrap_json_strings(obj)
    assert result == obj


def test_unwrap_json_list_value():
    """A JSON-encoded list string is unwrapped to a Python list."""
    obj = {"ids": json.dumps([10, 20, 30])}
    result = _unwrap_json_strings(obj)
    assert result == {"ids": [10, 20, 30]}


def test_unwrap_max_depth_prevents_infinite_recursion():
    """max_depth=1 stops recursion after one level."""
    innermost = {"deep": "value"}
    middle = {"nested": json.dumps(innermost)}
    outer = {"result": json.dumps(middle)}

    # With max_depth=1 the outer string is unwrapped but the inner string stays
    result = _unwrap_json_strings(outer, max_depth=1)
    assert isinstance(result["result"], dict)
    # The inner key "nested" should still be a JSON string (not recursed into)
    assert isinstance(result["result"]["nested"], str)


def test_unwrap_max_depth_zero_returns_unchanged():
    """max_depth=0 returns the dict unchanged (base case)."""
    obj = {"result": json.dumps({"x": 1})}
    result = _unwrap_json_strings(obj, max_depth=0)
    # At depth 0 the function returns the object as-is
    assert result is obj


def test_unwrap_non_dict_input_returned_as_is():
    """Non-dict inputs are returned unchanged (guards against wrong type)."""
    assert _unwrap_json_strings("a string") == "a string"
    assert _unwrap_json_strings([1, 2, 3]) == [1, 2, 3]
    assert _unwrap_json_strings(None) is None


def test_unwrap_scalar_json_string_stays_as_string():
    """JSON scalars encoded as strings (e.g. '"hello"', '42') are NOT unwrapped."""
    obj = {"label": '"quoted"', "number": "99"}
    result = _unwrap_json_strings(obj)
    # json.loads('"quoted"') = 'quoted' (str) → not a dict/list → stays unchanged
    # json.loads('99') = 99 (int) → not a dict/list → stays unchanged
    assert result == obj


# ---------------------------------------------------------------------------
# Integration: McpRunner.execute respects unwrap_json flag
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step stand-in."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _write_servers_yaml(tmp_path: Path, unwrap_json: bool = False) -> Path:
    config = {
        "servers": {
            "testserver": {
                "command": "echo",
                "args": [],
                "unwrap_json": unwrap_json,
            }
        }
    }
    path = tmp_path / "servers.yaml"
    path.write_text(yaml.dump(config))
    return path


async def test_execute_unwrap_json_false_leaves_data_untouched(tmp_path: Path):
    """When unwrap_json=False the raw parsed JSON dict is returned as-is."""
    config_path = _write_servers_yaml(tmp_path, unwrap_json=False)

    inner = {"value": 42}
    raw_response = json.dumps({"result": json.dumps(inner)})

    content_block = MagicMock()
    content_block.text = raw_response
    mock_result = MagicMock()
    mock_result.isError = False
    mock_result.structuredContent = None
    mock_result.content = [content_block]

    mock_session = AsyncMock()
    mock_session.call_tool = AsyncMock(return_value=mock_result)
    mock_session.initialize = AsyncMock()

    runner = McpRunner(servers_config_path=config_path)
    step = _Step(server="testserver", tool="get_data", params={})

    with patch("brix.runners.mcp.stdio_client") as mock_ctx:
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with patch("brix.runners.mcp.ClientSession") as mock_session_cls:
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await runner.execute(step, context=None)

    assert result["success"] is True
    # Without unwrap_json the nested JSON string stays as a string
    assert isinstance(result["data"]["result"], str)


async def test_execute_unwrap_json_true_resolves_nested_strings(tmp_path: Path):
    """When unwrap_json=True nested JSON strings in the response are resolved."""
    config_path = _write_servers_yaml(tmp_path, unwrap_json=True)

    inner = {"value": 42}
    raw_response = json.dumps({"result": json.dumps(inner)})

    content_block = MagicMock()
    content_block.text = raw_response
    mock_result = MagicMock()
    mock_result.isError = False
    mock_result.structuredContent = None
    mock_result.content = [content_block]

    mock_session = AsyncMock()
    mock_session.call_tool = AsyncMock(return_value=mock_result)
    mock_session.initialize = AsyncMock()

    runner = McpRunner(servers_config_path=config_path)
    step = _Step(server="testserver", tool="get_data", params={})

    with patch("brix.runners.mcp.stdio_client") as mock_ctx:
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with patch("brix.runners.mcp.ClientSession") as mock_session_cls:
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await runner.execute(step, context=None)

    assert result["success"] is True
    # With unwrap_json the nested JSON string is resolved to the inner dict
    assert result["data"]["result"] == inner
