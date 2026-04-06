from pathlib import Path
from unittest.mock import AsyncMock

from brix.cache import SchemaCache
from brix.runners.mcp import McpRunner


class _Step:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _make_cache_with_schema(tmp_path: Path, server_name: str, tool_name: str, properties: dict) -> SchemaCache:
    cache = SchemaCache(cache_dir=tmp_path / "cache")
    cache.save_tools(
        server_name,
        [
            {
                "name": tool_name,
                "inputSchema": {
                    "type": "object",
                    "properties": properties,
                },
            }
        ],
    )
    return cache


async def test_dict_value_stays_native_when_schema_says_object(tmp_path: Path):
    payload = {"query": "status:open"}
    cache = _make_cache_with_schema(
        tmp_path, "fake", "my_tool", {"params": {"type": "object"}}
    )
    pool = AsyncMock()
    pool.call_tool = AsyncMock(return_value={"success": True, "data": {}, "duration": 0.01})
    runner = McpRunner(schema_cache=cache, pool=pool)

    result = await runner.execute(
        _Step(server="fake", tool="my_tool", params={"params": payload}),
        context=None,
    )

    assert result["success"] is True
    pool.call_tool.assert_called_once()
    assert pool.call_tool.call_args.args[2]["params"] == payload
    assert isinstance(pool.call_tool.call_args.args[2]["params"], dict)


async def test_list_value_stays_native_when_schema_says_array(tmp_path: Path):
    payload = ["a", "b", "c"]
    cache = _make_cache_with_schema(
        tmp_path, "fake", "my_tool", {"items": {"type": "array"}}
    )
    pool = AsyncMock()
    pool.call_tool = AsyncMock(return_value={"success": True, "data": {}, "duration": 0.01})
    runner = McpRunner(schema_cache=cache, pool=pool)

    result = await runner.execute(
        _Step(server="fake", tool="my_tool", params={"items": payload}),
        context=None,
    )

    assert result["success"] is True
    pool.call_tool.assert_called_once()
    assert pool.call_tool.call_args.args[2]["items"] == payload
    assert isinstance(pool.call_tool.call_args.args[2]["items"], list)


async def test_dict_value_serialized_to_string_when_schema_says_string(tmp_path: Path):
    payload = {"query": "status:open"}
    cache = _make_cache_with_schema(
        tmp_path, "fake", "my_tool", {"params": {"type": "string"}}
    )
    pool = AsyncMock()
    pool.call_tool = AsyncMock(return_value={"success": True, "data": {}, "duration": 0.01})
    runner = McpRunner(schema_cache=cache, pool=pool)

    result = await runner.execute(
        _Step(server="fake", tool="my_tool", params={"params": payload}),
        context=None,
    )

    assert result["success"] is True
    pool.call_tool.assert_called_once()
    assert pool.call_tool.call_args.args[2]["params"] == '{"query": "status:open"}'
    assert isinstance(pool.call_tool.call_args.args[2]["params"], str)


async def test_dict_value_serialized_to_string_when_no_schema_available(tmp_path: Path):
    payload = {"query": "status:open"}
    cache = SchemaCache(cache_dir=tmp_path / "empty_cache")
    pool = AsyncMock()
    pool.call_tool = AsyncMock(return_value={"success": True, "data": {}, "duration": 0.01})
    runner = McpRunner(schema_cache=cache, pool=pool)

    result = await runner.execute(
        _Step(server="fake", tool="my_tool", params={"params": payload}),
        context=None,
    )

    assert result["success"] is True
    pool.call_tool.assert_called_once()
    assert pool.call_tool.call_args.args[2]["params"] == '{"query": "status:open"}'
    assert isinstance(pool.call_tool.call_args.args[2]["params"], str)
