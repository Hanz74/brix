# MCP Python SDK Research

**Date:** 2026-03-19
**SDK Version (local):** 1.12.4
**SDK Version (PyPI latest):** 1.26.0

---

## 1. stdio Client API

```python
from mcp import stdio_client, StdioServerParameters, ClientSession

params = StdioServerParameters(
    command="node",
    args=["./m365-mcp/dist/index.js"],
    env={"MY_API_KEY": "secret"},     # merged with default env
    cwd="/path/to/server",
)

async with stdio_client(params) as (read, write):
    async with ClientSession(read, write) as session:
        await session.initialize()
        # ... call tools
```

## 2. Connection Lifecycle

Within the context manager, multiple tool calls are possible. The subprocess runs throughout.

**Shutdown sequence:**
1. stdin closed
2. Wait for graceful exit (2s timeout)
3. If no exit: SIGTERM → SIGKILL

## 3. Built-in Connection Pooling: `ClientSessionGroup`

The SDK already provides multi-server connection pooling:

```python
from mcp.client.session_group import ClientSessionGroup

async with ClientSessionGroup() as group:
    await group.connect_to_server(StdioServerParameters(command="server-a"))
    await group.connect_to_server(StdioServerParameters(command="server-b"))

    # Tools aggregated across all servers — SDK routes automatically
    result = await group.call_tool("list-mail-messages", {...})
```

**This is exactly what Brix needs — no custom connection pooling required.**

Name collision handling:
```python
name_hook = lambda name, server_info: f"{server_info.name}.{name}"
async with ClientSessionGroup(component_name_hook=name_hook) as group:
    ...
```

## 4. Tool Discovery

```python
result = await session.list_tools()
for tool in result.tools:
    print(tool.name)          # str
    print(tool.description)   # str | None
    print(tool.inputSchema)   # dict — JSON Schema
    print(tool.outputSchema)  # dict | None (newer versions)
```

Pagination supported: `await session.list_tools(cursor="next_page")`

## 5. Tool Execution

```python
result = await session.call_tool(
    name="search_web",
    arguments={"query": "MCP Python SDK"},
    read_timeout_seconds=timedelta(seconds=30),
)
```

**CallToolResult structure:**
```python
result.content       # list[TextContent | ImageContent | EmbeddedResource]
result.structuredContent  # dict | None (if outputSchema defined)
result.isError       # bool — True if tool reports error (NOT an exception!)
```

## 6. Error Handling — 3 Levels

| Level | Type | When |
|-------|------|------|
| Protocol error | `McpError` (exception) | Server crash, timeout, invalid params |
| Tool error | `result.isError = True` | Tool reports failure (not an exception!) |
| Subprocess error | `OSError` (exception) | Server binary not found |

```python
try:
    result = await session.call_tool("my_tool", args)
    if result.isError:
        # Tool-level error — must check explicitly
        error_text = result.content[0].text
except McpError as e:
    # Protocol-level error
    print(e.error.code, e.error.message)
except OSError as e:
    # Subprocess couldn't start
    print(f"Server not startable: {e}")
```

**JSON-RPC error codes:**
| Code | Meaning |
|------|---------|
| -32600 | Invalid request |
| -32601 | Method not found |
| -32602 | Invalid params |
| -32603 | Internal error |

## 7. Environment Variables

**Whitelist principle:** Only specific env vars are inherited by default:

```python
# Linux default: HOME, LOGNAME, PATH, SHELL, TERM, USER
# Custom vars must be explicitly passed via env parameter
```

If `env=None`: only whitelist vars. If `env={"KEY": "val"}`: merged with whitelist.

## 8. Async

Everything is async. No sync wrapper. Use `asyncio.run()` for CLI:

```python
asyncio.run(call_mcp_tool("server", "tool_name", args))
```

SDK uses `anyio` internally — compatible with asyncio and trio.

## 9. Brix-Ready Code Template

```python
from contextlib import asynccontextmanager
from datetime import timedelta
from mcp import stdio_client, StdioServerParameters, ClientSession, McpError

@asynccontextmanager
async def mcp_session(command, args=None, env=None):
    params = StdioServerParameters(command=command, args=args or [], env=env)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write,
            read_timeout_seconds=timedelta(seconds=60)) as session:
            await session.initialize()
            yield session

async def run_mcp_tool(command, tool_name, arguments, env=None):
    try:
        async with mcp_session(command, env=env) as session:
            result = await session.call_tool(tool_name, arguments)

        if result.isError:
            error_text = next((b.text for b in result.content if b.type == "text"), "unknown")
            return {"success": False, "error": error_text}

        if result.structuredContent:
            return {"success": True, "data": result.structuredContent}

        texts = [b.text for b in result.content if b.type == "text"]
        return {"success": True, "data": "\n".join(texts)}

    except McpError as e:
        return {"success": False, "error": e.error.message, "code": e.error.code}
    except OSError as e:
        return {"success": False, "error": f"Server not startable: {e}"}
```

## 10. Version Recommendation

Minimum: `mcp>=1.12.0` (locally available). Recommended: `mcp>=1.20.0` for `outputSchema` and `ClientSessionGroup` features.
