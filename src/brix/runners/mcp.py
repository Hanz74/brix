"""MCP runner — tool calls via stdio/SSE protocol."""
import json
import time
import yaml
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout, get_default_timeout
from brix.models import ServerConfig
from brix.cache import SchemaCache

try:
    from mcp import ClientSession, StdioServerParameters, McpError
    from mcp.client.stdio import stdio_client
    from mcp.client.sse import sse_client
    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False
    ClientSession = None  # type: ignore[assignment,misc]
    StdioServerParameters = None  # type: ignore[assignment,misc]
    McpError = Exception  # type: ignore[assignment,misc]
    stdio_client = None  # type: ignore[assignment]
    sse_client = None  # type: ignore[assignment]


# Default path for servers.yaml
SERVERS_CONFIG_PATH = Path.home() / ".brix" / "servers.yaml"


def _unwrap_json_strings(obj: Any, max_depth: int = 3) -> Any:
    """Recursively unwrap JSON-encoded string values in a dict.

    Used when a server is configured with ``unwrap_json: true`` to
    automatically resolve double-wrapped responses such as those returned
    by Cody: ``{"result": "{\"result\": {...}}"}`` becomes the inner dict.
    """
    if max_depth <= 0 or not isinstance(obj, dict):
        return obj
    result: dict = {}
    for k, v in obj.items():
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    v = _unwrap_json_strings(parsed, max_depth - 1)
                elif isinstance(parsed, list):
                    v = parsed
                # Non-container JSON scalars (int, bool, None) stay as-is
            except (json.JSONDecodeError, ValueError):
                pass
        result[k] = v
    return result

# Forward-declare to avoid circular import; the real type is checked at runtime.
_McpConnectionPool = None


def load_server_config(server_name: str, config_path: Optional[Path] = None) -> ServerConfig:
    """Load a server configuration from servers.yaml.

    Args:
        server_name: Name of the server to look up.
        config_path: Override path for servers.yaml (default: ~/.brix/servers.yaml).

    Returns:
        ServerConfig for the named server.

    Raises:
        FileNotFoundError: If the servers.yaml file does not exist.
        KeyError: If the server name is not found in the file.
    """
    path = config_path or SERVERS_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"No servers.yaml found at {path}")

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    servers = data.get("servers", {})
    if server_name not in servers:
        raise KeyError(f"Server '{server_name}' not found in {path}")

    return ServerConfig(name=server_name, **servers[server_name])


class McpRunner(BaseRunner):
    """Runs MCP tool calls via stdio protocol using the official MCP SDK.

    When a pool is attached (via :attr:`pool`), the runner reuses the pooled
    connection instead of opening a new stdio subprocess per call.  Falls back
    transparently to per-call connections when no pool is set.
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "server": {"type": "string", "description": "MCP server name from servers.yaml"},
                "tool": {"type": "string", "description": "Tool name to call"},
                "params": {"type": "object", "description": "Tool arguments"},
                "timeout": {"type": "string", "description": "Timeout e.g. '30s'"},
            },
            "required": ["server", "tool"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "any"

    def __init__(
        self,
        servers_config_path: Optional[Path] = None,
        schema_cache: Optional[SchemaCache] = None,
        pool: Optional[Any] = None,
    ):
        self._config_path = servers_config_path or SERVERS_CONFIG_PATH
        self._schema_cache = schema_cache or SchemaCache()
        # Optional McpConnectionPool injected by PipelineEngine
        self._pool: Optional[Any] = pool

    @property
    def pool(self) -> Optional[Any]:
        """The attached McpConnectionPool, or None."""
        return self._pool

    @pool.setter
    def pool(self, value: Optional[Any]) -> None:
        """Attach or detach a McpConnectionPool."""
        self._pool = value

    def _validate_params_against_schema(self, server_name: str, tool_name: str, arguments: dict) -> list[str]:
        """Validate parameter types against cached tool schema. Returns list of warnings."""
        warnings: list[str] = []
        tool_schema = self._schema_cache.get_tool_schema(server_name, tool_name)
        if not tool_schema:
            return warnings  # No cached schema — skip validation

        input_schema = tool_schema.get("inputSchema", {})
        properties = input_schema.get("properties", {})

        type_map: dict[str, Any] = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
        }

        for param_name, param_value in arguments.items():
            if param_name in properties:
                expected_type = properties[param_name].get("type", "")
                expected_python_type = type_map.get(expected_type)
                if expected_python_type and not isinstance(param_value, expected_python_type):
                    actual_type = type(param_value).__name__
                    warnings.append(
                        f"Parameter '{param_name}' expects {expected_type}, "
                        f"got {actual_type} ({repr(param_value)[:50]})"
                    )

        return warnings

    async def execute(self, step: Any, context: Any) -> dict:
        """Execute an MCP tool call step.

        Reads server + tool from step, loads server config from servers.yaml,
        launches the server via stdio, calls the tool, and returns JSON output.

        Returns:
            dict with success, data (JSON-parsed or raw string), duration.
            On error: success=False, error=str.
        """
        start = time.monotonic()

        server_name = getattr(step, "server", None)
        tool_name = getattr(step, "tool", None)

        if not server_name:
            self.report_progress(0.0, "error: missing server")
            return {"success": False, "error": "MCP step needs 'server' field", "duration": 0.0}
        if not tool_name:
            self.report_progress(0.0, "error: missing tool")
            return {"success": False, "error": "MCP step needs 'tool' field", "duration": 0.0}

        # Get tool arguments from params — strip internal keys (prefixed with _)
        params = getattr(step, "params", {}) or {}
        arguments = {k: v for k, v in params.items() if not k.startswith("_")}

        # Re-serialize dict/list values back to JSON strings.
        # Many MCP servers (e.g. Cody) expect a JSON-encoded string for
        # their ``params`` argument, but Brix's Jinja2 renderer auto-parses
        # JSON strings into dicts.  Converting them back here keeps both
        # worlds happy: pipeline authors can use Jinja2 to build dicts,
        # and the MCP call receives the string the server expects.
        for k, v in list(arguments.items()):
            if isinstance(v, (dict, list)):
                arguments[k] = json.dumps(v, ensure_ascii=False)

        self.report_progress(0.0, f"Calling {server_name}/{tool_name}")

        # Timeout
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else get_default_timeout("mcp")

        # Pre-validate parameter types against cached schema
        type_warnings = self._validate_params_against_schema(server_name, tool_name, arguments)
        if type_warnings:
            return {
                "success": False,
                "error": "Parameter type mismatch: " + "; ".join(type_warnings),
                "duration": time.monotonic() - start,
                "warnings": type_warnings,
            }

        # Fast path: delegate to the connection pool if one is attached
        if self._pool is not None:
            pool_result = await self._pool.call_tool(
                server_name, tool_name, arguments, timeout=timeout_seconds
            )
            pool_duration = time.monotonic() - start
            pool_result["mcp_trace"] = self._build_trace(
                server=server_name,
                tool=tool_name,
                arguments=arguments,
                result=pool_result,
                duration=pool_duration,
            )
            return pool_result

        # Resolve server config before importing the SDK so config errors surface first
        try:
            server_config = load_server_config(server_name, self._config_path)
        except (FileNotFoundError, KeyError) as e:
            return {"success": False, "error": str(e), "duration": time.monotonic() - start}

        if not _MCP_AVAILABLE:
            return {
                "success": False,
                "error": "MCP SDK not installed. Run: pip install mcp",
                "duration": time.monotonic() - start,
            }

        # Choose transport based on server config
        if server_config.transport == "sse":
            if not server_config.url:
                return {
                    "success": False,
                    "error": f"SSE server '{server_name}' has no 'url' configured",
                    "duration": time.monotonic() - start,
                }
            ctx_manager = sse_client(url=server_config.url)
        else:
            server_params = StdioServerParameters(
                command=server_config.command,
                args=server_config.args,
                env=server_config.env if server_config.env else None,
            )
            ctx_manager = stdio_client(server_params)

        try:
            async with ctx_manager as (read, write):
                async with ClientSession(
                    read,
                    write,
                    read_timeout_seconds=timedelta(seconds=timeout_seconds),
                ) as session:
                    await session.initialize()
                    # Pass timeout explicitly to call_tool so individual tool
                    # calls are bounded even if the session-level timeout is
                    # longer or unset.  This prevents hangs when the MCP server
                    # stalls on invalid-parameter requests.
                    result = await session.call_tool(
                        tool_name,
                        arguments,
                        read_timeout_seconds=timedelta(seconds=timeout_seconds),
                    )

            duration = time.monotonic() - start

            # Check for tool-level error (isError flag)
            if result.isError:
                error_text = next(
                    (b.text for b in result.content if hasattr(b, "text")),
                    "unknown MCP tool error",
                )
                out = {"success": False, "error": error_text, "duration": duration}
                out["mcp_trace"] = self._build_trace(
                    server=server_name, tool=tool_name, arguments=arguments,
                    result=out, duration=duration,
                )
                return out

            # Prefer structuredContent if available (MCP spec >= 2025-06-18)
            if hasattr(result, "structuredContent") and result.structuredContent:
                out = {"success": True, "data": result.structuredContent, "duration": duration}
                out["mcp_trace"] = self._build_trace(
                    server=server_name, tool=tool_name, arguments=arguments,
                    result=out, duration=duration,
                )
                self.report_progress(100.0, "done")
                return out

            # Extract text from content blocks
            texts = [b.text for b in result.content if hasattr(b, "text")]
            combined = "\n".join(texts)

            # Try JSON parse, fall back to raw string
            try:
                data: Any = json.loads(combined)
            except (json.JSONDecodeError, ValueError):
                data = combined

            # Auto-unwrap nested JSON strings (e.g. Cody returns {"result": "{\"result\": {...)}"})
            # Step-level unwrap_json (T-BRIX-IMP-01) overrides server config when set.
            step_unwrap = getattr(step, "unwrap_json", None)
            effective_unwrap = step_unwrap if step_unwrap is not None else server_config.unwrap_json
            if effective_unwrap and isinstance(data, dict):
                data = _unwrap_json_strings(data)

            out = {"success": True, "data": data, "duration": duration}
            out["mcp_trace"] = self._build_trace(
                server=server_name, tool=tool_name, arguments=arguments,
                result=out, duration=duration,
            )
            self.report_progress(100.0, "done")
            return out

        except McpError as e:
            out = {
                "success": False,
                "error": f"MCP error: {e.error.message if hasattr(e, 'error') else str(e)}",
                "duration": time.monotonic() - start,
            }
            out["mcp_trace"] = self._build_trace(
                server=server_name, tool=tool_name, arguments=arguments,
                result=out, duration=out["duration"],
            )
            return out
        except OSError as e:
            out = {
                "success": False,
                "error": f"Server not startable: {e}",
                "duration": time.monotonic() - start,
            }
            out["mcp_trace"] = self._build_trace(
                server=server_name, tool=tool_name, arguments=arguments,
                result=out, duration=out["duration"],
            )
            return out
        except Exception as e:
            out = {
                "success": False,
                "error": f"MCP runner error: {e}",
                "duration": time.monotonic() - start,
            }
            out["mcp_trace"] = self._build_trace(
                server=server_name, tool=tool_name, arguments=arguments,
                result=out, duration=out["duration"],
            )
            return out

    @staticmethod
    def _build_trace(
        server: str,
        tool: str,
        arguments: dict,
        result: dict,
        duration: float,
    ) -> dict:
        """Build a structured MCP call trace record (T-BRIX-V7-05).

        Returns a dict suitable for storage in step_outputs alongside the
        step's rendered_params.

        Fields
        ------
        server:          MCP server name
        tool:            Tool name called
        arguments_summary: Truncated argument keys → value-type summary
        response_summary: Brief summary of the response payload
        duration:        Wall-clock seconds for the call
        status:          'ok' or 'error'
        """
        # Summarise arguments — keys + type/length, no raw values (may be large)
        def _arg_summary(v: Any) -> str:
            if isinstance(v, str):
                return f"str({len(v)})"
            if isinstance(v, (list, tuple)):
                return f"list({len(v)})"
            if isinstance(v, dict):
                return f"dict({len(v)})"
            return type(v).__name__

        args_summary = {k: _arg_summary(v) for k, v in (arguments or {}).items()}

        # Response summary
        if result.get("success"):
            data = result.get("data")
            if isinstance(data, dict):
                resp_summary = f"dict({len(data)} keys)"
            elif isinstance(data, list):
                resp_summary = f"list({len(data)} items)"
            elif isinstance(data, str):
                resp_summary = f"str({len(data)})"
            else:
                resp_summary = type(data).__name__
        else:
            err = result.get("error", "unknown")
            resp_summary = f"error: {str(err)[:120]}"

        return {
            "server": server,
            "tool": tool,
            "arguments_summary": args_summary,
            "response_summary": resp_summary,
            "duration": round(duration, 4),
            "status": "ok" if result.get("success") else "error",
        }
