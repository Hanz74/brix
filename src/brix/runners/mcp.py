"""MCP runner — tool calls via stdio protocol."""
import json
import time
import yaml
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout
from brix.models import ServerConfig


# Default path for servers.yaml
SERVERS_CONFIG_PATH = Path.home() / ".brix" / "servers.yaml"


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
    """Runs MCP tool calls via stdio protocol using the official MCP SDK."""

    def __init__(self, servers_config_path: Optional[Path] = None):
        self._config_path = servers_config_path or SERVERS_CONFIG_PATH

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
            return {"success": False, "error": "MCP step needs 'server' field", "duration": 0.0}
        if not tool_name:
            return {"success": False, "error": "MCP step needs 'tool' field", "duration": 0.0}

        # Get tool arguments from params — strip internal keys (prefixed with _)
        params = getattr(step, "params", {}) or {}
        arguments = {k: v for k, v in params.items() if not k.startswith("_")}

        # Timeout
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else 60.0

        # Resolve server config before importing the SDK so config errors surface first
        try:
            server_config = load_server_config(server_name, self._config_path)
        except (FileNotFoundError, KeyError) as e:
            return {"success": False, "error": str(e), "duration": time.monotonic() - start}

        try:
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
            from mcp import McpError
        except ImportError:
            return {
                "success": False,
                "error": "MCP SDK not installed. Run: pip install mcp",
                "duration": time.monotonic() - start,
            }

        server_params = StdioServerParameters(
            command=server_config.command,
            args=server_config.args,
            env=server_config.env if server_config.env else None,
        )

        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(
                    read,
                    write,
                    read_timeout_seconds=timedelta(seconds=timeout_seconds),
                ) as session:
                    await session.initialize()
                    result = await session.call_tool(tool_name, arguments)

            duration = time.monotonic() - start

            # Check for tool-level error (isError flag)
            if result.isError:
                error_text = next(
                    (b.text for b in result.content if hasattr(b, "text")),
                    "unknown MCP tool error",
                )
                return {"success": False, "error": error_text, "duration": duration}

            # Prefer structuredContent if available (MCP spec >= 2025-06-18)
            if hasattr(result, "structuredContent") and result.structuredContent:
                return {"success": True, "data": result.structuredContent, "duration": duration}

            # Extract text from content blocks
            texts = [b.text for b in result.content if hasattr(b, "text")]
            combined = "\n".join(texts)

            # Try JSON parse, fall back to raw string
            try:
                data: Any = json.loads(combined)
            except (json.JSONDecodeError, ValueError):
                data = combined

            return {"success": True, "data": data, "duration": duration}

        except McpError as e:
            return {
                "success": False,
                "error": f"MCP error: {e.error.message if hasattr(e, 'error') else str(e)}",
                "duration": time.monotonic() - start,
            }
        except OSError as e:
            return {
                "success": False,
                "error": f"Server not startable: {e}",
                "duration": time.monotonic() - start,
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"MCP runner error: {e}",
                "duration": time.monotonic() - start,
            }
