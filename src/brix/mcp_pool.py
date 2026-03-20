"""Lazy connection pool for MCP servers.

Uses the SDK's ClientSessionGroup for proper lifecycle management.
Connections are established on first use (lazy) and held open for
subsequent calls within the same pool lifetime.

Typical usage — one pool per pipeline run:

    async with McpConnectionPool() as pool:
        result = await pool.call_tool("m365", "list-mail-messages", {})
"""
import json
import time
import asyncio
from pathlib import Path
from datetime import timedelta
from typing import Optional

from mcp import StdioServerParameters
from mcp.client.session_group import ClientSessionGroup

from brix.runners.mcp import load_server_config, SERVERS_CONFIG_PATH
from brix.models import ServerConfig


class McpConnectionPool:
    """Manages persistent connections to MCP servers.

    Lazy: connects on first call per server, holds the connection open,
    reconnects automatically on failure.

    Backed by the SDK's ``ClientSessionGroup`` for correct async lifecycle
    management — no manual ``__aenter__``/``__aexit__`` hacks needed.

    Usage as async context manager (recommended)::

        async with McpConnectionPool() as pool:
            result = await pool.call_tool("m365", "list-mail-messages", {})

    Usage without context manager (caller manages cleanup)::

        pool = McpConnectionPool()
        await pool.__aenter__()
        try:
            result = await pool.call_tool("m365", "list-mail-messages", {})
        finally:
            await pool.__aexit__(None, None, None)
    """

    def __init__(self, config_path: Optional[Path] = None):
        self._config_path: Path = config_path or SERVERS_CONFIG_PATH
        # server_name → ClientSession (direct session reference)
        self._server_sessions: dict[str, object] = {}
        # server_name → asyncio.Lock (prevents duplicate connect races)
        self._locks: dict[str, asyncio.Lock] = {}
        # The SDK group managing all session lifecycles
        self._group: Optional[object] = None

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "McpConnectionPool":
        self._group = ClientSessionGroup()
        await self._group.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._group is not None:
            await self._group.__aexit__(exc_type, exc_val, exc_tb)
            self._group = None
        self._server_sessions.clear()
        self._locks.clear()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_session(self, server_name: str) -> object:
        """Return an open ``ClientSession`` for *server_name*.

        Connects lazily on first call; returns the cached session on
        subsequent calls.  Thread-safe via per-server ``asyncio.Lock``.

        Raises:
            RuntimeError: If the pool has not been entered (no group).
            FileNotFoundError: If ``servers.yaml`` does not exist.
            KeyError: If *server_name* is not found in ``servers.yaml``.
            OSError: If the server subprocess cannot be started.
        """
        if self._group is None:
            raise RuntimeError(
                "McpConnectionPool must be used as an async context manager "
                "(async with McpConnectionPool() as pool: ...)"
            )

        if server_name not in self._locks:
            self._locks[server_name] = asyncio.Lock()

        async with self._locks[server_name]:
            if server_name not in self._server_sessions:
                session = await self._connect(server_name)
                self._server_sessions[server_name] = session
            return self._server_sessions[server_name]

    async def call_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: dict,
    ) -> dict:
        """Call *tool_name* on *server_name* using a pooled connection.

        Returns a Brix-standard result dict::

            {"success": True,  "data": ..., "duration": float}
            {"success": False, "error": str, "duration": float}

        On connection failure the dead session is evicted so the next
        call will attempt a fresh reconnect.
        """
        start = time.monotonic()

        if self._group is None:
            return {
                "success": False,
                "error": (
                    "McpConnectionPool must be used as an async context manager"
                ),
                "duration": 0.0,
            }

        try:
            session = await self.get_session(server_name)
            result = await session.call_tool(tool_name, arguments)
            duration = time.monotonic() - start

            # Tool-level error (isError flag, NOT an exception)
            if result.isError:
                error_text = next(
                    (b.text for b in result.content if hasattr(b, "text")),
                    "unknown MCP tool error",
                )
                return {"success": False, "error": error_text, "duration": duration}

            # Prefer structured content (MCP spec >= 2025-06-18)
            if hasattr(result, "structuredContent") and result.structuredContent:
                return {
                    "success": True,
                    "data": result.structuredContent,
                    "duration": duration,
                }

            # Fall back to text content, try JSON parse
            texts = [b.text for b in result.content if hasattr(b, "text")]
            combined = "\n".join(texts)
            try:
                data = json.loads(combined)
            except (json.JSONDecodeError, ValueError):
                data = combined

            return {"success": True, "data": data, "duration": duration}

        except (FileNotFoundError, KeyError) as e:
            # Config errors — not a connection problem, don't evict
            return {
                "success": False,
                "error": str(e),
                "duration": time.monotonic() - start,
            }
        except Exception as e:
            # Connection or protocol error — evict so next call reconnects
            self._server_sessions.pop(server_name, None)
            return {
                "success": False,
                "error": f"MCP pool error: {e}",
                "duration": time.monotonic() - start,
            }

    async def close_all(self) -> None:
        """Close all connections and reset pool state.

        Safe to call even when the pool was not entered or is already closed.
        If the pool is used as a context manager, ``__aexit__`` handles this
        automatically — calling ``close_all`` explicitly is only needed for
        manual lifecycle management.
        """
        if self._group is not None:
            await self._group.__aexit__(None, None, None)
            self._group = None
        self._server_sessions.clear()
        self._locks.clear()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def connected_servers(self) -> list[str]:
        """Names of servers with an active open connection."""
        return list(self._server_sessions.keys())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _connect(self, server_name: str) -> object:
        """Establish a new connection to *server_name* via the SDK group.

        Loads server config from ``servers.yaml``, creates
        ``StdioServerParameters``, and calls ``group.connect_to_server``.

        Returns the ``ClientSession`` returned by the group.
        """
        server_config: ServerConfig = load_server_config(
            server_name, self._config_path
        )

        params = StdioServerParameters(
            command=server_config.command,
            args=server_config.args,
            env=server_config.env if server_config.env else None,
        )

        # connect_to_server returns the ClientSession
        session = await self._group.connect_to_server(params)
        return session
