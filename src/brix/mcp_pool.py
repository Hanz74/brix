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
from datetime import datetime, timezone
from pathlib import Path
from datetime import timedelta
from typing import Optional

from mcp import StdioServerParameters, McpError
from mcp.client.session_group import ClientSessionGroup

from brix.runners.mcp import load_server_config, SERVERS_CONFIG_PATH
from brix.models import ServerConfig
from brix.config import config

# Default timeout (seconds) for individual tool calls via the pool.
# Prevents hangs when an MCP server stalls on schema validation errors
# or other protocol-level issues that never produce a response.
DEFAULT_POOL_CALL_TIMEOUT = config.MCP_POOL_CALL_TIMEOUT


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        # Health tracking: server_name → health stats dict
        # Keys: last_contact_at (ISO str), latency_sum_ms (float),
        #        call_count (int), error_count (int)
        self._health: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "McpConnectionPool":
        self._group = ClientSessionGroup()
        await self._group.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._group is not None:
            try:
                await self._group.__aexit__(exc_type, exc_val, exc_tb)
            except RuntimeError as e:
                # ClientSessionGroup's internal stdio readers use anyio task
                # groups with cancel scopes.  When the pool is torn down the
                # scope exit can land in a different asyncio task than the one
                # that entered it (e.g. background reader tasks spawned by
                # connect_to_server).  This is a known anyio/MCP-SDK lifecycle
                # quirk — the connections are already closed at this point so
                # we suppress the error safely.
                if "cancel scope" in str(e):
                    pass  # Cleanup already done, safe to ignore
                else:
                    raise
            finally:
                self._group = None
        self._server_sessions.clear()
        self._locks.clear()
        self._health.clear()

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
        timeout: Optional[float] = None,
    ) -> dict:
        """Call *tool_name* on *server_name* using a pooled connection.

        Returns a Brix-standard result dict::

            {"success": True,  "data": ..., "duration": float}
            {"success": False, "error": str, "duration": float}

        On connection failure the dead session is evicted so the next
        call will attempt a fresh reconnect.

        Args:
            server_name: Registered MCP server name.
            tool_name: Tool to invoke on the server.
            arguments: Tool arguments dict.
            timeout: Per-call timeout in seconds.  Defaults to
                ``DEFAULT_POOL_CALL_TIMEOUT`` (60 s).  Pass ``None`` to keep
                the default.  The timeout is forwarded to the MCP SDK's
                ``call_tool`` as ``read_timeout_seconds`` so the SDK raises
                ``McpError`` on expiry rather than hanging forever.
        """
        start = time.monotonic()
        call_timeout = timeout if timeout is not None else DEFAULT_POOL_CALL_TIMEOUT

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
            result = await session.call_tool(
                tool_name,
                arguments,
                read_timeout_seconds=timedelta(seconds=call_timeout),
            )
            duration = time.monotonic() - start
            latency_ms = duration * 1000.0

            # Tool-level error (isError flag, NOT an exception)
            if result.isError:
                error_text = next(
                    (b.text for b in result.content if hasattr(b, "text")),
                    "unknown MCP tool error",
                )
                self._record_health(server_name, latency_ms, error=True)
                return {"success": False, "error": error_text, "duration": duration}

            self._record_health(server_name, latency_ms, error=False)

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
            self._record_health(server_name, (time.monotonic() - start) * 1000.0, error=True)
            return {
                "success": False,
                "error": str(e),
                "duration": time.monotonic() - start,
            }
        except McpError as e:
            # Protocol-level errors (invalid params, schema validation, timeout).
            # These are clean MCP errors — the session itself is still valid, so
            # we do NOT evict.  Surface a clear error message.
            self._record_health(server_name, (time.monotonic() - start) * 1000.0, error=True)
            error_msg = e.error.message if hasattr(e, "error") else str(e)
            return {
                "success": False,
                "error": f"MCP error: {error_msg}",
                "duration": time.monotonic() - start,
            }
        except Exception as e:
            # Connection or transport error — evict so next call reconnects
            self._record_health(server_name, (time.monotonic() - start) * 1000.0, error=True)
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
            try:
                await self._group.__aexit__(None, None, None)
            except RuntimeError as e:
                if "cancel scope" not in str(e):
                    raise
            finally:
                self._group = None
        self._server_sessions.clear()
        self._locks.clear()
        self._health.clear()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def connected_servers(self) -> list[str]:
        """Names of servers with an active open connection."""
        return list(self._server_sessions.keys())

    def get_health(self) -> dict[str, dict]:
        """Return health stats for all servers that have been called.

        Returns a dict keyed by server name.  Each value contains:

        - ``last_contact_at`` — ISO-8601 UTC timestamp of the last call (success or error).
        - ``avg_latency_ms`` — rolling average latency in milliseconds across all calls.
        - ``call_count`` — total number of calls (including errors).
        - ``error_count`` — number of calls that resulted in an error.

        Only servers that have been contacted at least once appear in the result.
        """
        result: dict[str, dict] = {}
        for server_name, stats in self._health.items():
            call_count = stats["call_count"]
            avg_latency = stats["latency_sum_ms"] / call_count if call_count else 0.0
            result[server_name] = {
                "last_contact_at": stats["last_contact_at"],
                "avg_latency_ms": round(avg_latency, 2),
                "call_count": call_count,
                "error_count": stats["error_count"],
            }
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record_health(self, server_name: str, latency_ms: float, *, error: bool) -> None:
        """Update in-memory health stats after a call attempt.

        Args:
            server_name: The MCP server that was called.
            latency_ms: Wall-clock latency of the call in milliseconds.
            error: True if the call resulted in any kind of error.
        """
        if server_name not in self._health:
            self._health[server_name] = {
                "last_contact_at": "",
                "latency_sum_ms": 0.0,
                "call_count": 0,
                "error_count": 0,
            }
        stats = self._health[server_name]
        stats["last_contact_at"] = _utc_now_iso()
        stats["latency_sum_ms"] += latency_ms
        stats["call_count"] += 1
        if error:
            stats["error_count"] += 1

    async def _connect(self, server_name: str) -> object:
        """Establish a new connection to *server_name* via the SDK group.

        Loads server config from ``servers.yaml``.  For stdio servers creates
        ``StdioServerParameters``; for SSE servers creates
        ``SseServerParameters``.  Both are passed to
        ``group.connect_to_server`` which handles the transport transparently.

        Returns the ``ClientSession`` returned by the group.
        """
        server_config: ServerConfig = load_server_config(
            server_name, self._config_path
        )

        if server_config.transport == "sse":
            if not server_config.url:
                raise ValueError(
                    f"SSE server '{server_name}' has no 'url' configured"
                )
            from mcp.client.session_group import SseServerParameters

            params = SseServerParameters(url=server_config.url)
        else:
            params = StdioServerParameters(
                command=server_config.command,
                args=server_config.args,
                env=server_config.env if server_config.env else None,
            )

        # connect_to_server returns the ClientSession
        session = await self._group.connect_to_server(params)
        return session
