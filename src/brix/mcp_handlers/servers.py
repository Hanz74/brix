"""MCP server management handler module."""
from __future__ import annotations


async def _handle_server_add(arguments: dict) -> dict:
    """Register a new MCP server configuration."""
    from brix.server_manager import ServerManager
    name = arguments.get("name", "").strip()
    command = arguments.get("command", "").strip()
    args = arguments.get("args") or []
    env = arguments.get("env") or {}

    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}
    if not command:
        return {"success": False, "error": "Parameter 'command' is required."}

    mgr = ServerManager()
    try:
        entry = mgr.add(name=name, command=command, args=args, env=env or None)
        return {"success": True, "server": entry}
    except ValueError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


async def _handle_server_list(arguments: dict) -> dict:
    """List all registered MCP server configurations."""
    from brix.server_manager import ServerManager
    mgr = ServerManager()
    servers = mgr.list_all()
    return {"success": True, "servers": servers, "total": len(servers)}


async def _handle_server_update(arguments: dict) -> dict:
    """Update an existing MCP server configuration."""
    from brix.server_manager import ServerManager
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    command = arguments.get("command") or None
    args = arguments.get("args")  # None means "don't touch"
    env = arguments.get("env")    # None means "don't touch"

    mgr = ServerManager()
    updated = mgr.update(name=name, command=command, args=args, env=env)
    if updated is None:
        return {"success": False, "error": f"Server '{name}' not found in servers.yaml."}
    return {"success": True, "server": updated}


async def _handle_server_remove(arguments: dict) -> dict:
    """Remove a registered MCP server configuration."""
    from brix.server_manager import ServerManager
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    mgr = ServerManager()
    removed = mgr.remove(name)
    if not removed:
        return {"success": False, "error": f"Server '{name}' not found in servers.yaml."}
    return {"success": True, "removed": name}


async def _handle_server_refresh(arguments: dict) -> dict:
    """Validate and return a registered MCP server configuration."""
    from brix.server_manager import ServerManager
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required."}

    mgr = ServerManager()
    try:
        entry = mgr.refresh(name)
        return {"success": True, "server": entry}
    except KeyError as exc:
        return {"success": False, "error": str(exc)}
    except ValueError as exc:
        return {"success": False, "error": str(exc)}


async def _handle_server_health(arguments: dict) -> dict:
    """Return health data for all MCP servers tracked by the connection pool.

    The pool is a module-level shared instance.  This handler exposes the
    health dict collected by ``McpConnectionPool.get_health()`` so that
    external callers can monitor server availability without direct Python
    access to the pool object.

    Returns:
        {
          "servers": {
            "<server_name>": {
              "last_contact_at": "<ISO-8601>",
              "avg_latency_ms": <float>,
              "call_count": <int>,
              "error_count": <int>
            },
            ...
          },
          "total": <int>
        }

    If the module-level pool has not been initialised (no pipeline run yet),
    ``servers`` will be an empty dict.
    """
    # The module-level pool is created lazily per run and torn down
    # afterward — expose whatever health data is currently in memory.
    health: dict = {}
    # Walk all live pool instances registered in the engine's context if
    # available; fall back to an empty response when no pool is active.
    try:
        from brix.context import _active_pool  # type: ignore[attr-defined]
        if _active_pool is not None:
            health = _active_pool.get_health()
    except (ImportError, AttributeError):
        pass

    return {"success": True, "servers": health, "total": len(health)}
