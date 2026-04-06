"""MCP Server Manager — CRUD for DB-backed server entries."""
from __future__ import annotations

from pathlib import Path
from typing import Optional


DEFAULT_SERVERS_PATH = Path.home() / ".brix" / "servers.yaml"


class ServerManager:
    """Manages MCP server entries stored in ``brix.db``.

    ``servers_path`` is retained for backward compatibility. When provided, it
    is used only to infer the adjacent ``brix.db`` location. No YAML fallback
    is performed.
    """

    def __init__(self, servers_path: Optional[Path] = None) -> None:
        self._servers_path = Path(servers_path) if servers_path else DEFAULT_SERVERS_PATH
        self._db_path = self._servers_path.with_name("brix.db")

    def _get_db(self):
        # Lazy import avoids circular imports during startup.
        from brix.db import BrixDB

        return BrixDB(self._db_path)

    @staticmethod
    def _entry_from_row(row: Optional[dict]) -> Optional[dict]:
        if row is None:
            return None
        return {
            "name": row["name"],
            "command": row.get("command", ""),
            "args": list(row.get("args") or []),
            "env": dict(row.get("env") or {}),
            "tools_prefix": row.get("tools_prefix") or None,
            "transport": row.get("transport") or "stdio",
            "url": row.get("url") or "",
            "unwrap_json": bool(row.get("unwrap_json", False)),
        }

    def add(
        self,
        name: str,
        command: str,
        args: Optional[list] = None,
        env: Optional[dict] = None,
        *,
        tools_prefix: Optional[str] = None,
        transport: str = "stdio",
        url: str = "",
        unwrap_json: bool = False,
    ) -> dict:
        """Add a new server entry. Raises ValueError if name already exists."""
        if self.get(name) is not None:
            raise ValueError(
                f"Server '{name}' already exists. Use update() to modify it."
            )
        row = self._get_db().upsert_mcp_server(
            name=name,
            command=command,
            args=list(args or []),
            env=dict(env or {}),
            tools_prefix=tools_prefix or "",
            transport=transport,
            url=url,
            unwrap_json=unwrap_json,
        )
        return self._entry_from_row(row) or {}

    def list_all(self) -> list[dict]:
        """Return all server entries as a list of dicts."""
        return [
            entry
            for entry in (
                self._entry_from_row(row) for row in self._get_db().list_mcp_servers()
            )
            if entry is not None
        ]

    def get(self, name: str) -> Optional[dict]:
        """Return a single server entry by name, or None if not found."""
        return self._entry_from_row(self._get_db().get_mcp_server(name))

    def update(
        self,
        name: str,
        command: Optional[str] = None,
        args: Optional[list] = None,
        env: Optional[dict] = None,
        *,
        tools_prefix: Optional[str] = None,
        transport: Optional[str] = None,
        url: Optional[str] = None,
        unwrap_json: Optional[bool] = None,
    ) -> Optional[dict]:
        """Update an existing server entry. Returns None if not found."""
        existing = self.get(name)
        if existing is None:
            return None
        row = self._get_db().upsert_mcp_server(
            name=name,
            command=command if command is not None else existing.get("command", ""),
            args=list(args) if args is not None else list(existing.get("args") or []),
            env=dict(env) if env is not None else dict(existing.get("env") or {}),
            tools_prefix=tools_prefix if tools_prefix is not None else (existing.get("tools_prefix") or ""),
            transport=transport if transport is not None else (existing.get("transport") or "stdio"),
            url=url if url is not None else (existing.get("url") or ""),
            unwrap_json=unwrap_json if unwrap_json is not None else bool(existing.get("unwrap_json", False)),
        )
        return self._entry_from_row(row)

    def remove(self, name: str) -> bool:
        """Remove a server entry. Returns True if removed, False if not found."""
        return self._get_db().delete_mcp_server(name)

    def refresh(self, name: str) -> dict:
        """Refresh (re-validate) a server config entry."""
        entry = self.get(name)
        if entry is None:
            raise KeyError(f"Server '{name}' not found in DB")

        if entry.get("transport") == "sse":
            if not entry.get("url"):
                raise ValueError(
                    f"Server '{name}' has no 'url' field. Use update() to fix it."
                )
            return entry

        if not entry.get("command"):
            raise ValueError(
                f"Server '{name}' has no 'command' field. Use update() to fix it."
            )
        return entry
