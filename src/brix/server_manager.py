"""MCP Server Manager — CRUD for servers.yaml entries.

Provides a clean API for managing MCP server configurations stored in
``~/.brix/servers.yaml``.  Each entry describes how to launch a server
(command, args, optional env) so the pipeline engine can connect to it.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml


DEFAULT_SERVERS_PATH = Path.home() / ".brix" / "servers.yaml"


def _get_servers_path() -> Path:
    path = DEFAULT_SERVERS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load(path: Path) -> dict:
    if path.exists():
        return yaml.safe_load(path.read_text()) or {}
    return {}


def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data, default_flow_style=False, allow_unicode=True))


class ServerManager:
    """Manages MCP server entries in servers.yaml.

    Parameters
    ----------
    servers_path:
        Path to the servers.yaml file.  Defaults to ``~/.brix/servers.yaml``.
    """

    def __init__(self, servers_path: Optional[Path] = None) -> None:
        self._path = Path(servers_path) if servers_path else DEFAULT_SERVERS_PATH

    def _load(self) -> dict:
        return _load(self._path)

    def _save(self, data: dict) -> None:
        _save(self._path, data)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def add(
        self,
        name: str,
        command: str,
        args: Optional[list] = None,
        env: Optional[dict] = None,
    ) -> dict:
        """Add a new server entry.  Raises ValueError if name already exists."""
        data = self._load()
        servers = data.setdefault("servers", {})
        if name in servers:
            raise ValueError(
                f"Server '{name}' already exists. Use update() to modify it."
            )
        entry: dict = {"command": command, "args": list(args or [])}
        if env:
            entry["env"] = dict(env)
        servers[name] = entry
        self._save(data)
        return {"name": name, **entry}

    def list_all(self) -> list[dict]:
        """Return all server entries as a list of dicts."""
        data = self._load()
        servers = data.get("servers", {})
        return [
            {"name": name, **cfg}
            for name, cfg in servers.items()
        ]

    def get(self, name: str) -> Optional[dict]:
        """Return a single server entry by name, or None if not found."""
        data = self._load()
        cfg = data.get("servers", {}).get(name)
        if cfg is None:
            return None
        return {"name": name, **cfg}

    def update(
        self,
        name: str,
        command: Optional[str] = None,
        args: Optional[list] = None,
        env: Optional[dict] = None,
    ) -> Optional[dict]:
        """Update an existing server entry.  Returns None if not found."""
        data = self._load()
        servers = data.get("servers", {})
        if name not in servers:
            return None
        entry = servers[name]
        if command is not None:
            entry["command"] = command
        if args is not None:
            entry["args"] = list(args)
        if env is not None:
            entry["env"] = dict(env)
        self._save(data)
        return {"name": name, **entry}

    def remove(self, name: str) -> bool:
        """Remove a server entry.  Returns True if removed, False if not found."""
        data = self._load()
        servers = data.get("servers", {})
        if name not in servers:
            return False
        del servers[name]
        self._save(data)
        return True

    def refresh(self, name: str) -> dict:
        """Refresh (re-validate) a server config entry.

        Loads the entry and returns its current state.  The actual MCP
        connection test is async — this method only validates the config
        is present and structurally valid, returning the stored entry.

        Raises KeyError if the server is not found.
        """
        entry = self.get(name)
        if entry is None:
            raise KeyError(f"Server '{name}' not found in servers.yaml")
        if not entry.get("command"):
            raise ValueError(
                f"Server '{name}' has no 'command' field. Use update() to fix it."
            )
        return entry
