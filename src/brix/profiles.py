"""Environment profile management backed by the ``env_profile`` DB table."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

PROFILES_PATH = Path.home() / ".brix" / "profiles.yaml"
BRIX_PROFILE_ENV = "BRIX_PROFILE"


class ProfileNotFoundError(Exception):
    """Raised when a requested profile does not exist."""


class ProfileManager:
    """Loads and applies environment profiles from ``env_profile`` in ``brix.db``."""

    def __init__(self, profiles_path: Path = PROFILES_PATH) -> None:
        # ``profiles_path`` is retained for compatibility with older tests/callers.
        # Storage is DB-backed; the path is no longer read or written.
        self._path = Path(profiles_path)

    def _db(self):
        """Import BrixDB lazily to avoid circular imports."""
        from brix.db import BrixDB

        return BrixDB()

    def list_profiles(self) -> list[str]:
        """Return all defined profile names."""
        return [row.get("name", "") for row in self._db().list_env_profiles()]

    def get_default(self) -> Optional[str]:
        """Return the configured default profile name, or ``None``."""
        row = self._db().get_default_env_profile()
        if row is None:
            return None
        return row.get("name")

    def get_default_profile(self) -> Optional[str]:
        """Compatibility alias for older callers."""
        return self.get_default()

    def active_profile_name(self, override: Optional[str] = None) -> Optional[str]:
        """Resolve the active profile name."""
        if override:
            return override
        env_profile = os.environ.get(BRIX_PROFILE_ENV)
        if env_profile:
            return env_profile
        return self.get_default()

    def get_profile(self, name: str) -> dict[str, Any]:
        """Load a profile by name and return its configuration dict."""
        row = self._db().get_env_profile(name)
        if row is None:
            available = self.list_profiles()
            raise ProfileNotFoundError(
                f"Profile '{name}' not found. Available: {available}"
            )

        return {
            "env": _resolve_env_values(row.get("env", {})),
            "input_defaults": row.get("input_defaults", {}) or {},
        }

    def load_profile(self, name: str) -> dict[str, Any]:
        """Compatibility alias for older callers."""
        return self.get_profile(name)

    def apply_profile(self, name: Optional[str]) -> dict[str, Any]:
        """Load and apply a profile's env vars to ``os.environ``."""
        if not name:
            return {"env": {}, "input_defaults": {}}

        config = self.get_profile(name)
        for key, value in config["env"].items():
            os.environ[key] = str(value)

        return config

    def save_profile(
        self,
        name: str,
        env: dict[str, str] | None = None,
        input_defaults: dict | None = None,
    ) -> None:
        """Create or update an environment profile in the DB."""
        db = self._db()
        existing = db.get_env_profile(name)
        is_default = bool(existing and existing.get("is_default"))
        description = existing.get("description", "") if existing else ""
        db.upsert_env_profile(
            name=name,
            env=env or {},
            input_defaults=input_defaults or {},
            is_default=is_default,
            description=description,
        )

    def delete_profile(self, name: str) -> None:
        """Remove a profile. Raises ``ProfileNotFoundError`` if not found."""
        if not self._db().delete_env_profile(name):
            raise ProfileNotFoundError(f"Profile '{name}' not found.")

    def set_default(self, name: Optional[str]) -> None:
        """Set or clear the default profile in the DB."""
        db = self._db()

        if name is None:
            for row in db.list_env_profiles():
                if row.get("is_default"):
                    db.upsert_env_profile(
                        name=row["name"],
                        env=row.get("env", {}),
                        input_defaults=row.get("input_defaults", {}),
                        is_default=False,
                        description=row.get("description", ""),
                    )
            return

        row = db.get_env_profile(name)
        if row is None:
            raise ProfileNotFoundError(
                f"Profile '{name}' not found. Create it first with 'brix profile add'."
            )

        db.upsert_env_profile(
            name=name,
            env=row.get("env", {}),
            input_defaults=row.get("input_defaults", {}),
            is_default=True,
            description=row.get("description", ""),
        )


def _resolve_env_values(env: dict) -> dict[str, str]:
    """Resolve ``${VAR}`` references in env value strings."""
    import re

    resolved = {}
    pattern = re.compile(r"^\$\{([^}]+)\}$")

    for key, value in env.items():
        str_value = str(value)
        match = pattern.match(str_value)
        if match:
            resolved[key] = os.environ.get(match.group(1), "")
        else:
            resolved[key] = str_value

    return resolved
