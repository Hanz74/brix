"""Environment profile management for Brix pipelines.

Profiles allow different configurations (credentials, input defaults) for
different environments (dev, prod, staging, etc.).

Profile file location: ~/.brix/profiles.yaml

Example profiles.yaml:
    default_profile: dev

    profiles:
      dev:
        env:
          MY_API_KEY: "dev-key-123"
          BASE_URL: "https://api.dev.example.com"
        input_defaults:
          limit: 10

      prod:
        env:
          MY_API_KEY: "${PROD_API_KEY}"   # resolved from OS env
          BASE_URL: "https://api.example.com"
        input_defaults:
          limit: 1000

Usage:
    brix run pipeline.yaml --profile prod
    BRIX_PROFILE=prod brix run pipeline.yaml
"""

import os
from pathlib import Path
from typing import Any, Optional

import yaml

PROFILES_PATH = Path.home() / ".brix" / "profiles.yaml"
BRIX_PROFILE_ENV = "BRIX_PROFILE"


class ProfileNotFoundError(Exception):
    """Raised when a requested profile does not exist."""


class ProfileManager:
    """Loads and applies environment profiles from ~/.brix/profiles.yaml."""

    def __init__(self, profiles_path: Path = PROFILES_PATH) -> None:
        self._path = profiles_path
        self._data: Optional[dict] = None

    def _load(self) -> dict:
        """Load profiles YAML file. Returns empty dict if file does not exist."""
        if self._data is None:
            if self._path.exists():
                with open(self._path) as f:
                    raw = yaml.safe_load(f) or {}
            else:
                raw = {}
            self._data = raw
        return self._data

    def list_profiles(self) -> list[str]:
        """Return all defined profile names."""
        data = self._load()
        return list(data.get("profiles", {}).keys())

    def get_default_profile(self) -> Optional[str]:
        """Return the configured default profile name, or None."""
        data = self._load()
        return data.get("default_profile")

    def active_profile_name(self, override: Optional[str] = None) -> Optional[str]:
        """Resolve the active profile name.

        Priority (highest to lowest):
        1. ``override`` argument (e.g. from --profile CLI flag)
        2. ``BRIX_PROFILE`` environment variable
        3. ``default_profile`` from profiles.yaml
        4. ``None`` (no profile active)
        """
        if override:
            return override
        env_profile = os.environ.get(BRIX_PROFILE_ENV)
        if env_profile:
            return env_profile
        return self.get_default_profile()

    def load_profile(self, name: str) -> dict[str, Any]:
        """Load a profile by name and return its configuration dict.

        Returns a dict with keys:
        - ``env``: dict of env var name → value to inject
        - ``input_defaults``: dict of pipeline input param overrides

        Env var values starting with ``$`` are resolved from the current
        OS environment (e.g. ``${PROD_SECRET}`` → os.environ["PROD_SECRET"]).

        Raises ProfileNotFoundError if the profile does not exist.
        """
        data = self._load()
        profiles = data.get("profiles", {})
        if name not in profiles:
            available = list(profiles.keys())
            raise ProfileNotFoundError(
                f"Profile '{name}' not found. Available: {available}"
            )

        raw = profiles[name] or {}
        env = _resolve_env_values(raw.get("env", {}))
        input_defaults = raw.get("input_defaults", {})

        return {
            "env": env,
            "input_defaults": input_defaults,
        }

    def apply_profile(self, name: Optional[str]) -> dict[str, Any]:
        """Load and apply a profile's env vars to ``os.environ``.

        If ``name`` is None, does nothing and returns empty config.
        Returns the full profile config dict (for further use by context).
        """
        if not name:
            return {"env": {}, "input_defaults": {}}

        config = self.load_profile(name)
        for key, value in config["env"].items():
            os.environ[key] = str(value)

        return config

    def save_profile(self, name: str, env: dict[str, str] = None, input_defaults: dict = None) -> None:
        """Save or update a profile in the profiles YAML file.

        Creates the file (and parent directory) if it does not exist.
        Merges into any existing profiles — does not overwrite other profiles.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)

        # Re-load from disk to avoid overwriting concurrent changes
        if self._path.exists():
            with open(self._path) as f:
                data = yaml.safe_load(f) or {}
        else:
            data = {}

        data.setdefault("profiles", {})[name] = {
            "env": env or {},
            "input_defaults": input_defaults or {},
        }

        with open(self._path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True)

        # Invalidate in-memory cache
        self._data = None

    def delete_profile(self, name: str) -> None:
        """Remove a profile. Raises ProfileNotFoundError if not found."""
        data = self._load()
        profiles = data.get("profiles", {})
        if name not in profiles:
            raise ProfileNotFoundError(f"Profile '{name}' not found.")

        del profiles[name]
        with open(self._path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
        self._data = None

    def set_default(self, name: Optional[str]) -> None:
        """Set (or clear) the default profile in profiles.yaml."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if self._path.exists():
            with open(self._path) as f:
                data = yaml.safe_load(f) or {}
        else:
            data = {}

        if name is None:
            data.pop("default_profile", None)
        else:
            # Validate profile exists
            if name not in data.get("profiles", {}):
                raise ProfileNotFoundError(
                    f"Profile '{name}' not found. Create it first with 'brix profile add'."
                )
            data["default_profile"] = name

        with open(self._path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
        self._data = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_env_values(env: dict) -> dict[str, str]:
    """Resolve ``${VAR}`` references in env value strings.

    Only simple ``${VAR_NAME}`` patterns are supported — the value is replaced
    with the OS env var if it exists, otherwise kept as-is (minus the sigils).
    """
    import re

    resolved = {}
    pattern = re.compile(r"^\$\{([^}]+)\}$")

    for key, value in env.items():
        str_value = str(value)
        m = pattern.match(str_value)
        if m:
            var_name = m.group(1)
            resolved[key] = os.environ.get(var_name, "")
        else:
            resolved[key] = str_value

    return resolved
