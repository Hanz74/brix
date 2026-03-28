"""Security configuration and enforcement."""
from pathlib import Path

import yaml

SECURITY_CONFIG_PATH = Path.home() / ".brix" / "security.yaml"


class SecurityConfig:
    """Manages security allowlists and restrictions."""

    def __init__(self, config_path: Path | None = None):
        self._config_path = config_path or SECURITY_CONFIG_PATH
        self._config: dict = {}
        self._load()

    def _load(self) -> None:
        if self._config_path.exists():
            with open(self._config_path) as f:
                self._config = yaml.safe_load(f) or {}
        else:
            self._config = {}

    @property
    def allowed_containers(self) -> list[str]:
        """Containers allowed for docker exec."""
        return self._config.get("allowed_containers", [])

    @property
    def allowed_executables(self) -> list[str]:
        """Executables allowed for CLI runner."""
        return self._config.get("allowed_executables", [])

    @property
    def allowed_script_paths(self) -> list[str]:
        """Paths allowed for Python scripts."""
        return self._config.get("allowed_script_paths", [])

    @property
    def enforce_shell_false(self) -> bool:
        """Whether to enforce shell=False always."""
        return self._config.get("enforce_shell_false", True)

    def check_container(self, container_name: str) -> tuple[bool, str]:
        """Check if a container is allowed. Returns (allowed, reason)."""
        if not self.allowed_containers:
            return True, "No container allowlist configured"
        if container_name in self.allowed_containers:
            return True, f"Container '{container_name}' is allowed"
        return False, f"Container '{container_name}' not in allowed list: {self.allowed_containers}"

    def check_executable(self, executable: str) -> tuple[bool, str]:
        """Check if an executable is allowed."""
        if not self.allowed_executables:
            return True, "No executable allowlist configured"
        if executable in self.allowed_executables:
            return True, f"Executable '{executable}' is allowed"
        return False, f"Executable '{executable}' not in allowed list"

    def check_script_path(self, script_path: str) -> tuple[bool, str]:
        """Check if a script path is within allowed directories."""
        if not self.allowed_script_paths:
            return True, "No script path allowlist configured"
        script = Path(script_path).resolve()
        for allowed in self.allowed_script_paths:
            if str(script).startswith(str(Path(allowed).resolve())):
                return True, f"Script path allowed under {allowed}"
        return False, f"Script path '{script_path}' not under any allowed path"

    def check_shell_mode(self, shell: bool) -> tuple[bool, str]:
        """Check if shell mode is allowed."""
        if shell and self.enforce_shell_false:
            return False, "shell=True is disabled by security policy (enforce_shell_false=true)"
        return True, "Shell mode check passed"
