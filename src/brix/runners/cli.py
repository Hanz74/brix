"""CLI runner — shell command execution."""

import asyncio
import json
import os
import time
from typing import Any

from .base import BaseRunner
from brix.config import config


def parse_timeout(timeout_str: str) -> float:
    """Parse timeout string like '30s', '5m', '1h' to seconds.

    Args:
        timeout_str: Timeout string with unit suffix (s, m, h).

    Returns:
        Number of seconds as float.

    Raises:
        ValueError: If the format is invalid.
    """
    timeout_str = timeout_str.strip()
    if timeout_str.endswith("h"):
        return float(timeout_str[:-1]) * 3600
    elif timeout_str.endswith("m"):
        return float(timeout_str[:-1]) * 60
    elif timeout_str.endswith("s"):
        return float(timeout_str[:-1])
    else:
        # Bare number — treat as seconds
        return float(timeout_str)


# Per-step-type default timeouts (seconds).  Applied when no explicit
# ``timeout:`` is set on a step.  All values are read from BrixConfig so
# they can be overridden via environment variables at runtime.
def _build_step_type_timeouts() -> dict[str, float]:
    return {
        "python":   config.TIMEOUT_PYTHON,
        "cli":      config.TIMEOUT_CLI,
        "mcp":      config.TIMEOUT_MCP,
        "http":     config.TIMEOUT_HTTP,
        "repeat":   config.TIMEOUT_REPEAT,
        "approval": config.TIMEOUT_APPROVAL,
    }


_STEP_TYPE_DEFAULT_TIMEOUTS: dict[str, float] = _build_step_type_timeouts()
_DEFAULT_TIMEOUT_FALLBACK = config.TIMEOUT_DEFAULT


def get_default_timeout(step_type: str) -> float:
    """Return the default timeout in seconds for a given step type.

    Explicit ``timeout:`` values on a step always take precedence; this
    function is only called when no timeout has been specified.

    Args:
        step_type: The step ``type`` field (e.g. ``"python"``, ``"cli"``).

    Returns:
        Default timeout in seconds as a float.
    """
    return _STEP_TYPE_DEFAULT_TIMEOUTS.get(step_type, _DEFAULT_TIMEOUT_FALLBACK)


class CliRunner(BaseRunner):
    """Runner for CLI commands via asyncio subprocess."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "args": {"type": "array", "items": {"type": "string"}, "description": "Command as list (shell=False)"},
                "command": {"type": "string", "description": "Shell command string (shell=True)"},
                "timeout": {"type": "string", "description": "Timeout e.g. '30s', '5m'"},
            },
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "any"

    async def execute(self, step: Any, context: Any) -> dict:
        """Execute a CLI step.

        Two modes (D-14):
        - args list (default, shell=False): step.args = ["cmd", "arg1"]
        - command string (opt-in, shell=True): step.command = "cmd | grep x"

        Returns:
            dict with success, data (JSON-parsed or raw string), duration.
        """
        start = time.monotonic()

        # Determine mode
        args = getattr(step, "args", None)
        command = getattr(step, "command", None)
        raw_timeout = getattr(step, "timeout", None)

        # Inject pipeline credentials as environment variables for the subprocess.
        env = None
        if context is not None and hasattr(context, 'credentials') and context.credentials:
            env = {**os.environ, **{k: str(v) for k, v in context.credentials.items() if v}}

        if args:
            # Safe mode: args list (shell=False)
            # Jinja2 may render values as int/float — str()-cast all elements.
            proc = await asyncio.create_subprocess_exec(
                *[str(a) for a in args],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
        elif command:
            # Shell mode (explicit opt-in)
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
        else:
            return {
                "success": False,
                "error": "CLI step needs 'args' or 'command'",
                "duration": 0.0,
            }

        # Resolve timeout
        timeout_seconds: float
        if raw_timeout:
            timeout_seconds = parse_timeout(str(raw_timeout))
        else:
            timeout_seconds = get_default_timeout("cli")

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout_seconds
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {
                "success": False,
                "error": f"Timeout after {timeout_seconds}s",
                "duration": time.monotonic() - start,
            }

        duration = time.monotonic() - start

        if proc.returncode != 0:
            return {
                "success": False,
                "error": stderr.decode().strip() or f"Exit code {proc.returncode}",
                "duration": duration,
            }

        # Try JSON parse, fallback to raw string
        output = stdout.decode().strip()
        try:
            data = json.loads(output)
        except (json.JSONDecodeError, ValueError):
            data = output

        self.report_progress(100.0, "done")
        return {"success": True, "data": data, "duration": duration}
