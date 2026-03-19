"""CLI runner — shell command execution."""

import asyncio
import json
import time
from typing import Any

from .base import BaseRunner


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


class CliRunner(BaseRunner):
    """Runner for CLI commands via asyncio subprocess."""

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

        if args:
            # Safe mode: args list (shell=False)
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        elif command:
            # Shell mode (explicit opt-in)
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
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
            timeout_seconds = 60.0

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

        return {"success": True, "data": data, "duration": duration}
