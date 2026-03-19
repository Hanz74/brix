"""Python script runner — executes scripts via subprocess."""
import asyncio
import json
import time
from typing import Any

from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout


class PythonRunner(BaseRunner):
    """Runs Python scripts as subprocesses (D-19: never importlib)."""

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        script = getattr(step, 'script', None)
        if not script:
            return {"success": False, "error": "Python step needs 'script' field", "duration": 0.0}

        # Build the command: python3 <script> <json_params>
        # Params are passed as JSON string in sys.argv[1]
        params = getattr(step, 'params', {}) or {}
        # Remove internal keys (prefixed with _)
        clean_params = {k: v for k, v in params.items() if not k.startswith('_')}

        cmd = ["python3", script]
        input_data = None

        if clean_params:
            params_json = json.dumps(clean_params)
            # Use stdin for large payloads (args have OS limits ~128KB)
            if len(params_json) > 100_000:
                input_data = params_json.encode()
            else:
                cmd.append(params_json)

        # Timeout
        timeout_str = getattr(step, 'timeout', None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else 60.0

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE if input_data else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(input=input_data), timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return {
                    "success": False,
                    "error": f"Timeout after {timeout_seconds}s",
                    "duration": time.monotonic() - start,
                }

        except FileNotFoundError:
            return {
                "success": False,
                "error": f"Script not found: {script}",
                "duration": time.monotonic() - start,
            }

        duration = time.monotonic() - start

        if proc.returncode != 0:
            error_msg = stderr.decode().strip() or f"Exit code {proc.returncode}"
            return {"success": False, "error": error_msg, "duration": duration}

        # Parse stdout as JSON, fallback to raw string
        output = stdout.decode().strip()
        try:
            data = json.loads(output)
        except (json.JSONDecodeError, ValueError):
            data = output

        return {"success": True, "data": data, "duration": duration}
