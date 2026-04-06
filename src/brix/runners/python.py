"""Python script runner — executes scripts via subprocess."""
import asyncio
import json
import os
import time
from typing import Any

from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout, get_default_timeout
from brix.runners._subprocess import _terminate_subprocess

# Prefix that helper scripts write to stderr to emit intra-step progress
BRIX_PROGRESS_PREFIX = "BRIX_PROGRESS:"


def _parse_brix_progress_line(line: str) -> dict | None:
    """Parse a BRIX_PROGRESS stderr line and return the payload dict, or None."""
    stripped = line.strip()
    if not stripped.startswith(BRIX_PROGRESS_PREFIX):
        return None
    payload_str = stripped[len(BRIX_PROGRESS_PREFIX):].strip()
    try:
        payload = json.loads(payload_str)
        if isinstance(payload, dict):
            return payload
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _extract_helper_params(step: Any) -> dict[str, Any]:
    """Return only user-facing params for python/helper subprocesses.

    The Python runner accepts step fields from both ``step.params`` and the
    legacy ``step.config`` shape. Helpers should receive only the nested
    ``params`` payload, not runner-internal fields like ``helper`` or
    ``script``.
    """
    def _step_value(field: str) -> Any:
        if isinstance(step, dict):
            return step.get(field)
        return getattr(step, field, None)

    params = _step_value("params")
    if isinstance(params, dict):
        source_params = dict(params)
    else:
        source_params = {}

    config = _step_value("config")
    if not source_params and isinstance(config, dict):
        config_params = config.get("params")
        if isinstance(config_params, dict):
            source_params = dict(config_params)

    return {
        key: value
        for key, value in source_params.items()
        if not key.startswith("_") and key not in {"helper", "script"}
    }


class PythonRunner(BaseRunner):
    """Runs Python scripts as subprocesses (D-19: never importlib)."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "script": {"type": "string", "description": "Path to Python script"},
                "helper": {"type": "string", "description": "Registered helper name"},
                "params": {"type": "object", "description": "Parameters passed as JSON to the script"},
                "timeout": {"type": "string", "description": "Timeout e.g. '30s', '5m', '1h'"},
                "progress": {"type": "boolean", "description": "Parse BRIX_PROGRESS lines from stderr"},
            },
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        script = config.get("script")
        helper = config.get("helper")
        if not script and not helper:
            errors.append("Python runner requires either 'script' or 'helper'")
        return errors

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "any"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        def _step_value(field: str) -> Any:
            if isinstance(step, dict):
                return step.get(field)
            return getattr(step, field, None)

        params = _extract_helper_params(step)

        config = _step_value("config") or {}
        if not isinstance(config, dict):
            config = {}

        script = (
            _step_value("script")
            or params.get("script")
            or config.get("script")
        )
        helper = (
            _step_value("helper")
            or params.get("helper")
            or config.get("helper")
        )
        if not script and not helper:
            return {"success": False, "error": "Python step needs 'script' or 'helper' field", "duration": 0.0}

        if not script and helper:
            try:
                from brix.helper_registry import HelperRegistry

                registry = HelperRegistry()
                entry = registry.get(helper)
            except Exception as exc:
                return {
                    "success": False,
                    "error": f"Failed to resolve helper '{helper}' from registry: {exc}",
                    "duration": time.monotonic() - start,
                }

            if entry is None:
                return {
                    "success": False,
                    "error": f"Helper '{helper}' not found in registry",
                    "duration": time.monotonic() - start,
                }

            script = getattr(entry, "script", None)
            if not script:
                return {
                    "success": False,
                    "error": f"Helper '{helper}' has no script path in registry",
                    "duration": time.monotonic() - start,
                }

        # Build the command: python3 <script> <json_params>
        # Params are passed as JSON string in sys.argv[1]
        clean_params = params

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
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else get_default_timeout("python")

        # Determine whether to parse BRIX_PROGRESS lines from stderr
        track_progress = bool(getattr(step, 'progress', False))

        # Inject pipeline credentials as environment variables for the subprocess.
        # This allows helpers (and their sub-processes) to read secrets via os.environ
        # without needing explicit parameter passing through the call chain.
        env = None
        if context is not None and hasattr(context, 'credentials') and context.credentials:
            env = {**os.environ, **{k: str(v) for k, v in context.credentials.items() if v}}

        # Inject BRIX_RUN_WORKDIR so helper scripts can call sdk.is_cancelled()
        # (T-BRIX-V6-BUG-03: cancel_run support)
        if context is not None and hasattr(context, 'workdir') and context.workdir:
            if env is None:
                env = dict(os.environ)
            env["BRIX_RUN_WORKDIR"] = str(context.workdir)

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE if input_data else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
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
            except asyncio.CancelledError:
                # Run was cancelled — kill the subprocess (INBOX-510)
                await _terminate_subprocess(proc)
                raise

        except FileNotFoundError:
            return {
                "success": False,
                "error": f"Script not found: {script}",
                "duration": time.monotonic() - start,
            }

        duration = time.monotonic() - start

        # Process stderr: split BRIX_PROGRESS lines from normal stderr
        stderr_text = stderr.decode()
        normal_stderr_lines: list[str] = []
        if track_progress and context is not None:
            last_progress: dict | None = None
            for line in stderr_text.splitlines():
                payload = _parse_brix_progress_line(line)
                if payload is not None:
                    last_progress = payload
                else:
                    normal_stderr_lines.append(line)
            # Store the last progress payload in context
            if last_progress is not None and hasattr(context, 'update_step_progress'):
                context.update_step_progress(step.id, last_progress)
            normal_stderr = "\n".join(normal_stderr_lines)
        else:
            normal_stderr = stderr_text

        if proc.returncode != 0:
            error_msg = normal_stderr.strip() or f"Exit code {proc.returncode}"
            return {"success": False, "error": error_msg, "duration": duration, "stderr": normal_stderr}

        # Parse stdout as JSON, fallback to raw string
        output = stdout.decode().strip()
        try:
            data = json.loads(output)
        except (json.JSONDecodeError, ValueError):
            data = output

        self.report_progress(100.0, "done")
        return {"success": True, "data": data, "duration": duration, "stderr": normal_stderr}
