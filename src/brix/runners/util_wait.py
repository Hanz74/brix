"""Util Wait runner — simple sleep/delay brick."""
import asyncio
import time
from typing import Any

from brix.runners.base import BaseRunner

_MAX_WAIT_SECONDS = 3600


class UtilWaitRunner(BaseRunner):
    """Simple sleep/delay brick.

    Pipeline YAML example::

        - id: pause
          type: util.wait
          params:
            seconds: 10

    Returns::

        {"success": true, "data": {"waited": 10.0, "success": true}, "duration": 10.0}
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "seconds": {
                    "type": "number",
                    "description": "Number of seconds to wait (max 3600)",
                },
            },
            "required": ["seconds"],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        seconds = config.get("seconds")
        if seconds is not None:
            try:
                val = float(seconds)
                if val < 0:
                    errors.append("'seconds' must be >= 0")
                if val > _MAX_WAIT_SECONDS:
                    errors.append(f"'seconds' must be <= {_MAX_WAIT_SECONDS}")
            except (TypeError, ValueError):
                errors.append("'seconds' must be a number")
        return errors

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        seconds_raw = params.get("seconds")

        if seconds_raw is None:
            self.report_progress(0.0, "error: missing seconds")
            return {
                "success": False,
                "error": "util.wait requires 'seconds'",
                "duration": time.monotonic() - start,
            }

        try:
            seconds = float(seconds_raw)
        except (TypeError, ValueError):
            return {
                "success": False,
                "error": f"'seconds' must be a number, got: {seconds_raw!r}",
                "duration": time.monotonic() - start,
            }

        # Cap at maximum
        if seconds > _MAX_WAIT_SECONDS:
            seconds = _MAX_WAIT_SECONDS
        if seconds < 0:
            seconds = 0

        self.report_progress(0.0, f"Waiting {seconds}s")

        if seconds > 0:
            # Sleep in chunks for progress reporting
            chunk = min(1.0, seconds / 10) if seconds > 0 else 0
            elapsed = 0.0
            while elapsed < seconds:
                remaining = seconds - elapsed
                await asyncio.sleep(min(chunk, remaining))
                elapsed = time.monotonic() - start
                pct = min(99.0, (elapsed / seconds) * 100.0)
                self.report_progress(pct, f"Waiting {seconds}s — {elapsed:.1f}s elapsed")

        waited = time.monotonic() - start
        self.report_progress(100.0, f"Done — waited {waited:.2f}s")
        return {
            "success": True,
            "data": {"waited": round(waited, 3), "success": True},
            "duration": waited,
        }
