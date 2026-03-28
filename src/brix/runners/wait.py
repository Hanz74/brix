"""Wait runner — time delay or condition-based polling (T-BRIX-DB-17)."""
import asyncio
import time
from typing import Any

from brix.runners.base import BaseRunner

_DEFAULT_POLL_INTERVAL = 5.0   # seconds between until-condition checks
_DEFAULT_MAX_TIMEOUT = 3600.0  # 1 hour maximum wait


class WaitRunner(BaseRunner):
    """Pauses pipeline execution for a fixed duration or until a condition is met.

    **Fixed delay** — wait a specific number of seconds::

        - id: pause
          type: wait
          seconds: 30

    **Condition polling** — poll until a Jinja2 expression becomes truthy::

        - id: wait_ready
          type: wait
          until: "{{ check.output.status == 'ready' }}"
          poll_interval: 5
          timeout: 300

    Returns::

        {
          "success": true,
          "data": {
            "waited_seconds": 12.3,
            "condition_met": true,    # for until mode
            "timed_out": false
          },
          "duration": 12.3
        }
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "seconds": {
                    "type": "number",
                    "description": "Fixed number of seconds to wait",
                },
                "until": {
                    "type": "string",
                    "description": "Jinja2 boolean expression; wait until it becomes truthy",
                },
                "poll_interval": {
                    "type": "number",
                    "description": f"Seconds between condition checks (default {_DEFAULT_POLL_INTERVAL})",
                },
                "timeout": {
                    "type": "number",
                    "description": f"Maximum seconds to wait (default {_DEFAULT_MAX_TIMEOUT})",
                },
            },
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        seconds = getattr(step, "seconds", None)
        until_expr = getattr(step, "until", None)
        poll_interval = float(getattr(step, "poll_interval", None) or _DEFAULT_POLL_INTERVAL)
        timeout = float(getattr(step, "timeout", None) or _DEFAULT_MAX_TIMEOUT)

        if seconds is not None:
            return await self._wait_seconds(float(seconds), start)
        elif until_expr:
            return await self._wait_until(until_expr, poll_interval, timeout, context, start)
        else:
            # Nothing configured — return immediately (no-op wait)
            self.report_progress(100.0, "done — nothing to wait for")
            return {
                "success": True,
                "data": {"waited_seconds": 0.0, "condition_met": None, "timed_out": False},
                "duration": time.monotonic() - start,
            }

    async def _wait_seconds(self, seconds: float, start: float) -> dict:
        """Sleep for a fixed number of seconds with progress reporting."""
        self.report_progress(0.0, f"waiting {seconds}s")

        if seconds <= 0:
            self.report_progress(100.0, "done")
            return {
                "success": True,
                "data": {"waited_seconds": 0.0, "condition_met": None, "timed_out": False},
                "duration": time.monotonic() - start,
            }

        # Sleep in small chunks to allow progress updates
        chunk = min(1.0, seconds / 10)
        elapsed = 0.0
        while elapsed < seconds:
            await asyncio.sleep(min(chunk, seconds - elapsed))
            elapsed = time.monotonic() - start
            pct = min(99.0, (elapsed / seconds) * 100.0)
            self.report_progress(pct, f"waiting {seconds}s — {elapsed:.1f}s elapsed")

        waited = time.monotonic() - start
        self.report_progress(100.0, f"done — slept {waited:.2f}s")
        return {
            "success": True,
            "data": {"waited_seconds": waited, "condition_met": None, "timed_out": False},
            "duration": waited,
        }

    async def _wait_until(
        self,
        until_expr: str,
        poll_interval: float,
        timeout: float,
        context: Any,
        start: float,
    ) -> dict:
        """Poll the until expression every poll_interval seconds."""
        from brix.loader import PipelineLoader
        loader = PipelineLoader()

        self.report_progress(0.0, f"polling until='{until_expr}'")

        poll_count = 0
        while True:
            elapsed = time.monotonic() - start

            # Check timeout
            if elapsed >= timeout:
                self.report_progress(100.0, "timed out")
                return {
                    "success": True,
                    "data": {
                        "waited_seconds": elapsed,
                        "condition_met": False,
                        "timed_out": True,
                        "poll_count": poll_count,
                    },
                    "duration": elapsed,
                }

            # Evaluate condition
            jinja_ctx = (
                context.to_jinja_context()
                if (context and hasattr(context, "to_jinja_context"))
                else {}
            )
            try:
                condition_met = loader.evaluate_condition(until_expr, jinja_ctx)
            except Exception as e:
                self.report_progress(100.0, "error")
                return {
                    "success": False,
                    "error": f"WaitRunner: until expression error: {e}",
                    "duration": time.monotonic() - start,
                }

            if condition_met:
                waited = time.monotonic() - start
                self.report_progress(100.0, "condition met")
                return {
                    "success": True,
                    "data": {
                        "waited_seconds": waited,
                        "condition_met": True,
                        "timed_out": False,
                        "poll_count": poll_count,
                    },
                    "duration": waited,
                }

            # Wait before next poll
            remaining = timeout - elapsed
            sleep_time = min(poll_interval, remaining)
            poll_count += 1
            pct = min(90.0, (elapsed / timeout) * 100.0)
            self.report_progress(pct, f"polling... elapsed={elapsed:.1f}s polls={poll_count}")
            await asyncio.sleep(sleep_time)
