"""Error handler runner — try/catch style step execution (T-BRIX-DB-17)."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class ErrorHandlerRunner(BaseRunner):
    """Executes a primary step; on failure runs a handler step instead.

    Pipeline YAML example::

        - id: safe_fetch
          type: error_handler
          try_step: fetch_data
          handler_step: fallback_data

    Returns::

        {
          "success": true,
          "data": {
            "success": true|false,
            "result": ...,        # output of the executed step
            "error": null|str,    # error message if try_step failed
            "used_handler": false|true
          },
          "duration": 0.123
        }

    Note: The overall execute() returns ``success=True`` even when
    ``try_step`` fails and ``handler_step`` is run — the runner itself
    succeeded (it handled the error).  Only if the handler itself fails
    does the outer ``success`` become ``False``.
    """

    def __init__(self, engine=None):
        self._engine = engine

    def set_engine(self, engine):
        """Set the parent engine (called by PipelineEngine)."""
        self._engine = engine

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "try_step": {
                    "type": "string",
                    "description": "ID of the step to attempt first",
                },
                "handler_step": {
                    "type": "string",
                    "description": "ID of the step to run when try_step fails",
                },
            },
            "required": ["try_step"],
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        if self._engine is None:
            return {
                "success": False,
                "error": "ErrorHandlerRunner not connected to engine",
                "duration": 0.0,
            }

        try_step_id = getattr(step, "try_step", None)
        handler_step_id = getattr(step, "handler_step", None)

        if not try_step_id:
            self.report_progress(100.0, "error")
            return {
                "success": False,
                "error": "ErrorHandlerRunner: 'try_step' config is required",
                "duration": time.monotonic() - start,
            }

        self.report_progress(10.0, f"trying step={try_step_id}")

        # --- Try the primary step ---
        try_result, try_error = await self._run_step_by_id(try_step_id, context)

        if try_result is not None and try_result.get("success"):
            # Primary step succeeded
            self.report_progress(100.0, "done — try succeeded")
            return {
                "success": True,
                "data": {
                    "success": True,
                    "result": try_result.get("data"),
                    "error": None,
                    "used_handler": False,
                },
                "duration": time.monotonic() - start,
            }

        # Primary step failed
        captured_error = try_error or (try_result.get("error") if try_result else "unknown error")
        self.report_progress(50.0, f"try failed, running handler={handler_step_id}")

        if not handler_step_id:
            # No handler — surface the original error
            self.report_progress(100.0, "done — no handler")
            return {
                "success": True,
                "data": {
                    "success": False,
                    "result": None,
                    "error": captured_error,
                    "used_handler": False,
                },
                "duration": time.monotonic() - start,
            }

        # --- Run the handler step ---
        handler_result, handler_error = await self._run_step_by_id(handler_step_id, context)

        if handler_result is not None and handler_result.get("success"):
            self.report_progress(100.0, "done — handler succeeded")
            return {
                "success": True,
                "data": {
                    "success": False,
                    "result": handler_result.get("data"),
                    "error": captured_error,
                    "used_handler": True,
                },
                "duration": time.monotonic() - start,
            }

        # Handler itself failed
        handler_err_msg = handler_error or (handler_result.get("error") if handler_result else "handler failed")
        self.report_progress(100.0, "error — handler also failed")
        return {
            "success": False,
            "error": f"ErrorHandlerRunner: handler step also failed: {handler_err_msg}",
            "data": {
                "success": False,
                "result": None,
                "error": captured_error,
                "used_handler": True,
            },
            "duration": time.monotonic() - start,
        }

    async def _run_step_by_id(self, step_id: str, context: Any) -> tuple[dict | None, str | None]:
        """Look up a step by ID in the engine and execute it.

        Returns (result_dict, error_str).  On exception: (None, str(exc)).
        """
        try:
            # The engine keeps the full list of resolved steps accessible.
            # We look up the step and run it through the engine's step executor.
            step_obj = self._engine._find_step(step_id) if hasattr(self._engine, "_find_step") else None

            if step_obj is None:
                # Fallback: ask engine to run a single-step mini-pipeline
                from brix.models import Pipeline
                mini = Pipeline(name=f"_error_handler_step_{step_id}", steps=[{"id": step_id, "type": "set", "values": {}}])
                mini_result = await self._engine.run(mini)
                if mini_result.success:
                    return {"success": True, "data": mini_result.result}, None
                return {"success": False, "error": "step not found or failed"}, None

            result = await self._engine._execute_step(step_obj, context)
            return result, None
        except Exception as exc:
            return None, str(exc)
