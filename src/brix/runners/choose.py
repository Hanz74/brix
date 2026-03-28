"""Choose runner — multi-branch conditional execution (T-BRIX-V4-05)."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class ChooseRunner(BaseRunner):
    """Evaluates a list of conditions and executes the first matching branch."""

    def __init__(self, engine=None):
        self._engine = engine

    def set_engine(self, engine):
        """Set the parent engine (called by PipelineEngine)."""
        self._engine = engine

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "choices": {"type": "array", "description": "List of {when, steps} branches"},
                "default_steps": {"type": "array", "description": "Steps to run if no choice matches"},
            },
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "any"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        if self._engine is None:
            self.report_progress(0.0, "error: not connected to engine")
            return {"success": False, "error": "ChooseRunner not connected to engine", "duration": 0.0}

        choices = getattr(step, "choices", None) or []
        default = getattr(step, "default_steps", None) or []
        self.report_progress(0.0, f"Evaluating {len(choices)} choices")

        from brix.loader import PipelineLoader
        loader = PipelineLoader()
        jinja_ctx = context.to_jinja_context() if hasattr(context, "to_jinja_context") else {}

        # Evaluate each choice in order — execute the first matching branch
        for choice in choices:
            when = choice.get("when", "")
            if loader.evaluate_condition(when, jinja_ctx):
                branch_steps = choice.get("steps", [])
                return await self._execute_branch(branch_steps, context, start)

        # No match → default branch
        if default:
            return await self._execute_branch(default, context, start)

        # No match, no default → success with None
        self.report_progress(100.0, "done")
        return {"success": True, "data": None, "duration": time.monotonic() - start}

    async def _execute_branch(self, steps_data: list, context: Any, start: float) -> dict:
        """Build a mini-pipeline from steps_data and run it via the engine."""
        from brix.models import Pipeline
        try:
            mini = Pipeline(name="_choose_branch", steps=steps_data)
            result = await self._engine.run(mini)
            duration = time.monotonic() - start
            if result.success:
                return {"success": True, "data": result.result, "duration": duration}
            else:
                return {"success": False, "error": "choose branch failed", "duration": duration}
        except Exception as e:
            return {"success": False, "error": str(e), "duration": time.monotonic() - start}
