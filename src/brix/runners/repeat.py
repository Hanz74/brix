"""Repeat runner — while/until loop execution (T-BRIX-V4-07)."""
import asyncio
import time
from typing import Any

from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout, get_default_timeout


class RepeatRunner(BaseRunner):
    """Executes a sequence of steps repeatedly until a condition is met."""

    def __init__(self, engine=None):
        self._engine = engine

    def set_engine(self, engine):
        """Set the parent engine (called by PipelineEngine)."""
        self._engine = engine

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "sequence": {"type": "array", "description": "Steps to execute in each iteration"},
                "until": {"type": "string", "description": "Jinja2 condition to stop when true"},
                "while_condition": {"type": "string", "description": "Jinja2 condition to continue while true"},
                "max_iterations": {"type": "integer", "description": "Maximum loop iterations (default 100)"},
                "timeout": {"type": "string", "description": "Total timeout for the loop e.g. '1h'"},
            },
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "any"

    async def _run_loop(self, step: Any, context: Any, start: float) -> dict:
        """Inner loop body — extracted so it can be wrapped with asyncio.wait_for."""
        sequence = getattr(step, "sequence", None) or []
        until_cond = getattr(step, "until", None)
        while_cond = getattr(step, "while_condition", None)
        max_iter = getattr(step, "max_iterations", 100)

        from brix.models import Pipeline
        from brix.loader import PipelineLoader
        loader = PipelineLoader()

        results = []
        # Max iterations is known upfront for bounded loops; 0 = unknown (while only)
        _known_max = max_iter if not while_cond else 0
        self.report_progress(0.0, f"Starting repeat loop (max {max_iter} iterations)", done=0, total=_known_max)

        for i in range(max_iter):
            # Pre-check: while condition must be true to enter loop body
            if while_cond:
                jinja_ctx = context.to_jinja_context() if hasattr(context, "to_jinja_context") else {}
                jinja_ctx["repeat"] = {"index": i, "first": i == 0}
                try:
                    if not loader.evaluate_condition(while_cond, jinja_ctx):
                        break
                except Exception as e:
                    # BUG-3: surface UndefinedError and similar as real error_message
                    total_duration = time.monotonic() - start
                    return {
                        "success": False,
                        "error": f"while condition error: {e}",
                        "duration": total_duration,
                        "iterations": i,
                    }

            # Execute the sequence as a mini-pipeline
            try:
                mini = Pipeline(name=f"_repeat_{i}", steps=sequence)
                # Propagate the parent context's resolved inputs into the mini-pipeline
                # so that {{ input.* }} templates inside sub-steps resolve correctly.
                # We use _inherit_input (not user_input) because the mini-pipeline has
                # no declared input spec — user_input is only merged for declared keys
                # (T-BRIX-V4-BUG-INPUT).
                parent_input = context.input if hasattr(context, "input") else {}
                result = await self._engine.run(mini, _inherit_input=parent_input, mcp_pool=self._engine._mcp_pool)
                results.append(result)

                # BUG-1 + BUG-2: merge sub-step outputs into the parent context so that:
                #  - the until/while condition can reference them ({{ check.output.status }})
                #  - subsequent steps after the repeat block can reference them
                sub_outputs = dict(self._engine._last_step_outputs)
                if hasattr(context, "set_output"):
                    for sub_step_id, sub_output in sub_outputs.items():
                        context.set_output(sub_step_id, sub_output)

            except Exception as e:
                # Build a failure placeholder so we can still report iterations
                from brix.models import RunResult
                dummy = RunResult(
                    success=False,
                    run_id=f"repeat_err_{i}",
                    steps={},
                    result=None,
                    duration=0.0,
                )
                results.append(dummy)

            # Report per-iteration progress
            _iter_done = len(results)
            _iter_pct = round(_iter_done / max_iter * 100, 1) if max_iter > 0 else 0.0
            self.report_progress(
                _iter_pct,
                f"Iteration {_iter_done}/{max_iter}",
                done=_iter_done,
                total=max_iter,
            )

            # Post-check: until condition — stop when true
            if until_cond:
                # Build jinja_ctx AFTER merging sub-step outputs so until can see them
                jinja_ctx = context.to_jinja_context() if hasattr(context, "to_jinja_context") else {}
                jinja_ctx["repeat"] = {"index": i, "first": i == 0, "last": True}
                try:
                    if loader.evaluate_condition(until_cond, jinja_ctx):
                        break
                except Exception as e:
                    # BUG-3: surface UndefinedError and similar as real error_message
                    total_duration = time.monotonic() - start
                    return {
                        "success": False,
                        "error": f"until condition error: {e}",
                        "duration": total_duration,
                        "iterations": len(results),
                    }

        last_result = results[-1] if results else None
        total_duration = time.monotonic() - start
        all_ok = all(r.success for r in results) if results else True

        ret: dict = {
            "success": all_ok,
            "data": last_result.result if last_result else None,
            "duration": total_duration,
            "iterations": len(results),
        }

        if not all_ok:
            # Find the last failed sub-run and surface its error message so that
            # the engine never falls back to the generic "unknown error" string.
            last_failed = next(
                (r for r in reversed(results) if not r.success), None
            )
            if last_failed is not None:
                # Collect error messages from the failed run's steps
                failed_msgs = [
                    s.error_message
                    for s in last_failed.steps.values()
                    if s.error_message
                ]
                ret["error"] = failed_msgs[-1] if failed_msgs else "repeat sub-step failed"
            else:
                ret["error"] = "repeat sub-step failed"

        return ret

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        if self._engine is None:
            return {"success": False, "error": "RepeatRunner not connected to engine", "duration": 0.0}

        # Resolve timeout for the whole repeat block
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else get_default_timeout("repeat")

        try:
            result = await asyncio.wait_for(
                self._run_loop(step, context, start),
                timeout=timeout_seconds,
            )
            self.report_progress(100.0, "done")
            return result
        except asyncio.TimeoutError:
            return {
                "success": False,
                "error": f"Timeout after {timeout_seconds}s",
                "duration": time.monotonic() - start,
            }
