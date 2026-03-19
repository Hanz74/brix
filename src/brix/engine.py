"""Pipeline execution engine."""
import asyncio
import sys
import time
from typing import Any

from brix.models import Pipeline, Step, StepStatus, RunResult, RetryConfig
from brix.loader import PipelineLoader
from brix.context import PipelineContext
from brix.runners.base import BaseRunner
from brix.runners.cli import CliRunner, parse_timeout
from brix.runners.python import PythonRunner
from brix.runners.http import HttpRunner


class PipelineEngine:
    """Executes pipeline steps sequentially."""

    def __init__(self):
        self.loader = PipelineLoader()
        self._runners: dict[str, BaseRunner] = {
            "cli": CliRunner(),
            "python": PythonRunner(),
            "http": HttpRunner(),
            # mcp, pipeline runners will be added later
        }

    def register_runner(self, step_type: str, runner: BaseRunner) -> None:
        """Register a runner for a step type."""
        self._runners[step_type] = runner

    async def run(self, pipeline: Pipeline, user_input: dict = None) -> RunResult:
        """Execute a pipeline and return results."""
        start_time = time.monotonic()
        context = PipelineContext.from_pipeline(pipeline, user_input)
        step_statuses: dict[str, StepStatus] = {}
        last_output: Any = None

        for step in pipeline.steps:
            # Evaluate when condition
            jinja_ctx = context.to_jinja_context()
            if step.when:
                should_run = self.loader.evaluate_condition(step.when, jinja_ctx)
                if not should_run:
                    step_statuses[step.id] = StepStatus(
                        status="skipped", duration=0.0, reason="condition not met"
                    )
                    continue

            # Get runner
            runner = self._runners.get(step.type)
            if not runner:
                step_statuses[step.id] = StepStatus(
                    status="error", duration=0.0, errors=1
                )
                print(f"✗ {step.id}: no runner registered for type '{step.type}'", file=sys.stderr)
                effective_on_error = step.on_error or pipeline.error_handling.on_error
                if effective_on_error == "stop":
                    return RunResult(
                        success=False,
                        run_id=context.run_id,
                        steps=step_statuses,
                        result=None,
                        duration=time.monotonic() - start_time,
                    )
                continue

            # --- foreach branch ---
            if step.foreach:
                jinja_ctx = context.to_jinja_context()
                items = self.loader.resolve_foreach(step.foreach, jinja_ctx)

                step_start = time.monotonic()
                if step.parallel:
                    foreach_result = await self._run_foreach_parallel(step, items, context, pipeline)
                else:
                    foreach_result = await self._run_foreach_sequential(step, items, context, pipeline)
                step_duration = time.monotonic() - step_start

                if foreach_result["success"]:
                    context.set_output(step.id, foreach_result)
                    last_output = foreach_result
                    summary = foreach_result.get("summary", {})
                    step_statuses[step.id] = StepStatus(
                        status="ok",
                        duration=step_duration,
                        items=summary.get("total"),
                        errors=summary.get("failed") or None,
                    )
                    print(
                        f"✓ {step.id}: ok ({step_duration:.1f}s) "
                        f"[{summary.get('succeeded', 0)}/{summary.get('total', 0)} items]",
                        file=sys.stderr,
                    )
                else:
                    summary = foreach_result.get("summary", {})
                    step_statuses[step.id] = StepStatus(
                        status="error",
                        duration=step_duration,
                        errors=summary.get("failed", 1),
                    )
                    print(
                        f"✗ {step.id}: foreach failed "
                        f"({summary.get('failed', '?')} of {summary.get('total', '?')} items failed)",
                        file=sys.stderr,
                    )
                    effective_on_error = step.on_error or pipeline.error_handling.on_error
                    if effective_on_error == "stop":
                        return RunResult(
                            success=False,
                            run_id=context.run_id,
                            steps=step_statuses,
                            result=None,
                            duration=time.monotonic() - start_time,
                        )
                continue

            # --- single-step branch ---
            # Render step params with current context
            jinja_ctx = context.to_jinja_context()
            rendered_params = self.loader.render_step_params(step, jinja_ctx)

            # Create a rendered step-like object for the runner
            rendered_step = _RenderedStep(step, rendered_params, self.loader, jinja_ctx)

            step_start = time.monotonic()
            result = await self._execute_with_retry(runner, rendered_step, context, step, pipeline)
            step_duration = time.monotonic() - step_start

            if result.get("success"):
                context.set_output(step.id, result.get("data"))
                last_output = result.get("data")
                step_statuses[step.id] = StepStatus(
                    status="ok",
                    duration=step_duration,
                    items=result.get("items_count"),
                )
                print(f"✓ {step.id}: ok ({step_duration:.1f}s)", file=sys.stderr)
            else:
                step_statuses[step.id] = StepStatus(
                    status="error", duration=step_duration, errors=1
                )
                print(f"✗ {step.id}: {result.get('error', 'unknown error')}", file=sys.stderr)

                effective_on_error = step.on_error or pipeline.error_handling.on_error
                if effective_on_error == "stop":
                    return RunResult(
                        success=False,
                        run_id=context.run_id,
                        steps=step_statuses,
                        result=None,
                        duration=time.monotonic() - start_time,
                    )
                # continue: log error and move on

        # Resolve output
        final_result = last_output
        if pipeline.output:
            jinja_ctx = context.to_jinja_context()
            final_result = self.loader.render_value(pipeline.output, jinja_ctx)

        total_duration = time.monotonic() - start_time
        all_ok = all(s.status in ("ok", "skipped") for s in step_statuses.values())

        return RunResult(
            success=all_ok,
            run_id=context.run_id,
            steps=step_statuses,
            result=final_result,
            duration=total_duration,
        )

    # ------------------------------------------------------------------
    # retry helper
    # ------------------------------------------------------------------

    async def _execute_with_retry(
        self, runner: BaseRunner, rendered_step: Any, context: Any, step: Step, pipeline: Pipeline
    ) -> dict:
        """Execute a step with retry logic if on_error=retry, otherwise single execution."""
        effective_on_error = step.on_error or pipeline.error_handling.on_error

        if effective_on_error != "retry":
            # No retry — single execution
            try:
                return await runner.execute(rendered_step, context)
            except Exception as e:
                return {"success": False, "error": str(e), "duration": 0.0}

        # Retry logic
        retry_config = pipeline.error_handling.retry or RetryConfig()
        max_attempts = retry_config.max
        backoff = retry_config.backoff

        last_result: dict = {"success": False, "error": "no attempts made", "duration": 0.0}
        for attempt in range(1, max_attempts + 1):
            try:
                result = await runner.execute(rendered_step, context)
                if result.get("success"):
                    return result
                last_result = result
            except Exception as e:
                last_result = {"success": False, "error": str(e), "duration": 0.0}

            if attempt < max_attempts:
                # Calculate backoff delay
                if backoff == "exponential":
                    delay = float(2 ** (attempt - 1))  # 1, 2, 4, 8...
                else:  # linear
                    delay = float(attempt)  # 1, 2, 3, 4...
                await asyncio.sleep(delay)

        # All attempts failed
        last_result["retry_count"] = max_attempts
        return last_result

    # ------------------------------------------------------------------
    # foreach helpers
    # ------------------------------------------------------------------

    async def _run_foreach_sequential(
        self, step: Step, items: list, context: PipelineContext, pipeline: Pipeline
    ) -> dict:
        """Run foreach items one by one in order."""
        runner = self._runners.get(step.type)
        results: list[tuple[Any, dict]] = []

        for item in items:
            jinja_ctx = context.to_jinja_context(item=item)
            rendered_params = self.loader.render_step_params(step, jinja_ctx)
            rendered_step = _RenderedStep(step, rendered_params, self.loader, jinja_ctx)
            result = await self._execute_with_retry(runner, rendered_step, context, step, pipeline)
            results.append((item, result))

        return self._build_foreach_result(results, step, pipeline)

    async def _run_foreach_parallel(
        self, step: Step, items: list, context: PipelineContext, pipeline: Pipeline
    ) -> dict:
        """Run foreach items concurrently, respecting the concurrency limit."""
        runner = self._runners.get(step.type)
        semaphore = asyncio.Semaphore(step.concurrency)

        async def run_item(item: Any) -> tuple[Any, dict]:
            async with semaphore:
                jinja_ctx = context.to_jinja_context(item=item)
                rendered_params = self.loader.render_step_params(step, jinja_ctx)
                rendered_step = _RenderedStep(step, rendered_params, self.loader, jinja_ctx)
                result = await self._execute_with_retry(runner, rendered_step, context, step, pipeline)
                return item, result

        tasks = [run_item(item) for item in items]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Normalise: exceptions from gather itself become failure entries
        processed: list[tuple[Any, dict]] = []
        for idx, r in enumerate(raw_results):
            if isinstance(r, Exception):
                processed.append((items[idx], {"success": False, "error": str(r), "duration": 0.0}))
            else:
                processed.append(r)  # type: ignore[arg-type]

        return self._build_foreach_result(processed, step, pipeline)

    def _build_foreach_result(
        self, results: list[tuple[Any, dict]], step: Step, pipeline: Pipeline
    ) -> dict:
        """Aggregate per-item results into a ForeachResult-compatible dict (D-15)."""
        effective_on_error = step.on_error or pipeline.error_handling.on_error
        items: list[dict] = []
        succeeded = 0
        failed = 0
        total_duration = 0.0

        for input_item, result in results:
            total_duration += result.get("duration", 0.0)
            if result.get("success"):
                items.append({"success": True, "data": result.get("data")})
                succeeded += 1
            else:
                items.append({
                    "success": False,
                    "error": result.get("error", "unknown"),
                    "input": input_item,
                })
                failed += 1
                if effective_on_error == "stop":
                    # Fill remaining items as not-run so callers see the full picture
                    break

        total = succeeded + failed
        return {
            "items": items,
            "summary": {"total": total, "succeeded": succeeded, "failed": failed},
            "success": failed == 0 or effective_on_error == "continue",
            "duration": total_duration,
        }


class _RenderedStep:
    """Wraps a Step with rendered Jinja2 values for the runner."""

    def __init__(self, step: Step, rendered: dict, loader: PipelineLoader, jinja_ctx: dict):
        # Copy original step attributes
        self.id = step.id
        self.type = step.type
        self.timeout = step.timeout
        self.shell = step.shell

        # Use rendered values where available, fall back to originals
        self.args = rendered.get("_args") or (
            [loader.render_value(a, jinja_ctx) for a in step.args] if step.args else None
        )
        self.command = rendered.get("_command") or (
            loader.render_value(step.command, jinja_ctx) if step.command else None
        )
        self.url = rendered.get("_url") or step.url
        self.headers = rendered.get("_headers") or step.headers
        self.body = step.body
        self.method = step.method
        self.script = step.script
        self.server = step.server
        self.tool = step.tool
        self.pipeline = step.pipeline
        self.params = rendered if rendered else (step.params or {})
