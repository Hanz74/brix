"""Pipeline execution engine."""
import sys
import time
from typing import Any

from brix.models import Pipeline, Step, StepStatus, RunResult
from brix.loader import PipelineLoader
from brix.context import PipelineContext
from brix.runners.base import BaseRunner
from brix.runners.cli import CliRunner
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

            # Render step params with current context
            jinja_ctx = context.to_jinja_context()
            rendered_params = self.loader.render_step_params(step, jinja_ctx)

            # Create a rendered step-like object for the runner
            rendered_step = _RenderedStep(step, rendered_params, self.loader, jinja_ctx)

            step_start = time.monotonic()
            try:
                result = await runner.execute(rendered_step, context)
            except Exception as e:
                result = {"success": False, "error": str(e), "duration": time.monotonic() - step_start}

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
