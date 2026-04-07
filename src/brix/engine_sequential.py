"""Sequential pipeline execution loop."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from brix.engine_step import StepExecutor
from brix.models import Pipeline, StepStatus

if TYPE_CHECKING:
    from brix.context import PipelineContext
    from brix.engine import PipelineEngine


async def run_pipeline_sequential(
    engine: "PipelineEngine",
    pipeline: Pipeline,
    context: "PipelineContext",
    step_statuses: dict[str, StepStatus],
    dry_run_steps: list[str] | None = None,
) -> tuple[bool, Any, float, bool | None]:
    """Execute pipeline steps in-order using ``StepExecutor``."""
    pipeline_aborted = False
    stop_step_success: bool | None = None
    total_cost_usd = 0.0
    last_output: Any = None
    step_executor = StepExecutor(engine)

    for step in pipeline.steps:
        if engine._is_run_cancelled(context):
            pipeline_aborted = True
            break

        if context.is_step_completed(step.id):
            step_statuses[step.id] = StepStatus(status="ok", duration=0.0)
            last_output = context.get_output(step.id)
            engine.progress.step_resumed(step.id)
            continue

        jinja_ctx = context.to_jinja_context()
        step_result = await step_executor.execute_step(
            step=step,
            context=context,
            pipeline=pipeline,
            step_statuses=step_statuses,
            jinja_ctx=jinja_ctx,
            dry_run_steps=dry_run_steps,
        )
        if step_result.output is not None:
            last_output = step_result.output
        total_cost_usd += step_result.cost
        if step_result.should_abort:
            pipeline_aborted = True
            if step_statuses.get(step.id, None) and step.type == "stop":
                stop_step_success = getattr(step, "success_on_stop", True)
            break
        if not step_result.should_continue:
            break

    return pipeline_aborted, last_output, total_cost_usd, stop_step_success
