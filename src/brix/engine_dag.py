"""DAG scheduling helpers for the pipeline engine."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from brix.context import PipelineContext
from brix.engine_step import StepExecutor
from brix.engine_types import DagSharedState, _build_logger
from brix.models import Pipeline, Step, StepStatus

if TYPE_CHECKING:
    from brix.engine import PipelineEngine

logger = _build_logger("brix.engine")


def detect_dag_mode(steps: list[Step]) -> bool:
    """Return True if any step declares depends_on."""
    return any(bool(step.depends_on) for step in steps)


def toposort_steps(steps: list[Step]) -> list[Step]:
    """Return steps in topological order (Kahn's algorithm).

    Raises ``ValueError`` if a dependency references an unknown step ID or
    if the dependency graph contains a cycle.
    """
    step_by_id: dict[str, Step] = {step.id: step for step in steps}

    for step in steps:
        for dep in step.depends_on:
            if dep not in step_by_id:
                raise ValueError(
                    f"Step '{step.id}' depends_on unknown step '{dep}'"
                )

    in_degree: dict[str, int] = {step.id: 0 for step in steps}
    dependents: dict[str, list[str]] = {step.id: [] for step in steps}
    for step in steps:
        for dep in step.depends_on:
            in_degree[step.id] += 1
            dependents[dep].append(step.id)

    from collections import deque

    queue: deque[str] = deque(step_id for step_id, deg in in_degree.items() if deg == 0)
    sorted_ids: list[str] = []

    while queue:
        step_id = queue.popleft()
        sorted_ids.append(step_id)
        for dependent in dependents[step_id]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(sorted_ids) != len(steps):
        cycled = [step_id for step_id, deg in in_degree.items() if deg > 0]
        raise ValueError(
            f"Cycle detected in depends_on graph involving step(s): {', '.join(sorted(cycled))}"
        )

    return [step_by_id[step_id] for step_id in sorted_ids]


async def run_dag(
    engine: PipelineEngine,
    pipeline: Pipeline,
    context: PipelineContext,
    step_statuses: dict[str, StepStatus],
    dry_run_steps: list[str] | None,
    dag_state: DagSharedState,
) -> tuple[bool, Any, bool, bool | None]:
    """Execute pipeline steps in DAG order.

    Steps without unsatisfied dependencies are dispatched concurrently.
    Each step waits until all its dependencies have completed successfully.

    Returns ``(pipeline_aborted, last_output, aborted_flag, stop_step_success)``.
    """
    steps = pipeline.steps

    # Validate references and cycles before launching tasks.
    toposort_steps(steps)

    done_events: dict[str, asyncio.Event] = {step.id: asyncio.Event() for step in steps}
    step_ok: dict[str, bool] = {}
    executor = StepExecutor(engine)

    async def run_step(step: Step) -> None:
        for dep_id in step.depends_on:
            await done_events[dep_id].wait()
            if not step_ok.get(dep_id, False):
                step_statuses[step.id] = StepStatus(
                    status="skipped",
                    duration=0.0,
                    reason=f"dependency '{dep_id}' failed",
                )
                engine.progress.step_skipped(step.id)
                step_ok[step.id] = False
                done_events[step.id].set()
                return

        if dag_state.pipeline_aborted:
            step_ok[step.id] = False
            done_events[step.id].set()
            return

        if context.is_step_completed(step.id):
            step_statuses[step.id] = StepStatus(status="ok", duration=0.0)
            dag_state.last_output = context.get_output(step.id)
            engine.progress.step_resumed(step.id)
            step_ok[step.id] = True
            done_events[step.id].set()
            return

        if engine._is_run_cancelled(context):
            dag_state.pipeline_aborted = True
            for event in done_events.values():
                event.set()
            step_ok[step.id] = False
            done_events[step.id].set()
            return

        result = await executor.execute_step(
            step=step,
            context=context,
            pipeline=pipeline,
            step_statuses=step_statuses,
            jinja_ctx=context.to_jinja_context(),
            dry_run_steps=dry_run_steps,
        )

        if result.output is not None:
            dag_state.last_output = result.output
        dag_state.total_cost_usd += result.cost

        if result.should_abort:
            dag_state.pipeline_aborted = True
            if step.type == "stop":
                dag_state.stop_step_success = getattr(step, "success_on_stop", True)
            step_ok[step.id] = False
            for event in done_events.values():
                event.set()
            done_events[step.id].set()
            return

        step_status = step_statuses.get(step.id)
        step_ok[step.id] = bool(
            step_status is not None
            and step_status.status in ("ok", "skipped", "dry_run")
        )
        done_events[step.id].set()

    tasks = [asyncio.create_task(run_step(step)) for step in steps]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            logger.error("DAG task raised: %s", result, exc_info=result)
            dag_state.pipeline_aborted = True

    return (
        dag_state.pipeline_aborted,
        dag_state.last_output,
        dag_state.pipeline_aborted,
        dag_state.stop_step_success,
    )
