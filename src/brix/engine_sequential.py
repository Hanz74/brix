"""Sequential pipeline execution loop."""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING, Any

from brix.engine_step import StepExecutor
from brix.engine_types import _build_logger, _db_log
from brix.models import Pipeline, RunResult, StepStatus

if TYPE_CHECKING:
    from brix.context import PipelineContext
    from brix.engine import PipelineEngine

logger = _build_logger("brix.engine_sequential")


async def finalize_run(
    engine,
    pipeline: Pipeline,
    context: "PipelineContext",
    step_statuses: dict[str, StepStatus],
    pipeline_aborted: bool,
    stop_step_success: bool | None,
    last_output: Any,
    total_cost_usd: float,
    start_time: float,
    history,
    keep_workdir: bool,
    deprecation_warnings: list[str],
) -> RunResult:
    """Finalize one pipeline run and build the public RunResult."""
    final_result = None
    if not pipeline_aborted and pipeline.output:
        jinja_ctx = context.to_jinja_context()
        final_result = engine.loader.render_value(pipeline.output, jinja_ctx)
    elif not pipeline_aborted:
        final_result = last_output

    total_duration = time.monotonic() - start_time
    if stop_step_success is not None:
        all_ok = stop_step_success
    else:
        all_ok = (not pipeline_aborted) and all(
            s.status in ("ok", "skipped", "dry_run") for s in step_statuses.values()
        )

    was_cancelled = engine._is_run_cancelled(context)
    if was_cancelled:
        context.save_run_metadata(pipeline.name, "cancelled")
    else:
        context.save_run_metadata(pipeline.name, "completed" if all_ok else "failed")
    if all_ok and not was_cancelled:
        context.cleanup(keep=keep_workdir)

    engine.progress.pipeline_done(pipeline.name, all_ok, total_duration, len(pipeline.steps))

    try:
        steps_summary: dict[str, dict[str, Any]] = {}
        for k, v in step_statuses.items():
            d = v.model_dump()
            entry: dict[str, Any] = {
                "status": d["status"],
                "duration": d.get("duration"),
                "items": d.get("items"),
                "errors": d.get("errors"),
            }
            if d.get("error_message") is not None:
                entry["error_message"] = d["error_message"]
            if d.get("resource_usage") is not None:
                entry["resource_usage"] = d["resource_usage"]
            steps_summary[k] = entry
        if was_cancelled:
            cancel_reason = ""
            try:
                import json as _json

                sentinel_path = context.workdir / "cancel_requested.json"
                cancel_data = _json.loads(sentinel_path.read_text())
                cancel_reason = cancel_data.get("reason", "")
            except Exception:
                pass
            history.cancel_run(
                context.run_id,
                reason=cancel_reason,
                cancelled_by="user",
            )
            try:
                history.record_finish(
                    context.run_id,
                    False,
                    total_duration,
                    steps_summary,
                    final_result,
                    cost_usd=total_cost_usd if total_cost_usd > 0.0 else None,
                )
            except Exception:
                pass
        else:
            history.record_finish(
                context.run_id,
                all_ok,
                total_duration,
                steps_summary,
                final_result,
                cost_usd=total_cost_usd if total_cost_usd > 0.0 else None,
            )
    except Exception:
        pass

    outcome = "cancelled" if was_cancelled else ("success" if all_ok else "failure")
    end_msg = (
        f"Run finished: pipeline={pipeline.name} run_id={context.run_id} "
        f"outcome={outcome} duration={total_duration:.2f}s"
    )
    end_level = "INFO" if all_ok or was_cancelled else "ERROR"
    if end_level == "INFO":
        logger.info(end_msg)
    else:
        logger.error(end_msg)
    _db_log(end_level, "engine", end_msg)

    try:
        from brix.triggers.state import TriggerState

        trigger_state = TriggerState()
        trigger_state.record_pipeline_completion(
            pipeline.name,
            context.run_id,
            "success" if all_ok else "failure",
            final_result,
            input=context.input,
        )
    except Exception:
        pass

    try:
        from brix.alerting import AlertManager

        run_result = RunResult(
            success=all_ok,
            run_id=context.run_id,
            steps=step_statuses,
            result=final_result,
            duration=total_duration,
            cost_usd=total_cost_usd if total_cost_usd > 0.0 else None,
            deprecation_warnings=list(deprecation_warnings),
        )
        run_result_dict = run_result.model_dump()
        run_result_dict["pipeline"] = pipeline.name
        AlertManager().check_alerts(run_result_dict)
    except Exception:
        pass

    return RunResult(
        success=all_ok,
        run_id=context.run_id,
        steps=step_statuses,
        result=final_result,
        duration=total_duration,
        cost_usd=total_cost_usd if total_cost_usd > 0.0 else None,
        deprecation_warnings=list(deprecation_warnings),
    )


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
