"""Step execution helpers for the pipeline engine."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from brix.context import PipelineContext
from brix.engine_types import (
    _RenderedStep,
    _VALIDATE_CONFIG_TOP_LEVEL_FIELDS,
    _build_logger,
    _extract_step_cost,
    _measure_rss_mb,
    _redact_secret_values,
    _step_config_dict,
    StepResult,
)
from brix.models import Pipeline, Step, StepStatus
from brix.runners.base import BaseRunner

if TYPE_CHECKING:
    from brix.engine import PipelineEngine

logger = _build_logger("brix.engine")


@dataclass
class PreExecuteStepResult:
    """Result of pre-execution step handling."""

    step: Step
    runner: BaseRunner | None = None
    action: Literal["run", "continue", "break"] = "run"
    pipeline_aborted: bool = False
    stop_step_success: bool | None = None


class StepExecutor:
    """Runs step execution logic for sequential step execution."""

    def __init__(self, engine: PipelineEngine):
        self.engine = engine

    def pre_execute_step(
        self,
        pipeline: Pipeline,
        context: PipelineContext,
        step: Step,
        step_statuses: dict[str, StepStatus],
        dry_run_steps: list[str] | None = None,
    ) -> PreExecuteStepResult:
        # Disabled steps are unconditionally skipped (T-BRIX-V4-02)
        if not step.enabled:
            step_statuses[step.id] = StepStatus(
                status="skipped", duration=0.0, reason="disabled"
            )
            self.engine.progress.step_skipped(step.id)
            return PreExecuteStepResult(step=step, action="continue")

        # Selective dry-run: skip named steps without executing (T-BRIX-V4-BUG-09)
        if dry_run_steps and step.id in dry_run_steps:
            step_statuses[step.id] = StepStatus(
                status="dry_run", duration=0.0, reason="dry_run_steps"
            )
            # Do not set context output — downstream steps see null for this step
            self.engine.progress.step_skipped(step.id)
            return PreExecuteStepResult(step=step, action="continue")

        # Evaluate when condition
        jinja_ctx = context.to_jinja_context()
        if step.when:
            should_run = self.engine.loader.evaluate_condition(step.when, jinja_ctx)
            if not should_run:
                step_statuses[step.id] = StepStatus(
                    status="skipped", duration=0.0, reason="condition not met"
                )
                self.engine.progress.step_skipped(step.id)
                return PreExecuteStepResult(step=step, action="continue")

        # Evaluate else_of: only run this step when the referenced step was skipped
        if step.else_of:
            ref_status = step_statuses.get(step.else_of)
            if ref_status is None or ref_status.status != "skipped":
                step_statuses[step.id] = StepStatus(
                    status="skipped",
                    duration=0.0,
                    reason=f"else_of '{step.else_of}' was not skipped",
                )
                self.engine.progress.step_skipped(step.id)
                return PreExecuteStepResult(step=step, action="continue")

        # stop step: end the pipeline immediately (T-BRIX-V4-04)
        # Evaluate the when condition directly so that a bool False (from YAML
        # `when: false` or Pydantic coercion) is handled correctly.  When the
        # when-block above is entered, `step.when` is a non-empty truthy string
        # and the condition is evaluated there.  But when `step.when` is Python
        # bool False, `if step.when:` is skipped entirely — so we must re-check
        # here to avoid firing the stop unconditionally.
        if step.type == "stop":
            _should_stop = True
            if step.when is not None:
                if isinstance(step.when, bool):
                    _should_stop = step.when
                elif isinstance(step.when, str) and step.when.strip():
                    _should_stop = self.engine.loader.evaluate_condition(
                        step.when,
                        jinja_ctx if "jinja_ctx" in dir() else context.to_jinja_context(),
                    )
                else:
                    _should_stop = False  # empty string → don't stop
            if not _should_stop:
                step_statuses[step.id] = StepStatus(
                    status="skipped", duration=0.0, reason="condition not met"
                )
                self.engine.progress.step_skipped(step.id)
                return PreExecuteStepResult(step=step, action="continue")
            jinja_ctx = context.to_jinja_context()
            msg = step.message or "Pipeline stopped"
            rendered_msg = self.engine.loader.render_template(msg, jinja_ctx) if "{{" in msg else msg
            step_statuses[step.id] = StepStatus(
                status="ok", duration=0.0, reason=rendered_msg
            )
            return PreExecuteStepResult(
                step=step,
                action="break",
                pipeline_aborted=True,
                stop_step_success=getattr(step, "success_on_stop", True),
            )

        # --- Compositor-Mode guard (T-BRIX-V8-07) ---
        if pipeline.compositor_mode and not pipeline.allow_code:
            if step.type in ("python", "cli"):
                _cm_msg = (
                    f"Compositor-Mode: {step.type} steps not allowed. "
                    "Use built-in bricks or set allow_code: true"
                )
                step_statuses[step.id] = StepStatus(
                    status="error", duration=0.0, errors=1,
                    error_message=_cm_msg,
                )
                self.engine.progress.step_start(step.id, step.type)
                self.engine.progress.step_error(step.id, _cm_msg)
                effective_on_error = step.on_error or pipeline.error_handling.on_error
                if effective_on_error == "stop":
                    return PreExecuteStepResult(
                        step=step,
                        action="break",
                        pipeline_aborted=True,
                    )
                return PreExecuteStepResult(step=step, action="continue")

        # --- Profile / Mixin (T-BRIX-DB-23) ---
        step = self.engine._apply_profile(step)

        # --- Brick config_defaults merge (T-BRIX-IMP-02) ---
        step = self.engine._apply_brick_defaults(step)

        # --- Test-Mode: intercept db.upsert and action.notify before validation ---
        from brix.engine import LEGACY_ALIASES

        effective_step_type = LEGACY_ALIASES.get(step.type, step.type)
        if pipeline.test_mode and effective_step_type in ("db.upsert", "db_upsert"):
            logger.info(
                "Test-mode: dry-running db.upsert step '%s' (pipeline=%s)",
                step.id,
                pipeline.name,
            )
            output = {"test_mode": True, "dry": True, "step_id": step.id}
            context.set_output(step.id, output)
            step_statuses[step.id] = StepStatus(
                status="ok",
                duration=0.0,
                reason="test_mode_dry",
            )
            self.engine.progress.step_ok(step.id, 0.0, None)
            return PreExecuteStepResult(step=step, action="continue")
        if pipeline.test_mode and effective_step_type in ("action.notify", "notify"):
            logger.info(
                "Test-mode: log-only action.notify step '%s' (pipeline=%s)",
                step.id,
                pipeline.name,
            )
            output = {"test_mode": True, "log_only": True, "step_id": step.id}
            context.set_output(step.id, output)
            step_statuses[step.id] = StepStatus(
                status="ok",
                duration=0.0,
                reason="test_mode_log_only",
            )
            self.engine.progress.step_ok(step.id, 0.0, None)
            return PreExecuteStepResult(step=step, action="continue")

        # Build an early jinja context for dynamic dispatch type rendering
        _early_jinja_ctx = context.to_jinja_context() if "{{" in step.type else None

        # Get runner
        runner = self.engine._resolve_runner(step.type, jinja_ctx=_early_jinja_ctx)
        if not runner:
            _no_runner_msg = f"no runner registered for type '{step.type}'"
            step_statuses[step.id] = StepStatus(
                status="error", duration=0.0, errors=1,
                error_message=_no_runner_msg,
            )
            self.engine.progress.step_start(step.id, step.type)
            self.engine.progress.step_error(step.id, _no_runner_msg)
            effective_on_error = step.on_error or pipeline.error_handling.on_error
            if effective_on_error == "stop":
                return PreExecuteStepResult(
                    step=step,
                    action="break",
                    pipeline_aborted=True,
                )
            return PreExecuteStepResult(step=step, action="continue")

        # --- validate_config (T-BRIX-STD-03) ---
        _vc_config = _step_config_dict(step)
        # Merge top-level step attributes that runners may read
        for _vc_attr in _VALIDATE_CONFIG_TOP_LEVEL_FIELDS:
            _vc_val = getattr(step, _vc_attr, None)
            if _vc_val is not None:
                _vc_config[_vc_attr] = _vc_val
        logger.error(
            "validate_config input for step '%s' (%s): %r",
            step.id,
            step.type,
            _vc_config,
        )
        _vc_errors = runner.validate_config(_vc_config)
        if _vc_errors:
            _vc_msg = f"Config validation failed for step '{step.id}': {'; '.join(_vc_errors)}"
            logger.warning(_vc_msg)
            step_statuses[step.id] = StepStatus(
                status="error", duration=0.0, errors=1,
                error_message=_vc_msg,
            )
            self.engine.progress.step_start(step.id, step.type)
            self.engine.progress.step_error(step.id, _vc_msg)
            effective_on_error = step.on_error or pipeline.error_handling.on_error
            if effective_on_error == "stop":
                return PreExecuteStepResult(
                    step=step,
                    action="break",
                    pipeline_aborted=True,
                )
            return PreExecuteStepResult(step=step, action="continue")

        # --- per-step dependency check (T-BRIX-V6-03) ---
        if step.requirements:
            dep_err = self.engine._ensure_step_requirements(step)
            if dep_err:
                step_statuses[step.id] = StepStatus(
                    status="error", duration=0.0, errors=1,
                    error_message=dep_err,
                )
                self.engine.progress.step_start(step.id, step.type)
                self.engine.progress.step_error(step.id, dep_err)
                effective_on_error = step.on_error or pipeline.error_handling.on_error
                if effective_on_error == "stop":
                    return PreExecuteStepResult(
                        step=step,
                        action="break",
                        pipeline_aborted=True,
                    )
                return PreExecuteStepResult(step=step, action="continue")

        return PreExecuteStepResult(step=step, runner=runner)

    async def execute_foreach(
        self,
        step: Step,
        context: PipelineContext,
        pipeline: Pipeline,
        step_statuses: dict[str, StepStatus],
        jinja_ctx: dict | None,
    ) -> StepResult:
        """Execute a foreach step and aggregate the per-item results."""
        if jinja_ctx is None:
            jinja_ctx = context.to_jinja_context()

        with self.engine._step_credentials_context(context, step):
            try:
                items = self.engine.loader.resolve_foreach(step.foreach, jinja_ctx)
            except (ValueError, TypeError) as foreach_resolve_err:
                foreach_err_msg = (
                    f"foreach expression failed to resolve for step '{step.id}': "
                    f"{foreach_resolve_err}"
                )
                logger.warning(foreach_err_msg)
                step_statuses[step.id] = StepStatus(
                    status="error",
                    duration=0.0,
                    errors=1,
                    error_message=foreach_err_msg,
                )
                self.engine.progress.step_start(step.id, step.type)
                self.engine.progress.step_error(step.id, foreach_err_msg, 0.0)
                effective_on_error = step.on_error or pipeline.error_handling.on_error
                return StepResult(
                    status="error",
                    output=None,
                    should_abort=effective_on_error == "stop",
                    should_continue=effective_on_error != "stop",
                )

            step_start = time.monotonic()
            if step.batch_size > 0:
                chunks = self.engine._chunk_items(items, step.batch_size)
                batch_results: list[tuple[Any, dict]] = []
                batch_aborted = False
                effective_on_error = step.on_error or pipeline.error_handling.on_error
                chunk_step = step.model_copy(update={"flat_output": False})
                for chunk_idx, chunk in enumerate(chunks):
                    self.engine.progress.step_start(
                        f"{step.id}[batch {chunk_idx + 1}/{len(chunks)}]",
                        step.type,
                    )
                    if step.parallel:
                        chunk_result = await self.engine._run_foreach_parallel(
                            chunk_step, chunk, context, pipeline
                        )
                    else:
                        chunk_result = await self.engine._run_foreach_sequential(
                            chunk_step, chunk, context, pipeline
                        )

                    for item_result in chunk_result.get("items", []):
                        if item_result.get("success"):
                            batch_results.append(
                                (None, {"success": True, "data": item_result.get("data")})
                            )
                        else:
                            batch_results.append(
                                (
                                    item_result.get("input"),
                                    {
                                        "success": False,
                                        "error": item_result.get("error", "unknown"),
                                        "duration": 0.0,
                                    },
                                )
                            )

                    if not chunk_result.get("success") and effective_on_error == "stop":
                        batch_aborted = True
                        break

                foreach_result = self.engine._build_foreach_result(
                    batch_results,
                    step,
                    pipeline,
                )
                if batch_aborted:
                    foreach_result["success"] = False
            elif step.parallel:
                foreach_result = await self.engine._run_foreach_parallel(
                    step, items, context, pipeline
                )
            else:
                foreach_result = await self.engine._run_foreach_sequential(
                    step, items, context, pipeline
                )

        step_duration = time.monotonic() - step_start

        perf_hints: list[str] = []
        num_items = len(items)
        if not step.parallel and not step.batch_size and num_items > 100:
            perf_hints.append(
                "Sequential foreach over 100+ items. Add parallel: true with concurrency: N."
            )
        if step.batch_size > 0 and not step.parallel:
            perf_hints.append(
                "batch_size set but parallel: false — batches run sequentially."
            )
        if step.parallel and num_items > 50 and step.concurrency == 10:
            perf_hints.append(
                "Large parallel foreach with default concurrency=10. For API steps consider concurrency: 3-5."
            )
        if perf_hints:
            foreach_result["hints"] = perf_hints

        summary = foreach_result.get("summary", {})
        if foreach_result.get("success"):
            context.set_output(step.id, foreach_result)
            step_statuses[step.id] = StepStatus(
                status="ok",
                duration=step_duration,
                items=summary.get("total"),
                errors=summary.get("failed") or None,
            )
            self.engine.progress.foreach_done(
                step.id,
                summary.get("total", 0),
                summary.get("succeeded", 0),
                summary.get("failed", 0),
                step_duration,
            )
            return StepResult(status="ok", output=foreach_result)

        foreach_err_msg = (
            f"foreach failed ({summary.get('failed', '?')} of {summary.get('total', '?')} items failed)"
        )
        step_statuses[step.id] = StepStatus(
            status="error",
            duration=step_duration,
            errors=summary.get("failed", 1),
            error_message=foreach_err_msg,
        )
        self.engine.progress.step_start(step.id, step.type)
        self.engine.progress.step_error(step.id, foreach_err_msg, step_duration)
        effective_on_error = step.on_error or pipeline.error_handling.on_error
        return StepResult(
            status="error",
            output=None,
            should_abort=effective_on_error == "stop",
            should_continue=effective_on_error != "stop",
        )

    async def execute_step(
        self,
        step: Step,
        context: PipelineContext,
        pipeline: Pipeline,
        step_statuses: dict[str, StepStatus],
        jinja_ctx: dict | None,
        dry_run_steps: list[str] | None = None,
    ) -> StepResult:
        """Run the full single-step execution lifecycle for sequential execution."""
        pre_execute_result = self.pre_execute_step(
            pipeline=pipeline,
            context=context,
            step=step,
            step_statuses=step_statuses,
            dry_run_steps=dry_run_steps,
        )
        step = pre_execute_result.step
        runner = pre_execute_result.runner

        if pre_execute_result.action != "run":
            return StepResult(
                status=(
                    step_statuses.get(step.id).status
                    if step_statuses.get(step.id) is not None
                    else ("ok" if pre_execute_result.action == "break" else "skipped")
                ),
                output=context.get_output(step.id),
                should_abort=pre_execute_result.pipeline_aborted,
                should_continue=pre_execute_result.action != "break",
            )

        if jinja_ctx is None:
            jinja_ctx = context.to_jinja_context()

        if step.foreach:
            return await self.execute_foreach(
                step=step,
                context=context,
                pipeline=pipeline,
                step_statuses=step_statuses,
                jinja_ctx=jinja_ctx,
            )

        rendered_params = self.engine.loader.render_step_params(step, jinja_ctx)
        rendered_step = _RenderedStep(step, rendered_params, self.engine.loader, jinja_ctx)

        pin_hit = None
        try:
            from brix.db import BrixDB as _PinDB

            pin_db = _PinDB()
            pin_record = pin_db.get_pin(pipeline.name, step.id)
            if pin_record is not None:
                pin_hit = pin_record["pinned_data"]
        except Exception as pin_err:
            logger.warning("Step pin check failed for '%s': %s", step.id, pin_err)
        if pin_hit is not None:
            logger.info("Step '%s' using pinned mock data (pipeline=%s)", step.id, pipeline.name)
            context.set_output(step.id, pin_hit)
            step_statuses[step.id] = StepStatus(status="ok", duration=0.0, reason="pin_mock")
            self.engine.progress.step_ok(step.id, 0.0, None)
            return StepResult(status="ok", output=pin_hit)

        from brix.engine import _warn_if_high_memory

        if step.cache is True:
            from brix.context import CacheManager

            cache_mgr = CacheManager()
            cached_output = cache_mgr.get(step.id, rendered_params)
            if cached_output is not None:
                context.set_output(step.id, cached_output)
                step_statuses[step.id] = StepStatus(
                    status="ok",
                    duration=0.0,
                    reason="cache_hit",
                )
                self.engine.progress.step_ok(step.id, 0.0, None)
                return StepResult(
                    status="ok",
                    output=cached_output,
                    cost=_extract_step_cost(cached_output),
                )

        brick_cache_instance = None
        brick_cache_rendered_key = None
        if isinstance(step.cache, dict):
            try:
                from brix.resilience import BrickCache as _BrickCache, BrixDB as _ResBrixDB

                brick_cache_instance = _BrickCache(step.cache, _ResBrixDB())
                brick_cache_rendered_key = self.engine.loader.render_template(
                    step.cache.get("key", step.id),
                    jinja_ctx,
                )
                bc_hit = brick_cache_instance.get(brick_cache_rendered_key)
                if bc_hit is not None:
                    context.set_output(step.id, bc_hit)
                    step_statuses[step.id] = StepStatus(
                        status="ok",
                        duration=0.0,
                        reason="cache_hit",
                    )
                    self.engine.progress.step_ok(step.id, 0.0, None)
                    return StepResult(
                        status="ok",
                        output=bc_hit,
                        cost=_extract_step_cost(bc_hit),
                    )
            except Exception as bc_err:
                logger.warning("Brick cache check failed for '%s': %s", step.id, bc_err)

        cb_instance = None
        if step.circuit_breaker:
            try:
                from brix.resilience import CircuitBreaker as _CircuitBreaker, BrixDB as _ResBrixDB

                cb_instance = _CircuitBreaker(step.id, step.circuit_breaker, _ResBrixDB())
                cb_pre = cb_instance.pre_check(context)
                if cb_pre is not None:
                    if cb_pre.get("success"):
                        output = cb_pre.get("data")
                        context.set_output(step.id, output)
                        step_statuses[step.id] = StepStatus(
                            status="ok",
                            duration=0.0,
                            reason="circuit_breaker_fallback",
                        )
                        self.engine.progress.step_ok(step.id, 0.0, None)
                        return StepResult(status="ok", output=output)
                    cb_err_msg = cb_pre.get("error", "Circuit breaker OPEN")
                    step_statuses[step.id] = StepStatus(
                        status="skipped",
                        duration=0.0,
                        reason=cb_err_msg,
                    )
                    self.engine.progress.step_skipped(step.id)
                    return StepResult(status="skipped", should_continue=True)
            except Exception as cb_err:
                logger.warning("Circuit breaker check failed for '%s': %s", step.id, cb_err)

        rl_instance = None
        if step.rate_limit:
            try:
                from brix.resilience import RateLimiter as _RateLimiter, BrixDB as _ResBrixDB

                rl_instance = _RateLimiter(step.id, step.rate_limit, _ResBrixDB())
                rl_wait = rl_instance.wait_seconds()
                if rl_wait > 0:
                    await asyncio.sleep(rl_wait)
            except Exception as rl_err:
                logger.warning("Rate limiter check failed for '%s': %s", step.id, rl_err)

        if step.pause_before:
            await self.engine._wait_for_breakpoint_resume(context, step.id)

        self.engine._write_context_snapshot(context)

        self.engine.progress.step_start(step.id, step.type)
        with self.engine._step_credentials_context(context, step):
            step_start = time.monotonic()
            step_started_at = datetime.now(timezone.utc).isoformat()
            result = await self.engine._execute_with_retry(
                runner,
                rendered_step,
                context,
                step,
                pipeline,
            )
            step_duration = time.monotonic() - step_start
            step_ended_at = datetime.now(timezone.utc).isoformat()

        if getattr(runner, "_progress", None) is None:
            logger.warning(
                "Runner '%s' (step '%s') did not call report_progress() — "
                "consider adding self.report_progress(100.0) at the end of execute()",
                step.type,
                step.id,
            )

        runner_progress = getattr(runner, "_progress", None)
        if runner_progress is not None and self.engine._run_db is not None:
            try:
                self.engine._run_db.update_step_progress(
                    run_id=context.run_id,
                    step_id=step.id,
                    pct=runner_progress.get("pct", 100.0),
                    msg=runner_progress.get("msg", ""),
                    done=runner_progress.get("done", 0),
                    total=runner_progress.get("total", 0),
                )
            except Exception:
                pass

        rss_mb = _measure_rss_mb()
        resource_usage = {"rss_mb": rss_mb, "duration": step_duration}
        result["resource_usage"] = resource_usage
        _warn_if_high_memory(rss_mb, step.id)

        persist_data_flag = getattr(step, "persist_data", True)
        secret_vals = getattr(context, "_secret_values", set())

        if result.get("success"):
            output = result.get("data")
            context.set_output(step.id, output)
            if step.cache is True:
                from brix.context import CacheManager

                CacheManager().set(step.id, rendered_params, output)
            if brick_cache_instance is not None and brick_cache_rendered_key is not None:
                try:
                    brick_cache_instance.set(brick_cache_rendered_key, output)
                except Exception as bc_set_err:
                    logger.warning("Brick cache set failed for '%s': %s", step.id, bc_set_err)
            if cb_instance is not None:
                try:
                    cb_instance.on_success()
                except Exception:
                    pass
            if rl_instance is not None:
                try:
                    rl_instance.record_call()
                except Exception:
                    pass
            if step.compensate and getattr(self.engine, "_saga_tracker", None) is not None:
                self.engine._saga_tracker.record(step.id, step.compensate)
            cost = _extract_step_cost(output)
            step_statuses[step.id] = StepStatus(
                status="ok",
                duration=step_duration,
                items=result.get("items_count"),
                resource_usage=resource_usage,
            )
            self.engine.progress.step_ok(step.id, step_duration, result.get("items_count"))
            if self.engine._should_persist(step):
                self.engine._persist_step_output(
                    context.run_id,
                    step,
                    result,
                    rendered_params,
                    context,
                    db=self.engine._run_db,
                )
            try:
                self.engine._run_db.record_step_execution(
                    run_id=context.run_id,
                    step_id=step.id,
                    step_type=step.type,
                    status="success",
                    input_data=_redact_secret_values(rendered_params, secret_vals) if persist_data_flag else None,
                    output_data=_redact_secret_values(output, secret_vals) if persist_data_flag else None,
                    data_source="",
                    started_at=step_started_at,
                    ended_at=step_ended_at,
                    duration_ms=int(step_duration * 1000),
                    persist_data=persist_data_flag,
                )
            except Exception:
                pass
            return StepResult(status="ok", output=output, cost=cost)

        error_msg = result.get("error", "unknown error")
        if cb_instance is not None:
            try:
                cb_instance.on_failure()
            except Exception:
                pass
        step_statuses[step.id] = StepStatus(
            status="error",
            duration=step_duration,
            errors=1,
            error_message=str(error_msg) if error_msg else None,
            resource_usage=resource_usage,
        )
        self.engine.progress.step_error(step.id, error_msg, step_duration)
        if self.engine._should_persist(step):
            self.engine._persist_step_output(
                context.run_id,
                step,
                result,
                rendered_params,
                context,
                db=self.engine._run_db,
            )
        try:
            self.engine._run_db.record_step_execution(
                run_id=context.run_id,
                step_id=step.id,
                step_type=step.type,
                status="error",
                input_data=_redact_secret_values(rendered_params, secret_vals) if persist_data_flag else None,
                output_data=None,
                error_detail={"error": str(error_msg)} if error_msg else None,
                data_source="",
                started_at=step_started_at,
                ended_at=step_ended_at,
                duration_ms=int(step_duration * 1000),
                persist_data=persist_data_flag,
            )
        except Exception:
            pass

        effective_on_error = step.on_error or pipeline.error_handling.on_error
        should_abort = effective_on_error == "stop"
        if should_abort and getattr(self.engine, "_saga_tracker", None) is not None:
            try:
                await self.engine._saga_tracker.run_compensations(context, self.engine, pipeline)
            except Exception:
                pass
        return StepResult(
            status="error",
            output=None,
            should_abort=should_abort,
            should_continue=not should_abort,
        )
