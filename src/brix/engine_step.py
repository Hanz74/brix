"""Step pre-execution helpers for the pipeline engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from brix.context import PipelineContext
from brix.engine_types import _VALIDATE_CONFIG_TOP_LEVEL_FIELDS, _build_logger, _step_config_dict
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
    """Runs pre-execution logic for sequential step execution."""

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
