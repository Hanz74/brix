"""Step execution helpers for the pipeline engine."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from brix.context import PipelineContext
from brix.credential_store import CredentialStore, is_credential_uuid, CredentialNotFoundError
from brix.engine_types import (
    _RenderedStep,
    _VALIDATE_CONFIG_TOP_LEVEL_FIELDS,
    _build_logger,
    _extract_brick_default_values,
    _extract_step_cost,
    _measure_rss_mb,
    _redact_secret_values,
    _step_config_dict,
    StepResult,
)
from brix.models import Pipeline, RetryConfig, RetryProfile, Step, StepStatus
from brix.runners.base import BaseRunner
from brix.serialization import json_dumps

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

    @staticmethod
    def _should_persist(step: Step) -> bool:
        """Return True when step output should be persisted to step_outputs table."""
        return step.persist_output or bool(os.environ.get("BRIX_DEBUG"))

    def _effective_policy_level(self) -> Literal["permissive", "strict", "locked"]:
        """Return the active step-type policy with strict_bricks back-compat."""
        raw_level = getattr(self.engine, "_policy_level", "permissive")
        if raw_level == "locked":
            return "locked"
        if raw_level == "strict" or getattr(self.engine, "_strict_bricks", False):
            return "strict"
        return "permissive"

    # ------------------------------------------------------------------
    # Methods moved from PipelineEngine (profile / brick / runner / creds)
    # ------------------------------------------------------------------

    def _apply_profile(self, step: Step) -> Step:
        """Apply a named profile's config to a step (T-BRIX-DB-23).

        If ``step.profile`` is set, load the profile from DB and merge its
        config fields into the step.  Step-level fields always take precedence
        over profile defaults (i.e. profile acts as fallback).

        Returns a new Step instance with merged fields, or the original step
        if no profile is set or the profile cannot be loaded.
        """
        if not step.profile:
            return step
        try:
            from brix.db import BrixDB as _BrixDB
            _db = _BrixDB()
            profile_data = _db.profile_get(step.profile)
            if not profile_data:
                logger.warning("Profile '%s' not found in DB — skipping merge", step.profile)
                return step
            profile_config: dict = profile_data.get("config", {})
            if not profile_config:
                return step
            # Build merged field dict: profile values as defaults, step values override
            step_dict = step.model_dump()
            merged = {}
            # Profile-applicable fields: resilience + runtime config
            _profile_fields = {
                "cache", "circuit_breaker", "rate_limit", "retry_profile",
                "timeout", "on_error",
            }
            for field_name in _profile_fields:
                if field_name in profile_config:
                    model_default = Step.model_fields[field_name].default if field_name in Step.model_fields else None
                    current_val = step_dict.get(field_name)
                    if current_val == model_default or current_val is None:
                        merged[field_name] = profile_config[field_name]
            if not merged:
                return step
            new_dict = {**step_dict, **merged}
            return Step.model_validate(new_dict)
        except Exception as _profile_err:
            logger.warning("Profile merge failed for step '%s': %s", step.id, _profile_err)
            return step

    def _apply_brick_defaults(self, step: Step) -> Step:
        """Merge config_defaults from a custom brick into step.params (T-BRIX-IMP-02).

        When a step type is a custom brick registered in the DB, the brick may
        declare ``config_defaults`` (stored as ``config_schema`` in the DB row as
        a flat key->value JSON object).  These defaults act as a baseline for
        ``step.params``: the step's own params always win, but any key present in
        the brick's defaults that is absent from step.params is filled in.

        Returns a new Step instance with the merged params, or the original step
        if the brick has no defaults or cannot be loaded.
        """
        # Only relevant for dot-notation custom brick types
        if "." not in step.type:
            return step
        try:
            from brix.db import BrixDB as _BrixDB
            _db = _BrixDB()
            row = _db.brick_definitions_get(step.type)
            if not row:
                return step
            raw_schema = row.get("config_schema", "{}")
            if isinstance(raw_schema, str):
                import json as _json
                try:
                    brick_defaults: dict = _json.loads(raw_schema)
                except Exception:
                    return step
            elif isinstance(raw_schema, dict):
                brick_defaults = raw_schema
            else:
                return step
            brick_defaults = _extract_brick_default_values(brick_defaults)
            if not brick_defaults:
                return step
            # Merge: brick defaults as base, step.params override
            merged_params = {**brick_defaults, **(step.params or {})}
            if merged_params == (step.params or {}):
                return step  # Nothing new to add
            return step.model_copy(update={"params": merged_params})
        except Exception as _brick_err:
            logger.warning("Brick defaults merge failed for step '%s': %s", step.id, _brick_err)
            return step

    def _resolve_runner(self, step_type: str, jinja_ctx: dict | None = None) -> BaseRunner | None:
        """Resolve a runner for a given step type using the Brick-First lookup chain.

        Resolution order (T-BRIX-DB-05c / T-BRIX-DB-23):
        0. Dynamic Dispatch: if step_type contains Jinja2 template syntax
           (``{{ ... }}``), render it using *jinja_ctx* first.
        1. Legacy-Alias lookup: if step_type is an old flat name mapped in
           LEGACY_ALIASES, emit a deprecation warning and use the new brick name.
        2. Brick-Registry lookup: dot-notation brick name -> runner.
        3. Direct lookup in engine._runners.

        Returns None if no runner can be resolved.
        """
        from brix.engine import LEGACY_ALIASES

        engine = self.engine
        policy_level = self._effective_policy_level()
        # 0. Dynamic Dispatch (T-BRIX-DB-23): render Jinja2 step type
        if "{{" in step_type and jinja_ctx is not None:
            try:
                rendered_type = engine.loader.render_template(step_type, jinja_ctx).strip()
            except Exception as _dyn_err:
                logger.warning("Dynamic dispatch: failed to render step type '%s': %s", step_type, _dyn_err)
                return None
            # Security: rendered type MUST exist in registry or direct runner map
            if rendered_type not in engine._runners and engine._brick_registry.get(rendered_type) is None:
                logger.warning(
                    "Dynamic dispatch: rendered type '%s' is not a registered brick or runner",
                    rendered_type,
                )
                return None
            step_type = rendered_type
        # 1. Legacy-Alias layer — old flat name -> new brick name -> runner (with warning)
        new_name = LEGACY_ALIASES.get(step_type)
        if new_name:
            if policy_level in {"strict", "locked"}:
                raise ValueError(
                    f"Step type '{step_type}' is a legacy alias (strict_bricks=True). "
                    f"Use '{new_name}' instead."
                )
            import warnings as _warnings
            _warnings.warn(
                f"Step type '{step_type}' is deprecated. Use '{new_name}' instead.",
                DeprecationWarning,
                stacklevel=4,
            )
            # Track deprecated usage in DB (T-BRIX-DB-05d)
            try:
                if engine._deprecation_db is None:
                    from brix.db import BrixDB as _BrixDB
                    engine._deprecation_db = _BrixDB()
                engine._deprecation_db.record_deprecated_usage(
                    pipeline_name=engine._current_pipeline_name or "unknown",
                    step_id=step_type,  # step_id not available here; use type as fallback
                    old_type=step_type,
                    new_type=new_name,
                )
            except Exception:
                pass  # Never crash the engine over tracking
            # Accumulate deprecation warning for run result
            warn_msg = f"Step type '{step_type}' is deprecated. Use '{new_name}' instead."
            if warn_msg not in engine._deprecation_warnings:
                engine._deprecation_warnings.append(warn_msg)
            brick = engine._brick_registry.get(new_name)
            if brick and brick.runner:
                runner = engine._runners.get(brick.runner)
                if runner is not None:
                    return runner

        # 2. Brick-Registry lookup (new dot-notation names like "db.query")
        brick = engine._brick_registry.get(step_type)
        if brick and brick.runner:
            runner = engine._runners.get(brick.runner)
            if runner is not None:
                return runner

        if policy_level == "locked":
            raise ValueError(
                f"Step type '{step_type}' is not a registered brick (policy_level=locked). "
                "Use a dot-notation brick type."
            )

        # 3. Direct runner lookup (fast path for flat names not in LEGACY_ALIASES)
        runner = engine._runners.get(step_type)
        if runner is not None:
            return runner

        return None

    def _resolve_step_credentials(self, step: Any) -> dict[str, Any]:
        """Resolve per-step credentials using the same rules as PipelineContext."""
        step_credentials = getattr(step, "credentials", None) or {}
        resolved: dict[str, Any] = {}
        for key, cred in step_credentials.items():
            if isinstance(cred, str):
                cred = {"env": cred}
            if not isinstance(cred, dict):
                continue
            env_ref = cred.get("env", "")
            if is_credential_uuid(env_ref):
                try:
                    value = CredentialStore().resolve(env_ref)
                except CredentialNotFoundError:
                    import warnings

                    warnings.warn(
                        f"Credential UUID '{env_ref}' not found in store for key '{key}'. "
                        "Using empty string.",
                        UserWarning,
                        stacklevel=2,
                    )
                    value = ""
            else:
                value = os.environ.get(env_ref, "")
            if cred.get("refresh") is not None:
                value = PipelineContext._refresh_credential(cred, value)
            resolved[key] = value
        return resolved

    @contextmanager
    def _step_credentials_context(self, context: PipelineContext, step: Any):
        """Overlay step credentials for a single step execution."""
        step_credentials = self._resolve_step_credentials(step)
        if not step_credentials:
            yield
            return

        original_credentials = dict(context.credentials)
        context.credentials = {**original_credentials, **step_credentials}
        context._jinja_cache = None
        try:
            yield
        finally:
            context.credentials = original_credentials
            context._jinja_cache = None

    @staticmethod
    def _context_snapshot(context: Any) -> dict:
        """Build a lightweight context snapshot: {key: type_name} for each key.

        Avoids serialising potentially large data values while still giving
        useful debugging information about what was available in the context.
        """
        try:
            jinja_ctx = context.to_jinja_context()
        except Exception:
            return {}

        def _type_name(v: Any) -> str:
            if isinstance(v, dict):
                return f"dict({len(v)} keys)"
            if isinstance(v, list):
                return f"list({len(v)} items)"
            return type(v).__name__

        return {k: _type_name(v) for k, v in jinja_ctx.items()}

    # ------------------------------------------------------------------
    # End of moved methods
    # ------------------------------------------------------------------

    def _persist_step_output(
        self,
        run_id: str,
        step: Step,
        result: dict,
        rendered_params: Any,
        context: Any,
        db: Any = None,
    ) -> None:
        """Write step execution data to the step_outputs table (best-effort)."""
        try:
            if db is None:
                from brix.db import BrixDB

                db = BrixDB()
            stored_params = rendered_params
            mcp_trace = result.get("mcp_trace")
            if mcp_trace is not None:
                if isinstance(rendered_params, dict):
                    stored_params = dict(rendered_params)
                else:
                    stored_params = {"_params": rendered_params}
                stored_params["_mcp_trace"] = mcp_trace
            db.save_step_output(
                run_id=run_id,
                step_id=step.id,
                output=result.get("data"),
                rendered_params=stored_params,
                stderr_text=result.get("stderr"),
                context_snapshot=self._context_snapshot(context),
            )
        except Exception:
            pass

    def _write_context_snapshot(self, context: Any) -> None:
        """Write the current Jinja2 context snapshot to workdir/context-snapshot.json."""
        try:
            snapshot = self._context_snapshot(context)
            snapshot_path = context.workdir / "context-snapshot.json"
            snapshot_path.write_text(json_dumps(snapshot))
        except Exception:
            pass

    async def _wait_for_breakpoint_resume(self, context: Any, step_id: str) -> None:
        """Write breakpoint.json and poll until it is deleted (resume signal)."""
        breakpoint_path = context.workdir / "breakpoint.json"
        try:
            breakpoint_path.write_text(
                json_dumps({"step_id": step_id, "paused_at": time.monotonic()})
            )
        except OSError:
            return

        try:
            context.save_run_metadata("(paused)", "paused")
        except Exception:
            pass

        while breakpoint_path.exists():
            if self.engine._is_run_cancelled(context):
                break
            await asyncio.sleep(2.0)

    def _ensure_step_requirements(self, step: Step) -> str | None:
        """Check and auto-install per-step requirements."""
        if not step.requirements:
            return None

        from brix.deps import check_requirements, install_requirements

        missing = check_requirements(step.requirements)
        if not missing:
            return None

        print(
            f"Step '{step.id}': installing {len(missing)} package(s): {', '.join(missing)}",
            file=sys.stderr,
        )
        ok = install_requirements(missing)
        if not ok:
            return f"Failed to install step packages for '{step.id}': {', '.join(missing)}"
        return None

    async def _execute_with_retry(
        self,
        runner: BaseRunner,
        rendered_step: Any,
        context: Any,
        step: Step,
        pipeline: Pipeline,
    ) -> dict:
        """Execute a step with retry logic if on_error=retry, otherwise single execution."""
        effective_on_error = step.on_error or pipeline.error_handling.on_error

        if effective_on_error != "retry":
            try:
                return await runner.execute(rendered_step, context)
            except Exception as exc:
                return {"success": False, "error": str(exc), "duration": 0.0}

        profile: RetryProfile | None = None
        profile_name = getattr(step, "retry_profile", None)
        if profile_name:
            profile = pipeline.retry_profiles.get(profile_name)
            if profile is None:
                return {
                    "success": False,
                    "error": f"retry_profile '{profile_name}' not found in pipeline.retry_profiles",
                    "duration": 0.0,
                }

        if profile is not None:
            max_attempts = profile.max
            backoff = profile.backoff
            retriable_codes: list[int] = profile.retriable_status_codes
        else:
            retry_config = pipeline.error_handling.retry or RetryConfig()
            max_attempts = retry_config.max
            backoff = retry_config.backoff
            retriable_codes = []

        last_result: dict = {"success": False, "error": "no attempts made", "duration": 0.0}
        for attempt in range(1, max_attempts + 1):
            try:
                result = await runner.execute(rendered_step, context)
                if result.get("success"):
                    return result
                last_result = result

                if retriable_codes:
                    status_code = result.get("status_code")
                    if status_code is not None and status_code not in retriable_codes:
                        last_result["retry_count"] = attempt
                        return last_result

                if result.get("rate_limited") and result.get("retry_after"):
                    await asyncio.sleep(result["retry_after"])
                    continue
            except Exception as exc:
                last_result = {"success": False, "error": str(exc), "duration": 0.0}

            if attempt < max_attempts:
                delay = float(2 ** (attempt - 1)) if backoff == "exponential" else float(attempt)
                await asyncio.sleep(delay)

        last_result["retry_count"] = max_attempts
        return last_result

    def _chunk_items(self, items: list, batch_size: int) -> list[list]:
        """Split items into chunks of batch_size. Returns [items] if batch_size <= 0."""
        if batch_size <= 0:
            return [items]
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    async def _run_foreach_sequential(
        self,
        step: Step,
        items: list,
        context: PipelineContext,
        pipeline: Pipeline,
    ) -> dict:
        """Run foreach items one by one in order."""
        foreach_jinja = context.to_jinja_context() if "{{" in step.type else None
        runner = self._resolve_runner(step.type, jinja_ctx=foreach_jinja)
        results: list[tuple[Any, dict]] = []
        foreach_start = time.monotonic()
        completed = context.load_foreach_checkpoint(step.id) if context._resume_from else {}

        for i, item in enumerate(items):
            if self.engine._is_run_cancelled(context):
                break

            if i in completed:
                results.append((item, completed[i]))
                self.engine.progress.step_resumed(f"{step.id}[{i}]")
                continue

            jinja_ctx = context.to_jinja_context(item=item)
            rendered_params = self.engine.loader.render_step_params(step, jinja_ctx)
            rendered_step = _RenderedStep(step, rendered_params, self.engine.loader, jinja_ctx)
            item_start = time.monotonic()
            result = await self._execute_with_retry(runner, rendered_step, context, step, pipeline)
            item_duration_ms = int((time.monotonic() - item_start) * 1000)
            results.append((item, result))

            context.write_foreach_checkpoint(step.id, i, item, result)

            if self.engine._run_db is not None:
                try:
                        self.engine._run_db.record_foreach_item(
                            run_id=context.run_id,
                            step_id=step.id,
                            item_index=i,
                            item_input=item,
                            item_output=result.get("data"),
                            status="success" if result.get("success") else "error",
                            error_detail=(
                                result.get("error")
                                if isinstance(result.get("error"), dict)
                                else {"error": result.get("error")}
                                if result.get("error")
                                else None
                            ),
                            duration_ms=item_duration_ms,
                        )
                except Exception:
                    pass

            failed_count = sum(1 for _, item_result in results if not item_result.get("success"))
            current_count = len(results)
            total_items = len(items)
            self.engine.progress.foreach_progress(step.id, current_count, total_items, failed_count)
            pct = round(current_count / total_items * 100, 1) if total_items > 0 else 0.0
            eta: float | None = None
            if current_count > 0 and total_items > current_count:
                elapsed = time.monotonic() - foreach_start
                avg_per_item = elapsed / current_count
                eta = round(avg_per_item * (total_items - current_count), 1)
            context.update_step_progress(
                step.id,
                {
                    "processed": current_count,
                    "total": total_items,
                    "percent": pct,
                    "eta_seconds": eta,
                    "message": f"foreach {current_count}/{total_items} ({failed_count} failed)",
                },
            )
            context.save_run_metadata(
                pipeline.name,
                "running",
                progress={
                    "step": step.id,
                    "current": current_count,
                    "total": total_items,
                    "failed": failed_count,
                },
            )

        return self._build_foreach_result(results, step, pipeline)

    async def _run_foreach_parallel(
        self,
        step: Step,
        items: list,
        context: PipelineContext,
        pipeline: Pipeline,
    ) -> dict:
        """Run foreach items concurrently, respecting the concurrency limit."""
        foreach_jinja = context.to_jinja_context() if "{{" in step.type else None
        runner = self._resolve_runner(step.type, jinja_ctx=foreach_jinja)
        semaphore = asyncio.Semaphore(step.concurrency)
        foreach_start = time.monotonic()
        completed = context.load_foreach_checkpoint(step.id) if context._resume_from else {}
        checkpoint_lock = asyncio.Lock()
        completed_count = 0
        failed_count = 0
        total_items = len(items)

        async def run_item(idx: int, item: Any) -> tuple[Any, dict]:
            nonlocal completed_count, failed_count
            if idx in completed:
                self.engine.progress.step_resumed(f"{step.id}[{idx}]")
                return item, completed[idx]

            async with semaphore:
                jinja_ctx = context.to_jinja_context(item=item)
                rendered_params = self.engine.loader.render_step_params(step, jinja_ctx)
                rendered_step = _RenderedStep(step, rendered_params, self.engine.loader, jinja_ctx)
                item_start = time.monotonic()
                result = await self._execute_with_retry(runner, rendered_step, context, step, pipeline)
                item_duration_ms = int((time.monotonic() - item_start) * 1000)

                if self.engine._run_db is not None:
                    try:
                        self.engine._run_db.record_foreach_item(
                            run_id=context.run_id,
                            step_id=step.id,
                            item_index=idx,
                            item_input=item,
                            item_output=result.get("data"),
                            status="success" if result.get("success") else "error",
                            error_detail=(
                                result.get("error")
                                if isinstance(result.get("error"), dict)
                                else {"error": result.get("error")}
                                if result.get("error")
                                else None
                            ),
                            duration_ms=item_duration_ms,
                        )
                    except Exception:
                        pass

                async with checkpoint_lock:
                    context.write_foreach_checkpoint(step.id, idx, item, result)
                    completed_count += 1
                    if not result.get("success"):
                        failed_count += 1
                    self.engine.progress.foreach_progress(step.id, completed_count, total_items, failed_count)
                    pct = round(completed_count / total_items * 100, 1) if total_items > 0 else 0.0
                    eta: float | None = None
                    if completed_count > 0 and total_items > completed_count:
                        elapsed = time.monotonic() - foreach_start
                        avg_per_item = elapsed / completed_count
                        eta = round(avg_per_item * (total_items - completed_count), 1)
                    context.update_step_progress(
                        step.id,
                        {
                            "processed": completed_count,
                            "total": total_items,
                            "percent": pct,
                            "eta_seconds": eta,
                            "message": f"foreach {completed_count}/{total_items} ({failed_count} failed)",
                        },
                    )
                    context.save_run_metadata(
                        pipeline.name,
                        "running",
                        progress={
                            "step": step.id,
                            "current": completed_count,
                            "total": total_items,
                            "failed": failed_count,
                        },
                    )

                return item, result

        tasks = [run_item(i, item) for i, item in enumerate(items)]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        processed: list[tuple[Any, dict]] = []
        for idx, result in enumerate(raw_results):
            if isinstance(result, Exception):
                processed.append((items[idx], {"success": False, "error": str(result), "duration": 0.0}))
            else:
                processed.append(result)

        return self._build_foreach_result(processed, step, pipeline)

    def _build_foreach_result(
        self,
        results: list[tuple[Any, dict]],
        step: Step,
        pipeline: Pipeline,
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
                items.append(
                    {
                        "success": False,
                        "error": result.get("error", "unknown"),
                        "input": input_item,
                    }
                )
                failed += 1
                if effective_on_error == "stop":
                    break

        total = succeeded + failed
        foreach_result = {
            "items": items,
            "summary": {"total": total, "succeeded": succeeded, "failed": failed},
            "success": failed == 0 or effective_on_error == "continue",
            "duration": total_duration,
        }

        if getattr(step, "flat_output", False):
            foreach_result["items"] = [
                item["data"] for item in foreach_result["items"] if item.get("success")
            ]

        return foreach_result

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
        step = self._apply_profile(step)

        # --- Brick config_defaults merge (T-BRIX-IMP-02) ---
        step = self._apply_brick_defaults(step)

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
        try:
            runner = self._resolve_runner(step.type, jinja_ctx=_early_jinja_ctx)
        except ValueError as policy_err:
            _policy_err_msg = str(policy_err)
            step_statuses[step.id] = StepStatus(
                status="error", duration=0.0, errors=1,
                error_message=_policy_err_msg,
            )
            self.engine.progress.step_start(step.id, step.type)
            self.engine.progress.step_error(step.id, _policy_err_msg)
            effective_on_error = step.on_error or pipeline.error_handling.on_error
            if effective_on_error == "stop":
                return PreExecuteStepResult(
                    step=step,
                    action="break",
                    pipeline_aborted=True,
                )
            return PreExecuteStepResult(step=step, action="continue")
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
        _vc_jinja_ctx = context.to_jinja_context()
        _vc_rendered_params = self.engine.loader.render_step_params(step, _vc_jinja_ctx)
        _vc_rendered_step = _RenderedStep(step, _vc_rendered_params, self.engine.loader, _vc_jinja_ctx)
        _vc_config = _step_config_dict(_vc_rendered_step)
        # Merge top-level step attributes that runners may read
        for _vc_attr in _VALIDATE_CONFIG_TOP_LEVEL_FIELDS:
            _vc_val = getattr(_vc_rendered_step, _vc_attr, None)
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
            dep_err = self._ensure_step_requirements(step)
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

        with self._step_credentials_context(context, step):
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
                chunks = self._chunk_items(items, step.batch_size)
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
                        chunk_result = await self._run_foreach_parallel(
                            chunk_step, chunk, context, pipeline
                        )
                    else:
                        chunk_result = await self._run_foreach_sequential(
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

                foreach_result = self._build_foreach_result(
                    batch_results,
                    step,
                    pipeline,
                )
                if batch_aborted:
                    foreach_result["success"] = False
            elif step.parallel:
                foreach_result = await self._run_foreach_parallel(
                    step, items, context, pipeline
                )
            else:
                foreach_result = await self._run_foreach_sequential(
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
            await self._wait_for_breakpoint_resume(context, step.id)

        self._write_context_snapshot(context)

        self.engine.progress.step_start(step.id, step.type)
        with self._step_credentials_context(context, step):
            step_start = time.monotonic()
            step_started_at = datetime.now(timezone.utc).isoformat()
            result = await self._execute_with_retry(
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
                    payload=runner_progress,
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
            if self._should_persist(step):
                self._persist_step_output(
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
        error_detail = error_msg if isinstance(error_msg, dict) else ({"error": str(error_msg)} if error_msg else None)
        if isinstance(error_msg, dict):
            human_error = str(error_msg.get("error") or error_msg.get("message") or "unknown error")
        else:
            human_error = str(error_msg) if error_msg else None
        if cb_instance is not None:
            try:
                cb_instance.on_failure()
            except Exception:
                pass
        step_statuses[step.id] = StepStatus(
            status="error",
            duration=step_duration,
            errors=1,
            error_message=human_error,
            error_detail=error_detail if isinstance(error_detail, dict) else None,
            resource_usage=resource_usage,
        )
        self.engine.progress.step_error(step.id, human_error, step_duration)
        if self._should_persist(step):
            self._persist_step_output(
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
                error_detail=error_detail,
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
