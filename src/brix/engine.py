"""Pipeline execution engine."""
import asyncio
import os
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from brix.models import Pipeline, Step, StepStatus, RunResult, RetryConfig, RetryProfile
from brix.loader import PipelineLoader
from brix.engine_types import (
    DagSharedState,
    _RenderedStep,
    _SPECIALIST_STEP_TYPES,
    _VALIDATE_CONFIG_TOP_LEVEL_FIELDS,
    _build_logger,
    _capture_environment as _capture_environment_impl,
    _db_log,
    _extract_brick_default_values,  # re-exported for backward compat
    _extract_step_cost,
    _JsonFormatter,
    _measure_rss_mb,
    _redact_secret_values,
    _step_config_dict,
    _total_ram_mb,
    StepResult,
)
from brix.context import PipelineContext
from brix.config import config
from brix.runners.base import BaseRunner, discover_runners
from brix.runners.cli import CliRunner, parse_timeout
from brix.runners.python import PythonRunner
from brix.runners.http import HttpRunner
from brix.runners.mcp import McpRunner
from brix.runners.pipeline import PipelineRunner
from brix.runners.filter import FilterRunner
from brix.runners.transform import TransformRunner
from brix.runners.set import SetRunner
from brix.runners.choose import ChooseRunner
from brix.runners.parallel_runner import ParallelStepRunner
from brix.runners.repeat import RepeatRunner
from brix.runners.notify import NotifyRunner
from brix.runners.approval import ApprovalRunner
from brix.runners.validate import ValidateRunner
from brix.runners.pipeline_group import PipelineGroupRunner
from brix.runners.specialist import SpecialistRunner
from brix.runners.queue import QueueRunner
from brix.runners.emit import EmitRunner
from brix.progress import ProgressReporter
from brix.mcp_pool import McpConnectionPool
from brix.serialization import json_dumps
from brix.engine_dag import detect_dag_mode, run_dag, toposort_steps
from brix.engine_step import StepExecutor
from brix.engine_sequential import finalize_run, run_pipeline_sequential

# ---------------------------------------------------------------------------
# Brick-First Engine — T-BRIX-DB-05c
# ---------------------------------------------------------------------------
# Legacy step type names (old flat names) mapped to their new dot-notation
# system brick names.  When a step uses an old name the engine emits a
# deprecation warning and resolves the runner via the new name.

LEGACY_ALIASES: dict[str, str] = {
    "python": "script.python",
    "http": "http.request",
    "mcp": "mcp.call",
    "cli": "script.cli",
    "filter": "flow.filter",
    "transform": "flow.transform",
    "set": "flow.set",
    "repeat": "flow.repeat",
    "choose": "flow.choose",
    "parallel": "flow.parallel",
    "pipeline": "flow.pipeline",
    "pipeline_group": "flow.pipeline_group",
    "validate": "flow.validate",
    "notify": "action.notify",
    "approval": "action.approval",
    "specialist": "extract.specialist",
    "db_query": "db.query",
    "db_upsert": "db.upsert",
    "db_exec": "db.exec",
    "llm_batch": "llm.batch",
    "markitdown": "markitdown.convert",
    "source": "source.fetch",
    "switch": "flow.switch",
    "merge": "flow.merge",
    "error_handler": "flow.error_handler",
    "wait": "flow.wait",
    "dedup": "flow.dedup",
    "aggregate": "flow.aggregate",
    "flatten": "flow.flatten",
    "diff": "flow.diff",
    "respond": "action.respond",
    # File I/O bricks (T-BRIX-BRICK-02)
    "file_read": "file.read",
    "file_read_base64": "file.read_base64",
    "file_write": "file.write",
    "file_list": "file.list",
    "file_load_json": "file.load_json",
    # Flow/Filter/Extract bricks (T-BRIX-BRICK-03)
    "keyword_filter": "filter.keyword",
    "extract_url": "extract.url",
    "extract_ics": "extract.ics",
}

logger = _build_logger("brix.engine")

_capture_environment = _capture_environment_impl


def _warn_if_high_memory(rss_mb: float, step_id: str) -> None:
    """Compatibility wrapper for tests that patch ``brix.engine._total_ram_mb``."""
    total_mb = _total_ram_mb()
    if total_mb <= 0.0 or rss_mb <= 0.0:
        return
    ratio = rss_mb / total_mb
    if ratio > 0.80:
        pct = round(ratio * 100, 1)
        msg = (
            f"[Resource Warning] Step '{step_id}' RSS={rss_mb:.1f}MB is {pct}% "
            f"of total RAM ({total_mb:.0f}MB). Consider reducing concurrency or batch_size."
        )
        print(msg, file=sys.stderr)
        logger.warning(msg)


class PipelineEngine:
    """Executes pipeline steps sequentially."""

    def __init__(self):
        self.loader = PipelineLoader()
        self.progress = ProgressReporter()
        pipeline_runner = PipelineRunner()
        pipeline_runner.set_engine(self)
        choose_runner = ChooseRunner()
        choose_runner.set_engine(self)
        parallel_runner = ParallelStepRunner()
        parallel_runner.set_engine(self)
        repeat_runner = RepeatRunner()
        repeat_runner.set_engine(self)
        pipeline_group_runner = PipelineGroupRunner()
        pipeline_group_runner.set_engine(self)
        self._runners: dict[str, BaseRunner] = {
            "cli": CliRunner(),
            "python": PythonRunner(),
            "http": HttpRunner(),
            "mcp": McpRunner(),
            "pipeline": pipeline_runner,
            "pipeline_group": pipeline_group_runner,
            "filter": FilterRunner(),
            "transform": TransformRunner(),
            "set": SetRunner(),
            "choose": choose_runner,
            "parallel": parallel_runner,
            "repeat": repeat_runner,
            "notify": NotifyRunner(),
            "approval": ApprovalRunner(),
            "validate": ValidateRunner(),
            "specialist": SpecialistRunner(),
            # T-BRIX-DB-22: Advanced flow runners
            "queue": QueueRunner(),
            "emit": EmitRunner(),
        }
        # Augment with any auto-discovered runners not already registered.
        # This allows third-party or future runners to be picked up automatically
        # without modifying engine.py (T-BRIX-DB-15).
        for step_type, runner_cls in discover_runners().items():
            if step_type not in self._runners:
                self._runners[step_type] = runner_cls()

        # Brick-First Engine — T-BRIX-DB-05c
        # Load the brick registry so that dot-notation step types (e.g. "db.query")
        # can be resolved to their underlying runner name.
        from brix.bricks.registry import BrickRegistry as _BrickRegistry
        from brix.db import BrixDB as _BrixDB
        self._brick_registry = _BrickRegistry(db=_BrixDB())

        # Deprecation tracking DB (T-BRIX-DB-05d) — lazy-init in _resolve_runner
        self._deprecation_db: "BrixDB | None" = None
        # Current pipeline name for deprecation tracking (set per run)
        self._current_pipeline_name: str = ""
        # Current pipeline strict_bricks flag (set per run)
        self._strict_bricks: bool = False
        # Current pipeline policy level (set per run)
        self._policy_level: str = "permissive"
        # Deprecation warnings accumulated during the current run
        self._deprecation_warnings: list[str] = []

        # Pool is created at the start of each run() and closed afterwards.
        self._mcp_pool: McpConnectionPool | None = None
        # Step outputs from the most recently completed run() — used by RepeatRunner
        # to propagate sub-step outputs into the parent context (T-BRIX-V4-BUG-07).
        self._last_step_outputs: dict[str, Any] = {}
        # BrixDB reference for current run — set at start of run(), cleared after (T-BRIX-DB-07)
        self._run_db: "Any | None" = None
        # Saga tracker for current sequential run — set during run(), cleared after.
        self._saga_tracker: "Any | None" = None

    def register_runner(self, step_type: str, runner: BaseRunner) -> None:
        """Register a runner for a step type."""
        self._runners[step_type] = runner

    def _apply_profile(self, step: "Step") -> "Step":
        """Compatibility shim — delegates to ``StepExecutor._apply_profile``."""
        return StepExecutor(self)._apply_profile(step)

    def _apply_brick_defaults(self, step: "Step") -> "Step":
        """Compatibility shim — delegates to ``StepExecutor._apply_brick_defaults``."""
        return StepExecutor(self)._apply_brick_defaults(step)

    def _resolve_runner(self, step_type: str, jinja_ctx: "dict | None" = None) -> "BaseRunner | None":
        """Compatibility shim — delegates to ``StepExecutor._resolve_runner``."""
        return StepExecutor(self)._resolve_runner(step_type, jinja_ctx=jinja_ctx)

    def _resolve_step_credentials(self, step: Any) -> dict[str, Any]:
        """Compatibility shim — delegates to ``StepExecutor._resolve_step_credentials``."""
        return StepExecutor(self)._resolve_step_credentials(step)

    @contextmanager
    def _step_credentials_context(self, context: "PipelineContext", step: Any):
        """Compatibility shim — delegates to ``StepExecutor._step_credentials_context``."""
        with StepExecutor(self)._step_credentials_context(context, step):
            yield

    async def run(self, pipeline: Pipeline, user_input: dict = None, keep_workdir: bool = False, run_id: str = None, profile: str = None, mcp_pool: "McpConnectionPool | None" = None, dry_run_steps: "list[str] | None" = None, _inherit_input: dict = None) -> RunResult:
        """Execute a pipeline and return results.

        If *dry_run_steps* is provided it must be a list of step IDs.  Those
        steps are skipped with ``status="dry_run"`` and ``output=null`` while
        all other steps execute normally.

        If *profile* is provided it is forwarded to ``PipelineContext.from_pipeline``
        which applies the profile's env vars and input defaults before execution.

        If *mcp_pool* is provided the caller's already-open pool is reused and
        this method will NOT close it — the caller owns the lifecycle.  This is
        required when ``engine.run()`` is dispatched via ``asyncio.create_task()``
        because ``ClientSessionGroup`` cancel-scopes must not cross task
        boundaries: open the pool *outside* the task, pass it in here.

        When *mcp_pool* is ``None`` (the default) a new pool is opened and
        closed entirely within this coroutine (the original synchronous behaviour).

        *_inherit_input* is an internal parameter used by sub-pipeline runners
        (e.g. RepeatRunner) to seed the new context's ``input`` dict with the
        parent pipeline's resolved inputs, regardless of whether the mini-pipeline
        declares those keys in its own ``input`` spec.  This ensures that
        ``{{ input.* }}`` templates inside repeat sub-steps resolve correctly.
        """
        from brix.history import RunHistory
        from contextlib import asynccontextmanager

        history = RunHistory()
        # Store DB reference for the duration of the run (T-BRIX-DB-07)
        self._run_db = history._db

        # Reset per-run deprecation state (T-BRIX-DB-05d)
        self._current_pipeline_name = pipeline.name
        self._strict_bricks = pipeline.strict_bricks
        self._policy_level = pipeline.policy_level
        self._deprecation_warnings = []

        start_time = time.monotonic()
        context = PipelineContext.from_pipeline(pipeline, user_input, run_id=run_id, profile=profile)
        context.pipeline_name = pipeline.name
        context._run_db = self._run_db
        # Propagate parent input into the sub-context so {{ input.* }} templates
        # resolve inside sub-pipelines that have no declared input spec (T-BRIX-V4-BUG-INPUT).
        if _inherit_input:
            context.input = {**_inherit_input, **context.input}
            context._jinja_cache = None  # Invalidate so to_jinja_context() rebuilds
        step_statuses: dict[str, StepStatus] = {}
        last_output: Any = None

        # --- Pipeline-Idempotency (T-BRIX-V6-22) ---
        # Evaluate the idempotency_key expression (if declared) and short-circuit
        # if a successful run with the same key exists in the last 24 hours.
        _resolved_idempotency_key: str | None = None
        if pipeline.idempotency_key:
            try:
                jinja_ctx_early = context.to_jinja_context()
                _resolved_idempotency_key = self.loader.render_template(
                    pipeline.idempotency_key, jinja_ctx_early
                ).strip()
            except Exception:
                _resolved_idempotency_key = None
            if _resolved_idempotency_key:
                existing = history.find_by_idempotency_key(_resolved_idempotency_key)
                if existing:
                    import json as _json
                    try:
                        _cached_result = _json.loads(existing["result_summary"]) if existing.get("result_summary") else None
                    except Exception:
                        _cached_result = existing.get("result_summary")
                    return RunResult(
                        success=True,
                        run_id=existing["run_id"],
                        steps={},
                        result=_cached_result,
                        duration=existing.get("duration") or 0.0,
                    )

        # _pool_ctx: if caller provided an open pool reuse it without closing;
        # otherwise open a fresh one and close it when we're done.
        @asynccontextmanager
        async def _pool_ctx():
            if mcp_pool is not None:
                yield mcp_pool
            else:
                async with McpConnectionPool() as fresh_pool:
                    yield fresh_pool

        async with _pool_ctx() as pool:
            self._mcp_pool = pool
            mcp_runner = self._runners.get("mcp")
            if mcp_runner is not None and hasattr(mcp_runner, "pool"):
                mcp_runner.pool = self._mcp_pool

            _env_snapshot = self._capture_environment()
            _container_id = os.environ.get("HOSTNAME", "unknown")
            history.record_start(
                context.run_id, pipeline.name, pipeline.version, user_input,
                idempotency_key=_resolved_idempotency_key,
                environment=_env_snapshot,
                container_id=_container_id,
            )

            # --- Application logging: run start (T-BRIX-V7-08) ---
            _start_msg = f"Run started: pipeline={pipeline.name} run_id={context.run_id}"
            logger.info(_start_msg)
            _db_log("INFO", "engine", _start_msg)

            # --- Run Input Persistence (T-BRIX-DB-07) ---
            try:
                history._db.record_run_input(
                    run_id=context.run_id,
                    input_params=user_input or {},
                    trigger_data={},
                )
            except Exception:
                pass  # Never crash pipeline over persistence

            # --- Auto-Annotation: project from pipeline metadata (T-BRIX-IMP-04) ---
            try:
                _pipeline_row = history._db.get_pipeline(pipeline.name)
                if _pipeline_row:
                    _pipeline_project = _pipeline_row.get("project", "")
                    if _pipeline_project:
                        import json as _json_ann
                        history._db.annotate_run(
                            context.run_id,
                            _json_ann.dumps({"project": _pipeline_project}),
                        )
            except Exception:
                pass  # Never crash pipeline over annotation

            # --- Helper registry resolution (T-BRIX-V4-BUG-12) ---
            # Resolve step.helper → step.script using the HelperRegistry and
            # install any helper-specific requirements before execution.
            from brix.helper_registry import HelperRegistry as _HelperRegistry
            _helper_registry = _HelperRegistry()
            for step in pipeline.steps:
                if step.helper:
                    entry = _helper_registry.get(step.helper)
                    if entry is None:
                        dep_error_msg = (
                            f"Step '{step.id}': Helper '{step.helper}' not found in registry"
                        )
                        print(f"✗ {dep_error_msg}", file=sys.stderr)
                        context.save_run_metadata(pipeline.name, "failed")
                        return RunResult(
                            success=False,
                            run_id=context.run_id,
                            steps={},
                            result=None,
                            duration=time.monotonic() - start_time,
                        )
                    # Only override script if not already explicitly set
                    if not step.script:
                        step.script = entry.script
                    # Install helper-level requirements
                    if entry.requirements:
                        from brix.deps import check_requirements, install_requirements
                        missing_helper = check_requirements(entry.requirements)
                        if missing_helper:
                            print(
                                f"⚙ Installing {len(missing_helper)} helper package(s) "
                                f"for '{step.helper}': {', '.join(missing_helper)}",
                                file=sys.stderr,
                            )
                            ok = install_requirements(missing_helper)
                            if not ok:
                                dep_error_msg = (
                                    f"Failed to install helper packages for '{step.helper}': "
                                    f"{', '.join(missing_helper)}"
                                )
                                print(f"✗ {dep_error_msg}", file=sys.stderr)
                                context.save_run_metadata(pipeline.name, "failed")
                                return RunResult(
                                    success=False,
                                    run_id=context.run_id,
                                    steps={},
                                    result=None,
                                    duration=time.monotonic() - start_time,
                                )

            # --- Dual-Path Resolution (T-BRIX-V5-02) ---
            # For steps with script: paths, apply search order:
            # 1. Absolute path → use as-is
            # 2. ~/.brix/helpers/<name>.py  (managed helper storage)
            # 3. /app/helpers/<name>.py     (legacy container path, deprecation warning)
            from pathlib import Path as _Path
            _managed_helpers_dir = _Path.home() / ".brix" / "helpers"
            _legacy_helpers_dir = _Path(config.LEGACY_HELPERS_DIR)
            for step in pipeline.steps:
                if step.script and not _Path(step.script).is_absolute():
                    # Relative path — extract the script filename and search
                    script_name = _Path(step.script).name
                    managed_candidate = _managed_helpers_dir / script_name
                    legacy_candidate = _legacy_helpers_dir / script_name
                    if managed_candidate.exists():
                        step.script = str(managed_candidate)
                    elif legacy_candidate.exists():
                        print(
                            f"⚠ Step '{step.id}': using legacy helper path {legacy_candidate}. "
                            f"Run 'brix migrate-helpers' to migrate to ~/.brix/helpers/",
                            file=sys.stderr,
                        )
                        step.script = str(legacy_candidate)

            # --- Dependency management (T-BRIX-V4-BUG-11) ---
            if pipeline.requirements:
                from brix.deps import check_requirements, install_requirements
                missing = check_requirements(pipeline.requirements)
                if missing:
                    print(
                        f"⚙ Installing {len(missing)} missing package(s): {', '.join(missing)}",
                        file=sys.stderr,
                    )
                    ok = install_requirements(missing)
                    if not ok:
                        dep_error_msg = (
                            f"Failed to install required packages: {', '.join(missing)}"
                        )
                        print(f"✗ {dep_error_msg}", file=sys.stderr)
                        context.save_run_metadata(pipeline.name, "failed")
                        return RunResult(
                            success=False,
                            run_id=context.run_id,
                            steps={},
                            result=None,
                            duration=time.monotonic() - start_time,
                        )

            # Wire workdir into ProgressReporter now that context (and its workdir) exists
            self.progress._workdir = str(context.workdir)

            # Save run metadata
            context.save_run_metadata(pipeline.name, "running")

            self.progress.pipeline_start(pipeline.name, len(pipeline.steps))

            pipeline_aborted = False  # set to True on early-stop so we skip post-loop work
            stop_step_success: bool | None = None  # set by 'stop' step to override all_ok
            total_cost_usd: float = 0.0  # accumulated LLM cost from step outputs (T-BRIX-V6-21)
            last_output: Any = None
            dag_state = DagSharedState()
            result: RunResult | None = None

            # --- Saga Tracker (T-BRIX-DB-21) ---
            from brix.resilience import SagaTracker as _SagaTracker
            self._saga_tracker = _SagaTracker()

            # --- DAG mode (T-BRIX-V6-19) ---
            # If any step declares depends_on, switch to parallel DAG execution.
            # The outer try/finally wraps both paths for cleanup.
            try:
                if detect_dag_mode(pipeline.steps):
                    try:
                        pipeline_aborted, last_output, _, stop_step_success = await run_dag(
                            self, pipeline, context, step_statuses, dry_run_steps, dag_state
                        )
                        total_cost_usd = dag_state.total_cost_usd
                    except ValueError as dag_err:
                        print(f"✗ DAG error: {dag_err}", file=sys.stderr)
                        pipeline_aborted = True
                else:
                    pipeline_aborted, last_output, total_cost_usd, stop_step_success = await (
                        run_pipeline_sequential(
                            self,
                            pipeline,
                            context,
                            step_statuses,
                            dry_run_steps,
                        )
                    )

            except Exception as e:
                # Unexpected exception (e.g. schema validation error, MCP crash) —
                # treat the run as failed but always reach the finally block.
                tb = traceback.format_exc()
                completed_steps = list(context.step_outputs.keys())
                last_completed_step = completed_steps[-1] if completed_steps else None
                step_context = (
                    f"last_completed_step={last_completed_step}"
                    if last_completed_step is not None
                    else "last_completed_step=<none>"
                )
                error_message = (
                    "Unhandled engine exception "
                    f"phase=execution boundary=engine {step_context} "
                    f"completed_steps={completed_steps} "
                    f"root_exception={type(e).__name__}: {e}\n{tb}"
                )
                step_statuses["_engine_error"] = StepStatus(
                    status="error",
                    duration=0.0,
                    errors=1,
                    error_message=error_message,
                )
                logger.error("Unhandled engine exception for run %s:\n%s", context.run_id, tb)
                print(f"✗ Pipeline error: {e}", file=sys.stderr)
                pipeline_aborted = True

            finally:
                # Detach pool from runner (pool itself is closed by the async with block above).
                mcp_runner = self._runners.get("mcp")
                if mcp_runner is not None and hasattr(mcp_runner, "pool"):
                    mcp_runner.pool = None
                self._mcp_pool = None
                self._run_db = None  # Clear run-scoped DB reference (T-BRIX-DB-07)
                self._saga_tracker = None
            result = await finalize_run(
                self,
                pipeline,
                context,
                step_statuses,
                pipeline_aborted,
                stop_step_success,
                last_output,
                total_cost_usd,
                start_time,
                history,
                keep_workdir,
                self._deprecation_warnings,
            )

        # Expose sub-step outputs for callers that need to propagate them
        # (e.g. RepeatRunner merging sub-step outputs into the parent context).
        self._last_step_outputs = dict(context.step_outputs)

        return result

    # ------------------------------------------------------------------
    # Execution Data persistence (T-BRIX-V7-04)
    # ------------------------------------------------------------------

    @staticmethod
    def _should_persist(step: Step) -> bool:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        return StepExecutor._should_persist(step)

    @staticmethod
    def _context_snapshot(context: Any) -> dict:
        """Compatibility shim — delegates to ``StepExecutor._context_snapshot``."""
        return StepExecutor._context_snapshot(context)

    @staticmethod
    def _capture_environment() -> dict:
        return _capture_environment()

    def _persist_step_output(
        self,
        run_id: str,
        step: Step,
        result: dict,
        rendered_params: dict,
        context: Any,
        db: Any = None,
    ) -> None:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        StepExecutor(self)._persist_step_output(run_id, step, result, rendered_params, context, db=db)

    # ------------------------------------------------------------------
    # Breakpoint helpers (T-BRIX-V7-06)
    # ------------------------------------------------------------------

    def _write_context_snapshot(self, context: Any) -> None:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        StepExecutor(self)._write_context_snapshot(context)

    async def _wait_for_breakpoint_resume(self, context: Any, step_id: str) -> None:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        await StepExecutor(self)._wait_for_breakpoint_resume(context, step_id)

    # ------------------------------------------------------------------
    # per-step dependency helper (T-BRIX-V6-03)
    # ------------------------------------------------------------------

    def _ensure_step_requirements(self, step: Step) -> "str | None":
        """Compatibility wrapper delegated to ``StepExecutor``."""
        return StepExecutor(self)._ensure_step_requirements(step)

    # ------------------------------------------------------------------
    # retry helper
    # ------------------------------------------------------------------

    async def _execute_with_retry(
        self, runner: BaseRunner, rendered_step: Any, context: Any, step: Step, pipeline: Pipeline
    ) -> dict:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        return await StepExecutor(self)._execute_with_retry(
            runner, rendered_step, context, step, pipeline
        )

    # ------------------------------------------------------------------
    # batch_size helper
    # ------------------------------------------------------------------

    def _chunk_items(self, items: list, batch_size: int) -> list[list]:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        return StepExecutor(self)._chunk_items(items, batch_size)

    # ------------------------------------------------------------------
    # foreach helpers
    # ------------------------------------------------------------------

    def _is_run_cancelled(self, context: PipelineContext) -> bool:
        """Return True if cancel_requested.json exists in the run workdir."""
        try:
            sentinel = context.workdir / "cancel_requested.json"
            return sentinel.exists()
        except Exception:
            return False

    async def _run_foreach_sequential(
        self, step: Step, items: list, context: PipelineContext, pipeline: Pipeline
    ) -> dict:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        return await StepExecutor(self)._run_foreach_sequential(step, items, context, pipeline)

    async def _run_foreach_parallel(
        self, step: Step, items: list, context: PipelineContext, pipeline: Pipeline
    ) -> dict:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        return await StepExecutor(self)._run_foreach_parallel(step, items, context, pipeline)

    # ------------------------------------------------------------------
    # DAG execution helper (T-BRIX-V6-19)
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_dag_mode(steps: list[Step]) -> bool:
        """Compatibility wrapper for DAG mode detection."""
        return detect_dag_mode(steps)

    @staticmethod
    def _toposort_steps(steps: list[Step]) -> list[Step]:
        """Compatibility wrapper for DAG topological sorting."""
        return toposort_steps(steps)

    def _build_foreach_result(
        self, results: list[tuple[Any, dict]], step: Step, pipeline: Pipeline
    ) -> dict:
        """Compatibility wrapper delegated to ``StepExecutor``."""
        return StepExecutor(self)._build_foreach_result(results, step, pipeline)
