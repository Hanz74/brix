"""Pipeline execution engine."""
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

from brix.models import Pipeline, Step, StepStatus, RunResult, RetryConfig, RetryProfile
from brix.loader import PipelineLoader
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
}

# ---------------------------------------------------------------------------
# Application logger (T-BRIX-V7-08)
# ---------------------------------------------------------------------------
# Reads BRIX_LOG_LEVEL from the environment (default INFO).
# Emits JSON-formatted records to stderr so they interleave cleanly with
# the progress reporter output that already goes to stderr.

_log_level_name = os.environ.get("BRIX_LOG_LEVEL", "INFO").upper()
_log_level = getattr(logging, _log_level_name, logging.INFO)


class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record to stderr."""

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                "level": record.levelname,
                "component": record.name,
                "message": record.getMessage(),
            }
        )


def _build_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(_log_level)
    return logger


logger = _build_logger("brix.engine")


def _db_log(level: str, component: str, message: str) -> None:
    """Write one entry to the brix.db app_log table (best-effort, never raises)."""
    try:
        from brix.db import BrixDB
        BrixDB().write_app_log(level=level, component=component, message=message)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Resource monitoring helpers (T-BRIX-V7-07)
# ---------------------------------------------------------------------------

def _measure_rss_mb() -> float:
    """Return the current RSS memory usage of this process in megabytes.

    Reads /proc/self/status (Linux).  Falls back to 0.0 if unavailable.
    """
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    # VmRSS:    12345 kB
                    kb = int(line.split()[1])
                    return round(kb / 1024.0, 2)
    except Exception:
        pass
    # Fallback via os.getpid() + resource module
    try:
        import resource
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # On Linux ru_maxrss is in kilobytes; on macOS in bytes
        if os.uname().sysname == "Darwin":
            return round(kb / (1024.0 * 1024.0), 2)
        return round(kb / 1024.0, 2)
    except Exception:
        return 0.0


def _total_ram_mb() -> float:
    """Return total system RAM in MB from /proc/meminfo, or 0.0 if unavailable."""
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return kb / 1024.0
    except Exception:
        pass
    return 0.0


def _warn_if_high_memory(rss_mb: float, step_id: str) -> None:
    """Emit a warning if RSS > 80% of total available RAM (best-effort)."""
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
        self._brick_registry = _BrickRegistry()

        # Deprecation tracking DB (T-BRIX-DB-05d) — lazy-init in _resolve_runner
        self._deprecation_db: "BrixDB | None" = None
        # Current pipeline name for deprecation tracking (set per run)
        self._current_pipeline_name: str = ""
        # Current pipeline strict_bricks flag (set per run)
        self._strict_bricks: bool = False
        # Deprecation warnings accumulated during the current run
        self._deprecation_warnings: list[str] = []

        # Pool is created at the start of each run() and closed afterwards.
        self._mcp_pool: McpConnectionPool | None = None
        # Step outputs from the most recently completed run() — used by RepeatRunner
        # to propagate sub-step outputs into the parent context (T-BRIX-V4-BUG-07).
        self._last_step_outputs: dict[str, Any] = {}
        # BrixDB reference for current run — set at start of run(), cleared after (T-BRIX-DB-07)
        self._run_db: "Any | None" = None

    def register_runner(self, step_type: str, runner: BaseRunner) -> None:
        """Register a runner for a step type."""
        self._runners[step_type] = runner

    def _apply_profile(self, step: "Step") -> "Step":
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
                    # Only apply profile value when step has NOT explicitly set this field
                    # (i.e. step field still holds its model default)
                    # Use Step.model_fields (class attribute) to avoid Pydantic V2.11 warning
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

    def _apply_brick_defaults(self, step: "Step") -> "Step":
        """Merge config_defaults from a custom brick into step.params (T-BRIX-IMP-02).

        When a step type is a custom brick registered in the DB, the brick may
        declare ``config_defaults`` (stored as ``config_schema`` in the DB row as
        a flat key→value JSON object).  These defaults act as a baseline for
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
            if not brick_defaults:
                return step
            # Merge: brick defaults as base, step.params override
            merged_params = {**brick_defaults, **(step.params or {})}
            if merged_params == (step.params or {}):
                return step  # Nothing new to add
            # Use model_construct to bypass Literal validation so custom brick
            # types (e.g. "cody.call") that are not in the Literal enum work.
            step_dict = step.__dict__.copy()
            step_dict["params"] = merged_params
            return step.model_copy(update={"params": merged_params})
        except Exception as _brick_err:
            logger.warning("Brick defaults merge failed for step '%s': %s", step.id, _brick_err)
            return step

    def _resolve_runner(self, step_type: str, jinja_ctx: "dict | None" = None) -> "BaseRunner | None":
        """Resolve a runner for a given step type using the Brick-First lookup chain.

        Resolution order (T-BRIX-DB-05c / T-BRIX-DB-23):
        0. Dynamic Dispatch: if step_type contains Jinja2 template syntax
           (``{{ ... }}``), render it using *jinja_ctx* first, then continue
           the normal resolution chain.  The rendered type MUST exist in the
           brick registry or runner map — unknown rendered types return None.
        1. Legacy-Alias lookup: if step_type is an old flat name that is mapped
           in LEGACY_ALIASES, emit a deprecation warning and use the new brick
           name for resolution.  This takes priority over the direct runner lookup
           so that deprecated names always produce a DeprecationWarning.
        2. Brick-Registry lookup: if step_type is a dot-notation brick name
           (e.g. "db.query"), look it up in the BrickRegistry, then resolve the
           runner via the brick's ``runner`` field.
        3. Direct lookup in self._runners (fast path for newly-added flat runner
           names that are not yet in LEGACY_ALIASES).

        Returns None if no runner can be resolved.
        """
        # 0. Dynamic Dispatch (T-BRIX-DB-23): render Jinja2 step type
        if "{{" in step_type and jinja_ctx is not None:
            try:
                rendered_type = self.loader.render_template(step_type, jinja_ctx).strip()
            except Exception as _dyn_err:
                logger.warning("Dynamic dispatch: failed to render step type '%s': %s", step_type, _dyn_err)
                return None
            # Security: rendered type MUST exist in registry or direct runner map
            if rendered_type not in self._runners and self._brick_registry.get(rendered_type) is None:
                logger.warning(
                    "Dynamic dispatch: rendered type '%s' is not a registered brick or runner",
                    rendered_type,
                )
                return None
            step_type = rendered_type
        # 1. Legacy-Alias layer — old flat name → new brick name → runner (with warning)
        new_name = LEGACY_ALIASES.get(step_type)
        if new_name:
            # strict_bricks=True: block old types with an error (T-BRIX-DB-05d)
            if self._strict_bricks:
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
                if self._deprecation_db is None:
                    from brix.db import BrixDB as _BrixDB
                    self._deprecation_db = _BrixDB()
                self._deprecation_db.record_deprecated_usage(
                    pipeline_name=self._current_pipeline_name or "unknown",
                    step_id=step_type,  # step_id not available here; use type as fallback
                    old_type=step_type,
                    new_type=new_name,
                )
            except Exception:
                pass  # Never crash the engine over tracking
            # Accumulate deprecation warning for run result
            warn_msg = f"Step type '{step_type}' is deprecated. Use '{new_name}' instead."
            if warn_msg not in self._deprecation_warnings:
                self._deprecation_warnings.append(warn_msg)
            brick = self._brick_registry.get(new_name)
            if brick and brick.runner:
                runner = self._runners.get(brick.runner)
                if runner is not None:
                    return runner

        # 2. Brick-Registry lookup (new dot-notation names like "db.query")
        brick = self._brick_registry.get(step_type)
        if brick and brick.runner:
            runner = self._runners.get(brick.runner)
            if runner is not None:
                return runner

        # 3. Direct runner lookup (fast path for flat names not in LEGACY_ALIASES)
        runner = self._runners.get(step_type)
        if runner is not None:
            return runner

        return None

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
        self._deprecation_warnings = []

        start_time = time.monotonic()
        context = PipelineContext.from_pipeline(pipeline, user_input, run_id=run_id, profile=profile)
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

            final_result = None
            all_ok = False
            pipeline_aborted = False  # set to True on early-stop so we skip post-loop work
            stop_step_success: bool | None = None  # set by 'stop' step to override all_ok
            total_cost_usd: float = 0.0  # accumulated LLM cost from step outputs (T-BRIX-V6-21)

            # --- Saga Tracker (T-BRIX-DB-21) ---
            from brix.resilience import SagaTracker as _SagaTracker
            _saga_tracker = _SagaTracker()

            # --- DAG mode (T-BRIX-V6-19) ---
            # If any step declares depends_on, switch to parallel DAG execution.
            # The outer try/finally wraps both paths for cleanup.
            try:
                if self._detect_dag_mode(pipeline.steps):
                    try:
                        pipeline_aborted, last_output, _, stop_step_success = await self._run_dag(
                            pipeline, context, step_statuses, dry_run_steps
                        )
                    except ValueError as dag_err:
                        print(f"✗ DAG error: {dag_err}", file=sys.stderr)
                        pipeline_aborted = True
                else:
                    for step in pipeline.steps:
                        # Cancel-flag check: abort pipeline cleanly if cancel was requested (T-BRIX-V6-BUG-03)
                        if self._is_run_cancelled(context):
                            pipeline_aborted = True
                            break

                        # Resume: skip completed steps
                        if context.is_step_completed(step.id):
                            step_statuses[step.id] = StepStatus(status="ok", duration=0.0)
                            last_output = context.get_output(step.id)
                            self.progress.step_resumed(step.id)
                            continue

                        # Disabled steps are unconditionally skipped (T-BRIX-V4-02)
                        if not step.enabled:
                            step_statuses[step.id] = StepStatus(
                                status="skipped", duration=0.0, reason="disabled"
                            )
                            self.progress.step_skipped(step.id)
                            continue

                        # Selective dry-run: skip named steps without executing (T-BRIX-V4-BUG-09)
                        if dry_run_steps and step.id in dry_run_steps:
                            step_statuses[step.id] = StepStatus(
                                status="dry_run", duration=0.0, reason="dry_run_steps"
                            )
                            # Do not set context output — downstream steps see null for this step
                            self.progress.step_skipped(step.id)
                            continue

                        # Evaluate when condition
                        jinja_ctx = context.to_jinja_context()
                        if step.when:
                            should_run = self.loader.evaluate_condition(step.when, jinja_ctx)
                            if not should_run:
                                step_statuses[step.id] = StepStatus(
                                    status="skipped", duration=0.0, reason="condition not met"
                                )
                                self.progress.step_skipped(step.id)
                                continue

                        # Evaluate else_of: only run this step when the referenced step was skipped
                        if step.else_of:
                            ref_status = step_statuses.get(step.else_of)
                            if ref_status is None or ref_status.status != "skipped":
                                step_statuses[step.id] = StepStatus(
                                    status="skipped",
                                    duration=0.0,
                                    reason=f"else_of '{step.else_of}' was not skipped",
                                )
                                self.progress.step_skipped(step.id)
                                continue

                        # stop step: end the pipeline immediately (T-BRIX-V4-04)
                        if step.type == "stop":
                            jinja_ctx = context.to_jinja_context()
                            msg = step.message or "Pipeline stopped"
                            rendered_msg = self.loader.render_template(msg, jinja_ctx) if "{{" in msg else msg
                            step_statuses[step.id] = StepStatus(
                                status="ok", duration=0.0, reason=rendered_msg
                            )
                            pipeline_aborted = True
                            stop_step_success = getattr(step, "success_on_stop", True)
                            break

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
                                self.progress.step_start(step.id, step.type)
                                self.progress.step_error(step.id, _cm_msg)
                                effective_on_error = step.on_error or pipeline.error_handling.on_error
                                if effective_on_error == "stop":
                                    pipeline_aborted = True
                                    break
                                continue

                        # --- Profile / Mixin (T-BRIX-DB-23) ---
                        step = self._apply_profile(step)

                        # --- Brick config_defaults merge (T-BRIX-IMP-02) ---
                        step = self._apply_brick_defaults(step)

                        # Build an early jinja context for dynamic dispatch type rendering
                        _early_jinja_ctx = context.to_jinja_context() if "{{" in step.type else None

                        # Get runner
                        runner = self._resolve_runner(step.type, jinja_ctx=_early_jinja_ctx)
                        if not runner:
                            _no_runner_msg = f"no runner registered for type '{step.type}'"
                            step_statuses[step.id] = StepStatus(
                                status="error", duration=0.0, errors=1,
                                error_message=_no_runner_msg,
                            )
                            self.progress.step_start(step.id, step.type)
                            self.progress.step_error(step.id, _no_runner_msg)
                            effective_on_error = step.on_error or pipeline.error_handling.on_error
                            if effective_on_error == "stop":
                                pipeline_aborted = True
                                break
                            continue

                        # --- foreach branch ---
                        if step.foreach:
                            # Per-step dependency check for foreach steps (T-BRIX-V6-03)
                            if step.requirements:
                                dep_err = self._ensure_step_requirements(step)
                                if dep_err:
                                    step_statuses[step.id] = StepStatus(
                                        status="error", duration=0.0, errors=1,
                                        error_message=dep_err,
                                    )
                                    self.progress.step_start(step.id, step.type)
                                    self.progress.step_error(step.id, dep_err)
                                    effective_on_error = step.on_error or pipeline.error_handling.on_error
                                    if effective_on_error == "stop":
                                        pipeline_aborted = True
                                        break
                                    continue

                            jinja_ctx = context.to_jinja_context()
                            items = self.loader.resolve_foreach(step.foreach, jinja_ctx)

                            step_start = time.monotonic()
                            if step.batch_size > 0:
                                # Batch mode: chunk items and run each batch through the existing foreach path
                                chunks = self._chunk_items(items, step.batch_size)
                                all_batch_items: list = []
                                all_batch_succeeded = 0
                                all_batch_failed = 0
                                batch_aborted = False
                                for chunk_idx, chunk in enumerate(chunks):
                                    self.progress.step_start(
                                        f"{step.id}[batch {chunk_idx + 1}/{len(chunks)}]", step.type
                                    )
                                    if step.parallel:
                                        chunk_result = await self._run_foreach_parallel(step, chunk, context, pipeline)
                                    else:
                                        chunk_result = await self._run_foreach_sequential(step, chunk, context, pipeline)
                                    chunk_summary = chunk_result.get("summary", {})
                                    all_batch_items.extend(chunk_result.get("items", []))
                                    all_batch_succeeded += chunk_summary.get("succeeded", 0)
                                    all_batch_failed += chunk_summary.get("failed", 0)
                                    if not chunk_result.get("success"):
                                        effective_on_error = step.on_error or pipeline.error_handling.on_error
                                        if effective_on_error == "stop":
                                            batch_aborted = True
                                            break
                                foreach_result = {
                                    "items": all_batch_items,
                                    "summary": {
                                        "total": all_batch_succeeded + all_batch_failed,
                                        "succeeded": all_batch_succeeded,
                                        "failed": all_batch_failed,
                                    },
                                    "success": all_batch_failed == 0 or (
                                        not batch_aborted and
                                        (step.on_error or pipeline.error_handling.on_error) == "continue"
                                    ),
                                    "duration": 0.0,
                                }
                            elif step.parallel:
                                foreach_result = await self._run_foreach_parallel(step, items, context, pipeline)
                            else:
                                foreach_result = await self._run_foreach_sequential(step, items, context, pipeline)
                            step_duration = time.monotonic() - step_start

                            # --- Performance hints ---
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
                                self.progress.foreach_done(
                                    step.id,
                                    summary.get("total", 0),
                                    summary.get("succeeded", 0),
                                    summary.get("failed", 0),
                                    step_duration,
                                )
                            else:
                                summary = foreach_result.get("summary", {})
                                _foreach_err_msg = f"foreach failed ({summary.get('failed', '?')} of {summary.get('total', '?')} items failed)"
                                step_statuses[step.id] = StepStatus(
                                    status="error",
                                    duration=step_duration,
                                    errors=summary.get("failed", 1),
                                    error_message=_foreach_err_msg,
                                )
                                self.progress.step_start(step.id, step.type)
                                self.progress.step_error(
                                    step.id,
                                    _foreach_err_msg,
                                    step_duration,
                                )
                                effective_on_error = step.on_error or pipeline.error_handling.on_error
                                if effective_on_error == "stop":
                                    pipeline_aborted = True
                                    break
                            continue

                        # --- per-step dependency check (T-BRIX-V6-03) ---
                        if step.requirements:
                            dep_err = self._ensure_step_requirements(step)
                            if dep_err:
                                step_statuses[step.id] = StepStatus(
                                    status="error", duration=0.0, errors=1,
                                    error_message=dep_err,
                                )
                                self.progress.step_start(step.id, step.type)
                                self.progress.step_error(step.id, dep_err)
                                effective_on_error = step.on_error or pipeline.error_handling.on_error
                                if effective_on_error == "stop":
                                    pipeline_aborted = True
                                    break
                                continue

                        # --- single-step branch ---
                        # Render step params with current context
                        jinja_ctx = context.to_jinja_context()
                        rendered_params = self.loader.render_step_params(step, jinja_ctx)

                        # Create a rendered step-like object for the runner
                        rendered_step = _RenderedStep(step, rendered_params, self.loader, jinja_ctx)

                        # --- Step Pin check (T-BRIX-DB-24): use mock data if step is pinned ---
                        _pin_hit = None
                        try:
                            from brix.db import BrixDB as _PinDB
                            _pin_db = _PinDB()
                            _pin_record = _pin_db.get_pin(pipeline.name, step.id)
                            if _pin_record is not None:
                                _pin_hit = _pin_record["pinned_data"]
                        except Exception as _pin_err:
                            logger.warning("Step pin check failed for '%s': %s", step.id, _pin_err)
                        if _pin_hit is not None:
                            logger.info("Step '%s' using pinned mock data (pipeline=%s)", step.id, pipeline.name)
                            context.set_output(step.id, _pin_hit)
                            last_output = _pin_hit
                            step_statuses[step.id] = StepStatus(
                                status="ok",
                                duration=0.0,
                                reason="pin_mock",
                            )
                            self.progress.step_ok(step.id, 0.0, None)
                            continue

                        # --- Test-Mode: intercept db.upsert and action.notify (T-BRIX-DB-24) ---
                        _effective_step_type = LEGACY_ALIASES.get(step.type, step.type)
                        if pipeline.test_mode and _effective_step_type in ("db.upsert", "db_upsert"):
                            logger.info(
                                "Test-mode: dry-running db.upsert step '%s' (pipeline=%s)",
                                step.id, pipeline.name,
                            )
                            context.set_output(step.id, {"test_mode": True, "dry": True, "step_id": step.id})
                            last_output = {"test_mode": True, "dry": True, "step_id": step.id}
                            step_statuses[step.id] = StepStatus(
                                status="ok",
                                duration=0.0,
                                reason="test_mode_dry",
                            )
                            self.progress.step_ok(step.id, 0.0, None)
                            continue
                        if pipeline.test_mode and _effective_step_type in ("action.notify", "notify"):
                            logger.info(
                                "Test-mode: log-only action.notify step '%s' (pipeline=%s)",
                                step.id, pipeline.name,
                            )
                            context.set_output(step.id, {"test_mode": True, "log_only": True, "step_id": step.id})
                            last_output = {"test_mode": True, "log_only": True, "step_id": step.id}
                            step_statuses[step.id] = StepStatus(
                                status="ok",
                                duration=0.0,
                                reason="test_mode_log_only",
                            )
                            self.progress.step_ok(step.id, 0.0, None)
                            continue

                        # --- Step-Level Cache (T-BRIX-V6-24, legacy bool form) ---
                        if step.cache is True:
                            from brix.context import CacheManager
                            _cache_mgr = CacheManager()
                            _cached_output = _cache_mgr.get(step.id, rendered_params)
                            if _cached_output is not None:
                                context.set_output(step.id, _cached_output)
                                last_output = _cached_output
                                total_cost_usd += _extract_step_cost(_cached_output)
                                step_statuses[step.id] = StepStatus(
                                    status="ok",
                                    duration=0.0,
                                    reason="cache_hit",
                                )
                                self.progress.step_ok(step.id, 0.0, None)
                                continue

                        # --- Resilience: Brick Cache check (T-BRIX-DB-21, dict form) ---
                        _brick_cache_instance = None
                        _brick_cache_rendered_key = None
                        if isinstance(step.cache, dict):
                            try:
                                from brix.resilience import BrickCache as _BrickCache, BrixDB as _res_BrixDB
                                _brick_cache_instance = _BrickCache(step.cache, _res_BrixDB())
                                _brick_cache_rendered_key = self.loader.render_template(
                                    step.cache.get("key", step.id), jinja_ctx
                                )
                                _bc_hit = _brick_cache_instance.get(_brick_cache_rendered_key)
                                if _bc_hit is not None:
                                    context.set_output(step.id, _bc_hit)
                                    last_output = _bc_hit
                                    total_cost_usd += _extract_step_cost(_bc_hit)
                                    step_statuses[step.id] = StepStatus(
                                        status="ok",
                                        duration=0.0,
                                        reason="cache_hit",
                                    )
                                    self.progress.step_ok(step.id, 0.0, None)
                                    continue
                            except Exception as _bc_err:
                                logger.warning("Brick cache check failed for '%s': %s", step.id, _bc_err)

                        # --- Resilience: Circuit Breaker pre-check (T-BRIX-DB-21) ---
                        _cb_instance = None
                        if step.circuit_breaker:
                            try:
                                from brix.resilience import CircuitBreaker as _CircuitBreaker, BrixDB as _res_BrixDB
                                _cb_instance = _CircuitBreaker(step.id, step.circuit_breaker, _res_BrixDB())
                                _cb_pre = _cb_instance.pre_check(context)
                                if _cb_pre is not None:
                                    # Circuit is open — skip or fallback
                                    if _cb_pre.get("success"):
                                        context.set_output(step.id, _cb_pre.get("data"))
                                        last_output = _cb_pre.get("data")
                                        step_statuses[step.id] = StepStatus(
                                            status="ok",
                                            duration=0.0,
                                            reason="circuit_breaker_fallback",
                                        )
                                        self.progress.step_ok(step.id, 0.0, None)
                                    else:
                                        _cb_err_msg = _cb_pre.get("error", "Circuit breaker OPEN")
                                        step_statuses[step.id] = StepStatus(
                                            status="skipped",
                                            duration=0.0,
                                            reason=_cb_err_msg,
                                        )
                                        self.progress.step_skipped(step.id)
                                    continue
                            except Exception as _cb_err:
                                logger.warning("Circuit breaker check failed for '%s': %s", step.id, _cb_err)

                        # --- Resilience: Rate Limiter (T-BRIX-DB-21) ---
                        _rl_instance = None
                        if step.rate_limit:
                            try:
                                from brix.resilience import RateLimiter as _RateLimiter, BrixDB as _res_BrixDB
                                _rl_instance = _RateLimiter(step.id, step.rate_limit, _res_BrixDB())
                                _rl_wait = _rl_instance.wait_seconds()
                                if _rl_wait > 0:
                                    await asyncio.sleep(_rl_wait)
                            except Exception as _rl_err:
                                logger.warning("Rate limiter check failed for '%s': %s", step.id, _rl_err)

                        # --- Breakpoint (T-BRIX-V7-06) ---
                        if step.pause_before:
                            await self._wait_for_breakpoint_resume(context, step.id)

                        # --- Context Snapshot to workdir (T-BRIX-V7-06) ---
                        # Written for every step so brix__inspect_context can
                        # read the current Jinja2 context of a running run.
                        self._write_context_snapshot(context)

                        self.progress.step_start(step.id, step.type)
                        step_start = time.monotonic()
                        _step_started_at = datetime.now(timezone.utc).isoformat()
                        result = await self._execute_with_retry(runner, rendered_step, context, step, pipeline)
                        step_duration = time.monotonic() - step_start
                        _step_ended_at = datetime.now(timezone.utc).isoformat()

                        # --- report_progress compliance check (T-BRIX-DB-15) ---
                        # Warn if the runner did not call report_progress() at all.
                        if getattr(runner, "_progress", None) is None:
                            logger.warning(
                                "Runner '%s' (step '%s') did not call report_progress() — "
                                "consider adding self.report_progress(100.0) at the end of execute()",
                                step.type, step.id,
                            )

                        # --- Persist runner progress to DB (T-BRIX-DB-14) ---
                        _runner_progress = getattr(runner, "_progress", None)
                        if _runner_progress is not None and self._run_db is not None:
                            try:
                                self._run_db.update_step_progress(
                                    run_id=context.run_id,
                                    step_id=step.id,
                                    pct=_runner_progress.get("pct", 100.0),
                                    msg=_runner_progress.get("msg", ""),
                                    done=_runner_progress.get("done", 0),
                                    total=_runner_progress.get("total", 0),
                                )
                            except Exception:
                                pass  # Never crash pipeline over progress persistence

                        # --- Resource usage measurement (T-BRIX-V7-07) ---
                        _rss_mb = _measure_rss_mb()
                        _resource_usage = {"rss_mb": _rss_mb, "duration": step_duration}
                        result["resource_usage"] = _resource_usage
                        _warn_if_high_memory(_rss_mb, step.id)

                        if result.get("success"):
                            context.set_output(step.id, result.get("data"))
                            last_output = result.get("data")
                            # --- Step-Level Cache: persist on success (T-BRIX-V6-24, legacy bool) ---
                            if step.cache is True:
                                from brix.context import CacheManager
                                CacheManager().set(step.id, rendered_params, result.get("data"))
                            # --- Resilience: Brick Cache persist on success (T-BRIX-DB-21) ---
                            if _brick_cache_instance is not None and _brick_cache_rendered_key is not None:
                                try:
                                    _brick_cache_instance.set(_brick_cache_rendered_key, result.get("data"))
                                except Exception as _bc_set_err:
                                    logger.warning("Brick cache set failed for '%s': %s", step.id, _bc_set_err)
                            # --- Resilience: Circuit Breaker reset on success (T-BRIX-DB-21) ---
                            if _cb_instance is not None:
                                try:
                                    _cb_instance.on_success()
                                except Exception:
                                    pass
                            # --- Resilience: Rate Limiter record on success (T-BRIX-DB-21) ---
                            if _rl_instance is not None:
                                try:
                                    _rl_instance.record_call()
                                except Exception:
                                    pass
                            # --- Saga: record compensatable step (T-BRIX-DB-21) ---
                            if step.compensate:
                                _saga_tracker.record(step.id, step.compensate)
                            # --- LLM cost extraction (T-BRIX-V6-21) ---
                            total_cost_usd += _extract_step_cost(result.get("data"))
                            step_statuses[step.id] = StepStatus(
                                status="ok",
                                duration=step_duration,
                                items=result.get("items_count"),
                                resource_usage=_resource_usage,
                            )
                            self.progress.step_ok(step.id, step_duration, result.get("items_count"))
                            # --- Execution Data persistence (T-BRIX-V7-04) ---
                            if self._should_persist(step):
                                self._persist_step_output(
                                    context.run_id, step, result, rendered_params, context,
                                    db=history._db,
                                )
                            # --- Step Execution Record (T-BRIX-DB-07) ---
                            _persist_data_flag = getattr(step, "persist_data", True)
                            _secret_vals = getattr(context, "_secret_values", set())
                            try:
                                history._db.record_step_execution(
                                    run_id=context.run_id,
                                    step_id=step.id,
                                    step_type=step.type,
                                    status="success",
                                    input_data=_redact_secret_values(rendered_params, _secret_vals) if _persist_data_flag else None,
                                    output_data=_redact_secret_values(result.get("data"), _secret_vals) if _persist_data_flag else None,
                                    data_source="",
                                    started_at=_step_started_at,
                                    ended_at=_step_ended_at,
                                    duration_ms=int(step_duration * 1000),
                                    persist_data=_persist_data_flag,
                                )
                            except Exception:
                                pass  # Never crash pipeline over persistence
                        else:
                            error_msg = result.get("error", "unknown error")
                            # --- Resilience: Circuit Breaker on failure (T-BRIX-DB-21) ---
                            if _cb_instance is not None:
                                try:
                                    _cb_instance.on_failure()
                                except Exception:
                                    pass
                            step_statuses[step.id] = StepStatus(
                                status="error", duration=step_duration, errors=1,
                                error_message=str(error_msg) if error_msg else None,
                                resource_usage=_resource_usage,
                            )
                            self.progress.step_error(step.id, error_msg, step_duration)
                            # --- Execution Data persistence on error (T-BRIX-V7-04) ---
                            if self._should_persist(step):
                                self._persist_step_output(
                                    context.run_id, step, result, rendered_params, context,
                                    db=history._db,
                                )
                            # --- Step Execution Record on error (T-BRIX-DB-07) ---
                            _persist_data_flag = getattr(step, "persist_data", True)
                            _secret_vals = getattr(context, "_secret_values", set())
                            try:
                                history._db.record_step_execution(
                                    run_id=context.run_id,
                                    step_id=step.id,
                                    step_type=step.type,
                                    status="error",
                                    input_data=_redact_secret_values(rendered_params, _secret_vals) if _persist_data_flag else None,
                                    output_data=None,
                                    error_detail={"error": str(error_msg)} if error_msg else None,
                                    data_source="",
                                    started_at=_step_started_at,
                                    ended_at=_step_ended_at,
                                    duration_ms=int(step_duration * 1000),
                                    persist_data=_persist_data_flag,
                                )
                            except Exception:
                                pass  # Never crash pipeline over persistence

                            effective_on_error = step.on_error or pipeline.error_handling.on_error
                            if effective_on_error == "stop":
                                # --- Saga: run compensations on pipeline abort (T-BRIX-DB-21) ---
                                try:
                                    await _saga_tracker.run_compensations(context, self, pipeline)
                                except Exception:
                                    pass
                                pipeline_aborted = True
                                break
                            # continue: log error and move on

            except Exception as e:
                # Unexpected exception (e.g. schema validation error, MCP crash) —
                # treat the run as failed but always reach the finally block.
                print(f"✗ Pipeline error: {e}", file=sys.stderr)
                pipeline_aborted = True

            finally:
                # Detach pool from runner (pool itself is closed by the async with block above).
                mcp_runner = self._runners.get("mcp")
                if mcp_runner is not None and hasattr(mcp_runner, "pool"):
                    mcp_runner.pool = None
                self._mcp_pool = None
                self._run_db = None  # Clear run-scoped DB reference (T-BRIX-DB-07)

                # Resolve output (best-effort; may be None if pipeline aborted early)
                if not pipeline_aborted and pipeline.output:
                    jinja_ctx = context.to_jinja_context()
                    final_result = self.loader.render_value(pipeline.output, jinja_ctx)
                elif not pipeline_aborted:
                    final_result = last_output

                total_duration = time.monotonic() - start_time
                if stop_step_success is not None:
                    # 'stop' step result overrides normal all_ok calculation
                    all_ok = stop_step_success
                else:
                    all_ok = (not pipeline_aborted) and all(
                        s.status in ("ok", "skipped", "dry_run") for s in step_statuses.values()
                    )

                # Detect cancellation: if the cancel sentinel exists treat the run as cancelled
                _was_cancelled = self._is_run_cancelled(context)
                if _was_cancelled:
                    context.save_run_metadata(pipeline.name, "cancelled")
                else:
                    context.save_run_metadata(pipeline.name, "completed" if all_ok else "failed")
                if all_ok and not _was_cancelled:
                    context.cleanup(keep=keep_workdir)

                self.progress.pipeline_done(pipeline.name, all_ok, total_duration, len(pipeline.steps))

                try:
                    # Build a compact steps summary — never include raw items arrays.
                    steps_summary: dict = {}
                    for k, v in step_statuses.items():
                        d = v.model_dump()
                        entry: dict = {
                            "status": d["status"],
                            "duration": d.get("duration"),
                            "items": d.get("items"),
                            "errors": d.get("errors"),
                        }
                        # Include error_message only when present to keep records compact
                        if d.get("error_message") is not None:
                            entry["error_message"] = d["error_message"]
                        # Include resource_usage when present (T-BRIX-V7-07)
                        if d.get("resource_usage") is not None:
                            entry["resource_usage"] = d["resource_usage"]
                        steps_summary[k] = entry
                    if _was_cancelled:
                        # Read cancel reason from sentinel file for partial-results record
                        _cancel_reason = ""
                        try:
                            import json as _json
                            _sentinel_path = context.workdir / "cancel_requested.json"
                            _cancel_data = _json.loads(_sentinel_path.read_text())
                            _cancel_reason = _cancel_data.get("reason", "")
                        except Exception:
                            pass
                        history.cancel_run(
                            context.run_id,
                            reason=_cancel_reason,
                            cancelled_by="user",
                        )
                        # Also persist step data collected so far
                        try:
                            history.record_finish(
                                context.run_id, False, total_duration,
                                steps_summary,
                                final_result,
                                cost_usd=total_cost_usd if total_cost_usd > 0.0 else None,
                            )
                        except Exception:
                            pass
                    else:
                        history.record_finish(
                            context.run_id, all_ok, total_duration,
                            steps_summary,
                            final_result,
                            cost_usd=total_cost_usd if total_cost_usd > 0.0 else None,
                        )
                except Exception:
                    pass  # Never let history errors mask the real result

                # --- Application logging: run end (T-BRIX-V7-08) ---
                _outcome = "cancelled" if _was_cancelled else ("success" if all_ok else "failure")
                _end_msg = (
                    f"Run finished: pipeline={pipeline.name} run_id={context.run_id} "
                    f"outcome={_outcome} duration={total_duration:.2f}s"
                )
                _end_level = "INFO" if all_ok or _was_cancelled else "ERROR"
                if _end_level == "INFO":
                    logger.info(_end_msg)
                else:
                    logger.error(_end_msg)
                _db_log(_end_level, "engine", _end_msg)

                try:
                    from brix.triggers.state import TriggerState
                    trigger_state = TriggerState()
                    trigger_state.record_pipeline_completion(
                        pipeline.name,
                        context.run_id,
                        "success" if all_ok else "failure",
                        final_result,
                        input=user_input,
                    )
                except Exception:
                    pass  # Don't fail the pipeline because of trigger state

                try:
                    from brix.alerting import AlertManager
                    _run_result = RunResult(
                        success=all_ok,
                        run_id=context.run_id,
                        steps=step_statuses,
                        result=final_result,
                        duration=time.monotonic() - start_time,
                        deprecation_warnings=list(self._deprecation_warnings),
                    )
                    # Attach pipeline name so alerting rules can access it
                    _run_result_dict = _run_result.model_dump()
                    _run_result_dict["pipeline"] = pipeline.name
                    AlertManager().check_alerts(_run_result_dict)
                except Exception:
                    pass  # Never let alerting errors mask the real result

        # Expose sub-step outputs for callers that need to propagate them
        # (e.g. RepeatRunner merging sub-step outputs into the parent context).
        self._last_step_outputs = dict(context.step_outputs)

        return RunResult(
            success=all_ok,
            run_id=context.run_id,
            steps=step_statuses,
            result=final_result,
            duration=total_duration,
            deprecation_warnings=list(self._deprecation_warnings),
        )

    # ------------------------------------------------------------------
    # Execution Data persistence (T-BRIX-V7-04)
    # ------------------------------------------------------------------

    @staticmethod
    def _should_persist(step: Step) -> bool:
        """Return True when step output should be persisted to step_outputs table."""
        return step.persist_output or bool(os.environ.get("BRIX_DEBUG"))

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

    @staticmethod
    def _capture_environment() -> dict:
        """Capture a lightweight environment snapshot at run start (T-BRIX-V7-05).

        Returns a dict with:
        - python_version: sys.version_info tuple as string
        - installed_packages: list of "name==version" strings (top-level, sorted)
        - mcp_servers: list of server names from ~/.brix/servers.yaml
        """
        import sys as _sys
        snapshot: dict = {
            "python_version": f"{_sys.version_info.major}.{_sys.version_info.minor}.{_sys.version_info.micro}",
        }

        # Installed packages — use importlib.metadata (stdlib, no subprocess needed)
        try:
            from importlib.metadata import packages_distributions
            dists: list[str] = []
            try:
                import importlib.metadata as _imeta
                for dist in sorted(_imeta.distributions(), key=lambda d: (d.metadata.get("Name") or "").lower()):
                    name = dist.metadata.get("Name") or dist.name or ""
                    version = dist.metadata.get("Version") or ""
                    if name:
                        dists.append(f"{name}=={version}")
            except Exception:
                pass
            snapshot["installed_packages"] = dists[:200]  # Cap at 200 to keep JSON small
        except Exception:
            snapshot["installed_packages"] = []

        # MCP servers from servers.yaml
        try:
            import yaml as _yaml
            from pathlib import Path as _Path
            _servers_path = _Path.home() / ".brix" / "servers.yaml"
            if _servers_path.exists():
                raw = _yaml.safe_load(_servers_path.read_text()) or {}
                snapshot["mcp_servers"] = sorted(raw.get("servers", {}).keys())
            else:
                snapshot["mcp_servers"] = []
        except Exception:
            snapshot["mcp_servers"] = []

        return snapshot

    def _persist_step_output(
        self,
        run_id: str,
        step: Step,
        result: dict,
        rendered_params: dict,
        context: Any,
        db: Any = None,
    ) -> None:
        """Write step execution data to the step_outputs table (best-effort).

        Parameters
        ----------
        db:
            Optional BrixDB instance. When provided it is reused directly
            (avoids opening a second connection to a different DB path in
            tests). When omitted a fresh default BrixDB() is created.
        """
        try:
            if db is None:
                from brix.db import BrixDB
                db = BrixDB()
            # Merge mcp_trace into rendered_params when present (T-BRIX-V7-05)
            stored_params = rendered_params
            mcp_trace = result.get("mcp_trace")
            if mcp_trace is not None:
                stored_params = dict(rendered_params) if rendered_params else {}
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
            pass  # Never crash the pipeline over persistence failures

    # ------------------------------------------------------------------
    # Breakpoint helpers (T-BRIX-V7-06)
    # ------------------------------------------------------------------

    def _write_context_snapshot(self, context: Any) -> None:
        """Write the current Jinja2 context snapshot to workdir/context-snapshot.json.

        Written before each step so that brix__inspect_context can read it
        even while the run is paused at a breakpoint.  Non-fatal.
        """
        try:
            snapshot = self._context_snapshot(context)
            snapshot_path = context.workdir / "context-snapshot.json"
            snapshot_path.write_text(json.dumps(snapshot, default=str))
        except Exception:
            pass  # Never crash the pipeline over snapshot failures

    async def _wait_for_breakpoint_resume(self, context: Any, step_id: str) -> None:
        """Write breakpoint.json and poll until it is deleted (resume signal).

        The engine pauses by writing ``workdir/breakpoint.json`` and then
        polls every 2 seconds until the file no longer exists.  The
        ``brix__resume_run`` MCP tool deletes the sentinel to resume.

        The breakpoint is automatically cleared if the run is cancelled.
        """
        breakpoint_path = context.workdir / "breakpoint.json"
        try:
            breakpoint_path.write_text(
                json.dumps({"step_id": step_id, "paused_at": time.monotonic()})
            )
        except OSError:
            return  # Cannot write sentinel — skip breakpoint gracefully

        # Update run metadata so polling tools can see the paused state
        try:
            context.save_run_metadata("(paused)", "paused")
        except Exception:
            pass

        while breakpoint_path.exists():
            # Check for cancellation so a breakpoint never blocks a cancel
            if self._is_run_cancelled(context):
                break
            await asyncio.sleep(2.0)

    # ------------------------------------------------------------------
    # per-step dependency helper (T-BRIX-V6-03)
    # ------------------------------------------------------------------

    def _ensure_step_requirements(self, step: Step) -> "str | None":
        """Check and auto-install per-step requirements.

        Returns an error message string if installation fails, or ``None``
        if all requirements are satisfied (or successfully installed).
        """
        if not step.requirements:
            return None

        from brix.deps import check_requirements, install_requirements

        missing = check_requirements(step.requirements)
        if not missing:
            return None

        print(
            f"⚙ Step '{step.id}': installing {len(missing)} package(s): {', '.join(missing)}",
            file=sys.stderr,
        )
        ok = install_requirements(missing)
        if not ok:
            return (
                f"Failed to install step packages for '{step.id}': {', '.join(missing)}"
            )
        return None

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

        # Resolve retry profile: step-level profile name takes precedence over
        # pipeline-level error_handling.retry config.
        profile: RetryProfile | None = None
        profile_name = getattr(step, "retry_profile", None)
        if profile_name:
            profile = pipeline.retry_profiles.get(profile_name)
            # Unknown profile name — surface as error so misconfigs are visible
            if profile is None:
                return {
                    "success": False,
                    "error": f"retry_profile '{profile_name}' not found in pipeline.retry_profiles",
                    "duration": 0.0,
                }

        # Determine max_attempts and backoff from profile (if resolved) or
        # pipeline-level retry config, falling back to RetryConfig defaults.
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

                # Check retriable_status_codes: if the profile defines a non-empty
                # list, only retry when the HTTP status code is in that list.
                if retriable_codes:
                    status_code = result.get("status_code")
                    if status_code is not None and status_code not in retriable_codes:
                        # Non-retriable status code — stop immediately
                        last_result["retry_count"] = attempt
                        return last_result

                # Rate-limited: honour Retry-After header before next attempt
                if result.get("rate_limited") and result.get("retry_after"):
                    await asyncio.sleep(result["retry_after"])
                    continue
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
    # batch_size helper
    # ------------------------------------------------------------------

    def _chunk_items(self, items: list, batch_size: int) -> list[list]:
        """Split items into chunks of batch_size. Returns [items] if batch_size <= 0."""
        if batch_size <= 0:
            return [items]
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

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
        """Run foreach items one by one in order."""
        _fe_jinja = context.to_jinja_context() if "{{" in step.type else None
        runner = self._resolve_runner(step.type, jinja_ctx=_fe_jinja)
        results: list[tuple[Any, dict]] = []
        foreach_start = time.monotonic()

        # Load checkpoint for resume — skip already-completed items
        completed = context.load_foreach_checkpoint(step.id) if context._resume_from else {}

        for i, item in enumerate(items):
            # Cancel-flag check: abort foreach cleanly if cancel was requested (T-BRIX-V6-BUG-03)
            if self._is_run_cancelled(context):
                break

            if i in completed:
                # Already completed in a previous run — restore and skip
                results.append((item, completed[i]))
                self.progress.step_resumed(f"{step.id}[{i}]")
                continue

            jinja_ctx = context.to_jinja_context(item=item)
            rendered_params = self.loader.render_step_params(step, jinja_ctx)
            rendered_step = _RenderedStep(step, rendered_params, self.loader, jinja_ctx)
            _item_start = time.monotonic()
            result = await self._execute_with_retry(runner, rendered_step, context, step, pipeline)
            _item_duration_ms = int((time.monotonic() - _item_start) * 1000)
            results.append((item, result))

            # Persist checkpoint so a crash can resume from here
            context.write_foreach_checkpoint(step.id, i, item, result)

            # Record foreach item execution (T-BRIX-DB-07)
            if self._run_db is not None:
                try:
                    self._run_db.record_foreach_item(
                        run_id=context.run_id,
                        step_id=step.id,
                        item_index=i,
                        item_input=item,
                        item_output=result.get("data"),
                        status="success" if result.get("success") else "error",
                        error_detail={"error": result.get("error")} if result.get("error") else None,
                        duration_ms=_item_duration_ms,
                    )
                except Exception:
                    pass  # Never crash pipeline over persistence

            # Report progress after each item
            failed_count = sum(1 for _, r in results if not r.get("success"))
            current_count = len(results)
            total_items = len(items)
            self.progress.foreach_progress(step.id, current_count, total_items, failed_count)
            # Auto-progress: store foreach progress in context so get_run_status can report it
            _pct = round(current_count / total_items * 100, 1) if total_items > 0 else 0.0
            _eta: float | None = None
            if current_count > 0 and total_items > current_count:
                _elapsed = time.monotonic() - foreach_start
                _avg_per_item = _elapsed / current_count
                _eta = round(_avg_per_item * (total_items - current_count), 1)
            context.update_step_progress(step.id, {
                "processed": current_count,
                "total": total_items,
                "percent": _pct,
                "eta_seconds": _eta,
                "message": f"foreach {current_count}/{total_items} ({failed_count} failed)",
            })
            context.save_run_metadata(pipeline.name, "running", progress={
                "step": step.id,
                "current": current_count,
                "total": total_items,
                "failed": failed_count,
            })

        return self._build_foreach_result(results, step, pipeline)

    async def _run_foreach_parallel(
        self, step: Step, items: list, context: PipelineContext, pipeline: Pipeline
    ) -> dict:
        """Run foreach items concurrently, respecting the concurrency limit."""
        _fp_jinja = context.to_jinja_context() if "{{" in step.type else None
        runner = self._resolve_runner(step.type, jinja_ctx=_fp_jinja)
        semaphore = asyncio.Semaphore(step.concurrency)
        foreach_start = time.monotonic()

        # Load checkpoint for resume — skip already-completed items
        completed = context.load_foreach_checkpoint(step.id) if context._resume_from else {}
        # Lock to ensure thread-safe JSONL appends and progress updates from concurrent coroutines
        checkpoint_lock = asyncio.Lock()
        completed_count = 0
        failed_count = 0
        total_items = len(items)

        async def run_item(idx: int, item: Any) -> tuple[Any, dict]:
            nonlocal completed_count, failed_count
            if idx in completed:
                # Already completed in a previous run — restore without executing
                self.progress.step_resumed(f"{step.id}[{idx}]")
                return item, completed[idx]

            async with semaphore:
                jinja_ctx = context.to_jinja_context(item=item)
                rendered_params = self.loader.render_step_params(step, jinja_ctx)
                rendered_step = _RenderedStep(step, rendered_params, self.loader, jinja_ctx)
                _item_start_p = time.monotonic()
                result = await self._execute_with_retry(runner, rendered_step, context, step, pipeline)
                _item_duration_ms_p = int((time.monotonic() - _item_start_p) * 1000)

                # Record foreach item execution (T-BRIX-DB-07)
                if self._run_db is not None:
                    try:
                        self._run_db.record_foreach_item(
                            run_id=context.run_id,
                            step_id=step.id,
                            item_index=idx,
                            item_input=item,
                            item_output=result.get("data"),
                            status="success" if result.get("success") else "error",
                            error_detail={"error": result.get("error")} if result.get("error") else None,
                            duration_ms=_item_duration_ms_p,
                        )
                    except Exception:
                        pass  # Never crash pipeline over persistence

                # Persist checkpoint and report progress (serialised via lock)
                async with checkpoint_lock:
                    context.write_foreach_checkpoint(step.id, idx, item, result)
                    completed_count += 1
                    if not result.get("success"):
                        failed_count += 1
                    self.progress.foreach_progress(step.id, completed_count, total_items, failed_count)
                    # Auto-progress: store in context for get_run_status
                    _pct = round(completed_count / total_items * 100, 1) if total_items > 0 else 0.0
                    _eta: float | None = None
                    if completed_count > 0 and total_items > completed_count:
                        _elapsed = time.monotonic() - foreach_start
                        _avg_per_item = _elapsed / completed_count
                        _eta = round(_avg_per_item * (total_items - completed_count), 1)
                    context.update_step_progress(step.id, {
                        "processed": completed_count,
                        "total": total_items,
                        "percent": _pct,
                        "eta_seconds": _eta,
                        "message": f"foreach {completed_count}/{total_items} ({failed_count} failed)",
                    })
                    context.save_run_metadata(pipeline.name, "running", progress={
                        "step": step.id,
                        "current": completed_count,
                        "total": total_items,
                        "failed": failed_count,
                    })

                return item, result

        tasks = [run_item(i, item) for i, item in enumerate(items)]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Normalise: exceptions from gather itself become failure entries
        processed: list[tuple[Any, dict]] = []
        for idx, r in enumerate(raw_results):
            if isinstance(r, Exception):
                processed.append((items[idx], {"success": False, "error": str(r), "duration": 0.0}))
            else:
                processed.append(r)  # type: ignore[arg-type]

        return self._build_foreach_result(processed, step, pipeline)

    # ------------------------------------------------------------------
    # DAG execution helpers (T-BRIX-V6-19)
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_dag_mode(steps: list[Step]) -> bool:
        """Return True if any step declares depends_on."""
        return any(bool(s.depends_on) for s in steps)

    @staticmethod
    def _toposort_steps(steps: list[Step]) -> list[Step]:
        """Return steps in topological order (Kahn's algorithm).

        Raises ``ValueError`` if a dependency references an unknown step ID or
        if the dependency graph contains a cycle.
        """
        step_by_id: dict[str, Step] = {s.id: s for s in steps}

        # Validate that all depends_on references are valid step IDs
        for step in steps:
            for dep in step.depends_on:
                if dep not in step_by_id:
                    raise ValueError(
                        f"Step '{step.id}' depends_on unknown step '{dep}'"
                    )

        # Build in-degree map and adjacency list
        in_degree: dict[str, int] = {s.id: 0 for s in steps}
        dependents: dict[str, list[str]] = {s.id: [] for s in steps}
        for step in steps:
            for dep in step.depends_on:
                in_degree[step.id] += 1
                dependents[dep].append(step.id)

        # Kahn's algorithm
        from collections import deque
        queue: deque[str] = deque(sid for sid, deg in in_degree.items() if deg == 0)
        sorted_ids: list[str] = []

        while queue:
            sid = queue.popleft()
            sorted_ids.append(sid)
            for dependent in dependents[sid]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(sorted_ids) != len(steps):
            # Some steps were not reached — cycle detected
            cycled = [sid for sid, deg in in_degree.items() if deg > 0]
            raise ValueError(
                f"Cycle detected in depends_on graph involving step(s): {', '.join(sorted(cycled))}"
            )

        return [step_by_id[sid] for sid in sorted_ids]

    async def _run_dag(
        self,
        pipeline: Pipeline,
        context: "PipelineContext",
        step_statuses: dict,
        dry_run_steps: "list[str] | None",
    ) -> tuple[bool, Any, bool, "bool | None"]:
        """Execute pipeline steps in DAG order.

        Steps without unsatisfied dependencies are dispatched concurrently.
        Each step waits until all its dependencies have completed successfully.

        Returns ``(pipeline_aborted, last_output, aborted_flag, stop_step_success)``.
        """
        steps = pipeline.steps
        step_by_id: dict[str, Step] = {s.id: s for s in steps}

        # Topological sort validates references and detects cycles
        try:
            _toposorted = self._toposort_steps(steps)
        except ValueError as exc:
            raise exc  # propagate to caller's try/except

        last_output: Any = None
        pipeline_aborted = False
        stop_step_success: "bool | None" = None

        # Asyncio events: set when a step is done (success *or* skip/error+continue)
        done_events: dict[str, asyncio.Event] = {s.id: asyncio.Event() for s in steps}
        # Outcome: True = "usable for downstream deps", False = "aborted"
        step_ok: dict[str, bool] = {}

        async def run_step(step: Step) -> None:
            nonlocal last_output, pipeline_aborted, stop_step_success

            # Wait for all dependencies to complete
            for dep_id in step.depends_on:
                await done_events[dep_id].wait()
                if not step_ok.get(dep_id, False):
                    # A dependency failed and pipeline is aborting — skip this step
                    step_statuses[step.id] = StepStatus(
                        status="skipped",
                        duration=0.0,
                        reason=f"dependency '{dep_id}' failed",
                    )
                    self.progress.step_skipped(step.id)
                    step_ok[step.id] = False
                    done_events[step.id].set()
                    return

            # --- Resume: skip completed steps ---
            if context.is_step_completed(step.id):
                step_statuses[step.id] = StepStatus(status="ok", duration=0.0)
                last_output = context.get_output(step.id)
                self.progress.step_resumed(step.id)
                step_ok[step.id] = True
                done_events[step.id].set()
                return

            # --- Disabled steps ---
            if not step.enabled:
                step_statuses[step.id] = StepStatus(
                    status="skipped", duration=0.0, reason="disabled"
                )
                self.progress.step_skipped(step.id)
                step_ok[step.id] = True  # disabled is treated as "ok for downstream"
                done_events[step.id].set()
                return

            # --- Selective dry-run ---
            if dry_run_steps and step.id in dry_run_steps:
                step_statuses[step.id] = StepStatus(
                    status="dry_run", duration=0.0, reason="dry_run_steps"
                )
                self.progress.step_skipped(step.id)
                step_ok[step.id] = True
                done_events[step.id].set()
                return

            # --- Evaluate when condition ---
            jinja_ctx = context.to_jinja_context()
            if step.when:
                should_run = self.loader.evaluate_condition(step.when, jinja_ctx)
                if not should_run:
                    step_statuses[step.id] = StepStatus(
                        status="skipped", duration=0.0, reason="condition not met"
                    )
                    self.progress.step_skipped(step.id)
                    step_ok[step.id] = True
                    done_events[step.id].set()
                    return

            # --- else_of ---
            if step.else_of:
                ref_status = step_statuses.get(step.else_of)
                if ref_status is None or ref_status.status != "skipped":
                    step_statuses[step.id] = StepStatus(
                        status="skipped",
                        duration=0.0,
                        reason=f"else_of '{step.else_of}' was not skipped",
                    )
                    self.progress.step_skipped(step.id)
                    step_ok[step.id] = True
                    done_events[step.id].set()
                    return

            # --- stop step ---
            if step.type == "stop":
                jinja_ctx = context.to_jinja_context()
                msg = step.message or "Pipeline stopped"
                rendered_msg = self.loader.render_template(msg, jinja_ctx) if "{{" in msg else msg
                step_statuses[step.id] = StepStatus(
                    status="ok", duration=0.0, reason=rendered_msg
                )
                self.progress.step_ok(step.id, 0.0)
                pipeline_aborted = True
                stop_step_success = getattr(step, "success_on_stop", True)
                # Signal all waiting steps so they can bail
                for ev in done_events.values():
                    ev.set()
                step_ok[step.id] = False  # prevent downstream from running
                return

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
                    self.progress.step_start(step.id, step.type)
                    self.progress.step_error(step.id, _cm_msg)
                    effective_on_error = step.on_error or pipeline.error_handling.on_error
                    if effective_on_error == "stop":
                        pipeline_aborted = True
                        for ev in done_events.values():
                            ev.set()
                    step_ok[step.id] = False
                    done_events[step.id].set()
                    return

            # --- Profile / Mixin (T-BRIX-DB-23) ---
            step = self._apply_profile(step)

            # --- Brick config_defaults merge (T-BRIX-IMP-02) ---
            step = self._apply_brick_defaults(step)

            # Build an early jinja context for dynamic dispatch type rendering
            _early_jinja_ctx_dag = context.to_jinja_context() if "{{" in step.type else None

            # --- Get runner ---
            runner = self._resolve_runner(step.type, jinja_ctx=_early_jinja_ctx_dag)
            if not runner:
                _no_runner_msg = f"no runner registered for type '{step.type}'"
                step_statuses[step.id] = StepStatus(
                    status="error", duration=0.0, errors=1,
                    error_message=_no_runner_msg,
                )
                self.progress.step_start(step.id, step.type)
                self.progress.step_error(step.id, _no_runner_msg)
                effective_on_error = step.on_error or pipeline.error_handling.on_error
                if effective_on_error == "stop":
                    pipeline_aborted = True
                    for ev in done_events.values():
                        ev.set()
                step_ok[step.id] = False
                done_events[step.id].set()
                return

            # --- per-step dependency check ---
            if step.requirements:
                dep_err = self._ensure_step_requirements(step)
                if dep_err:
                    step_statuses[step.id] = StepStatus(
                        status="error", duration=0.0, errors=1,
                        error_message=dep_err,
                    )
                    self.progress.step_start(step.id, step.type)
                    self.progress.step_error(step.id, dep_err)
                    effective_on_error = step.on_error or pipeline.error_handling.on_error
                    if effective_on_error == "stop":
                        pipeline_aborted = True
                        for ev in done_events.values():
                            ev.set()
                    step_ok[step.id] = False
                    done_events[step.id].set()
                    return

            # --- Single-step execution ---
            jinja_ctx = context.to_jinja_context()
            rendered_params = self.loader.render_step_params(step, jinja_ctx)
            rendered_step = _RenderedStep(step, rendered_params, self.loader, jinja_ctx)

            self.progress.step_start(step.id, step.type)
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
                self.progress.step_ok(step.id, step_duration, result.get("items_count"))
                step_ok[step.id] = True
            else:
                error_msg = result.get("error", "unknown error")
                step_statuses[step.id] = StepStatus(
                    status="error", duration=step_duration, errors=1,
                    error_message=str(error_msg) if error_msg else None,
                )
                self.progress.step_error(step.id, error_msg, step_duration)
                effective_on_error = step.on_error or pipeline.error_handling.on_error
                if effective_on_error == "stop":
                    pipeline_aborted = True
                    for ev in done_events.values():
                        ev.set()
                step_ok[step.id] = False

            done_events[step.id].set()

        # Dispatch all steps as concurrent tasks; each waits on its deps via events
        tasks = [asyncio.create_task(run_step(s)) for s in steps]
        await asyncio.gather(*tasks, return_exceptions=True)

        return pipeline_aborted, last_output, pipeline_aborted, stop_step_success

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
        foreach_result = {
            "items": items,
            "summary": {"total": total, "succeeded": succeeded, "failed": failed},
            "success": failed == 0 or effective_on_error == "continue",
            "duration": total_duration,
        }

        if getattr(step, "flat_output", False):
            # Flat mode: replace items with a plain list of data values (successes only)
            flat = [item["data"] for item in foreach_result["items"] if item.get("success")]
            foreach_result["items"] = flat

        return foreach_result


def _redact_secret_values(data: Any, secret_values: set) -> Any:
    """Replace all secret variable plaintext occurrences with '***REDACTED***'.

    Serializes data to JSON string, performs string replacements, then deserializes.
    Returns original data unchanged if secret_values is empty or data cannot be
    serialized (best-effort, never raises).
    """
    if not secret_values or data is None:
        return data
    try:
        json_str = json.dumps(data)
        for secret in secret_values:
            if secret and secret in json_str:
                json_str = json_str.replace(secret, "***REDACTED***")
        return json.loads(json_str)
    except Exception:
        return data


def _extract_step_cost(data: Any) -> float:
    """Extract LLM cost in USD from a step output dict.

    Helpers that use LLMs may include an ``llm_usage`` key in their output:

        {"llm_usage": {"input_tokens": N, "output_tokens": N, "model": "mistral-large"}}

    Pricing table (per 1M tokens, in USD) is a best-effort estimate.
    Returns 0.0 if no llm_usage key is found or the data is not a dict.
    """
    if not isinstance(data, dict):
        return 0.0
    usage = data.get("llm_usage")
    if not isinstance(usage, dict):
        return 0.0

    input_tokens: int = int(usage.get("input_tokens") or 0)
    output_tokens: int = int(usage.get("output_tokens") or 0)
    model: str = str(usage.get("model") or "").lower()

    # Pricing per 1M tokens (input, output) in USD — approximate public rates
    _PRICING: dict[str, tuple[float, float]] = {
        "mistral-large": (4.0, 12.0),
        "mistral-medium": (2.7, 8.1),
        "mistral-small": (1.0, 3.0),
        "mistral-tiny": (0.25, 0.25),
        "gpt-4o": (5.0, 15.0),
        "gpt-4o-mini": (0.15, 0.6),
        "gpt-4-turbo": (10.0, 30.0),
        "gpt-3.5-turbo": (0.5, 1.5),
        "claude-3-opus": (15.0, 75.0),
        "claude-3-sonnet": (3.0, 15.0),
        "claude-3-haiku": (0.25, 1.25),
        "claude-sonnet-4": (3.0, 15.0),
        "claude-opus-4": (15.0, 75.0),
        "gemini-1.5-pro": (3.5, 10.5),
        "gemini-1.5-flash": (0.35, 1.05),
    }

    # Find matching price (prefix match so "mistral-large-latest" still resolves)
    price_in, price_out = 0.0, 0.0
    for key, (p_in, p_out) in _PRICING.items():
        if model.startswith(key) or key in model:
            price_in, price_out = p_in, p_out
            break

    cost = (input_tokens / 1_000_000) * price_in + (output_tokens / 1_000_000) * price_out
    return cost


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
        self.body = rendered["_body"] if "_body" in rendered else step.body
        self.method = step.method
        self.script = step.script
        self.server = step.server
        self.tool = step.tool
        self.pipeline = rendered.get("_pipeline") or step.pipeline
        self.params = rendered if rendered else (step.params or {})
        # set runner: rendered values under _values key, fall back to raw values field
        self.values = rendered.get("_values") or getattr(step, "values", None) or {}
        # set runner: persist flag (T-BRIX-DB-13)
        self.persist = getattr(step, "persist", False)
        # stop runner fields
        self.message = getattr(step, "message", None)
        self.success_on_stop = getattr(step, "success_on_stop", True)
        # choose runner fields (T-BRIX-V4-05)
        self.choices = getattr(step, "choices", None)
        self.default_steps = getattr(step, "default_steps", None)
        # parallel step runner fields (T-BRIX-V4-06)
        self.sub_steps = getattr(step, "sub_steps", None)
        # repeat runner fields (T-BRIX-V4-07)
        self.sequence = getattr(step, "sequence", None)
        self.until = getattr(step, "until", None)
        self.while_condition = getattr(step, "while_condition", None)
        self.max_iterations = getattr(step, "max_iterations", 100)
        # notify runner fields (T-BRIX-V4-11)
        self.channel = getattr(step, "channel", None)
        self.to = getattr(step, "to", None)
        # approval runner fields (T-BRIX-V4-12)
        self.approval_timeout = getattr(step, "approval_timeout", "24h")
        self.on_timeout = getattr(step, "on_timeout", "stop")
        # intra-step progress (T-BRIX-V4-BUG-05)
        self.progress = getattr(step, "progress", False)
        # pipeline_group runner fields (T-BRIX-V6-17)
        self.pipelines = getattr(step, "pipelines", None)
        self.shared_params = getattr(step, "shared_params", {}) or {}
        # concurrency is already set on Step; expose it here for pipeline_group runner
        self.concurrency = getattr(step, "concurrency", 3)
