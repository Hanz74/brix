# `src/brix/engine.py` Refactoring Analysis — Consolidated

File analyzed: `src/brix/engine.py`
Analyzed line count: `2697`
Sources: Codex deep analysis + Explore-Agent structural analysis
Consolidated: 2026-04-06

This document is intentionally exhaustive. It merges two independent analyses of `engine.py` — a Codex line-by-line analysis and an Explore-Agent structural analysis — into a single authoritative reference. Where the two analyses agree, that agreement is noted. Where they differ or offer complementary views, both perspectives are preserved.

---

## 1. Complete Method Inventory

### Top-level constants and module state

- `_SPECIALIST_STEP_TYPES` (`41`)
  Purpose: Internal allowlist used by `_step_config_dict()` to avoid feeding `config` into specialist-style runners when specialist steps already own their own config semantics.
  Path use: both.

- `_VALIDATE_CONFIG_TOP_LEVEL_FIELDS` (`46-50`)
  Purpose: Derives the set of `Step` model fields that should be merged into the runner validation payload. This is a compatibility shim between step model fields and `runner.validate_config()`.
  Public/internal: Internal by naming, but imported by tests and effectively part of the compatibility surface.

- `LEGACY_ALIASES` (`101-143`)
  Purpose: Maps legacy flat step names to new dot-notation brick names. Used during runner resolution and also imported by validator and MCP handlers.
  Public/internal: Public in practice. Imported by `brix.validator`, `mcp_handlers/pipelines.py`, `mcp_handlers/steps.py`.

- `logger` (`181`)
  Purpose: Module logger configured through `_build_logger()`.
  Path use: both.

### `_step_config_dict` (`53-65`)

- Parameters: `step: Any`
- Return type: `dict[str, Any]`
- Purpose: Returns the config payload a non-specialist runner should see. It prefers `step.params`, falls back to `step.config` for non-specialist steps, and otherwise returns `{}`.
- Calls: `getattr`, `isinstance`, `dict`
- Called by: `_RenderedStep.__init__`, `PipelineEngine.run` during `validate_config` payload construction
- Path use: both

### `_extract_brick_default_values` (`68-92`)

- Parameters: `raw_schema: Any`
- Return type: `dict[str, Any]`
- Purpose: Normalizes brick `config_schema` payloads into runtime default values. It handles both flat dict defaults and schema-style dict entries containing `default`.
- Calls: `isinstance`, `getattr`
- Called by: `PipelineEngine._apply_brick_defaults`
- Path use: both

### `_JsonFormatter` (`156-167`)

- Purpose: Logging formatter that serializes each log record as one JSON object. Used only for stderr logging.
- Called by: `_build_logger`
- Path use: both

#### `_JsonFormatter.format` (`159-167`)

- Parameters: `self`, `record: logging.LogRecord`
- Return type: `str`
- Purpose: Produces the JSON log line with timestamp, level, component, and message. It is the only formatter used by the module logger.
- Calls: `json.dumps`, `self.formatTime`, `record.getMessage`
- Called by: Python logging machinery through `logger`
- Path use: both

### `_build_logger` (`170-178`)

- Parameters: `name: str`
- Return type: `logging.Logger`
- Purpose: Creates or reuses a named logger and attaches `_JsonFormatter` once. Ensures the logger writes JSON to stderr and does not propagate.
- Calls: `logging.getLogger`, `logging.StreamHandler`, `_JsonFormatter`, `logger.addHandler`, `logger.setLevel`
- Called by: module initialization
- Path use: both

### `_db_log` (`184-190`)

- Parameters: `level: str`, `component: str`, `message: str`
- Return type: `None`
- Purpose: Best-effort app log persistence into `brix.db`. It intentionally swallows all failures so logging never breaks execution.
- Calls: lazy import `brix.db.BrixDB`, `BrixDB().write_app_log`
- Called by: `PipelineEngine.run`
- Path use: both

### `_measure_rss_mb` (`197-220`)

- Parameters: none
- Return type: `float`
- Purpose: Reads current process RSS in MB. It prefers `/proc/self/status`, then falls back to `resource.getrusage`.
- Calls: `open`, `resource.getrusage`, `os.uname`, `round`
- Called by: `PipelineEngine.run`
- Imported by tests: `tests/test_v7_07_resource_monitoring.py`
- Path use: sequential only

### `_total_ram_mb` (`223-233`)

- Parameters: none
- Return type: `float`
- Purpose: Reads system total RAM from `/proc/meminfo`. It returns `0.0` on any failure.
- Calls: `open`
- Called by: `_warn_if_high_memory`
- Imported by tests: `tests/test_v7_07_resource_monitoring.py`
- Path use: sequential only through `_warn_if_high_memory`

### `_warn_if_high_memory` (`236-249`)

- Parameters: `rss_mb: float`, `step_id: str`
- Return type: `None`
- Purpose: Emits a warning when the process is above 80 percent of total RAM. It is advisory only.
- Calls: `_total_ram_mb`, `print`, `logger.warning`, `round`
- Called by: `PipelineEngine.run`
- Imported by tests: `tests/test_v7_07_resource_monitoring.py`
- Path use: sequential only

### `PipelineEngine` (`252-2569`)

- Purpose: Main execution façade for pipeline execution. It owns runner registration, context setup, history integration, sequential execution, DAG execution, foreach execution, retry logic, persistence hooks, and run finalization.
- External callers: CLI, API, trigger runners, MCP handlers, testing helpers, debug tools, many test files
- Path use: both

**Explore-Agent metric:** 25 methods on `PipelineEngine` + 14 standalone functions/classes at module level.

#### `PipelineEngine.__init__` (`255-318`)

- Parameters: `self`
- Return type: `None`
- Purpose: Instantiates loader, progress reporter, built-in runners, discovered runners, brick registry, and all per-run mutable engine state. It also wires engine-aware runners back to the engine instance.
- Calls: `PipelineLoader`, `ProgressReporter`, `PipelineRunner.set_engine`, `ChooseRunner.set_engine`, `ParallelStepRunner.set_engine`, `RepeatRunner.set_engine`, `PipelineGroupRunner.set_engine`, `discover_runners`, lazy `BrickRegistry`, lazy `BrixDB`
- Called by: all engine consumers constructing `PipelineEngine`
- Path use: both

#### `PipelineEngine.register_runner` (`320-322`)

- Parameters: `self`, `step_type: str`, `runner: BaseRunner`
- Return type: `None`
- Purpose: Adds or replaces a runner in the registry. This is the engine's explicit extension hook.
- Calls: none besides dict assignment
- Called by: tests and potential runtime setup code
- Path use: both

#### `PipelineEngine._apply_profile` (`324-369`)

- Parameters: `self`, `step: Step`
- Return type: `Step`
- Purpose: Loads a named DB profile and fills selected step fields when the step still has defaults. It is fallback-only merging, never override-first.
- Calls: lazy `BrixDB.profile_get`, `step.model_dump`, `Step.model_validate`, `logger.warning`
- Called by: `run`, `_run_dag`
- Path use: both

#### `PipelineEngine._apply_brick_defaults` (`371-417`)

- Parameters: `self`, `step: Step`
- Return type: `Step`
- Purpose: Reads custom brick defaults from the brick definition and merges them into `step.params`. It only applies to dot-notation brick types.
- Calls: lazy `BrixDB.brick_definitions_get`, `_extract_brick_default_values`, `step.model_copy`, `logger.warning`
- Called by: `run`, `_run_dag`
- Path use: both

#### `PipelineEngine._resolve_runner` (`419-504`)

- Parameters: `self`, `step_type: str`, `jinja_ctx: dict | None = None`
- Return type: `BaseRunner | None`
- Purpose: Resolves a step type to a runner using dynamic dispatch, legacy alias mapping, brick registry lookup, then direct runner map lookup. It also enforces `strict_bricks` and accumulates deprecation warnings.
- Calls: `self.loader.render_template`, `self._brick_registry.get`, `LEGACY_ALIASES.get`, lazy `BrixDB.record_deprecated_usage`, `warnings.warn`, `logger.warning`
- Called by: `run`, `_run_foreach_sequential`, `_run_foreach_parallel`, `_run_dag`
- Called externally by tests: brick-first, dynamic dispatch, deprecation, smoke tests
- Path use: both

#### `PipelineEngine._resolve_step_credentials` (`506-534`)

- Parameters: `self`, `step`
- Return type: `dict[str, Any]`
- Purpose: Resolves per-step credentials into plaintext values using either env names or credential UUIDs. It mirrors `PipelineContext` credential resolution rules.
- Calls: `CredentialStore.resolve`, `is_credential_uuid`, `PipelineContext._refresh_credential`, `warnings.warn`
- Called by: `_step_credentials_context`
- Path use: both

#### `PipelineEngine._step_credentials_context` (`537-551`)

- Parameters: `self`, `context: PipelineContext`, `step`
- Return type: context manager yielding `None`
- Purpose: Temporarily overlays step credentials onto the pipeline context for a single execution scope. It resets the Jinja cache on enter and exit.
- Calls: `self._resolve_step_credentials`
- Called by: `run` foreach resolution, `run` single-step execution, `_run_dag` single-step execution
- Path use: both

#### `PipelineEngine.run` (`553-1583`)

- Parameters: `self`, `pipeline`, `user_input`, `keep_workdir`, `run_id`, `profile`, `mcp_pool`, `dry_run_steps`, `_inherit_input`
- Return type: `RunResult`
- Purpose: Complete top-level pipeline execution entrypoint. It sets up context/history, performs preflight work, dispatches either the sequential loop or DAG path, finalizes run history, emits logs, updates trigger/alert systems, and returns the final `RunResult`.
- Calls: nearly every helper in the module, `PipelineContext.from_pipeline`, `RunHistory`, `_capture_environment`, `_detect_dag_mode`, `_run_dag`, `_run_foreach_*`, `_execute_with_retry`, `_persist_step_output`, `_write_context_snapshot`, `_wait_for_breakpoint_resume`, `_ensure_step_requirements`, `_extract_step_cost`, `_redact_secret_values`, `_measure_rss_mb`, `_warn_if_high_memory`, many DB and resilience helpers
- Called by: CLI, API, MCP handlers, trigger runners, testing helpers, pipeline runners, many tests
- Path use: both

#### Local function `run._pool_ctx` (`631-636`)

- Parameters: none
- Return type: async context manager yielding `McpConnectionPool`
- Purpose: Encapsulates "reuse supplied pool vs open/close a fresh pool" behavior. This isolates MCP pool lifetime handling from the rest of `run()`.
- Calls: `McpConnectionPool`
- Called by: `run`
- Path use: both

#### `PipelineEngine._should_persist` (`1590-1592`)

- Parameters: `step: Step`
- Return type: `bool`
- Purpose: Determines whether step output should be written to the `step_outputs` table. It uses `step.persist_output` or global `BRIX_DEBUG`.
- Calls: `os.environ.get`
- Called by: `run`
- Imported by tests: `tests/test_v7_04_execution_data.py`
- Path use: sequential only

#### `PipelineEngine._context_snapshot` (`1595-1613`)

- Parameters: `context`
- Return type: `dict`
- Purpose: Produces a lightweight Jinja context snapshot showing key names and type summaries instead of raw values. Used for debug persistence and live inspection.
- Calls: `context.to_jinja_context`
- Called by: `_persist_step_output`, `_write_context_snapshot`
- Imported by tests: `tests/test_v7_04_execution_data.py`
- Path use: sequential only

#### Local function `_context_snapshot._type_name` (`1606-1611`)

- Parameters: `v: Any`
- Return type: `str`
- Purpose: Internal helper for human-readable type strings like `dict(3 keys)` and `list(8 items)`. It intentionally keeps snapshots small.
- Calls: `isinstance`, `len`, `type`
- Called by: `_context_snapshot`
- Path use: sequential only

#### `PipelineEngine._capture_environment` (`1616-1656`)

- Parameters: none
- Return type: `dict`
- Purpose: Captures environment metadata for run-diff diagnostics: Python version, installed packages, and MCP server names. It is best-effort and compact by design.
- Calls: `importlib.metadata.distributions`, lazy `ServerManager.list_all`
- Called by: `run`
- Imported by tests: `tests/test_v7_05_run_diff_mcp_trace_env.py`
- Path use: both

#### `PipelineEngine._persist_step_output` (`1658-1695`)

- Parameters: `self`, `run_id`, `step`, `result`, `rendered_params`, `context`, `db=None`
- Return type: `None`
- Purpose: Persists debug execution data into `step_outputs`, optionally merging `_mcp_trace` into stored params. It never raises.
- Calls: lazy `BrixDB`, `db.save_step_output`, `self._context_snapshot`
- Called by: `run`
- Imported by tests: `tests/test_v7_05_run_diff_mcp_trace_env.py`
- Path use: sequential only

#### `PipelineEngine._write_context_snapshot` (`1701-1712`)

- Parameters: `self`, `context`
- Return type: `None`
- Purpose: Writes `context-snapshot.json` into the run workdir before step execution. This supports live debugging and breakpoints.
- Calls: `self._context_snapshot`, `json_dumps`, `snapshot_path.write_text`
- Called by: `run`
- Imported by tests: `tests/test_v7_06_debug_tools.py`
- Path use: sequential only

#### `PipelineEngine._wait_for_breakpoint_resume` (`1714-1741`)

- Parameters: `self`, `context`, `step_id: str`
- Return type: `None`
- Purpose: Implements pause-before-step by writing `breakpoint.json` and polling until it disappears or cancellation is requested. It also updates run metadata to `paused`.
- Calls: `json_dumps`, `context.save_run_metadata`, `self._is_run_cancelled`, `asyncio.sleep`
- Called by: `run`, `_run_dag`
- Imported by tests: `tests/test_v7_06_debug_tools.py`
- Path use: both

#### `PipelineEngine._ensure_step_requirements` (`1747-1771`)

- Parameters: `self`, `step: Step`
- Return type: `str | None`
- Purpose: Performs per-step dependency auto-installation. On failure it returns a string error instead of raising.
- Calls: `brix.deps.check_requirements`, `brix.deps.install_requirements`, `print`
- Called by: `run` foreach branch, `run` single-step branch, `_run_dag`
- Imported by tests: `tests/test_self_healing.py`
- Path use: both

#### `PipelineEngine._execute_with_retry` (`1777-1850`)

- Parameters: `self`, `runner`, `rendered_step`, `context`, `step`, `pipeline`
- Return type: `dict`
- Purpose: Wraps `runner.execute()` with retry policy resolution, backoff handling, retry profile lookup, status-code filtering, and rate-limit honor behavior. Non-retry paths are normalized into result dicts.
- Calls: `runner.execute`, `RetryConfig`, `asyncio.sleep`
- Called by: `run`, `_run_foreach_sequential`, `_run_foreach_parallel`, `_run_dag`
- Imported indirectly through engine integration tests, retry-profile tests, many runner tests
- Path use: both

#### `PipelineEngine._chunk_items` (`1856-1860`)

- Parameters: `self`, `items: list`, `batch_size: int`
- Return type: `list[list]`
- Purpose: Splits foreach items into fixed-size batches. Used only by the sequential foreach path.
- Calls: slicing, `range`
- Called by: `run`
- Path use: sequential only

#### `PipelineEngine._is_run_cancelled` (`1866-1872`)

- Parameters: `self`, `context: PipelineContext`
- Return type: `bool`
- Purpose: Checks for `cancel_requested.json` in the run workdir. It is the engine's only cancellation primitive.
- Calls: `sentinel.exists`
- Called by: `run`, `_wait_for_breakpoint_resume`, `_run_foreach_sequential`
- Imported indirectly through cancel tests
- Path use: sequential only in current implementation

#### `PipelineEngine._run_foreach_sequential` (`1874-1950`)

- Parameters: `self`, `step`, `items`, `context`, `pipeline`
- Return type: `dict`
- Purpose: Executes foreach items one by one, with resume checkpoint restore, per-item execution persistence, progress updates, and checkpoint writes. It returns the aggregated foreach result.
- Calls: `self._resolve_runner`, `context.load_foreach_checkpoint`, `self._is_run_cancelled`, `context.to_jinja_context`, `self.loader.render_step_params`, `_RenderedStep`, `self._execute_with_retry`, `context.write_foreach_checkpoint`, `self._run_db.record_foreach_item`, `self.progress.foreach_progress`, `context.update_step_progress`, `context.save_run_metadata`, `self._build_foreach_result`
- Called by: `run`
- Path use: sequential only

#### `PipelineEngine._run_foreach_parallel` (`1952-2041`)

- Parameters: `self`, `step`, `items`, `context`, `pipeline`
- Return type: `dict`
- Purpose: Executes foreach items concurrently under a semaphore. It also serializes checkpoint/progress updates under a lock and normalizes gathered exceptions into result dicts.
- Calls: `self._resolve_runner`, `asyncio.Semaphore`, `context.load_foreach_checkpoint`, `asyncio.Lock`, local `run_item`, `asyncio.gather`, `self._build_foreach_result`
- Called by: `run`
- Path use: sequential only

#### Local function `_run_foreach_parallel.run_item` (`1969-2028`)

- Parameters: `idx: int`, `item: Any`
- Return type: `tuple[Any, dict]`
- Purpose: Per-item coroutine used by parallel foreach. It performs rendering, execution, DB item persistence, checkpoint writes, and progress counters under shared state.
- Calls: `context.to_jinja_context`, `self.loader.render_step_params`, `_RenderedStep`, `self._execute_with_retry`, `self._run_db.record_foreach_item`, `context.write_foreach_checkpoint`, `self.progress.foreach_progress`, `context.update_step_progress`, `context.save_run_metadata`
- Called by: `_run_foreach_parallel`
- Path use: sequential only

#### `PipelineEngine._detect_dag_mode` (`2048-2050`)

- Parameters: `steps: list[Step]`
- Return type: `bool`
- Purpose: Switches execution mode based on whether any step declares `depends_on`. This is the only gate between sequential and DAG execution.
- Calls: `any`, `bool`
- Called by: `run`
- Imported by tests: `tests/test_dag_execution.py`
- Path use: neither, it chooses the path

#### `PipelineEngine._toposort_steps` (`2053-2097`)

- Parameters: `steps: list[Step]`
- Return type: `list[Step]`
- Purpose: Validates DAG dependencies and computes a topological ordering using Kahn's algorithm. It rejects unknown dependency references and cycles.
- Calls: `collections.deque`, `ValueError`
- Called by: `_run_dag`
- Imported by tests: `tests/test_dag_execution.py`
- Path use: DAG only

#### `PipelineEngine._run_dag` (`2099-2528`)

- Parameters: `self`, `pipeline`, `context`, `step_statuses`, `dry_run_steps`
- Return type: `tuple[bool, Any, bool, bool | None]`
- Purpose: Executes steps as a dependency-driven concurrent DAG. Each step runs in its own task and coordinates with dependency-completion events.
- Calls: `self._toposort_steps`, local `run_step`, `asyncio.Event`, `asyncio.create_task`, `asyncio.gather`
- Called by: `run`
- Imported indirectly by DAG tests through `run`
- Path use: DAG only

#### Local function `_run_dag.run_step` (`2131-2522`)

- Parameters: `step: Step`
- Return type: `None`
- Purpose: Implements the actual DAG step lifecycle for one step: dependency waiting, gating, execution, caches, resilience, and status updates. It mirrors part of the sequential path, but not all of it.
- Calls: `done_events[dep].wait`, `context.is_step_completed`, `context.get_output`, `context.to_jinja_context`, `self.loader.evaluate_condition`, `self.loader.render_template`, `self._apply_profile`, `self._apply_brick_defaults`, `self._resolve_runner`, `self._ensure_step_requirements`, `self.loader.render_step_params`, `_RenderedStep`, pin/cache/circuit/rate-limit helpers, `self._wait_for_breakpoint_resume`, `self._step_credentials_context`, `self._execute_with_retry`, `self.progress.*`
- Called by: `_run_dag`
- Path use: DAG only

#### `PipelineEngine._build_foreach_result` (`2530-2569`)

- Parameters: `self`, `results`, `step`, `pipeline`
- Return type: `dict`
- Purpose: Aggregates per-item foreach results into the engine's standard foreach result payload. It also implements `flat_output` and stop-on-first-error semantics.
- Calls: `result.get`, `items.append`, `getattr`
- Called by: `_run_foreach_sequential`, `_run_foreach_parallel`
- Path use: sequential only

### `_redact_secret_values` (`2572-2588`)

- Parameters: `data: Any`, `secret_values: set`
- Return type: `Any`
- Purpose: Best-effort redaction of plaintext secret values before persistence by JSON-serializing, string-replacing, then deserializing. It preserves original data on failure.
- Calls: `sanitize_for_json`, `json_dumps`, `json.loads`
- Called by: `run`
- Imported by tests: `tests/test_secret_variables.py`
- Path use: sequential only

### `_extract_step_cost` (`2591-2638`)

- Parameters: `data: Any`
- Return type: `float`
- Purpose: Parses `llm_usage` metadata from step output and estimates cost in USD using a built-in pricing table. It is advisory accounting, not billing.
- Calls: `isinstance`, `int`, `str`, `_PRICING.items`, `model.startswith`
- Called by: `run`
- Imported by tests: `tests/test_v6_llm_cost_tracking.py`
- Path use: sequential only

### `_RenderedStep` (`2641-2697`)

- Purpose: Lightweight execution view over a `Step` with Jinja-rendered values applied. It is the object handed to runners.
- Called by: `run`, `_run_foreach_sequential`, `_run_foreach_parallel`, `_run_dag`, `brix.resilience`, `brix.debug_tools`, tests
- Path use: both

#### `_RenderedStep.__init__` (`2644-2697`)

- Parameters: `self`, `step: Step`, `rendered: dict`, `loader: PipelineLoader`, `jinja_ctx: dict`
- Return type: `None`
- Purpose: Copies the source step's execution-relevant attributes and overlays rendered values for command, args, body, config, pipeline, params, and runner-specific fields. It is compatibility glue for all runner interfaces.
- Calls: `loader.render_value`, `_step_config_dict`, `rendered.get`, `getattr`
- Called by: `run`, `_run_foreach_sequential`, `_run_foreach_parallel`, `_run_dag`, debug/resilience code
- Path use: both

---

## 2. Sequential Path — Complete Block-by-Block

Sequential execution lives inside `engine.py` L798 through L1431 when `_detect_dag_mode()` returns `False`.

**Explore-Agent metric:** 18 sequential execution blocks, approximately 620 lines.

### S1. DAG Gate

- Line range: `798-810`
- Reads: `pipeline.steps`
- Writes: `pipeline_aborted`, `last_output`, `stop_step_success`
- Error handling: catches `ValueError` only from `_run_dag` on the DAG branch; sequential branch itself is inside outer `try`
- Pseudocode:

```text
if detect_dag_mode(steps):
    run DAG path
    if DAG raises ValueError:
        print DAG error
        pipeline_aborted = True
else:
    enter sequential for step in pipeline.steps
```

### S2. Cancel Sentinel Check

- Line range: `812-815`
- Reads: `context.workdir` through `_is_run_cancelled(context)`
- Writes: `pipeline_aborted`
- Error handling: `_is_run_cancelled()` swallows filesystem errors and returns `False`
- Pseudocode:

```text
if is_run_cancelled(context):
    pipeline_aborted = True
    break
```

### S3. Resume Completed Step

- Line range: `817-823`
- Reads: `context.is_step_completed(step.id)`, `context.get_output(step.id)`
- Writes: `step_statuses[step.id]`, `last_output`
- Error handling: none locally
- Pseudocode:

```text
if context says step already completed:
    mark status ok duration 0
    restore last_output from context
    emit progress.step_resumed
    continue
```

### S4. Disabled-Step Skip

- Line range: `824-830`
- Reads: `step.enabled`
- Writes: `step_statuses[step.id]`
- Error handling: none locally
- Pseudocode:

```text
if not step.enabled:
    mark skipped(reason="disabled")
    progress.step_skipped
    continue
```

### S5. Selective Dry Run

- Line range: `832-839`
- Reads: `dry_run_steps`, `step.id`
- Writes: `step_statuses[step.id]`
- Error handling: none locally
- Pseudocode:

```text
if dry_run_steps and step.id in dry_run_steps:
    mark dry_run(reason="dry_run_steps")
    do not set context output
    progress.step_skipped
    continue
```

### S6. `when` Condition Gate

- Line range: `841-850`
- Reads: `context.to_jinja_context()`, `step.when`
- Writes: `step_statuses[step.id]` when skipped
- Error handling: any evaluation exception escapes to outer `except` and aborts run
- Pseudocode:

```text
jinja_ctx = context.to_jinja_context()
if step.when:
    should_run = loader.evaluate_condition(step.when, jinja_ctx)
    if not should_run:
        mark skipped(reason="condition not met")
        progress.step_skipped
        continue
```

### S7. `else_of` Gate

- Line range: `852-862`
- Reads: `step.else_of`, `step_statuses`
- Writes: `step_statuses[step.id]`
- Error handling: none locally
- Pseudocode:

```text
if step.else_of:
    ref_status = step_statuses.get(step.else_of)
    if ref missing or ref_status.status != "skipped":
        mark skipped(reason=f"else_of '{ref}' was not skipped")
        progress.step_skipped
        continue
```

### S8. `stop` Step Handling

- Line range: `864-897`
- Reads: `step.type`, `step.when`, `step.message`, `context.to_jinja_context()`
- Writes: `step_statuses[step.id]`, `pipeline_aborted`, `stop_step_success`
- Error handling: any template or condition exception escapes to outer `except`
- Pseudocode:

```text
if step.type == "stop":
    _should_stop = True
    if step.when is not None:
        if bool -> use directly
        elif non-empty string -> evaluate condition
        else -> False
    if not _should_stop:
        mark skipped(reason="condition not met")
        progress.step_skipped
        continue
    render stop message if needed
    mark ok(reason=rendered_msg)
    pipeline_aborted = True
    stop_step_success = step.success_on_stop defaulting True
    break
```

### S9. Compositor-Mode Guard

- Line range: `899-917`
- Reads: `pipeline.compositor_mode`, `pipeline.allow_code`, `step.type`, `step.on_error`, `pipeline.error_handling.on_error`
- Writes: `step_statuses[step.id]`, maybe `pipeline_aborted`
- Error handling: none locally
- Pseudocode:

```text
if compositor_mode and not allow_code and step.type in {"python","cli"}:
    build error message
    mark error
    progress.step_start + step_error
    if effective_on_error == "stop":
        pipeline_aborted = True
        break
    continue
```

### S10. Profile + Brick Default Merge

- Line range: `918-922`
- Reads: `step.profile`, brick registry / DB inside helpers
- Writes: local `step` variable rebound to updated `Step`
- Error handling: helpers swallow their own failures and return original step
- Pseudocode:

```text
step = _apply_profile(step)
step = _apply_brick_defaults(step)
```

### S11. Dynamic Type Render + Runner Resolution

- Line range: `924-941`
- Reads: `step.type`, `context.to_jinja_context()`
- Writes: `_early_jinja_ctx`, `runner`, `step_statuses[step.id]`, maybe `pipeline_aborted`
- Error handling: `_resolve_runner()` may raise `ValueError` for strict bricks; that escapes to outer `except`
- Pseudocode:

```text
early_jinja_ctx = context.to_jinja_context() if "{{" in step.type else None
runner = _resolve_runner(step.type, early_jinja_ctx)
if no runner:
    mark error("no runner registered ...")
    progress.step_start + step_error
    if effective_on_error == "stop":
        pipeline_aborted = True
        break
    continue
```

### S12. `validate_config` Guard

- Line range: `943-970`
- Reads: `_step_config_dict(step)`, `_VALIDATE_CONFIG_TOP_LEVEL_FIELDS`, step attributes, `runner.validate_config()`
- Writes: `_vc_config`, `step_statuses[step.id]`, maybe `pipeline_aborted`
- Error handling: validation errors are converted to `StepStatus(error)`; exceptions from `runner.validate_config()` escape to outer `except`
- Pseudocode:

```text
vc_config = _step_config_dict(step)
for each allowed top-level field:
    if step has non-None value:
        copy into vc_config
log vc_config
vc_errors = runner.validate_config(vc_config)
if vc_errors:
    mark error("Config validation failed ...")
    progress.step_start + step_error
    if effective_on_error == "stop":
        pipeline_aborted = True
        break
    continue
```

### S13. Foreach Preflight Dependency Check

- Line range: `972-989`
- Reads: `step.foreach`, `step.requirements`
- Writes: `step_statuses[step.id]`, maybe `pipeline_aborted`
- Error handling: `_ensure_step_requirements()` returns string instead of raising
- Pseudocode:

```text
if step.foreach:
    if step.requirements:
        dep_err = _ensure_step_requirements(step)
        if dep_err:
            mark error(dep_err)
            progress.step_start + step_error
            if effective_on_error == "stop":
                pipeline_aborted = True
                break
            continue
```

### S14. Foreach Resolution

- Line range: `990-1011`
- Reads: step credentials, `context.to_jinja_context()`, `step.foreach`
- Writes: `items`, `step_statuses[step.id]`, maybe `pipeline_aborted`
- Error handling: explicitly catches `ValueError` and `TypeError` from `loader.resolve_foreach()`
- Pseudocode:

```text
with step_credentials_context:
    jinja_ctx = context.to_jinja_context()
    try:
        items = loader.resolve_foreach(step.foreach, jinja_ctx)
    except ValueError | TypeError as err:
        mark error("foreach expression failed to resolve ...")
        progress.step_start + step_error
        if effective_on_error == "stop":
            pipeline_aborted = True
            break
        continue
```

### S15. Foreach Execution Mode Selection

- Line range: `1012-1054`
- Reads: `step.batch_size`, `step.parallel`, `step.on_error`, `pipeline.error_handling.on_error`
- Writes: `step_start`, `chunks`, `foreach_result`, batch accumulation locals
- Error handling: per-chunk failures can trigger `batch_aborted`; actual item errors are delegated to `_run_foreach_*`
- Pseudocode:

```text
step_start = now
if batch_size > 0:
    chunks = _chunk_items(items, batch_size)
    init aggregate counters
    for each chunk:
        progress.step_start("step[batch i/n]")
        chunk_result = run_foreach_parallel or sequential
        merge chunk items + counts
        if chunk_result unsuccessful and effective_on_error == "stop":
            batch_aborted = True
            break
    foreach_result = aggregate summary dict
elif step.parallel:
    foreach_result = _run_foreach_parallel(...)
else:
    foreach_result = _run_foreach_sequential(...)
step_duration = now - step_start
```

### S16. Foreach Post-Processing

- Line range: `1056-1110`
- Reads: `items`, `step.parallel`, `step.batch_size`, `step.concurrency`, `foreach_result`
- Writes: optional `foreach_result["hints"]`, `context.set_output`, `last_output`, `step_statuses[step.id]`, maybe `pipeline_aborted`
- Error handling: unsuccessful foreach result becomes `StepStatus(error)` and may abort on `stop`
- Pseudocode:

```text
build perf_hints based on item count / parallel / batch_size / concurrency
if hints exist:
    foreach_result["hints"] = perf_hints
if foreach_result["success"]:
    context.set_output(step.id, foreach_result)
    last_output = foreach_result
    mark ok with total items and failed count
    progress.foreach_done(...)
else:
    mark error("foreach failed ...")
    progress.step_start + step_error
    if effective_on_error == "stop":
        pipeline_aborted = True
        break
continue
```

### S17. Single-Step Dependency Check

- Line range: `1112-1126`
- Reads: `step.requirements`
- Writes: `step_statuses[step.id]`, maybe `pipeline_aborted`
- Error handling: `_ensure_step_requirements()` returns string instead of raising
- Pseudocode:

```text
if step.requirements:
    dep_err = _ensure_step_requirements(step)
    if dep_err:
        mark error(dep_err)
        progress.step_start + step_error
        if effective_on_error == "stop":
            pipeline_aborted = True
            break
        continue
```

### S18. Render Step Params + Build `_RenderedStep`

- Line range: `1128-1134`
- Reads: `context.to_jinja_context()`, `step`
- Writes: `jinja_ctx`, `rendered_params`, `rendered_step`
- Error handling: render exceptions escape to outer `except`
- Pseudocode:

```text
jinja_ctx = context.to_jinja_context()
rendered_params = loader.render_step_params(step, jinja_ctx)
rendered_step = _RenderedStep(step, rendered_params, loader, jinja_ctx)
```

### S19. Pin Mock Short-Circuit

- Line range: `1136-1156`
- Reads: `pipeline.name`, `step.id`
- Writes: `_pin_hit`, `context.set_output`, `last_output`, `step_statuses[step.id]`
- Error handling: DB lookup errors are logged and ignored
- Pseudocode:

```text
try:
    _pin_record = BrixDB().get_pin(pipeline.name, step.id)
    if record exists:
        _pin_hit = pinned_data
except Exception:
    logger.warning(...)
if _pin_hit is not None:
    context.set_output(step.id, _pin_hit)
    last_output = _pin_hit
    mark ok(reason="pin_mock")
    progress.step_ok
    continue
```

### S20. Test-Mode Intercepts

- Line range: `1158-1187`
- Reads: `pipeline.test_mode`, effective step type
- Writes: `context.set_output`, `last_output`, `step_statuses[step.id]`
- Error handling: none locally
- Pseudocode:

```text
effective_step_type = LEGACY_ALIASES.get(step.type, step.type)
if pipeline.test_mode and effective type is db.upsert:
    emit synthetic dry result
    mark ok(reason="test_mode_dry")
    progress.step_ok
    continue
if pipeline.test_mode and effective type is action.notify:
    emit synthetic log-only result
    mark ok(reason="test_mode_log_only")
    progress.step_ok
    continue
```

### S21. Boolean Step Cache Check

- Line range: `1189-1205`
- Reads: `step.cache is True`, `rendered_params`
- Writes: `context.set_output`, `last_output`, `total_cost_usd`, `step_statuses[step.id]`
- Error handling: cache manager lookup exceptions are not caught here
- Pseudocode:

```text
if step.cache is True:
    cached_output = CacheManager().get(step.id, rendered_params)
    if cached_output is not None:
        context.set_output(step.id, cached_output)
        last_output = cached_output
        total_cost_usd += extract_step_cost(cached_output)
        mark ok(reason="cache_hit")
        progress.step_ok
        continue
```

### S22. Brick Cache Check

- Line range: `1206-1230`
- Reads: `step.cache` dict, `jinja_ctx`, `step.id`
- Writes: `_brick_cache_instance`, `_brick_cache_rendered_key`, `context.set_output`, `last_output`, `total_cost_usd`, `step_statuses[step.id]`
- Error handling: all cache setup/get failures are logged and ignored
- Pseudocode:

```text
if isinstance(step.cache, dict):
    try:
        cache = BrickCache(config, db)
        rendered_key = loader.render_template(step.cache.key or step.id, jinja_ctx)
        hit = cache.get(rendered_key)
        if hit is not None:
            context.set_output(step.id, hit)
            last_output = hit
            total_cost_usd += extract_step_cost(hit)
            mark ok(reason="cache_hit")
            progress.step_ok
            continue
    except Exception:
        logger.warning(...)
```

### S23. Circuit Breaker Pre-Check

- Line range: `1231-1260`
- Reads: `step.circuit_breaker`, `context`
- Writes: `_cb_instance`, maybe `context.set_output`, `last_output`, `step_statuses[step.id]`
- Error handling: breaker failures are logged and ignored
- Pseudocode:

```text
if step.circuit_breaker:
    try:
        cb = CircuitBreaker(step.id, step.circuit_breaker, db)
        cb_pre = cb.pre_check(context)
        if cb_pre is not None:
            if cb_pre.success:
                context.set_output(step.id, cb_pre.data)
                last_output = cb_pre.data
                mark ok(reason="circuit_breaker_fallback")
                progress.step_ok
            else:
                mark skipped(reason=cb_pre.error or "Circuit breaker OPEN")
                progress.step_skipped
            continue
    except Exception:
        logger.warning(...)
```

### S24. Rate Limiter

- Line range: `1261-1271`
- Reads: `step.rate_limit`
- Writes: `_rl_instance`
- Error handling: limiter failures are logged and ignored
- Pseudocode:

```text
if step.rate_limit:
    try:
        rl = RateLimiter(step.id, step.rate_limit, db)
        wait = rl.wait_seconds()
        if wait > 0:
            await sleep(wait)
    except Exception:
        logger.warning(...)
```

### S25. Breakpoint + Context Snapshot

- Line range: `1273-1280`
- Reads: `step.pause_before`, `context`
- Writes: `breakpoint.json`, `context-snapshot.json`
- Error handling: helper methods swallow their own failures
- Pseudocode:

```text
if step.pause_before:
    await _wait_for_breakpoint_resume(context, step.id)
_write_context_snapshot(context)
```

### S26. Execute Runner Under Step Credential Overlay

- Line range: `1282-1288`
- Reads: `runner`, `rendered_step`, `context`, `step`, `pipeline`
- Writes: `step_start`, `_step_started_at`, `result`, `step_duration`, `_step_ended_at`
- Error handling: `_execute_with_retry()` normalizes runner exceptions into failure dicts
- Pseudocode:

```text
progress.step_start(step.id, step.type)
with step_credentials_context(context, step):
    step_start = now
    started_at = utc now iso
    result = await _execute_with_retry(...)
    step_duration = now - step_start
    ended_at = utc now iso
```

### S27. Post-Execution Telemetry

- Line range: `1290-1318`
- Reads: `runner._progress`, `self._run_db`, `step_duration`
- Writes: DB progress row, `_rss_mb`, `_resource_usage`, `result["resource_usage"]`
- Error handling: DB progress persistence is swallowed; memory measurement helpers are best-effort
- Pseudocode:

```text
if runner._progress is None:
    logger.warning(no report_progress)
if runner._progress and _run_db:
    try update_step_progress(...)
measure rss_mb
resource_usage = {"rss_mb": rss_mb, "duration": step_duration}
result["resource_usage"] = resource_usage
warn_if_high_memory(rss_mb, step.id)
```

### S28. Success Path

- Line range: `1320-1381`
- Reads: `result`, caches, breaker, limiter, `step.compensate`, persistence flags
- Writes: `context.set_output`, `last_output`, caches, breaker/limiter state, saga tracker, `total_cost_usd`, `step_statuses[step.id]`, DB step output, DB step execution row
- Error handling: cache persistence warnings logged; breaker/limiter/persistence failures swallowed
- Pseudocode:

```text
if result.success:
    context.set_output(step.id, result.data)
    last_output = result.data
    if bool cache enabled: CacheManager.set(...)
    if brick cache active: cache.set(...)
    if circuit breaker active: cb.on_success()
    if rate limiter active: rl.record_call()
    if step.compensate: saga_tracker.record(step.id, step.compensate)
    total_cost_usd += extract_step_cost(result.data)
    mark ok(duration, items_count, resource_usage)
    progress.step_ok
    if should_persist(step): _persist_step_output(...)
    try history._db.record_step_execution(status="success", ...)
```

### S29. Error Path + Saga Abort

- Line range: `1382-1431`
- Reads: `result`, breaker, `step.on_error`, `pipeline.error_handling.on_error`
- Writes: `step_statuses[step.id]`, maybe `pipeline_aborted`
- Error handling: breaker/persistence/saga failures swallowed
- Pseudocode:

```text
else:
    error_msg = result.error or "unknown error"
    if circuit breaker active: cb.on_failure()
    mark error(duration, errors=1, error_message, resource_usage)
    progress.step_error
    if should_persist(step): _persist_step_output(...)
    try history._db.record_step_execution(status="error", ...)
    effective_on_error = step.on_error or pipeline.error_handling.on_error
    if effective_on_error == "stop":
        try saga_tracker.run_compensations(...)
        pipeline_aborted = True
        break
    else:
        continue loop
```

---

## 3. DAG Path — Complete Block-by-Block

DAG execution lives in `engine.py` L2099 through L2528.

**Explore-Agent metric:** 18 DAG execution blocks, approximately 390 lines.

### D1. DAG Setup

- Line range: `2113-2129`
- Reads: `pipeline.steps`
- Writes: `step_by_id`, `last_output`, `pipeline_aborted`, `stop_step_success`, `done_events`, `step_ok`
- Error handling: `_toposort_steps()` exceptions are re-raised to caller
- Pseudocode:

```text
steps = pipeline.steps
step_by_id = {id -> step}
toposort_steps(steps) for validation only
init last_output = None
init pipeline_aborted = False
init stop_step_success = None
done_events = {step.id: asyncio.Event()}
step_ok = {}
```

### D2. Dependency Wait / Failed Dependency Bailout

- Line range: `2134-2148`
- Reads: `step.depends_on`, `done_events`, `step_ok`
- Writes: `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: none locally
- Pseudocode:

```text
for dep_id in step.depends_on:
    await done_events[dep_id].wait()
    if not step_ok.get(dep_id, False):
        mark skipped(reason=f"dependency '{dep_id}' failed")
        progress.step_skipped
        step_ok[step.id] = False
        done_events[step.id].set()
        return
```

### D3. Resume Completed Step

- Line range: `2149-2157`
- Reads: `context.is_step_completed`, `context.get_output`
- Writes: `step_statuses[step.id]`, `last_output`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: none locally
- Pseudocode:

```text
if context says step already completed:
    mark ok duration 0
    restore last_output
    progress.step_resumed
    step_ok[step.id] = True
    done_events[step.id].set()
    return
```

### D4. Disabled-Step Skip

- Line range: `2158-2167`
- Reads: `step.enabled`
- Writes: `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: none
- Pseudocode:

```text
if not step.enabled:
    mark skipped(reason="disabled")
    progress.step_skipped
    step_ok[step.id] = True
    done_events[step.id].set()
    return
```

### D5. Selective Dry Run

- Line range: `2168-2176`
- Reads: `dry_run_steps`
- Writes: `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: none
- Pseudocode:

```text
if dry_run_steps and step.id in dry_run_steps:
    mark dry_run(reason="dry_run_steps")
    progress.step_skipped
    step_ok[step.id] = True
    done_events[step.id].set()
    return
```

### D6. `when` Condition Gate

- Line range: `2178-2190`
- Reads: `context.to_jinja_context()`, `step.when`
- Writes: `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: exceptions escape to task; caller uses `gather(return_exceptions=True)` so they are swallowed at gather level
- Pseudocode:

```text
jinja_ctx = context.to_jinja_context()
if step.when:
    should_run = loader.evaluate_condition(step.when, jinja_ctx)
    if not should_run:
        mark skipped(reason="condition not met")
        progress.step_skipped
        step_ok[step.id] = True
        done_events[step.id].set()
        return
```

### D7. `else_of` Gate

- Line range: `2191-2204`
- Reads: `step.else_of`, `step_statuses`
- Writes: `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: none
- Pseudocode:

```text
if step.else_of:
    ref_status = step_statuses.get(step.else_of)
    if ref missing or ref_status.status != "skipped":
        mark skipped(reason=f"else_of '{ref}' was not skipped")
        progress.step_skipped
        step_ok[step.id] = True
        done_events[step.id].set()
        return
```

### D8. `stop` Step Handling

- Line range: `2205-2243`
- Reads: `step.type`, `step.when`, `step.message`, `context.to_jinja_context()`
- Writes: `step_statuses[step.id]`, `pipeline_aborted`, `stop_step_success`, all `done_events`, `step_ok[step.id]`
- Error handling: exceptions escape task
- Pseudocode:

```text
if step.type == "stop":
    compute _should_stop using same bool/string logic as sequential path
    if not _should_stop:
        mark skipped(reason="condition not met")
        progress.step_skipped
        step_ok[step.id] = True
        done_events[step.id].set()
        return
    render stop message if needed
    mark ok(reason=rendered_msg)
    progress.step_ok
    pipeline_aborted = True
    stop_step_success = step.success_on_stop default True
    set every done_event to unblock waiters
    step_ok[step.id] = False
    return
```

### D9. Compositor-Mode Guard

- Line range: `2244-2265`
- Reads: `pipeline.compositor_mode`, `pipeline.allow_code`, `step.type`, `step.on_error`, `pipeline.error_handling.on_error`
- Writes: `step_statuses[step.id]`, `pipeline_aborted`, all `done_events` on stop, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: none
- Pseudocode:

```text
if compositor_mode and not allow_code and step.type in {"python","cli"}:
    mark error
    progress.step_start + step_error
    if effective_on_error == "stop":
        pipeline_aborted = True
        set all done_events
    step_ok[step.id] = False
    done_events[step.id].set()
    return
```

### D10. Profile + Brick Default Merge

- Line range: `2266-2270`
- Reads: step profile and brick metadata
- Writes: local `step`
- Error handling: helper methods swallow failures
- Pseudocode:

```text
step = _apply_profile(step)
step = _apply_brick_defaults(step)
```

### D11. Dynamic Type Render + Runner Resolution

- Line range: `2272-2292`
- Reads: `step.type`, `context.to_jinja_context()`
- Writes: `_early_jinja_ctx_dag`, `runner`, `step_statuses[step.id]`, maybe `pipeline_aborted`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: strict-bricks `ValueError` can escape task; unresolved runner is converted to step error
- Pseudocode:

```text
early_jinja_ctx = context.to_jinja_context() if "{{" in step.type else None
runner = _resolve_runner(step.type, early_jinja_ctx)
if no runner:
    mark error("no runner registered ...")
    progress.step_start + step_error
    if effective_on_error == "stop":
        pipeline_aborted = True
        set all done_events
    step_ok[step.id] = False
    done_events[step.id].set()
    return
```

### D12. Single-Step Dependency Check

- Line range: `2294-2311`
- Reads: `step.requirements`
- Writes: `step_statuses[step.id]`, maybe `pipeline_aborted`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: `_ensure_step_requirements()` returns string error
- Pseudocode:

```text
if step.requirements:
    dep_err = _ensure_step_requirements(step)
    if dep_err:
        mark error(dep_err)
        progress.step_start + step_error
        if effective_on_error == "stop":
            pipeline_aborted = True
            set all done_events
        step_ok[step.id] = False
        done_events[step.id].set()
        return
```

### D13. Render Step Params + Build `_RenderedStep`

- Line range: `2313-2316`
- Reads: `context.to_jinja_context()`, `step`
- Writes: `jinja_ctx`, `rendered_params`, `rendered_step`
- Error handling: render exceptions escape task
- Pseudocode:

```text
jinja_ctx = context.to_jinja_context()
rendered_params = loader.render_step_params(step, jinja_ctx)
rendered_step = _RenderedStep(step, rendered_params, loader, jinja_ctx)
```

### D14. Pin Mock Short-Circuit

- Line range: `2318-2340`
- Reads: `pipeline.name`, `step.id`
- Writes: `_pin_hit`, `context.set_output`, `last_output`, `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: pin DB lookup failures logged and ignored
- Pseudocode:

```text
try get pin
if pin hit:
    context.set_output(step.id, pinned_data)
    last_output = pinned_data
    mark ok(reason="pin_mock")
    progress.step_ok
    step_ok[step.id] = True
    done_events[step.id].set()
    return
```

### D15. Test-Mode Intercepts

- Line range: `2342-2375`
- Reads: `pipeline.test_mode`, effective step type
- Writes: `context.set_output`, `last_output`, `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: none locally
- Pseudocode:

```text
if test_mode and effective type is db.upsert:
    emit dry synthetic result
    mark ok(reason="test_mode_dry")
    progress.step_ok
    step_ok[step.id] = True
    done_events[step.id].set()
    return
if test_mode and effective type is action.notify:
    emit log-only synthetic result
    mark ok(reason="test_mode_log_only")
    progress.step_ok
    step_ok[step.id] = True
    done_events[step.id].set()
    return
```

### D16. Boolean Step Cache Check

- Line range: `2377-2394`
- Reads: `step.cache`, `rendered_params`
- Writes: `context.set_output`, `last_output`, `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: no local catch
- Pseudocode:

```text
if step.cache is True:
    cached_output = CacheManager().get(step.id, rendered_params)
    if cached_output is not None:
        context.set_output(step.id, cached_output)
        last_output = cached_output
        mark ok(reason="cache_hit")
        progress.step_ok
        step_ok[step.id] = True
        done_events[step.id].set()
        return
```

### D17. Brick Cache Check

- Line range: `2395-2420`
- Reads: `step.cache` dict, `jinja_ctx`
- Writes: `_brick_cache_instance`, `_brick_cache_rendered_key`, `context.set_output`, `last_output`, `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: exceptions logged and ignored
- Pseudocode:

```text
if isinstance(step.cache, dict):
    try:
        cache = BrickCache(...)
        rendered_key = loader.render_template(...)
        hit = cache.get(rendered_key)
        if hit is not None:
            context.set_output(step.id, hit)
            last_output = hit
            mark ok(reason="cache_hit")
            progress.step_ok
            step_ok[step.id] = True
            done_events[step.id].set()
            return
    except Exception:
        logger.warning(...)
```

### D18. Circuit Breaker Pre-Check

- Line range: `2421-2452`
- Reads: `step.circuit_breaker`, `context`
- Writes: `_cb_instance`, `context.set_output`, `last_output`, `step_statuses[step.id]`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: exceptions logged and ignored
- Pseudocode:

```text
if step.circuit_breaker:
    try:
        cb = CircuitBreaker(...)
        cb_pre = cb.pre_check(context)
        if cb_pre is not None:
            if cb_pre.success:
                context.set_output(step.id, cb_pre.data)
                last_output = cb_pre.data
                mark ok(reason="circuit_breaker_fallback")
                progress.step_ok
                step_ok[step.id] = True
            else:
                mark skipped(reason=cb_pre.error or "Circuit breaker OPEN")
                progress.step_skipped
                step_ok[step.id] = True
            done_events[step.id].set()
            return
    except Exception:
        logger.warning(...)
```

### D19. Rate Limiter

- Line range: `2453-2463`
- Reads: `step.rate_limit`
- Writes: `_rl_instance`
- Error handling: exceptions logged and ignored
- Pseudocode:

```text
if step.rate_limit:
    try:
        rl = RateLimiter(...)
        wait = rl.wait_seconds()
        if wait > 0:
            await sleep(wait)
    except Exception:
        logger.warning(...)
```

### D20. Breakpoint

- Line range: `2465-2467`
- Reads: `step.pause_before`
- Writes: breakpoint sentinel and paused metadata via helper
- Error handling: helper swallows its own failures
- Pseudocode:

```text
if step.pause_before:
    await _wait_for_breakpoint_resume(context, step.id)
```

### D21. Execute Runner Under Step Credential Overlay

- Line range: `2469-2474`
- Reads: `runner`, `rendered_step`, `context`, `step`, `pipeline`
- Writes: `step_start`, `result`, `step_duration`
- Error handling: `_execute_with_retry()` normalizes runner exceptions
- Pseudocode:

```text
progress.step_start(step.id, step.type)
with step_credentials_context(context, step):
    step_start = now
    result = await _execute_with_retry(...)
    step_duration = now - step_start
```

### D22. Success Path

- Line range: `2475-2503`
- Reads: `result`, caches, breaker, limiter
- Writes: `context.set_output`, `last_output`, cache stores, breaker/limiter state, `step_statuses[step.id]`, `step_ok[step.id]`
- Error handling: cache set failures logged; breaker/limiter failures swallowed
- Pseudocode:

```text
if result.success:
    context.set_output(step.id, result.data)
    last_output = result.data
    if bool cache: CacheManager.set(...)
    if brick cache: cache.set(...)
    if circuit breaker: cb.on_success()
    if rate limiter: rl.record_call()
    mark ok(duration, items_count)
    progress.step_ok
    step_ok[step.id] = True
```

### D23. Error Path

- Line range: `2504-2522`
- Reads: `result`, breaker, `step.on_error`, `pipeline.error_handling.on_error`
- Writes: `step_statuses[step.id]`, maybe `pipeline_aborted`, maybe all `done_events`, `step_ok[step.id]`, `done_events[step.id]`
- Error handling: breaker failures swallowed
- Pseudocode:

```text
else:
    error_msg = result.error or "unknown error"
    if circuit breaker active: cb.on_failure()
    mark error(duration, errors=1, error_message)
    progress.step_error
    if effective_on_error == "stop":
        pipeline_aborted = True
        set all done_events
    step_ok[step.id] = False
done_events[step.id].set()
```

### D24. Task Dispatch / Gather / Return

- Line range: `2524-2528`
- Reads: `steps`
- Writes: `tasks`
- Error handling: `asyncio.gather(*tasks, return_exceptions=True)` suppresses task exceptions from propagating
- Pseudocode:

```text
tasks = [create_task(run_step(step)) for step in steps]
await gather(tasks, return_exceptions=True)
return pipeline_aborted, last_output, pipeline_aborted, stop_step_success
```

---

## 4. Block-by-Block Comparison Table

**Explore-Agent summary:** 12 blocks IDENTICAL between paths, 6 SIMILAR. The DAG path is missing 9 sequential capabilities entirely.

| Block | Sequential | DAG | Status | Exact Difference |
|---|---:|---:|---|---|
| Mode dispatch | `798-810` | `2113-2129` | DIFFERENT | Sequential branch decides whether to enter DAG at all. DAG setup validates graph and allocates events; no equivalent branch logic inside DAG. |
| Cancel sentinel check | `812-815` | MISSING | MISSING | DAG path never checks `_is_run_cancelled()` before or during normal step execution. Only breakpoint wait honors cancellation. |
| Resume completed step | `817-823` | `2149-2157` | SIMILAR | DAG sets `step_ok[step.id]=True` and `done_events[step.id].set()`. Sequential has no event bookkeeping. |
| Disabled-step skip | `824-830` | `2158-2167` | SIMILAR | DAG marks disabled steps as downstream-usable via `step_ok=True` and signals event completion. Sequential only records status and continues. |
| Selective dry-run | `832-839` | `2168-2176` | SIMILAR | Same skip behavior; DAG also sets `step_ok=True` and event. |
| `when` condition | `841-850` | `2178-2190` | SIMILAR | DAG sets `step_ok=True` and event on skip; sequential does not. Exception behavior differs because DAG task exceptions are swallowed by `gather(return_exceptions=True)`. |
| `else_of` gate | `852-862` | `2191-2204` | SIMILAR | Same gating logic; DAG additionally sets `step_ok=True` and event. |
| `stop` step | `864-897` | `2205-2243` | SIMILAR | Logic is nearly identical, but DAG calls `progress.step_ok`, sets all events to unblock waiters, and records `step_ok[step.id]=False`; sequential just sets `pipeline_aborted` and breaks. |
| Compositor guard | `899-917` | `2244-2265` | SIMILAR | Same error generation. DAG also sets `step_ok=False`, signals events, and may fan out abort to all waiting tasks. |
| Profile merge | `918-919` | `2266-2267` | IDENTICAL | Same helper call order. |
| Brick default merge | `921-922` | `2269-2270` | IDENTICAL | Same helper call order. |
| Dynamic runner resolution | `924-941` | `2272-2292` | SIMILAR | Same resolution logic. DAG adds `step_ok` and `done_events` bookkeeping on failure. |
| `validate_config` | `943-970` | MISSING | MISSING | DAG path never builds `_vc_config`, never calls `runner.validate_config()`, and therefore can execute steps the sequential path would reject. |
| Foreach dependency check | `972-989` | MISSING | MISSING | DAG has no foreach branch at all. |
| Foreach resolution | `990-1011` | MISSING | MISSING | DAG cannot execute `step.foreach`; no counterpart. |
| Foreach batching / parallel selection | `1012-1054` | MISSING | MISSING | No DAG foreach support. |
| Foreach post-processing | `1056-1110` | MISSING | MISSING | No DAG foreach hints, outputs, or aggregate failure handling. |
| Single-step dependency install | `1112-1126` | `2294-2311` | SIMILAR | Same install logic; DAG adds event bookkeeping and global abort signaling on stop. |
| Render params + `_RenderedStep` | `1128-1134` | `2313-2316` | IDENTICAL | Same render/build sequence. |
| Pin mock | `1136-1156` | `2318-2340` | SIMILAR | Same pin lookup and synthetic success. DAG adds `step_ok=True` and event signaling. |
| Test-mode intercepts | `1158-1187` | `2342-2375` | SIMILAR | Same synthetic outputs; DAG adds `step_ok=True` and event signaling. |
| Boolean cache read | `1189-1205` | `2377-2394` | SIMILAR | Same cache-hit short-circuit. Sequential increments `total_cost_usd`; DAG does not track cost. DAG adds event bookkeeping. |
| Brick cache read | `1206-1230` | `2395-2420` | SIMILAR | Same cache hit flow. Sequential increments `total_cost_usd`; DAG does not. DAG adds event bookkeeping. |
| Circuit breaker pre-check | `1231-1260` | `2421-2452` | SIMILAR | Same open/fallback handling. DAG treats both fallback and skipped-open as `step_ok=True` for downstream, then signals event. Sequential has no dependency bookkeeping. |
| Rate limiter | `1261-1271` | `2453-2463` | IDENTICAL | Same wait logic. |
| Breakpoint wait | `1273-1275` | `2465-2467` | IDENTICAL | Same helper call. |
| Context snapshot write | `1277-1280` | MISSING | MISSING | DAG does not write `context-snapshot.json` before steps. |
| Execute step | `1282-1288` | `2469-2474` | SIMILAR | Same credentials wrapper and retry helper. Sequential additionally records UTC timestamps before and after execution. |
| Progress compliance warning | `1290-1297` | MISSING | MISSING | DAG never warns if a runner failed to call `report_progress()`. |
| DB progress persistence | `1299-1312` | MISSING | MISSING | DAG never persists runner progress to `step_progress`. |
| Resource measurement | `1314-1318` | MISSING | MISSING | DAG never attaches `resource_usage` to results or statuses, and never calls `_warn_if_high_memory()`. |
| Success handling core | `1320-1356` | `2475-2503` | SIMILAR | Both set output, persist caches, reset breaker, record rate limiter, and mark status ok. Sequential additionally records saga compensation, accumulates LLM cost, attaches `resource_usage`, and stores richer `StepStatus`. |
| Persist `step_outputs` | `1357-1362` | MISSING | MISSING | DAG never calls `_persist_step_output()`. |
| Persist `step_executions` success row | `1363-1381` | MISSING | MISSING | DAG never records success execution rows. |
| Error handling core | `1382-1395` | `2504-2514` | SIMILAR | Both call breaker failure hook, record error status, and emit progress error. Sequential stores `resource_usage`; DAG does not. |
| Persist `step_outputs` on error | `1396-1401` | MISSING | MISSING | DAG lacks debug output persistence on failure. |
| Persist `step_executions` error row | `1402-1421` | MISSING | MISSING | DAG never records error execution rows. |
| Error `on_error=stop` handling | `1423-1431` | `2515-2522` | DIFFERENT | Sequential runs saga compensations before aborting. DAG just flips `pipeline_aborted`, signals all events, and stops; no saga compensation path exists. |
| Task dispatch / gather | MISSING | `2524-2528` | DAG-ONLY | Sequential path has direct loop; DAG creates one task per step and gathers with `return_exceptions=True`. |

**Major parity conclusion (both analyses agree):** The DAG path is not a scheduling variant. It omits validate_config, foreach, cancellation polling, context snapshotting, progress DB persistence, resource measurement, step-output persistence, step-execution persistence, LLM cost tracking, and saga compensation. This is the most important finding in the entire analysis.

---

## 4a. DAG Missing Features — Authoritative List

The Explore-Agent analysis explicitly enumerated 9 capabilities absent from the DAG path. This list is confirmed by the Codex block-by-block comparison and collected here for planning purposes.

| # | Missing Capability | Sequential Block(s) | Risk if Not Fixed |
|---|---|---|---|
| 1 | `validate_config` | S12 | DAG can run steps the sequential path would reject as misconfigured |
| 2 | `foreach` | S13-S16 | Pipelines with `foreach` + `depends_on` silently skip foreach entirely |
| 3 | Resource usage tracking | S27 | No RSS/memory data on DAG runs; no `_warn_if_high_memory` calls |
| 4 | DB step records (`step_outputs` + `step_executions`) | S28 success, S29 error | DAG runs produce zero debuggable execution history |
| 5 | Saga compensation | S29 | DAG `on_error=stop` leaves compensatable steps uncompensated |
| 6 | LLM cost accumulation | S21, S22, S28 | `total_cost_usd` is always 0 for DAG runs |
| 7 | Progress compliance check | S27 | Runners that forget `report_progress()` go unchecked in DAG mode |
| 8 | Context snapshot write | S25 | DAG step debugging requires `pause_before` but has no pre-step snapshot |
| 9 | Cancel sentinel poll | S2 | Cancellation only works at breakpoint; mid-DAG cancel requests are ignored |

---

## 5. Shared State Analysis

### `PipelineEngine` instance variables

- `self.loader`
  Type: `PipelineLoader`
  Role: template rendering, condition evaluation, foreach resolution.

- `self.progress`
  Type: `ProgressReporter`
  Role: all stderr/log style execution progress events.

- `self._runners`
  Type: `dict[str, BaseRunner]`
  Role: flat runner registry used after brick/type resolution.

- `self._brick_registry`
  Type: `BrickRegistry`
  Role: maps brick names to runner names and schema metadata.

- `self._deprecation_db`
  Type: `BrixDB | None`
  Role: lazy DB handle for deprecated alias usage tracking.

- `self._current_pipeline_name`
  Type: `str`
  Role: per-run context for deprecation tracking.

- `self._strict_bricks`
  Type: `bool`
  Role: per-run strict-alias enforcement flag.

- `self._deprecation_warnings`
  Type: `list[str]`
  Role: accumulated run warnings returned in `RunResult`.

- `self._mcp_pool`
  Type: `McpConnectionPool | None`
  Role: current run's MCP pool, also wired into `McpRunner`.

- `self._last_step_outputs`
  Type: `dict[str, Any]`
  Role: exposes sub-step outputs from the most recent run for outer runners like `RepeatRunner`.

- `self._run_db`
  Type: `Any | None`
  Role: current run DB handle used by foreach item recording and step progress updates.

### Run-scoped locals shared across the main sequential loop

- `history`: Run-history façade used for `record_start`, idempotency lookup, `record_finish`, and cancellation.
- `context`: Central mutable execution state: inputs, outputs, credentials, workdir, metadata, resume checkpoints, progress snapshots.
- `step_statuses`: Shared dict of `step_id -> StepStatus`. Read by `else_of`, final result computation, history persistence, alerts, and trigger completion recording.
- `last_output`: Shared "latest successful output" cursor, used for implicit pipeline result when no explicit `pipeline.output`.
- `pipeline_aborted`: Shared loop control flag. Set by cancel, stop step, DAG errors, compositor blocks, dependency install failures, execution failures, and outer exceptions.
- `stop_step_success`: Shared override that controls final `all_ok` if a `stop` step intentionally ended the run.
- `total_cost_usd`: Shared accumulator updated from cache hits and step successes. Used only in sequential final history persistence.
- `_saga_tracker`: Shared resilience tracker that records compensatable steps and runs compensations on stop-on-error.
- `jinja_ctx`: Recomputed frequently; shared only within one iteration, but critical because many branch decisions depend on its freshness after context mutation.
- `rendered_params`, `rendered_step`, `runner`, `result`: Per-step temporaries passed through multiple later blocks.

### DAG shared locals

- `done_events`: `dict[str, asyncio.Event]`; this is the DAG scheduler's synchronization fabric.
- `step_ok`: `dict[str, bool]`; defines whether downstream dependent steps are allowed to run.
- `pipeline_aborted`, `last_output`, `stop_step_success`: Shared through `nonlocal` closure capture inside `run_step`.
- `step_statuses`: Shared mutable status map, same conceptual role as in sequential path.

### Values passed between blocks

- `step -> step` rebinding after `_apply_profile()` and `_apply_brick_defaults()`
- `jinja_ctx -> rendered_params -> _RenderedStep -> runner.execute()`
- `result -> status/persistence/cache/cost/last_output`
- `step.on_error or pipeline.error_handling.on_error -> control-flow branch`
- `context.step_outputs -> downstream Jinja rendering -> pipeline output rendering`
- `context.workdir sentinel files -> cancellation/breakpoint behavior`

### Interface between engine and `PipelineContext`

Methods/properties used by engine:

- `PipelineContext.from_pipeline(...)`
- `context.input`, `context.credentials`, `context._jinja_cache`
- `context.to_jinja_context(...)`
- `context.is_step_completed(step_id)`, `context.get_output(step_id)`, `context.set_output(step_id, value)`
- `context.save_run_metadata(name, status, progress=...)`
- `context.cleanup(keep=...)`, `context.workdir`, `context.run_id`, `context._resume_from`
- `context.load_foreach_checkpoint(step_id)`, `context.write_foreach_checkpoint(step_id, item_index, item_input, result)`
- `context.update_step_progress(step_id, payload)`
- `context._secret_values`, `PipelineContext._refresh_credential(...)`

This is a large interface. Any extraction that moves step execution into another module either needs direct `PipelineContext` coupling or an adapter object that preserves all of these calls.

### Interface between engine and `PipelineLoader`

Methods used: `render_template`, `render_value`, `render_step_params`, `evaluate_condition`, `resolve_foreach`.

The engine is tightly coupled to `PipelineLoader` because it decides execution and scheduling branches based on rendered values, not just runner inputs.

### Interface between engine and `ProgressReporter`

Methods used: `pipeline_start`, `pipeline_done`, `step_start`, `step_ok`, `step_error`, `step_skipped`, `step_resumed`, `foreach_progress`, `foreach_done`.

The progress interface is stable and clean. It is a good candidate to pass into extracted sequential/DAG executors unchanged.

---

## 6. External Dependencies

### What `engine.py` imports

Top-level imports:

- Standard library: `asyncio`, `json`, `logging`, `os`, `sys`, `time`, `traceback`, `contextlib.contextmanager`, `datetime`, `timezone`, `typing.Any`
- Models and engine-adjacent infrastructure: `Pipeline`, `Step`, `StepStatus`, `RunResult`, `RetryConfig`, `RetryProfile`, `PipelineLoader`, `PipelineContext`, `config`, `ProgressReporter`, `McpConnectionPool`, `CredentialStore`, `is_credential_uuid`, `CredentialNotFoundError`, `json_dumps`, `sanitize_for_json`
- Built-in runners: `BaseRunner`, `discover_runners`, `CliRunner`, `parse_timeout` (imported but unused in this file), `PythonRunner`, `HttpRunner`, `McpRunner`, `PipelineRunner`, `FilterRunner`, `TransformRunner`, `SetRunner`, `ChooseRunner`, `ParallelStepRunner`, `RepeatRunner`, `NotifyRunner`, `ApprovalRunner`, `ValidateRunner`, `PipelineGroupRunner`, `SpecialistRunner`, `QueueRunner`, `EmitRunner`

Lazy imports inside methods:

- `brix.db.BrixDB`
- `brix.bricks.registry.BrickRegistry`
- `brix.history.RunHistory`
- `brix.helper_registry.HelperRegistry`
- `pathlib.Path`
- `brix.deps.check_requirements`, `install_requirements`
- `brix.resilience.SagaTracker`, `BrickCache`, `CircuitBreaker`, `RateLimiter`
- `brix.context.CacheManager`
- `brix.server_manager.ServerManager`
- `brix.triggers.state.TriggerState`
- `brix.alerting.AlertManager`
- `warnings`, `importlib.metadata`, `collections.deque`

### What imports `engine.py`

Runtime modules:

- `src/brix/api.py`: `PipelineEngine`
- `src/brix/cli.py`: `PipelineEngine`
- `src/brix/debug_tools.py`: `PipelineEngine`, `_RenderedStep`
- `src/brix/mcp_handlers/_shared.py`: `PipelineEngine`
- `src/brix/mcp_handlers/pipelines.py`: `LEGACY_ALIASES`
- `src/brix/mcp_handlers/runs.py`: `PipelineEngine`
- `src/brix/mcp_handlers/steps.py`: `LEGACY_ALIASES`
- `src/brix/mcp_server.py`: `PipelineEngine`
- `src/brix/resilience.py`: `_RenderedStep`
- `src/brix/testing.py`: `PipelineEngine`
- `src/brix/triggers/runners.py`: `PipelineEngine`
- `src/brix/validator.py`: `LEGACY_ALIASES`

Test modules importing `engine.py` symbols are listed in Section 7.

### Public API vs internal API

Likely intended public runtime API: `PipelineEngine`, `LEGACY_ALIASES`.

Internal-by-naming but externally consumed: `_VALIDATE_CONFIG_TOP_LEVEL_FIELDS`, `_RenderedStep`, `_extract_step_cost`, `_redact_secret_values`, `_measure_rss_mb`, `_total_ram_mb`, `_warn_if_high_memory`, `_step_config_dict`.

Implication: splitting this file is not just internal cleanup. Several underscore-prefixed symbols are imported by tests and runtime modules. Either re-export them from `brix.engine` or update every downstream import.

---

## 7. Test Coverage Map

### Test files importing from `engine.py`

- `tests/debug_v3_pipeline.py`: ad hoc debug script; not formal coverage.
- `tests/test_advanced_flow.py`: advanced flow runners (queue, emit, debounce).
- `tests/test_alerting.py`: alerting subsystem; engine integration verifies runs produce alert-evaluable output.
- `tests/test_backward_compat.py`: legacy CLI and v1 pipeline compatibility. Exercises `PipelineEngine.run()` through old pipeline formats.
- `tests/test_brick_composition.py`: `_apply_profile`, `_resolve_runner` dynamic dispatch, and composition behavior.
- `tests/test_brick_first_engine.py`: brick registry resolution, system brick mapping, `LEGACY_ALIASES`, and runner lookup.
- `tests/test_dag_execution.py`: `_detect_dag_mode`, `_toposort_steps`, simple DAG scheduling, output visibility, diamond graph, cycle detection.
- `tests/test_dag_feature_parity.py`: explicitly covers DAG parity for `pause_before`, `test_mode`, pin mocks, and cache hits.
- `tests/test_db_only_handlers.py`: DB-backed MCP handlers; step-level credentials injection via engine.
- `tests/test_deprecation_enforcement.py`: deprecated alias tracking, `RunResult.deprecation_warnings`, and `strict_bricks`.
- `tests/test_deps.py`: dependency management utilities and pipeline-level dependency install behavior.
- `tests/test_engine.py`: core sequential engine behavior including ordering, `when`, `on_error`, output handling.
- `tests/test_engine_mcp_pool.py`: `run(..., mcp_pool=...)` lifecycle semantics, ownership, cleanup.
- `tests/test_engine_validate_config_allowlist.py`: `_VALIDATE_CONFIG_TOP_LEVEL_FIELDS` correctness.
- `tests/test_flow_pipeline_config_bug.py`: flow.pipeline config promotion bug and foreach resolve errors.
- `tests/test_helper_registry.py`: helper registry plus engine integration.
- `tests/test_idempotency.py`: run idempotency short-circuit.
- `tests/test_imp_engine.py`: brick default merging, validation payload precedence.
- `tests/test_intra_step_progress.py`: intra-step progress parsing and `_RenderedStep` integration.
- `tests/test_pin_mock.py`: DB pin CRUD plus engine short-circuiting on pinned steps.
- `tests/test_pipeline_group_runner.py`: `flow.pipeline_group` runner integration.
- `tests/test_pipeline_runner.py`: `flow.pipeline` sub-pipeline runner integration.
- `tests/test_poc_brick_pipeline.py`: end-to-end brick pipeline flow and alias resolution.
- `tests/test_progress.py`: `ProgressReporter` plus engine-driven foreach progress emission.
- `tests/test_progress_everywhere.py`: runner-level progress calls across multiple runner types.
- `tests/test_python_jinja_rendering.py`: `_RenderedStep` behavior as consumed by `PythonRunner`.
- `tests/test_repeat_runner.py`: repeat runner semantics and propagation of child outputs via engine state.
- `tests/test_resilience.py`: circuit breaker, rate limiter, brick cache, saga pieces.
- `tests/test_run_persistence.py`: DB persistence tables and engine integration.
- `tests/test_secret_variables.py`: `_redact_secret_values` and secret-variable DB behavior.
- `tests/test_self_healing.py`: `_ensure_step_requirements` and engine pre-execution dependency installation.
- `tests/test_smoke_regression.py`: broad end-to-end smoke coverage.
- `tests/test_step_cache.py`: step-level cache manager and engine cache-hit short-circuit.
- `tests/test_stop_step_skipped.py`: sequential `stop` step skip/execute semantics.
- `tests/test_trigger_chaining.py`: trigger chaining pipeline completion semantics.
- `tests/test_triggers.py`: trigger system state and pipeline-completion event generation.
- `tests/test_v5_managed_helpers.py`: dual-path helper resolution in engine.
- `tests/test_v6_cancel_run.py`: cancellation sentinel behavior, cancel cleanup, foreach cancel polling.
- `tests/test_v6_llm_cost_tracking.py`: `_extract_step_cost`, sequential cost accumulation.
- `tests/test_v6_retry_profiles.py`: retry profiles, retry config fallback, backoff behavior.
- `tests/test_v7_04_execution_data.py`: `persist_output`, `_should_persist`, `_context_snapshot`.
- `tests/test_v7_05_run_diff_mcp_trace_env.py`: `_capture_environment`, `_persist_step_output` with `_mcp_trace`.
- `tests/test_v7_06_debug_tools.py`: `_write_context_snapshot`, `_wait_for_breakpoint_resume`, pause-before integration.
- `tests/test_v7_07_resource_monitoring.py`: `_measure_rss_mb`, `_total_ram_mb`, `_warn_if_high_memory`, resource usage in statuses.
- `tests/test_v8_compositor_mode.py`: compositor-mode model behavior and engine runtime enforcement.
- `tests/test_v8_specialist.py`: specialist runner registration inside engine.

### Blocks with weak or no direct test coverage

DAG path lacks direct tests for:
- dependency failure propagation via `step_ok`
- `else_of` inside DAG
- `stop` semantics inside DAG
- strict-bricks failure inside DAG
- per-step requirements install failure inside DAG
- circuit breaker fallback/open behavior inside DAG
- rate limiter delay inside DAG
- runner failure with `on_error=stop` in DAG
- swallowed task exceptions because of `gather(return_exceptions=True)`

Sequential-only blocks with weak direct tests:
- `validate_config` failure branch
- runner `_progress` missing compliance warning
- DB step progress persistence (`update_step_progress`)
- `_db_log` integration
- final trigger completion recording
- alert-manager invocation from `run`
- helper requirement installation failure path
- batch foreach aggregation path
- perf hints in foreach results

Entire parity gaps with little or no explicit tests: all 9 items in Section 4a.

---

## 8. Risk Assessment

### Move 1: Extract `_RenderedStep` and step-render helpers into shared types/module

- What could break: `PythonRunner`, `debug_tools`, and `resilience` import `_RenderedStep` directly. Subtle field-copy behavior in `_RenderedStep.__init__` is relied on by multiple runners.
- Tests likely affected: `test_python_jinja_rendering.py`, `test_intra_step_progress.py`, `test_pipeline_runner.py`, `test_repeat_runner.py`
- Circular import risk: High if `_RenderedStep` moves into a module that imports runner classes or `PipelineEngine`. Low if moved to a pure `engine_types.py` importing only `Step` and `PipelineLoader`.

### Move 2: Extract runner resolution/profile/default-merging logic

- What could break: `strict_bricks` enforcement and deprecation warning accumulation are engine-stateful. `LEGACY_ALIASES` is imported by validators and handlers.
- Tests likely affected: `test_brick_first_engine.py`, `test_brick_composition.py`, `test_deprecation_enforcement.py`, `test_imp_engine.py`
- Circular import risk: Medium if extracted code imports `PipelineEngine` or DB-layer code.

### Move 3: Extract sequential step executor

- What could break: Shared mutation of `step_statuses`, `last_output`, `pipeline_aborted`, `total_cost_usd`, and saga tracker. The exact ordering between status writes, context writes, progress calls, and DB persistence.
- Tests likely affected: `test_engine.py`, `test_flow_pipeline_config_bug.py`, `test_pin_mock.py`, `test_step_cache.py`, `test_v7_04_execution_data.py`, `test_v7_05_run_diff_mcp_trace_env.py`, `test_v7_07_resource_monitoring.py`, `test_v6_llm_cost_tracking.py`
- Circular import risk: Medium if the extracted executor imports `PipelineEngine` instead of receiving an engine façade/protocol.

### Move 4: Extract foreach executor

- What could break: Resume checkpoint semantics. Shared `self._run_db` usage. Progress updates and metadata snapshots used by `get_run_status`.
- Tests likely affected: `test_flow_pipeline_config_bug.py`, `test_progress.py`, `test_v6_cancel_run.py`, `test_engine.py`
- Circular import risk: Low if foreach helpers receive dependencies as parameters.

### Move 5: Extract DAG scheduler into `engine_dag.py`

- What could break: Nonlocal/shared mutation of `pipeline_aborted`, `last_output`, `stop_step_success`, `step_statuses`. Event signaling correctness; a missed `done_events[step.id].set()` can deadlock the whole DAG. Existing parity gaps could be preserved accidentally or changed unintentionally.
- Tests likely affected: `test_dag_execution.py`, `test_dag_feature_parity.py`, `test_v8_compositor_mode.py`
- Circular import risk: Medium. Prefer inversion: `engine.py` imports a thin `run_dag(engine, ...)` function rather than the reverse.

### Move 6: Extract run finalization/history persistence

- What could break: `RunResult` shape, deprecation warnings, cancellation handling, `history.record_finish`, trigger completion state, alert invocation. The exact `all_ok` calculation and stop-step override.
- Tests likely affected: `test_run_persistence.py`, `test_v6_llm_cost_tracking.py`, `test_trigger_chaining.py`, `test_triggers.py`, `test_alerting.py`, `test_smoke_regression.py`
- Circular import risk: Low to medium. Finalization code imports history, triggers, and alerting lazily already.

### Move 7: Extract debug/persistence helpers

- What could break: `persist_output`, `_mcp_trace` merge behavior, context snapshot shape, live debug tools.
- Tests likely affected: `test_v7_04_execution_data.py`, `test_v7_05_run_diff_mcp_trace_env.py`, `test_v7_06_debug_tools.py`
- Circular import risk: Low if helpers remain pure and do not import `PipelineEngine`.

### Cross-cutting circular import risks

- `brix.debug_tools` imports `PipelineEngine` and `_RenderedStep`.
- `brix.resilience` imports `_RenderedStep`.
- `brix.triggers.runners`, `brix.api`, `brix.cli`, `brix.testing`, and MCP handlers import `PipelineEngine`.
- If `engine.py` becomes a thin façade, it should continue re-exporting moved symbols from stable names to avoid churn and circular import accidents.

---

## 9. Proposed Architecture

### Codex Proposal (4-file split)

Goal: split by responsibility without changing the public import surface of `brix.engine`.

#### `engine.py` (thin façade)

What stays: `LEGACY_ALIASES`, `_VALIDATE_CONFIG_TOP_LEVEL_FIELDS`, public façade `PipelineEngine`, re-exports for moved underscore symbols still imported elsewhere.

Exact method list to keep in `PipelineEngine`: `__init__`, `register_runner`, `run`.

Optional thin wrapper methods to keep for compatibility but delegate:
`_apply_profile`, `_apply_brick_defaults`, `_resolve_runner`, `_resolve_step_credentials`, `_step_credentials_context`, `_should_persist`, `_context_snapshot`, `_capture_environment`, `_persist_step_output`, `_write_context_snapshot`, `_wait_for_breakpoint_resume`, `_ensure_step_requirements`, `_execute_with_retry`, `_chunk_items`, `_is_run_cancelled`, `_run_foreach_sequential`, `_run_foreach_parallel`, `_detect_dag_mode`, `_toposort_steps`, `_run_dag`, `_build_foreach_result`.

Recommendation: keep these method names on `PipelineEngine` as delegation wrappers in phase 1, even after moving implementations. That minimizes downstream breakage.

#### `engine_step.py` (shared primitives, ~500 lines)

Purpose: shared step-execution primitives used by both sequential and DAG paths.

Contents:
- `_step_config_dict`, `_extract_brick_default_values`, `_redact_secret_values`, `_extract_step_cost`, `_RenderedStep`
- `apply_profile(engine, step) -> Step`
- `apply_brick_defaults(engine, step) -> Step`
- `resolve_runner(engine, step_type, jinja_ctx=None) -> BaseRunner | None`
- `resolve_step_credentials(engine, step) -> dict[str, Any]`
- `step_credentials_context(engine, context, step)`
- `should_persist(step) -> bool`
- `context_snapshot(context) -> dict`
- `persist_step_output(engine, run_id, step, result, rendered_params, context, db=None) -> None`
- `write_context_snapshot(engine, context) -> None`
- `wait_for_breakpoint_resume(engine, context, step_id) -> None`
- `ensure_step_requirements(step) -> str | None`
- `execute_with_retry(engine, runner, rendered_step, context, step, pipeline) -> dict`
- `is_run_cancelled(context) -> bool`

`EngineRuntimeView` protocol (required attributes): `loader`, `progress`, `_runners`, `_brick_registry`, `_strict_bricks`, `_current_pipeline_name`, `_deprecation_db`, `_deprecation_warnings`, `_run_db`.

#### `engine_sequential.py` (sequential + foreach, ~200 lines)

Contents:
- `chunk_items(items, batch_size) -> list[list]`
- `build_foreach_result(results, step, pipeline) -> dict`
- `run_foreach_sequential(engine, step, items, context, pipeline) -> dict`
- `run_foreach_parallel(engine, step, items, context, pipeline) -> dict`
- `run_sequential(engine, pipeline, context, step_statuses, dry_run_steps, history, start_time, keep_workdir) -> SequentialRunState`

`SequentialRunState` dataclass: `pipeline_aborted: bool`, `last_output: Any`, `stop_step_success: bool | None`, `total_cost_usd: float`.

#### `engine_dag.py` (DAG scheduler, ~250 lines)

Contents:
- `detect_dag_mode(steps) -> bool`
- `toposort_steps(steps) -> list[Step]`
- `run_dag(engine, pipeline, context, step_statuses, dry_run_steps) -> DagRunState`

`DagRunState` dataclass: `pipeline_aborted: bool`, `last_output: Any`, `stop_step_success: bool | None`.

`engine_dag.py` must call shared functions from `engine_step.py` instead of duplicating logic. That is where parity gaps should be fixed.

### Shared types / dataclasses

Recommended additions (applies to both proposals):

- `ExecutionTelemetry`: `started_at: str | None`, `ended_at: str | None`, `duration: float`, `resource_usage: dict[str, Any] | None`
- `StepExecutionOutcome`: `result: dict`, `rendered_params: dict`, `rendered_step: _RenderedStep`, `duration: float`, `started_at: str | None`, `ended_at: str | None`
- `LoopControl`: `pipeline_aborted: bool`, `stop_step_success: bool | None`, `last_output: Any`
- `RunArtifacts`: `step_statuses: dict[str, StepStatus]`, `total_cost_usd: float`, `deprecation_warnings: list[str]`

These dataclasses reduce tuple/closure coupling and make the sequential and DAG executors less state-fragile.

### Explore-Agent Alternative: `StepExecutor` Class

The Explore-Agent proposed a `StepExecutor` class as an alternative to the module-level functions approach:

```python
class StepExecutor:
    def __init__(self, engine: PipelineEngine):
        self.engine = engine

    async def execute_step(
        self,
        step: Step,
        context: PipelineContext,
        pipeline: Pipeline,
        step_statuses: dict,
        done_events: dict | None = None,  # None in sequential mode
        step_ok: dict | None = None,       # None in sequential mode
    ) -> StepExecutionOutcome:
        # handles: profile, brick defaults, runner resolution, validate_config,
        #          pin mock, test mode, cache, circuit breaker, rate limit,
        #          breakpoint, execution, telemetry, persistence

    async def execute_foreach(
        self,
        step: Step,
        context: PipelineContext,
        pipeline: Pipeline,
    ) -> dict:
        # handles: foreach resolution, parallel/sequential/batch dispatch,
        #          checkpointing, progress
```

**Codex approach vs Explore-Agent approach:**

| Dimension | Codex (module functions) | Explore-Agent (StepExecutor class) |
|---|---|---|
| State passing | Engine instance passed as first arg | Engine instance stored once in `self.engine` |
| Testability | Each function independently mockable | Easier to mock entire executor; harder to unit-test individual phases |
| Extensibility | Add new function; wire into callers | Subclass `StepExecutor` for custom behavior |
| DAG event integration | `done_events`/`step_ok` passed as optional params | Same; `None` for sequential mode |
| Circular import risk | Lower: thin functions, no class state | Similar if class lives in `engine_step.py` |
| Granularity | Each phase is a separate function | Phases are private methods on a class |

---

## 10. Migration Plan (detailed steps)

### Step 1. Freeze the public surface first

- What to move: Nothing yet.
- What to update: Add or confirm re-export expectations in `brix.engine` for `PipelineEngine`, `LEGACY_ALIASES`, `_RenderedStep`, `_VALIDATE_CONFIG_TOP_LEVEL_FIELDS`, `_extract_step_cost`, `_redact_secret_values`.
- How to verify: `test_brick_first_engine.py`, `test_engine_validate_config_allowlist.py`, `test_v6_llm_cost_tracking.py`, `test_secret_variables.py`
- Rollback: No structural move yet; revert the new compatibility assertions only.

### Step 2. Extract pure helpers into `engine_step.py`

- What to move: `_step_config_dict`, `_extract_brick_default_values`, `_redact_secret_values`, `_extract_step_cost`, `_RenderedStep`
- What to update: `engine.py` re-imports and re-exports these symbols. Keep old names alive from `brix.engine`.
- How to verify: `test_python_jinja_rendering.py`, `test_intra_step_progress.py`, `test_secret_variables.py`, `test_v6_llm_cost_tracking.py`
- Rollback: Restore definitions into `engine.py` and remove re-export imports.

### Step 3. Extract shared debug/persistence helpers

- What to move: `_should_persist`, `_context_snapshot`, `_capture_environment`, `_persist_step_output`, `_write_context_snapshot`, `_wait_for_breakpoint_resume`
- What to update: `PipelineEngine` keeps delegating wrapper methods so tests and callers do not notice the move.
- How to verify: `test_v7_04_execution_data.py`, `test_v7_05_run_diff_mcp_trace_env.py`, `test_v7_06_debug_tools.py`, `test_v7_07_resource_monitoring.py`
- Rollback: Move helper bodies back into `PipelineEngine`.

### Step 4. Extract runner/profile/credential resolution primitives

- What to move: `_apply_profile`, `_apply_brick_defaults`, `_resolve_runner`, `_resolve_step_credentials`, `_step_credentials_context`
- What to update: Keep `PipelineEngine` methods delegating to shared functions. Ensure `LEGACY_ALIASES` remains imported from `brix.engine`.
- How to verify: `test_brick_composition.py`, `test_brick_first_engine.py`, `test_deprecation_enforcement.py`, `test_imp_engine.py`
- Rollback: Inline these methods back into `PipelineEngine`.

### Step 5. Extract dependency/cancellation/retry primitives

- What to move: `_ensure_step_requirements`, `_execute_with_retry`, `_is_run_cancelled`
- What to update: Delegating methods on `PipelineEngine`. Shared executor modules call these helpers.
- How to verify: `test_self_healing.py`, `test_v6_retry_profiles.py`, `test_v6_cancel_run.py`
- Rollback: Re-inline helper implementations into `PipelineEngine`.

### Step 6. Extract foreach logic into `engine_sequential.py`

- What to move: `_chunk_items`, `_run_foreach_sequential`, `_run_foreach_parallel`, `_build_foreach_result`
- What to update: `PipelineEngine` wrappers delegate to extracted implementation.
- How to verify: `test_flow_pipeline_config_bug.py`, `test_progress.py`, `test_v6_cancel_run.py`, `test_engine.py`
- Rollback: Restore foreach helpers inside `PipelineEngine`.

### Step 7. Extract sequential loop orchestration

- What to move: The body of the non-DAG `for step in pipeline.steps` loop plus its direct supporting state into `run_sequential(...)`
- What to update: `PipelineEngine.run()` becomes "preflight -> choose path -> finalize". Introduce `SequentialRunState` return object.
- How to verify: `test_engine.py`, `test_pin_mock.py`, `test_step_cache.py`, `test_run_persistence.py`, `test_v7_07_resource_monitoring.py`, `test_smoke_regression.py`
- Rollback: Paste the orchestration body back into `run()`.

### Step 8. Extract DAG scheduler into `engine_dag.py` without changing behavior yet

- What to move: `_detect_dag_mode`, `_toposort_steps`, `_run_dag`
- What to update: `PipelineEngine.run()` calls extracted `run_dag(...)`. `PipelineEngine` keeps wrapper methods for compatibility.
- How to verify: `test_dag_execution.py`, `test_dag_feature_parity.py`, `test_v8_compositor_mode.py`
- Rollback: Move DAG logic back into `engine.py`.

### Step 9. Close the existing DAG parity gaps

- What to move: No new move. This is behavior alignment work inside `engine_dag.py`.
- What to update: Add shared validation call, context snapshot writing, progress persistence, resource measurement, step-output persistence, step-execution persistence, cost tracking, cancellation polling, and saga compensation to DAG. Decide whether `foreach` in DAG is forbidden explicitly or supported fully.
- How to verify: Existing DAG tests plus new tests covering each of the 9 parity gaps (Section 4a). Regression runs of `test_v7_04_execution_data.py`, `test_v7_05_run_diff_mcp_trace_env.py`, `test_v7_07_resource_monitoring.py`, `test_v6_llm_cost_tracking.py`, `test_resilience.py`
- Rollback: Revert only the parity changes while keeping the module split if needed.

### Step 10. Extract run finalization into a dedicated helper or module

- What to move: The `finally` block logic that computes `final_result`, `all_ok`, cleanup, `history.record_finish`, cancellation handling, trigger-state updates, and alerting.
- What to update: `PipelineEngine.run()` passes a compact `RunArtifacts` structure into `finalize_run(...)`
- How to verify: `test_run_persistence.py`, `test_alerting.py`, `test_trigger_chaining.py`, `test_triggers.py`, `test_smoke_regression.py`
- Rollback: Restore finalization block inline in `run()`.

### Step 11. Remove dead duplication and keep compatibility wrappers

- What to move: Replace remaining duplicated logic between sequential and DAG with shared primitives from `engine_step.py`.
- What to update: Keep wrapper methods on `PipelineEngine` for one release cycle.
- How to verify: Full engine-related test subset listed in this document.
- Rollback: Re-enable wrappers that directly contain old logic.

### Step 12. Final verification pass

- What to move: Nothing. Verification only.
- Minimum high-signal suite: `test_engine.py`, `test_dag_execution.py`, `test_dag_feature_parity.py`, `test_brick_composition.py`, `test_deprecation_enforcement.py`, `test_self_healing.py`, `test_v6_retry_profiles.py`, `test_v7_04_execution_data.py`, `test_v7_05_run_diff_mcp_trace_env.py`, `test_v7_06_debug_tools.py`, `test_v7_07_resource_monitoring.py`, `test_resilience.py`, `test_run_persistence.py`, `test_smoke_regression.py`
- Rollback: Revert the refactor branch to the last green commit if any path-parity tests regress.

---

## 11. Consolidated Recommendations

This section synthesizes the Codex analysis and Explore-Agent analysis into a single recommended approach.

### Summary of Findings (Both Analyses Agree)

`engine.py` is currently three things at once:
1. The public execution façade (`PipelineEngine`, `LEGACY_ALIASES`, `_RenderedStep`)
2. The shared step-execution policy layer (render, resolve, validate, execute, persist)
3. Two partially duplicated schedulers (sequential loop + DAG event-driven)

Both analyses independently confirmed 2697 lines and the same set of public symbols.

### The Central Technical Risk

The single biggest technical risk is not the file split. It is the fact that the DAG path is behaviorally incomplete relative to the sequential path. Both analyses independently reached this conclusion:

- Codex: "the DAG path is not just a scheduling variant. It omits validation, foreach, cancellation polling, context snapshotting, progress DB persistence, resource measurement, step-output persistence, step-execution persistence, LLM cost tracking, and saga compensation."
- Explore-Agent: enumerated exactly 9 missing capabilities with specific block references (Section 4a).

If the refactor simply relocates code without first making those gaps visible, the codebase will remain easier to read but still semantically inconsistent.

### Where the Two Analyses Agree

1. Split into 4 files: `engine.py` (façade), `engine_step.py` (shared primitives), `engine_sequential.py` (sequential + foreach), `engine_dag.py` (DAG).
2. Public import surface (`brix.engine`) must not change — re-export everything moved.
3. Extract pure helpers first; move orchestrators second.
4. Close DAG parity gaps as part of Step 9, not as a deferred post-refactor task.
5. `SequentialRunState` and `DagRunState` dataclasses reduce closure/nonlocal coupling risk.
6. The Codex Step 1-12 migration sequence is sound.

### Where the Two Analyses Differ

**Disagreement 1: Class vs. functions for step execution**

- Codex: module-level functions in `engine_step.py`, each taking `engine` as a first argument.
- Explore-Agent: a `StepExecutor` class with `execute_step()` and `execute_foreach()` methods.

Recommendation: Use the `StepExecutor` class only if DAG parity gaps are being fixed simultaneously (Step 9 is in scope). The class API is cleaner for the DAG path because `execute_step()` can be called from both schedulers with optional `done_events`/`step_ok` parameters. If parity is deferred, use module functions — simpler migration, easier rollback.

**Disagreement 2: File size estimates**

- Codex: `engine_step.py` ~500 lines, `engine_sequential.py` ~200 lines, `engine_dag.py` ~250 lines.
- Explore-Agent: `engine.py` façade ~400 lines, `engine_step.py` ~500 lines, `engine_sequential.py` ~200 lines, `engine_dag.py` ~250 lines.

The ~400 line estimate for the façade from the Explore-Agent is more realistic: it accounts for delegation wrapper methods that must remain on `PipelineEngine` during phase 1.

Recommendation: Accept the Explore-Agent estimate for the façade. Total target after split: ~1350 lines across 4 files.

**Disagreement 3: Whether foreach in DAG should be fixed**

- Codex: "decide whether `foreach` in DAG is forbidden explicitly or supported fully" — defers the decision.
- Explore-Agent: treats foreach-in-DAG as a parity gap that should be fixed.

Recommendation: Make the decision explicit. If `foreach` steps and `depends_on` steps are mutually exclusive, add a validator that rejects the combination and document it. If both are valid, implement foreach support in the DAG path using the existing `_run_foreach_sequential`/`_run_foreach_parallel` functions from `engine_step.py`. Do not leave it as a silent no-op.

### Final Recommended Migration Sequence

**Phase 1 — Stabilize (no behavioral change, no parity fixes):**
1. Confirm public surface with import assertions (Step 1)
2. Extract `_RenderedStep`, cost, redaction, config helpers into `engine_step.py` (Step 2)
3. Extract debug/persistence helpers into `engine_step.py` (Step 3)
4. Extract runner resolution, profile, credentials into `engine_step.py` (Step 4)
5. Extract dependency, retry, cancel helpers into `engine_step.py` (Step 5)

**Phase 2 — Split schedulers:**
6. Extract foreach into `engine_sequential.py` (Step 6)
7. Extract sequential loop into `engine_sequential.py` (Step 7)
8. Extract DAG into `engine_dag.py` without behavior change (Step 8)

**Phase 3 — Close gaps:**
9. Fix all 9 DAG parity gaps using shared primitives from `engine_step.py` (Step 9)
10. Decide and implement or ban foreach-in-DAG explicitly
11. Extract run finalization (Step 10)
12. Remove duplication, clean compatibility wrappers (Step 11)
13. Full verification (Step 12)

Each phase is independently committable and independently rollbackable. Phase 2 should not begin until Phase 1 tests are green. Phase 3 should not begin until Phase 2 tests are green.

### Non-Negotiable Rules for Implementation

1. `from brix.engine import PipelineEngine` must continue to work after every step.
2. `from brix.engine import LEGACY_ALIASES` must continue to work after every step.
3. `from brix.engine import _RenderedStep` must continue to work after every step (re-export from new location).
4. Every DAG parity gap must have a test before the gap is considered closed.
5. `gather(return_exceptions=True)` in the DAG must not silently absorb parity-fix failures — add explicit exception logging at the gather level.
6. No step in the migration plan may both move code and fix behavior simultaneously. Structure moves and behavior changes must be separate commits.
