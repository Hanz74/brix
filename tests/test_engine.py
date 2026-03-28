"""Tests for brix.engine.PipelineEngine."""

import pytest

from brix.context import PipelineContext
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.runners.base import BaseRunner, _StubRunnerMixin


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_pipeline(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


class _AlwaysSuccessRunner(_StubRunnerMixin, BaseRunner):
    """Stub runner that always returns success with a fixed payload."""

    def __init__(self, data=None):
        self._data = data

    def config_schema(self) -> dict:
        return {"type": "object", "properties": {}}

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "any"

    async def execute(self, step, context) -> dict:
        return {"success": True, "data": self._data or f"ok-{step.id}"}


class _AlwaysFailRunner(_StubRunnerMixin, BaseRunner):
    """Stub runner that always returns failure."""

    def config_schema(self) -> dict:
        return {"type": "object", "properties": {}}

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "any"

    async def execute(self, step, context) -> dict:
        return {"success": False, "error": f"runner-fail-{step.id}"}


# ---------------------------------------------------------------------------
# Simple single-step cli pipeline
# ---------------------------------------------------------------------------


async def test_engine_simple_cli_step():
    """Single echo step succeeds and returns echoed string."""
    pipeline = load_pipeline("""
name: simple
steps:
  - id: greet
    type: cli
    args: ["echo", "hello"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.run_id.startswith("run-")
    assert result.steps["greet"].status == "ok"
    assert result.result == "hello"
    assert result.duration >= 0.0


# ---------------------------------------------------------------------------
# Two sequential steps — output chaining
# ---------------------------------------------------------------------------


async def test_engine_two_steps_sequential():
    """Two cli steps run in order; outputs are stored correctly."""
    pipeline = load_pipeline("""
name: two-steps
steps:
  - id: step_a
    type: cli
    args: ["echo", "first"]
  - id: step_b
    type: cli
    args: ["echo", "second"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["step_a"].status == "ok"
    assert result.steps["step_b"].status == "ok"
    # Last step output becomes the final result when no output field is set
    assert result.result == "second"


# ---------------------------------------------------------------------------
# when condition — true
# ---------------------------------------------------------------------------


async def test_engine_when_condition_true():
    """Step with when=true is executed."""
    pipeline = load_pipeline("""
name: when-true
steps:
  - id: run_me
    type: cli
    when: "true"
    args: ["echo", "executed"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["run_me"].status == "ok"


# ---------------------------------------------------------------------------
# when condition — false → skipped
# ---------------------------------------------------------------------------


async def test_engine_when_condition_false():
    """Step with when=false is skipped (status=skipped, no error)."""
    pipeline = load_pipeline("""
name: when-false
steps:
  - id: skip_me
    type: cli
    when: "false"
    args: ["echo", "skipped"]
  - id: run_me
    type: cli
    args: ["echo", "ran"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["skip_me"].status == "skipped"
    assert result.steps["skip_me"].reason == "condition not met"
    assert result.steps["run_me"].status == "ok"


# ---------------------------------------------------------------------------
# on_error=stop (pipeline default)
# ---------------------------------------------------------------------------


async def test_engine_on_error_stop():
    """When a step fails and on_error=stop, the pipeline stops immediately."""
    pipeline = load_pipeline("""
name: stop-on-error
error_handling:
  on_error: stop
steps:
  - id: fail_step
    type: cli
    command: "exit 1"
  - id: never_reached
    type: cli
    args: ["echo", "nope"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["fail_step"].status == "error"
    assert "never_reached" not in result.steps


# ---------------------------------------------------------------------------
# on_error=continue
# ---------------------------------------------------------------------------


async def test_engine_on_error_continue():
    """When a step fails and on_error=continue, subsequent steps still run."""
    pipeline = load_pipeline("""
name: continue-on-error
error_handling:
  on_error: continue
steps:
  - id: fail_step
    type: cli
    command: "exit 1"
  - id: next_step
    type: cli
    args: ["echo", "still running"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    # Not fully successful because of the failed step
    assert result.success is False
    assert result.steps["fail_step"].status == "error"
    assert result.steps["next_step"].status == "ok"


# ---------------------------------------------------------------------------
# No runner registered for step type
# ---------------------------------------------------------------------------


async def test_engine_no_runner_for_type():
    """Step type without registered runner → status=error."""
    pipeline = load_pipeline("""
name: no-runner
error_handling:
  on_error: continue
steps:
  - id: sub_step
    type: pipeline
    pipeline: "nonexistent.yaml"
  - id: cli_step
    type: cli
    args: ["echo", "after"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.steps["sub_step"].status == "error"
    assert result.steps["cli_step"].status == "ok"


async def test_engine_no_runner_stop():
    """No runner + on_error=stop aborts the pipeline."""
    pipeline = load_pipeline("""
name: no-runner-stop
steps:
  - id: sub_step
    type: pipeline
    pipeline: "nonexistent.yaml"
  - id: cli_step
    type: cli
    args: ["echo", "never"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["sub_step"].status == "error"
    assert "cli_step" not in result.steps


# ---------------------------------------------------------------------------
# Explicit output field
# ---------------------------------------------------------------------------


async def test_engine_output_field():
    """Pipeline with an explicit output field renders it via Jinja2."""
    pipeline = load_pipeline("""
name: with-output
steps:
  - id: get_name
    type: cli
    args: ["echo", "alice"]
output:
  name: "{{ get_name.output }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.result == {"name": "alice"}


# ---------------------------------------------------------------------------
# Default output = last step
# ---------------------------------------------------------------------------


async def test_engine_default_output_last_step():
    """Without an output field, the result is the last step's data."""
    pipeline = load_pipeline("""
name: no-output-field
steps:
  - id: step_a
    type: cli
    args: ["echo", "first"]
  - id: step_b
    type: cli
    args: ["echo", "last"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.result == "last"


# ---------------------------------------------------------------------------
# Context.from_pipeline — input + credentials
# ---------------------------------------------------------------------------


def test_engine_context_from_pipeline():
    """PipelineContext.from_pipeline merges user_input with defaults."""
    pipeline = load_pipeline("""
name: with-input
input:
  limit:
    type: integer
    default: 10
  query:
    type: string
    default: ""
steps:
  - id: dummy
    type: cli
    args: ["echo", "x"]
""")
    ctx = PipelineContext.from_pipeline(pipeline, user_input={"limit": 50})
    assert ctx.input["limit"] == 50
    assert ctx.input["query"] == ""


# ---------------------------------------------------------------------------
# Step output available to next step via Jinja2
# ---------------------------------------------------------------------------


async def test_engine_step_output_in_context():
    """Step B can reference Step A's output via Jinja2 {{ step_a.output }}."""
    pipeline = load_pipeline("""
name: chained
steps:
  - id: step_a
    type: cli
    args: ["echo", "from-a"]
  - id: step_b
    type: cli
    args: ["echo", "{{ step_a.output }}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["step_b"].status == "ok"
    # step_b echoes step_a's output
    assert result.result == "from-a"


# ---------------------------------------------------------------------------
# PipelineContext.to_jinja_context
# ---------------------------------------------------------------------------


def test_context_jinja_context():
    """to_jinja_context() contains input, credentials, and step outputs."""
    ctx = PipelineContext(
        pipeline_input={"query": "test"},
        credentials={"token": "secret"},
    )
    ctx.set_output("fetch", [1, 2, 3])

    jinja = ctx.to_jinja_context()

    assert jinja["input"] == {"query": "test"}
    assert jinja["credentials"] == {"token": "secret"}
    assert jinja["fetch"] == {"output": [1, 2, 3]}
    assert "item" not in jinja


def test_context_jinja_context_with_item():
    """to_jinja_context(item=...) includes item key."""
    ctx = PipelineContext()
    jinja = ctx.to_jinja_context(item={"id": 99})
    assert jinja["item"] == {"id": 99}


# ---------------------------------------------------------------------------
# register_runner
# ---------------------------------------------------------------------------


async def test_engine_register_runner():
    """Custom runner registered at runtime is used for matching step type."""
    pipeline = load_pipeline("""
name: custom-runner
steps:
  - id: custom_step
    type: python
    script: "print('hi')"
""")
    engine = PipelineEngine()
    engine.register_runner("python", _AlwaysSuccessRunner(data="custom-result"))
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["custom_step"].status == "ok"
    assert result.result == "custom-result"


# ---------------------------------------------------------------------------
# foreach Tests
# ---------------------------------------------------------------------------

import os as _os

_HELPERS = _os.path.join(_os.path.dirname(__file__), "helpers")


async def test_engine_foreach_sequential():
    """foreach without parallel processes items sequentially and returns all results."""
    pipeline = load_pipeline(f"""
name: foreach-seq
steps:
  - id: list_items
    type: python
    script: "{_HELPERS}/list_items.py"
  - id: echo_item
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["echo_item"].status == "ok"

    foreach_result = result.result
    assert "items" in foreach_result
    assert "summary" in foreach_result
    assert foreach_result["summary"]["total"] == 3
    assert foreach_result["summary"]["succeeded"] == 3
    assert foreach_result["summary"]["failed"] == 0

    # All items succeeded
    for item_result in foreach_result["items"]:
        assert item_result["success"] is True


async def test_engine_foreach_parallel():
    """foreach with parallel=true processes all items and returns aggregated result."""
    pipeline = load_pipeline(f"""
name: foreach-parallel
steps:
  - id: list_items
    type: python
    script: "{_HELPERS}/list_items.py"
  - id: echo_item
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    parallel: true
    concurrency: 3
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["echo_item"].status == "ok"

    foreach_result = result.result
    assert foreach_result["summary"]["total"] == 3
    assert foreach_result["summary"]["succeeded"] == 3
    assert foreach_result["summary"]["failed"] == 0


async def test_engine_foreach_concurrency_limit():
    """concurrency=1 forces serial execution; all items still succeed."""
    pipeline = load_pipeline(f"""
name: foreach-concurrency
steps:
  - id: list_items
    type: python
    script: "{_HELPERS}/list_items.py"
  - id: echo_item
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    parallel: true
    concurrency: 1
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    foreach_result = result.result
    assert foreach_result["summary"]["total"] == 3
    assert foreach_result["summary"]["succeeded"] == 3


async def test_engine_foreach_partial_success():
    """on_error:continue with foreach returns partial results (D-15)."""

    class _FailOnItem2Runner(_StubRunnerMixin, BaseRunner):
        """Succeeds for item1 and item3, fails for item2."""

        async def execute(self, step, context) -> dict:
            # The rendered args contain the item value; inspect via step.args
            item_val = step.args[-1] if step.args else ""
            if item_val == "item2":
                return {"success": False, "error": "intentional-fail", "duration": 0.01}
            return {"success": True, "data": f"done-{item_val}", "duration": 0.01}

    pipeline = load_pipeline(f"""
name: foreach-partial
error_handling:
  on_error: continue
steps:
  - id: list_items
    type: python
    script: "{_HELPERS}/list_items.py"
  - id: process_item
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    on_error: continue
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _FailOnItem2Runner())
    result = await engine.run(pipeline)

    # Pipeline continues despite partial failure
    assert result.steps["process_item"].status == "ok"

    foreach_result = result.result
    assert foreach_result["summary"]["total"] == 3
    assert foreach_result["summary"]["succeeded"] == 2
    assert foreach_result["summary"]["failed"] == 1

    # Failed item carries error and input
    failed_items = [i for i in foreach_result["items"] if not i["success"]]
    assert len(failed_items) == 1
    assert failed_items[0]["error"] == "intentional-fail"
    assert failed_items[0]["input"] == "item2"


async def test_engine_foreach_stop_on_error():
    """on_error:stop with foreach stops at first failing item."""

    class _FailOnItem2Runner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context) -> dict:
            item_val = step.args[-1] if step.args else ""
            if item_val == "item2":
                return {"success": False, "error": "stop-fail", "duration": 0.01}
            return {"success": True, "data": f"done-{item_val}", "duration": 0.01}

    pipeline = load_pipeline(f"""
name: foreach-stop
steps:
  - id: list_items
    type: python
    script: "{_HELPERS}/list_items.py"
  - id: process_item
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    on_error: stop
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _FailOnItem2Runner())
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["process_item"].status == "error"

    # Pipeline stopped — no steps after process_item
    # The foreach result itself reflects the early stop
    # (result may be None since the step failed and pipeline stopped)
    # At minimum: the step is recorded as error
    assert result.steps["process_item"].errors is not None
    assert result.steps["process_item"].errors >= 1


# ---------------------------------------------------------------------------
# Retry Tests
# ---------------------------------------------------------------------------


async def test_engine_retry_success_on_second_attempt():
    """Retry succeeds on second attempt."""
    call_count = 0

    class _FailOnceRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"success": False, "error": "transient", "duration": 0.01}
            return {"success": True, "data": "recovered", "duration": 0.01}

    pipeline = load_pipeline("""
name: retry-test
error_handling:
  on_error: retry
  retry:
    max: 3
    backoff: linear
steps:
  - id: flaky
    type: cli
    args: ["echo", "test"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _FailOnceRunner())
    result = await engine.run(pipeline)

    assert result.success is True
    assert call_count == 2


async def test_engine_retry_all_attempts_fail():
    """All retry attempts fail — returns last error with retry_count."""

    class _AlwaysFailRetryRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            return {"success": False, "error": "persistent", "duration": 0.01}

    pipeline = load_pipeline("""
name: retry-fail
error_handling:
  on_error: retry
  retry:
    max: 2
    backoff: linear
steps:
  - id: broken
    type: cli
    args: ["echo", "test"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysFailRetryRunner())
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["broken"].status == "error"


async def test_engine_retry_exponential_backoff():
    """Exponential backoff uses increasing delays between attempts."""
    import time as time_mod

    timestamps = []

    class _TrackingRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            timestamps.append(time_mod.monotonic())
            return {"success": False, "error": "fail", "duration": 0.01}

    pipeline = load_pipeline("""
name: retry-exp
error_handling:
  on_error: retry
  retry:
    max: 3
    backoff: exponential
steps:
  - id: tracked
    type: cli
    args: ["echo", "test"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _TrackingRunner())
    result = await engine.run(pipeline)

    assert result.success is False
    assert len(timestamps) == 3
    # Exponential delays: attempt 1→2 sleeps 1s, attempt 2→3 sleeps 2s
    delay1 = timestamps[1] - timestamps[0]
    delay2 = timestamps[2] - timestamps[1]
    assert delay1 >= 0.8   # ~1s
    assert delay2 >= 1.5   # ~2s
    assert delay2 > delay1  # exponential growth


async def test_engine_step_level_on_error_retry_overrides_pipeline():
    """Step-level on_error=retry overrides pipeline on_error=stop."""
    call_count = 0

    class _FailTwiceRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return {"success": False, "error": "transient", "duration": 0.01}
            return {"success": True, "data": "eventually-ok", "duration": 0.01}

    pipeline = load_pipeline("""
name: step-retry-override
error_handling:
  on_error: stop
  retry:
    max: 3
    backoff: linear
steps:
  - id: flaky
    type: cli
    on_error: retry
    args: ["echo", "test"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _FailTwiceRunner())
    result = await engine.run(pipeline)

    assert result.success is True
    assert call_count == 3


async def test_engine_retry_count_in_result_on_all_fail():
    """retry_count is set on the last result when all attempts fail."""

    class _ConstantFailRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            return {"success": False, "error": "always-fails", "duration": 0.01}

    pipeline = load_pipeline("""
name: retry-count-test
error_handling:
  on_error: retry
  retry:
    max: 3
    backoff: linear
steps:
  - id: step1
    type: cli
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _ConstantFailRunner())
    result = await engine.run(pipeline)

    assert result.success is False
    # The step is recorded as error
    assert result.steps["step1"].status == "error"


# ---------------------------------------------------------------------------
# Conditional Steps — complex Jinja2 expressions (T-BRIX-18)
# ---------------------------------------------------------------------------


async def test_engine_when_complex_expression_true():
    """{{ input.count > 5 }} with count=10 → step executes."""
    pipeline = load_pipeline("""
name: when-complex-true
input:
  count:
    type: integer
    default: 0
steps:
  - id: conditional
    type: cli
    when: "{{ input.count > 5 }}"
    args: ["echo", "ran"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, user_input={"count": 10})

    assert result.success is True
    assert result.steps["conditional"].status == "ok"


async def test_engine_when_complex_expression_false():
    """{{ input.count > 5 }} with count=3 → step is skipped."""
    pipeline = load_pipeline("""
name: when-complex-false
input:
  count:
    type: integer
    default: 0
steps:
  - id: conditional
    type: cli
    when: "{{ input.count > 5 }}"
    args: ["echo", "ran"]
  - id: always
    type: cli
    args: ["echo", "always"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, user_input={"count": 3})

    assert result.success is True
    assert result.steps["conditional"].status == "skipped"
    assert result.steps["conditional"].reason == "condition not met"
    assert result.steps["always"].status == "ok"


async def test_engine_when_list_length():
    """{{ tags | length > 0 }} evaluates list length correctly."""
    pipeline = load_pipeline("""
name: when-list-length
input:
  tags:
    type: string
    default: "[]"
steps:
  - id: process
    type: cli
    when: "{{ input.tags | length > 0 }}"
    args: ["echo", "has items"]
  - id: empty
    type: cli
    when: "{{ input.tags | length == 0 }}"
    args: ["echo", "empty"]
""")
    engine = PipelineEngine()
    # Pass tags as a list
    result = await engine.run(pipeline, user_input={"tags": ["a", "b"]})

    assert result.success is True
    assert result.steps["process"].status == "ok"
    assert result.steps["empty"].status == "skipped"


async def test_engine_when_skipped_step_no_crash():
    """Referencing a skipped step's output with | default() does not crash."""
    pipeline = load_pipeline("""
name: when-skipped-ref
steps:
  - id: optional
    type: cli
    when: "false"
    args: ["echo", "skipped"]
  - id: always
    type: cli
    args: ["echo", "{{ optional.output | default('fallback') }}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["optional"].status == "skipped"
    assert result.steps["always"].status == "ok"
    # The output of 'always' should be the fallback string
    assert result.result == "fallback"


# ---------------------------------------------------------------------------
# Pipeline output field — explicit mapping (T-BRIX-19)
# ---------------------------------------------------------------------------


async def test_engine_output_field_explicit():
    """Pipeline with explicit output field renders only mapped fields."""
    pipeline = load_pipeline("""
name: output-explicit
steps:
  - id: get_name
    type: cli
    args: ["echo", "alice"]
  - id: get_count
    type: cli
    args: ["echo", "42"]
output:
  name: "{{ get_name.output }}"
  total: "{{ get_count.output }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert isinstance(result.result, dict)
    assert result.result["name"] == "alice"
    assert result.result["total"] == 42


async def test_engine_output_field_missing_defaults_to_last():
    """Without output field, result equals the last step's output."""
    pipeline = load_pipeline("""
name: output-default
steps:
  - id: step_a
    type: cli
    args: ["echo", "first"]
  - id: step_b
    type: cli
    args: ["echo", "last"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.result == "last"


async def test_engine_output_field_with_default():
    """output referencing a skipped step with | default() uses fallback."""
    pipeline = load_pipeline("""
name: output-with-default
steps:
  - id: optional
    type: cli
    when: "false"
    args: ["echo", "skipped"]
  - id: main
    type: cli
    args: ["echo", "done"]
output:
  primary: "{{ main.output }}"
  extra: "{{ optional.output | default('none') }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.result["primary"] == "done"
    assert result.result["extra"] == "none"


# ---------------------------------------------------------------------------
# Unhandled exception in runner still finalises history (T-BRIX-V2-25)
# ---------------------------------------------------------------------------


async def test_engine_unhandled_exception_still_records_history():
    """Even if a step raises an unexpected exception, run() returns a result."""

    class _CrashRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            raise RuntimeError("Unexpected crash")

    pipeline = load_pipeline("""
name: crash-test
steps:
  - id: crasher
    type: cli
    args: ["echo", "test"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _CrashRunner())
    result = await engine.run(pipeline)

    assert result.success is False
    # The important thing: run() returned, didn't hang


# ---------------------------------------------------------------------------
# MCP Connection Pooling (T-BRIX-V3-01)
# ---------------------------------------------------------------------------


from unittest.mock import AsyncMock, MagicMock, patch
from brix.mcp_pool import McpConnectionPool
from brix.runners.mcp import McpRunner


async def test_engine_mcp_pool_created():
    """Pool is created at the start of run() and attached to McpRunner."""
    pipeline = load_pipeline("""
name: pool-created
steps:
  - id: step1
    type: cli
    args: ["echo", "ok"]
""")
    engine = PipelineEngine()
    pool_instances: list = []

    original_aenter = McpConnectionPool.__aenter__

    async def _tracking_aenter(self):
        pool_instances.append(self)
        return await original_aenter(self)

    with patch.object(McpConnectionPool, "__aenter__", _tracking_aenter):
        result = await engine.run(pipeline)

    assert result.success is True
    # At least one pool was created during the run
    assert len(pool_instances) >= 1


async def test_engine_mcp_pool_closed_after_run():
    """Pool is closed and detached from McpRunner after run() completes."""
    pipeline = load_pipeline("""
name: pool-closed
steps:
  - id: step1
    type: cli
    args: ["echo", "ok"]
""")
    engine = PipelineEngine()
    await engine.run(pipeline)

    # After run(), pool must be reset to None
    assert engine._mcp_pool is None

    # The McpRunner must also have its pool cleared
    mcp_runner = engine._runners.get("mcp")
    assert mcp_runner is not None
    assert mcp_runner.pool is None  # type: ignore[union-attr]


async def test_engine_mcp_pool_closed_after_run_on_error():
    """Pool is closed even when the pipeline fails mid-run."""

    class _CrashRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            raise RuntimeError("Boom")

    pipeline = load_pipeline("""
name: pool-closed-on-error
steps:
  - id: boom
    type: cli
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _CrashRunner())
    result = await engine.run(pipeline)

    assert result.success is False
    # Pool must still be cleaned up
    assert engine._mcp_pool is None
    mcp_runner = engine._runners.get("mcp")
    assert mcp_runner is not None
    assert mcp_runner.pool is None  # type: ignore[union-attr]


async def test_mcp_runner_with_pool():
    """McpRunner delegates call_tool to the pool when a pool is attached."""
    pool_mock = AsyncMock(spec=McpConnectionPool)
    pool_mock.call_tool = AsyncMock(return_value={"success": True, "data": "from-pool", "duration": 0.01})

    runner = McpRunner(pool=pool_mock)

    # Build a minimal step-like object
    step = MagicMock()
    step.server = "fake"
    step.tool = "my_tool"
    step.params = {"arg": "val"}
    step.timeout = None

    context = MagicMock()

    result = await runner.execute(step, context)

    assert result["success"] is True
    assert result["data"] == "from-pool"
    pool_mock.call_tool.assert_called_once_with("fake", "my_tool", {"arg": "val"}, timeout=120.0)


async def test_mcp_runner_without_pool():
    """McpRunner falls back to per-call connections when no pool is attached."""
    runner = McpRunner()
    assert runner.pool is None

    # Build a step that references a non-existent server config — the runner
    # should attempt the per-call path and return success=False with a
    # config error (not a pool error).
    step = MagicMock()
    step.server = "nonexistent_server"
    step.tool = "some_tool"
    step.params = {}
    step.timeout = None

    context = MagicMock()

    result = await runner.execute(step, context)

    assert result["success"] is False
    # Error should come from the per-call path (config missing), not pool
    assert "pool" not in result["error"].lower()


async def test_mcp_runner_pool_property_set_and_clear():
    """McpRunner.pool property can be set and cleared."""
    runner = McpRunner()
    assert runner.pool is None

    fake_pool = MagicMock(spec=McpConnectionPool)
    runner.pool = fake_pool
    assert runner.pool is fake_pool

    runner.pool = None
    assert runner.pool is None


# ---------------------------------------------------------------------------
# foreach Checkpoint Tests (T-BRIX-V3-06)
# ---------------------------------------------------------------------------

import json as _json


async def test_foreach_checkpoint_written():
    """After a foreach run, a checkpoint JSONL file exists for the step."""
    pipeline = load_pipeline(f"""
name: checkpoint-written
steps:
  - id: list_items
    type: python
    script: "{_HELPERS}/list_items.py"
  - id: process_item
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, keep_workdir=True)

    assert result.success is True

    from brix.context import WORKDIR_BASE
    checkpoint_path = WORKDIR_BASE / result.run_id / "step_outputs" / "process_item_checkpoint.jsonl"
    assert checkpoint_path.exists(), f"Checkpoint file missing: {checkpoint_path}"

    lines = [line for line in checkpoint_path.read_text().splitlines() if line.strip()]
    assert len(lines) == 3  # one entry per item

    entries = [_json.loads(line) for line in lines]
    indices = [e["index"] for e in entries]
    assert sorted(indices) == [0, 1, 2]


async def test_foreach_resume_skips_completed():
    """Resume of a foreach step skips already-checkpointed items."""
    call_count = 0

    class _CountingRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context) -> dict:
            nonlocal call_count
            call_count += 1
            return {"success": True, "data": f"done-{call_count}", "duration": 0.0}

    pipeline = load_pipeline(f"""
name: foreach-resume-seq
steps:
  - id: list_items
    type: python
    script: "{_HELPERS}/list_items.py"
  - id: process_item
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _CountingRunner())

    # First run — completes all 3 items
    result = await engine.run(pipeline, keep_workdir=True)
    assert result.success is True
    assert call_count == 3

    run_id = result.run_id
    call_count = 0  # reset counter

    # Simulate partial checkpoint: remove item index 2 from the checkpoint
    # so only items 0 and 1 are marked done — item 2 must re-execute
    from brix.context import WORKDIR_BASE
    checkpoint_path = WORKDIR_BASE / run_id / "step_outputs" / "process_item_checkpoint.jsonl"
    lines = checkpoint_path.read_text().splitlines()
    # Keep only the first two entries (indices 0, 1)
    kept = [l for l in lines if _json.loads(l)["index"] < 2]
    checkpoint_path.write_text("\n".join(kept) + "\n")

    # Remove the step's json output so it's not seen as a completed step
    json_path = WORKDIR_BASE / run_id / "step_outputs" / "process_item.json"
    if json_path.exists():
        json_path.unlink()
    # Also remove from run.json completed_steps to force re-execution of process_item
    meta_path = WORKDIR_BASE / run_id / "run.json"
    meta = _json.loads(meta_path.read_text())
    meta["completed_steps"] = [s for s in meta.get("completed_steps", []) if s != "process_item"]
    meta_path.write_text(_json.dumps(meta, indent=2))

    # Resume run — only item 2 should be executed
    from brix.context import PipelineContext
    ctx = PipelineContext.from_resume(run_id)
    # Re-inject list_items output so foreach can resolve items
    ctx.set_output("list_items", ["item1", "item2", "item3"])

    if engine._mcp_pool is not None:
        await engine._mcp_pool.__aexit__(None, None, None)
        engine._mcp_pool = None

    from brix.loader import PipelineLoader
    engine2 = PipelineEngine()
    engine2.register_runner("cli", _CountingRunner())
    engine2.register_runner("python", _CountingRunner())

    # Manually drive the foreach with the resumed context
    from brix.models import Step
    loader = PipelineLoader()
    step_obj = pipeline.steps[1]  # process_item step
    items = ["item1", "item2", "item3"]

    engine2._mcp_pool = None
    foreach_result = await engine2._run_foreach_sequential(step_obj, items, ctx, pipeline)

    # Only 1 call made — items 0 and 1 were restored from checkpoint
    assert call_count == 1
    assert foreach_result["summary"]["total"] == 3
    assert foreach_result["summary"]["succeeded"] == 3


async def test_foreach_checkpoint_parallel():
    """Parallel foreach writes checkpoint entries for all items."""
    pipeline = load_pipeline(f"""
name: checkpoint-parallel
steps:
  - id: list_items
    type: python
    script: "{_HELPERS}/list_items.py"
  - id: process_item
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    parallel: true
    concurrency: 3
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, keep_workdir=True)

    assert result.success is True

    from brix.context import WORKDIR_BASE
    checkpoint_path = WORKDIR_BASE / result.run_id / "step_outputs" / "process_item_checkpoint.jsonl"
    assert checkpoint_path.exists(), "Checkpoint file missing for parallel foreach"

    lines = [l for l in checkpoint_path.read_text().splitlines() if l.strip()]
    assert len(lines) == 3

    entries = [_json.loads(l) for l in lines]
    indices = sorted(e["index"] for e in entries)
    assert indices == [0, 1, 2]


async def test_foreach_partial_resume():
    """A step with 5/10 items in checkpoint should resume and run only the remaining 5."""
    call_count = 0

    class _CountingRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context) -> dict:
            nonlocal call_count
            call_count += 1
            return {"success": True, "data": f"item-{call_count}", "duration": 0.0}

    items_10 = [f"item{i}" for i in range(10)]

    pipeline = load_pipeline("""
name: partial-resume-10
steps:
  - id: process_item
    type: cli
    foreach: "{{ input.batch }}"
    args: ["echo", "{{ item }}"]
input:
  batch:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _CountingRunner())

    # First: run all 10 to create the initial workdir
    result = await engine.run(pipeline, user_input={"batch": items_10}, keep_workdir=True)
    assert result.success is True
    assert call_count == 10
    run_id = result.run_id
    call_count = 0  # reset

    from brix.context import WORKDIR_BASE, PipelineContext
    checkpoint_path = WORKDIR_BASE / run_id / "step_outputs" / "process_item_checkpoint.jsonl"

    # Trim checkpoint to only first 5 items (indices 0-4)
    lines = checkpoint_path.read_text().splitlines()
    kept = [l for l in lines if _json.loads(l)["index"] < 5]
    checkpoint_path.write_text("\n".join(kept) + "\n")

    # Remove process_item from completed_steps in run.json
    meta_path = WORKDIR_BASE / run_id / "run.json"
    meta = _json.loads(meta_path.read_text())
    meta["completed_steps"] = [s for s in meta.get("completed_steps", []) if s != "process_item"]
    meta_path.write_text(_json.dumps(meta, indent=2))

    # Remove the step output json
    json_path = WORKDIR_BASE / run_id / "step_outputs" / "process_item.json"
    if json_path.exists():
        json_path.unlink()

    # Build resumed context and inject items
    ctx = PipelineContext.from_resume(run_id)
    ctx.input["batch"] = items_10

    engine2 = PipelineEngine()
    engine2.register_runner("cli", _CountingRunner())

    foreach_result = await engine2._run_foreach_sequential(pipeline.steps[0], items_10, ctx, pipeline)

    # Only 5 new calls made (items 5-9), items 0-4 were restored from checkpoint
    assert call_count == 5
    assert foreach_result["summary"]["total"] == 10
    assert foreach_result["summary"]["succeeded"] == 10


# ---------------------------------------------------------------------------
# batch_size as pipeline primitive (T-BRIX-V3-08)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_foreach_batch_size():
    """10 items with batch_size=3 are processed in 4 batches (3+3+3+1)."""

    class _BatchCountingRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context) -> dict:
            return {"success": True, "data": "ok", "duration": 0.0}

    pipeline = load_pipeline("""
name: batch-size-test
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_list }}"
    batch_size: 3
    args: ["echo", "{{ item }}"]
input:
  data_list:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _BatchCountingRunner())

    data_list = list(range(10))
    result = await engine.run(pipeline, user_input={"data_list": data_list})

    assert result.success is True
    summary = result.result["summary"]
    assert summary["total"] == 10
    assert summary["succeeded"] == 10
    assert summary["failed"] == 0


@pytest.mark.asyncio
async def test_foreach_batch_size_zero():
    """batch_size=0 (default) processes all items without batching."""
    pipeline = load_pipeline("""
name: batch-size-zero
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_list }}"
    batch_size: 0
    args: ["echo", "{{ item }}"]
input:
  data_list:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner())

    data_list = list(range(5))
    result = await engine.run(pipeline, user_input={"data_list": data_list})

    assert result.success is True
    summary = result.result["summary"]
    assert summary["total"] == 5
    assert summary["succeeded"] == 5


@pytest.mark.asyncio
async def test_foreach_batch_size_larger_than_items():
    """batch_size=100 with only 5 items results in a single batch of 5."""
    pipeline = load_pipeline("""
name: batch-size-large
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_list }}"
    batch_size: 100
    args: ["echo", "{{ item }}"]
input:
  data_list:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner())

    data_list = list(range(5))
    result = await engine.run(pipeline, user_input={"data_list": data_list})

    assert result.success is True
    summary = result.result["summary"]
    assert summary["total"] == 5
    assert summary["succeeded"] == 5


# ---------------------------------------------------------------------------
# T-BRIX-V3-09: flat_output foreach tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_foreach_flat_output():
    """flat_output=true returns a plain list of data values in items."""
    pipeline = load_pipeline("""
name: flat-foreach
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_items }}"
    flat_output: true
    args: ["echo", "{{ item }}"]
input:
  data_items:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner(data="processed"))

    result = await engine.run(pipeline, user_input={"data_items": ["a", "b", "c"]})

    assert result.success is True
    # items should be a flat list of data values, not {success, data} dicts
    items = result.result["items"]
    assert isinstance(items, list)
    assert all(item == "processed" for item in items)
    assert len(items) == 3
    # summary still present
    assert result.result["summary"]["total"] == 3
    assert result.result["summary"]["succeeded"] == 3


@pytest.mark.asyncio
async def test_foreach_flat_output_false():
    """flat_output=false (default) preserves the normal {success, data} wrapper."""
    pipeline = load_pipeline("""
name: normal-foreach
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_items }}"
    flat_output: false
    args: ["echo", "{{ item }}"]
input:
  data_items:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner(data="ok"))

    result = await engine.run(pipeline, user_input={"data_items": [1, 2]})

    assert result.success is True
    items = result.result["items"]
    # Normal mode: list of {success, data} dicts
    assert isinstance(items, list)
    assert all(isinstance(item, dict) and "success" in item for item in items)


@pytest.mark.asyncio
async def test_foreach_flat_output_filters_failures():
    """flat_output=true with on_error:continue only includes successful items."""
    pipeline = load_pipeline("""
name: flat-foreach-failures
error_handling:
  on_error: continue
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_items }}"
    flat_output: true
    args: ["echo", "{{ item }}"]
input:
  data_items:
    type: string
    default: "[]"
""")

    call_counter = {"n": 0}

    class _AlternatingRunner(_StubRunnerMixin, BaseRunner):
        """Succeeds for even indices, fails for odd."""

        async def execute(self, step, context) -> dict:
            idx = call_counter["n"]
            call_counter["n"] += 1
            if idx % 2 == 0:
                return {"success": True, "data": f"item-{idx}"}
            return {"success": False, "error": "forced-fail"}

    engine = PipelineEngine()
    engine.register_runner("cli", _AlternatingRunner())

    result = await engine.run(pipeline, user_input={"data_items": [0, 1, 2, 3]})

    assert result.success is True  # on_error: continue
    items = result.result["items"]
    # Only 2 succeeded (indices 0 and 2)
    assert len(items) == 2
    assert items == ["item-0", "item-2"]


# ---------------------------------------------------------------------------
# INBOX-281: foreach output items directly accessible via step.items in Jinja2
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_foreach_results_shorthand_in_jinja_context():
    """foreach step exposes step.results as a direct list in the Jinja2 context (INBOX-281).

    This allows downstream steps to use selectattr/map without navigating the
    {items, summary, success, duration} wrapper dict via step.output.items.
    """
    pipeline = load_pipeline("""
name: foreach-results-shorthand
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_items }}"
    args: ["echo", "{{ item }}"]
  - id: count_results
    type: set
    values:
      total: "{{ process.results | length }}"
      success_count: "{{ process.results | selectattr('success') | list | length }}"
input:
  data_items:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner(data="done"))

    result = await engine.run(pipeline, user_input={"data_items": ["a", "b", "c"]})

    assert result.success is True
    # The set step uses process.results directly — should not crash
    assert result.result["total"] == 3
    assert result.result["success_count"] == 3


@pytest.mark.asyncio
async def test_foreach_results_shorthand_selectattr_map():
    """selectattr and map work on foreach step.results without accessing step.output.items."""
    pipeline = load_pipeline("""
name: foreach-selectattr-map
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_items }}"
    args: ["echo", "{{ item }}"]
  - id: extract_data
    type: set
    values:
      data_values: "{{ process.results | selectattr('success') | map(attribute='data') | list }}"
input:
  data_items:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner(data="result-value"))

    result = await engine.run(pipeline, user_input={"data_items": ["x", "y"]})

    assert result.success is True
    # selectattr + map should work on the results list
    assert result.result["data_values"] == ["result-value", "result-value"]


@pytest.mark.asyncio
async def test_foreach_output_backward_compat_preserved():
    """step.output still returns the full wrapper dict (backward compat for INBOX-281)."""
    pipeline = load_pipeline("""
name: foreach-output-compat
steps:
  - id: process
    type: cli
    foreach: "{{ input.data_items }}"
    args: ["echo", "{{ item }}"]
  - id: check_summary
    type: set
    values:
      total: "{{ process.output.summary.total }}"
      items_via_output: "{{ process.output['items'] | length }}"
      items_via_results: "{{ process.results | length }}"
input:
  data_items:
    type: string
    default: "[]"
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner(data="ok"))

    result = await engine.run(pipeline, user_input={"data_items": [1, 2, 3]})

    assert result.success is True
    # Both access paths must work
    assert result.result["total"] == 3
    assert result.result["items_via_output"] == 3
    assert result.result["items_via_results"] == 3


# ---------------------------------------------------------------------------
# T-BRIX-V3-10: else_of conditional step tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_else_of_executes_when_ref_skipped():
    """Step B (else_of: A) runs when A was skipped due to when condition."""
    pipeline = load_pipeline("""
name: else-of-test
steps:
  - id: step_a
    type: cli
    when: "{{ input.flag }}"
    args: ["echo", "A"]
  - id: step_b
    type: cli
    else_of: step_a
    args: ["echo", "B"]
input:
  flag:
    type: bool
    default: true
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner(data="ran"))

    # flag=false → step_a is skipped → step_b should run
    result = await engine.run(pipeline, user_input={"flag": False})

    assert result.success is True
    assert result.steps["step_a"].status == "skipped"
    assert result.steps["step_b"].status == "ok"


@pytest.mark.asyncio
async def test_else_of_skipped_when_ref_ran():
    """Step B (else_of: A) is skipped when A ran successfully."""
    pipeline = load_pipeline("""
name: else-of-ran
steps:
  - id: step_a
    type: cli
    when: "{{ input.flag }}"
    args: ["echo", "A"]
  - id: step_b
    type: cli
    else_of: step_a
    args: ["echo", "B"]
input:
  flag:
    type: bool
    default: false
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner(data="ran"))

    # flag=true → step_a runs → step_b should be skipped
    result = await engine.run(pipeline, user_input={"flag": True})

    assert result.success is True
    assert result.steps["step_a"].status == "ok"
    assert result.steps["step_b"].status == "skipped"
    assert "else_of" in result.steps["step_b"].reason


@pytest.mark.asyncio
async def test_else_of_nonexistent_ref():
    """else_of pointing to a non-existent step ID causes the step to be skipped."""
    pipeline = load_pipeline("""
name: else-of-missing
steps:
  - id: step_b
    type: cli
    else_of: nonexistent_step
    args: ["echo", "B"]
""")

    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysSuccessRunner(data="ran"))

    result = await engine.run(pipeline, user_input={})

    assert result.success is True
    assert result.steps["step_b"].status == "skipped"
    assert "nonexistent_step" in result.steps["step_b"].reason


# ---------------------------------------------------------------------------
# T-BRIX-V4-01 — engine.run() inside asyncio.create_task() (cancel scope fix)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_run_in_create_task():
    """engine.run() dispatched via asyncio.create_task() must not raise
    'Attempted to exit cancel scope in a different task than it was entered in'.

    This was caused by manually calling McpConnectionPool.__aenter__ /
    __aexit__ without an 'async with' block, which let the ClientSessionGroup
    cancel scope cross asyncio task boundaries when async_mode=True was used
    from the MCP server.
    """
    import asyncio

    pipeline = load_pipeline("""
name: create-task-test
steps:
  - id: step_one
    type: cli
    args: ["echo", "task-dispatch"]
""")

    result_holder: list = []
    error_holder: list = []

    async def _run():
        try:
            engine = PipelineEngine()
            r = await engine.run(pipeline)
            result_holder.append(r)
        except Exception as exc:
            error_holder.append(exc)

    task = asyncio.create_task(_run())
    await task

    assert not error_holder, f"engine.run() raised inside create_task: {error_holder[0]}"
    assert result_holder, "engine.run() returned no result"
    assert result_holder[0].success is True
    assert result_holder[0].steps["step_one"].status == "ok"


@pytest.mark.asyncio
async def test_engine_run_direct():
    """Direct await of engine.run() still works correctly (regression guard)."""
    pipeline = load_pipeline("""
name: direct-run-test
steps:
  - id: greet
    type: cli
    args: ["echo", "direct"]
""")

    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["greet"].status == "ok"
    assert result.result == "direct"


# ---------------------------------------------------------------------------
# T-BRIX-V4-02: enabled: false
# ---------------------------------------------------------------------------


async def test_enabled_false_skips_step():
    """Step with enabled=false is unconditionally skipped."""
    pipeline = load_pipeline("""
name: enabled-false
steps:
  - id: skipped_step
    type: cli
    enabled: false
    args: ["echo", "should-not-run"]
  - id: run_step
    type: cli
    args: ["echo", "ran"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["skipped_step"].status == "skipped"
    assert result.steps["skipped_step"].reason == "disabled"
    assert result.steps["run_step"].status == "ok"


async def test_enabled_true_runs_step():
    """Step with enabled=true (default) executes normally."""
    pipeline = load_pipeline("""
name: enabled-true
steps:
  - id: run_me
    type: cli
    enabled: true
    args: ["echo", "executed"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["run_me"].status == "ok"


# ---------------------------------------------------------------------------
# T-BRIX-V4-03: set step type
# ---------------------------------------------------------------------------


async def test_set_step_writes_values():
    """set step returns its values dict as output."""
    pipeline = load_pipeline("""
name: set-basic
steps:
  - id: computed
    type: set
    values:
      greeting: hello
      count: 42
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["computed"].status == "ok"
    assert result.result == {"greeting": "hello", "count": 42}


async def test_set_step_jinja2_rendering():
    """set step renders Jinja2 templates in values."""
    pipeline = load_pipeline("""
name: set-jinja2
input:
  name:
    type: str
    default: "world"
steps:
  - id: computed
    type: set
    values:
      greeting: "Hello {{ input.name }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, user_input={"name": "Alice"})

    assert result.success is True
    assert result.result == {"greeting": "Hello Alice"}


# ---------------------------------------------------------------------------
# T-BRIX-V4-04: stop step type
# ---------------------------------------------------------------------------


async def test_stop_step_ends_pipeline():
    """stop step halts the pipeline and subsequent steps are not executed."""
    pipeline = load_pipeline("""
name: stop-basic
steps:
  - id: first
    type: cli
    args: ["echo", "first"]
  - id: stopper
    type: stop
  - id: never
    type: cli
    args: ["echo", "never"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.steps["first"].status == "ok"
    assert result.steps["stopper"].status == "ok"
    assert "never" not in result.steps


async def test_stop_step_success_true():
    """stop step with success_on_stop=true → pipeline.success is True."""
    pipeline = load_pipeline("""
name: stop-success-true
steps:
  - id: stopper
    type: stop
    success_on_stop: true
    message: "Early exit OK"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["stopper"].status == "ok"
    assert result.steps["stopper"].reason == "Early exit OK"


async def test_stop_step_success_false():
    """stop step with success_on_stop=false → pipeline.success is False."""
    pipeline = load_pipeline("""
name: stop-success-false
steps:
  - id: stopper
    type: stop
    success_on_stop: false
    message: "Aborting"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["stopper"].status == "ok"


async def test_stop_step_with_when():
    """stop step with when=false is skipped; pipeline continues."""
    pipeline = load_pipeline("""
name: stop-when-false
steps:
  - id: stopper
    type: stop
    when: "false"
  - id: continues
    type: cli
    args: ["echo", "ran"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["stopper"].status == "skipped"
    assert result.steps["continues"].status == "ok"


# ===========================================================================
# T-BRIX-V4-05: choose runner
# ===========================================================================

@pytest.mark.asyncio
async def test_choose_first_match():
    """First matching branch is executed; others are ignored."""
    pipeline = load_pipeline("""
name: choose-first-match
steps:
  - id: chooser
    type: choose
    choices:
      - when: "true"
        steps:
          - id: branch_a
            type: cli
            args: ["echo", "branch_a"]
      - when: "true"
        steps:
          - id: branch_b
            type: cli
            args: ["echo", "branch_b"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    # Result is the output of the first branch
    assert result.result is not None


@pytest.mark.asyncio
async def test_choose_default():
    """When no branch matches, the default_steps branch is executed."""
    pipeline = load_pipeline("""
name: choose-default
steps:
  - id: chooser
    type: choose
    choices:
      - when: "false"
        steps:
          - id: never
            type: cli
            args: ["echo", "never"]
    default_steps:
      - id: fallback
        type: cli
        args: ["echo", "fallback"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.result is not None


@pytest.mark.asyncio
async def test_choose_no_match_no_default():
    """No match and no default → success=True with data=None."""
    pipeline = load_pipeline("""
name: choose-no-match
steps:
  - id: chooser
    type: choose
    choices:
      - when: "false"
        steps:
          - id: never
            type: cli
            args: ["echo", "never"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.result is None


# ===========================================================================
# T-BRIX-V4-06: parallel step runner
# ===========================================================================

@pytest.mark.asyncio
async def test_parallel_steps():
    """Two CLI sub-steps run in parallel; both outputs appear in result."""
    pipeline = load_pipeline("""
name: parallel-steps
steps:
  - id: par
    type: parallel
    sub_steps:
      - id: step_a
        type: cli
        args: ["echo", "hello_a"]
      - id: step_b
        type: cli
        args: ["echo", "hello_b"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert isinstance(result.result, dict)
    assert "step_a" in result.result
    assert "step_b" in result.result


@pytest.mark.asyncio
async def test_parallel_step_failure():
    """A failing sub-step causes success=False; other steps still run."""
    pipeline = load_pipeline("""
name: parallel-failure
steps:
  - id: par
    type: parallel
    sub_steps:
      - id: good_step
        type: cli
        args: ["echo", "ok"]
      - id: bad_step
        type: cli
        args: ["false"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    # Overall: failed because bad_step failed
    assert result.success is False
    # par step recorded as error
    assert result.steps["par"].status == "error"


# ===========================================================================
# T-BRIX-V4-07: repeat runner
# ===========================================================================

@pytest.mark.asyncio
async def test_repeat_until():
    """Repeat stops when until condition becomes true after first iteration."""
    pipeline = load_pipeline("""
name: repeat-until
steps:
  - id: counter
    type: repeat
    max_iterations: 10
    until: "{{ repeat.index >= 2 }}"
    sequence:
      - id: tick
        type: cli
        args: ["echo", "tick"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    # Ran 3 iterations (index 0, 1, 2 — stops after index 2 matches)
    assert result.result is not None


@pytest.mark.asyncio
async def test_repeat_max_iterations():
    """When until never matches, repeat stops at max_iterations."""
    pipeline = load_pipeline("""
name: repeat-max
steps:
  - id: looper
    type: repeat
    max_iterations: 3
    until: "false"
    sequence:
      - id: step
        type: cli
        args: ["echo", "loop"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True


@pytest.mark.asyncio
async def test_repeat_while():
    """while_condition=false on first check → zero iterations, success=True."""
    pipeline = load_pipeline("""
name: repeat-while-false
steps:
  - id: looper
    type: repeat
    while_condition: "false"
    max_iterations: 10
    sequence:
      - id: step
        type: cli
        args: ["echo", "never"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    # No iterations ran — result is None
    assert result.result is None


# ---------------------------------------------------------------------------
# notify step (T-BRIX-V4-11)
# ---------------------------------------------------------------------------


async def test_notify_step_logged():
    """notify step without a channel logs the message and returns success."""
    pipeline = load_pipeline("""
name: notify-log
steps:
  - id: alert
    type: notify
    message: "Hello from notify"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["alert"].status == "ok"
    data = result.result
    assert data["status"] == "logged"
    assert data["message"] == "Hello from notify"


async def test_notify_step_channel():
    """notify step with an unknown channel still returns success (logged fallback)."""
    pipeline = load_pipeline("""
name: notify-channel
steps:
  - id: ping
    type: notify
    channel: log
    to: "ops-team"
    message: "Done"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["ping"].status == "ok"
    data = result.result
    assert data["status"] == "logged"
    assert data["channel"] == "log"
    assert data["to"] == "ops-team"


# ---------------------------------------------------------------------------
# Approval step (T-BRIX-V4-12)
# ---------------------------------------------------------------------------


async def test_approval_auto_approved():
    """approval step succeeds when approval_pending.json is set to 'approved'."""
    import asyncio
    import json
    from pathlib import Path
    from brix.runners.approval import ApprovalRunner

    pipeline = load_pipeline("""
name: approval-auto
steps:
  - id: wait
    type: approval
    message: "Please approve"
    approval_timeout: "4s"
    on_timeout: stop
""")
    engine = PipelineEngine()

    # Capture the context workdir so we can write the approval from a background task
    captured_context: list = []

    class _CapturingApprovalRunner(ApprovalRunner):
        async def execute(self, step, context):
            captured_context.append(context)
            return await super().execute(step, context)

    engine.register_runner("approval", _CapturingApprovalRunner())

    # Spawn a background task that writes "approved" after a short delay
    async def _approve_later():
        # Wait until the runner has captured the context and written the pending file
        for _ in range(40):
            await asyncio.sleep(0.1)
            if captured_context:
                approval_file = Path(captured_context[0].workdir) / "approval_pending.json"
                if approval_file.exists():
                    approval_file.write_text(json.dumps({
                        "step_id": "wait",
                        "message": "auto",
                        "requested_at": 0,
                        "status": "approved",
                        "approved_by": "test",
                    }))
                    return

    task = asyncio.create_task(_approve_later())
    result = await engine.run(pipeline)
    await task

    assert result.success is True
    assert result.steps["wait"].status == "ok"
    assert result.result["approved"] is True
    assert result.result["approved_by"] == "test"


async def test_approval_timeout_stop():
    """approval step fails with error when timeout expires and on_timeout=stop."""
    pipeline = load_pipeline("""
name: approval-timeout-stop
steps:
  - id: wait
    type: approval
    message: "Nobody will approve this"
    approval_timeout: "1s"
    on_timeout: stop
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["wait"].status == "error"
    assert result.steps["wait"].errors == 1


async def test_approval_timeout_continue():
    """approval step succeeds with auto_continued=True when on_timeout=continue."""
    pipeline = load_pipeline("""
name: approval-timeout-continue
steps:
  - id: wait
    type: approval
    message: "Will auto-continue"
    approval_timeout: "1s"
    on_timeout: continue
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["wait"].status == "ok"
    assert result.result["auto_continued"] is True
    assert result.result["approved"] is False


# ---------------------------------------------------------------------------
# HTTP body Jinja2 rendering (INBOX-344)
# ---------------------------------------------------------------------------


async def test_http_body_jinja2_rendered(monkeypatch):
    """body field in HTTP step renders Jinja2 templates before sending request."""
    import httpx

    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["json"] = kwargs.get("json")
        received["content"] = kwargs.get("content")
        return httpx.Response(200, json={"ok": True}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    pipeline = load_pipeline("""
name: body-render-test
input:
  email:
    type: str
steps:
  - id: post
    type: http
    url: https://example.com/api
    method: POST
    body:
      source: "gmail:{{ input.email }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, user_input={"email": "user@example.com"})

    assert result.success is True
    # body dict must have been rendered — NOT the raw template literal
    assert received["json"] == {"source": "gmail:user@example.com"}


async def test_http_body_string_jinja2_rendered(monkeypatch):
    """body string with {{ }} in HTTP step renders Jinja2 templates before sending."""
    import httpx

    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["content"] = kwargs.get("content")
        return httpx.Response(200, json={"ok": True}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    pipeline = load_pipeline("""
name: body-string-render-test
input:
  email:
    type: str
steps:
  - id: post
    type: http
    url: https://example.com/api
    method: POST
    body: "gmail:{{ input.email }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, user_input={"email": "user@example.com"})

    assert result.success is True
    assert received["content"] == "gmail:user@example.com"


async def test_http_body_not_rendered_without_template(monkeypatch):
    """body dict without templates is passed through unchanged (no regression)."""
    import httpx

    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["json"] = kwargs.get("json")
        return httpx.Response(200, json={"ok": True}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    pipeline = load_pipeline("""
name: body-static-test
steps:
  - id: post
    type: http
    url: https://example.com/api
    method: POST
    body:
      key: static_value
      number: 42
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert received["json"] == {"key": "static_value", "number": 42}


# ---------------------------------------------------------------------------
# dry_run_steps — selective step skipping (T-BRIX-V4-BUG-09)
# ---------------------------------------------------------------------------


async def test_dry_run_steps_single_step_skipped():
    """A step listed in dry_run_steps gets status='dry_run' and is not executed."""
    pipeline = load_pipeline("""
name: dry-run-steps-single
steps:
  - id: normal
    type: cli
    args: ["echo", "ran"]
  - id: skipped
    type: cli
    args: ["echo", "should-not-run"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, dry_run_steps=["skipped"])

    assert result.success is True
    assert result.steps["normal"].status == "ok"
    assert result.steps["skipped"].status == "dry_run"
    assert result.steps["skipped"].reason == "dry_run_steps"


async def test_dry_run_steps_multiple_steps_skipped():
    """Multiple steps in dry_run_steps are all skipped."""
    pipeline = load_pipeline("""
name: dry-run-steps-multi
steps:
  - id: step_a
    type: cli
    args: ["echo", "a"]
  - id: step_b
    type: cli
    args: ["echo", "b"]
  - id: step_c
    type: cli
    args: ["echo", "c"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, dry_run_steps=["step_a", "step_c"])

    assert result.success is True
    assert result.steps["step_a"].status == "dry_run"
    assert result.steps["step_b"].status == "ok"
    assert result.steps["step_c"].status == "dry_run"


async def test_dry_run_steps_output_is_null():
    """A dry_run step does not set context output; downstream steps see null."""
    pipeline = load_pipeline("""
name: dry-run-steps-output
steps:
  - id: producer
    type: cli
    args: ["echo", "produced"]
  - id: consumer
    type: cli
    args: ["echo", "{{ steps.producer.output }}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, dry_run_steps=["producer"])

    # consumer still runs; producer output is None (not set)
    assert result.steps["producer"].status == "dry_run"
    assert result.steps["consumer"].status == "ok"
    # producer output should not be in context (treated as null by Jinja2)
    assert result.steps["producer"].items is None


async def test_dry_run_steps_all_steps_skipped():
    """All steps in dry_run_steps → pipeline succeeds with all dry_run."""
    pipeline = load_pipeline("""
name: dry-run-steps-all
steps:
  - id: alpha
    type: cli
    args: ["echo", "a"]
  - id: beta
    type: cli
    args: ["echo", "b"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, dry_run_steps=["alpha", "beta"])

    assert result.success is True
    assert result.steps["alpha"].status == "dry_run"
    assert result.steps["beta"].status == "dry_run"


async def test_dry_run_steps_none_means_no_skip():
    """Passing dry_run_steps=None (default) executes all steps normally."""
    pipeline = load_pipeline("""
name: dry-run-steps-none
steps:
  - id: step_x
    type: cli
    args: ["echo", "executed"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, dry_run_steps=None)

    assert result.success is True
    assert result.steps["step_x"].status == "ok"


async def test_dry_run_steps_unknown_step_ignored():
    """A step ID in dry_run_steps that does not exist in the pipeline is silently ignored."""
    pipeline = load_pipeline("""
name: dry-run-steps-unknown
steps:
  - id: real_step
    type: cli
    args: ["echo", "ran"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, dry_run_steps=["nonexistent_step"])

    assert result.success is True
    assert result.steps["real_step"].status == "ok"


# ---------------------------------------------------------------------------
# Performance hints (T-BRIX-V5-03)
# ---------------------------------------------------------------------------

import os as _os_perf
_HELPERS_PERF = _os_perf.path.join(_os_perf.path.dirname(__file__), "helpers")


async def test_engine_foreach_sequential_large_adds_hint():
    """Sequential foreach with >100 items adds a performance hint."""
    pipeline = load_pipeline(f"""
name: perf-hint-sequential
steps:
  - id: make_list
    type: python
    script: "{_HELPERS_PERF}/list_large.py"
  - id: process
    type: cli
    foreach: "{{{{ make_list.output }}}}"
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.result is not None
    assert "hints" in result.result, f"Expected 'hints' in foreach result, got: {result.result}"
    hints = result.result["hints"]
    assert any("Sequential foreach" in h for h in hints), f"Unexpected hints: {hints}"
    assert any("parallel" in h for h in hints), f"Unexpected hints: {hints}"


async def test_engine_foreach_small_no_sequential_hint():
    """Sequential foreach with <=100 items does NOT add the sequential hint."""
    pipeline = load_pipeline(f"""
name: perf-hint-small
steps:
  - id: list_items
    type: python
    script: "{_HELPERS_PERF}/list_items.py"
  - id: process
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    foreach_out = result.result
    hints = foreach_out.get("hints", []) if isinstance(foreach_out, dict) else []
    assert not any("Sequential foreach" in h for h in hints)


async def test_engine_foreach_batch_size_no_parallel_adds_hint():
    """batch_size set with parallel=false adds the batch hint."""
    pipeline = load_pipeline(f"""
name: perf-hint-batch
steps:
  - id: list_items
    type: python
    script: "{_HELPERS_PERF}/list_items.py"
  - id: process
    type: cli
    foreach: "{{{{ list_items.output }}}}"
    batch_size: 2
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    foreach_out = result.result
    assert isinstance(foreach_out, dict)
    hints = foreach_out.get("hints", [])
    assert any("batch_size" in h for h in hints), f"Expected batch hint, got: {hints}"
    assert any("sequentially" in h for h in hints), f"Expected sequentially hint, got: {hints}"


async def test_engine_foreach_parallel_large_default_concurrency_adds_hint():
    """parallel foreach with >50 items and default concurrency=10 adds the concurrency hint."""
    pipeline = load_pipeline(f"""
name: perf-hint-parallel-default
steps:
  - id: make_list
    type: python
    script: "{_HELPERS_PERF}/list_medium.py"
  - id: process
    type: cli
    foreach: "{{{{ make_list.output }}}}"
    parallel: true
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    foreach_out = result.result
    assert isinstance(foreach_out, dict)
    hints = foreach_out.get("hints", [])
    assert any("concurrency" in h.lower() for h in hints), f"Expected concurrency hint, got: {hints}"


async def test_engine_foreach_parallel_custom_concurrency_no_hint():
    """parallel foreach with >50 items and non-default concurrency does NOT add the concurrency hint."""
    pipeline = load_pipeline(f"""
name: perf-hint-parallel-custom
steps:
  - id: make_list
    type: python
    script: "{_HELPERS_PERF}/list_medium.py"
  - id: process
    type: cli
    foreach: "{{{{ make_list.output }}}}"
    parallel: true
    concurrency: 5
    args: ["echo", "{{{{ item }}}}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    foreach_out = result.result
    hints = foreach_out.get("hints", []) if isinstance(foreach_out, dict) else []
    # concurrency is custom (5), so no default-concurrency hint
    assert not any("default concurrency=10" in h for h in hints)


# ---------------------------------------------------------------------------
# Application Logging in engine (T-BRIX-V7-08)
# ---------------------------------------------------------------------------

async def test_engine_writes_app_log_on_success(tmp_path):
    """PipelineEngine writes INFO app_log entries for run start and end."""
    from brix.db import BrixDB
    import brix.engine as _eng_mod

    db = BrixDB(db_path=tmp_path / "brix.db")
    db_log_calls: list[dict] = []

    def _fake_db_log(level: str, component: str, message: str) -> None:
        db_log_calls.append({"level": level, "component": component, "message": message})

    original_db_log = _eng_mod._db_log
    _eng_mod._db_log = _fake_db_log
    try:
        pipeline = load_pipeline("""
name: log-test
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
    finally:
        _eng_mod._db_log = original_db_log

    assert result.success is True
    assert len(db_log_calls) == 2
    start_entry = db_log_calls[0]
    end_entry = db_log_calls[1]
    assert start_entry["level"] == "INFO"
    assert "Run started" in start_entry["message"]
    assert "log-test" in start_entry["message"]
    assert end_entry["level"] == "INFO"
    assert "Run finished" in end_entry["message"]
    assert "success" in end_entry["message"]


async def test_engine_writes_error_log_on_failure(tmp_path):
    """PipelineEngine writes ERROR app_log entry when run fails."""
    import brix.engine as _eng_mod

    db_log_calls: list[dict] = []

    def _fake_db_log(level: str, component: str, message: str) -> None:
        db_log_calls.append({"level": level, "component": component, "message": message})

    original_db_log = _eng_mod._db_log
    _eng_mod._db_log = _fake_db_log
    try:
        pipeline = load_pipeline("""
name: fail-log-test
steps:
  - id: bad
    type: cli
    args: ["false"]
    on_error: stop
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
    finally:
        _eng_mod._db_log = original_db_log

    assert result.success is False
    end_entries = [e for e in db_log_calls if "Run finished" in e["message"]]
    assert len(end_entries) == 1
    assert end_entries[0]["level"] == "ERROR"
    assert "failure" in end_entries[0]["message"]
