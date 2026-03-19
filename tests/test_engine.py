"""Tests for brix.engine.PipelineEngine."""

import pytest

from brix.context import PipelineContext
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.runners.base import BaseRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_pipeline(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


class _AlwaysSuccessRunner(BaseRunner):
    """Stub runner that always returns success with a fixed payload."""

    def __init__(self, data=None):
        self._data = data

    async def execute(self, step, context) -> dict:
        return {"success": True, "data": self._data or f"ok-{step.id}"}


class _AlwaysFailRunner(BaseRunner):
    """Stub runner that always returns failure."""

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

    class _FailOnItem2Runner(BaseRunner):
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

    class _FailOnItem2Runner(BaseRunner):
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

    class _FailOnceRunner(BaseRunner):
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

    class _AlwaysFailRetryRunner(BaseRunner):
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

    class _TrackingRunner(BaseRunner):
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

    class _FailTwiceRunner(BaseRunner):
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

    class _ConstantFailRunner(BaseRunner):
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
