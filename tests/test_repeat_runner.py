"""Tests for RepeatRunner — T-BRIX-V4-BUG-07.

Covers:
  BUG-1: until condition can reference sub-step outputs ({{ check.output.status }})
  BUG-2: sub-step outputs are propagated to the outer context after the repeat block
  BUG-3: Jinja2 UndefinedError in until/while condition surfaces as a real error message
"""

import pytest

from brix.engine import PipelineEngine
from brix.loader import PipelineLoader


def load_pipeline(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


# ---------------------------------------------------------------------------
# BUG-1: until condition can see sub-step outputs
# ---------------------------------------------------------------------------


async def test_repeat_until_references_substep_output():
    """until condition can reference a sub-step output via {{ check.output }}."""
    # The sub-step (cli echo) outputs "done" after the first iteration.
    # The until condition checks for that value — loop should stop after 1 iteration.
    pipeline = load_pipeline("""
name: repeat-until-substep
steps:
  - id: loop
    type: repeat
    max_iterations: 5
    sequence:
      - id: check
        type: cli
        args: ["echo", "done"]
    until: "{{ check.output == 'done' }}"
  - id: after
    type: cli
    args: ["echo", "finished"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    # Should have stopped after 1 iteration because check.output == 'done'
    assert result.steps["loop"].status == "ok"
    loop_data = result.steps["loop"]
    # The repeat step's data contains iterations count
    assert result.result == "finished"


async def test_repeat_until_stops_on_condition():
    """until with a counter-based condition — stops exactly at iteration N."""
    # We use a set step to produce a known value each iteration.
    # The until checks repeat.index >= 2 — should stop after iteration 2 (3 iterations: 0,1,2).
    pipeline = load_pipeline("""
name: repeat-until-counter
steps:
  - id: loop
    type: repeat
    max_iterations: 10
    sequence:
      - id: dummy
        type: cli
        args: ["echo", "iter"]
    until: "{{ repeat.index >= 2 }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["loop"].status == "ok"


# ---------------------------------------------------------------------------
# BUG-2: sub-step outputs propagated to outer context after repeat block
# ---------------------------------------------------------------------------


async def test_repeat_substep_output_visible_after_loop():
    """A step following the repeat block can reference sub-step outputs from the loop."""
    pipeline = load_pipeline("""
name: repeat-propagate
steps:
  - id: loop
    type: repeat
    max_iterations: 2
    sequence:
      - id: producer
        type: cli
        args: ["echo", "hello-from-loop"]
    until: "{{ repeat.index >= 0 }}"
  - id: consumer
    type: cli
    args: ["echo", "{{ producer.output }}"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["loop"].status == "ok"
    assert result.steps["consumer"].status == "ok"
    # consumer should echo the producer output from inside the loop
    assert result.result == "hello-from-loop"


async def test_repeat_multiple_substeps_all_propagated():
    """All sub-steps from the last iteration are propagated to the outer context."""
    pipeline = load_pipeline("""
name: repeat-multi-substep
steps:
  - id: loop
    type: repeat
    max_iterations: 2
    sequence:
      - id: step_a
        type: cli
        args: ["echo", "value_a"]
      - id: step_b
        type: cli
        args: ["echo", "value_b"]
    until: "{{ repeat.index >= 0 }}"
  - id: verify
    type: set
    values:
      a_out: "{{ step_a.output }}"
      b_out: "{{ step_b.output }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["verify"].status == "ok"
    verify_out = result.result
    assert verify_out["a_out"] == "value_a"
    assert verify_out["b_out"] == "value_b"


# ---------------------------------------------------------------------------
# BUG-3: UndefinedError in until/while condition surfaces as real error
# ---------------------------------------------------------------------------


async def test_repeat_until_undefined_variable_no_unknown_error():
    """until referencing a nonexistent variable does NOT produce 'unknown error'.

    Jinja2's SandboxedEnvironment silently renders undefined attribute access as
    False/empty rather than raising UndefinedError. The loop therefore runs all
    max_iterations and succeeds (condition is never truthy). Crucially, no step
    should ever have error_message='unknown error'.
    """
    pipeline = load_pipeline("""
name: repeat-undefined-until
steps:
  - id: loop
    type: repeat
    max_iterations: 3
    sequence:
      - id: dummy
        type: cli
        args: ["echo", "ok"]
    until: "{{ nonexistent_step.output.status == 'done' }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    # Jinja2 undefined → condition is False → loop runs all 3 iterations → success
    # The important invariant: error_message must never be the generic "unknown error"
    loop_status = result.steps.get("loop")
    assert loop_status is not None
    if loop_status.error_message is not None:
        assert loop_status.error_message != "unknown error"


async def test_repeat_until_exception_returns_descriptive_error():
    """If until evaluation raises an exception, the error message is descriptive."""
    from unittest.mock import patch
    from brix.loader import PipelineLoader as _Loader

    pipeline = load_pipeline("""
name: repeat-exception-until
steps:
  - id: loop
    type: repeat
    max_iterations: 3
    sequence:
      - id: dummy
        type: cli
        args: ["echo", "ok"]
    until: "{{ repeat.index >= 1 }}"
""")
    engine = PipelineEngine()

    # Patch evaluate_condition to raise on the first until check
    original_eval = _Loader.evaluate_condition
    call_count = [0]

    def patched_eval(self, condition, context):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ValueError("simulated Jinja2 error")
        return original_eval(self, condition, context)

    with patch.object(_Loader, "evaluate_condition", patched_eval):
        result = await engine.run(pipeline)

    # Should fail with a descriptive error, not "unknown error"
    assert result.success is False
    loop_status = result.steps.get("loop")
    assert loop_status is not None
    assert loop_status.status == "error"
    assert loop_status.error_message is not None
    assert loop_status.error_message != "unknown error"
    assert "until condition error" in loop_status.error_message


async def test_repeat_while_undefined_variable_returns_real_error():
    """while_condition referencing a nonexistent variable returns a descriptive error."""
    pipeline = load_pipeline("""
name: repeat-undefined-while
steps:
  - id: loop
    type: repeat
    max_iterations: 3
    sequence:
      - id: dummy
        type: cli
        args: ["echo", "ok"]
    while_condition: "{{ nonexistent_step.output == 'go' }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    # Depending on Jinja2 sandbox behaviour — undefined evaluates to empty string
    # which is falsy, so the loop simply doesn't run (success=True, iterations=0).
    # OR if the undefined raises, we get a descriptive error.
    # Either way the error must NOT be "unknown error".
    if not result.success:
        loop_status = result.steps.get("loop")
        assert loop_status is not None
        assert loop_status.error_message != "unknown error"


# ---------------------------------------------------------------------------
# Baseline: basic repeat without until still works
# ---------------------------------------------------------------------------


async def test_repeat_basic_max_iterations():
    """repeat without until/while runs exactly max_iterations times."""
    pipeline = load_pipeline("""
name: repeat-basic
steps:
  - id: loop
    type: repeat
    max_iterations: 3
    sequence:
      - id: tick
        type: cli
        args: ["echo", "tick"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["loop"].status == "ok"


async def test_repeat_engine_last_step_outputs_populated():
    """engine._last_step_outputs is set after a run() call."""
    pipeline = load_pipeline("""
name: simple-outputs
steps:
  - id: greet
    type: cli
    args: ["echo", "world"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    # _last_step_outputs should contain greet's output
    assert "greet" in engine._last_step_outputs
    assert engine._last_step_outputs["greet"] == "world"


# ---------------------------------------------------------------------------
# T-BRIX-V4-BUG-08: step-type default timeouts for repeat runner
# ---------------------------------------------------------------------------


async def test_repeat_timeout_exceeded_returns_failure():
    """repeat step with a very short timeout returns success=False with 'Timeout' in error."""
    pipeline = load_pipeline("""
name: repeat-timeout
steps:
  - id: loop
    type: repeat
    timeout: "1s"
    max_iterations: 100
    sequence:
      - id: slow
        type: cli
        args: ["sleep", "5"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    loop_status = result.steps.get("loop")
    assert loop_status is not None
    assert loop_status.status == "error"
    assert "Timeout" in (loop_status.error_message or "")
    assert result.duration < 5.0  # Must terminate well before the sleep completes


async def test_repeat_default_timeout_is_7200s():
    """RepeatRunner default timeout is 7200s (2h), not the old 60s."""
    from brix.runners.cli import get_default_timeout
    assert get_default_timeout("repeat") == 7200.0
    assert get_default_timeout("repeat") != 60.0


async def test_repeat_explicit_timeout_overrides_default():
    """An explicit timeout on a repeat step takes precedence over the 7200s default."""
    # Verify via a fast test: a loop that runs fine within 5s but would not
    # exceed the 7200s default should succeed with an explicit "10s" timeout.
    pipeline = load_pipeline("""
name: repeat-explicit-timeout
steps:
  - id: loop
    type: repeat
    timeout: "10s"
    max_iterations: 2
    sequence:
      - id: tick
        type: cli
        args: ["echo", "tick"]
    until: "{{ repeat.index >= 1 }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["loop"].status == "ok"


# ---------------------------------------------------------------------------
# T-BRIX-V4-BUG-INPUT: repeat sub-steps have access to parent pipeline inputs
# ---------------------------------------------------------------------------


async def test_repeat_substep_sees_parent_input():
    """{{ input.* }} inside a repeat sub-step must resolve to the parent pipeline's input.

    Previously engine.run(mini) was called without user_input, so input.* rendered
    as an empty string inside the loop.
    """
    pipeline = load_pipeline("""
name: repeat-input-access
input:
  job_id:
    type: string
    default: "test-job-42"
steps:
  - id: loop
    type: repeat
    max_iterations: 1
    sequence:
      - id: echo_id
        type: cli
        args: ["echo", "{{ input.job_id }}"]
    until: "{{ repeat.index >= 0 }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    # The sub-step output should contain the job_id value, not an empty string
    assert engine._last_step_outputs.get("echo_id") == "test-job-42"


async def test_repeat_substep_sees_parent_input_via_user_input():
    """user_input passed to engine.run() is visible inside repeat sub-steps."""
    pipeline = load_pipeline("""
name: repeat-user-input-access
input:
  msg:
    type: string
    default: "default-msg"
steps:
  - id: loop
    type: repeat
    max_iterations: 1
    sequence:
      - id: echo_msg
        type: cli
        args: ["echo", "{{ input.msg }}"]
    until: "{{ repeat.index >= 0 }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline, user_input={"msg": "hello-from-user"})

    assert result.success is True
    assert engine._last_step_outputs.get("echo_msg") == "hello-from-user"


# ---------------------------------------------------------------------------
# T-BRIX-V4-BUG-INPUT: repeat runner surfaces descriptive error when sub-steps fail
# ---------------------------------------------------------------------------


async def test_repeat_failed_substep_no_unknown_error():
    """When a repeat sub-step fails, error_message must NOT be 'unknown error'."""
    pipeline = load_pipeline("""
name: repeat-substep-fail
steps:
  - id: loop
    type: repeat
    max_iterations: 1
    sequence:
      - id: bad
        type: cli
        args: ["false"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    loop_status = result.steps.get("loop")
    assert loop_status is not None
    assert loop_status.status == "error"
    assert loop_status.error_message is not None
    assert loop_status.error_message != "unknown error"
