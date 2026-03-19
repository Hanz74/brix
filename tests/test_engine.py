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
