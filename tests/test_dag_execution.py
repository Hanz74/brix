"""Tests for DAG execution (T-BRIX-V6-19): depends_on support."""

import asyncio
import pytest

from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.models import Step
from brix.runners.base import BaseRunner, _StubRunnerMixin


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_pipeline(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


class _TrackingRunner(_StubRunnerMixin, BaseRunner):
    """Stub runner that records execution order and returns success."""

    def __init__(self):
        self.order: list[str] = []

    async def execute(self, step, context) -> dict:
        self.order.append(step.id)
        return {"success": True, "data": f"ok-{step.id}"}


class _AlwaysFailRunner(_StubRunnerMixin, BaseRunner):
    async def execute(self, step, context) -> dict:
        return {"success": False, "error": f"fail-{step.id}"}


# ---------------------------------------------------------------------------
# depends_on field on Step model
# ---------------------------------------------------------------------------


def test_step_depends_on_default():
    """Step.depends_on defaults to empty list."""
    step = Step(id="x", type="cli", command="echo hi")
    assert step.depends_on == []


def test_step_depends_on_set():
    """Step.depends_on can be set to a list of step IDs."""
    step = Step(id="c", type="cli", command="echo", depends_on=["a", "b"])
    assert step.depends_on == ["a", "b"]


# ---------------------------------------------------------------------------
# DAG: A + B run in parallel, C waits for both
# ---------------------------------------------------------------------------


async def test_dag_a_b_parallel_c_waits():
    """Steps A and B (no deps) run; C waits for both before starting."""
    pipeline = load_pipeline("""
name: dag-test
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
    depends_on: [step_a, step_b]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["step_a"].status == "ok"
    assert result.steps["step_b"].status == "ok"
    assert result.steps["step_c"].status == "ok"


async def test_dag_c_output_uses_deps():
    """Step C can reference outputs from A and B via context."""
    # Use a tracking runner to verify execution order correctness
    tracker = _TrackingRunner()
    pipeline = load_pipeline("""
name: dag-tracking
steps:
  - id: a
    type: python
    script: helpers/dummy.py
  - id: b
    type: python
    script: helpers/dummy.py
  - id: c
    type: python
    script: helpers/dummy.py
    depends_on: [a, b]
""")
    engine = PipelineEngine()
    engine.register_runner("python", tracker)
    result = await engine.run(pipeline)

    # All three steps must run
    assert result.success is True
    assert set(tracker.order) == {"a", "b", "c"}
    # c must come after both a and b
    assert tracker.order.index("c") > tracker.order.index("a")
    assert tracker.order.index("c") > tracker.order.index("b")


# ---------------------------------------------------------------------------
# DAG: diamond pattern (A → B, A → C, B+C → D)
# ---------------------------------------------------------------------------


async def test_dag_diamond():
    """Diamond dependency: A → B, A → C, B+C → D."""
    tracker = _TrackingRunner()
    pipeline = load_pipeline("""
name: dag-diamond
steps:
  - id: a
    type: python
    script: helpers/dummy.py
  - id: b
    type: python
    script: helpers/dummy.py
    depends_on: [a]
  - id: c
    type: python
    script: helpers/dummy.py
    depends_on: [a]
  - id: d
    type: python
    script: helpers/dummy.py
    depends_on: [b, c]
""")
    engine = PipelineEngine()
    engine.register_runner("python", tracker)
    result = await engine.run(pipeline)

    assert result.success is True
    assert set(tracker.order) == {"a", "b", "c", "d"}
    assert tracker.order.index("b") > tracker.order.index("a")
    assert tracker.order.index("c") > tracker.order.index("a")
    assert tracker.order.index("d") > tracker.order.index("b")
    assert tracker.order.index("d") > tracker.order.index("c")


# ---------------------------------------------------------------------------
# DAG: cycle detection
# ---------------------------------------------------------------------------


async def test_dag_cycle_detection():
    """A cycle in depends_on raises an error and fails the pipeline."""
    pipeline = load_pipeline("""
name: dag-cycle
steps:
  - id: a
    type: cli
    args: ["echo", "a"]
    depends_on: [b]
  - id: b
    type: cli
    args: ["echo", "b"]
    depends_on: [a]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False


# ---------------------------------------------------------------------------
# DAG: unknown dependency reference
# ---------------------------------------------------------------------------


async def test_dag_unknown_dependency():
    """depends_on referencing a non-existent step ID fails the pipeline."""
    pipeline = load_pipeline("""
name: dag-unknown-dep
steps:
  - id: a
    type: cli
    args: ["echo", "a"]
    depends_on: [nonexistent]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False


# ---------------------------------------------------------------------------
# DAG: dependency failure skips downstream steps
# ---------------------------------------------------------------------------


async def test_dag_failed_dep_skips_downstream():
    """If step A fails, step B (depends_on: [a]) is skipped."""
    pipeline = load_pipeline("""
name: dag-fail-dep
error_handling:
  on_error: continue
steps:
  - id: a
    type: python
    script: helpers/dummy.py
    on_error: continue
  - id: b
    type: python
    script: helpers/dummy.py
    depends_on: [a]
""")
    engine = PipelineEngine()
    fail_runner = _AlwaysFailRunner()
    engine.register_runner("python", fail_runner)
    result = await engine.run(pipeline)

    assert result.steps["a"].status == "error"
    assert result.steps["b"].status == "skipped"
    assert "dependency" in (result.steps["b"].reason or "").lower()


# ---------------------------------------------------------------------------
# DAG: no depends_on → sequential mode (regression)
# ---------------------------------------------------------------------------


async def test_sequential_mode_regression():
    """Pipelines without depends_on still run sequentially (backward compat)."""
    pipeline = load_pipeline("""
name: sequential-regression
steps:
  - id: step1
    type: cli
    args: ["echo", "first"]
  - id: step2
    type: cli
    args: ["echo", "second"]
  - id: step3
    type: cli
    args: ["echo", "third"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["step1"].status == "ok"
    assert result.steps["step2"].status == "ok"
    assert result.steps["step3"].status == "ok"
    assert result.result == "third"


# ---------------------------------------------------------------------------
# DAG detect_dag_mode helper
# ---------------------------------------------------------------------------


def test_detect_dag_mode_false_when_no_depends_on():
    """_detect_dag_mode returns False when no step has depends_on."""
    steps = [
        Step(id="a", type="cli", command="echo a"),
        Step(id="b", type="cli", command="echo b"),
    ]
    assert PipelineEngine._detect_dag_mode(steps) is False


def test_detect_dag_mode_true_when_any_has_depends_on():
    """_detect_dag_mode returns True when at least one step has depends_on."""
    steps = [
        Step(id="a", type="cli", command="echo a"),
        Step(id="b", type="cli", command="echo b", depends_on=["a"]),
    ]
    assert PipelineEngine._detect_dag_mode(steps) is True


# ---------------------------------------------------------------------------
# Toposort helpers
# ---------------------------------------------------------------------------


def test_toposort_linear_chain():
    """A → B → C toposort produces [A, B, C]."""
    steps = [
        Step(id="c", type="cli", command="echo c", depends_on=["b"]),
        Step(id="a", type="cli", command="echo a"),
        Step(id="b", type="cli", command="echo b", depends_on=["a"]),
    ]
    sorted_steps = PipelineEngine._toposort_steps(steps)
    ids = [s.id for s in sorted_steps]
    assert ids.index("a") < ids.index("b")
    assert ids.index("b") < ids.index("c")


def test_toposort_cycle_raises():
    """A cycle raises ValueError."""
    steps = [
        Step(id="a", type="cli", command="echo", depends_on=["b"]),
        Step(id="b", type="cli", command="echo", depends_on=["a"]),
    ]
    with pytest.raises(ValueError, match="[Cc]ycle"):
        PipelineEngine._toposort_steps(steps)


def test_toposort_unknown_dep_raises():
    """Reference to unknown step ID raises ValueError."""
    steps = [
        Step(id="a", type="cli", command="echo", depends_on=["nonexistent"]),
    ]
    with pytest.raises(ValueError, match="unknown"):
        PipelineEngine._toposort_steps(steps)
