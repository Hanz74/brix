"""Tests for stop step skip behaviour (bugfix).

A ``stop`` step whose ``when`` condition evaluates to *false* must be skipped
and the pipeline must continue executing subsequent steps.  Only when the
``when`` condition is *true* (or absent) should the stop step actually abort
the pipeline.
"""

import pytest

from brix.engine import PipelineEngine
from brix.loader import PipelineLoader


def load_pipeline(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


# ---------------------------------------------------------------------------
# Sequential (non-DAG) execution path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_skipped_when_bool_false():
    """stop step with when=False (Python bool, from YAML `when: false`) is skipped; pipeline continues."""
    pipeline = load_pipeline("""
name: stop-skip-bool-false
steps:
  - id: before
    type: set
    values: {ran_before: true}
  - id: stopper
    type: stop
    when: false
    message: "Should not stop"
  - id: after
    type: set
    values: {ran_after: true}
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True, f"Pipeline should succeed, got: {result}"
    assert result.steps["stopper"].status == "skipped", (
        "stop step with when=False (bool) must be skipped"
    )
    assert "after" in result.steps, "Step after the skipped stop must have been executed"
    assert result.steps["after"].status == "ok", (
        "Step after skipped stop must be ok"
    )


@pytest.mark.asyncio
async def test_stop_skipped_when_empty_string():
    """stop step with when='' (empty string) is skipped; pipeline continues."""
    pipeline = load_pipeline("""
name: stop-skip-empty-string
steps:
  - id: before
    type: set
    values: {ran_before: true}
  - id: stopper
    type: stop
    when: ""
    message: "Should not stop"
  - id: after
    type: set
    values: {ran_after: true}
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True, f"Pipeline should succeed, got: {result}"
    assert result.steps["stopper"].status == "skipped", (
        "stop step with when='' (empty string) must be skipped"
    )
    assert "after" in result.steps, "Step after the skipped stop must have been executed"
    assert result.steps["after"].status == "ok", (
        "Step after skipped stop must be ok"
    )


@pytest.mark.asyncio
async def test_stop_skipped_when_false_string():
    """stop step with when='false' is skipped; pipeline continues to next step."""
    pipeline = load_pipeline("""
name: stop-skip-false-string
steps:
  - id: before
    type: set
    values: {ran_before: true}
  - id: stopper
    type: stop
    when: "false"
    message: "Should not stop"
  - id: after
    type: set
    values: {ran_after: true}
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True, f"Pipeline should succeed, got: {result}"
    assert result.steps["stopper"].status == "skipped", (
        "stop step with when=false must be skipped"
    )
    assert "after" in result.steps, "Step after the skipped stop must have been executed"
    assert result.steps["after"].status == "ok", (
        "Step after skipped stop must be ok"
    )


@pytest.mark.asyncio
async def test_stop_skipped_when_jinja_false():
    """stop step with when='{{ false }}' is skipped; pipeline continues."""
    pipeline = load_pipeline("""
name: stop-skip-jinja-false
steps:
  - id: before
    type: set
    values: {x: 1}
  - id: stopper
    type: stop
    when: "{{ false }}"
    message: "Should not stop"
  - id: after
    type: set
    values: {x: 2}
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["stopper"].status == "skipped"
    assert result.steps.get("after") is not None
    assert result.steps["after"].status == "ok"


@pytest.mark.asyncio
async def test_stop_skipped_when_condition_evaluates_false():
    """stop step whose when condition evaluates to false at runtime is skipped."""
    pipeline = load_pipeline("""
name: stop-skip-runtime-false
steps:
  - id: init
    type: set
    values: {should_stop: false}
  - id: stopper
    type: stop
    when: "{{ steps.init.output.should_stop }}"
    message: "Conditional stop"
  - id: after
    type: set
    values: {reached: true}
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["stopper"].status == "skipped"
    assert result.steps.get("after") is not None
    assert result.steps["after"].status == "ok"


@pytest.mark.asyncio
async def test_stop_executes_when_condition_true():
    """stop step with when='true' actually stops the pipeline."""
    pipeline = load_pipeline("""
name: stop-active-true
steps:
  - id: before
    type: set
    values: {x: 1}
  - id: stopper
    type: stop
    when: "true"
    message: "Stopping now"
  - id: after
    type: set
    values: {x: 2}
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    # The pipeline is aborted by the stop step; success is determined by
    # success_on_stop (default True)
    assert result.success is True
    assert result.steps["stopper"].status == "ok", (
        "stop step with when=true must be executed (status=ok)"
    )
    assert "after" not in result.steps, (
        "Step after an active stop must NOT have been executed"
    )


@pytest.mark.asyncio
async def test_stop_executes_unconditionally_when_no_when():
    """stop step with no when condition always stops the pipeline."""
    pipeline = load_pipeline("""
name: stop-unconditional
steps:
  - id: before
    type: set
    values: {x: 1}
  - id: stopper
    type: stop
    message: "Always stops"
  - id: after
    type: set
    values: {x: 2}
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True  # success_on_stop defaults to True
    assert result.steps["stopper"].status == "ok"
    assert "after" not in result.steps


@pytest.mark.asyncio
async def test_stop_success_on_stop_false():
    """stop step with success_on_stop=false marks the run as failed."""
    pipeline = load_pipeline("""
name: stop-failure
steps:
  - id: stopper
    type: stop
    message: "Failing stop"
    success_on_stop: false
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["stopper"].status == "ok"


# ---------------------------------------------------------------------------
# DAG (depends_on) execution path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_skipped_dag_when_false():
    """In DAG mode: skipped stop step does not abort; dependent step runs."""
    pipeline = load_pipeline("""
name: stop-dag-skip-false
steps:
  - id: step1
    type: set
    values: {x: 1}
  - id: stopper
    type: stop
    when: "false"
    depends_on: [step1]
  - id: step3
    type: set
    values: {y: 2}
    depends_on: [stopper]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["stopper"].status == "skipped"
    assert result.steps.get("step3") is not None
    assert result.steps["step3"].status == "ok"


@pytest.mark.asyncio
async def test_stop_active_dag_when_true():
    """In DAG mode: active stop step prevents downstream dependent steps."""
    pipeline = load_pipeline("""
name: stop-dag-active-true
steps:
  - id: step1
    type: set
    values: {x: 1}
  - id: stopper
    type: stop
    when: "true"
    depends_on: [step1]
  - id: step3
    type: set
    values: {y: 2}
    depends_on: [stopper]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True  # success_on_stop defaults to True
    assert result.steps["stopper"].status == "ok"
    # step3 depends on stopper which set step_ok=False, so step3 is skipped
    assert result.steps.get("step3") is None or result.steps["step3"].status == "skipped"
