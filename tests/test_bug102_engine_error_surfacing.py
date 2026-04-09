"""Regression test for surfacing unexpected engine aborts in run history."""

import json

import pytest

from brix.engine import PipelineEngine
from brix.history import RunHistory
from brix.loader import PipelineLoader
from brix.mcp_handlers.insights import _handle_diagnose_run


@pytest.mark.asyncio
async def test_engine_abort_persists_synthetic_error_step(monkeypatch, tmp_path):
    """Unexpected engine crashes must be visible through steps_data and get_run_errors."""
    import brix.context as context_mod
    import brix.engine as engine_mod

    async def fake_run_pipeline_sequential(
        engine,
        pipeline,
        context,
        step_statuses,
        dry_run_steps=None,
    ):
        raise RuntimeError("synthetic engine crash")

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr(engine_mod, "run_pipeline_sequential", fake_run_pipeline_sequential)

    pipeline = PipelineLoader().load_from_string("""
name: bug102-engine-crash
steps:
  - id: first
    type: cli
    args: ["echo", "hello"]
    """)

    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    assert "_engine_error" in result.steps
    assert result.steps["_engine_error"].status == "error"
    engine_error = result.steps["_engine_error"].error_message or ""
    assert "synthetic engine crash" in engine_error
    assert "phase=execution" in engine_error
    assert "root_exception=RuntimeError: synthetic engine crash" in engine_error
    assert "Traceback" in engine_error

    history = RunHistory()
    run = history.get_run(result.run_id)

    assert run is not None
    steps_data = json.loads(run["steps_data"])
    assert steps_data["_engine_error"]["status"] == "error"
    assert "synthetic engine crash" in steps_data["_engine_error"]["error_message"]

    errors = history.get_run_errors(run_id=result.run_id)

    assert len(errors) == 1
    assert errors[0]["step_id"] == "_engine_error"
    assert "synthetic engine crash" in errors[0]["error_message"]
    assert errors[0]["phase"] == "execution"
    assert errors[0]["root_cause"] == "RuntimeError: synthetic engine crash"


@pytest.mark.asyncio
async def test_real_engine_path_persists_engine_error_when_rendering_crashes(monkeypatch, tmp_path):
    """Unexpected crashes inside step execution must still surface as _engine_error."""
    import brix.context as context_mod
    from brix.loader import PipelineLoader as LoaderClass

    original_render_step_params = LoaderClass.render_step_params

    def crashing_render_step_params(self, step, context):
        if step.id == "explode":
            raise RuntimeError("synthetic render crash")
        return original_render_step_params(self, step, context)

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr(LoaderClass, "render_step_params", crashing_render_step_params)

    pipeline = PipelineLoader().load_from_string("""
name: bug102-real-engine-crash
steps:
  - id: prepare
    type: flow.set
    values:
      ready: true
  - id: explode
    type: flow.set
    values:
      never: reached
""")

    engine = PipelineEngine()
    engine.register_runner("flow.set", engine._runners["set"])
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["prepare"].status == "ok"
    assert "_engine_error" in result.steps
    assert result.steps["_engine_error"].status == "error"
    assert "synthetic render crash" in (result.steps["_engine_error"].error_message or "")

    history = RunHistory()
    errors = history.get_run_errors(run_id=result.run_id)

    assert len(errors) == 1
    assert errors[0]["step_id"] == "_engine_error"
    assert "synthetic render crash" in errors[0]["error_message"]
    assert errors[0]["phase"] == "execution"
    assert errors[0]["root_cause"] == "RuntimeError: synthetic render crash"

    diagnosis = await _handle_diagnose_run({"run_id": result.run_id})

    assert diagnosis["success"] is True
    assert diagnosis["total_failed_steps"] == 1
    assert diagnosis["diagnoses"][0]["step_id"] == "_engine_error"
    assert diagnosis["diagnoses"][0]["phase"] == "execution"
    assert diagnosis["diagnoses"][0]["root_cause"] == "RuntimeError: synthetic render crash"
