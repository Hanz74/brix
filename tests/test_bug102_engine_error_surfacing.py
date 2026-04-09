"""Regression test for surfacing unexpected engine aborts in run history."""

import json

import pytest

from brix.engine import PipelineEngine
from brix.history import RunHistory
from brix.loader import PipelineLoader


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
    assert "pipeline execution phase" in engine_error
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
