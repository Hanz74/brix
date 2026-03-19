"""Tests for workdir management and resume."""
import json
import pytest
from pathlib import Path
from brix.context import PipelineContext, WORKDIR_BASE


def test_workdir_created():
    ctx = PipelineContext()
    assert ctx.workdir.exists()
    assert (ctx.workdir / "step_outputs").exists()
    assert (ctx.workdir / "files").exists()
    ctx.cleanup()


def test_set_output_persists(tmp_path):
    ctx = PipelineContext(workdir=tmp_path / "test-run")
    ctx.set_output("step1", {"key": "value"})

    persisted = json.loads((ctx.workdir / "step_outputs" / "step1.json").read_text())
    assert persisted == {"key": "value"}


def test_save_run_metadata(tmp_path):
    ctx = PipelineContext(workdir=tmp_path / "test-run")
    ctx.save_run_metadata("test-pipeline", "running")

    meta = json.loads((ctx.workdir / "run.json").read_text())
    assert meta["pipeline"] == "test-pipeline"
    assert meta["status"] == "running"


def test_resume_from_workdir(tmp_path):
    # Create initial run
    workdir = tmp_path / "run-resume-test"
    ctx1 = PipelineContext(workdir=workdir)
    ctx1.run_id = "run-resume-test"
    ctx1.set_output("step1", {"data": "from-step1"})
    ctx1.save_run_metadata("test", "failed")

    # Resume
    # Monkeypatch WORKDIR_BASE
    import brix.context
    original_base = brix.context.WORKDIR_BASE
    brix.context.WORKDIR_BASE = tmp_path
    try:
        ctx2 = PipelineContext.from_resume("run-resume-test")
        assert ctx2.run_id == "run-resume-test"
        assert ctx2.get_output("step1") == {"data": "from-step1"}
        assert ctx2.is_step_completed("step1") is True
        assert ctx2.is_step_completed("step2") is False
    finally:
        brix.context.WORKDIR_BASE = original_base


def test_save_file(tmp_path):
    ctx = PipelineContext(workdir=tmp_path / "test-run")
    path = ctx.save_file("test.txt", b"hello world")
    assert path.exists()
    assert path.read_bytes() == b"hello world"


def test_cleanup(tmp_path):
    ctx = PipelineContext(workdir=tmp_path / "test-run")
    assert ctx.workdir.exists()
    ctx.cleanup()
    assert not ctx.workdir.exists()


def test_cleanup_keep(tmp_path):
    ctx = PipelineContext(workdir=tmp_path / "test-run")
    ctx.cleanup(keep=True)
    assert ctx.workdir.exists()  # Still exists


def test_is_step_completed_no_resume():
    ctx = PipelineContext()
    ctx.set_output("s1", "data")
    assert ctx.is_step_completed("s1") is False  # Not in resume mode
    ctx.cleanup()
