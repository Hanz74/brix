"""Tests for T-BRIX-V4-BUG-05: Intra-Step Progress.

Covers:
1. BRIX_PROGRESS stderr parsing in PythonRunner
2. sdk.progress() function
3. progress: bool field on Step model
4. StepProgress model in models.py
5. PipelineContext.update_step_progress()
6. foreach auto-progress tracking
7. get_run_status step_progress injection
"""
import json
import sys
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from brix.models import Step, StepProgress, StepStatus
from brix.runners.python import PythonRunner, _parse_brix_progress_line, BRIX_PROGRESS_PREFIX


# ---------------------------------------------------------------------------
# 1. BRIX_PROGRESS parsing helper
# ---------------------------------------------------------------------------


def test_parse_brix_progress_valid():
    """A valid BRIX_PROGRESS line returns the payload dict."""
    line = 'BRIX_PROGRESS: {"processed": 50, "total": 200, "message": "half way"}'
    result = _parse_brix_progress_line(line)
    assert result is not None
    assert result["processed"] == 50
    assert result["total"] == 200
    assert result["message"] == "half way"


def test_parse_brix_progress_minimal():
    """Minimal BRIX_PROGRESS line (no message) is parsed correctly."""
    line = 'BRIX_PROGRESS: {"processed": 10, "total": 100}'
    result = _parse_brix_progress_line(line)
    assert result == {"processed": 10, "total": 100}


def test_parse_brix_progress_non_matching():
    """Non-BRIX_PROGRESS lines return None."""
    assert _parse_brix_progress_line("some random stderr line") is None
    assert _parse_brix_progress_line("ERROR: something went wrong") is None
    assert _parse_brix_progress_line("") is None


def test_parse_brix_progress_invalid_json():
    """BRIX_PROGRESS line with invalid JSON returns None."""
    line = "BRIX_PROGRESS: not-json"
    result = _parse_brix_progress_line(line)
    assert result is None


def test_parse_brix_progress_non_dict_json():
    """BRIX_PROGRESS with non-dict JSON returns None."""
    line = 'BRIX_PROGRESS: [1, 2, 3]'
    result = _parse_brix_progress_line(line)
    assert result is None


def test_parse_brix_progress_with_leading_whitespace():
    """BRIX_PROGRESS line with leading whitespace is parsed correctly."""
    line = '  BRIX_PROGRESS: {"processed": 5, "total": 10}'
    result = _parse_brix_progress_line(line)
    assert result is not None
    assert result["processed"] == 5


# ---------------------------------------------------------------------------
# 2. sdk.progress() function
# ---------------------------------------------------------------------------


def test_sdk_progress_writes_to_stderr(capsys):
    """sdk.progress() emits BRIX_PROGRESS: line to stderr."""
    from brix.sdk import progress

    progress(50, 200, "processing items")
    captured = capsys.readouterr()

    assert "BRIX_PROGRESS:" in captured.err
    # The payload should be valid JSON
    prefix = "BRIX_PROGRESS: "
    line = captured.err.strip()
    assert line.startswith(prefix)
    payload = json.loads(line[len(prefix):])
    assert payload["processed"] == 50
    assert payload["total"] == 200
    assert payload["message"] == "processing items"


def test_sdk_progress_without_message(capsys):
    """sdk.progress() without message omits the message key."""
    from brix.sdk import progress

    progress(10, 100)
    captured = capsys.readouterr()

    prefix = "BRIX_PROGRESS: "
    line = captured.err.strip()
    payload = json.loads(line[len(prefix):])
    assert payload["processed"] == 10
    assert payload["total"] == 100
    assert "message" not in payload


def test_sdk_progress_zero_total(capsys):
    """sdk.progress() handles zero total gracefully."""
    from brix.sdk import progress

    progress(0, 0)
    captured = capsys.readouterr()

    prefix = "BRIX_PROGRESS: "
    line = captured.err.strip()
    payload = json.loads(line[len(prefix):])
    assert payload["processed"] == 0
    assert payload["total"] == 0


# ---------------------------------------------------------------------------
# 3. Step model: progress field
# ---------------------------------------------------------------------------


def test_step_progress_field_default():
    """Step.progress defaults to False."""
    step = Step(id="test", type="python", script="test.py")
    assert step.progress is False


def test_step_progress_field_true():
    """Step.progress can be set to True."""
    step = Step(id="test", type="python", script="test.py", progress=True)
    assert step.progress is True


def test_step_progress_field_in_yaml_model():
    """Step model can be created from a dict with progress=true."""
    data = {"id": "fetch", "type": "python", "script": "fetch.py", "progress": True}
    step = Step(**data)
    assert step.progress is True


# ---------------------------------------------------------------------------
# 4. StepProgress model
# ---------------------------------------------------------------------------


def test_step_progress_model_defaults():
    """StepProgress model has sensible defaults."""
    sp = StepProgress()
    assert sp.processed == 0
    assert sp.total == 0
    assert sp.percent == 0.0
    assert sp.eta_seconds is None
    assert sp.message is None


def test_step_progress_model_values():
    """StepProgress model stores values correctly."""
    sp = StepProgress(processed=50, total=200, percent=25.0, eta_seconds=150.0, message="halfway")
    assert sp.processed == 50
    assert sp.total == 200
    assert sp.percent == 25.0
    assert sp.eta_seconds == 150.0
    assert sp.message == "halfway"


def test_step_status_has_step_progress_field():
    """StepStatus model has the step_progress optional field."""
    status = StepStatus(status="ok", duration=1.0)
    assert status.step_progress is None

    sp = StepProgress(processed=10, total=20, percent=50.0)
    status_with_progress = StepStatus(status="ok", duration=1.0, step_progress=sp)
    assert status_with_progress.step_progress is not None
    assert status_with_progress.step_progress.processed == 10


# ---------------------------------------------------------------------------
# 5. PipelineContext.update_step_progress
# ---------------------------------------------------------------------------


def test_context_update_step_progress(tmp_path):
    """update_step_progress stores progress and persists to disk."""
    from brix.context import PipelineContext

    ctx = PipelineContext(workdir=tmp_path)
    ctx.update_step_progress("my_step", {"processed": 25, "total": 100})

    # In-memory
    assert "my_step" in ctx.step_progress
    sp = ctx.step_progress["my_step"]
    assert sp["processed"] == 25
    assert sp["total"] == 100
    assert sp["percent"] == 25.0  # derived field

    # Persisted to disk
    sp_path = tmp_path / "step_progress.json"
    assert sp_path.exists()
    on_disk = json.loads(sp_path.read_text())
    assert "my_step" in on_disk
    assert on_disk["my_step"]["processed"] == 25


def test_context_update_step_progress_overwrites(tmp_path):
    """update_step_progress keeps only the latest payload per step."""
    from brix.context import PipelineContext

    ctx = PipelineContext(workdir=tmp_path)
    ctx.update_step_progress("step1", {"processed": 10, "total": 100})
    ctx.update_step_progress("step1", {"processed": 50, "total": 100})

    assert ctx.step_progress["step1"]["processed"] == 50


def test_context_update_step_progress_percent_calculation(tmp_path):
    """update_step_progress computes percent correctly."""
    from brix.context import PipelineContext

    ctx = PipelineContext(workdir=tmp_path)
    ctx.update_step_progress("step1", {"processed": 1, "total": 4})
    assert ctx.step_progress["step1"]["percent"] == 25.0

    ctx.update_step_progress("step1", {"processed": 0, "total": 0})
    assert ctx.step_progress["step1"]["percent"] == 0.0


def test_context_update_step_progress_with_message(tmp_path):
    """update_step_progress stores message field correctly."""
    from brix.context import PipelineContext

    ctx = PipelineContext(workdir=tmp_path)
    ctx.update_step_progress("step1", {"processed": 5, "total": 10, "message": "almost done"})
    assert ctx.step_progress["step1"]["message"] == "almost done"


# ---------------------------------------------------------------------------
# 6. PythonRunner with progress=True
# ---------------------------------------------------------------------------


async def test_python_runner_progress_parsing(tmp_path):
    """PythonRunner with progress=True parses BRIX_PROGRESS from stderr."""
    # Write a helper script that emits BRIX_PROGRESS
    script = tmp_path / "progress_script.py"
    script.write_text(
        'import json, sys\n'
        'sys.stderr.write("BRIX_PROGRESS: {\\"processed\\": 5, \\"total\\": 10}\\n")\n'
        'sys.stderr.flush()\n'
        'print(json.dumps({"result": "done"}))\n'
    )

    from brix.context import PipelineContext

    ctx = PipelineContext(workdir=tmp_path / "ctx")

    runner = PythonRunner()
    step = MagicMock()
    step.id = "my_step"
    step.script = str(script)
    step.params = {}
    step.timeout = None
    step.progress = True

    result = await runner.execute(step, ctx)

    assert result["success"] is True
    assert result["data"] == {"result": "done"}
    # Progress should have been stored in context
    assert "my_step" in ctx.step_progress
    assert ctx.step_progress["my_step"]["processed"] == 5
    assert ctx.step_progress["my_step"]["total"] == 10


async def test_python_runner_progress_false_no_parsing(tmp_path):
    """PythonRunner with progress=False does NOT parse BRIX_PROGRESS from stderr."""
    script = tmp_path / "no_progress.py"
    script.write_text(
        'import sys\n'
        'sys.stderr.write("BRIX_PROGRESS: {\\"processed\\": 5, \\"total\\": 10}\\n")\n'
        'print("done")\n'
    )

    from brix.context import PipelineContext

    ctx = PipelineContext(workdir=tmp_path / "ctx")

    runner = PythonRunner()
    step = MagicMock()
    step.id = "my_step"
    step.script = str(script)
    step.params = {}
    step.timeout = None
    step.progress = False

    result = await runner.execute(step, ctx)

    assert result["success"] is True
    # No progress should be stored when progress=False
    assert "my_step" not in ctx.step_progress


async def test_python_runner_mixed_stderr(tmp_path):
    """PythonRunner with progress=True separates BRIX_PROGRESS from normal stderr."""
    script = tmp_path / "mixed.py"
    script.write_text(
        'import sys, json\n'
        'sys.stderr.write("normal log line\\n")\n'
        'sys.stderr.write("BRIX_PROGRESS: {\\"processed\\": 3, \\"total\\": 5}\\n")\n'
        'sys.stderr.write("another log\\n")\n'
        'print(json.dumps({"ok": True}))\n'
    )

    from brix.context import PipelineContext

    ctx = PipelineContext(workdir=tmp_path / "ctx")

    runner = PythonRunner()
    step = MagicMock()
    step.id = "step1"
    step.script = str(script)
    step.params = {}
    step.timeout = None
    step.progress = True

    result = await runner.execute(step, ctx)

    assert result["success"] is True
    # Progress captured
    assert ctx.step_progress["step1"]["processed"] == 3
    # Normal stderr lines don't cause failure
    assert result.get("error") is None


async def test_python_runner_progress_last_value_wins(tmp_path):
    """When multiple BRIX_PROGRESS lines are emitted, the last one wins."""
    script = tmp_path / "multi_progress.py"
    script.write_text(
        'import sys, json\n'
        'sys.stderr.write("BRIX_PROGRESS: {\\"processed\\": 1, \\"total\\": 5}\\n")\n'
        'sys.stderr.write("BRIX_PROGRESS: {\\"processed\\": 3, \\"total\\": 5}\\n")\n'
        'sys.stderr.write("BRIX_PROGRESS: {\\"processed\\": 5, \\"total\\": 5}\\n")\n'
        'print(json.dumps({"done": True}))\n'
    )

    from brix.context import PipelineContext

    ctx = PipelineContext(workdir=tmp_path / "ctx")

    runner = PythonRunner()
    step = MagicMock()
    step.id = "step1"
    step.script = str(script)
    step.params = {}
    step.timeout = None
    step.progress = True

    await runner.execute(step, ctx)
    # Last value wins
    assert ctx.step_progress["step1"]["processed"] == 5


async def test_python_runner_progress_no_context(tmp_path):
    """PythonRunner with progress=True but context=None doesn't crash."""
    script = tmp_path / "no_ctx.py"
    script.write_text(
        'import sys, json\n'
        'sys.stderr.write("BRIX_PROGRESS: {\\"processed\\": 5, \\"total\\": 10}\\n")\n'
        'print(json.dumps({"ok": True}))\n'
    )

    runner = PythonRunner()
    step = MagicMock()
    step.id = "step1"
    step.script = str(script)
    step.params = {}
    step.timeout = None
    step.progress = True

    # Should not raise even with context=None
    result = await runner.execute(step, None)
    assert result["success"] is True


# ---------------------------------------------------------------------------
# 7. foreach auto-progress in engine
# ---------------------------------------------------------------------------


async def test_foreach_sequential_auto_progress(tmp_path):
    """Sequential foreach tracks auto-progress in context.step_progress."""
    from brix.engine import PipelineEngine
    from brix.loader import PipelineLoader

    # Write a trivial script
    script = tmp_path / "item_script.py"
    script.write_text('import json, sys\nparams = json.loads(sys.argv[1])\nprint(json.dumps({"val": params["v"]}))\n')

    pipeline_yaml = f"""
name: test-foreach-progress
input:
  data:
    type: array
    default: []
steps:
  - id: process
    type: python
    script: {script}
    foreach: "{{{{ input.data }}}}"
    params:
      v: "{{{{ item }}}}"
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pipeline_yaml)

    engine = PipelineEngine()
    result = await engine.run(pipeline, user_input={"data": [1, 2, 3]})

    assert result.success is True
    assert result.steps["process"].status == "ok"
    assert result.steps["process"].items == 3


async def test_foreach_parallel_auto_progress(tmp_path):
    """Parallel foreach tracks auto-progress in context.step_progress."""
    from brix.engine import PipelineEngine
    from brix.loader import PipelineLoader

    script = tmp_path / "item_script.py"
    script.write_text('import json, sys\nparams = json.loads(sys.argv[1])\nprint(json.dumps({"val": params["v"]}))\n')

    pipeline_yaml = f"""
name: test-foreach-parallel-progress
input:
  data:
    type: array
    default: []
steps:
  - id: process
    type: python
    script: {script}
    foreach: "{{{{ input.data }}}}"
    parallel: true
    concurrency: 2
    params:
      v: "{{{{ item }}}}"
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pipeline_yaml)

    engine = PipelineEngine()
    result = await engine.run(pipeline, user_input={"data": [1, 2, 3, 4]})

    assert result.success is True
    assert result.steps["process"].items == 4


# ---------------------------------------------------------------------------
# 8. _RenderedStep carries progress field
# ---------------------------------------------------------------------------


def test_rendered_step_has_progress_field():
    """_RenderedStep copies the progress attribute from the original step."""
    from brix.engine import _RenderedStep
    from brix.loader import PipelineLoader

    loader = PipelineLoader()
    step = Step(id="s", type="python", script="x.py", progress=True)
    rendered = _RenderedStep(step, {}, loader, {})
    assert rendered.progress is True

    step_no_progress = Step(id="s2", type="python", script="x.py")
    rendered2 = _RenderedStep(step_no_progress, {}, loader, {})
    assert rendered2.progress is False


# ---------------------------------------------------------------------------
# 9. get_run_status injects step_progress
# ---------------------------------------------------------------------------


async def test_get_run_status_injects_step_progress(tmp_path):
    """_handle_get_run_status reads step_progress.json and returns step_progress in response."""
    # Simulate what a running pipeline would write
    run_id = "run-test123"
    run_dir = tmp_path / run_id
    run_dir.mkdir()

    import time as _time

    run_meta = {
        "run_id": run_id,
        "pipeline": "test",
        "status": "running",
        "completed_steps": [],
        "last_heartbeat": _time.time(),
    }
    (run_dir / "run.json").write_text(json.dumps(run_meta))

    step_progress_data = {
        "fetch": {
            "processed": 50,
            "total": 200,
            "percent": 25.0,
            "eta_seconds": 180.0,
            "message": "Processing emails",
            "_updated_at": _time.time(),
        }
    }
    (run_dir / "step_progress.json").write_text(json.dumps(step_progress_data))

    # WORKDIR_BASE is imported locally inside _handle_get_run_status, so patch brix.context
    with patch("brix.context.WORKDIR_BASE", tmp_path):
        from brix.mcp_server import _handle_get_run_status
        result = await _handle_get_run_status({"run_id": run_id})

    assert result["success"] is True
    assert result["source"] == "live"
    assert "step_progress" in result
    fetch_prog = result["step_progress"]["fetch"]
    assert fetch_prog["processed"] == 50
    assert fetch_prog["total"] == 200
    assert fetch_prog["percent"] == 25.0
    assert fetch_prog["eta_seconds"] == 180.0
    assert fetch_prog["message"] == "Processing emails"


async def test_get_run_status_no_step_progress_file(tmp_path):
    """_handle_get_run_status works normally when step_progress.json doesn't exist."""
    run_id = "run-nosp"
    run_dir = tmp_path / run_id
    run_dir.mkdir()

    import time as _time

    run_meta = {
        "run_id": run_id,
        "pipeline": "test",
        "status": "running",
        "completed_steps": [],
        "last_heartbeat": _time.time(),
    }
    (run_dir / "run.json").write_text(json.dumps(run_meta))
    # No step_progress.json written

    with patch("brix.context.WORKDIR_BASE", tmp_path):
        from brix.mcp_server import _handle_get_run_status
        result = await _handle_get_run_status({"run_id": run_id})

    assert result["success"] is True
    assert "step_progress" not in result


async def test_get_run_status_step_progress_percent_recalculated(tmp_path):
    """_handle_get_run_status recalculates percent from processed/total."""
    run_id = "run-pct"
    run_dir = tmp_path / run_id
    run_dir.mkdir()

    import time as _time

    run_meta = {
        "run_id": run_id,
        "pipeline": "test",
        "status": "running",
        "completed_steps": [],
        "last_heartbeat": _time.time(),
    }
    (run_dir / "run.json").write_text(json.dumps(run_meta))

    # Percent in file is stale/wrong — server should recalculate
    step_progress_data = {
        "step1": {
            "processed": 1,
            "total": 4,
            "percent": 99.0,  # wrong — should be 25.0
        }
    }
    (run_dir / "step_progress.json").write_text(json.dumps(step_progress_data))

    with patch("brix.context.WORKDIR_BASE", tmp_path):
        from brix.mcp_server import _handle_get_run_status
        result = await _handle_get_run_status({"run_id": run_id})

    assert result["step_progress"]["step1"]["percent"] == 25.0


# ---------------------------------------------------------------------------
# 10. Version bump
# ---------------------------------------------------------------------------


def test_version_bumped():
    """Version should match current release."""
    import brix
    assert brix.__version__ is not None
    assert len(brix.__version__.split(".")) >= 2
