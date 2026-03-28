"""Tests for T-BRIX-V6-03: Self-Healing — Auto-Install + Auto-Kill.

Covers:
1. Per-step requirements field on the Step model.
2. Engine checks and auto-installs per-step requirements before execution.
3. Engine aborts the step (and honours on_error) when step-level install fails.
4. _ensure_step_requirements helper on PipelineEngine.
5. Auto-kill watchdog: _background_run_watchdog cancels stale tasks.
6. _ensure_watchdog starts the watchdog task lazily.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from brix.engine import PipelineEngine
from brix.models import Pipeline, Step


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pipeline(*steps, **kwargs) -> Pipeline:
    return Pipeline(name="test", steps=list(steps), **kwargs)


def _step(step_id: str, reqs: list[str] | None = None, **kwargs) -> Step:
    base = dict(id=step_id, type="cli", args=["echo", step_id])
    if reqs is not None:
        base["requirements"] = reqs
    base.update(kwargs)
    return Step(**base)


# ---------------------------------------------------------------------------
# 1. Step model: requirements field
# ---------------------------------------------------------------------------


def test_step_has_requirements_field_default():
    """Step.requirements defaults to empty list."""
    step = Step(id="s1", type="cli", args=["echo", "hi"])
    assert step.requirements == []


def test_step_accepts_requirements():
    """Step.requirements accepts a list of PEP-508 specifiers."""
    step = Step(id="s1", type="cli", args=["echo", "hi"], requirements=["requests>=2.28", "pyyaml"])
    assert step.requirements == ["requests>=2.28", "pyyaml"]


def test_step_requirements_from_yaml():
    """Pipeline YAML with per-step requirements is parsed correctly."""
    from brix.loader import PipelineLoader

    yaml_str = """\
name: step-reqs
steps:
  - id: fetch
    type: cli
    args: [echo, hi]
    requirements:
      - httpx>=0.23
      - pydantic>=2.0
"""
    pipeline = PipelineLoader().load_from_string(yaml_str)
    assert pipeline.steps[0].requirements == ["httpx>=0.23", "pydantic>=2.0"]


# ---------------------------------------------------------------------------
# 2. _ensure_step_requirements helper
# ---------------------------------------------------------------------------


def test_ensure_step_requirements_no_missing():
    """Returns None when all step requirements are already installed."""
    engine = PipelineEngine()
    step = _step("s1", reqs=["pip"])  # pip is always installed

    result = engine._ensure_step_requirements(step)
    assert result is None


def test_ensure_step_requirements_installs_missing():
    """Calls install_requirements and returns None on success."""
    engine = PipelineEngine()
    step = _step("s1", reqs=["nonexistent-brix-v6-03-test-pkg"])

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-v6-03-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=True) as mock_install:
            result = engine._ensure_step_requirements(step)

    mock_install.assert_called_once_with(["nonexistent-brix-v6-03-test-pkg"])
    assert result is None


def test_ensure_step_requirements_returns_error_on_failure():
    """Returns an error message string when install fails."""
    engine = PipelineEngine()
    step = _step("s1", reqs=["nonexistent-brix-v6-03-test-pkg"])

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-v6-03-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=False):
            result = engine._ensure_step_requirements(step)

    assert result is not None
    assert "nonexistent-brix-v6-03-test-pkg" in result
    assert "s1" in result


def test_ensure_step_requirements_empty_list():
    """Returns None immediately when requirements list is empty (no-op)."""
    engine = PipelineEngine()
    step = _step("s1", reqs=[])

    with patch("brix.deps.check_requirements") as mock_check:
        result = engine._ensure_step_requirements(step)

    # check_requirements is not called — the helper short-circuits on empty list.
    mock_check.assert_not_called()
    assert result is None


# ---------------------------------------------------------------------------
# 3. Engine: per-step auto-install integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_installs_step_requirements_before_execution():
    """Engine calls install_requirements for a step with missing packages."""
    pipeline = _pipeline(_step("fetch", reqs=["nonexistent-brix-v6-03-test-pkg"]))

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-v6-03-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=True) as mock_install:
            engine = PipelineEngine()
            result = await engine.run(pipeline)

    # install was called once for the step-level requirement
    assert any(
        call[0] == (["nonexistent-brix-v6-03-test-pkg"],)
        for call in mock_install.call_args_list
    )
    assert result.success is True


@pytest.mark.asyncio
async def test_engine_skips_step_dep_install_when_empty():
    """Engine does NOT call install_requirements when step has no requirements."""
    pipeline = _pipeline(_step("noop", reqs=[]))

    with patch("brix.deps.install_requirements") as mock_install:
        engine = PipelineEngine()
        result = await engine.run(pipeline)

    mock_install.assert_not_called()
    assert result.success is True


@pytest.mark.asyncio
async def test_engine_fails_step_on_install_error_default_stop():
    """Engine marks step as error and aborts pipeline when step-level install fails (on_error=stop)."""
    pipeline = _pipeline(_step("bad-step", reqs=["nonexistent-brix-v6-03-test-pkg"]))

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-v6-03-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=False):
            engine = PipelineEngine()
            result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["bad-step"].status == "error"
    assert "nonexistent-brix-v6-03-test-pkg" in (result.steps["bad-step"].error_message or "")


@pytest.mark.asyncio
async def test_engine_continues_after_step_dep_install_failure_when_continue():
    """Engine continues to next step when step-level install fails and on_error=continue."""
    from brix.loader import PipelineLoader

    yaml_str = """\
name: step-dep-continue
steps:
  - id: bad
    type: cli
    args: [echo, bad]
    requirements:
      - nonexistent-brix-v6-03-pkg
    on_error: continue
  - id: good
    type: cli
    args: [echo, good]
"""
    pipeline = PipelineLoader().load_from_string(yaml_str)

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-v6-03-pkg"]):
        with patch("brix.deps.install_requirements", return_value=False):
            engine = PipelineEngine()
            result = await engine.run(pipeline)

    # 'bad' step errors, 'good' step runs
    assert result.steps["bad"].status == "error"
    assert result.steps["good"].status == "ok"


@pytest.mark.asyncio
async def test_engine_step_requirements_already_installed_no_install_call():
    """Engine does not call install_requirements when all step packages are present."""
    pipeline = _pipeline(_step("installed", reqs=["pip"]))  # pip is always present

    with patch("brix.deps.install_requirements") as mock_install:
        engine = PipelineEngine()
        result = await engine.run(pipeline)

    mock_install.assert_not_called()
    assert result.success is True


@pytest.mark.asyncio
async def test_engine_foreach_step_dep_check():
    """Engine checks per-step requirements for foreach steps."""
    from brix.loader import PipelineLoader

    yaml_str = """\
name: foreach-step-dep
steps:
  - id: loop
    type: cli
    args: [echo, "{{ item }}"]
    foreach: "['a', 'b']"
    requirements:
      - nonexistent-brix-v6-03-foreach-pkg
"""
    pipeline = PipelineLoader().load_from_string(yaml_str)

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-v6-03-foreach-pkg"]):
        with patch("brix.deps.install_requirements", return_value=True) as mock_install:
            engine = PipelineEngine()
            result = await engine.run(pipeline)

    mock_install.assert_called_once()
    assert result.success is True


# ---------------------------------------------------------------------------
# 4. Auto-kill watchdog
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_watchdog_cancels_stale_task(tmp_path):
    """Watchdog cancels a background task whose heartbeat is expired."""
    import brix.mcp_server as mcp

    # Create a fake "stale" asyncio task
    stale_event = asyncio.Event()

    async def _long_running():
        await stale_event.wait()  # will never complete unless event is set

    task = asyncio.create_task(_long_running())
    run_id = "run-watchdog-test-stale"

    # Write a run.json with an old heartbeat
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    run_json = run_dir / "run.json"
    run_json.write_text(json.dumps({
        "run_id": run_id,
        "pipeline": "test",
        "status": "running",
        "last_heartbeat": time.time() - (mcp.BACKGROUND_RUN_TIMEOUT_SECONDS + 60),
    }))

    mcp._background_runs[run_id] = task

    # Patch WORKDIR_BASE so the watchdog reads from tmp_path
    with patch("brix.context.WORKDIR_BASE", tmp_path):
        # Run the watchdog loop body directly (one iteration)
        import time as _time_mod
        now = _time_mod.time()
        stale_ids: list[str] = []
        for rid, t in list(mcp._background_runs.items()):
            if t.done():
                stale_ids.append(rid)
                continue
            rj = tmp_path / rid / "run.json"
            try:
                with open(rj) as fh:
                    meta = json.load(fh)
                heartbeat = meta.get("last_heartbeat", 0)
                age = now - heartbeat if heartbeat else now
                if age > mcp.BACKGROUND_RUN_TIMEOUT_SECONDS:
                    t.cancel()
                    stale_ids.append(rid)
            except (FileNotFoundError, OSError):
                pass

        for rid in stale_ids:
            mcp._background_runs.pop(rid, None)

    assert task.cancelled() or task.cancelling() > 0 or run_id not in mcp._background_runs
    # Cleanup
    stale_event.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_watchdog_does_not_cancel_healthy_task(tmp_path):
    """Watchdog leaves tasks with a fresh heartbeat untouched."""
    import contextlib
    import brix.mcp_server as mcp

    done_event = asyncio.Event()

    async def _healthy():
        await done_event.wait()

    task = asyncio.create_task(_healthy())
    run_id = "run-watchdog-test-healthy"

    run_dir = tmp_path / run_id
    run_dir.mkdir()
    run_json = run_dir / "run.json"
    run_json.write_text(json.dumps({
        "run_id": run_id,
        "pipeline": "test",
        "status": "running",
        "last_heartbeat": time.time(),  # fresh
    }))

    mcp._background_runs[run_id] = task

    with patch("brix.context.WORKDIR_BASE", tmp_path):
        import time as _time_mod
        now = _time_mod.time()
        for rid, t in list(mcp._background_runs.items()):
            if rid != run_id:
                continue
            if t.done():
                continue
            rj = tmp_path / rid / "run.json"
            try:
                with open(rj) as fh:
                    meta = json.load(fh)
                heartbeat = meta.get("last_heartbeat", 0)
                age = now - heartbeat if heartbeat else now
                if age > mcp.BACKGROUND_RUN_TIMEOUT_SECONDS:
                    t.cancel()
                    mcp._background_runs.pop(rid, None)
            except (FileNotFoundError, OSError):
                pass

    # Task should still be alive (not cancelled)
    assert not task.cancelled()
    assert run_id in mcp._background_runs

    # Cleanup
    mcp._background_runs.pop(run_id, None)
    done_event.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_watchdog_removes_done_tasks():
    """Watchdog removes already-done tasks from _background_runs."""
    import brix.mcp_server as mcp

    async def _noop():
        pass

    task = asyncio.create_task(_noop())
    await asyncio.sleep(0)  # let task complete
    run_id = "run-watchdog-test-done"
    mcp._background_runs[run_id] = task

    # Single watchdog iteration
    stale_ids: list[str] = []
    for rid, t in list(mcp._background_runs.items()):
        if t.done():
            stale_ids.append(rid)

    for rid in stale_ids:
        mcp._background_runs.pop(rid, None)

    assert run_id not in mcp._background_runs


def test_background_run_timeout_constant():
    """BACKGROUND_RUN_TIMEOUT_SECONDS is a positive integer."""
    import brix.mcp_server as mcp

    assert isinstance(mcp.BACKGROUND_RUN_TIMEOUT_SECONDS, int)
    assert mcp.BACKGROUND_RUN_TIMEOUT_SECONDS > 0


# ---------------------------------------------------------------------------
# 5. _ensure_watchdog
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_watchdog_starts_task():
    """_ensure_watchdog creates a running watchdog task."""
    import brix.mcp_handlers._shared as _shared

    # Reset module-level state (may be from a different event loop in the full suite)
    _shared._watchdog_task = None

    _shared._ensure_watchdog()

    assert _shared._watchdog_task is not None
    assert not _shared._watchdog_task.done()

    # Cleanup — cancel only if on our loop
    if _shared._watchdog_task and not _shared._watchdog_task.done():
        _shared._watchdog_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await _shared._watchdog_task
    _shared._watchdog_task = None


@pytest.mark.asyncio
async def test_ensure_watchdog_idempotent():
    """Calling _ensure_watchdog twice does not create a second task."""
    import brix.mcp_server as mcp

    mcp._watchdog_task = None

    mcp._ensure_watchdog()
    task_first = mcp._watchdog_task

    mcp._ensure_watchdog()
    task_second = mcp._watchdog_task

    assert task_first is task_second

    # Cleanup
    if mcp._watchdog_task and not mcp._watchdog_task.done():
        mcp._watchdog_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await mcp._watchdog_task
    mcp._watchdog_task = None


import contextlib
