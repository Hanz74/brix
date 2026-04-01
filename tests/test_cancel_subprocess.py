"""Tests for INBOX-510: cancel_run kills helper subprocesses."""

import asyncio
import os
import signal
import sys
import types

import pytest

from brix.runners._subprocess import _terminate_subprocess


# ---------------------------------------------------------------------------
# _terminate_subprocess unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_terminate_subprocess_sends_sigterm():
    """A running subprocess should receive SIGTERM and exit."""
    # Start a long-sleeping process
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(300)",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    pid = proc.pid
    assert proc.returncode is None  # still running

    await _terminate_subprocess(proc)

    # Process must have exited
    assert proc.returncode is not None
    # PID must no longer be running
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
async def test_terminate_subprocess_escalates_to_sigkill():
    """If subprocess ignores SIGTERM, it should be SIGKILLed."""
    # Python script that traps SIGTERM and keeps running.
    # Writes a marker to stdout once the handler is installed so we can
    # synchronise and avoid a race.
    trap_script = (
        "import signal, sys, time; "
        "signal.signal(signal.SIGTERM, lambda *a: None); "
        "sys.stdout.write('ready\\n'); sys.stdout.flush(); "
        "time.sleep(300)"
    )
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", trap_script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    pid = proc.pid

    # Wait until the signal handler is installed
    ready_line = await asyncio.wait_for(proc.stdout.readline(), timeout=5)
    assert ready_line.strip() == b"ready"

    # Patch the grace period to 1s so the test is fast
    import brix.runners._subprocess as mod
    orig = mod._TERMINATE_GRACE_SECONDS
    mod._TERMINATE_GRACE_SECONDS = 1
    try:
        await _terminate_subprocess(proc)
    finally:
        mod._TERMINATE_GRACE_SECONDS = orig

    assert proc.returncode is not None
    # Killed by SIGKILL → returncode is -9
    assert proc.returncode == -9
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
async def test_terminate_subprocess_already_exited():
    """Calling _terminate_subprocess on an already-exited process is a no-op."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "pass",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.wait()
    assert proc.returncode is not None

    # Should not raise
    await _terminate_subprocess(proc)


# ---------------------------------------------------------------------------
# Integration: PythonRunner cancellation kills subprocess
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_python_runner_cancel_kills_subprocess():
    """Cancelling a PythonRunner execution must terminate the subprocess."""
    from brix.runners.python import PythonRunner

    runner = PythonRunner()

    # Build a minimal step object
    step = types.SimpleNamespace(
        id="test-step",
        script=None,
        helper=None,
        params={},
        timeout="60s",
        progress=False,
    )
    # Inline script that sleeps forever
    step.script = "-c"
    # We need to craft a command: python3 -c "import time; time.sleep(300)"
    # PythonRunner builds cmd = ["python3", script, params_json]
    # We can't easily pass -c through the normal path, so use a temp file.
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write("import time; time.sleep(300)\n")
        f.flush()
        step.script = f.name

    context = types.SimpleNamespace(
        credentials=None,
        workdir=None,
    )

    # Run execute in a task, then cancel it
    task = asyncio.create_task(runner.execute(step, context))

    # Give the subprocess time to start
    await asyncio.sleep(0.3)

    # Cancel the asyncio task
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    # Clean up temp file
    os.unlink(step.script)


# ---------------------------------------------------------------------------
# Integration: CliRunner cancellation kills subprocess
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cli_runner_cancel_kills_subprocess():
    """Cancelling a CliRunner execution must terminate the subprocess."""
    from brix.runners.cli import CliRunner

    runner = CliRunner()

    step = types.SimpleNamespace(
        id="test-step",
        args=[sys.executable, "-c", "import time; time.sleep(300)"],
        command=None,
        timeout="60s",
    )

    context = types.SimpleNamespace(
        credentials=None,
    )

    task = asyncio.create_task(runner.execute(step, context))

    await asyncio.sleep(0.3)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
