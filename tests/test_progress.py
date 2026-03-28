"""Tests for ProgressReporter structured progress streaming."""
import asyncio
import io
import json
import os
import tempfile

import pytest

from brix.progress import ProgressReporter


def make_reporter():
    """Create a ProgressReporter writing to an in-memory stream."""
    stream = io.StringIO()
    return ProgressReporter(stream=stream), stream


def test_progress_pipeline_start():
    reporter, stream = make_reporter()
    reporter.pipeline_start("my-pipeline", 5)
    out = stream.getvalue()
    assert "my-pipeline" in out
    assert "5" in out


def test_progress_step_ok():
    reporter, stream = make_reporter()
    reporter.step_start("fetch_data", "http")
    reporter.step_ok("fetch_data", 1.23)
    out = stream.getvalue()
    assert "1.2s" in out


def test_progress_step_ok_with_items():
    reporter, stream = make_reporter()
    reporter.step_start("fetch_data", "http")
    reporter.step_ok("fetch_data", 0.5, items=42)
    out = stream.getvalue()
    assert "42 items" in out


def test_progress_step_error():
    reporter, stream = make_reporter()
    reporter.step_start("broken_step", "cli")
    reporter.step_error("broken_step", "connection refused", 0.1)
    out = stream.getvalue()
    assert "connection refused" in out
    assert "FAILED" in out


def test_progress_step_skipped():
    reporter, stream = make_reporter()
    reporter.step_skipped("optional_step", "condition not met")
    out = stream.getvalue()
    assert "skipped" in out
    assert "optional_step" in out


def test_progress_foreach_progress():
    reporter, stream = make_reporter()
    reporter.foreach_progress("process_items", current=5, total=10)
    out = stream.getvalue()
    assert "5/10" in out
    # Progress bar characters
    assert "█" in out or "░" in out


def test_progress_foreach_progress_with_failed():
    reporter, stream = make_reporter()
    reporter.foreach_progress("process_items", current=8, total=10, failed=2)
    out = stream.getvalue()
    assert "2 failed" in out


def test_progress_pipeline_done_success():
    reporter, stream = make_reporter()
    reporter.pipeline_done("my-pipeline", success=True, duration=3.7, step_count=4)
    out = stream.getvalue()
    assert "Done" in out
    assert "4 steps" in out
    assert "3.7s" in out


def test_progress_pipeline_done_failure():
    reporter, stream = make_reporter()
    reporter.pipeline_done("my-pipeline", success=False, duration=1.0, step_count=2)
    out = stream.getvalue()
    assert "FAILED" in out


def test_progress_log_entries():
    reporter, stream = make_reporter()
    reporter.pipeline_start("test-pipeline", 3)
    reporter.step_start("step1", "cli")
    reporter.step_ok("step1", 0.5)
    reporter.pipeline_done("test-pipeline", True, 0.5, 3)

    entries = reporter.log_entries
    assert len(entries) == 4

    events = [e["event"] for e in entries]
    assert "pipeline_start" in events
    assert "step_start" in events
    assert "step_ok" in events
    assert "pipeline_done" in events


def test_progress_log_entries_have_timestamps():
    reporter, stream = make_reporter()
    reporter.pipeline_start("test", 1)
    entries = reporter.log_entries
    assert len(entries) == 1
    assert "timestamp" in entries[0]
    assert isinstance(entries[0]["timestamp"], float)


def test_progress_log_entries_have_correct_fields():
    reporter, stream = make_reporter()
    reporter.step_start("my_step", "python", "detail info")
    entry = reporter.log_entries[0]
    assert entry["event"] == "step_start"
    assert entry["step"] == "my_step"
    assert entry["type"] == "python"


def test_progress_log_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        log_path = f.name

    try:
        stream = io.StringIO()
        reporter = ProgressReporter(stream=stream, log_file=log_path)
        reporter.pipeline_start("file-test", 2)
        reporter.step_start("step1", "cli")
        reporter.step_ok("step1", 0.3)
        reporter.pipeline_done("file-test", True, 0.3, 2)

        # Read back the log file
        with open(log_path) as f:
            lines = [line.strip() for line in f if line.strip()]

        assert len(lines) == 4
        for line in lines:
            entry = json.loads(line)
            assert "event" in entry
            assert "timestamp" in entry
    finally:
        os.unlink(log_path)


def test_progress_log_file_step_error():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        log_path = f.name

    try:
        stream = io.StringIO()
        reporter = ProgressReporter(stream=stream, log_file=log_path)
        reporter.step_error("bad_step", "timeout", 5.0)

        with open(log_path) as f:
            lines = [line.strip() for line in f if line.strip()]

        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["event"] == "step_error"
        assert entry["error"] == "timeout"
    finally:
        os.unlink(log_path)


def test_progress_step_resumed():
    reporter, stream = make_reporter()
    reporter.step_resumed("cached_step")
    out = stream.getvalue()
    assert "cached_step" in out
    assert "resumed" in out

    entries = reporter.log_entries
    assert entries[0]["event"] == "step_resumed"


def test_progress_retry():
    reporter, stream = make_reporter()
    reporter.retry("flaky_step", attempt=2, max_attempts=3, error="timeout")
    out = stream.getvalue()
    assert "2/3" in out
    assert "timeout" in out

    entry = reporter.log_entries[0]
    assert entry["event"] == "retry"
    assert entry["attempt"] == 2
    assert entry["max_attempts"] == 3


def test_progress_foreach_done():
    reporter, stream = make_reporter()
    reporter.foreach_done("batch_step", total=10, succeeded=8, failed=2, duration=4.5)
    out = stream.getvalue()
    assert "8/10" in out
    assert "4.5s" in out
    assert "2 failed" in out

    entry = reporter.log_entries[0]
    assert entry["event"] == "foreach_done"
    assert entry["succeeded"] == 8
    assert entry["failed"] == 2


# --- T-BRIX-V3-14: foreach_progress called in engine ---

def test_foreach_progress_called_in_engine():
    """After a foreach run, ProgressReporter log_entries must contain foreach_progress events."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    from brix.engine import PipelineEngine
    from brix.models import Pipeline, Step, ErrorConfig, InputParam

    stream = io.StringIO()

    # Build a minimal pipeline with a foreach step
    step = Step(
        id="process_items",
        type="python",
        foreach="{{ input.data_list }}",
        script="result = item",
        parallel=False,
    )
    pipeline = Pipeline(
        name="test-foreach-progress",
        steps=[step],
        input={"data_list": InputParam(type="list")},
        error_handling=ErrorConfig(on_error="continue"),
    )

    # Patch PythonRunner.execute to return success immediately
    async def fake_execute(rendered_step, context):
        return {"success": True, "data": "ok", "duration": 0.01}

    engine = PipelineEngine()
    engine.progress = ProgressReporter(stream=stream)
    engine._runners["python"].execute = fake_execute

    result = asyncio.run(engine.run(pipeline, user_input={"data_list": ["a", "b", "c"]}))

    events = [e["event"] for e in engine.progress.log_entries]
    assert "foreach_progress" in events, f"Expected foreach_progress in events, got: {events}"

    progress_entries = [e for e in engine.progress.log_entries if e["event"] == "foreach_progress"]
    assert len(progress_entries) == 3  # one per item
    assert progress_entries[-1]["current"] == 3
    assert progress_entries[-1]["total"] == 3


# --- T-BRIX-V4-BUG-02: McpProgressReporter sends MCP progress notifications ---

class TestMcpProgressReporter:
    """McpProgressReporter forwards pipeline events as MCP progress notifications."""

    def _make_mock_session(self):
        """Return an async-mock session and a list that collects sent notifications."""
        from unittest.mock import AsyncMock
        calls: list[dict] = []

        session = AsyncMock()

        async def capture_progress(progress_token, progress, total, message=None):
            calls.append({"token": progress_token, "progress": progress, "total": total, "message": message})

        session.send_progress_notification.side_effect = capture_progress
        return session, calls

    @pytest.mark.asyncio
    async def test_pipeline_start_sends_notification(self):
        from brix.progress import McpProgressReporter
        session, calls = self._make_mock_session()
        reporter = McpProgressReporter(session=session, progress_token="tok-1")
        reporter.pipeline_start("my-pipeline", 4)
        # Allow the scheduled task to run
        await asyncio.sleep(0)
        assert len(calls) == 1
        assert calls[0]["token"] == "tok-1"
        assert calls[0]["progress"] == 0
        assert calls[0]["total"] == 4
        assert "my-pipeline" in calls[0]["message"]

    @pytest.mark.asyncio
    async def test_step_ok_increments_counter(self):
        from brix.progress import McpProgressReporter
        session, calls = self._make_mock_session()
        reporter = McpProgressReporter(session=session, progress_token="tok-2")
        reporter.pipeline_start("p", 3)
        await asyncio.sleep(0)
        reporter.step_start("s1", "cli")
        await asyncio.sleep(0)
        reporter.step_ok("s1", 1.0)
        await asyncio.sleep(0)
        # step_start sends progress=0 (before increment), step_ok sends progress=1
        step_ok_call = calls[-1]
        assert step_ok_call["progress"] == 1
        assert step_ok_call["total"] == 3

    @pytest.mark.asyncio
    async def test_step_error_increments_counter(self):
        from brix.progress import McpProgressReporter
        session, calls = self._make_mock_session()
        reporter = McpProgressReporter(session=session, progress_token="tok-3")
        reporter.pipeline_start("p", 2)
        await asyncio.sleep(0)
        reporter.step_start("bad", "cli")
        await asyncio.sleep(0)
        reporter.step_error("bad", "timeout", 0.5)
        await asyncio.sleep(0)
        last = calls[-1]
        assert last["progress"] == 1
        assert "FAILED" in last["message"]

    @pytest.mark.asyncio
    async def test_step_skipped_increments_counter(self):
        from brix.progress import McpProgressReporter
        session, calls = self._make_mock_session()
        reporter = McpProgressReporter(session=session, progress_token="tok-4")
        reporter.pipeline_start("p", 2)
        await asyncio.sleep(0)
        reporter.step_skipped("optional", "condition not met")
        await asyncio.sleep(0)
        last = calls[-1]
        assert last["progress"] == 1
        assert "skipped" in last["message"]

    @pytest.mark.asyncio
    async def test_foreach_progress_sends_notification(self):
        from brix.progress import McpProgressReporter
        session, calls = self._make_mock_session()
        reporter = McpProgressReporter(session=session, progress_token="tok-5")
        reporter.pipeline_start("p", 1)
        await asyncio.sleep(0)
        reporter.foreach_progress("batch", 5, 10)
        await asyncio.sleep(0)
        last = calls[-1]
        assert "50%" in last["message"]
        assert "batch" in last["message"]

    @pytest.mark.asyncio
    async def test_no_token_no_notification(self):
        """When progress_token is None, no MCP notifications are sent."""
        from brix.progress import McpProgressReporter
        session, calls = self._make_mock_session()
        reporter = McpProgressReporter(session=session, progress_token=None)
        reporter.pipeline_start("p", 2)
        reporter.step_start("s1", "cli")
        reporter.step_ok("s1", 0.5)
        await asyncio.sleep(0)
        assert calls == []

    @pytest.mark.asyncio
    async def test_pipeline_done_sends_final_notification(self):
        from brix.progress import McpProgressReporter
        session, calls = self._make_mock_session()
        reporter = McpProgressReporter(session=session, progress_token="tok-6")
        reporter.pipeline_start("p", 2)
        await asyncio.sleep(0)
        reporter.pipeline_done("p", success=True, duration=5.0, step_count=2)
        await asyncio.sleep(0)
        last = calls[-1]
        assert last["progress"] == 2
        assert last["total"] == 2
        assert "done" in last["message"]

    @pytest.mark.asyncio
    async def test_notification_error_does_not_raise(self):
        """Errors in send_progress_notification must not propagate to caller."""
        from brix.progress import McpProgressReporter
        from unittest.mock import AsyncMock
        session = AsyncMock()
        session.send_progress_notification.side_effect = RuntimeError("network error")
        reporter = McpProgressReporter(session=session, progress_token="tok-err")
        # Must not raise
        reporter.pipeline_start("p", 1)
        reporter.step_start("s1", "cli")
        reporter.step_ok("s1", 0.1)
        await asyncio.sleep(0)  # let futures run (they swallow errors internally)


# --- T-BRIX-V3-16: progress.jsonl written to workdir ---

def test_progress_writes_to_workdir():
    """ProgressReporter with workdir set writes all log entries to progress.jsonl."""
    with tempfile.TemporaryDirectory() as workdir:
        stream = io.StringIO()
        reporter = ProgressReporter(stream=stream, workdir=workdir)

        reporter.pipeline_start("test-pipeline", 2)
        reporter.step_start("step1", "cli")
        reporter.step_ok("step1", 0.5)
        reporter.foreach_progress("step2", current=3, total=5, failed=1)
        reporter.pipeline_done("test-pipeline", True, 0.5, 2)

        progress_file = os.path.join(workdir, "progress.jsonl")
        assert os.path.exists(progress_file), "progress.jsonl was not created"

        with open(progress_file) as f:
            lines = [line.strip() for line in f if line.strip()]

        assert len(lines) == 5
        events = [json.loads(line)["event"] for line in lines]
        assert "pipeline_start" in events
        assert "foreach_progress" in events
        assert "pipeline_done" in events
