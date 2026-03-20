"""Tests for ProgressReporter structured progress streaming."""
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
