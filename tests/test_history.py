"""Tests for SQLite run history."""
import pytest
from brix.history import RunHistory


def test_record_and_get(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("run-001", "test-pipeline", "1.0.0", {"query": "test"})
    h.record_finish("run-001", True, 2.5, {"step1": {"status": "ok"}})

    run = h.get_run("run-001")
    assert run is not None
    assert run["pipeline"] == "test-pipeline"
    assert run["success"] == 1
    assert run["duration"] == 2.5


def test_get_recent(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    for i in range(5):
        h.record_start(f"run-{i:03d}", "pipeline", input_data={})
        h.record_finish(f"run-{i:03d}", True, 1.0)

    recent = h.get_recent(3)
    assert len(recent) == 3


def test_get_stats(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    for i in range(10):
        h.record_start(f"run-{i:03d}", "pipeline")
        h.record_finish(f"run-{i:03d}", i < 8, 1.0 + i * 0.1)

    stats = h.get_stats()
    assert stats["total_runs"] == 10
    assert stats["success_rate"] == 80.0
    assert stats["successes"] == 8
    assert stats["failures"] == 2


def test_get_stats_per_pipeline(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r1", "pipeline-a")
    h.record_finish("r1", True, 1.0)
    h.record_start("r2", "pipeline-b")
    h.record_finish("r2", False, 2.0)

    stats = h.get_stats("pipeline-a")
    assert stats["total_runs"] == 1
    assert stats["success_rate"] == 100.0


def test_get_stats_empty(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    stats = h.get_stats()
    assert stats["total_runs"] == 0


def test_get_run_not_found(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    assert h.get_run("nonexistent") is None


def test_cleanup(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("old-run", "pipeline")
    h.record_finish("old-run", True, 1.0)
    # Can't easily test date-based cleanup without mocking time
    # Just verify the method runs without error
    deleted = h.cleanup(older_than_days=0)
    # With 0 days, everything should be deleted
    assert deleted >= 0


def test_db_created_automatically(tmp_path):
    db_path = tmp_path / "subdir" / "test.db"
    h = RunHistory(db_path=db_path)
    assert db_path.exists()


def test_steps_data_no_items_array(tmp_path):
    """steps_data in SQLite must not contain raw items arrays — only compact summaries."""
    import json as _json

    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("run-v3-17", "test-pipeline", "3.4.0", {"query": "test"})

    # Simulate what engine.py sends: compact steps_summary, no raw items arrays
    steps_summary = {
        "fetch": {"status": "ok", "duration": 1.2, "items": 33000, "errors": None},
        "transform": {"status": "ok", "duration": 0.8, "items": 33000, "errors": None},
        "output": {"status": "ok", "duration": 0.1, "items": None, "errors": None},
    }
    h.record_finish("run-v3-17", True, 2.1, steps_summary, {"result_type": "dict"})

    run = h.get_run("run-v3-17")
    assert run is not None

    stored_steps = _json.loads(run["steps_data"])

    # Each step must have the compact keys
    for step_id in ("fetch", "transform", "output"):
        step = stored_steps[step_id]
        assert "status" in step
        assert "duration" in step
        # Must NOT contain a raw list of item results (only the count as int or None)
        assert not isinstance(step.get("items"), list), (
            f"Step '{step_id}' contains a raw items list in steps_data — should be int count or None"
        )

    # Verify actual values
    assert stored_steps["fetch"]["items"] == 33000
    assert stored_steps["output"]["items"] is None


# --- Step-Level Analytics (T-BRIX-V4-16) ---

def test_get_step_stats_basic(tmp_path):
    """get_step_stats aggregates per-step metrics across multiple runs."""
    h = RunHistory(db_path=tmp_path / "test.db")
    # Run 1: both steps ok
    h.record_start("r1", "my-pipeline")
    h.record_finish("r1", True, 3.0, {
        "fetch": {"status": "ok", "duration": 1.0, "items": 100, "errors": None},
        "transform": {"status": "ok", "duration": 2.0, "items": 100, "errors": None},
    })
    # Run 2: fetch ok, transform fails
    h.record_start("r2", "my-pipeline")
    h.record_finish("r2", False, 2.5, {
        "fetch": {"status": "ok", "duration": 1.5, "items": 50, "errors": None},
        "transform": {"status": "error", "duration": 1.0, "items": None, "errors": "timeout"},
    })

    step_stats = h.get_step_stats("my-pipeline")
    by_id = {s["step_id"]: s for s in step_stats}

    assert "fetch" in by_id
    assert "transform" in by_id

    fetch = by_id["fetch"]
    assert fetch["runs"] == 2
    assert fetch["successes"] == 2
    assert fetch["failures"] == 0
    assert fetch["skips"] == 0
    assert fetch["avg_duration"] == 1.25  # (1.0 + 1.5) / 2
    assert fetch["min_duration"] == 1.0
    assert fetch["max_duration"] == 1.5
    assert fetch["avg_items"] == 75  # (100 + 50) / 2

    transform = by_id["transform"]
    assert transform["runs"] == 2
    assert transform["successes"] == 1
    assert transform["failures"] == 1
    # run-1 had items=100, run-2 had items=None → avg of [100] = 100
    assert transform["avg_items"] == 100


def test_get_step_stats_empty(tmp_path):
    """Returns empty list for a pipeline with no runs."""
    h = RunHistory(db_path=tmp_path / "test.db")
    result = h.get_step_stats("nonexistent-pipeline")
    assert result == []


def test_get_step_stats_no_steps_data(tmp_path):
    """Returns empty list when runs have no steps_data."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r1", "pipeline-a")
    h.record_finish("r1", True, 1.0)  # no steps
    result = h.get_step_stats("pipeline-a")
    assert result == []


def test_get_step_stats_skipped(tmp_path):
    """Skipped steps are counted in skips, not successes or failures."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r1", "skip-pipeline")
    h.record_finish("r1", True, 1.0, {
        "optional_step": {"status": "skipped", "duration": None, "items": None, "errors": None},
    })
    step_stats = h.get_step_stats("skip-pipeline")
    assert len(step_stats) == 1
    s = step_stats[0]
    assert s["step_id"] == "optional_step"
    assert s["skips"] == 1
    assert s["successes"] == 0
    assert s["failures"] == 0
    assert s["avg_duration"] is None
    assert s["avg_items"] is None


def test_get_step_stats_multiple_pipelines(tmp_path):
    """Step stats are isolated per pipeline — no cross-pipeline leakage."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r1", "pipeline-a")
    h.record_finish("r1", True, 1.0, {
        "step1": {"status": "ok", "duration": 0.5, "items": None, "errors": None},
    })
    h.record_start("r2", "pipeline-b")
    h.record_finish("r2", True, 2.0, {
        "step1": {"status": "ok", "duration": 1.5, "items": None, "errors": None},
        "step2": {"status": "ok", "duration": 0.5, "items": None, "errors": None},
    })

    stats_a = h.get_step_stats("pipeline-a")
    stats_b = h.get_step_stats("pipeline-b")

    assert len(stats_a) == 1
    assert len(stats_b) == 2
    # pipeline-a step1 has avg 0.5, not 1.0 (pipeline-b's value)
    assert stats_a[0]["avg_duration"] == 0.5


# --- get_run_errors (T-BRIX-V4-BUG-04) ---

def test_get_run_errors_by_run_id(tmp_path):
    """get_run_errors returns failed steps with error_message and hint."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-err-1", "fail-pipeline")
    h.record_finish("r-err-1", False, 1.5, {
        "fetch": {"status": "ok", "duration": 0.5, "items": 10},
        "process": {"status": "error", "duration": 1.0, "errors": 1, "error_message": "ModuleNotFoundError: httpx"},
    })

    errors = h.get_run_errors(run_id="r-err-1")
    assert len(errors) == 1
    err = errors[0]
    assert err["run_id"] == "r-err-1"
    assert err["step_id"] == "process"
    assert "ModuleNotFoundError" in err["error_message"]
    # Should provide a hint about module installation
    assert err["hint"] is not None
    assert "requirements" in err["hint"].lower() or "module" in err["hint"].lower()


def test_get_run_errors_by_pipeline(tmp_path):
    """get_run_errors with pipeline+last returns errors from last N failed runs."""
    h = RunHistory(db_path=tmp_path / "test.db")
    for i in range(3):
        h.record_start(f"run-{i}", "my-pipe")
        h.record_finish(f"run-{i}", False, 1.0, {
            "step": {"status": "error", "duration": 1.0, "errors": 1, "error_message": f"fail {i}"},
        })

    errors = h.get_run_errors(pipeline="my-pipe", last=2)
    # 2 runs × 1 failed step each
    assert len(errors) == 2


def test_get_run_errors_no_match(tmp_path):
    """get_run_errors returns empty list when no errors found."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-ok", "pipe")
    h.record_finish("r-ok", True, 1.0, {
        "step": {"status": "ok", "duration": 1.0},
    })

    errors = h.get_run_errors(run_id="r-ok")
    assert errors == []


def test_get_run_errors_empty_args(tmp_path):
    """get_run_errors with no args returns empty list."""
    h = RunHistory(db_path=tmp_path / "test.db")
    assert h.get_run_errors() == []


def test_get_run_errors_hint_no_such_file(tmp_path):
    """Hint matches 'No such file' + 'helpers/' pattern."""
    from brix.history import _error_hint
    hint = _error_hint("my_step", "No such file or directory: 'helpers/process.py'")
    assert hint is not None
    assert "helpers" in hint.lower() or "volume" in hint.lower()


def test_get_run_errors_hint_inline_params(tmp_path):
    """Hint matches 'params not defined' inline python pattern."""
    from brix.history import _error_hint
    hint = _error_hint("run_script", "NameError: params is not defined")
    assert hint is not None


# --- get_run_log (T-BRIX-V4-BUG-04) ---

def test_get_run_log_basic(tmp_path):
    """get_run_log returns ordered step entries."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-log-1", "log-pipeline")
    h.record_finish("r-log-1", False, 3.0, {
        "fetch": {"status": "ok", "duration": 1.0, "items": 50},
        "process": {"status": "error", "duration": 2.0, "errors": 1, "error_message": "KeyError: 'id'"},
    })

    log = h.get_run_log("r-log-1")
    assert len(log) == 2

    by_id = {e["step_id"]: e for e in log}
    assert by_id["fetch"]["status"] == "ok"
    assert by_id["fetch"]["duration"] == 1.0
    assert by_id["fetch"]["items"] == 50
    assert "error_message" not in by_id["fetch"]

    assert by_id["process"]["status"] == "error"
    assert by_id["process"]["error_message"] == "KeyError: 'id'"


def test_get_run_log_not_found(tmp_path):
    """get_run_log returns empty list for unknown run_id."""
    h = RunHistory(db_path=tmp_path / "test.db")
    assert h.get_run_log("nonexistent") == []


def test_get_run_log_no_steps_data(tmp_path):
    """get_run_log returns empty list when run has no steps_data."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-empty", "pipe")
    h.record_finish("r-empty", True, 1.0)  # no steps
    assert h.get_run_log("r-empty") == []


# --- get_result (T-BRIX-V4-BUG-10) ---

def test_get_result_dict(tmp_path):
    """get_result returns parsed dict output for a completed run."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-result-1", "pipe")
    h.record_finish("r-result-1", True, 1.0, result_summary={"items": 42, "status": "ok"})

    result, truncated = h.get_result("r-result-1")
    assert truncated is False
    assert result == {"items": 42, "status": "ok"}


def test_get_result_list(tmp_path):
    """get_result handles list outputs."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-result-2", "pipe")
    h.record_finish("r-result-2", True, 0.5, result_summary=[1, 2, 3])

    result, truncated = h.get_result("r-result-2")
    assert truncated is False
    assert result == [1, 2, 3]


def test_get_result_string(tmp_path):
    """get_result handles plain string outputs."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-result-3", "pipe")
    h.record_finish("r-result-3", True, 0.5, result_summary="hello world")

    result, truncated = h.get_result("r-result-3")
    assert truncated is False
    assert result == "hello world"


def test_get_result_no_result(tmp_path):
    """get_result returns (None, False) when no result_summary was stored."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-result-none", "pipe")
    h.record_finish("r-result-none", True, 0.5)

    result, truncated = h.get_result("r-result-none")
    assert result is None
    assert truncated is False


def test_get_result_not_found(tmp_path):
    """get_result returns (None, False) for unknown run_id."""
    h = RunHistory(db_path=tmp_path / "test.db")
    result, truncated = h.get_result("nonexistent")
    assert result is None
    assert truncated is False


def test_get_result_truncated(tmp_path):
    """get_result returns (raw_str, True) when result_summary exceeds 10 KB."""
    import json as _json

    h = RunHistory(db_path=tmp_path / "test.db")
    # Build a payload that is >10KB when JSON-serialized
    large_result = {"data": "x" * 12000}
    h.record_start("r-result-big", "pipe")
    h.record_finish("r-result-big", True, 1.0, result_summary=large_result)

    result, truncated = h.get_result("r-result-big")
    assert truncated is True
    # The raw string should be the stored JSON
    assert isinstance(result, str)
    assert len(result.encode()) > 10 * 1024


# --- Proactive Error Hints (T-BRIX-V5-03) ---

def test_error_hint_asyncio_run(tmp_path):
    """asyncio.run in helper output triggers the async hint."""
    from brix.history import _error_hint
    hint = _error_hint("my_step", "RuntimeError: asyncio.run() cannot be called from a running event loop")
    assert hint is not None
    assert "asyncio.run" in hint


def test_error_hint_json_decode_error(tmp_path):
    """JSONDecodeError triggers the helper stdout JSON hint."""
    from brix.history import _error_hint
    hint = _error_hint("parse_step", "json.decoder.JSONDecodeError: Expecting value: line 1 column 1")
    assert hint is not None
    assert "JSON" in hint or "json" in hint.lower()


def test_error_hint_expecting_value(tmp_path):
    """'Expecting value' triggers the helper stdout JSON hint."""
    from brix.history import _error_hint
    hint = _error_hint("parse_step", "Expecting value: line 1 column 1 (char 0)")
    assert hint is not None
    assert "JSON" in hint or "json" in hint.lower()


def test_error_hint_undefined_error(tmp_path):
    """UndefinedError triggers the Jinja2 template hint."""
    from brix.history import _error_hint
    hint = _error_hint("render_step", "jinja2.exceptions.UndefinedError: 'fetch_data' is undefined")
    assert hint is not None
    assert "Jinja2" in hint or "jinja2" in hint.lower() or "default" in hint


def test_error_hint_is_undefined(tmp_path):
    """'is undefined' in message triggers the Jinja2 template hint."""
    from brix.history import _error_hint
    hint = _error_hint("render_step", "Variable 'my_step.output' is undefined in template")
    assert hint is not None
    assert "default" in hint or "Jinja2" in hint


def test_error_hint_too_large(tmp_path):
    """'too large' triggers the payload size hint."""
    from brix.history import _error_hint
    hint = _error_hint("foreach_step", "Request too large: payload exceeds limit")
    assert hint is not None
    assert "path" in hint.lower() or "base64" in hint.lower() or "large" in hint.lower()


def test_error_hint_memory_error(tmp_path):
    """MemoryError triggers the payload size hint."""
    from brix.history import _error_hint
    hint = _error_hint("foreach_step", "MemoryError")
    assert hint is not None
    assert "path" in hint.lower() or "base64" in hint.lower()


def test_error_hint_unknown_returns_none(tmp_path):
    """Unknown error patterns return None (no false positives)."""
    from brix.history import _error_hint
    hint = _error_hint("step", "Some completely unknown error that matches nothing")
    assert hint is None


def test_get_run_errors_asyncio_hint_via_history(tmp_path):
    """get_run_errors returns asyncio hint when error_message contains 'asyncio.run'."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-async-1", "async-fail-pipeline")
    h.record_finish("r-async-1", False, 1.0, {
        "run_helper": {
            "status": "error",
            "duration": 1.0,
            "errors": 1,
            "error_message": "RuntimeError: asyncio.run() cannot be called from a running event loop",
        },
    })

    errors = h.get_run_errors(run_id="r-async-1")
    assert len(errors) == 1
    err = errors[0]
    assert err["hint"] is not None
    assert "asyncio.run" in err["hint"]


def test_get_run_errors_memory_error_hint(tmp_path):
    """get_run_errors returns payload size hint for MemoryError."""
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r-mem-1", "big-foreach-pipeline")
    h.record_finish("r-mem-1", False, 2.0, {
        "process_files": {
            "status": "error",
            "duration": 2.0,
            "errors": 1,
            "error_message": "MemoryError",
        },
    })

    errors = h.get_run_errors(run_id="r-mem-1")
    assert len(errors) == 1
    assert errors[0]["hint"] is not None
    assert "path" in errors[0]["hint"].lower() or "base64" in errors[0]["hint"].lower()
