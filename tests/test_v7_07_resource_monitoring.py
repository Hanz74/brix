"""Tests for T-BRIX-V7-07 — Resource Monitoring: Usage, Timeline, Regression, Container-Tracking.

Covers:
1. Resource usage (RSS measurement helpers)
2. resource_usage stored in StepStatus model
3. Container-ID persisted at run start
4. BrixDB.get_step_durations() -- regression data source
5. BrixDB.get_run_timeline() -- chronological step timeline
6. AlertManager step_regression condition
7. HTTP runner X-Brix-Run-Id header
8. MCP tool brix__get_timeline
"""
import asyncio
import json
import os
import sqlite3
from pathlib import Path
from typing import Any
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    from brix.db import BrixDB
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def tmp_db_path(tmp_path):
    return tmp_path / "brix.db"


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    return tmp_path


def _run_async(coro):
    """Run a coroutine in a fresh event loop (avoids 'no current event loop' errors)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# 1. Resource usage helpers
# ---------------------------------------------------------------------------

class TestRSSMeasurement:
    def test_measure_rss_mb_returns_float(self):
        from brix.engine import _measure_rss_mb
        result = _measure_rss_mb()
        assert isinstance(result, float)
        assert result >= 0.0

    def test_measure_rss_mb_positive_on_linux(self):
        """On Linux /proc/self/status should yield a positive RSS."""
        from brix.engine import _measure_rss_mb
        if not Path("/proc/self/status").exists():
            pytest.skip("Not on Linux")
        result = _measure_rss_mb()
        assert result > 0.0

    def test_total_ram_mb_positive(self):
        from brix.engine import _total_ram_mb
        if not Path("/proc/meminfo").exists():
            pytest.skip("Not on Linux")
        result = _total_ram_mb()
        assert result > 0.0

    def test_warn_if_high_memory_no_warning_low_usage(self, capsys):
        from brix.engine import _warn_if_high_memory
        # 10MB RSS vs 10GB RAM -- should not warn
        with mock.patch("brix.engine._total_ram_mb", return_value=10 * 1024.0):
            _warn_if_high_memory(10.0, "step1")
        captured = capsys.readouterr()
        assert "Resource Warning" not in captured.err

    def test_warn_if_high_memory_warns_high_usage(self, capsys):
        from brix.engine import _warn_if_high_memory
        # 900MB RSS vs 1000MB RAM -- should warn (90%)
        with mock.patch("brix.engine._total_ram_mb", return_value=1000.0):
            _warn_if_high_memory(900.0, "step_heavy")
        captured = capsys.readouterr()
        assert "Resource Warning" in captured.err
        assert "step_heavy" in captured.err

    def test_warn_if_high_memory_zero_total_no_crash(self, capsys):
        from brix.engine import _warn_if_high_memory
        # Total RAM unknown -- should not crash or warn
        with mock.patch("brix.engine._total_ram_mb", return_value=0.0):
            _warn_if_high_memory(500.0, "step_x")
        captured = capsys.readouterr()
        assert "Resource Warning" not in captured.err


# ---------------------------------------------------------------------------
# 2. StepStatus resource_usage field
# ---------------------------------------------------------------------------

class TestStepStatusResourceUsage:
    def test_default_resource_usage_is_none(self):
        from brix.models import StepStatus
        s = StepStatus(status="ok", duration=1.0)
        assert s.resource_usage is None

    def test_can_set_resource_usage(self):
        from brix.models import StepStatus
        s = StepStatus(status="ok", duration=1.0, resource_usage={"rss_mb": 42.5, "duration": 1.0})
        assert s.resource_usage == {"rss_mb": 42.5, "duration": 1.0}

    def test_resource_usage_serializes(self):
        from brix.models import StepStatus
        s = StepStatus(status="ok", duration=1.0, resource_usage={"rss_mb": 10.0, "duration": 0.5})
        d = s.model_dump()
        assert d["resource_usage"] == {"rss_mb": 10.0, "duration": 0.5}


# ---------------------------------------------------------------------------
# 3. Container-ID at run start
# ---------------------------------------------------------------------------

class TestContainerIdTracking:
    def test_container_id_column_exists(self, db):
        """The runs table must have a container_id column."""
        with db._connect() as conn:
            cursor = conn.execute("PRAGMA table_info(runs)")
            cols = {row[1] for row in cursor.fetchall()}
        assert "container_id" in cols

    def test_record_run_start_stores_container_id(self, db):
        db.record_run_start(
            run_id="r-container-01",
            pipeline="test-pipe",
            container_id="my-container-abc",
        )
        run = db.get_run("r-container-01")
        assert run is not None
        assert run["container_id"] == "my-container-abc"

    def test_record_run_start_no_container_id_defaults_none(self, db):
        db.record_run_start(run_id="r-container-02", pipeline="test-pipe")
        run = db.get_run("r-container-02")
        assert run is not None
        assert run.get("container_id") is None

    def test_engine_writes_container_id_from_hostname(self, tmp_home):
        """Engine must pass HOSTNAME env var as container_id to history.record_start."""
        from brix.history import RunHistory

        captured_calls = []

        original_record_start = RunHistory.record_start

        def spy_record_start(self, *args, **kwargs):
            captured_calls.append(kwargs)
            return original_record_start(self, *args, **kwargs)

        with mock.patch.dict(os.environ, {"HOSTNAME": "test-container-xyz"}):
            with mock.patch.object(RunHistory, "record_start", spy_record_start):
                from brix.engine import PipelineEngine
                from brix.loader import PipelineLoader
                loader = PipelineLoader()
                pipeline = loader.load_from_string(
                    "name: test\nsteps:\n  - id: s1\n    type: set\n    values:\n      x: 1\n"
                )
                engine = PipelineEngine()
                _run_async(engine.run(pipeline))

        assert len(captured_calls) >= 1
        assert captured_calls[0].get("container_id") == "test-container-xyz"


# ---------------------------------------------------------------------------
# 4. BrixDB.get_step_durations()
# ---------------------------------------------------------------------------

class TestGetStepDurations:
    def _insert_run(self, db, run_id, pipeline, success, steps_data):
        db.record_run_start(run_id=run_id, pipeline=pipeline)
        db.record_run_finish(
            run_id=run_id,
            success=success,
            duration=1.0,
            steps=steps_data,
        )

    def test_returns_empty_when_no_runs(self, db):
        result = db.get_step_durations("no-pipeline", "step1")
        assert result == []

    def test_returns_durations_from_successful_runs(self, db):
        steps = {"s1": {"status": "ok", "duration": 2.5}}
        self._insert_run(db, "r1", "pipe-a", True, steps)
        self._insert_run(db, "r2", "pipe-a", True, steps)

        result = db.get_step_durations("pipe-a", "s1")
        assert len(result) == 2
        assert all(d == 2.5 for d in result)

    def test_excludes_failed_runs(self, db):
        steps_ok = {"s1": {"status": "ok", "duration": 1.0}}
        steps_fail = {"s1": {"status": "error", "duration": 1.0}}
        self._insert_run(db, "r-ok", "pipe-b", True, steps_ok)
        self._insert_run(db, "r-fail", "pipe-b", False, steps_fail)

        result = db.get_step_durations("pipe-b", "s1")
        assert len(result) == 1

    def test_excludes_non_ok_steps(self, db):
        steps = {"s1": {"status": "skipped", "duration": 0.0}}
        self._insert_run(db, "r-skip", "pipe-c", True, steps)

        result = db.get_step_durations("pipe-c", "s1")
        assert result == []

    def test_respects_limit(self, db):
        for i in range(15):
            self._insert_run(
                db, f"r-{i}", "pipe-d", True,
                {"s1": {"status": "ok", "duration": float(i)}}
            )
        result = db.get_step_durations("pipe-d", "s1", limit=5)
        assert len(result) == 5

    def test_returns_chronological_order(self, db):
        for dur in [1.0, 2.0, 3.0]:
            self._insert_run(
                db, f"r-dur-{dur}", "pipe-e", True,
                {"s1": {"status": "ok", "duration": dur}}
            )
        result = db.get_step_durations("pipe-e", "s1")
        assert result == sorted(result), "Expected chronological (ascending) order"


# ---------------------------------------------------------------------------
# 5. BrixDB.get_run_timeline()
# ---------------------------------------------------------------------------

class TestGetRunTimeline:
    def test_returns_empty_for_unknown_run(self, db):
        result = db.get_run_timeline("no-such-run")
        assert result == []

    def test_returns_empty_when_no_steps_data(self, db):
        db.record_run_start(run_id="r-notimeline", pipeline="p")
        db.record_run_finish(run_id="r-notimeline", success=True, duration=1.0, steps=None)
        result = db.get_run_timeline("r-notimeline")
        assert result == []

    def test_timeline_contains_all_steps(self, db):
        steps = {
            "s1": {"status": "ok", "duration": 1.0},
            "s2": {"status": "ok", "duration": 2.0},
            "s3": {"status": "error", "duration": 0.5, "error_message": "oops"},
        }
        db.record_run_start(run_id="r-tl-01", pipeline="p")
        db.record_run_finish(run_id="r-tl-01", success=False, duration=3.5, steps=steps)

        timeline = db.get_run_timeline("r-tl-01")
        assert len(timeline) == 3
        ids = [e["step_id"] for e in timeline]
        assert "s1" in ids
        assert "s2" in ids
        assert "s3" in ids

    def test_timeline_entry_has_required_fields(self, db):
        steps = {"s1": {"status": "ok", "duration": 1.5}}
        db.record_run_start(run_id="r-tl-02", pipeline="p")
        db.record_run_finish(run_id="r-tl-02", success=True, duration=1.5, steps=steps)

        timeline = db.get_run_timeline("r-tl-02")
        assert len(timeline) == 1
        entry = timeline[0]
        assert entry["step_id"] == "s1"
        assert entry["status"] == "ok"
        assert entry["duration"] == 1.5
        assert "start_time" in entry
        assert "end_time" in entry

    def test_timeline_error_message_included(self, db):
        steps = {"s1": {"status": "error", "duration": 0.5, "error_message": "Something went wrong"}}
        db.record_run_start(run_id="r-tl-03", pipeline="p")
        db.record_run_finish(run_id="r-tl-03", success=False, duration=0.5, steps=steps)

        timeline = db.get_run_timeline("r-tl-03")
        assert timeline[0]["error_message"] == "Something went wrong"

    def test_timeline_durations_accumulate_for_timestamps(self, db):
        """end_time of step N should be >= start_time of step N+1."""
        steps = {
            "s1": {"status": "ok", "duration": 1.0},
            "s2": {"status": "ok", "duration": 2.0},
        }
        db.record_run_start(run_id="r-tl-04", pipeline="p")
        db.record_run_finish(run_id="r-tl-04", success=True, duration=3.0, steps=steps)

        timeline = db.get_run_timeline("r-tl-04")
        assert len(timeline) == 2
        # s2.start_time should be >= s1.end_time
        from datetime import datetime, timezone
        s1_end = datetime.fromisoformat(timeline[0]["end_time"])
        s2_start = datetime.fromisoformat(timeline[1]["start_time"])
        assert s2_start >= s1_end


# ---------------------------------------------------------------------------
# 6. Alerting: step_regression condition
# ---------------------------------------------------------------------------

class TestStepRegressionAlert:
    def _build_alert_manager(self, tmp_db_path):
        from brix.alerting import AlertManager
        return AlertManager(db_path=tmp_db_path)

    def _insert_run(self, db, run_id, pipeline, success, steps_data):
        db.record_run_start(run_id=run_id, pipeline=pipeline)
        db.record_run_finish(
            run_id=run_id, success=success, duration=1.0, steps=steps_data,
        )

    def test_step_regression_condition_is_valid(self, tmp_db_path):
        am = self._build_alert_manager(tmp_db_path)
        rule = am.add_rule(
            name="regression-alert",
            condition="step_regression",
            channel="log",
        )
        assert rule.condition == "step_regression"

    def test_step_regression_fires_when_duration_3x_median(self, tmp_db_path):
        from brix.db import BrixDB
        from brix.models import StepStatus
        db = BrixDB(db_path=tmp_db_path)
        am = self._build_alert_manager(tmp_db_path)

        # Insert 5 historical runs with step duration ~1.0s
        for i in range(5):
            self._insert_run(db, f"hist-{i}", "pipe-reg", True,
                             {"s1": {"status": "ok", "duration": 1.0}})

        am.add_rule(name="regr", condition="step_regression", channel="log")

        # Current run: s1 takes 5.0s (5x median of 1.0s) -- should fire
        run_result = {
            "success": True,
            "run_id": "curr-run",
            "pipeline": "pipe-reg",
            "steps": {
                "s1": StepStatus(status="ok", duration=5.0),
            },
        }
        fired = am.check_alerts(run_result)
        assert len(fired) == 1
        assert fired[0]["condition"] == "step_regression"

    def test_step_regression_no_fire_within_threshold(self, tmp_db_path):
        from brix.db import BrixDB
        from brix.models import StepStatus
        db = BrixDB(db_path=tmp_db_path)
        am = self._build_alert_manager(tmp_db_path)

        for i in range(5):
            self._insert_run(db, f"hist2-{i}", "pipe-normal", True,
                             {"s1": {"status": "ok", "duration": 1.0}})

        am.add_rule(name="regr2", condition="step_regression", channel="log")

        # 2.0s vs median 1.0s -- only 2x threshold, below 3x default
        run_result = {
            "success": True,
            "run_id": "curr-run2",
            "pipeline": "pipe-normal",
            "steps": {"s1": StepStatus(status="ok", duration=2.0)},
        }
        fired = am.check_alerts(run_result)
        assert len(fired) == 0

    def test_step_regression_no_fire_insufficient_history(self, tmp_db_path):
        from brix.db import BrixDB
        from brix.models import StepStatus
        db = BrixDB(db_path=tmp_db_path)
        am = self._build_alert_manager(tmp_db_path)

        # Only 1 historical run -- not enough history
        self._insert_run(db, "hist-only", "pipe-new", True,
                         {"s1": {"status": "ok", "duration": 1.0}})

        am.add_rule(name="regr3", condition="step_regression", channel="log")

        run_result = {
            "success": True,
            "run_id": "curr-run3",
            "pipeline": "pipe-new",
            "steps": {"s1": StepStatus(status="ok", duration=100.0)},
        }
        fired = am.check_alerts(run_result)
        assert len(fired) == 0


# ---------------------------------------------------------------------------
# 7. HTTP runner X-Brix-Run-Id header
# ---------------------------------------------------------------------------

class TestHttpRunnerCorrelationHeader:
    def test_x_brix_run_id_header_injected(self):
        """HTTP runner must add X-Brix-Run-Id to outgoing requests."""
        import httpx
        from brix.runners.http import HttpRunner

        runner = HttpRunner()

        # Context mock with run_id
        context = mock.MagicMock()
        context.run_id = "test-run-abc123"

        # Step mock
        step = mock.MagicMock()
        step.url = "http://example.com/test"
        step.method = "GET"
        step.headers = None
        step.body = None
        step.fetch_all_pages = False
        step.timeout = None
        step.params = {}

        captured_headers = {}

        class MockAsyncClient:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                pass
            async def request(self, method, url, **kwargs):
                captured_headers.update(kwargs.get("headers", {}))
                resp = mock.MagicMock(spec=httpx.Response)
                resp.status_code = 200
                resp.headers = httpx.Headers({})
                resp.json = lambda: {"ok": True}
                resp.text = '{"ok": true}'
                return resp
            async def get(self, url, **kwargs):
                return await self.request("GET", url, **kwargs)

        with mock.patch("httpx.AsyncClient", return_value=MockAsyncClient()):
            result = _run_async(runner.execute(step, context))

        assert result["success"] is True
        assert "X-Brix-Run-Id" in captured_headers
        assert captured_headers["X-Brix-Run-Id"] == "test-run-abc123"

    def test_x_brix_run_id_does_not_overwrite_existing(self):
        """If caller already set X-Brix-Run-Id, the runner must not overwrite it."""
        import httpx
        from brix.runners.http import HttpRunner

        runner = HttpRunner()
        context = mock.MagicMock()
        context.run_id = "engine-run-id"

        step = mock.MagicMock()
        step.url = "http://example.com/test"
        step.method = "GET"
        step.headers = {"X-Brix-Run-Id": "caller-set-id"}
        step.body = None
        step.fetch_all_pages = False
        step.timeout = None
        step.params = {}

        captured_headers = {}

        class MockAsyncClient:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                pass
            async def request(self, method, url, **kwargs):
                captured_headers.update(kwargs.get("headers", {}))
                resp = mock.MagicMock(spec=httpx.Response)
                resp.status_code = 200
                resp.headers = httpx.Headers({})
                resp.json = lambda: {}
                resp.text = "{}"
                return resp

        with mock.patch("httpx.AsyncClient", return_value=MockAsyncClient()):
            _run_async(runner.execute(step, context))

        # Original value must be preserved
        assert captured_headers["X-Brix-Run-Id"] == "caller-set-id"

    def test_no_header_when_no_run_id(self):
        """Without a run_id in context, no X-Brix-Run-Id header is added."""
        import httpx
        from brix.runners.http import HttpRunner

        runner = HttpRunner()
        context = mock.MagicMock()
        context.run_id = None

        step = mock.MagicMock()
        step.url = "http://example.com/test"
        step.method = "GET"
        step.headers = None
        step.body = None
        step.fetch_all_pages = False
        step.timeout = None
        step.params = {}

        captured_headers = {}

        class MockAsyncClient:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                pass
            async def request(self, method, url, **kwargs):
                captured_headers.update(kwargs.get("headers", {}))
                resp = mock.MagicMock(spec=httpx.Response)
                resp.status_code = 200
                resp.headers = httpx.Headers({})
                resp.json = lambda: {}
                resp.text = "{}"
                return resp

        with mock.patch("httpx.AsyncClient", return_value=MockAsyncClient()):
            _run_async(runner.execute(step, context))

        assert "X-Brix-Run-Id" not in captured_headers


# ---------------------------------------------------------------------------
# 8. MCP tool brix__get_timeline
# ---------------------------------------------------------------------------

class TestMcpGetTimeline:
    def test_get_timeline_unknown_run(self, tmp_home):
        from brix.mcp_server import _handle_get_timeline
        result = _run_async(_handle_get_timeline({"run_id": "no-such-run"}))
        assert result["success"] is False
        assert "not found" in result["error"]

    def test_get_timeline_missing_run_id(self, tmp_home):
        from brix.mcp_server import _handle_get_timeline
        result = _run_async(_handle_get_timeline({}))
        assert result["success"] is False

    def test_get_timeline_returns_steps(self, tmp_home):
        from brix.db import BrixDB
        from brix.mcp_server import _handle_get_timeline

        db = BrixDB()
        steps = {
            "s1": {"status": "ok", "duration": 1.0},
            "s2": {"status": "ok", "duration": 2.0},
        }
        db.record_run_start(run_id="r-mcp-tl", pipeline="p")
        db.record_run_finish(run_id="r-mcp-tl", success=True, duration=3.0, steps=steps)

        result = _run_async(_handle_get_timeline({"run_id": "r-mcp-tl"}))
        assert result["success"] is True
        assert result["total_steps"] == 2
        assert result["total_duration"] == pytest.approx(3.0)
        step_ids = [e["step_id"] for e in result["timeline"]]
        assert "s1" in step_ids
        assert "s2" in step_ids

    def test_get_timeline_in_handlers_registry(self):
        from brix.mcp_server import _HANDLERS
        assert "brix__get_timeline" in _HANDLERS
