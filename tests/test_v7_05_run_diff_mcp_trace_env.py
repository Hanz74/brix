"""Tests for T-BRIX-V7-05 — Run-Diff + MCP Call Trace + Environment Snapshot.

Covers:
1. DB: environment_json column migration (idempotent)
2. DB: save_run_environment / get_run_environment
3. DB: record_run_start accepts environment param
4. Engine: _capture_environment returns expected keys
5. Engine: _persist_step_output stores mcp_trace in rendered_params._mcp_trace
6. McpRunner._build_trace returns correct structure
7. MCP Server: diff_runs handler — identical runs, input diff, step diff, env diff
"""
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from brix.db import BrixDB
from brix.history import RunHistory
from brix.models import Step


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def history(tmp_path):
    return RunHistory(db_path=tmp_path / "brix.db")


# ---------------------------------------------------------------------------
# 1. DB: environment_json column
# ---------------------------------------------------------------------------


class TestEnvironmentJsonColumn:
    def test_column_exists_after_init(self, db):
        with db._connect() as conn:
            cols = [
                row[1]
                for row in conn.execute("PRAGMA table_info(runs)").fetchall()
            ]
        assert "environment_json" in cols

    def test_migration_is_idempotent(self, db):
        # Calling _init_schema a second time must not raise
        db._init_schema()
        with db._connect() as conn:
            cols = [
                row[1]
                for row in conn.execute("PRAGMA table_info(runs)").fetchall()
            ]
        assert "environment_json" in cols


# ---------------------------------------------------------------------------
# 2. DB: save_run_environment / get_run_environment
# ---------------------------------------------------------------------------


class TestSaveGetRunEnvironment:
    def _make_run(self, db: BrixDB, run_id: str = "run-1") -> str:
        db.record_run_start(run_id=run_id, pipeline="test-pipe")
        return run_id

    def test_returns_none_when_no_snapshot(self, db):
        run_id = self._make_run(db)
        assert db.get_run_environment(run_id) is None

    def test_returns_none_for_unknown_run(self, db):
        assert db.get_run_environment("no-such-run") is None

    def test_save_and_retrieve(self, db):
        run_id = self._make_run(db)
        env = {"python_version": "3.11.0", "installed_packages": ["requests==2.31.0"], "mcp_servers": ["m365"]}
        db.save_run_environment(run_id, env)
        result = db.get_run_environment(run_id)
        assert result == env

    def test_record_run_start_with_environment(self, db):
        env = {"python_version": "3.12.1", "installed_packages": [], "mcp_servers": []}
        db.record_run_start(run_id="run-env", pipeline="p", environment=env)
        result = db.get_run_environment("run-env")
        assert result == env

    def test_environment_json_in_get_run_row(self, db):
        env = {"python_version": "3.10.0"}
        db.record_run_start(run_id="run-row", pipeline="p", environment=env)
        row = db.get_run("run-row")
        assert row is not None
        assert "environment_json" in row
        assert json.loads(row["environment_json"]) == env


# ---------------------------------------------------------------------------
# 3. Engine: _capture_environment
# ---------------------------------------------------------------------------


class TestCaptureEnvironment:
    def test_returns_dict(self):
        from brix.engine import PipelineEngine
        result = PipelineEngine._capture_environment()
        assert isinstance(result, dict)

    def test_has_required_keys(self):
        from brix.engine import PipelineEngine
        result = PipelineEngine._capture_environment()
        assert "python_version" in result
        assert "installed_packages" in result
        assert "mcp_servers" in result

    def test_python_version_is_string(self):
        import sys
        from brix.engine import PipelineEngine
        result = PipelineEngine._capture_environment()
        expected_prefix = f"{sys.version_info.major}.{sys.version_info.minor}."
        assert result["python_version"].startswith(expected_prefix)

    def test_installed_packages_is_list(self):
        from brix.engine import PipelineEngine
        result = PipelineEngine._capture_environment()
        assert isinstance(result["installed_packages"], list)

    def test_mcp_servers_is_list(self):
        from brix.engine import PipelineEngine
        result = PipelineEngine._capture_environment()
        assert isinstance(result["mcp_servers"], list)

    def test_package_entries_are_name_version_strings(self):
        from brix.engine import PipelineEngine
        result = PipelineEngine._capture_environment()
        for pkg in result["installed_packages"]:
            assert "==" in pkg, f"Expected 'name==version', got: {pkg!r}"

    def test_capped_at_200_packages(self):
        from brix.engine import PipelineEngine
        result = PipelineEngine._capture_environment()
        assert len(result["installed_packages"]) <= 200


# ---------------------------------------------------------------------------
# 4. Engine: _persist_step_output stores mcp_trace
# ---------------------------------------------------------------------------


class TestPersistStepOutputMcpTrace:
    def test_mcp_trace_stored_in_rendered_params(self, db):
        from brix.engine import PipelineEngine
        step = Step(id="step-mcp", type="mcp", server="m365", tool="list-mails", persist_output=True)
        trace = {"server": "m365", "tool": "list-mails", "status": "ok", "duration": 0.123}
        result = {
            "success": True,
            "data": {"items": []},
            "mcp_trace": trace,
        }
        rendered_params = {"folder": "INBOX"}

        class FakeCtx:
            def to_jinja_context(self):
                return {}

        engine = PipelineEngine()
        engine._persist_step_output(
            run_id="run-trace",
            step=step,
            result=result,
            rendered_params=rendered_params,
            context=FakeCtx(),
            db=db,
        )
        row = db.get_step_output("run-trace", "step-mcp")
        assert row is not None
        rp = row["rendered_params"]
        assert "_mcp_trace" in rp
        assert rp["_mcp_trace"] == trace
        # Original params preserved
        assert rp["folder"] == "INBOX"

    def test_no_mcp_trace_when_absent(self, db):
        from brix.engine import PipelineEngine
        step = Step(id="step-py", type="python", persist_output=True)
        result = {"success": True, "data": {"x": 1}}
        rendered_params = {"script": "/app/helpers/foo.py"}

        class FakeCtx:
            def to_jinja_context(self):
                return {}

        engine = PipelineEngine()
        engine._persist_step_output(
            run_id="run-notrace",
            step=step,
            result=result,
            rendered_params=rendered_params,
            context=FakeCtx(),
            db=db,
        )
        row = db.get_step_output("run-notrace", "step-py")
        assert row is not None
        rp = row["rendered_params"]
        assert "_mcp_trace" not in (rp or {})


# ---------------------------------------------------------------------------
# 5. McpRunner._build_trace
# ---------------------------------------------------------------------------


class TestMcpRunnerBuildTrace:
    def _trace(self, **kwargs) -> dict:
        from brix.runners.mcp import McpRunner
        defaults = dict(
            server="srv",
            tool="my-tool",
            arguments={"key": "value"},
            result={"success": True, "data": {"count": 5}},
            duration=0.42,
        )
        defaults.update(kwargs)
        return McpRunner._build_trace(**defaults)

    def test_has_required_fields(self):
        t = self._trace()
        assert t["server"] == "srv"
        assert t["tool"] == "my-tool"
        assert "arguments_summary" in t
        assert "response_summary" in t
        assert "duration" in t
        assert "status" in t

    def test_status_ok_on_success(self):
        t = self._trace(result={"success": True, "data": {}})
        assert t["status"] == "ok"

    def test_status_error_on_failure(self):
        t = self._trace(result={"success": False, "error": "timeout"})
        assert t["status"] == "error"

    def test_arguments_summary_contains_type_info(self):
        t = self._trace(arguments={"name": "foo", "items": [1, 2, 3], "config": {"k": "v"}})
        summary = t["arguments_summary"]
        assert summary["name"] == "str(3)"
        assert summary["items"] == "list(3)"
        assert summary["config"] == "dict(1)"

    def test_response_summary_dict(self):
        t = self._trace(result={"success": True, "data": {"a": 1, "b": 2}})
        assert "dict(2" in t["response_summary"]

    def test_response_summary_list(self):
        t = self._trace(result={"success": True, "data": [1, 2, 3]})
        assert "list(3" in t["response_summary"]

    def test_response_summary_error(self):
        t = self._trace(result={"success": False, "error": "connection refused"})
        assert "connection refused" in t["response_summary"]

    def test_duration_rounded(self):
        t = self._trace(duration=1.23456789)
        assert isinstance(t["duration"], float)
        assert t["duration"] == round(1.23456789, 4)

    def test_empty_arguments(self):
        t = self._trace(arguments={})
        assert t["arguments_summary"] == {}

    def test_none_arguments(self):
        t = self._trace(arguments=None)
        assert t["arguments_summary"] == {}


# ---------------------------------------------------------------------------
# 6. MCP Server: _handle_diff_runs
# ---------------------------------------------------------------------------


class TestHandleDiffRuns:
    """Tests for the brix__diff_runs MCP handler."""

    def _handler(self):
        from brix.mcp_server import _handle_diff_runs
        return _handle_diff_runs

    def _seed_runs(self, db: BrixDB, run_id: str, pipeline: str = "test-pipe",
                   input_data: dict = None, version: str = "1.0",
                   steps: dict = None, success: bool = True,
                   env: dict = None) -> None:
        db.record_run_start(
            run_id=run_id, pipeline=pipeline, version=version,
            input_data=input_data, environment=env,
        )
        db.record_run_finish(
            run_id=run_id, success=success, duration=1.0, steps=steps
        )

    @pytest.mark.asyncio
    async def test_missing_both_ids(self, db):
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "", "run_id_b": ""})
        assert result["success"] is False
        assert "required" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_same_run_id(self, db):
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "x", "run_id_b": "x"})
        assert result["success"] is False
        assert "different" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_run_not_found(self, db):
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "no-1", "run_id_b": "no-2"})
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_identical_runs_flagged(self, db):
        self._seed_runs(db, "r1", input_data={"k": "v"}, version="1.0",
                        steps={"step1": {"status": "ok"}}, env={"python_version": "3.11.0"})
        self._seed_runs(db, "r2", input_data={"k": "v"}, version="1.0",
                        steps={"step1": {"status": "ok"}}, env={"python_version": "3.11.0"})
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        assert result["success"] is True
        assert result["identical"] is True
        assert result["step_diffs"] == []
        assert not result["input_diff"]

    @pytest.mark.asyncio
    async def test_input_diff_detected(self, db):
        self._seed_runs(db, "r1", input_data={"mode": "fast"})
        self._seed_runs(db, "r2", input_data={"mode": "slow"})
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        assert result["success"] is True
        assert "mode" in result["input_diff"]
        assert result["input_diff"]["mode"] == {"a": "fast", "b": "slow"}
        assert result["summary"]["has_input_diff"] is True

    @pytest.mark.asyncio
    async def test_version_diff_detected(self, db):
        self._seed_runs(db, "r1", version="1.0")
        self._seed_runs(db, "r2", version="2.0")
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        assert result["version_diff"] == {"a": "1.0", "b": "2.0"}
        assert result["summary"]["has_version_diff"] is True

    @pytest.mark.asyncio
    async def test_step_status_diff_detected(self, db):
        self._seed_runs(db, "r1", steps={"step1": {"status": "ok"}, "step2": {"status": "ok"}})
        self._seed_runs(db, "r2", steps={"step1": {"status": "ok"}, "step2": {"status": "error", "error_message": "boom"}})
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        assert result["success"] is True
        step_diff_ids = {s["step_id"] for s in result["step_diffs"]}
        assert "step2" in step_diff_ids
        diff = next(s for s in result["step_diffs"] if s["step_id"] == "step2")
        assert diff["a_status"] == "ok"
        assert diff["b_status"] == "error"
        assert diff["b_error"] == "boom"

    @pytest.mark.asyncio
    async def test_step_output_diff_when_persisted(self, db):
        self._seed_runs(db, "r1", steps={"step1": {"status": "ok"}})
        self._seed_runs(db, "r2", steps={"step1": {"status": "ok"}})
        # Save different step outputs for the same step
        db.save_step_output(run_id="r1", step_id="step1", output={"count": 10})
        db.save_step_output(run_id="r2", step_id="step1", output={"count": 20})
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        assert len(result["step_diffs"]) == 1
        diff = result["step_diffs"][0]
        assert diff["step_id"] == "step1"
        assert diff["output_diff"] == {"a": {"count": 10}, "b": {"count": 20}}

    @pytest.mark.asyncio
    async def test_env_diff_python_version(self, db):
        self._seed_runs(db, "r1", env={"python_version": "3.10.0", "installed_packages": [], "mcp_servers": []})
        self._seed_runs(db, "r2", env={"python_version": "3.11.0", "installed_packages": [], "mcp_servers": []})
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        env_diff = result["environment_diff"]
        assert env_diff is not None
        assert env_diff["python_version"] == {"a": "3.10.0", "b": "3.11.0"}
        assert result["summary"]["has_env_diff"] is True

    @pytest.mark.asyncio
    async def test_env_diff_packages(self, db):
        env_a = {"python_version": "3.11.0", "installed_packages": ["requests==2.31.0", "httpx==0.24.0"], "mcp_servers": []}
        env_b = {"python_version": "3.11.0", "installed_packages": ["requests==2.32.0", "httpx==0.24.0"], "mcp_servers": []}
        self._seed_runs(db, "r1", env=env_a)
        self._seed_runs(db, "r2", env=env_b)
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        pkg_diff = result["environment_diff"]["installed_packages"]
        assert "requests==2.32.0" in pkg_diff["added"]
        assert "requests==2.31.0" in pkg_diff["removed"]

    @pytest.mark.asyncio
    async def test_no_env_diff_when_same(self, db):
        env = {"python_version": "3.11.0", "installed_packages": [], "mcp_servers": []}
        self._seed_runs(db, "r1", env=env)
        self._seed_runs(db, "r2", env=env)
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        assert result["environment_diff"] is None

    @pytest.mark.asyncio
    async def test_summary_fields_present(self, db):
        self._seed_runs(db, "r1", success=True)
        self._seed_runs(db, "r2", success=False)
        handler = self._handler()
        with patch("brix.mcp_handlers.runs.BrixDB", return_value=db):
            result = await handler({"run_id_a": "r1", "run_id_b": "r2"})
        assert "summary" in result
        summary = result["summary"]
        assert summary["a_success"] is True
        assert summary["b_success"] is False
        assert "changed_steps" in summary
        assert "has_input_diff" in summary
        assert "has_version_diff" in summary
        assert "has_env_diff" in summary
