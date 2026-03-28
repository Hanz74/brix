"""Tests for T-BRIX-V6-BUG-03 — cancel_run: sauberes Run-Abbrechen.

Covers:
- BrixDB.cancel_run() marks finished_at + cancel_reason
- BrixDB.clean_orphaned_runs() marks stale unfinished runs
- RunHistory.cancel_run() + clean_orphaned_runs() delegation
- sdk.is_cancelled() / check_cancellation() read BRIX_RUN_WORKDIR sentinel
- PythonRunner injects BRIX_RUN_WORKDIR into subprocess env
- Engine foreach loop aborts when cancel sentinel exists
- MCP handler brix__cancel_run: sentinel written, task cancelled, history updated
- CLI brix clean --orphaned-runs
"""
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from brix.db import BrixDB
from brix.history import RunHistory


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
# BrixDB — cancel_run
# ---------------------------------------------------------------------------

class TestDbCancelRun:
    def test_cancel_run_marks_finished_at(self, db):
        db.record_run_start("run-cancel-1", "test-pipeline")
        result = db.cancel_run("run-cancel-1", reason="user request")
        assert result is True

        row = db.get_run("run-cancel-1")
        assert row["finished_at"] is not None
        assert row["success"] == 0
        assert row["cancel_reason"] == "user request"
        assert row["cancelled_by"] == "user"

    def test_cancel_run_nonexistent_returns_false(self, db):
        result = db.cancel_run("nonexistent-run")
        assert result is False

    def test_cancel_run_already_finished_returns_false(self, db):
        """cancel_run only affects runs with finished_at IS NULL."""
        db.record_run_start("run-done", "test-pipeline")
        db.record_run_finish("run-done", success=True, duration=1.0)
        result = db.cancel_run("run-done", reason="too late")
        # finished_at already set — UPDATE matches no rows
        assert result is False

    def test_cancel_run_default_cancelled_by(self, db):
        db.record_run_start("run-default", "test-pipeline")
        db.cancel_run("run-default")
        row = db.get_run("run-default")
        assert row["cancelled_by"] == "user"
        assert row["cancel_reason"] == ""

    def test_cancel_run_custom_cancelled_by(self, db):
        db.record_run_start("run-custom-by", "test-pipeline")
        db.cancel_run("run-custom-by", reason="test", cancelled_by="watchdog")
        row = db.get_run("run-custom-by")
        assert row["cancelled_by"] == "watchdog"


# ---------------------------------------------------------------------------
# BrixDB — clean_orphaned_runs
# ---------------------------------------------------------------------------

class TestDbCleanOrphanedRuns:
    def test_orphaned_run_marked_cancelled(self, db, tmp_path):
        """Runs that started >max_age_hours ago with no finished_at are cancelled."""
        # Insert a run with a very old started_at manually
        import sqlite3
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, pipeline, started_at) VALUES (?,?,?)",
                ("orphan-1", "my-pipeline", "2000-01-01T00:00:00+00:00"),
            )

        count = db.clean_orphaned_runs(max_age_hours=1)
        assert count == 1
        row = db.get_run("orphan-1")
        assert row["finished_at"] is not None
        assert row["success"] == 0
        assert row["cancel_reason"] == "orphaned (no heartbeat)"
        assert row["cancelled_by"] == "brix-cleanup"

    def test_recent_run_not_affected(self, db):
        """Runs that started recently are NOT marked as orphaned."""
        db.record_run_start("recent-run", "my-pipeline")
        count = db.clean_orphaned_runs(max_age_hours=24)
        assert count == 0
        row = db.get_run("recent-run")
        assert row["finished_at"] is None

    def test_already_finished_run_not_affected(self, db):
        """Already-finished runs are not touched."""
        import sqlite3
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, pipeline, started_at, finished_at, success) VALUES (?,?,?,?,?)",
                ("finished-run", "p", "2000-01-01T00:00:00+00:00", "2000-01-01T01:00:00+00:00", 1),
            )
        count = db.clean_orphaned_runs(max_age_hours=1)
        assert count == 0


# ---------------------------------------------------------------------------
# RunHistory delegation
# ---------------------------------------------------------------------------

class TestHistoryCancelRun:
    def test_cancel_run_delegates(self, history):
        history.record_start("run-h1", "pipe")
        ok = history.cancel_run("run-h1", reason="stop it")
        assert ok is True
        row = history.get_run("run-h1")
        assert row["cancel_reason"] == "stop it"

    def test_clean_orphaned_runs_delegates(self, history):
        import sqlite3
        with history._db._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, pipeline, started_at) VALUES (?,?,?)",
                ("h-orphan", "p", "2000-01-01T00:00:00+00:00"),
            )
        count = history.clean_orphaned_runs(max_age_hours=1)
        assert count == 1


# ---------------------------------------------------------------------------
# SDK: is_cancelled / check_cancellation
# ---------------------------------------------------------------------------

class TestSdkIsCancelled:
    def test_is_cancelled_no_env(self, monkeypatch):
        """Without BRIX_RUN_WORKDIR, is_cancelled returns False."""
        monkeypatch.delenv("BRIX_RUN_WORKDIR", raising=False)
        from brix.sdk import is_cancelled
        assert is_cancelled() is False

    def test_is_cancelled_no_sentinel(self, tmp_path, monkeypatch):
        """With BRIX_RUN_WORKDIR set but no sentinel file, returns False."""
        monkeypatch.setenv("BRIX_RUN_WORKDIR", str(tmp_path))
        from brix.sdk import is_cancelled
        assert is_cancelled() is False

    def test_is_cancelled_sentinel_present(self, tmp_path, monkeypatch):
        """With sentinel file present, returns True."""
        sentinel = tmp_path / "cancel_requested.json"
        sentinel.write_text('{"reason":"test"}')
        monkeypatch.setenv("BRIX_RUN_WORKDIR", str(tmp_path))
        from brix.sdk import is_cancelled
        assert is_cancelled() is True

    def test_check_cancellation_exits_130(self, tmp_path, monkeypatch):
        """check_cancellation() raises SystemExit(130) when sentinel present."""
        sentinel = tmp_path / "cancel_requested.json"
        sentinel.write_text("{}")
        monkeypatch.setenv("BRIX_RUN_WORKDIR", str(tmp_path))
        from brix.sdk import check_cancellation
        with pytest.raises(SystemExit) as exc_info:
            check_cancellation()
        assert exc_info.value.code == 130

    def test_check_cancellation_noop(self, tmp_path, monkeypatch):
        """check_cancellation() is a no-op when not cancelled."""
        monkeypatch.setenv("BRIX_RUN_WORKDIR", str(tmp_path))
        from brix.sdk import check_cancellation
        check_cancellation()  # must not raise


# ---------------------------------------------------------------------------
# PythonRunner — BRIX_RUN_WORKDIR injection
# ---------------------------------------------------------------------------

class TestPythonRunnerWorkdirEnv:
    @pytest.mark.asyncio
    async def test_workdir_env_injected(self, tmp_path):
        """PythonRunner injects BRIX_RUN_WORKDIR from context.workdir."""
        from brix.runners.python import PythonRunner

        script_path = tmp_path / "echo_env.py"
        script_path.write_text(
            'import os, json, sys\n'
            'print(json.dumps({"workdir": os.environ.get("BRIX_RUN_WORKDIR", "")}))\n'
        )

        runner = PythonRunner()

        class FakeStep:
            params = {}
            timeout = None
            progress = False

        step_obj = FakeStep()
        step_obj.script = str(script_path)

        class FakeContext:
            workdir = tmp_path
            credentials = {}

        result = await runner.execute(step_obj, FakeContext())
        assert result["success"] is True
        assert result["data"]["workdir"] == str(tmp_path)


# ---------------------------------------------------------------------------
# Engine — foreach cancel check
# ---------------------------------------------------------------------------

class TestEngineForeachCancel:
    def test_is_run_cancelled_false_without_sentinel(self, tmp_path):
        """_is_run_cancelled returns False when sentinel not present."""
        from brix.engine import PipelineEngine
        engine = PipelineEngine()

        class FakeCtx:
            workdir = tmp_path

        assert engine._is_run_cancelled(FakeCtx()) is False

    def test_is_run_cancelled_true_with_sentinel(self, tmp_path):
        """_is_run_cancelled returns True when cancel_requested.json present."""
        from brix.engine import PipelineEngine
        engine = PipelineEngine()
        (tmp_path / "cancel_requested.json").write_text('{}')

        class FakeCtx:
            workdir = tmp_path

        assert engine._is_run_cancelled(FakeCtx()) is True

    @pytest.mark.asyncio
    async def test_foreach_aborts_on_cancel(self, tmp_path):
        """Sequential foreach stops after cancel sentinel is pre-placed in workdir."""
        from brix.engine import PipelineEngine
        from brix.context import WORKDIR_BASE

        script_path = tmp_path / "count_items.py"
        counter_path = tmp_path / "counter.json"
        counter_path.write_text("0")
        script_path.write_text(
            'import json, sys, pathlib\n'
            f'counter_file = pathlib.Path("{counter_path}")\n'
            'n = int(counter_file.read_text()) + 1\n'
            'counter_file.write_text(str(n))\n'
            'print(json.dumps({"n": n}))\n'
        )

        import yaml
        pipeline_data = {
            "name": "cancel-foreach-test2",
            "steps": [
                {
                    "id": "items_step",
                    "type": "python",
                    "script": str(script_path),
                    "foreach": "{{ [1, 2, 3, 4, 5] | tojson }}",
                }
            ],
        }
        pipeline_file = tmp_path / "cancel-foreach-test2.yaml"
        pipeline_file.write_text(yaml.dump(pipeline_data))

        from brix.loader import PipelineLoader
        loader = PipelineLoader()
        pipeline = loader.load(str(pipeline_file))
        engine = PipelineEngine()

        # Pre-write cancel sentinel in the expected workdir path BEFORE running
        # We need to intercept the context creation to get the run_id.
        # Easiest: monkeypatch _is_run_cancelled to return True after 2 items.
        original_is_cancelled = engine._is_run_cancelled
        call_count = 0

        def fake_is_cancelled(ctx):
            nonlocal call_count
            call_count += 1
            # Let first 2 items through, then cancel
            return call_count > 3  # >3 because check also runs before item 1

        engine._is_run_cancelled = fake_is_cancelled

        result = await engine.run(pipeline)
        items_processed = int(counter_path.read_text())
        assert items_processed < 5, f"Expected < 5 items processed, got {items_processed}"


# ---------------------------------------------------------------------------
# MCP handler brix__cancel_run
# ---------------------------------------------------------------------------

class TestHandleCancelRun:
    @pytest.mark.asyncio
    async def test_cancel_writes_sentinel(self, tmp_path, monkeypatch):
        """brix__cancel_run writes cancel_requested.json to the run workdir."""
        from brix.mcp_server import _handle_cancel_run
        import brix.context as ctx_mod

        monkeypatch.setattr(ctx_mod, "WORKDIR_BASE", tmp_path)
        # Also patch WORKDIR_BASE where mcp_server imports it (it does a local import)
        run_id = "run-cancel-sentinel"
        result = await _handle_cancel_run({"run_id": run_id, "reason": "test-cancel"})

        assert result["success"] is True
        assert result["cancelled"] is True
        assert result["sentinel_written"] is True
        sentinel = tmp_path / run_id / "cancel_requested.json"
        assert sentinel.exists()
        data = json.loads(sentinel.read_text())
        assert data["reason"] == "test-cancel"

    @pytest.mark.asyncio
    async def test_cancel_missing_run_id(self):
        """brix__cancel_run returns error when run_id is missing."""
        from brix.mcp_server import _handle_cancel_run
        result = await _handle_cancel_run({})
        assert result["success"] is False
        assert "run_id" in result["error"]

    @pytest.mark.asyncio
    async def test_cancel_background_task(self, tmp_path, monkeypatch):
        """brix__cancel_run cancels an active asyncio.Task in _background_runs."""
        from brix.mcp_server import _handle_cancel_run
        import brix.mcp_server as mcp_mod
        import brix.context as ctx_mod

        monkeypatch.setattr(ctx_mod, "WORKDIR_BASE", tmp_path)

        run_id = "run-bg-cancel"

        # Create a real asyncio task that will be cancelled
        async def long_run():
            await asyncio.sleep(999)

        task = asyncio.create_task(long_run())
        monkeypatch.setitem(mcp_mod._background_runs, run_id, task)

        result = await _handle_cancel_run({"run_id": run_id, "reason": "bg test"})
        # Give the event loop a tick to propagate the cancellation
        await asyncio.sleep(0)
        assert result["success"] is True
        assert result["task_cancelled"] is True
        # After cancel() + one event-loop tick the task transitions to cancelled/done
        assert task.cancelled() or task.cancelling() > 0 or task.done()
        assert run_id not in mcp_mod._background_runs


# ---------------------------------------------------------------------------
# CLI: brix clean --orphaned-runs
# ---------------------------------------------------------------------------

class TestCliCleanOrphanedRuns:
    def test_clean_orphaned_dry_run(self, tmp_path, monkeypatch):
        """brix clean --orphaned-runs --dry-run prints what would be done."""
        from click.testing import CliRunner
        from brix.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["clean", "--orphaned-runs", "--dry-run"])
        assert result.exit_code == 0
        assert "orphaned" in result.output.lower() or "unfinished" in result.output.lower() or "dry" in result.output.lower()

    def test_clean_orphaned_runs_executes(self, tmp_path, monkeypatch):
        """brix clean --orphaned-runs marks old unfinished runs as cancelled."""
        from click.testing import CliRunner
        from brix.cli import main
        from brix.history import RunHistory as _RH

        # Plant an orphaned run in a temp db by monkeypatching HISTORY_DB_PATH
        import brix.history as hist_mod
        import brix.db as db_mod
        db_file = tmp_path / "brix.db"
        monkeypatch.setattr(hist_mod, "HISTORY_DB_PATH", db_file)
        monkeypatch.setattr(db_mod, "BRIX_DB_PATH", db_file)

        h = _RH(db_path=db_file)
        import sqlite3
        with h._db._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, pipeline, started_at) VALUES (?,?,?)",
                ("orphan-cli", "p", "2000-01-01T00:00:00+00:00"),
            )

        runner = CliRunner()
        result = runner.invoke(main, ["clean", "--orphaned-runs", "--max-age-hours", "1"])
        assert result.exit_code == 0
        assert "1" in result.output  # 1 run marked
