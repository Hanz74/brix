"""Tests for T-BRIX-V6-07 (diagnose_run + auto_fix_step),
T-BRIX-V6-08 (get_insights), and T-BRIX-V6-09 (get_proactive_suggestions).

Coverage:
- diagnose_run: unknown run_id → error
- diagnose_run: run with no steps_data → empty diagnoses
- diagnose_run: ModuleNotFoundError step → hint + fix_suggestion
- diagnose_run: UndefinedError step → hint + fix_suggestion
- diagnose_run: Timeout step → hint + fix_suggestion
- diagnose_run: multiple failed steps → all returned
- diagnose_run: success steps are ignored
- auto_fix_step: unknown run_id → error
- auto_fix_step: step not found → error
- auto_fix_step: step not in error status → error
- auto_fix_step: ModuleNotFoundError → installs module (mocked)
- auto_fix_step: ModuleNotFoundError undetectable module → fixed=False
- auto_fix_step: UndefinedError → patches pipeline YAML
- auto_fix_step: Timeout → doubles timeout in pipeline YAML
- auto_fix_step: unknown error → fixed=False
- get_insights: empty DB → empty lists
- get_insights: slow_steps detected when one step is 3x+ median
- get_insights: failure_patterns grouped correctly
- get_insights: dead_helpers finds helpers not in pipeline_helpers
- get_proactive_suggestions: returns list (may be empty)
- get_proactive_suggestions: performance suggestion for slow steps
- get_proactive_suggestions: reliability suggestion for repeated failures
- get_proactive_suggestions: cleanup suggestion for dead helpers
"""
import asyncio
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from brix.history import RunHistory
from brix.db import BrixDB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_run(history: RunHistory, run_id: str, pipeline: str, steps: dict, success: bool = False):
    """Helper: record a finished run with the given steps_data."""
    history.record_start(run_id, pipeline)
    history.record_finish(run_id, success, 1.0, steps)


def _run(coro):
    """Run a coroutine synchronously for testing."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# diagnose_run
# ---------------------------------------------------------------------------

class TestDiagnoseRun:

    def test_unknown_run_id_returns_error(self, tmp_path):
        from brix.mcp_server import _handle_diagnose_run
        import brix.mcp_server as srv
        orig = srv.RunHistory
        srv.RunHistory = lambda: RunHistory(db_path=tmp_path / "t.db")
        try:
            result = _run(_handle_diagnose_run({"run_id": "nonexistent"}))
            assert result["success"] is False
            assert "not found" in result["error"]
        finally:
            srv.RunHistory = orig

    def test_missing_run_id_param(self, tmp_path):
        from brix.mcp_server import _handle_diagnose_run
        result = _run(_handle_diagnose_run({}))
        assert result["success"] is False
        assert "run_id" in result["error"]

    def test_run_with_no_steps_data_returns_empty_diagnoses(self, tmp_path):
        from brix.mcp_server import _handle_diagnose_run
        h = RunHistory(db_path=tmp_path / "t.db")
        h.record_start("r1", "pipe-a")
        h.record_finish("r1", False, 1.0, steps=None)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_diagnose_run({"run_id": "r1"}))
            assert result["success"] is True
            assert result["diagnoses"] == []

    def test_module_not_found_error_hint(self, tmp_path):
        from brix.mcp_server import _handle_diagnose_run
        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"fetch": {"status": "error", "error_message": "ModuleNotFoundError: No module named 'mistralai'"}}
        make_run(h, "r1", "pipe-a", steps, success=False)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_diagnose_run({"run_id": "r1"}))
            assert result["success"] is True
            assert len(result["diagnoses"]) == 1
            d = result["diagnoses"][0]
            assert d["step_id"] == "fetch"
            assert "ModuleNotFoundError" in d["error"]
            assert d["hint"] is not None
            assert d["fix_suggestion"] is not None
            assert "auto_fix_step" in d["fix_suggestion"]

    def test_undefined_error_hint(self, tmp_path):
        from brix.mcp_server import _handle_diagnose_run
        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"process": {"status": "error", "error_message": "UndefinedError: 'input.foo' is undefined"}}
        make_run(h, "r2", "pipe-b", steps, success=False)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_diagnose_run({"run_id": "r2"}))
            assert result["success"] is True
            d = result["diagnoses"][0]
            assert "UndefinedError" in d["error"] or "is undefined" in d["error"]
            assert d["fix_suggestion"] is not None
            assert "auto_fix_step" in d["fix_suggestion"]

    def test_timeout_error_hint(self, tmp_path):
        from brix.mcp_server import _handle_diagnose_run
        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"slow_step": {"status": "error", "error_message": "Timeout: step exceeded 30s"}}
        make_run(h, "r3", "pipe-c", steps, success=False)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_diagnose_run({"run_id": "r3"}))
            assert result["success"] is True
            d = result["diagnoses"][0]
            assert "Timeout" in d["error"]
            assert d["fix_suggestion"] is not None
            assert "auto_fix_step" in d["fix_suggestion"]

    def test_success_steps_are_ignored(self, tmp_path):
        from brix.mcp_server import _handle_diagnose_run
        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {
            "ok_step": {"status": "ok"},
            "bad_step": {"status": "error", "error_message": "ModuleNotFoundError: No module named 'foo'"},
        }
        make_run(h, "r4", "pipe-d", steps, success=False)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_diagnose_run({"run_id": "r4"}))
            assert result["total_failed_steps"] == 1
            assert result["diagnoses"][0]["step_id"] == "bad_step"

    def test_multiple_failed_steps_all_returned(self, tmp_path):
        from brix.mcp_server import _handle_diagnose_run
        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {
            "s1": {"status": "error", "error_message": "Timeout: exceeded"},
            "s2": {"status": "error", "error_message": "ModuleNotFoundError: No module named 'x'"},
            "s3": {"status": "ok"},
        }
        make_run(h, "r5", "pipe-e", steps, success=False)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_diagnose_run({"run_id": "r5"}))
            assert result["total_failed_steps"] == 2
            ids = {d["step_id"] for d in result["diagnoses"]}
            assert ids == {"s1", "s2"}


# ---------------------------------------------------------------------------
# auto_fix_step
# ---------------------------------------------------------------------------

class TestAutoFixStep:

    def test_missing_run_id(self):
        from brix.mcp_server import _handle_auto_fix_step
        result = _run(_handle_auto_fix_step({"step_id": "s1"}))
        assert result["success"] is False
        assert "run_id" in result["error"]

    def test_missing_step_id(self):
        from brix.mcp_server import _handle_auto_fix_step
        result = _run(_handle_auto_fix_step({"run_id": "r1"}))
        assert result["success"] is False
        assert "step_id" in result["error"]

    def test_unknown_run_id(self, tmp_path):
        from brix.mcp_server import _handle_auto_fix_step
        with patch("brix.mcp_handlers.insights.RunHistory", return_value=RunHistory(db_path=tmp_path / "t.db")), \
             patch("brix.history.RunHistory", return_value=RunHistory(db_path=tmp_path / "t.db")):
            result = _run(_handle_auto_fix_step({"run_id": "nope", "step_id": "s1"}))
            assert result["success"] is False
            assert "not found" in result["error"]

    def test_step_not_found_in_run(self, tmp_path):
        from brix.mcp_server import _handle_auto_fix_step
        h = RunHistory(db_path=tmp_path / "t.db")
        make_run(h, "r1", "pipe-a", {"other": {"status": "ok"}}, success=True)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_auto_fix_step({"run_id": "r1", "step_id": "missing"}))
            assert result["success"] is False
            assert "not found" in result["error"]

    def test_step_not_in_error_status(self, tmp_path):
        from brix.mcp_server import _handle_auto_fix_step
        h = RunHistory(db_path=tmp_path / "t.db")
        make_run(h, "r1", "pipe-a", {"s1": {"status": "ok"}}, success=True)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_auto_fix_step({"run_id": "r1", "step_id": "s1"}))
            assert result["success"] is False
            assert "did not fail" in result["error"]

    def test_module_not_found_install_success(self, tmp_path):
        from brix.mcp_server import _handle_auto_fix_step
        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"fetch": {"status": "error", "error_message": "ModuleNotFoundError: No module named 'mistralai'"}}
        make_run(h, "r1", "pipe-a", steps, success=False)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h), \
             patch("brix.deps.install_requirements", return_value=True):
            result = _run(_handle_auto_fix_step({"run_id": "r1", "step_id": "fetch"}))
            assert result["fixed"] is True
            assert "mistralai" in result["action"]
            assert "rerun_hint" in result

    def test_module_not_found_install_failure(self, tmp_path):
        from brix.mcp_server import _handle_auto_fix_step
        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"fetch": {"status": "error", "error_message": "ModuleNotFoundError: No module named 'badpkg'"}}
        make_run(h, "r1", "pipe-a", steps, success=False)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h), \
             patch("brix.deps.install_requirements", return_value=False):
            result = _run(_handle_auto_fix_step({"run_id": "r1", "step_id": "fetch"}))
            assert result["fixed"] is False
            assert "pip install" in result["action"]

    def test_undefined_error_patches_pipeline(self, tmp_path):
        """auto_fix_step patches Jinja2 {{ ref }} → {{ ref | default('') }} for UndefinedError."""
        from brix.mcp_server import _handle_auto_fix_step
        from brix.pipeline_store import PipelineStore
        import brix.mcp_server as srv

        # Create a minimal pipeline with an undefined Jinja2 expression
        pipeline_dir = tmp_path / "pipelines"
        pipeline_dir.mkdir()
        raw_pipeline = {
            "name": "pipe-x",
            "steps": [
                {"id": "process", "type": "set", "values": {"out": "{{ input.foo }}"}}
            ],
        }
        import yaml
        (pipeline_dir / "pipe-x.yaml").write_text(yaml.dump(raw_pipeline))

        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"process": {"status": "error", "error_message": "UndefinedError: 'input.foo' is undefined"}}
        make_run(h, "r1", "pipe-x", steps, success=False)

        with patch("brix.history.RunHistory", return_value=h), \
             patch("brix.mcp_handlers.steps._pipeline_dir", return_value=pipeline_dir):
            result = _run(_handle_auto_fix_step({"run_id": "r1", "step_id": "process"}))
            assert result["fixed"] is True
            assert "default" in result["action"]
            # Check that the YAML was patched
            updated = yaml.safe_load((pipeline_dir / "pipe-x.yaml").read_text())
            step = next(s for s in updated["steps"] if s["id"] == "process")
            assert "default" in step["values"]["out"]

    def test_timeout_doubles_timeout_value(self, tmp_path):
        """auto_fix_step doubles the timeout for Timeout errors."""
        from brix.mcp_server import _handle_auto_fix_step
        from brix.pipeline_store import PipelineStore
        import brix.mcp_server as srv

        pipeline_dir = tmp_path / "pipelines"
        pipeline_dir.mkdir()
        raw_pipeline = {
            "name": "pipe-y",
            "steps": [
                {"id": "slow", "type": "set", "values": {"x": "1"}, "timeout": 30}
            ],
        }
        import yaml
        (pipeline_dir / "pipe-y.yaml").write_text(yaml.dump(raw_pipeline))

        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"slow": {"status": "error", "error_message": "Timeout: step exceeded 30s"}}
        make_run(h, "r1", "pipe-y", steps, success=False)

        with patch("brix.history.RunHistory", return_value=h), \
             patch("brix.mcp_handlers.steps._pipeline_dir", return_value=pipeline_dir):
            result = _run(_handle_auto_fix_step({"run_id": "r1", "step_id": "slow"}))
            assert result["fixed"] is True
            assert "60" in result["action"]  # doubled from 30
            updated = yaml.safe_load((pipeline_dir / "pipe-y.yaml").read_text())
            step = next(s for s in updated["steps"] if s["id"] == "slow")
            assert step["timeout"] == 60

    def test_timeout_default_when_none(self, tmp_path):
        """auto_fix_step uses 120 as default when timeout field is missing."""
        from brix.mcp_server import _handle_auto_fix_step
        import brix.mcp_server as srv

        pipeline_dir = tmp_path / "pipelines"
        pipeline_dir.mkdir()
        raw_pipeline = {
            "name": "pipe-z",
            "steps": [{"id": "slow", "type": "set", "values": {"x": "1"}}],
        }
        import yaml
        (pipeline_dir / "pipe-z.yaml").write_text(yaml.dump(raw_pipeline))

        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"slow": {"status": "error", "error_message": "Timeout: step exceeded"}}
        make_run(h, "r1", "pipe-z", steps, success=False)

        with patch("brix.history.RunHistory", return_value=h), \
             patch("brix.mcp_handlers.steps._pipeline_dir", return_value=pipeline_dir):
            result = _run(_handle_auto_fix_step({"run_id": "r1", "step_id": "slow"}))
            assert result["fixed"] is True
            assert "120" in result["action"]

    def test_unknown_error_not_fixed(self, tmp_path):
        from brix.mcp_server import _handle_auto_fix_step
        h = RunHistory(db_path=tmp_path / "t.db")
        steps = {"s1": {"status": "error", "error_message": "Some random unexpected failure"}}
        make_run(h, "r1", "pipe-a", steps, success=False)

        with patch("brix.mcp_handlers.insights.RunHistory", return_value=h), \
             patch("brix.history.RunHistory", return_value=h):
            result = _run(_handle_auto_fix_step({"run_id": "r1", "step_id": "s1"}))
            assert result["fixed"] is False
            assert "no automatic fix" in result["action"]


# ---------------------------------------------------------------------------
# get_insights
# ---------------------------------------------------------------------------

class TestGetInsights:

    def _make_db_patcher(self, db: BrixDB):
        """Return a context manager that patches BrixDB() to return db."""
        return patch("brix.mcp_handlers.insights.BrixDB", side_effect=lambda **kw: db)

    def test_empty_db_returns_empty_lists(self, tmp_path):
        from brix.mcp_server import _handle_get_insights

        db = BrixDB(db_path=tmp_path / "t.db")
        with self._make_db_patcher(db):
            result = _run(_handle_get_insights({}))

        assert result["success"] is True
        assert result["slow_steps"] == []
        assert result["failure_patterns"] == []
        assert result["dead_helpers"] == []

    def test_slow_steps_detected(self, tmp_path):
        """A step with avg_duration >3x median should appear in slow_steps.

        Uses 3 fast steps (1s avg) and 1 very slow step (100s avg).
        Median of [1.0, 1.0, 1.0, 100.0] = 1.0 (middle of sorted list for n=4: (1+1)/2=1).
        100.0 > 3*1.0=3.0 → slow step qualifies.
        """
        from brix.mcp_server import _handle_get_insights

        db = BrixDB(db_path=tmp_path / "t.db")

        h = RunHistory(db_path=tmp_path / "t.db")
        for i in range(4):
            run_id = f"r{i}"
            h.record_start(run_id, "pipe-a")
            h.record_finish(run_id, True, float(i + 1), {
                "s1": {"status": "ok", "duration": 1.0},
                "s2": {"status": "ok", "duration": 1.0},
                "s3": {"status": "ok", "duration": 1.0},
                "snail": {"status": "ok", "duration": 100.0},
            })

        with self._make_db_patcher(db):
            result = _run(_handle_get_insights({}))

        slow_ids = {s["step_id"] for s in result["slow_steps"]}
        assert "snail" in slow_ids

    def test_failure_patterns_grouped(self, tmp_path):
        """Repeated errors should appear in failure_patterns."""
        from brix.mcp_server import _handle_get_insights

        db = BrixDB(db_path=tmp_path / "t.db")

        h = RunHistory(db_path=tmp_path / "t.db")
        for i in range(3):
            run_id = f"fail-{i}"
            h.record_start(run_id, "pipe-b")
            h.record_finish(run_id, False, 1.0, {
                "s1": {"status": "error", "error_message": "Connection refused by remote server"},
            })

        with self._make_db_patcher(db):
            result = _run(_handle_get_insights({}))

        patterns = result["failure_patterns"]
        assert len(patterns) > 0
        assert any("pipe-b" in p["pipeline"] for p in patterns)
        assert any(p["occurrences"] >= 3 for p in patterns)

    def test_dead_helpers_returned(self, tmp_path):
        """Helpers not in pipeline_helpers should appear in dead_helpers."""
        from brix.mcp_server import _handle_get_insights

        db = BrixDB(db_path=tmp_path / "t.db")

        # Insert a helper that is NOT linked to any pipeline
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO helpers (id, name, script_path, created_at, updated_at) "
                "VALUES ('h1', 'orphan-helper', '/helpers/orphan.py', '2024-01-01', '2024-01-01')"
            )

        with self._make_db_patcher(db):
            result = _run(_handle_get_insights({}))

        dead_names = {d["name"] for d in result["dead_helpers"]}
        assert "orphan-helper" in dead_names

    def test_linked_helper_not_dead(self, tmp_path):
        """A helper linked to a pipeline should NOT appear in dead_helpers."""
        from brix.mcp_server import _handle_get_insights

        db = BrixDB(db_path=tmp_path / "t.db")

        with db._connect() as conn:
            conn.execute(
                "INSERT INTO pipelines (id, name, path, created_at, updated_at) "
                "VALUES ('p1', 'my-pipe', '/pipelines/my-pipe.yaml', '2024-01-01', '2024-01-01')"
            )
            conn.execute(
                "INSERT INTO helpers (id, name, script_path, created_at, updated_at) "
                "VALUES ('h1', 'used-helper', '/helpers/used.py', '2024-01-01', '2024-01-01')"
            )
            conn.execute(
                "INSERT INTO pipeline_helpers (pipeline_id, helper_id) VALUES ('p1', 'h1')"
            )

        with self._make_db_patcher(db):
            result = _run(_handle_get_insights({}))

        dead_names = {d["name"] for d in result["dead_helpers"]}
        assert "used-helper" not in dead_names


# ---------------------------------------------------------------------------
# get_proactive_suggestions
# ---------------------------------------------------------------------------

class TestGetProactiveSuggestions:

    @staticmethod
    def _make_db_patcher(db: BrixDB):
        return patch("brix.mcp_handlers.insights.BrixDB", side_effect=lambda **kw: db)

    def test_empty_db_returns_empty_suggestions(self, tmp_path):
        from brix.mcp_server import _handle_get_proactive_suggestions

        db = BrixDB(db_path=tmp_path / "t.db")

        with self._make_db_patcher(db):
            result = _run(_handle_get_proactive_suggestions({}))

        assert result["success"] is True
        assert isinstance(result["suggestions"], list)
        assert result["total"] == len(result["suggestions"])

    def test_slow_step_generates_performance_suggestion(self, tmp_path):
        from brix.mcp_server import _handle_get_proactive_suggestions

        db = BrixDB(db_path=tmp_path / "t.db")

        h = RunHistory(db_path=tmp_path / "t.db")
        for i in range(4):
            run_id = f"r{i}"
            h.record_start(run_id, "pipe-perf")
            h.record_finish(run_id, True, 5.0, {
                "s1": {"status": "ok", "duration": 1.0},
                "s2": {"status": "ok", "duration": 1.0},
                "s3": {"status": "ok", "duration": 1.0},
                "snail": {"status": "ok", "duration": 100.0},
            })

        with self._make_db_patcher(db):
            result = _run(_handle_get_proactive_suggestions({}))

        types = [s["type"] for s in result["suggestions"]]
        assert "performance" in types

        perf = next(s for s in result["suggestions"] if s["type"] == "performance")
        assert "snail" in perf["message"]
        assert perf["action_tool"] == "brix__get_step"

    def test_repeated_failure_generates_reliability_suggestion(self, tmp_path):
        from brix.mcp_server import _handle_get_proactive_suggestions

        db = BrixDB(db_path=tmp_path / "t.db")

        h = RunHistory(db_path=tmp_path / "t.db")
        for i in range(3):
            run_id = f"fail-{i}"
            h.record_start(run_id, "pipe-fail")
            h.record_finish(run_id, False, 1.0, {
                "s1": {"status": "error", "error_message": "Connection refused reliably"},
            })

        with self._make_db_patcher(db):
            result = _run(_handle_get_proactive_suggestions({}))

        types = [s["type"] for s in result["suggestions"]]
        assert "reliability" in types

        rel = next(s for s in result["suggestions"] if s["type"] == "reliability")
        assert "pipe-fail" in rel["message"]
        assert rel["action_tool"] == "brix__get_run_errors"

    def test_dead_helper_generates_cleanup_suggestion(self, tmp_path):
        from brix.mcp_server import _handle_get_proactive_suggestions

        db = BrixDB(db_path=tmp_path / "t.db")

        with db._connect() as conn:
            conn.execute(
                "INSERT INTO helpers (id, name, script_path, created_at, updated_at) "
                "VALUES ('h1', 'dead-one', '/helpers/dead.py', '2024-01-01', '2024-01-01')"
            )

        with self._make_db_patcher(db):
            result = _run(_handle_get_proactive_suggestions({}))

        types = [s["type"] for s in result["suggestions"]]
        assert "cleanup" in types

        cleanup = next(s for s in result["suggestions"] if s["type"] == "cleanup")
        assert "dead-one" in cleanup["message"]
        assert cleanup["action_tool"] == "brix__delete_helper"

    def test_suggestions_have_required_fields(self, tmp_path):
        """Every suggestion must have type, message, action_tool, action_params."""
        from brix.mcp_server import _handle_get_proactive_suggestions

        db = BrixDB(db_path=tmp_path / "t.db")

        h = RunHistory(db_path=tmp_path / "t.db")
        for i in range(3):
            h.record_start(f"r{i}", "test-pipe")
            h.record_finish(f"r{i}", False, 1.0, {
                "s1": {"status": "error", "error_message": "Something went wrong consistently"},
            })

        with self._make_db_patcher(db):
            result = _run(_handle_get_proactive_suggestions({}))

        for s in result["suggestions"]:
            assert "type" in s, f"Missing 'type' in suggestion: {s}"
            assert "message" in s, f"Missing 'message' in suggestion: {s}"
            assert "action_tool" in s, f"Missing 'action_tool' in suggestion: {s}"
            assert "action_params" in s, f"Missing 'action_params' in suggestion: {s}"
