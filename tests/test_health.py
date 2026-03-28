"""Tests for the brix__health MCP tool — T-BRIX-DB-25.

Covers:
- _check_db(): status ok, size_mb, tables dict
- _check_runners(): status ok, total > 0, runners list
- _check_bricks(): status ok, total > 0, system/custom split
- _check_bricks(): broken extends refs trigger warn status
- _check_pipelines(): status ok, total, recent_runs_24h
- _check_deprecated(): status ok when count == 0, warn when count > 0
- _check_triggers(): status ok with no failing, warn with failing triggers
- _check_retention(): ok / warn / error thresholds
- _aggregate_overall(): ok < warn < error precedence
- _handle_health(): integration — returns overall + all subsystem keys
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import brix.mcp_handlers.health as health_mod
from brix.mcp_handlers.health import (
    _aggregate_overall,
    _check_bricks,
    _check_db,
    _check_deprecated,
    _check_pipelines,
    _check_retention,
    _check_runners,
    _check_triggers,
    _handle_health,
)


# ---------------------------------------------------------------------------
# _check_db
# ---------------------------------------------------------------------------

class TestCheckDb:
    def test_returns_ok_status(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        with patch.object(health_mod, "BrixDB", return_value=db):
            result = _check_db()
        assert result["status"] == "ok"

    def test_includes_size_mb(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        with patch.object(health_mod, "BrixDB", return_value=db):
            result = _check_db()
        assert "size_mb" in result
        assert isinstance(result["size_mb"], float)

    def test_includes_tables_dict(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        with patch.object(health_mod, "BrixDB", return_value=db):
            result = _check_db()
        assert "tables" in result
        assert isinstance(result["tables"], dict)
        # runs table should exist in a freshly initialised DB
        assert "runs" in result["tables"]

    def test_error_on_exception(self):
        with patch.object(health_mod, "BrixDB", side_effect=RuntimeError("boom")):
            result = _check_db()
        assert result["status"] == "error"
        assert "boom" in result["error"]


# ---------------------------------------------------------------------------
# _check_runners
# ---------------------------------------------------------------------------

class TestCheckRunners:
    def test_returns_ok_and_has_runners(self):
        result = _check_runners()
        assert result["status"] == "ok"
        assert result["total"] > 0
        assert isinstance(result["runners"], list)
        assert len(result["runners"]) == result["total"]

    def test_error_on_exception(self):
        with patch.object(health_mod, "discover_runners", side_effect=ImportError("nope")):
            result = _check_runners()
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# _check_bricks
# ---------------------------------------------------------------------------

class TestCheckBricks:
    def test_returns_ok_normally(self):
        result = _check_bricks()
        assert result["status"] in ("ok", "warn")
        assert result["total"] > 0
        assert "system" in result
        assert "custom" in result

    def test_system_plus_custom_equals_total(self):
        result = _check_bricks()
        assert result["system"] + result["custom"] == result["total"]

    def test_broken_extends_triggers_warn(self):
        from brix.bricks.schema import BrickSchema
        bad_brick = BrickSchema(
            name="bad_child",
            type="python",
            runner="python",
            description="",
            when_to_use="test",
            extends="nonexistent_parent",
        )
        mock_registry = MagicMock()
        mock_registry.list_all.return_value = [bad_brick]
        with patch.object(health_mod, "_registry", mock_registry):
            result = _check_bricks()
        assert result["status"] == "warn"
        assert "broken_extends" in result
        assert any("bad_child" in r for r in result["broken_extends"])

    def test_error_on_exception(self):
        mock_registry = MagicMock()
        mock_registry.list_all.side_effect = RuntimeError("crash")
        with patch.object(health_mod, "_registry", mock_registry):
            result = _check_bricks()
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# _check_pipelines
# ---------------------------------------------------------------------------

class TestCheckPipelines:
    def test_returns_ok(self, tmp_path):
        from brix.db import BrixDB
        from brix.pipeline_store import PipelineStore
        db = BrixDB(db_path=tmp_path / "brix.db")
        store = PipelineStore(pipelines_dir=tmp_path)
        with patch.object(health_mod, "BrixDB", return_value=db), \
             patch.object(health_mod, "_pipeline_dir", return_value=tmp_path), \
             patch.object(health_mod, "PipelineStore", return_value=store):
            result = _check_pipelines()
        assert result["status"] == "ok"
        assert "total" in result
        assert "recent_runs_24h" in result

    def test_total_is_integer(self, tmp_path):
        from brix.db import BrixDB
        from brix.pipeline_store import PipelineStore
        db = BrixDB(db_path=tmp_path / "brix.db")
        store = PipelineStore(pipelines_dir=tmp_path)
        with patch.object(health_mod, "BrixDB", return_value=db), \
             patch.object(health_mod, "_pipeline_dir", return_value=tmp_path), \
             patch.object(health_mod, "PipelineStore", return_value=store):
            result = _check_pipelines()
        assert isinstance(result["total"], int)


# ---------------------------------------------------------------------------
# _check_deprecated
# ---------------------------------------------------------------------------

class TestCheckDeprecated:
    def test_ok_when_zero(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        with patch.object(health_mod, "BrixDB", return_value=db):
            result = _check_deprecated()
        assert result["status"] == "ok"
        assert result["count"] == 0

    def test_warn_when_deprecated_exist(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        db.record_deprecated_usage("pipe", "step1", "old_type", "new_type")
        with patch.object(health_mod, "BrixDB", return_value=db):
            result = _check_deprecated()
        assert result["status"] == "warn"
        assert result["count"] >= 1


# ---------------------------------------------------------------------------
# _check_triggers
# ---------------------------------------------------------------------------

class TestCheckTriggers:
    def test_ok_when_no_failing(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        with patch.object(health_mod, "BrixDB", return_value=db):
            result = _check_triggers()
        assert result["status"] == "ok"
        assert "failing" not in result

    def test_warn_when_failing_trigger(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        # Insert a trigger with last_status = error
        from uuid import uuid4
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        with db._connect() as conn:
            conn.execute(
                """INSERT INTO triggers
                   (id, name, type, config_json, pipeline, enabled, created_at, updated_at, last_status)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (str(uuid4()), "broken-trigger", "cron", "{}", "my-pipeline", 1, now, now, "error"),
            )
        with patch.object(health_mod, "BrixDB", return_value=db):
            result = _check_triggers()
        assert result["status"] == "warn"
        assert "broken-trigger" in result.get("failing", [])


# ---------------------------------------------------------------------------
# _check_retention
# ---------------------------------------------------------------------------

class TestCheckRetention:
    def test_ok_below_70_pct(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        with patch.object(health_mod, "BrixDB", return_value=db), \
             patch.dict("os.environ", {"BRIX_RETENTION_MAX_MB": "500"}):
            result = _check_retention()
        assert result["status"] == "ok"
        assert result["used_pct"] < 70

    def test_warn_at_70_pct(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        # Patch db.db_path.stat() to return a fake size at 75% of 1 MB
        fake_stat = MagicMock()
        fake_stat.st_size = int(0.75 * 1024 * 1024)
        mock_path = MagicMock(spec=Path)
        mock_path.stat.return_value = fake_stat
        mock_path.exists.return_value = True
        db.db_path = mock_path
        with patch.object(health_mod, "BrixDB", return_value=db), \
             patch.dict("os.environ", {"BRIX_RETENTION_MAX_MB": "1"}):
            result = _check_retention()
        assert result["status"] == "warn"

    def test_error_at_90_pct(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "brix.db")
        fake_stat = MagicMock()
        fake_stat.st_size = int(0.95 * 1024 * 1024)
        mock_path = MagicMock(spec=Path)
        mock_path.stat.return_value = fake_stat
        mock_path.exists.return_value = True
        db.db_path = mock_path
        with patch.object(health_mod, "BrixDB", return_value=db), \
             patch.dict("os.environ", {"BRIX_RETENTION_MAX_MB": "1"}):
            result = _check_retention()
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# _aggregate_overall
# ---------------------------------------------------------------------------

class TestAggregateOverall:
    def test_all_ok(self):
        subs = {"a": {"status": "ok"}, "b": {"status": "ok"}}
        assert _aggregate_overall(subs) == "ok"

    def test_warn_beats_ok(self):
        subs = {"a": {"status": "ok"}, "b": {"status": "warn"}}
        assert _aggregate_overall(subs) == "warn"

    def test_error_beats_warn(self):
        subs = {"a": {"status": "warn"}, "b": {"status": "error"}}
        assert _aggregate_overall(subs) == "error"

    def test_error_beats_ok(self):
        subs = {"a": {"status": "ok"}, "b": {"status": "error"}}
        assert _aggregate_overall(subs) == "error"


# ---------------------------------------------------------------------------
# _handle_health — integration
# ---------------------------------------------------------------------------

class TestHandleHealth:
    @pytest.mark.asyncio
    async def test_returns_all_subsystem_keys(self):
        result = await _handle_health({})
        expected_keys = {"overall", "db", "runners", "bricks", "pipelines",
                         "deprecated", "triggers", "retention"}
        assert expected_keys.issubset(result.keys())

    @pytest.mark.asyncio
    async def test_overall_is_valid_value(self):
        result = await _handle_health({})
        assert result["overall"] in ("ok", "warn", "error")

    @pytest.mark.asyncio
    async def test_subsystems_all_have_status(self):
        result = await _handle_health({})
        for key in ("db", "runners", "bricks", "pipelines", "deprecated", "triggers", "retention"):
            assert "status" in result[key], f"subsystem '{key}' missing 'status'"
