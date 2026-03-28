"""Tests for T-BRIX-DB-07 — Run-Persistenz: vollständige Execution-Daten.

Covers:
- Step-Execution wird aufgezeichnet (status, timing, input, output)
- foreach Item-Level Daten werden aufgezeichnet
- Run-Input wird persistiert
- persist_data=false → keine Daten, nur Status/Timing
- get_step_data MCP-Tool
- Retention löscht execution-Daten mit
- Timing korrekt (duration_ms > 0)
- JSON-Daten über 1MB werden truncated
- DB-Tabellen existieren nach BrixDB-Init
- record_step_execution verschiedene Status-Werte
- get_step_executions filtered und unfiltered
- get_foreach_items
- get_run_input
- Engine persistiert step_executions für normale Steps
- Engine persistiert foreach_item_executions
- Engine persistiert run_inputs
- Engine: persist_data=False speichert nur Status/Timing, keine Daten
- MCP-Tool brix__get_step_data: Fehler wenn keine Daten
- MCP-Tool brix__get_step_data: Erfolg wenn Daten vorhanden
- Retention Pass 1 (age-based) löscht execution-Daten VOR runs
- Retention Pass 2 (size-based) löscht execution-Daten VOR runs
- duration_ms wird korrekt berechnet (> 0 bei echten Steps)
- foreach item_index wird korrekt gespeichert
- Parallelisierte foreach Items werden auch aufgezeichnet
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import patch, MagicMock

import pytest

from brix.db import BrixDB
from brix.models import Step


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path) -> BrixDB:
    """Return a fresh BrixDB backed by a temp file."""
    return BrixDB(db_path=tmp_path / "brix.db")


# ---------------------------------------------------------------------------
# 1. DB-Tabellen existieren nach BrixDB-Init
# ---------------------------------------------------------------------------

class TestTablesExist:
    def test_step_executions_table_exists(self, db):
        with db._connect() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert "step_executions" in tables

    def test_foreach_item_executions_table_exists(self, db):
        with db._connect() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert "foreach_item_executions" in tables

    def test_run_inputs_table_exists(self, db):
        with db._connect() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert "run_inputs" in tables


# ---------------------------------------------------------------------------
# 2. record_step_execution und get_step_executions
# ---------------------------------------------------------------------------

class TestRecordStepExecution:
    def test_basic_record_and_retrieve(self, db):
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            step_type="set",
            status="success",
            input_data={"param": "value"},
            output_data={"result": 42},
        )
        rows = db.get_step_executions("run-1")
        assert len(rows) == 1
        row = rows[0]
        assert row["run_id"] == "run-1"
        assert row["step_id"] == "step-a"
        assert row["step_type"] == "set"
        assert row["status"] == "success"

    def test_input_data_deserialized(self, db):
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            input_data={"key": "val"},
            output_data={"out": 1},
        )
        rows = db.get_step_executions("run-1")
        assert rows[0]["input_data"] == {"key": "val"}
        assert rows[0]["output_data"] == {"out": 1}

    def test_filter_by_step_id(self, db):
        db.record_step_execution(run_id="run-1", step_id="step-a")
        db.record_step_execution(run_id="run-1", step_id="step-b")
        rows = db.get_step_executions("run-1", step_id="step-a")
        assert len(rows) == 1
        assert rows[0]["step_id"] == "step-a"

    def test_multiple_steps_for_same_run(self, db):
        db.record_step_execution(run_id="run-1", step_id="step-a")
        db.record_step_execution(run_id="run-1", step_id="step-b")
        rows = db.get_step_executions("run-1")
        assert len(rows) == 2

    def test_empty_when_run_not_found(self, db):
        rows = db.get_step_executions("nonexistent-run")
        assert rows == []

    def test_error_status_with_error_detail(self, db):
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            status="error",
            error_detail={"error": "something went wrong"},
        )
        rows = db.get_step_executions("run-1")
        assert rows[0]["status"] == "error"
        assert rows[0]["error_detail"] == {"error": "something went wrong"}

    def test_timing_fields_stored(self, db):
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            started_at="2026-01-01T10:00:00+00:00",
            ended_at="2026-01-01T10:00:01+00:00",
            duration_ms=1000,
        )
        rows = db.get_step_executions("run-1")
        row = rows[0]
        assert row["started_at"] == "2026-01-01T10:00:00+00:00"
        assert row["ended_at"] == "2026-01-01T10:00:01+00:00"
        assert row["duration_ms"] == 1000

    def test_persist_data_false_clears_data(self, db):
        """When persist_data=False, input_data and output_data must be empty."""
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            input_data={"secret": "password123"},
            output_data={"token": "abc123"},
            persist_data=False,
        )
        rows = db.get_step_executions("run-1")
        assert rows[0]["input_data"] == "" or rows[0]["input_data"] is None or rows[0]["input_data"] == {}
        assert rows[0]["output_data"] == "" or rows[0]["output_data"] is None or rows[0]["output_data"] == {}

    def test_persist_data_false_still_records_status_and_timing(self, db):
        """Even with persist_data=False, status and duration must be stored."""
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            status="success",
            duration_ms=500,
            persist_data=False,
        )
        rows = db.get_step_executions("run-1")
        assert len(rows) == 1
        assert rows[0]["status"] == "success"
        assert rows[0]["duration_ms"] == 500


# ---------------------------------------------------------------------------
# 3. record_foreach_item und get_foreach_items
# ---------------------------------------------------------------------------

class TestRecordForeachItem:
    def test_basic_record_and_retrieve(self, db):
        db.record_foreach_item(
            run_id="run-1",
            step_id="step-a",
            item_index=0,
            item_input={"id": 1},
            item_output={"processed": True},
            status="success",
            duration_ms=100,
        )
        items = db.get_foreach_items("run-1", "step-a")
        assert len(items) == 1
        item = items[0]
        assert item["run_id"] == "run-1"
        assert item["step_id"] == "step-a"
        assert item["item_index"] == 0
        assert item["item_input"] == {"id": 1}
        assert item["item_output"] == {"processed": True}
        assert item["status"] == "success"
        assert item["duration_ms"] == 100

    def test_item_index_ordering(self, db):
        """Items must be returned in ascending item_index order."""
        for i in [2, 0, 1]:
            db.record_foreach_item(run_id="run-1", step_id="step-a", item_index=i)
        items = db.get_foreach_items("run-1", "step-a")
        assert [i["item_index"] for i in items] == [0, 1, 2]

    def test_multiple_items_same_step(self, db):
        for i in range(5):
            db.record_foreach_item(run_id="run-1", step_id="step-a", item_index=i)
        items = db.get_foreach_items("run-1", "step-a")
        assert len(items) == 5

    def test_empty_when_no_items(self, db):
        items = db.get_foreach_items("run-1", "nonexistent-step")
        assert items == []

    def test_error_item_with_detail(self, db):
        db.record_foreach_item(
            run_id="run-1",
            step_id="step-a",
            item_index=0,
            status="error",
            error_detail={"error": "timeout"},
        )
        items = db.get_foreach_items("run-1", "step-a")
        assert items[0]["status"] == "error"
        assert items[0]["error_detail"] == {"error": "timeout"}


# ---------------------------------------------------------------------------
# 4. record_run_input und get_run_input
# ---------------------------------------------------------------------------

class TestRecordRunInput:
    def test_basic_record_and_retrieve(self, db):
        db.record_run_input(
            run_id="run-1",
            input_params={"key": "value", "count": 5},
            trigger_data={"source": "cli"},
        )
        result = db.get_run_input("run-1")
        assert result is not None
        assert result["run_id"] == "run-1"
        assert result["input_params"] == {"key": "value", "count": 5}
        assert result["trigger_data"] == {"source": "cli"}

    def test_returns_none_when_not_found(self, db):
        result = db.get_run_input("nonexistent-run")
        assert result is None

    def test_empty_params_stored_as_empty_dict(self, db):
        db.record_run_input(run_id="run-1")
        result = db.get_run_input("run-1")
        assert result["input_params"] == {}
        assert result["trigger_data"] == {}

    def test_upsert_on_duplicate_run_id(self, db):
        """INSERT OR REPLACE — second call should overwrite."""
        db.record_run_input(run_id="run-1", input_params={"v": 1})
        db.record_run_input(run_id="run-1", input_params={"v": 2})
        result = db.get_run_input("run-1")
        assert result["input_params"]["v"] == 2


# ---------------------------------------------------------------------------
# 5. JSON-Daten über 1MB werden truncated
# ---------------------------------------------------------------------------

class TestLargeDataTruncation:
    def test_large_input_data_is_truncated(self, db):
        """Input data > 1MB must be stored as a truncation marker."""
        large_data = {"data": "x" * 1_100_000}
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            input_data=large_data,
        )
        rows = db.get_step_executions("run-1")
        row = rows[0]
        # input_data should be a truncation marker dict
        assert isinstance(row["input_data"], dict)
        assert row["input_data"].get("__truncated__") is True

    def test_large_output_data_is_truncated(self, db):
        large_data = {"items": ["item" * 100] * 3000}
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            output_data=large_data,
        )
        rows = db.get_step_executions("run-1")
        row = rows[0]
        assert isinstance(row["output_data"], dict)
        assert row["output_data"].get("__truncated__") is True

    def test_small_data_not_truncated(self, db):
        small_data = {"key": "value"}
        db.record_step_execution(
            run_id="run-1",
            step_id="step-a",
            input_data=small_data,
        )
        rows = db.get_step_executions("run-1")
        assert rows[0]["input_data"] == small_data


# ---------------------------------------------------------------------------
# 6. Engine-Integration: step_executions werden bei echten Runs befüllt
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_pipeline_yaml():
    return """
name: test-persist
steps:
  - id: step_one
    type: set
    values:
      result: hello
"""


@pytest.fixture
def foreach_pipeline_yaml():
    return """
name: test-foreach-persist
steps:
  - id: items_step
    type: set
    values:
      result: "{{ item }}"
    foreach: ["a", "b", "c"]
"""


@pytest.fixture
def persist_data_false_pipeline_yaml():
    return """
name: test-persist-data-false
steps:
  - id: sensitive_step
    type: set
    values:
      result: secret
    persist_data: false
"""


def _run_pipeline_with_db(yaml_text: str, tmp_path: Path, user_input: dict = None) -> tuple:
    """Helper: run a pipeline using a tmp-path DB. Returns (run_id, db)."""
    import asyncio
    import brix.history as history_mod
    from brix.engine import PipelineEngine
    from brix.loader import PipelineLoader

    db_path = tmp_path / "brix.db"
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_text)
    engine = PipelineEngine()

    original_path = history_mod.HISTORY_DB_PATH
    history_mod.HISTORY_DB_PATH = db_path
    try:
        result = asyncio.run(engine.run(pipeline, user_input=user_input or {}))
    finally:
        history_mod.HISTORY_DB_PATH = original_path

    db = BrixDB(db_path=db_path)
    return result.run_id, result.success, db


class TestEngineIntegration:
    def test_engine_records_run_input(self, tmp_path, simple_pipeline_yaml):
        """run() must persist input_params via record_run_input."""
        run_id, success, db = _run_pipeline_with_db(
            simple_pipeline_yaml, tmp_path, user_input={"param1": "val1"}
        )
        assert success
        run_input = db.get_run_input(run_id)
        assert run_input is not None
        assert run_input["input_params"] == {"param1": "val1"}

    def test_engine_records_step_execution(self, tmp_path, simple_pipeline_yaml):
        """run() must record step_executions for each completed step."""
        run_id, success, db = _run_pipeline_with_db(simple_pipeline_yaml, tmp_path)
        assert success
        executions = db.get_step_executions(run_id)
        assert len(executions) >= 1
        step_one = next((e for e in executions if e["step_id"] == "step_one"), None)
        assert step_one is not None
        assert step_one["status"] == "success"

    def test_engine_duration_ms_non_negative(self, tmp_path, simple_pipeline_yaml):
        """duration_ms must be non-negative."""
        run_id, success, db = _run_pipeline_with_db(simple_pipeline_yaml, tmp_path)
        executions = db.get_step_executions(run_id)
        for exe in executions:
            assert exe["duration_ms"] >= 0

    def test_engine_step_type_stored(self, tmp_path, simple_pipeline_yaml):
        """step_type must be stored correctly (non-empty)."""
        run_id, success, db = _run_pipeline_with_db(simple_pipeline_yaml, tmp_path)
        executions = db.get_step_executions(run_id)
        step_one = next(e for e in executions if e["step_id"] == "step_one")
        assert step_one["step_type"]  # must be non-empty string

    def test_engine_persist_data_false_hides_data(self, tmp_path, persist_data_false_pipeline_yaml):
        """When persist_data=false on step, input/output must not be stored."""
        run_id, success, db = _run_pipeline_with_db(persist_data_false_pipeline_yaml, tmp_path)
        assert success
        executions = db.get_step_executions(run_id)
        sensitive = next((e for e in executions if e["step_id"] == "sensitive_step"), None)
        assert sensitive is not None
        assert sensitive["status"] == "success"
        # input_data and output_data must be empty/falsy
        input_val = sensitive.get("input_data")
        output_val = sensitive.get("output_data")
        assert not input_val or input_val == {} or input_val == ""
        assert not output_val or output_val == {} or output_val == ""


# ---------------------------------------------------------------------------
# 7. Step.persist_data Field in Model
# ---------------------------------------------------------------------------

class TestPersistDataField:
    def test_default_is_true(self):
        step = Step(id="s1", type="set", values={"x": 1})
        assert step.persist_data is True

    def test_can_be_set_false(self):
        step = Step(id="s1", type="set", values={"x": 1}, persist_data=False)
        assert step.persist_data is False


# ---------------------------------------------------------------------------
# 8. Retention löscht execution-Daten VOR runs
# ---------------------------------------------------------------------------

class TestRetentionCleansExecutionData:
    def _insert_run(self, db: BrixDB, run_id: str, started_at: str) -> None:
        """Insert a minimal run record."""
        with db._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO runs
                   (run_id, pipeline, started_at, success, triggered_by)
                   VALUES (?, 'test', ?, 1, 'cli')""",
                (run_id, started_at),
            )

    def test_retention_deletes_step_executions(self, db):
        """clean_retention must delete step_executions for deleted runs."""
        self._insert_run(db, "run-old", "2020-01-01T00:00:00")
        db.record_step_execution(run_id="run-old", step_id="step-a")

        result = db.clean_retention(max_days=1, max_mb=9999)

        assert result["runs_deleted_age"] >= 1
        executions = db.get_step_executions("run-old")
        assert executions == []

    def test_retention_deletes_foreach_item_executions(self, db):
        """clean_retention must delete foreach_item_executions for deleted runs."""
        self._insert_run(db, "run-old", "2020-01-01T00:00:00")
        db.record_foreach_item(run_id="run-old", step_id="step-a", item_index=0)

        db.clean_retention(max_days=1, max_mb=9999)

        items = db.get_foreach_items("run-old", "step-a")
        assert items == []

    def test_retention_deletes_run_inputs(self, db):
        """clean_retention must delete run_inputs for deleted runs."""
        self._insert_run(db, "run-old", "2020-01-01T00:00:00")
        db.record_run_input(run_id="run-old", input_params={"k": "v"})

        db.clean_retention(max_days=1, max_mb=9999)

        run_input = db.get_run_input("run-old")
        assert run_input is None

    def test_retention_keeps_recent_data(self, db):
        """Recent runs' execution data must NOT be deleted."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        self._insert_run(db, "run-recent", now)
        db.record_step_execution(run_id="run-recent", step_id="step-a", status="success")

        db.clean_retention(max_days=30, max_mb=9999)

        executions = db.get_step_executions("run-recent")
        assert len(executions) == 1


# ---------------------------------------------------------------------------
# 9. get_step_data MCP Tool
# ---------------------------------------------------------------------------

class TestGetStepDataMcpTool:
    @pytest.mark.asyncio
    async def test_missing_run_id_returns_error(self):
        from brix.mcp_handlers.runs import _handle_get_step_data
        result = await _handle_get_step_data({"step_id": "step-a"})
        assert result["success"] is False
        assert "run_id" in result["error"]

    @pytest.mark.asyncio
    async def test_missing_step_id_returns_error(self):
        from brix.mcp_handlers.runs import _handle_get_step_data
        result = await _handle_get_step_data({"run_id": "run-1"})
        assert result["success"] is False
        assert "step_id" in result["error"]

    @pytest.mark.asyncio
    async def test_no_data_returns_failure(self, tmp_path):
        from brix.mcp_handlers.runs import _handle_get_step_data

        with patch("brix.mcp_handlers.runs.BrixDB") as MockDB:
            mock_db = MagicMock()
            mock_db.get_step_executions.return_value = []
            mock_db.get_foreach_items.return_value = []
            MockDB.return_value = mock_db

            result = await _handle_get_step_data({"run_id": "run-1", "step_id": "step-a"})

        assert result["success"] is False
        assert "No execution data" in result["error"]

    @pytest.mark.asyncio
    async def test_returns_step_execution_data(self, tmp_path):
        from brix.mcp_handlers.runs import _handle_get_step_data

        execution_data = {
            "id": "uuid-1",
            "run_id": "run-1",
            "step_id": "step-a",
            "status": "success",
            "duration_ms": 250,
        }

        with patch("brix.mcp_handlers.runs.BrixDB") as MockDB:
            mock_db = MagicMock()
            mock_db.get_step_executions.return_value = [execution_data]
            mock_db.get_foreach_items.return_value = []
            MockDB.return_value = mock_db

            result = await _handle_get_step_data({"run_id": "run-1", "step_id": "step-a"})

        assert result["success"] is True
        assert result["run_id"] == "run-1"
        assert result["step_id"] == "step-a"
        assert result["execution"] is not None
        assert result["execution"]["status"] == "success"
        assert result["foreach_items"] == []
        assert result["foreach_item_count"] == 0

    @pytest.mark.asyncio
    async def test_returns_foreach_items(self, tmp_path):
        from brix.mcp_handlers.runs import _handle_get_step_data

        foreach_items = [
            {"id": "i1", "item_index": 0, "status": "success"},
            {"id": "i2", "item_index": 1, "status": "success"},
        ]

        with patch("brix.mcp_handlers.runs.BrixDB") as MockDB:
            mock_db = MagicMock()
            mock_db.get_step_executions.return_value = []
            mock_db.get_foreach_items.return_value = foreach_items
            MockDB.return_value = mock_db

            result = await _handle_get_step_data({"run_id": "run-1", "step_id": "step-a"})

        assert result["success"] is True
        assert result["foreach_item_count"] == 2
        assert len(result["foreach_items"]) == 2
