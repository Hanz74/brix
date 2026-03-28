"""Tests for T-BRIX-V7-04 — Execution Data: persist_output + rendered params +
context snapshot + stderr.

Covers:
- Step.persist_output field defaults to False
- BrixDB.save_step_output() / get_step_output() / get_step_outputs()
- RunHistory.get_step_outputs()
- Engine persists when persist_output=True
- Engine persists when BRIX_DEBUG env var is set
- Engine does NOT persist when neither flag is active
- stderr is captured and stored for Python runner steps
- context snapshot contains key→type info (not raw data)
"""
import json
import os
from pathlib import Path
from typing import Any

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
# Model: persist_output field
# ---------------------------------------------------------------------------

class TestPersistOutputField:
    def test_default_is_false(self):
        step = Step(id="s1", type="set", values={"x": 1})
        assert step.persist_output is False

    def test_can_be_set_true(self):
        step = Step(id="s1", type="set", values={"x": 1}, persist_output=True)
        assert step.persist_output is True


# ---------------------------------------------------------------------------
# DB: step_outputs table
# ---------------------------------------------------------------------------

class TestStepOutputsTable:
    def test_table_exists(self, db):
        with db._connect() as conn:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "step_outputs" in tables

    def test_save_and_get_step_output(self, db):
        db.save_step_output(
            run_id="run-1",
            step_id="step-a",
            output={"key": "value"},
            rendered_params={"p": 42},
            stderr_text="some warning",
            context_snapshot={"input": "dict(2 keys)"},
        )
        row = db.get_step_output("run-1", "step-a")
        assert row is not None
        assert row["run_id"] == "run-1"
        assert row["step_id"] == "step-a"
        assert row["output"] == {"key": "value"}
        assert row["rendered_params"] == {"p": 42}
        assert row["stderr_text"] == "some warning"
        assert row["context"] == {"input": "dict(2 keys)"}
        assert "created_at" in row

    def test_get_step_output_not_found(self, db):
        assert db.get_step_output("no-run", "no-step") is None

    def test_get_step_outputs_returns_all_for_run(self, db):
        db.save_step_output(run_id="run-2", step_id="step-1", output={"a": 1})
        db.save_step_output(run_id="run-2", step_id="step-2", output={"b": 2})
        db.save_step_output(run_id="run-3", step_id="step-1", output={"c": 3})

        results = db.get_step_outputs("run-2")
        assert len(results) == 2
        step_ids = {r["step_id"] for r in results}
        assert step_ids == {"step-1", "step-2"}

    def test_get_step_outputs_empty_when_no_data(self, db):
        assert db.get_step_outputs("non-existent-run") == []

    def test_save_with_none_values(self, db):
        db.save_step_output(run_id="run-4", step_id="step-x")
        row = db.get_step_output("run-4", "step-x")
        assert row["output"] is None
        assert row["rendered_params"] is None
        assert row["stderr_text"] is None
        assert row["context"] is None

    def test_json_columns_are_deserialized(self, db):
        db.save_step_output(
            run_id="run-5",
            step_id="step-y",
            output=[1, 2, 3],
            rendered_params={"list": [4, 5]},
        )
        row = db.get_step_output("run-5", "step-y")
        assert row["output"] == [1, 2, 3]
        assert row["rendered_params"] == {"list": [4, 5]}


# ---------------------------------------------------------------------------
# RunHistory: get_step_outputs proxy
# ---------------------------------------------------------------------------

class TestRunHistoryGetStepOutputs:
    def test_returns_empty_list_when_no_outputs(self, history):
        assert history.get_step_outputs("no-run") == []

    def test_returns_persisted_outputs(self, history):
        history._db.save_step_output(
            run_id="run-A",
            step_id="step-1",
            output={"x": 10},
        )
        results = history.get_step_outputs("run-A")
        assert len(results) == 1
        assert results[0]["output"] == {"x": 10}


# ---------------------------------------------------------------------------
# Engine: persist_output integration
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_pipeline_yaml():
    return """\
name: test-persist
steps:
  - id: produce
    type: set
    values:
      answer: 42
"""


@pytest.fixture
def python_pipeline_yaml(tmp_path):
    script = tmp_path / "echo_helper.py"
    script.write_text(
        'import json, sys\n'
        'print(json.dumps({"result": "ok"}))\n'
        'print("stderr line", file=sys.stderr)\n'
    )
    return f"""\
name: test-persist-python
steps:
  - id: run_script
    type: python
    script: {script}
    persist_output: true
"""


class TestEnginePersistOutput:
    def _run_pipeline(self, yaml_text: str, tmp_path: Path, env_overrides: dict = None) -> str:
        """Helper: run a pipeline and return the run_id."""
        import asyncio
        import yaml as pyyaml
        from brix.engine import PipelineEngine
        from brix.models import Pipeline
        from brix.history import RunHistory
        import brix.history as history_mod

        data = pyyaml.safe_load(yaml_text)
        pipeline = Pipeline(**data)
        engine = PipelineEngine()

        # Redirect history DB to tmp_path
        db_path = tmp_path / "brix.db"
        original_path = history_mod.HISTORY_DB_PATH
        history_mod.HISTORY_DB_PATH = db_path

        old_env = {}
        if env_overrides:
            for k, v in env_overrides.items():
                old_env[k] = os.environ.get(k)
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

        try:
            result = asyncio.run(engine.run(pipeline))
            return result.run_id, db_path
        finally:
            history_mod.HISTORY_DB_PATH = original_path
            for k, v in old_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_no_persist_by_default(self, tmp_path, simple_pipeline_yaml):
        run_id, db_path = self._run_pipeline(
            simple_pipeline_yaml, tmp_path,
            env_overrides={"BRIX_DEBUG": None},
        )
        db = BrixDB(db_path=db_path)
        outputs = db.get_step_outputs(run_id)
        assert outputs == [], "No step outputs should be persisted without persist_output or BRIX_DEBUG"

    def test_persist_when_brix_debug_set(self, tmp_path, simple_pipeline_yaml):
        run_id, db_path = self._run_pipeline(
            simple_pipeline_yaml, tmp_path,
            env_overrides={"BRIX_DEBUG": "true"},
        )
        db = BrixDB(db_path=db_path)
        outputs = db.get_step_outputs(run_id)
        assert len(outputs) == 1
        assert outputs[0]["step_id"] == "produce"
        # set runner output is stored
        assert outputs[0]["output"] is not None or outputs[0]["rendered_params"] is not None

    def test_persist_python_step_captures_stderr(self, tmp_path, python_pipeline_yaml):
        run_id, db_path = self._run_pipeline(python_pipeline_yaml, tmp_path)
        db = BrixDB(db_path=db_path)
        outputs = db.get_step_outputs(run_id)
        assert len(outputs) == 1
        row = outputs[0]
        assert row["step_id"] == "run_script"
        assert row["output"] == {"result": "ok"}
        assert "stderr line" in (row["stderr_text"] or "")

    def test_context_snapshot_has_type_info(self, tmp_path, python_pipeline_yaml):
        run_id, db_path = self._run_pipeline(python_pipeline_yaml, tmp_path)
        db = BrixDB(db_path=db_path)
        outputs = db.get_step_outputs(run_id)
        assert outputs
        snapshot = outputs[0]["context"]
        # Should be a dict of {key: type_description} — not raw values
        assert isinstance(snapshot, dict)
        # Each value should be a string (type description), not raw data
        for v in snapshot.values():
            assert isinstance(v, str), f"Expected type string, got {type(v)}: {v}"


# ---------------------------------------------------------------------------
# Engine: _should_persist helper
# ---------------------------------------------------------------------------

class TestShouldPersist:
    def test_false_by_default(self):
        from brix.engine import PipelineEngine
        step = Step(id="s", type="set", values={})
        orig = os.environ.pop("BRIX_DEBUG", None)
        try:
            assert PipelineEngine._should_persist(step) is False
        finally:
            if orig is not None:
                os.environ["BRIX_DEBUG"] = orig

    def test_true_when_persist_output(self):
        from brix.engine import PipelineEngine
        step = Step(id="s", type="set", values={}, persist_output=True)
        orig = os.environ.pop("BRIX_DEBUG", None)
        try:
            assert PipelineEngine._should_persist(step) is True
        finally:
            if orig is not None:
                os.environ["BRIX_DEBUG"] = orig

    def test_true_when_brix_debug(self):
        from brix.engine import PipelineEngine
        step = Step(id="s", type="set", values={})
        os.environ["BRIX_DEBUG"] = "1"
        try:
            assert PipelineEngine._should_persist(step) is True
        finally:
            del os.environ["BRIX_DEBUG"]


# ---------------------------------------------------------------------------
# Engine: _context_snapshot helper
# ---------------------------------------------------------------------------

class TestContextSnapshot:
    def test_returns_dict_of_type_strings(self):
        from brix.engine import PipelineEngine

        class FakeContext:
            def to_jinja_context(self):
                return {
                    "input": {"a": 1, "b": 2},
                    "step1": {"output": [1, 2, 3]},
                    "run_id": "abc",
                }

        snapshot = PipelineEngine._context_snapshot(FakeContext())
        assert snapshot["input"] == "dict(2 keys)"
        assert snapshot["step1"] == "dict(1 keys)"
        assert snapshot["run_id"] == "str"

    def test_returns_empty_on_error(self):
        from brix.engine import PipelineEngine

        class BrokenContext:
            def to_jinja_context(self):
                raise RuntimeError("boom")

        snapshot = PipelineEngine._context_snapshot(BrokenContext())
        assert snapshot == {}
