"""Regression coverage for T-BRIX-BUG-103 db.exec positional params."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from brix.engine import PipelineEngine
from brix.mcp_handlers.pipelines import _handle_test_pipeline
from brix.mcp_handlers.runs import (
    _handle_get_run_errors,
    _handle_get_run_log,
    _handle_run_pipeline,
)
from brix.pipeline_store import PipelineStore


def _configure_runtime_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    pipelines_dir = tmp_path / "pipelines"
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr("brix.context.CACHE_BASE", tmp_path / "cache")
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", pipelines_dir)
    _register_engine_aliases(monkeypatch)
    return pipelines_dir


def _create_users_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE users (name TEXT, age INTEGER, city TEXT)")
        conn.commit()
    finally:
        conn.close()


def _register_engine_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    original_init = PipelineEngine.__init__

    def patched_init(self) -> None:
        original_init(self)
        if "db_exec" in self._runners:
            self.register_runner("db.exec", self._runners["db_exec"])
        self.register_runner("flow.set", self._runners["set"])
        self.register_runner("flow.pipeline", self._runners["pipeline"])

    monkeypatch.setattr(PipelineEngine, "__init__", patched_init)


def _save_bug103_pipelines(store: PipelineStore, sqlite_db: Path, *, suffix: str) -> tuple[str, str]:
    child_name = f"bug103-child-{suffix}"
    parent_name = f"bug103-parent-{suffix}"

    store.save(
        {
            "name": child_name,
            "input": {
                "user_name": {"type": "string"},
                "user_age": {"type": "integer"},
                "city": {"type": "string"},
            },
            "steps": [
                {
                    "id": "prepare",
                    "type": "flow.set",
                    "values": {
                        "user_name": "{{ input.user_name }}",
                        "user_age": "{{ input.user_age }}",
                        "city": "{{ input.city }}",
                    },
                },
                {
                    "id": "insert_user",
                    "type": "db.exec",
                    "config": {
                        "connection": f"sqlite:///{sqlite_db}",
                        "query": "INSERT INTO users (name, age, city) VALUES (?, ?, ?)",
                        "params": [
                            "{{ prepare.output.user_name }}",
                            "{{ prepare.output.user_age }}",
                            "{{ prepare.output.city }}",
                        ],
                    },
                },
                {
                    "id": "after_insert",
                    "type": "flow.set",
                    "values": {
                        "inserted": "{{ insert_user.output.affected_rows }}",
                        "marker": "child-after",
                        "city": "{{ prepare.output.city }}",
                    },
                },
            ],
        }
    )

    store.save(
        {
            "name": parent_name,
            "input": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "city": {"type": "string"},
            },
            "steps": [
                {
                    "id": "run_child",
                    "type": "flow.pipeline",
                    "pipeline": child_name,
                    "params": {
                        "user_name": "{{ input.name }}",
                        "user_age": "{{ input.age }}",
                        "city": "{{ input.city }}",
                    },
                },
                {
                    "id": "after_child",
                    "type": "flow.set",
                    "values": {
                        "child_marker": "{{ run_child.output.marker }}",
                        "child_rows": "{{ run_child.output.inserted }}",
                        "city": "{{ run_child.output.city }}",
                    },
                },
            ],
        }
    )

    return parent_name, child_name


@pytest.mark.asyncio
async def test_run_pipeline_executes_positional_db_exec_in_subpipeline_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipelines_dir = _configure_runtime_dirs(tmp_path, monkeypatch)
    sqlite_db = tmp_path / "bug103-success.db"
    _create_users_db(sqlite_db)

    store = PipelineStore(pipelines_dir=pipelines_dir, search_paths=[pipelines_dir])
    parent_name, _child_name = _save_bug103_pipelines(store, sqlite_db, suffix="success")

    result = await _handle_run_pipeline(
        {
            "pipeline_id": parent_name,
            "input": {"name": "Alice", "age": 33, "city": "Berlin"},
        }
    )

    assert result["success"] is True, result
    assert result["steps"]["run_child"]["status"] == "ok"
    assert result["steps"]["after_child"]["status"] == "ok"
    assert result["result"] == {
        "child_marker": "child-after",
        "child_rows": 1,
        "city": "Berlin",
    }

    conn = sqlite3.connect(sqlite_db)
    try:
        rows = conn.execute("SELECT name, age, city FROM users").fetchall()
    finally:
        conn.close()

    assert rows == [("Alice", 33, "Berlin")]


@pytest.mark.asyncio
async def test_get_run_errors_and_log_surface_db_exec_positional_param_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipelines_dir = _configure_runtime_dirs(tmp_path, monkeypatch)
    sqlite_db = tmp_path / "bug103-failure.db"
    _create_users_db(sqlite_db)

    store = PipelineStore(pipelines_dir=pipelines_dir, search_paths=[pipelines_dir])
    pipeline_name = "bug103-db-exec-binding-failure"
    store.save(
        {
            "name": pipeline_name,
            "input": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
            },
            "steps": [
                {
                    "id": "prepare",
                    "type": "flow.set",
                    "values": {
                        "user_name": "{{ input.name }}",
                        "user_age": "{{ input.age }}",
                    },
                },
                {
                    "id": "insert_user",
                    "type": "db.exec",
                    "config": {
                        "connection": f"sqlite:///{sqlite_db}",
                        "query": "INSERT INTO users (name) VALUES (?)",
                        "params": [
                            "{{ prepare.output.user_name }}",
                            "{{ prepare.output.user_age }}",
                        ],
                    },
                },
                {
                    "id": "after_insert",
                    "type": "flow.set",
                    "values": {"marker": "should-not-run"},
                },
            ],
        }
    )

    result = await _handle_run_pipeline(
        {
            "pipeline_id": pipeline_name,
            "input": {"name": "Bob", "age": 41},
        }
    )

    assert result["success"] is False, result
    assert result["error"]["step_id"] == "insert_user"
    assert result["steps"]["prepare"]["status"] == "ok"
    assert result["steps"]["insert_user"]["status"] == "error"

    run_id = result["run_id"]
    errors = await _handle_get_run_errors({"run_id": run_id})
    log = await _handle_get_run_log({"run_id": run_id})

    assert errors["success"] is True
    assert errors["count"] == 1
    assert errors["errors"][0]["step_id"] == "insert_user"
    assert "Incorrect number of bindings supplied" in errors["errors"][0]["error_message"]

    assert log["success"] is True
    assert log["run_id"] == run_id
    by_id = {entry["step_id"]: entry for entry in log["steps"]}
    assert by_id["prepare"]["status"] == "ok"
    assert by_id["insert_user"]["status"] == "error"
    assert "Incorrect number of bindings supplied" in by_id["insert_user"]["error_message"]


@pytest.mark.asyncio
async def test_test_pipeline_runs_same_db_exec_positional_param_subpipeline_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipelines_dir = _configure_runtime_dirs(tmp_path, monkeypatch)
    sqlite_db = tmp_path / "bug103-test-pipeline.db"
    _create_users_db(sqlite_db)

    store = PipelineStore(pipelines_dir=pipelines_dir, search_paths=[pipelines_dir])
    parent_name, _child_name = _save_bug103_pipelines(store, sqlite_db, suffix="test-pipeline")

    result = await _handle_test_pipeline(
        {
            "name": parent_name,
            "input": {"name": "Cara", "age": 29, "city": "Hamburg"},
        }
    )

    assert result["success"] is True, result
    assert result["steps"]["run_child"]["status"] == "ok"
    assert result["steps"]["after_child"]["status"] == "ok"
    assert result["summary"]["steps_passed"] == 2
    assert result["summary"]["steps_total"] == 2

    conn = sqlite3.connect(sqlite_db)
    try:
        rows = conn.execute("SELECT name, age, city FROM users").fetchall()
    finally:
        conn.close()

    assert rows == [("Cara", 29, "Hamburg")]
