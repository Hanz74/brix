from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

import brix.context as context_module
import brix.db as db_module
import brix.history as history_module
from brix.engine import PipelineEngine
from brix.history import RunHistory
from brix.loader import PipelineLoader
from brix.runners.db_query import DbQueryRunner


class _Step:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.fixture()
def sqlite_dsn(tmp_path):
    db_path = tmp_path / "bug104.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice')")
    conn.commit()
    conn.close()
    return str(db_path)


@pytest.mark.parametrize(
    ("query", "expected_row_count"),
    [
        ("SELECT * FROM users", 1),
        ("WITH selected AS (SELECT * FROM users) SELECT * FROM selected", 1),
        ("EXPLAIN QUERY PLAN SELECT * FROM users", 1),
    ],
)
async def test_db_query_allows_select_like_statements(sqlite_dsn, query, expected_row_count):
    runner = DbQueryRunner()
    step = _Step(connection=sqlite_dsn, query=query)

    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert len(result["data"]) == expected_row_count


@pytest.mark.parametrize(
    "query",
    [
        "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        "  /* leading block comment */ UPDATE users SET name = 'Bob' WHERE id = 1",
        "\n-- leading line comment\nDELETE FROM users WHERE id = 1",
        "/* a */ -- b\n MERGE INTO users USING users ON 1=0 WHEN NOT MATCHED THEN INSERT (id, name) VALUES (3, 'Carol')",
    ],
)
async def test_db_query_blocks_dml_before_execution(query):
    runner = DbQueryRunner()
    step = _Step(connection="postgresql://user:pw@host/db", query=query)

    with patch.object(DbQueryRunner, "_resolve_connection") as resolve_connection:
        with patch.object(DbQueryRunner, "_run_query") as run_query:
            result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert result["error"] == "db.query does not support DML. Use db.exec for INSERT/UPDATE/DELETE."
    resolve_connection.assert_not_called()
    run_query.assert_not_called()


@pytest.mark.asyncio
async def test_engine_run_surfaces_db_query_dml_guard_in_real_runtime(tmp_path):
    db_module.BRIX_DB_PATH = tmp_path / "brix.db"
    history_module.HISTORY_DB_PATH = db_module.BRIX_DB_PATH
    context_module.WORKDIR_BASE = tmp_path / "runs"
    context_module.CACHE_BASE = tmp_path / "cache"

    db_path = tmp_path / "bug104-runtime.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO users VALUES (1, 'Alice')")
        conn.commit()
    finally:
        conn.close()

    pipeline = PipelineLoader().load_from_string(
        f"""
name: bug104-runtime-dml-guard
steps:
  - id: prepare
    type: flow.set
    values:
      target_id: 1
  - id: blocked_write
    type: db.query
    config:
      connection: "sqlite:///{db_path}"
      query: "UPDATE users SET name = 'Bob' WHERE id = {{ prepare.output.target_id }}"
  - id: after_write
    type: flow.set
    values:
      marker: should-not-run
"""
    )

    engine = PipelineEngine()
    engine.register_runner("flow.set", engine._runners["set"])
    engine.register_runner("db.query", engine._runners["db_query"])

    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["prepare"].status == "ok"
    assert result.steps["blocked_write"].status == "error"
    assert result.steps["blocked_write"].error_message == (
        "db.query does not support DML. Use db.exec for INSERT/UPDATE/DELETE."
    )
    assert "after_write" not in result.steps or result.steps["after_write"].status != "ok"

    history = RunHistory()
    errors = history.get_run_errors(run_id=result.run_id)

    assert len(errors) == 1
    assert errors[0]["step_id"] == "blocked_write"
    assert errors[0]["error_message"] == "db.query does not support DML. Use db.exec for INSERT/UPDATE/DELETE."

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT name FROM users WHERE id = 1").fetchone()
    finally:
        conn.close()

    assert row == ("Alice",)
