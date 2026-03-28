"""Tests for brix.runners.db_query.DbQueryRunner."""
from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from brix.runners.db_query import DbQueryRunner, _detect_driver, _strip_sqlite_prefix


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step stand-in for tests."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _FakeContext:
    """Minimal PipelineContext stand-in."""

    def __init__(self, input_data: dict | None = None):
        self.input = input_data or {}

    def to_jinja_context(self) -> dict:
        return {"input": self.input}


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------


def test_detect_driver_postgresql():
    assert _detect_driver("postgresql://user:pw@host/db") == "postgresql"
    assert _detect_driver("postgres://user:pw@host/db") == "postgresql"


def test_detect_driver_sqlite():
    assert _detect_driver("sqlite:///path/to/db.sqlite") == "sqlite"
    assert _detect_driver(":memory:") == "sqlite"
    assert _detect_driver("/tmp/mydb.sqlite3") == "sqlite"


def test_strip_sqlite_prefix():
    assert _strip_sqlite_prefix("sqlite:///tmp/test.db") == "/tmp/test.db"
    assert _strip_sqlite_prefix("sqlite:///:memory:") == ":memory:"
    assert _strip_sqlite_prefix(":memory:") == ":memory:"
    assert _strip_sqlite_prefix("/tmp/plain.db") == "/tmp/plain.db"


# ---------------------------------------------------------------------------
# config_schema / input_type / output_type
# ---------------------------------------------------------------------------


def test_config_schema():
    runner = DbQueryRunner()
    schema = runner.config_schema()
    assert schema["type"] == "object"
    assert "connection" in schema["properties"]
    assert "query" in schema["properties"]
    assert "params" in schema["properties"]
    assert "connection" in schema["required"]
    assert "query" in schema["required"]


def test_input_type():
    assert DbQueryRunner().input_type() == "none"


def test_output_type():
    assert DbQueryRunner().output_type() == "list[dict]"


# ---------------------------------------------------------------------------
# SELECT against SQLite in-memory
# ---------------------------------------------------------------------------


@pytest.fixture()
def sqlite_dsn(tmp_path):
    """Create a temporary SQLite DB with a users table and return its DSN."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
    conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
    conn.commit()
    conn.close()
    return str(db_path)


async def test_select_all_rows(sqlite_dsn):
    """Simple SELECT returns all rows as list of dicts."""
    runner = DbQueryRunner()
    step = _Step(connection=sqlite_dsn, query="SELECT * FROM users ORDER BY id")
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    data = result["data"]
    assert data["row_count"] == 2
    assert data["rows"][0]["name"] == "Alice"
    assert data["rows"][1]["name"] == "Bob"
    assert set(data["columns"]) == {"id", "name", "age"}


async def test_empty_result(sqlite_dsn):
    """SELECT with no matching rows returns empty list, row_count=0."""
    runner = DbQueryRunner()
    step = _Step(connection=sqlite_dsn, query="SELECT * FROM users WHERE id = 999")
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    data = result["data"]
    assert data["row_count"] == 0
    assert data["rows"] == []
    assert data["columns"] == []


# ---------------------------------------------------------------------------
# Parametrized query
# ---------------------------------------------------------------------------


async def test_parameterized_query(sqlite_dsn):
    """Named params are passed to cursor.execute (SQL-injection-safe)."""
    runner = DbQueryRunner()
    step = _Step(
        connection=sqlite_dsn,
        query="SELECT * FROM users WHERE id = :id",
        params={"id": 1},
    )
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"]["row_count"] == 1
    assert result["data"]["rows"][0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# Jinja2 rendering in query
# ---------------------------------------------------------------------------


async def test_jinja2_in_query(sqlite_dsn):
    """{{ input.min_age }} is rendered into the query before execution."""
    runner = DbQueryRunner()
    ctx = _FakeContext({"min_age": 28})
    step = _Step(
        connection=sqlite_dsn,
        query="SELECT * FROM users WHERE age >= {{ input.min_age }}",
    )
    result = await runner.execute(step, context=ctx)

    assert result["success"] is True
    assert result["data"]["row_count"] == 1
    assert result["data"]["rows"][0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# Connection via ConnectionManager (mock)
# ---------------------------------------------------------------------------


async def test_connection_manager_resolution(sqlite_dsn):
    """When ConnectionManager has the name, its DSN is used."""
    fake_conn = MagicMock()
    fake_conn.driver = "sqlite"
    fake_conn.dsn = sqlite_dsn

    with patch("brix.runners.db_query.DbQueryRunner._resolve_connection", return_value=("sqlite", sqlite_dsn)):
        runner = DbQueryRunner()
        step = _Step(connection="my_named_connection", query="SELECT * FROM users ORDER BY id")
        result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"]["row_count"] == 2


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


async def test_invalid_sql(sqlite_dsn):
    """Invalid SQL returns success=False with a SQL error message."""
    runner = DbQueryRunner()
    step = _Step(connection=sqlite_dsn, query="SELECT * FROM nonexistent_table")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "SQL error" in result["error"] or "error" in result


async def test_missing_connection_field():
    """Step without 'connection' field returns a descriptive error."""
    runner = DbQueryRunner()
    step = _Step(query="SELECT 1")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "connection" in result["error"]


async def test_missing_query_field():
    """Step without 'query' field returns a descriptive error."""
    runner = DbQueryRunner()
    step = _Step(connection=":memory:")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "query" in result["error"]


# ---------------------------------------------------------------------------
# report_progress is called
# ---------------------------------------------------------------------------


async def test_report_progress_called(sqlite_dsn):
    """execute() calls report_progress at least once (end state has pct=100)."""
    runner = DbQueryRunner()
    step = _Step(connection=sqlite_dsn, query="SELECT * FROM users")
    await runner.execute(step, context=None)

    assert runner._progress is not None
    assert runner._progress["pct"] == 100


# ---------------------------------------------------------------------------
# in-memory SQLite DSN
# ---------------------------------------------------------------------------


async def test_in_memory_sqlite():
    """':memory:' DSN works for simple queries."""
    runner = DbQueryRunner()
    # sqlite3 :memory: — create table inline via Python before passing to runner.
    # We can't set up state in the same connection, so use a helper query.
    step = _Step(connection=":memory:", query="SELECT 1 AS value")
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"]["row_count"] == 1
    assert result["data"]["rows"][0]["value"] == 1
