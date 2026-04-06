"""Tests for the db.exec brick."""
from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from brix.runners.db_exec import DbExecRunner


def _make_step(**kwargs):
    defaults = {"timeout": None}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _fetch_one(db_path: str, query: str, params: tuple = ()) -> tuple | None:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(query, params)
        return cur.fetchone()
    finally:
        conn.close()


async def test_sqlite_insert_and_verify_row(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()

    runner = DbExecRunner()
    step = _make_step(connection=str(db_path), query="INSERT INTO users (name) VALUES (?)", params=["Alice"])
    result = await runner.execute(step, None)

    assert result["success"] is True
    assert result["data"]["affected_rows"] == 1
    assert _fetch_one(str(db_path), "SELECT name FROM users WHERE id = 1") == ("Alice",)


async def test_sqlite_update_verify_affected_rows(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users (name) VALUES ('Alice')")
    conn.commit()
    conn.close()

    runner = DbExecRunner()
    step = _make_step(connection=str(db_path), query="UPDATE users SET name = ? WHERE id = ?", params=["Bob", 1])
    result = await runner.execute(step, None)

    assert result["success"] is True
    assert result["data"]["affected_rows"] == 1
    assert _fetch_one(str(db_path), "SELECT name FROM users WHERE id = 1") == ("Bob",)


async def test_sqlite_delete_verify_affected_rows(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users (name) VALUES ('Alice')")
    conn.commit()
    conn.close()

    runner = DbExecRunner()
    step = _make_step(connection=str(db_path), query="DELETE FROM users WHERE id = ?", params=[1])
    result = await runner.execute(step, None)

    assert result["success"] is True
    assert result["data"]["affected_rows"] == 1
    assert _fetch_one(str(db_path), "SELECT COUNT(*) FROM users") == (0,)


async def test_parameterized_query_with_qmark(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()

    runner = DbExecRunner()
    step = _make_step(
        connection=str(db_path),
        query="INSERT INTO users (name) VALUES (?)",
        params=["Charlie"],
    )
    result = await runner.execute(step, None)

    assert result["success"] is True
    assert _fetch_one(str(db_path), "SELECT name FROM users") == ("Charlie",)


async def test_bad_query_returns_error(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()

    runner = DbExecRunner()
    step = _make_step(connection=str(db_path), query="INSERT INTO missing_table (name) VALUES (?)", params=["Alice"])
    result = await runner.execute(step, None)

    assert result["success"] is False
    assert "missing_table" in result["error"] or "no such table" in result["error"].lower()
