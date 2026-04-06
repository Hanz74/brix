from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from brix.runners.db_exec import DbExecRunner


def _make_step(**kwargs):
    defaults = {"timeout": None, "config": None, "params": None}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _fetch_one(db_path: str, query: str, params: tuple | dict = ()) -> tuple | None:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(query, params)
        return cur.fetchone()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_empty_dict_params_does_not_fall_through_to_step_config(monkeypatch, tmp_path):
    db_path = tmp_path / "rf02-empty-dict.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()

    runner = DbExecRunner()
    captured: dict[str, object] = {}

    def _resolve_connection(connection_ref: str, context: object) -> tuple[str, str]:
        captured["connection"] = connection_ref
        return "sqlite", str(db_path)

    def _run_exec(driver: str, dsn: str, query: str, params: object) -> int:
        captured["driver"] = driver
        captured["dsn"] = dsn
        captured["query"] = query
        captured["params"] = params
        return 1

    runner._resolve_connection = _resolve_connection  # type: ignore[method-assign]
    runner._run_exec = _run_exec  # type: ignore[method-assign]

    step = _make_step(
        connection=str(db_path),
        query="INSERT INTO users DEFAULT VALUES",
        params={},
        config={"connection": "wrong-conn", "query": "SELECT 1", "params": ["wrong"]},
    )

    result = await runner.execute(step, None)

    assert result["success"] is True
    assert captured == {
        "connection": str(db_path),
        "driver": "sqlite",
        "dsn": str(db_path),
        "query": "INSERT INTO users DEFAULT VALUES",
        "params": {},
    }


@pytest.mark.asyncio
async def test_list_params_passed_correctly(tmp_path):
    db_path = tmp_path / "rf02-list.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()

    runner = DbExecRunner()
    step = _make_step(
        connection=str(db_path),
        query="INSERT INTO users (name) VALUES (?)",
        params=["Alice"],
    )

    result = await runner.execute(step, None)

    assert result["success"] is True
    assert result["data"]["affected_rows"] == 1
    assert _fetch_one(str(db_path), "SELECT name FROM users WHERE id = 1") == ("Alice",)


@pytest.mark.asyncio
async def test_dict_params_with_named_placeholders_work(tmp_path):
    db_path = tmp_path / "rf02-dict.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    conn.close()

    runner = DbExecRunner()
    step = _make_step(
        connection=str(db_path),
        query="INSERT INTO users (name) VALUES (:name)",
        params={"name": "Bob"},
    )

    result = await runner.execute(step, None)

    assert result["success"] is True
    assert result["data"]["affected_rows"] == 1
    assert _fetch_one(str(db_path), "SELECT name FROM users WHERE id = 1") == ("Bob",)
