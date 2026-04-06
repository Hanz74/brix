from __future__ import annotations

import sqlite3

import pytest

from brix.runners.db_query import DbQueryRunner


class _Step:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.fixture()
def sqlite_dsn(tmp_path):
    db_path = tmp_path / "query-output.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
    conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
    conn.commit()
    conn.close()
    return str(db_path)


@pytest.mark.asyncio
async def test_db_query_returns_list_directly(sqlite_dsn):
    runner = DbQueryRunner()
    step = _Step(connection=sqlite_dsn, query="SELECT id, name, age FROM users ORDER BY id")

    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert isinstance(result["data"], list)
    assert "rows" not in result["data"]
    assert result["data"] == [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
    ]


@pytest.mark.asyncio
async def test_db_query_list_items_are_row_dicts(sqlite_dsn):
    runner = DbQueryRunner()
    step = _Step(connection=sqlite_dsn, query="SELECT id, name FROM users WHERE id = 1")

    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"] == [{"id": 1, "name": "Alice"}]
    assert set(result["data"][0].keys()) == {"id", "name"}


@pytest.mark.asyncio
async def test_db_query_empty_result_returns_empty_list(sqlite_dsn):
    runner = DbQueryRunner()
    step = _Step(connection=sqlite_dsn, query="SELECT id, name FROM users WHERE id = 999")

    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"] == []
