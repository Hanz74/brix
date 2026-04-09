from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

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
