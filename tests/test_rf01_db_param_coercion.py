from __future__ import annotations

import sqlite3
import sys
from types import SimpleNamespace

from brix.runners.db_exec import _execute_postgresql as exec_postgresql
from brix.runners.db_query import _colon_to_pyformat, _execute_sqlite


def test_colon_to_pyformat_converts_single_param():
    query = "SELECT * FROM users WHERE name = :name"
    params = {"name": "Alice"}

    assert _colon_to_pyformat(query, params) == "SELECT * FROM users WHERE name = %(name)s"


def test_colon_to_pyformat_converts_multiple_params():
    query = "SELECT * FROM users WHERE name = :name AND age >= :min_age"
    params = {"name": "Alice", "min_age": 21}

    assert _colon_to_pyformat(query, params) == (
        "SELECT * FROM users WHERE name = %(name)s AND age >= %(min_age)s"
    )


def test_colon_to_pyformat_does_not_convert_inside_quotes():
    query = "SELECT ':name', created_at FROM events WHERE slug = :slug AND ts >= '2024-01-01T12:34:56'"
    params = {"slug": "launch"}

    assert _colon_to_pyformat(query, params) == (
        "SELECT ':name', created_at FROM events WHERE slug = %(slug)s "
        "AND ts >= '2024-01-01T12:34:56'"
    )


def test_sqlite_path_still_accepts_colon_name_params(tmp_path):
    db_path = tmp_path / "rf01.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users (name) VALUES ('Alice')")
    conn.commit()
    conn.close()

    rows = _execute_sqlite(
        str(db_path),
        "SELECT id, name FROM users WHERE name = :name",
        {"name": "Alice"},
    )

    assert rows == [{"id": 1, "name": "Alice"}]


def test_db_exec_postgresql_converts_dict_params_before_execute(monkeypatch):
    calls: list[tuple[str, dict[str, str]]] = []

    class FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params):
            calls.append((query, params))

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def close(self):
            return None

    fake_psycopg2 = SimpleNamespace(connect=lambda dsn: FakeConnection())
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)

    affected = exec_postgresql(
        "postgresql://example",
        "UPDATE users SET name = :name WHERE id = :id",
        {"name": "Bob", "id": 1},
    )

    assert affected == 1
    assert calls == [("UPDATE users SET name = %(name)s WHERE id = %(id)s", {"name": "Bob", "id": 1})]
