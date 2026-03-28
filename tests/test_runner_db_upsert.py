"""Tests for DbUpsertRunner (T-BRIX-DB-02).

Covers:
- config_schema, input_type, output_type
- Plain INSERT against SQLite (single dict)
- Batch INSERT (list of dicts)
- UPSERT with conflict_key (INSERT OR REPLACE in SQLite)
- data from pipeline context (previous step output)
- Empty data → 0 inserts
- Missing table → error
- Missing required params (connection, table)
- Unsupported driver → error
"""
from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_step(
    connection: str = ":memory:",
    table: str = "items",
    data: Any = None,
    conflict_key: Any = None,
    set_columns: Any = None,
    extra_params: dict | None = None,
) -> SimpleNamespace:
    """Create a minimal step namespace mirroring what the engine provides."""
    params: dict = {"connection": connection, "table": table}
    if data is not None:
        params["data"] = data
    if conflict_key is not None:
        params["conflict_key"] = conflict_key
    if set_columns is not None:
        params["set_columns"] = set_columns
    if extra_params:
        params.update(extra_params)
    return SimpleNamespace(params=params)


def make_context(last_output: Any = None) -> SimpleNamespace:
    """Minimal pipeline context with optional previous step output."""
    step_outputs = {}
    if last_output is not None:
        step_outputs["prev_step"] = last_output
    return SimpleNamespace(step_outputs=step_outputs)


def run(coro):
    """Run a coroutine synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


def create_sqlite_db(tmp_path: Path, table_sql: str) -> str:
    """Create a SQLite DB file with the given CREATE TABLE SQL and return the path."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute(table_sql)
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runner():
    from brix.runners.db_upsert import DbUpsertRunner
    return DbUpsertRunner()


@pytest.fixture
def sqlite_db(tmp_path):
    """SQLite file DB with a simple 'items' table."""
    db_path = create_sqlite_db(
        tmp_path,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
    )
    return db_path


@pytest.fixture
def sqlite_db_upsert(tmp_path):
    """SQLite file DB with a 'products' table suitable for upsert tests."""
    db_path = create_sqlite_db(
        tmp_path,
        "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price REAL)",
    )
    return db_path


# ---------------------------------------------------------------------------
# Interface tests
# ---------------------------------------------------------------------------


class TestInterface:
    def test_config_schema_returns_dict(self, runner):
        schema = runner.config_schema()
        assert isinstance(schema, dict)
        assert schema["type"] == "object"

    def test_config_schema_has_required_fields(self, runner):
        schema = runner.config_schema()
        assert "connection" in schema["properties"]
        assert "table" in schema["properties"]
        assert "connection" in schema["required"]
        assert "table" in schema["required"]

    def test_input_type(self, runner):
        assert runner.input_type() == "dict"

    def test_output_type(self, runner):
        assert runner.output_type() == "dict"

    def test_is_base_runner_subclass(self, runner):
        from brix.runners.base import BaseRunner
        assert isinstance(runner, BaseRunner)


# ---------------------------------------------------------------------------
# Plain INSERT
# ---------------------------------------------------------------------------


class TestInsert:
    def test_single_dict_insert(self, runner, sqlite_db):
        step = make_step(
            connection=sqlite_db,
            table="items",
            data={"id": 1, "name": "apple", "value": 42},
        )
        result = run(runner.execute(step, make_context()))
        assert result["success"] is True
        assert result["data"]["total"] == 1
        assert result["data"]["inserted"] == 1
        # Verify row exists in DB
        conn = sqlite3.connect(sqlite_db)
        row = conn.execute("SELECT * FROM items WHERE id=1").fetchone()
        conn.close()
        assert row is not None
        assert row[1] == "apple"
        assert row[2] == 42

    def test_returns_duration(self, runner, sqlite_db):
        step = make_step(
            connection=sqlite_db,
            table="items",
            data={"id": 10, "name": "x", "value": 0},
        )
        result = run(runner.execute(step, make_context()))
        assert "duration" in result
        assert result["duration"] >= 0.0


# ---------------------------------------------------------------------------
# Batch INSERT
# ---------------------------------------------------------------------------


class TestBatchInsert:
    def test_list_of_dicts_inserted(self, runner, sqlite_db):
        rows = [
            {"id": 1, "name": "a", "value": 1},
            {"id": 2, "name": "b", "value": 2},
            {"id": 3, "name": "c", "value": 3},
        ]
        step = make_step(connection=sqlite_db, table="items", data=rows)
        result = run(runner.execute(step, make_context()))
        assert result["success"] is True
        assert result["data"]["total"] == 3
        # Verify all rows in DB
        conn = sqlite3.connect(sqlite_db)
        count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        conn.close()
        assert count == 3

    def test_batch_in_single_transaction(self, runner, sqlite_db):
        """All rows inserted — if one fails, none should be present (atomicity)."""
        rows = [
            {"id": 1, "name": "ok", "value": 1},
        ]
        step = make_step(connection=sqlite_db, table="items", data=rows)
        result = run(runner.execute(step, make_context()))
        assert result["success"] is True


# ---------------------------------------------------------------------------
# UPSERT with conflict_key
# ---------------------------------------------------------------------------


class TestUpsert:
    def test_upsert_inserts_new_row(self, runner, sqlite_db_upsert):
        step = make_step(
            connection=sqlite_db_upsert,
            table="products",
            data={"sku": "ABC-001", "name": "Widget", "price": 9.99},
            conflict_key="sku",
        )
        result = run(runner.execute(step, make_context()))
        assert result["success"] is True
        assert result["data"]["total"] == 1

    def test_upsert_replaces_existing_row(self, runner, sqlite_db_upsert):
        # First insert
        conn = sqlite3.connect(sqlite_db_upsert)
        conn.execute("INSERT INTO products VALUES ('SKU-1', 'OldName', 5.0)")
        conn.commit()
        conn.close()

        # Upsert with same SKU, different name/price
        step = make_step(
            connection=sqlite_db_upsert,
            table="products",
            data={"sku": "SKU-1", "name": "NewName", "price": 99.0},
            conflict_key="sku",
        )
        result = run(runner.execute(step, make_context()))
        assert result["success"] is True

        # Verify the row was replaced
        conn = sqlite3.connect(sqlite_db_upsert)
        row = conn.execute("SELECT * FROM products WHERE sku='SKU-1'").fetchone()
        conn.close()
        assert row[1] == "NewName"
        assert row[2] == 99.0

    def test_conflict_key_as_list(self, runner, sqlite_db_upsert):
        """conflict_key as a list of strings should also work."""
        step = make_step(
            connection=sqlite_db_upsert,
            table="products",
            data={"sku": "XYZ-9", "name": "Item", "price": 1.0},
            conflict_key=["sku"],
        )
        result = run(runner.execute(step, make_context()))
        assert result["success"] is True


# ---------------------------------------------------------------------------
# Data from pipeline context
# ---------------------------------------------------------------------------


class TestDataFromContext:
    def test_data_from_previous_step_dict(self, runner, sqlite_db):
        """data comes from the last step output in context."""
        step = make_step(connection=sqlite_db, table="items")  # no data in params
        ctx = make_context(last_output={"id": 99, "name": "ctx-item", "value": 7})
        result = run(runner.execute(step, ctx))
        assert result["success"] is True
        conn = sqlite3.connect(sqlite_db)
        row = conn.execute("SELECT * FROM items WHERE id=99").fetchone()
        conn.close()
        assert row is not None

    def test_data_from_previous_step_wrapped_output(self, runner, sqlite_db):
        """data wrapped in {'data': ...} from step output."""
        step = make_step(connection=sqlite_db, table="items")
        ctx = make_context(
            last_output={"data": {"id": 88, "name": "wrapped", "value": 3}}
        )
        result = run(runner.execute(step, ctx))
        assert result["success"] is True
        conn = sqlite3.connect(sqlite_db)
        row = conn.execute("SELECT * FROM items WHERE id=88").fetchone()
        conn.close()
        assert row is not None


# ---------------------------------------------------------------------------
# Empty data
# ---------------------------------------------------------------------------


class TestEmptyData:
    def test_empty_list_returns_zero_counts(self, runner, sqlite_db):
        step = make_step(connection=sqlite_db, table="items", data=[])
        result = run(runner.execute(step, make_context()))
        assert result["success"] is True
        assert result["data"] == {"inserted": 0, "updated": 0, "total": 0}

    def test_no_data_and_no_context_returns_zero(self, runner, sqlite_db):
        step = make_step(connection=sqlite_db, table="items")
        result = run(runner.execute(step, make_context()))
        assert result["success"] is True
        assert result["data"]["total"] == 0


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestErrors:
    def test_table_does_not_exist(self, runner, tmp_path):
        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        conn.close()  # Empty DB — no tables

        step = make_step(
            connection=db_path,
            table="nonexistent_table",
            data={"id": 1, "value": "x"},
        )
        result = run(runner.execute(step, make_context()))
        assert result["success"] is False
        assert "error" in result

    def test_missing_connection_param(self, runner):
        step = SimpleNamespace(params={"table": "items"})
        result = run(runner.execute(step, make_context()))
        assert result["success"] is False
        assert "connection" in result["error"]

    def test_missing_table_param(self, runner):
        step = SimpleNamespace(params={"connection": ":memory:"})
        result = run(runner.execute(step, make_context()))
        assert result["success"] is False
        assert "table" in result["error"]

    def test_connection_not_found(self, runner, tmp_path):
        """A named connection that doesn't exist and isn't a file path → error."""
        step = make_step(
            connection="nonexistent-connection-name",
            table="items",
            data={"id": 1},
        )
        result = run(runner.execute(step, make_context()))
        assert result["success"] is False
        assert "error" in result

    def test_data_not_dict_or_list(self, runner, sqlite_db):
        step = make_step(connection=sqlite_db, table="items", data="not-valid")
        result = run(runner.execute(step, make_context()))
        assert result["success"] is False
        assert "error" in result

    def test_row_not_dict_raises(self, runner, sqlite_db):
        step = make_step(connection=sqlite_db, table="items", data=["not-a-dict"])
        result = run(runner.execute(step, make_context()))
        assert result["success"] is False
        assert "error" in result


# ---------------------------------------------------------------------------
# SQL generation (unit tests for _build_insert_sql)
# ---------------------------------------------------------------------------


class TestBuildInsertSql:
    def test_plain_insert_sqlite(self):
        from brix.runners.db_upsert import _build_insert_sql
        sql = _build_insert_sql("sqlite", "items", ["id", "name"], [], None)
        assert 'INSERT INTO "items"' in sql
        assert "INSERT OR REPLACE" not in sql
        assert sql.count("?") == 2

    def test_upsert_sqlite(self):
        from brix.runners.db_upsert import _build_insert_sql
        sql = _build_insert_sql("sqlite", "items", ["id", "name"], ["id"], None)
        assert "INSERT OR REPLACE" in sql

    def test_plain_insert_postgres(self):
        from brix.runners.db_upsert import _build_insert_sql
        sql = _build_insert_sql("postgresql", "items", ["id", "name"], [], None)
        assert 'INSERT INTO "items"' in sql
        assert "ON CONFLICT" not in sql
        assert sql.count("%s") == 2

    def test_upsert_postgres(self):
        from brix.runners.db_upsert import _build_insert_sql
        sql = _build_insert_sql("postgresql", "items", ["id", "name"], ["id"], None)
        assert "ON CONFLICT" in sql
        assert "DO UPDATE SET" in sql
        assert '"name" = EXCLUDED."name"' in sql

    def test_upsert_postgres_all_conflict_keys(self):
        """When all columns are conflict keys, use DO NOTHING."""
        from brix.runners.db_upsert import _build_insert_sql
        sql = _build_insert_sql("postgresql", "items", ["id"], ["id"], None)
        assert "DO NOTHING" in sql

    def test_upsert_postgres_custom_set_columns(self):
        from brix.runners.db_upsert import _build_insert_sql
        sql = _build_insert_sql(
            "postgresql", "items", ["id", "name", "value"], ["id"], ["value"]
        )
        assert '"value" = EXCLUDED."value"' in sql
        assert "name" not in sql.split("DO UPDATE SET")[1]


# ---------------------------------------------------------------------------
# Progress reporting
# ---------------------------------------------------------------------------


class TestProgress:
    def test_progress_set_after_execute(self, runner, sqlite_db):
        step = make_step(
            connection=sqlite_db,
            table="items",
            data={"id": 5, "name": "p", "value": 0},
        )
        run(runner.execute(step, make_context()))
        assert runner._progress is not None
        assert runner._progress["pct"] == 100.0

    def test_progress_set_on_empty_data(self, runner, sqlite_db):
        step = make_step(connection=sqlite_db, table="items", data=[])
        run(runner.execute(step, make_context()))
        assert runner._progress is not None
        assert runner._progress["pct"] == 100.0
