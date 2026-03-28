"""Tests for Managed Variables and Persistent Data Store (T-BRIX-DB-13).

Covers:
- Variable CRUD (variable_set, variable_get, variable_list, variable_delete)
- {{ var.name }} in Jinja2 templates via PipelineContext
- Persistent Store CRUD (store_set, store_get, store_list, store_delete)
- {{ store.key }} in Jinja2 templates via PipelineContext
- flow.set with persist: true writes to persistent_store
- Variable not found → empty string (not error)
- MCP handlers for variables and store
"""
import asyncio
from pathlib import Path

import pytest

from brix.db import BrixDB
from brix.context import PipelineContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def ctx(tmp_path):
    """PipelineContext backed by a test DB."""
    return PipelineContext(
        pipeline_input={"x": "hello"},
        workdir=tmp_path / "run",
    )


# ===========================================================================
# Part 1: Managed Variables — DB Layer
# ===========================================================================

class TestVariablesSchema:
    def test_table_exists(self, db):
        with db._connect() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert "variables" in tables

    def test_columns(self, db):
        with db._connect() as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(variables)").fetchall()}
        assert {"name", "value", "description", "created_at", "updated_at"} <= cols


class TestVariableCRUD:
    def test_set_and_get(self, db):
        db.variable_set("api_key", "secret123")
        assert db.variable_get("api_key") == "secret123"

    def test_set_with_description(self, db):
        db.variable_set("base_url", "https://example.com", description="API base URL")
        rows = db.variable_list()
        row = next(r for r in rows if r["name"] == "base_url")
        assert row["description"] == "API base URL"
        assert row["value"] == "https://example.com"

    def test_set_updates_existing(self, db):
        db.variable_set("counter", "1")
        db.variable_set("counter", "42")
        assert db.variable_get("counter") == "42"

    def test_get_missing_returns_none(self, db):
        assert db.variable_get("nonexistent") is None

    def test_list_returns_all(self, db):
        db.variable_set("a", "1")
        db.variable_set("b", "2")
        db.variable_set("c", "3")
        names = {r["name"] for r in db.variable_list()}
        assert {"a", "b", "c"} <= names

    def test_list_empty(self, db):
        assert db.variable_list() == []

    def test_delete_existing(self, db):
        db.variable_set("to_delete", "val")
        assert db.variable_delete("to_delete") is True
        assert db.variable_get("to_delete") is None

    def test_delete_nonexistent(self, db):
        assert db.variable_delete("ghost") is False

    def test_timestamps_set(self, db):
        db.variable_set("ts_test", "value")
        rows = db.variable_list()
        row = next(r for r in rows if r["name"] == "ts_test")
        assert row["created_at"] is not None
        assert row["updated_at"] is not None

    def test_update_changes_updated_at(self, db):
        db.variable_set("upd", "v1")
        rows1 = db.variable_list()
        row1 = next(r for r in rows1 if r["name"] == "upd")
        import time
        time.sleep(0.01)
        db.variable_set("upd", "v2")
        rows2 = db.variable_list()
        row2 = next(r for r in rows2 if r["name"] == "upd")
        assert row2["updated_at"] >= row1["updated_at"]


# ===========================================================================
# Part 2: Variables in Jinja2 Context
# ===========================================================================

class TestVariablesJinja2:
    def test_var_namespace_available(self, tmp_path):
        db = BrixDB(db_path=tmp_path / "brix_jinja.db")
        # Context loads from DB — monkeypatch db path via env or use real file
        # We test that var dict is loaded via to_jinja_context
        # Since context uses BrixDB() (default path), we test via the dict directly
        ctx = PipelineContext(workdir=tmp_path / "run")
        jctx = ctx.to_jinja_context()
        assert "var" in jctx
        assert isinstance(jctx["var"], dict)

    def test_store_namespace_available(self, tmp_path):
        ctx = PipelineContext(workdir=tmp_path / "run")
        jctx = ctx.to_jinja_context()
        assert "store" in jctx
        assert isinstance(jctx["store"], dict)

    def test_var_missing_returns_empty_dict_key(self, tmp_path):
        """Missing variable key in var dict → KeyError caught by Jinja2 as empty."""
        ctx = PipelineContext(workdir=tmp_path / "run")
        jctx = ctx.to_jinja_context()
        # var is a dict — missing key raises KeyError (Jinja2 returns '' for undefined)
        assert jctx["var"].get("nonexistent_var", "") == ""

    def test_var_dict_contains_db_variable(self, tmp_path, monkeypatch):
        """Variable written to default DB path should appear in Jinja2 context."""
        # Use a temp db and monkeypatch BrixDB default path
        import brix.context as ctx_module
        import brix.db as db_module

        test_db_path = tmp_path / "brix_ctx_test.db"

        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        # Set a variable in the patched DB
        db = BrixDB()
        db.variable_set("my_var", "hello_world")

        # Create context — should load variables from patched DB
        ctx = PipelineContext(workdir=tmp_path / "run")
        jctx = ctx.to_jinja_context()
        assert jctx["var"].get("my_var") == "hello_world"

    def test_store_dict_contains_db_entry(self, tmp_path, monkeypatch):
        """Store entry written to default DB path should appear in Jinja2 context."""
        test_db_path = tmp_path / "brix_store_test.db"

        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        db = BrixDB()
        db.store_set("last_run_id", "run-abc123")

        ctx = PipelineContext(workdir=tmp_path / "run")
        jctx = ctx.to_jinja_context()
        assert jctx["store"].get("last_run_id") == "run-abc123"


# ===========================================================================
# Part 3: Persistent Store — DB Layer
# ===========================================================================

class TestPersistentStoreSchema:
    def test_table_exists(self, db):
        with db._connect() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert "persistent_store" in tables

    def test_columns(self, db):
        with db._connect() as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(persistent_store)").fetchall()}
        assert {"key", "value", "pipeline_name", "updated_at"} <= cols


class TestPersistentStoreCRUD:
    def test_set_and_get(self, db):
        db.store_set("last_page", "5")
        assert db.store_get("last_page") == "5"

    def test_set_with_pipeline_name(self, db):
        db.store_set("cursor", "token123", pipeline_name="my-pipeline")
        rows = db.store_list()
        row = next(r for r in rows if r["key"] == "cursor")
        assert row["pipeline_name"] == "my-pipeline"

    def test_set_updates_existing(self, db):
        db.store_set("page", "1")
        db.store_set("page", "2")
        assert db.store_get("page") == "2"

    def test_get_missing_returns_none(self, db):
        assert db.store_get("nonexistent") is None

    def test_list_all(self, db):
        db.store_set("k1", "v1", "pipeline-a")
        db.store_set("k2", "v2", "pipeline-b")
        keys = {r["key"] for r in db.store_list()}
        assert {"k1", "k2"} <= keys

    def test_list_filter_by_pipeline(self, db):
        db.store_set("k1", "v1", "pipe-a")
        db.store_set("k2", "v2", "pipe-b")
        db.store_set("k3", "v3", "pipe-a")
        rows = db.store_list(pipeline_name="pipe-a")
        keys = {r["key"] for r in rows}
        assert keys == {"k1", "k3"}

    def test_list_empty(self, db):
        assert db.store_list() == []

    def test_delete_existing(self, db):
        db.store_set("del_key", "val")
        assert db.store_delete("del_key") is True
        assert db.store_get("del_key") is None

    def test_delete_nonexistent(self, db):
        assert db.store_delete("ghost") is False

    def test_upsert_updates_value(self, db):
        db.store_set("upsert_key", "original", "pipe1")
        db.store_set("upsert_key", "updated", "pipe2")
        assert db.store_get("upsert_key") == "updated"
        rows = db.store_list()
        row = next(r for r in rows if r["key"] == "upsert_key")
        assert row["pipeline_name"] == "pipe2"


# ===========================================================================
# Part 4: flow.set with persist: true
# ===========================================================================

class TestSetRunnerPersist:
    def test_persist_true_writes_to_store(self, tmp_path, monkeypatch):
        """SetRunner with persist=True should write values to persistent_store."""
        test_db_path = tmp_path / "brix_set_test.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.runners.set import SetRunner

        class FakeStep:
            values = {"result_count": "42", "last_status": "ok"}
            persist = True

        runner = SetRunner()
        ctx = PipelineContext(workdir=tmp_path / "run")

        result = asyncio.get_event_loop().run_until_complete(runner.execute(FakeStep(), ctx))
        assert result["success"] is True

        db = BrixDB()
        assert db.store_get("result_count") == "42"
        assert db.store_get("last_status") == "ok"

    def test_persist_false_does_not_write(self, tmp_path, monkeypatch):
        """SetRunner with persist=False should NOT write to persistent_store."""
        test_db_path = tmp_path / "brix_nopersist.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.runners.set import SetRunner

        class FakeStep:
            values = {"temp_val": "123"}
            persist = False

        runner = SetRunner()
        ctx = PipelineContext(workdir=tmp_path / "run")
        result = asyncio.get_event_loop().run_until_complete(runner.execute(FakeStep(), ctx))
        assert result["success"] is True

        db = BrixDB()
        assert db.store_get("temp_val") is None

    def test_persist_default_is_false(self, tmp_path, monkeypatch):
        """SetRunner without persist field defaults to False — no DB writes."""
        test_db_path = tmp_path / "brix_default.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.runners.set import SetRunner

        class FakeStep:
            values = {"key": "val"}
            # No 'persist' attr — should default to False

        runner = SetRunner()
        ctx = PipelineContext(workdir=tmp_path / "run")
        result = asyncio.get_event_loop().run_until_complete(runner.execute(FakeStep(), ctx))
        assert result["success"] is True

        db = BrixDB()
        assert db.store_get("key") is None


# ===========================================================================
# Part 5: MCP Handlers
# ===========================================================================

class TestVariableMCPHandlers:
    def test_set_variable(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_mcp.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_set_variable
        result = asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "test_var", "value": "test_value"})
        )
        assert result["set"] is True
        assert result["name"] == "test_var"

    def test_get_variable_found(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_mcp2.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_set_variable, _handle_get_variable
        asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "found_var", "value": "found_value"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_get_variable({"name": "found_var"})
        )
        assert result["found"] is True
        assert result["value"] == "found_value"

    def test_get_variable_not_found(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_mcp3.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_get_variable
        result = asyncio.get_event_loop().run_until_complete(
            _handle_get_variable({"name": "missing"})
        )
        assert result["found"] is False
        assert result["value"] == ""

    def test_list_variables(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_mcp4.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_set_variable, _handle_list_variables
        asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "v1", "value": "x"})
        )
        asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "v2", "value": "y"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_list_variables({})
        )
        assert result["count"] == 2
        names = {v["name"] for v in result["variables"]}
        assert names == {"v1", "v2"}

    def test_delete_variable(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_mcp5.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_set_variable, _handle_delete_variable
        asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "del_me", "value": "bye"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_delete_variable({"name": "del_me"})
        )
        assert result["deleted"] is True

    def test_set_variable_missing_name(self):
        from brix.mcp_handlers.variables import _handle_set_variable
        result = asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"value": "x"})
        )
        assert "error" in result

    def test_set_variable_missing_value(self):
        from brix.mcp_handlers.variables import _handle_set_variable
        result = asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "x"})
        )
        assert "error" in result


class TestStoreMCPHandlers:
    def test_store_set(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_store_mcp.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_store_set
        result = asyncio.get_event_loop().run_until_complete(
            _handle_store_set({"key": "my_key", "value": "my_val"})
        )
        assert result["set"] is True
        assert result["key"] == "my_key"

    def test_store_get_found(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_store_mcp2.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_store_set, _handle_store_get
        asyncio.get_event_loop().run_until_complete(
            _handle_store_set({"key": "k", "value": "v"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_store_get({"key": "k"})
        )
        assert result["found"] is True
        assert result["value"] == "v"

    def test_store_get_not_found(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_store_mcp3.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_store_get
        result = asyncio.get_event_loop().run_until_complete(
            _handle_store_get({"key": "missing"})
        )
        assert result["found"] is False
        assert result["value"] == ""

    def test_store_list(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_store_mcp4.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_store_set, _handle_store_list
        asyncio.get_event_loop().run_until_complete(_handle_store_set({"key": "k1", "value": "v1"}))
        asyncio.get_event_loop().run_until_complete(_handle_store_set({"key": "k2", "value": "v2"}))
        result = asyncio.get_event_loop().run_until_complete(_handle_store_list({}))
        assert result["count"] == 2

    def test_store_delete(self, tmp_path, monkeypatch):
        test_db_path = tmp_path / "brix_store_mcp5.db"
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

        from brix.mcp_handlers.variables import _handle_store_set, _handle_store_delete
        asyncio.get_event_loop().run_until_complete(_handle_store_set({"key": "bye", "value": "val"}))
        result = asyncio.get_event_loop().run_until_complete(_handle_store_delete({"key": "bye"}))
        assert result["deleted"] is True
