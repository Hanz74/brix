"""Tests for ConnectionManager — Named DB-Connections (T-BRIX-DB-05b).

Covers:
- Connection registrieren + abrufen
- DSN wird verschlüsselt (Credential Store)
- ENV-Fallback funktioniert
- Connection testen (Mock)
- list zeigt keine DSNs
- delete entfernt Connection + Credential
- MCP-Tools: connection_add, connection_list, connection_test, connection_delete
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def set_master_key(monkeypatch):
    """Set a deterministic master key for all tests."""
    monkeypatch.setenv("BRIX_MASTER_KEY", "b" * 64)


@pytest.fixture
def db(tmp_path):
    """BrixDB backed by a temp file."""
    from brix.db import BrixDB
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def cred_store(tmp_path):
    """CredentialStore backed by a temp file."""
    from brix.credential_store import CredentialStore
    return CredentialStore(db_path=tmp_path / "credentials.db")


@pytest.fixture
def manager(db, cred_store):
    """ConnectionManager with temp DB and CredentialStore."""
    from brix.connections import ConnectionManager
    mgr = ConnectionManager(db)
    mgr._cred_store = cred_store
    return mgr


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TestSchema:
    def test_connections_table_exists(self, db):
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "connections" in tables

    def test_connections_table_columns(self, db):
        with db._connect() as conn:
            cols = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(connections)"
                ).fetchall()
            }
        expected = {"id", "name", "driver", "dsn_credential_id", "env_var", "description", "created_at", "updated_at"}
        assert expected.issubset(cols)


# ---------------------------------------------------------------------------
# register / get
# ---------------------------------------------------------------------------

class TestRegister:
    def test_register_returns_metadata(self, manager):
        meta = manager.register("prod-db", "postgresql://localhost/mydb", driver="postgresql")
        assert meta["name"] == "prod-db"
        assert meta["driver"] == "postgresql"
        assert "id" in meta
        assert "created_at" in meta
        # DSN must NOT be in metadata
        assert "dsn" not in meta

    def test_register_stores_dsn_encrypted(self, manager, cred_store):
        meta = manager.register("enc-test", "postgresql://user:secret@host/db")
        cred_id = meta["dsn_credential_id"]
        assert cred_id is not None
        # Verify credential exists in store
        cred_meta = cred_store.get(cred_id)
        assert cred_meta["id"] == cred_id
        # Verify DSN is correctly encrypted (resolve returns original)
        resolved = cred_store.resolve(cred_id)
        assert resolved == "postgresql://user:secret@host/db"

    def test_register_dsn_not_in_plaintext_in_db(self, db, manager):
        """DSN must never appear as plaintext in brix.db."""
        manager.register("secret-conn", "postgresql://user:supersecret@host/db")
        # Read raw DB content and check DSN is not present
        with db._connect() as conn:
            rows = conn.execute("SELECT * FROM connections").fetchall()
        assert len(rows) == 1
        # Convert row to string and ensure DSN is not there
        row_str = str(rows[0])
        assert "supersecret" not in row_str

    def test_register_duplicate_name_raises(self, manager):
        import sqlite3
        manager.register("dup-conn", "postgresql://localhost/db1")
        with pytest.raises(sqlite3.IntegrityError):
            manager.register("dup-conn", "postgresql://localhost/db2")

    def test_register_empty_name_raises(self, manager):
        with pytest.raises(ValueError, match="name"):
            manager.register("", "postgresql://localhost/db")

    def test_register_unsupported_driver_raises(self, manager):
        with pytest.raises(ValueError, match="Unsupported driver"):
            manager.register("bad-driver", "mssql://localhost/db", driver="mssql")

    def test_register_empty_dsn_raises(self, manager):
        with pytest.raises(ValueError, match="DSN"):
            manager.register("empty-dsn", "")


class TestGet:
    def test_get_returns_connection_object(self, manager):
        from brix.connections import Connection
        manager.register("my-conn", "postgresql://localhost/mydb")
        conn = manager.get("my-conn")
        assert isinstance(conn, Connection)
        assert conn.name == "my-conn"
        assert conn.driver == "postgresql"
        assert conn.dsn == "postgresql://localhost/mydb"

    def test_get_unknown_raises(self, manager):
        from brix.connections import ConnectionNotFoundError
        with pytest.raises(ConnectionNotFoundError):
            manager.get("nonexistent")

    def test_get_decrypts_dsn(self, manager):
        dsn = "postgresql://admin:password123@prod-host:5432/mydb"
        manager.register("decrypt-test", dsn)
        conn = manager.get("decrypt-test")
        assert conn.dsn == dsn


# ---------------------------------------------------------------------------
# ENV-Variable fallback
# ---------------------------------------------------------------------------

class TestEnvFallback:
    def test_env_fallback_when_credential_missing(self, db, monkeypatch):
        """Connection resolves via ENV var if credential is gone."""
        from brix.connections import ConnectionManager, Connection
        from brix.credential_store import CredentialStore, CredentialNotFoundError

        # Register with a working credential
        cred_store_real = CredentialStore(db_path=db.db_path.parent / "creds.db")
        mgr = ConnectionManager(db)
        mgr._cred_store = cred_store_real

        mgr.register("env-test", "postgresql://original/db", env_var="TEST_DB_URL")

        # Now delete the credential to simulate it being gone
        with db._connect() as conn:
            row = conn.execute("SELECT dsn_credential_id FROM connections WHERE name = 'env-test'").fetchone()
        cred_store_real.delete(row[0])

        # Set ENV var
        monkeypatch.setenv("TEST_DB_URL", "postgresql://env-host/envdb")

        conn = mgr.get("env-test")
        assert conn.dsn == "postgresql://env-host/envdb"
        assert conn.env_var == "TEST_DB_URL"

    def test_env_var_stored_in_db(self, manager):
        meta = manager.register("with-env", "postgresql://localhost/db", env_var="MY_DB")
        assert meta["env_var"] == "MY_DB"

    def test_no_dsn_no_env_raises(self, db, monkeypatch):
        """If credential is deleted and ENV not set, raise ConnectionNotFoundError."""
        from brix.connections import ConnectionManager, ConnectionNotFoundError
        from brix.credential_store import CredentialStore

        cred_store_real = CredentialStore(db_path=db.db_path.parent / "creds.db")
        mgr = ConnectionManager(db)
        mgr._cred_store = cred_store_real

        mgr.register("orphan", "postgresql://orig/db", env_var="ORPHAN_DB_URL")

        # Delete credential
        with db._connect() as conn:
            row = conn.execute("SELECT dsn_credential_id FROM connections WHERE name = 'orphan'").fetchone()
        cred_store_real.delete(row[0])

        # Ensure ENV not set
        monkeypatch.delenv("ORPHAN_DB_URL", raising=False)

        with pytest.raises(ConnectionNotFoundError):
            mgr.get("orphan")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

class TestList:
    def test_list_returns_all_connections(self, manager):
        manager.register("conn-a", "postgresql://host/a")
        manager.register("conn-b", "sqlite:///tmp/b.db", driver="sqlite")
        items = manager.list()
        names = [i["name"] for i in items]
        assert "conn-a" in names
        assert "conn-b" in names

    def test_list_does_not_expose_dsn(self, manager):
        manager.register("no-dsn-test", "postgresql://secret:pass@host/db")
        items = manager.list()
        for item in items:
            assert "dsn" not in item
            assert "secret" not in str(item)
            assert "pass" not in str(item)

    def test_list_sorted_by_name(self, manager):
        manager.register("z-conn", "postgresql://host/z")
        manager.register("a-conn", "postgresql://host/a")
        manager.register("m-conn", "postgresql://host/m")
        items = manager.list()
        names = [i["name"] for i in items]
        assert names == sorted(names)

    def test_list_empty_returns_empty(self, manager):
        assert manager.list() == []


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------

class TestDelete:
    def test_delete_removes_connection(self, manager):
        manager.register("to-delete", "postgresql://host/db")
        assert len(manager.list()) == 1
        result = manager.delete("to-delete")
        assert result is True
        assert len(manager.list()) == 0

    def test_delete_also_removes_credential(self, manager, cred_store):
        from brix.credential_store import CredentialNotFoundError
        meta = manager.register("del-with-cred", "postgresql://host/db")
        cred_id = meta["dsn_credential_id"]
        # Credential exists
        cred_store.get(cred_id)

        manager.delete("del-with-cred")
        # Credential should be gone
        with pytest.raises(CredentialNotFoundError):
            cred_store.get(cred_id)

    def test_delete_nonexistent_returns_false(self, manager):
        result = manager.delete("does-not-exist")
        assert result is False

    def test_delete_then_re_register(self, manager):
        manager.register("reuse-name", "postgresql://host/db1")
        manager.delete("reuse-name")
        meta = manager.register("reuse-name", "postgresql://host/db2")
        conn = manager.get("reuse-name")
        assert conn.dsn == "postgresql://host/db2"


# ---------------------------------------------------------------------------
# test (ping)
# ---------------------------------------------------------------------------

class TestConnectionTest:
    def test_test_sqlite_in_memory(self, manager):
        manager.register("sqlite-mem", ":memory:", driver="sqlite")
        result = manager.test("sqlite-mem")
        assert result["success"] is True
        assert "sqlite" in result.get("driver", "").lower() or "SQLite" in result.get("message", "")

    def test_test_sqlite_real_file(self, manager, tmp_path):
        db_file = str(tmp_path / "test.db")
        manager.register("sqlite-file", db_file, driver="sqlite")
        result = manager.test("sqlite-file")
        assert result["success"] is True

    def test_test_postgresql_fail_gracefully(self, manager):
        manager.register("bad-pg", "postgresql://user:pass@nonexistent-host:5432/db")
        result = manager.test("bad-pg")
        # Should not raise, should return success=False with error
        assert "success" in result
        if not result["success"]:
            assert "error" in result

    def test_test_nonexistent_connection(self, manager):
        result = manager.test("no-such-conn")
        assert result["success"] is False
        assert "error" in result


# ---------------------------------------------------------------------------
# MCP Tools
# ---------------------------------------------------------------------------

class TestMcpConnectionAdd:
    @pytest.mark.asyncio
    async def test_add_success(self, tmp_path, monkeypatch):
        from brix.db import BrixDB
        from brix.credential_store import CredentialStore
        from brix.mcp_handlers.connections import _handle_connection_add
        from brix.connections import ConnectionManager

        db = BrixDB(db_path=tmp_path / "brix.db")
        cred_store = CredentialStore(db_path=tmp_path / "creds.db")

        # Patch at the module where BrixDB is imported inside the handler function
        with patch("brix.db.BrixDB", return_value=db):
            with patch("brix.connections.CredentialStore", return_value=cred_store):
                result = await _handle_connection_add({
                    "name": "test-conn",
                    "dsn": "postgresql://localhost/testdb",
                    "driver": "postgresql",
                    "description": "Test connection",
                })
        assert result["success"] is True
        assert result["name"] == "test-conn"
        assert "dsn" not in result

    @pytest.mark.asyncio
    async def test_add_missing_name(self):
        from brix.mcp_handlers.connections import _handle_connection_add
        result = await _handle_connection_add({"dsn": "postgresql://localhost/db"})
        assert result["success"] is False
        assert "name" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_add_missing_dsn(self):
        from brix.mcp_handlers.connections import _handle_connection_add
        result = await _handle_connection_add({"name": "test"})
        assert result["success"] is False
        assert "dsn" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_add_invalid_driver(self):
        from brix.mcp_handlers.connections import _handle_connection_add
        result = await _handle_connection_add({
            "name": "test",
            "dsn": "mssql://host/db",
            "driver": "mssql",
        })
        assert result["success"] is False
        assert "driver" in result["error"].lower() or "Unsupported" in result["error"]


class TestMcpConnectionList:
    @pytest.mark.asyncio
    async def test_list_success(self, tmp_path):
        from brix.db import BrixDB
        from brix.credential_store import CredentialStore
        from brix.mcp_handlers.connections import _handle_connection_list
        from brix.connections import ConnectionManager

        db = BrixDB(db_path=tmp_path / "brix.db")
        cred_store = CredentialStore(db_path=tmp_path / "creds.db")
        mgr = ConnectionManager(db)
        mgr._cred_store = cred_store
        mgr.register("list-conn", "postgresql://host/db")

        with patch("brix.db.BrixDB", return_value=db):
            with patch("brix.connections.CredentialStore", return_value=cred_store):
                result = await _handle_connection_list({})
        assert result["success"] is True
        assert result["count"] == 1
        assert result["connections"][0]["name"] == "list-conn"
        # DSN must not appear
        for item in result["connections"]:
            assert "dsn" not in item


class TestMcpConnectionDelete:
    @pytest.mark.asyncio
    async def test_delete_success(self, tmp_path):
        from brix.db import BrixDB
        from brix.credential_store import CredentialStore
        from brix.mcp_handlers.connections import _handle_connection_delete
        from brix.connections import ConnectionManager

        db = BrixDB(db_path=tmp_path / "brix.db")
        cred_store = CredentialStore(db_path=tmp_path / "creds.db")
        mgr = ConnectionManager(db)
        mgr._cred_store = cred_store
        mgr.register("del-conn", "postgresql://host/db")

        with patch("brix.db.BrixDB", return_value=db):
            with patch("brix.connections.CredentialStore", return_value=cred_store):
                result = await _handle_connection_delete({"name": "del-conn"})
        assert result["success"] is True
        assert result["deleted"] == "del-conn"

    @pytest.mark.asyncio
    async def test_delete_not_found(self, tmp_path):
        from brix.db import BrixDB
        from brix.credential_store import CredentialStore
        from brix.mcp_handlers.connections import _handle_connection_delete

        db = BrixDB(db_path=tmp_path / "brix.db")
        cred_store = CredentialStore(db_path=tmp_path / "creds.db")

        with patch("brix.db.BrixDB", return_value=db):
            with patch("brix.connections.CredentialStore", return_value=cred_store):
                result = await _handle_connection_delete({"name": "ghost"})
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_delete_missing_name(self):
        from brix.mcp_handlers.connections import _handle_connection_delete
        result = await _handle_connection_delete({})
        assert result["success"] is False
        assert "name" in result["error"].lower()


class TestMcpConnectionTest:
    @pytest.mark.asyncio
    async def test_test_sqlite_success(self, tmp_path):
        from brix.db import BrixDB
        from brix.credential_store import CredentialStore
        from brix.mcp_handlers.connections import _handle_connection_test
        from brix.connections import ConnectionManager

        db = BrixDB(db_path=tmp_path / "brix.db")
        cred_store = CredentialStore(db_path=tmp_path / "creds.db")
        mgr = ConnectionManager(db)
        mgr._cred_store = cred_store
        mgr.register("sqlite-test", ":memory:", driver="sqlite")

        with patch("brix.db.BrixDB", return_value=db):
            with patch("brix.connections.CredentialStore", return_value=cred_store):
                result = await _handle_connection_test({"name": "sqlite-test"})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_test_missing_name(self):
        from brix.mcp_handlers.connections import _handle_connection_test
        result = await _handle_connection_test({})
        assert result["success"] is False
        assert "name" in result["error"].lower()
