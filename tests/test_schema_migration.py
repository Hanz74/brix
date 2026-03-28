"""Tests for the DB schema migration system (T-BRIX-DB-27).

Covers:
- ensure_migrations_table: creates table, idempotent
- get_current_version: returns 0 on fresh DB, correct version after apply
- run_pending_migrations: empty list when no migrations, applies migrations in order
- rollback_migration: rolls back applied migration, removes tracking row
- get_migration_status: returns correct summary dict
- Integration with BrixDB._init_schema (migrations run automatically)
- MCP handler _handle_db_status
"""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.migrations import (
    MIGRATIONS,
    ensure_migrations_table,
    get_current_version,
    get_migration_status,
    rollback_migration,
    run_pending_migrations,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_migration(version: int, up: str = "", down: str = "") -> dict:
    return {"version": version, "name": f"test_v{version}", "up": up, "down": down}


@pytest.fixture
def db(tmp_path):
    """Fresh BrixDB backed by a temp file."""
    return BrixDB(db_path=tmp_path / "test.db")


# ---------------------------------------------------------------------------
# ensure_migrations_table
# ---------------------------------------------------------------------------

class TestEnsureMigrationsTable:
    def test_creates_table(self, db):
        ensure_migrations_table(db)
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "schema_migrations" in tables

    def test_idempotent(self, db):
        """Calling twice should not raise."""
        ensure_migrations_table(db)
        ensure_migrations_table(db)  # must not raise


# ---------------------------------------------------------------------------
# get_current_version
# ---------------------------------------------------------------------------

class TestGetCurrentVersion:
    def test_returns_baseline_on_fresh_db(self, db):
        # BrixDB.__init__ runs real migrations v1 + v2 automatically
        assert get_current_version(db) == len(MIGRATIONS)

    def test_returns_max_applied_version(self, db, monkeypatch):
        ensure_migrations_table(db)
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [
                _make_migration(100),
                _make_migration(101),
            ],
        )
        run_pending_migrations(db)
        assert get_current_version(db) == 101


# ---------------------------------------------------------------------------
# run_pending_migrations
# ---------------------------------------------------------------------------

class TestRunPendingMigrations:
    def test_no_extra_migrations_when_current(self, db, monkeypatch):
        """No additional migrations applied when only real migrations exist."""
        applied = run_pending_migrations(db)
        assert applied == []

    def test_applies_new_migration(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(100)],
        )
        applied = run_pending_migrations(db)
        assert len(applied) == 1
        assert applied[0]["version"] == 100
        assert "applied_at" in applied[0]

    def test_applies_migrations_in_order(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(103), _make_migration(101), _make_migration(102)],
        )
        applied = run_pending_migrations(db)
        versions = [m["version"] for m in applied]
        assert versions == [101, 102, 103]

    def test_skips_already_applied(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(100), _make_migration(101)],
        )
        run_pending_migrations(db)
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(100), _make_migration(101), _make_migration(102)],
        )
        applied = run_pending_migrations(db)
        assert len(applied) == 1
        assert applied[0]["version"] == 102

    def test_idempotent_on_second_call(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(100)],
        )
        run_pending_migrations(db)
        applied = run_pending_migrations(db)
        assert applied == []

    def test_executes_up_sql(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [
                _make_migration(
                    100,
                    up="CREATE TABLE IF NOT EXISTS _migration_test (id INTEGER PRIMARY KEY)",
                )
            ],
        )
        run_pending_migrations(db)
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "_migration_test" in tables

    def test_raises_on_bad_sql(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(100, up="THIS IS NOT VALID SQL !!!")],
        )
        with pytest.raises(RuntimeError, match="Migration v100"):
            run_pending_migrations(db)


# ---------------------------------------------------------------------------
# rollback_migration
# ---------------------------------------------------------------------------

class TestRollbackMigration:
    def test_rolls_back_applied_migration(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(100)],
        )
        run_pending_migrations(db)
        result = rollback_migration(db, 100)
        assert result is True
        assert get_current_version(db) == len(MIGRATIONS)

    def test_returns_false_for_unapplied(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(100)],
        )
        # v100 not applied yet (only real migrations ran)
        result = rollback_migration(db, 100)
        assert result is False

    def test_returns_false_for_unknown_version(self, db, monkeypatch):
        monkeypatch.setattr("brix.migrations.MIGRATIONS", MIGRATIONS)
        result = rollback_migration(db, 99)
        assert result is False

    def test_executes_down_sql(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [
                _make_migration(
                    100,
                    up="CREATE TABLE IF NOT EXISTS _rb_test (id INTEGER PRIMARY KEY)",
                    down="DROP TABLE IF EXISTS _rb_test",
                )
            ],
        )
        run_pending_migrations(db)
        rollback_migration(db, 100)
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "_rb_test" not in tables


# ---------------------------------------------------------------------------
# get_migration_status
# ---------------------------------------------------------------------------

class TestGetMigrationStatus:
    def test_fresh_db_status(self, db, monkeypatch):
        # Real migrations v1+v2 already applied during init
        status = get_migration_status(db)
        assert status["current_version"] == len(MIGRATIONS)
        assert len(status["applied"]) == len(MIGRATIONS)
        assert status["pending"] == []
        assert "db_size_bytes" in status
        assert "db_path" in status

    def test_pending_listed_before_apply(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            MIGRATIONS + [_make_migration(100), _make_migration(101)],
        )
        status = get_migration_status(db)
        assert len(status["pending"]) == 2
        assert status["pending"][0]["version"] == 100

    def test_applied_and_pending_after_partial_apply(self, db, monkeypatch):
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            [_make_migration(1), _make_migration(2)],
        )
        run_pending_migrations(db)
        monkeypatch.setattr(
            "brix.migrations.MIGRATIONS",
            [_make_migration(1), _make_migration(2), _make_migration(3)],
        )
        status = get_migration_status(db)
        assert status["current_version"] == 2
        assert len(status["applied"]) == 2
        assert len(status["pending"]) == 1
        assert status["pending"][0]["version"] == 3

    def test_db_size_is_int(self, db, monkeypatch):
        monkeypatch.setattr("brix.migrations.MIGRATIONS", [])
        status = get_migration_status(db)
        assert isinstance(status["db_size_bytes"], int)


# ---------------------------------------------------------------------------
# Integration: BrixDB._init_schema calls run_pending_migrations
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_schema_migrations_table_created_on_init(self, tmp_path):
        """BrixDB.__init__ should create schema_migrations table."""
        db = BrixDB(db_path=tmp_path / "integration.db")
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "schema_migrations" in tables

    def test_no_pending_migrations_on_clean_start(self, tmp_path):
        """With current MIGRATIONS list, no pending after init (all applied automatically)."""
        db = BrixDB(db_path=tmp_path / "clean.db")
        status = get_migration_status(db)
        assert len(status["pending"]) == 0


# ---------------------------------------------------------------------------
# MCP handler _handle_db_status
# ---------------------------------------------------------------------------

class TestHandleDbStatus:
    @pytest.mark.asyncio
    async def test_returns_success(self, tmp_path, monkeypatch):
        from brix.mcp_handlers.insights import _handle_db_status
        monkeypatch.setattr("brix.migrations.MIGRATIONS", [])

        result = await _handle_db_status({})
        assert result["success"] is True
        assert "current_version" in result
        assert "applied" in result
        assert "pending" in result
        assert "db_size_bytes" in result
        assert "db_path" in result

    @pytest.mark.asyncio
    async def test_no_pending_in_normal_state(self, tmp_path, monkeypatch):
        from brix.mcp_handlers.insights import _handle_db_status
        monkeypatch.setattr("brix.migrations.MIGRATIONS", [])

        result = await _handle_db_status({})
        assert result["pending"] == []
