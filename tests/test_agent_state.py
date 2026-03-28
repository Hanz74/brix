"""Tests for Agent State features (T-BRIX-V6-10 / V6-11 / V6-12).

Covers:
- V6-10: Agent-Kontext-Persistenz (save/restore agent_sessions)
- V6-11: Resource-Claims (claim/check/release resource_locks)
- V6-12: Blackboard shared KV-State (state_set/get/list/delete)

And corresponding MCP handlers.
"""
import asyncio
import json
import time
from pathlib import Path

import pytest

from brix.db import BrixDB


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "brix.db")


# ===========================================================================
# V6-10: Agent Sessions
# ===========================================================================

class TestAgentSessionsSchema:
    def test_table_exists(self, db):
        with db._connect() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert "agent_sessions" in tables


class TestSaveRestoreAgentContext:
    def test_save_and_restore_basic(self, db):
        db.save_agent_context("s1", "Working on intake pipeline")
        ctx = db.restore_agent_context("s1")
        assert ctx is not None
        assert ctx["session_id"] == "s1"
        assert ctx["summary"] == "Working on intake pipeline"
        assert ctx["active_pipeline"] is None
        assert ctx["last_run_id"] is None
        assert ctx["pending_decisions"] == []
        assert ctx["updated_at"] is not None

    def test_save_all_fields(self, db):
        db.save_agent_context(
            session_id="s2",
            summary="Processing emails",
            active_pipeline="buddy-intake",
            last_run_id="run-abc123",
            pending_decisions=["Should we move emails to Archive?", "Confirm IBAN extraction?"],
        )
        ctx = db.restore_agent_context("s2")
        assert ctx["active_pipeline"] == "buddy-intake"
        assert ctx["last_run_id"] == "run-abc123"
        assert len(ctx["pending_decisions"]) == 2
        assert "Should we move emails" in ctx["pending_decisions"][0]

    def test_restore_not_found(self, db):
        assert db.restore_agent_context("nonexistent") is None

    def test_upsert_updates_existing(self, db):
        db.save_agent_context("s3", "First summary")
        db.save_agent_context("s3", "Updated summary", active_pipeline="new-pipe")
        ctx = db.restore_agent_context("s3")
        assert ctx["summary"] == "Updated summary"
        assert ctx["active_pipeline"] == "new-pipe"

    def test_list_sessions(self, db):
        db.save_agent_context("alpha", "Session alpha")
        db.save_agent_context("beta", "Session beta")
        sessions = db.list_agent_sessions()
        names = [s["session_id"] for s in sessions]
        assert "alpha" in names
        assert "beta" in names

    def test_delete_session(self, db):
        db.save_agent_context("todel", "To delete")
        assert db.delete_agent_session("todel") is True
        assert db.restore_agent_context("todel") is None

    def test_delete_missing_session(self, db):
        assert db.delete_agent_session("ghost") is False

    def test_pending_decisions_empty_list(self, db):
        db.save_agent_context("s4", "Summary", pending_decisions=[])
        ctx = db.restore_agent_context("s4")
        assert ctx["pending_decisions"] == []

    def test_updated_at_is_set(self, db):
        db.save_agent_context("s5", "Summary")
        ctx = db.restore_agent_context("s5")
        assert ctx["updated_at"] is not None
        assert "T" in ctx["updated_at"]  # ISO format


# ===========================================================================
# V6-11: Resource Locks
# ===========================================================================

class TestResourceLocksSchema:
    def test_table_exists(self, db):
        with db._connect() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert "resource_locks" in tables


class TestClaimResource:
    def test_claim_success(self, db):
        result = db.claim_resource("res:file1", "run-001")
        assert result["claimed"] is True
        assert result["resource_id"] == "res:file1"
        assert result["run_id"] == "run-001"
        assert "expires_at" in result

    def test_claim_conflict(self, db):
        db.claim_resource("res:file2", "run-001")
        result = db.claim_resource("res:file2", "run-002")
        assert result["claimed"] is False
        assert result["held_by"] == "run-001"
        assert "expires_at" in result

    def test_claim_same_run_after_release(self, db):
        db.claim_resource("res:file3", "run-001")
        db.release_resource("res:file3")
        result = db.claim_resource("res:file3", "run-002")
        assert result["claimed"] is True

    def test_claim_expired_lock_auto_released(self, db):
        # Manually insert an expired lock
        import datetime
        past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)).isoformat()
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO resource_locks (resource_id, run_id, claimed_at, expires_at) VALUES (?,?,?,?)",
                ("res:expired", "old-run", past, past),
            )
        # Now a new claim should succeed (expired lock is auto-cleared)
        result = db.claim_resource("res:expired", "new-run")
        assert result["claimed"] is True

    def test_custom_ttl(self, db):
        result = db.claim_resource("res:ttl", "run-001", ttl_minutes=60)
        assert result["claimed"] is True
        assert "expires_at" in result


class TestCheckResource:
    def test_check_locked(self, db):
        db.claim_resource("res:check1", "run-001")
        status = db.check_resource("res:check1")
        assert status["locked"] is True
        assert status["run_id"] == "run-001"

    def test_check_free(self, db):
        status = db.check_resource("res:free")
        assert status["locked"] is False
        assert status["run_id"] is None
        assert status["expires_at"] is None

    def test_check_expired_lock_is_free(self, db):
        import datetime
        past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)).isoformat()
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO resource_locks (resource_id, run_id, claimed_at, expires_at) VALUES (?,?,?,?)",
                ("res:exp-check", "old-run", past, past),
            )
        status = db.check_resource("res:exp-check")
        assert status["locked"] is False


class TestReleaseResource:
    def test_release_existing(self, db):
        db.claim_resource("res:rel1", "run-001")
        assert db.release_resource("res:rel1") is True
        assert db.check_resource("res:rel1")["locked"] is False

    def test_release_nonexistent(self, db):
        assert db.release_resource("res:ghost") is False

    def test_list_locks(self, db):
        db.claim_resource("res:list1", "run-A")
        db.claim_resource("res:list2", "run-B")
        locks = db.list_resource_locks()
        ids = [l["resource_id"] for l in locks]
        assert "res:list1" in ids
        assert "res:list2" in ids


# ===========================================================================
# V6-12: Shared State / Blackboard
# ===========================================================================

class TestSharedStateSchema:
    def test_table_exists(self, db):
        with db._connect() as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert "shared_state" in tables


class TestStateSetGet:
    def test_set_and_get_string(self, db):
        db.state_set("key1", "hello")
        assert db.state_get("key1") == "hello"

    def test_set_and_get_int(self, db):
        db.state_set("key2", 42)
        assert db.state_get("key2") == 42

    def test_set_and_get_dict(self, db):
        db.state_set("key3", {"a": 1, "b": [1, 2, 3]})
        val = db.state_get("key3")
        assert val["a"] == 1
        assert val["b"] == [1, 2, 3]

    def test_set_and_get_list(self, db):
        db.state_set("key4", [1, "two", True])
        assert db.state_get("key4") == [1, "two", True]

    def test_get_missing_returns_none(self, db):
        assert db.state_get("nonexistent") is None

    def test_set_upserts(self, db):
        db.state_set("upsert-key", "first")
        db.state_set("upsert-key", "second")
        assert db.state_get("upsert-key") == "second"

    def test_set_none_value(self, db):
        db.state_set("null-key", None)
        assert db.state_get("null-key") is None

    def test_set_updates_updated_at(self, db):
        db.state_set("ts-key", "value")
        entries = db.state_list()
        entry = next(e for e in entries if e["key"] == "ts-key")
        assert entry["updated_at"] is not None


class TestStateList:
    def test_list_all(self, db):
        db.state_set("a:1", "v1")
        db.state_set("b:1", "v2")
        entries = db.state_list()
        keys = [e["key"] for e in entries]
        assert "a:1" in keys
        assert "b:1" in keys

    def test_list_with_prefix(self, db):
        db.state_set("buddy:last_date", "2026-03-25")
        db.state_set("buddy:count", 10)
        db.state_set("other:key", "ignored")
        entries = db.state_list(prefix="buddy:")
        keys = [e["key"] for e in entries]
        assert "buddy:last_date" in keys
        assert "buddy:count" in keys
        assert "other:key" not in keys

    def test_list_empty(self, db):
        entries = db.state_list()
        assert entries == []

    def test_list_sorted_by_key(self, db):
        db.state_set("z-key", 1)
        db.state_set("a-key", 2)
        entries = db.state_list()
        keys = [e["key"] for e in entries]
        assert keys == sorted(keys)

    def test_list_entry_has_value_deserialized(self, db):
        db.state_set("obj-key", {"x": 99})
        entries = db.state_list()
        entry = next(e for e in entries if e["key"] == "obj-key")
        assert entry["value"]["x"] == 99


class TestStateDelete:
    def test_delete_existing(self, db):
        db.state_set("del-key", "to-delete")
        assert db.state_delete("del-key") is True
        assert db.state_get("del-key") is None

    def test_delete_nonexistent(self, db):
        assert db.state_delete("ghost") is False


# ===========================================================================
# MCP Handler integration tests
# ===========================================================================

class TestMcpHandlerSaveRestoreContext:
    @pytest.fixture(autouse=True)
    def patch_db(self, tmp_path, monkeypatch):
        """Patch BrixDB() in handlers to use a temp DB."""
        real_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            real_init(self, db_path=tmp_path / "brix.db")

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

    def test_save_returns_saved_true(self):
        from brix.mcp_server import _handle_save_agent_context
        result = asyncio.get_event_loop().run_until_complete(
            _handle_save_agent_context({
                "session_id": "test-session",
                "summary": "Working on buddy pipeline",
            })
        )
        assert result["saved"] is True
        assert result["session_id"] == "test-session"
        assert result["updated_at"] is not None

    def test_restore_returns_context(self):
        from brix.mcp_server import _handle_save_agent_context, _handle_restore_agent_context
        asyncio.get_event_loop().run_until_complete(
            _handle_save_agent_context({
                "session_id": "s-restore",
                "summary": "Restored session",
                "active_pipeline": "buddy-intake",
                "pending_decisions": ["Confirm extraction?"],
            })
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_restore_agent_context({"session_id": "s-restore"})
        )
        assert result["found"] is True
        assert result["summary"] == "Restored session"
        assert result["active_pipeline"] == "buddy-intake"
        assert "Confirm extraction?" in result["pending_decisions"]

    def test_restore_not_found(self):
        from brix.mcp_server import _handle_restore_agent_context
        result = asyncio.get_event_loop().run_until_complete(
            _handle_restore_agent_context({"session_id": "ghost"})
        )
        assert result["found"] is False

    def test_save_missing_session_id(self):
        from brix.mcp_server import _handle_save_agent_context
        result = asyncio.get_event_loop().run_until_complete(
            _handle_save_agent_context({"summary": "No session ID"})
        )
        assert "error" in result

    def test_save_missing_summary(self):
        from brix.mcp_server import _handle_save_agent_context
        result = asyncio.get_event_loop().run_until_complete(
            _handle_save_agent_context({"session_id": "s-no-summary"})
        )
        assert "error" in result


class TestMcpHandlerResourceLocks:
    @pytest.fixture(autouse=True)
    def patch_db(self, tmp_path, monkeypatch):
        real_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            real_init(self, db_path=tmp_path / "brix.db")

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

    def test_claim_success(self):
        from brix.mcp_server import _handle_claim_resource
        result = asyncio.get_event_loop().run_until_complete(
            _handle_claim_resource({"resource_id": "res:test", "run_id": "run-001"})
        )
        assert result["claimed"] is True

    def test_claim_conflict(self):
        from brix.mcp_server import _handle_claim_resource
        asyncio.get_event_loop().run_until_complete(
            _handle_claim_resource({"resource_id": "res:shared", "run_id": "run-001"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_claim_resource({"resource_id": "res:shared", "run_id": "run-002"})
        )
        assert result["claimed"] is False
        assert result["held_by"] == "run-001"

    def test_check_resource(self):
        from brix.mcp_server import _handle_claim_resource, _handle_check_resource
        asyncio.get_event_loop().run_until_complete(
            _handle_claim_resource({"resource_id": "res:check", "run_id": "run-X"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_check_resource({"resource_id": "res:check"})
        )
        assert result["locked"] is True
        assert result["run_id"] == "run-X"

    def test_check_free_resource(self):
        from brix.mcp_server import _handle_check_resource
        result = asyncio.get_event_loop().run_until_complete(
            _handle_check_resource({"resource_id": "res:free"})
        )
        assert result["locked"] is False

    def test_release_resource(self):
        from brix.mcp_server import _handle_claim_resource, _handle_release_resource, _handle_check_resource
        asyncio.get_event_loop().run_until_complete(
            _handle_claim_resource({"resource_id": "res:rel", "run_id": "run-Y"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_release_resource({"resource_id": "res:rel"})
        )
        assert result["released"] is True

        check = asyncio.get_event_loop().run_until_complete(
            _handle_check_resource({"resource_id": "res:rel"})
        )
        assert check["locked"] is False

    def test_claim_missing_resource_id(self):
        from brix.mcp_server import _handle_claim_resource
        result = asyncio.get_event_loop().run_until_complete(
            _handle_claim_resource({"run_id": "run-001"})
        )
        assert "error" in result

    def test_claim_missing_run_id(self):
        from brix.mcp_server import _handle_claim_resource
        result = asyncio.get_event_loop().run_until_complete(
            _handle_claim_resource({"resource_id": "res:x"})
        )
        assert "error" in result


class TestMcpHandlerBlackboard:
    @pytest.fixture(autouse=True)
    def patch_db(self, tmp_path, monkeypatch):
        real_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            real_init(self, db_path=tmp_path / "brix.db")

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

    def test_state_set_and_get(self):
        from brix.mcp_server import _handle_state_set, _handle_state_get
        asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"key": "test-key", "value": "test-value"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_get({"key": "test-key"})
        )
        assert result["found"] is True
        assert result["value"] == "test-value"

    def test_state_get_not_found(self):
        from brix.mcp_server import _handle_state_get
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_get({"key": "nonexistent"})
        )
        assert result["found"] is False

    def test_state_set_complex_value(self):
        from brix.mcp_server import _handle_state_set, _handle_state_get
        payload = {"items": [1, 2, 3], "nested": {"x": True}}
        asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"key": "complex", "value": payload})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_get({"key": "complex"})
        )
        assert result["value"]["items"] == [1, 2, 3]
        assert result["value"]["nested"]["x"] is True

    def test_state_list(self):
        from brix.mcp_server import _handle_state_set, _handle_state_list
        asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"key": "buddy:a", "value": 1})
        )
        asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"key": "buddy:b", "value": 2})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_list({})
        )
        assert result["count"] >= 2
        keys = [e["key"] for e in result["entries"]]
        assert "buddy:a" in keys
        assert "buddy:b" in keys

    def test_state_list_with_prefix(self):
        from brix.mcp_server import _handle_state_set, _handle_state_list
        asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"key": "x:1", "value": "a"})
        )
        asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"key": "y:1", "value": "b"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_list({"prefix": "x:"})
        )
        keys = [e["key"] for e in result["entries"]]
        assert "x:1" in keys
        assert "y:1" not in keys

    def test_state_delete(self):
        from brix.mcp_server import _handle_state_set, _handle_state_delete, _handle_state_get
        asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"key": "del-me", "value": "bye"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_delete({"key": "del-me"})
        )
        assert result["deleted"] is True

        check = asyncio.get_event_loop().run_until_complete(
            _handle_state_get({"key": "del-me"})
        )
        assert check["found"] is False

    def test_state_delete_nonexistent(self):
        from brix.mcp_server import _handle_state_delete
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_delete({"key": "ghost"})
        )
        assert result["deleted"] is False

    def test_state_set_missing_key(self):
        from brix.mcp_server import _handle_state_set
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"value": "no-key"})
        )
        assert "error" in result

    def test_state_set_missing_value(self):
        from brix.mcp_server import _handle_state_set
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_set({"key": "no-value"})
        )
        assert "error" in result

    def test_state_get_missing_key_param(self):
        from brix.mcp_server import _handle_state_get
        result = asyncio.get_event_loop().run_until_complete(
            _handle_state_get({})
        )
        assert "error" in result
