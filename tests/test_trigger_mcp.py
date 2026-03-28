"""Tests for Trigger CRUD MCP tools (T-BRIX-V5-12).

Tests cover:
- TriggerStore CRUD operations
- MCP handler functions: trigger_add, trigger_list, trigger_get,
  trigger_update, trigger_delete, scheduler_status, scheduler_start, scheduler_stop
- trigger_test handler (with mocked runner)
"""
import tempfile
from pathlib import Path

import pytest

from brix.triggers.store import TriggerStore


# ---------------------------------------------------------------------------
# TriggerStore — unit tests
# ---------------------------------------------------------------------------

def test_trigger_store_add(tmp_path):
    """TriggerStore.add creates a trigger and returns its dict."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    t = store.add(
        name="test-mail",
        type="mail",
        config={"folder": "Inbox", "interval": "5m"},
        pipeline="import-mails",
        enabled=True,
    )
    assert t["name"] == "test-mail"
    assert t["type"] == "mail"
    assert t["pipeline"] == "import-mails"
    assert t["enabled"] is True
    assert t["config"] == {"folder": "Inbox", "interval": "5m"}
    assert "id" in t
    assert "created_at" in t
    assert "updated_at" in t


def test_trigger_store_add_invalid_type(tmp_path):
    """TriggerStore.add raises ValueError for unknown trigger type."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    with pytest.raises(ValueError, match="Unknown trigger type"):
        store.add(name="bad", type="unknown", config={}, pipeline="pipe")


def test_trigger_store_add_duplicate_name(tmp_path):
    """TriggerStore.add raises ValueError for duplicate name."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    store.add(name="dup", type="file", config={}, pipeline="p1")
    with pytest.raises(ValueError, match="already exists"):
        store.add(name="dup", type="file", config={}, pipeline="p2")


def test_trigger_store_list(tmp_path):
    """TriggerStore.list_all returns all triggers sorted by name."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    store.add(name="b-trigger", type="file", config={}, pipeline="p1")
    store.add(name="a-trigger", type="mail", config={}, pipeline="p2")

    all_triggers = store.list_all()
    assert len(all_triggers) == 2
    assert all_triggers[0]["name"] == "a-trigger"
    assert all_triggers[1]["name"] == "b-trigger"


def test_trigger_store_get_by_name(tmp_path):
    """TriggerStore.get retrieves a trigger by name."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    store.add(name="my-trigger", type="http_poll", config={"url": "http://x"}, pipeline="p")

    t = store.get("my-trigger")
    assert t is not None
    assert t["name"] == "my-trigger"
    assert t["config"] == {"url": "http://x"}


def test_trigger_store_get_by_id(tmp_path):
    """TriggerStore.get retrieves a trigger by UUID."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    created = store.add(name="uuid-trigger", type="file", config={}, pipeline="p")
    trigger_id = created["id"]

    t = store.get(trigger_id)
    assert t is not None
    assert t["name"] == "uuid-trigger"


def test_trigger_store_get_missing(tmp_path):
    """TriggerStore.get returns None for unknown trigger."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    assert store.get("nonexistent") is None


def test_trigger_store_update_config(tmp_path):
    """TriggerStore.update changes the config."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    store.add(name="upd", type="mail", config={"folder": "Inbox"}, pipeline="p")

    updated = store.update(name="upd", config={"folder": "Sent"})
    assert updated is not None
    assert updated["config"] == {"folder": "Sent"}


def test_trigger_store_update_enabled(tmp_path):
    """TriggerStore.update can toggle enabled state."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    store.add(name="toggle", type="file", config={}, pipeline="p", enabled=True)

    updated = store.update(name="toggle", enabled=False)
    assert updated is not None
    assert updated["enabled"] is False

    re_enabled = store.update(name="toggle", enabled=True)
    assert re_enabled is not None
    assert re_enabled["enabled"] is True


def test_trigger_store_update_pipeline(tmp_path):
    """TriggerStore.update can change the target pipeline."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    store.add(name="pipe-change", type="file", config={}, pipeline="old-pipeline")

    updated = store.update(name="pipe-change", pipeline="new-pipeline")
    assert updated is not None
    assert updated["pipeline"] == "new-pipeline"


def test_trigger_store_update_missing(tmp_path):
    """TriggerStore.update returns None for unknown trigger."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    result = store.update(name="ghost", enabled=False)
    assert result is None


def test_trigger_store_delete(tmp_path):
    """TriggerStore.delete removes a trigger and returns True."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    store.add(name="del-me", type="file", config={}, pipeline="p")

    deleted = store.delete("del-me")
    assert deleted is True
    assert store.get("del-me") is None


def test_trigger_store_delete_missing(tmp_path):
    """TriggerStore.delete returns False for unknown trigger."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    assert store.delete("ghost") is False


def test_trigger_store_record_fired(tmp_path):
    """TriggerStore.record_fired updates last_fired_at and last_run_id."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    store.add(name="fired", type="file", config={}, pipeline="p")

    store.record_fired("fired", run_id="run-001", status="success")

    t = store.get("fired")
    assert t is not None
    assert t["last_run_id"] == "run-001"
    assert t["last_status"] == "success"
    assert t["last_fired_at"] is not None


def test_trigger_store_all_types(tmp_path):
    """TriggerStore.add accepts all 4 valid trigger types."""
    store = TriggerStore(db_path=tmp_path / "brix.db")
    for trigger_type in ("mail", "file", "http_poll", "pipeline_done"):
        store.add(name=f"t-{trigger_type}", type=trigger_type, config={}, pipeline="p")

    all_triggers = store.list_all()
    assert len(all_triggers) == 4


# ---------------------------------------------------------------------------
# MCP handler tests — using _handle_* functions directly
# ---------------------------------------------------------------------------

import brix.mcp_server as _mcp

from helpers import run_coro as _run


def _patch_store(monkeypatch, tmp_path):
    """Monkeypatch TriggerStore to use a temp DB."""
    original_init = TriggerStore.__init__

    def patched_init(self, db_path=None):
        original_init(self, db_path=tmp_path / "brix.db")

    monkeypatch.setattr(TriggerStore, "__init__", patched_init)


def test_mcp_trigger_add(monkeypatch, tmp_path):
    """_handle_trigger_add creates a trigger via MCP."""
    _patch_store(monkeypatch, tmp_path)
    result = _run(_mcp._handle_trigger_add({
        "name": "mcp-mail",
        "type": "mail",
        "pipeline": "import-mails",
        "config": {"folder": "Inbox"},
    }))
    assert result["success"] is True
    assert result["trigger"]["name"] == "mcp-mail"
    assert result["trigger"]["type"] == "mail"


def test_mcp_trigger_add_missing_name(monkeypatch, tmp_path):
    """_handle_trigger_add returns error when name is missing."""
    _patch_store(monkeypatch, tmp_path)
    result = _run(_mcp._handle_trigger_add({
        "type": "file",
        "pipeline": "p",
    }))
    assert result["success"] is False
    assert "name" in result["error"]


def test_mcp_trigger_add_invalid_type(monkeypatch, tmp_path):
    """_handle_trigger_add returns error for unknown type."""
    _patch_store(monkeypatch, tmp_path)
    result = _run(_mcp._handle_trigger_add({
        "name": "bad",
        "type": "webhook",
        "pipeline": "p",
    }))
    assert result["success"] is False
    assert "Unknown trigger type" in result["error"]


def test_mcp_trigger_list_empty(monkeypatch, tmp_path):
    """_handle_trigger_list returns empty list when no triggers exist."""
    _patch_store(monkeypatch, tmp_path)
    result = _run(_mcp._handle_trigger_list({}))
    assert result["triggers"] == []
    assert result["total"] == 0


def test_mcp_trigger_list_populated(monkeypatch, tmp_path):
    """_handle_trigger_list returns all triggers."""
    _patch_store(monkeypatch, tmp_path)
    _run(_mcp._handle_trigger_add({"name": "t1", "type": "file", "pipeline": "p1"}))
    _run(_mcp._handle_trigger_add({"name": "t2", "type": "mail", "pipeline": "p2"}))

    result = _run(_mcp._handle_trigger_list({}))
    assert result["total"] == 2
    names = {t["name"] for t in result["triggers"]}
    assert names == {"t1", "t2"}


def test_mcp_trigger_get(monkeypatch, tmp_path):
    """_handle_trigger_get returns trigger details."""
    _patch_store(monkeypatch, tmp_path)
    _run(_mcp._handle_trigger_add({
        "name": "get-me",
        "type": "http_poll",
        "pipeline": "p",
        "config": {"url": "https://api.example.com"},
    }))

    result = _run(_mcp._handle_trigger_get({"name": "get-me"}))
    assert result["success"] is True
    assert result["trigger"]["config"]["url"] == "https://api.example.com"


def test_mcp_trigger_get_missing(monkeypatch, tmp_path):
    """_handle_trigger_get returns error for unknown trigger."""
    _patch_store(monkeypatch, tmp_path)
    result = _run(_mcp._handle_trigger_get({"name": "ghost"}))
    assert result["success"] is False
    assert "not found" in result["error"]


def test_mcp_trigger_update(monkeypatch, tmp_path):
    """_handle_trigger_update modifies a trigger."""
    _patch_store(monkeypatch, tmp_path)
    _run(_mcp._handle_trigger_add({
        "name": "upd-me",
        "type": "file",
        "pipeline": "p",
        "enabled": True,
    }))

    result = _run(_mcp._handle_trigger_update({"name": "upd-me", "enabled": False}))
    assert result["success"] is True
    assert result["trigger"]["enabled"] is False


def test_mcp_trigger_update_missing(monkeypatch, tmp_path):
    """_handle_trigger_update returns error for unknown trigger."""
    _patch_store(monkeypatch, tmp_path)
    result = _run(_mcp._handle_trigger_update({"name": "ghost", "enabled": False}))
    assert result["success"] is False
    assert "not found" in result["error"]


def test_mcp_trigger_delete(monkeypatch, tmp_path):
    """_handle_trigger_delete removes a trigger."""
    _patch_store(monkeypatch, tmp_path)
    _run(_mcp._handle_trigger_add({"name": "del-me", "type": "file", "pipeline": "p"}))

    result = _run(_mcp._handle_trigger_delete({"name": "del-me"}))
    assert result["success"] is True
    assert result["name"] == "del-me"

    # Verify gone
    get_result = _run(_mcp._handle_trigger_get({"name": "del-me"}))
    assert get_result["success"] is False


def test_mcp_trigger_delete_missing(monkeypatch, tmp_path):
    """_handle_trigger_delete returns error for unknown trigger."""
    _patch_store(monkeypatch, tmp_path)
    result = _run(_mcp._handle_trigger_delete({"name": "ghost"}))
    assert result["success"] is False
    assert "not found" in result["error"]


def test_mcp_scheduler_status_no_triggers(monkeypatch, tmp_path):
    """_handle_scheduler_status returns running=False with 0 triggers."""
    _patch_store(monkeypatch, tmp_path)
    # Reset module-level state
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", False)

    result = _run(_mcp._handle_scheduler_status({}))
    assert result["running"] is False
    assert result["trigger_count"] == 0
    assert result["enabled_count"] == 0


def test_mcp_scheduler_status_with_triggers(monkeypatch, tmp_path):
    """_handle_scheduler_status shows correct counts."""
    _patch_store(monkeypatch, tmp_path)
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", False)

    _run(_mcp._handle_trigger_add({"name": "s1", "type": "file", "pipeline": "p", "enabled": True}))
    _run(_mcp._handle_trigger_add({"name": "s2", "type": "mail", "pipeline": "p", "enabled": False}))

    result = _run(_mcp._handle_scheduler_status({}))
    assert result["trigger_count"] == 2
    assert result["enabled_count"] == 1


def test_mcp_scheduler_start_no_triggers(monkeypatch, tmp_path):
    """_handle_scheduler_start returns error when no enabled triggers."""
    _patch_store(monkeypatch, tmp_path)
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", False)

    result = _run(_mcp._handle_scheduler_start({}))
    assert result["success"] is False
    assert "no_enabled_triggers" in result["status"]


def test_mcp_scheduler_start_with_triggers(monkeypatch, tmp_path):
    """_handle_scheduler_start succeeds when enabled triggers exist."""
    _patch_store(monkeypatch, tmp_path)
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", False)

    _run(_mcp._handle_trigger_add({
        "name": "watch-inbox",
        "type": "mail",
        "pipeline": "import-mails",
        "enabled": True,
    }))

    result = _run(_mcp._handle_scheduler_start({}))
    assert result["success"] is True
    assert result["status"] == "started"
    assert result["enabled_triggers"] == 1

    # Cleanup
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", False)


def test_mcp_scheduler_start_already_running(monkeypatch, tmp_path):
    """_handle_scheduler_start returns already_running when already active."""
    _patch_store(monkeypatch, tmp_path)
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", True)

    _run(_mcp._handle_trigger_add({
        "name": "watch-files",
        "type": "file",
        "pipeline": "p",
        "enabled": True,
    }))

    result = _run(_mcp._handle_scheduler_start({}))
    assert result["success"] is True
    assert result["status"] == "already_running"

    # Cleanup
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", False)


def test_mcp_scheduler_stop(monkeypatch, tmp_path):
    """_handle_scheduler_stop sets running to False."""
    _patch_store(monkeypatch, tmp_path)
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", True)

    result = _run(_mcp._handle_scheduler_stop({}))
    assert result["success"] is True
    assert result["status"] == "stopped"
    import brix.mcp_handlers.triggers as _trig_mod
    assert _trig_mod._scheduler_running is False


def test_mcp_scheduler_stop_not_running(monkeypatch, tmp_path):
    """_handle_scheduler_stop is a no-op when not running."""
    _patch_store(monkeypatch, tmp_path)
    monkeypatch.setattr("brix.mcp_handlers.triggers._scheduler_running", False)

    result = _run(_mcp._handle_scheduler_stop({}))
    assert result["success"] is True
    assert result["status"] == "already_stopped"


def test_mcp_trigger_test_missing(monkeypatch, tmp_path):
    """_handle_trigger_test returns error for unknown trigger."""
    _patch_store(monkeypatch, tmp_path)
    result = _run(_mcp._handle_trigger_test({"name": "ghost"}))
    assert result["success"] is False
    assert "not found" in result["error"]


def test_mcp_trigger_test_file(monkeypatch, tmp_path):
    """_handle_trigger_test fires a file trigger and returns events."""
    _patch_store(monkeypatch, tmp_path)

    # Create a watch directory with a file
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (watch_dir / "report.pdf").write_bytes(b"%PDF")

    _run(_mcp._handle_trigger_add({
        "name": "watch-dir",
        "type": "file",
        "pipeline": "process-file",
        "config": {"path": str(watch_dir), "pattern": "*.pdf"},
    }))

    # Mock the engine so we don't need a real pipeline
    import unittest.mock as mock
    from brix.triggers.runners import BaseTriggerRunner

    mock_result = mock.MagicMock()
    mock_result.run_id = "run-test-001"
    mock_result.success = True

    with mock.patch.object(
        BaseTriggerRunner, "fire",
        new_callable=mock.AsyncMock,
        return_value=mock_result,
    ):
        result = _run(_mcp._handle_trigger_test({"name": "watch-dir"}))

    assert result["success"] is True
    assert result["events_found"] == 1
    assert result["events_fired"] == 1
    assert result["results"][0]["run_id"] == "run-test-001"
    assert result["results"][0]["success"] is True


def test_mcp_trigger_roundtrip(monkeypatch, tmp_path):
    """Full CRUD roundtrip: add → get → update → list → delete."""
    _patch_store(monkeypatch, tmp_path)

    # Add
    add_result = _run(_mcp._handle_trigger_add({
        "name": "roundtrip",
        "type": "http_poll",
        "pipeline": "check-api",
        "config": {"url": "https://api.example.com/status"},
        "enabled": True,
    }))
    assert add_result["success"] is True

    # Get
    get_result = _run(_mcp._handle_trigger_get({"name": "roundtrip"}))
    assert get_result["success"] is True
    assert get_result["trigger"]["pipeline"] == "check-api"

    # Update
    upd_result = _run(_mcp._handle_trigger_update({
        "name": "roundtrip",
        "enabled": False,
        "pipeline": "check-api-v2",
    }))
    assert upd_result["success"] is True
    assert upd_result["trigger"]["enabled"] is False
    assert upd_result["trigger"]["pipeline"] == "check-api-v2"

    # List
    list_result = _run(_mcp._handle_trigger_list({}))
    assert list_result["total"] == 1

    # Delete
    del_result = _run(_mcp._handle_trigger_delete({"name": "roundtrip"}))
    assert del_result["success"] is True

    # Confirm gone
    list_after = _run(_mcp._handle_trigger_list({}))
    assert list_after["total"] == 0
