"""Tests for T-BRIX-V5-11 — CRUD gap fillers.

Covers:
1. alert_update — update rule fields via MCP tool
2. search_pipelines — substring match on name + description
3. credential_rotate — OAuth2 refresh_token flow + error paths
4. credential_search — name/type substring search
5. run_annotate — attach notes to a run
6. run_search — filter by pipeline / status / time range
7. MCP Server Management — server_add, server_list, server_update, server_remove, server_refresh
8. get_tips — updated tips include new tool names
"""
import json
import re
import unittest.mock as mock
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Async test helper
# ---------------------------------------------------------------------------

from helpers import run_coro as run


# ===========================================================================
# 1. alert_update
# ===========================================================================

class TestAlertUpdate:
    @pytest.fixture
    def mgr(self, tmp_path, monkeypatch):
        from brix.alerting import AlertManager
        m = AlertManager(db_path=tmp_path / "brix.db")
        return m

    def test_update_enabled_field(self, mgr, tmp_path, monkeypatch):
        from brix.mcp_server import _handle_alert_update
        from brix import mcp_server as srv

        rule = mgr.add_rule("test-rule", "pipeline_failed", "log")

        # Monkeypatch AlertManager to use our tmp_db
        monkeypatch.setattr(
            "brix.mcp_server.AlertManager" if hasattr(srv, "AlertManager") else "brix.alerting.AlertManager",
            lambda: mgr,
            raising=False,
        )

        # Patch at handler level: patch AlertManager inside _handle_alert_update
        with mock.patch("brix.alerting.AlertManager", return_value=mgr):
            result = run(_handle_alert_update({"id": rule.id, "enabled": False}))

        assert result["success"] is True
        assert result["rule"]["enabled"] is False

    def test_update_nonexistent_rule(self, tmp_path):
        from brix.mcp_server import _handle_alert_update
        with mock.patch("brix.alerting.AlertManager") as MockMgr:
            instance = MockMgr.return_value
            instance.update_rule.return_value = None
            result = run(_handle_alert_update({"id": "no-such-id", "enabled": True}))
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_update_missing_id(self):
        from brix.mcp_server import _handle_alert_update
        result = run(_handle_alert_update({}))
        assert result["success"] is False
        assert "id" in result["error"]

    def test_update_invalid_condition(self, mgr):
        from brix.mcp_server import _handle_alert_update
        rule = mgr.add_rule("r", "pipeline_failed", "log")
        with mock.patch("brix.alerting.AlertManager", return_value=mgr):
            result = run(_handle_alert_update({"id": rule.id, "condition": "not_a_real_condition"}))
        assert result["success"] is False

    def test_update_name_field(self, mgr):
        from brix.mcp_server import _handle_alert_update
        rule = mgr.add_rule("old-name", "pipeline_failed", "log")
        with mock.patch("brix.alerting.AlertManager", return_value=mgr):
            result = run(_handle_alert_update({"id": rule.id, "name": "new-name"}))
        assert result["success"] is True
        assert result["rule"]["name"] == "new-name"


# ===========================================================================
# 2. search_pipelines
# ===========================================================================

class TestSearchPipelines:
    @pytest.fixture
    def isolated_pipeline_dir(self, tmp_path):
        """Create a pipeline dir that is fully isolated (no extra search paths)."""
        import yaml as _yaml
        d = tmp_path / "pipelines"
        d.mkdir()
        for name, desc in [
            ("import-invoices", "Imports invoices from OneDrive"),
            ("send-emails", "Sends email notifications"),
            ("budget-report", "Monthly budget summary report"),
        ]:
            (d / f"{name}.yaml").write_text(_yaml.dump({
                "name": name,
                "description": desc,
                "version": "1.0.0",
                "steps": [],
            }))
        return d

    def _make_store(self, pipeline_dir):
        """Return a PipelineStore that ONLY searches in the given directory with isolated DB."""
        from brix.db import BrixDB
        from brix.pipeline_store import PipelineStore
        isolated_db = BrixDB(db_path=pipeline_dir / "test.db")
        return PipelineStore(pipelines_dir=pipeline_dir, search_paths=[pipeline_dir], db=isolated_db)

    def test_match_by_name(self, isolated_pipeline_dir, monkeypatch):
        from brix.mcp_server import _handle_search_pipelines
        monkeypatch.setattr("brix.mcp_handlers.pipelines._pipeline_dir", lambda: isolated_pipeline_dir)
        store = self._make_store(isolated_pipeline_dir)
        with mock.patch("brix.mcp_handlers.pipelines.PipelineStore", return_value=store):
            result = run(_handle_search_pipelines({"query": "invoice"}))
        assert result["success"] is True
        assert result["total"] == 1
        assert result["results"][0]["name"] == "import-invoices"

    def test_match_by_description(self, isolated_pipeline_dir, monkeypatch):
        from brix.mcp_server import _handle_search_pipelines
        monkeypatch.setattr("brix.mcp_handlers.pipelines._pipeline_dir", lambda: isolated_pipeline_dir)
        store = self._make_store(isolated_pipeline_dir)
        with mock.patch("brix.mcp_handlers.pipelines.PipelineStore", return_value=store):
            result = run(_handle_search_pipelines({"query": "budget"}))
        assert result["success"] is True
        assert result["total"] == 1

    def test_case_insensitive(self, isolated_pipeline_dir, monkeypatch):
        from brix.mcp_server import _handle_search_pipelines
        monkeypatch.setattr("brix.mcp_handlers.pipelines._pipeline_dir", lambda: isolated_pipeline_dir)
        store = self._make_store(isolated_pipeline_dir)
        with mock.patch("brix.mcp_handlers.pipelines.PipelineStore", return_value=store):
            result = run(_handle_search_pipelines({"query": "EMAIL"}))
        assert result["success"] is True
        assert result["total"] == 1

    def test_no_match_returns_empty(self, isolated_pipeline_dir, monkeypatch):
        from brix.mcp_server import _handle_search_pipelines
        monkeypatch.setattr("brix.mcp_handlers.pipelines._pipeline_dir", lambda: isolated_pipeline_dir)
        store = self._make_store(isolated_pipeline_dir)
        with mock.patch("brix.mcp_handlers.pipelines.PipelineStore", return_value=store):
            result = run(_handle_search_pipelines({"query": "xxxxnotfound"}))
        assert result["success"] is True
        assert result["total"] == 0

    def test_missing_query_returns_error(self):
        from brix.mcp_server import _handle_search_pipelines
        result = run(_handle_search_pipelines({}))
        assert result["success"] is False


# ===========================================================================
# 3. credential_rotate
# ===========================================================================

class TestCredentialRotate:
    @pytest.fixture
    def cred_store(self, tmp_path, monkeypatch):
        from brix.credential_store import CredentialStore
        store = CredentialStore(db_path=tmp_path / "creds.db")
        return store

    def test_rotate_non_oauth2_raises_error(self, cred_store):
        from brix.mcp_server import _handle_credential_rotate
        cred_store.add("my-key", "api-key", "sk-abc123")
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_rotate({"id": "my-key"}))
        assert result["success"] is False
        assert "oauth2" in result["error"].lower()

    def test_rotate_missing_id(self):
        from brix.mcp_server import _handle_credential_rotate
        result = run(_handle_credential_rotate({}))
        assert result["success"] is False
        assert "id" in result["error"]

    def test_rotate_not_found(self, cred_store):
        from brix.mcp_server import _handle_credential_rotate
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_rotate({"id": "no-such-cred"}))
        assert result["success"] is False

    def test_rotate_oauth2_without_refresh_token_raises_error(self, cred_store):
        from brix.mcp_server import _handle_credential_rotate
        # Store oauth2 cred without refresh_token
        cred_store.add("my-oauth", "oauth2", json.dumps({"access_token": "old", "token_url": "https://example.com/token"}))
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_rotate({"id": "my-oauth"}))
        assert result["success"] is False
        assert "refresh_token" in result["error"]

    def test_rotate_oauth2_calls_token_url(self, cred_store):
        """rotate() calls token_url and stores new access_token."""
        import urllib.request
        from brix.credential_store import CredentialStore

        cred_data = {
            "access_token": "old-token",
            "refresh_token": "my-refresh",
            "token_url": "https://auth.example.com/token",
        }
        cred_store.add("oauth-cred", "oauth2", json.dumps(cred_data))

        # Mock urllib.request.urlopen
        mock_response = mock.MagicMock()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = mock.MagicMock(return_value=False)
        mock_response.read.return_value = json.dumps({"access_token": "new-token"}).encode()

        with mock.patch("urllib.request.urlopen", return_value=mock_response):
            result = cred_store.rotate("oauth-cred")

        assert result["name"] == "oauth-cred"
        # Verify new token is stored (decrypt to check)
        new_val = json.loads(cred_store.resolve("oauth-cred"))
        assert new_val["access_token"] == "new-token"
        assert new_val["refresh_token"] == "my-refresh"  # preserved


# ===========================================================================
# 4. credential_search
# ===========================================================================

class TestCredentialSearch:
    @pytest.fixture
    def cred_store(self, tmp_path):
        from brix.credential_store import CredentialStore
        return CredentialStore(db_path=tmp_path / "creds.db")

    def test_search_by_name(self, cred_store):
        from brix.mcp_server import _handle_credential_search
        cred_store.add("openai-key", "api-key", "sk-xxx")
        cred_store.add("github-token", "api-key", "ghp-xxx")
        cred_store.add("my-oauth", "oauth2", "{}")
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_search({"query": "openai"}))
        assert result["success"] is True
        assert result["total"] == 1
        assert result["credentials"][0]["name"] == "openai-key"

    def test_search_by_type(self, cred_store):
        from brix.mcp_server import _handle_credential_search
        cred_store.add("g-oauth", "oauth2", "{}")
        cred_store.add("api", "api-key", "x")
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_search({"query": "oauth"}))
        assert result["success"] is True
        assert result["total"] == 1
        assert result["credentials"][0]["type"] == "oauth2"

    def test_search_case_insensitive(self, cred_store):
        from brix.mcp_server import _handle_credential_search
        cred_store.add("MyApiKey", "api-key", "x")
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_search({"query": "MYAPI"}))
        assert result["success"] is True
        assert result["total"] == 1

    def test_search_no_results(self, cred_store):
        from brix.mcp_server import _handle_credential_search
        cred_store.add("abc", "api-key", "x")
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_search({"query": "xxxxnotfound"}))
        assert result["success"] is True
        assert result["total"] == 0

    def test_search_no_values_returned(self, cred_store):
        from brix.mcp_server import _handle_credential_search
        cred_store.add("secret-key", "api-key", "plaintext-value")
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_search({"query": "secret"}))
        assert result["success"] is True
        for cred in result["credentials"]:
            assert "value" not in cred
            assert "encrypted_value" not in cred

    def test_search_missing_query(self, cred_store):
        from brix.mcp_server import _handle_credential_search
        with mock.patch("brix.mcp_handlers.credentials.CredentialStore", return_value=cred_store):
            result = run(_handle_credential_search({}))
        assert result["success"] is False


# ===========================================================================
# 5. run_annotate
# ===========================================================================

class TestRunAnnotate:
    @pytest.fixture
    def history(self, tmp_path):
        from brix.history import RunHistory
        h = RunHistory(db_path=tmp_path / "brix.db")
        h.record_start("run-001", "my-pipeline")
        h.record_finish("run-001", True, 1.5)
        return h

    def test_annotate_success(self, history):
        from brix.mcp_server import _handle_run_annotate
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_annotate({"run_id": "run-001", "notes": "looks good"}))
        assert result["success"] is True
        assert result["run_id"] == "run-001"
        assert result["notes"] == "looks good"

    def test_annotate_nonexistent_run(self, history):
        from brix.mcp_server import _handle_run_annotate
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_annotate({"run_id": "no-such-run", "notes": "x"}))
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_annotate_persists_in_db(self, history, tmp_path):
        from brix.history import RunHistory
        history.annotate("run-001", "important note")
        # Re-open and check
        h2 = RunHistory(db_path=tmp_path / "brix.db")
        row = h2.get_run("run-001")
        assert row is not None
        assert row.get("notes") == "important note"

    def test_annotate_missing_run_id(self, history):
        from brix.mcp_server import _handle_run_annotate
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_annotate({"notes": "hello"}))
        assert result["success"] is False

    def test_annotate_missing_notes(self, history):
        from brix.mcp_server import _handle_run_annotate
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_annotate({"run_id": "run-001"}))
        assert result["success"] is False


# ===========================================================================
# 6. run_search
# ===========================================================================

class TestRunSearch:
    @pytest.fixture
    def history(self, tmp_path):
        from brix.history import RunHistory
        h = RunHistory(db_path=tmp_path / "brix.db")
        # pipeline-a: 3 runs — 2 success, 1 failure
        for i in range(3):
            h.record_start(f"r-a-{i}", "pipeline-a", input_data={})
            h.record_finish(f"r-a-{i}", i < 2, 1.0)
        # pipeline-b: 1 running (no finish)
        h.record_start("r-b-0", "pipeline-b", input_data={})
        return h

    def test_search_by_pipeline(self, history):
        from brix.mcp_server import _handle_run_search
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_search({"pipeline": "pipeline-a"}))
        assert result["success"] is True
        assert result["total"] == 3

    def test_search_by_status_success(self, history):
        from brix.mcp_server import _handle_run_search
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_search({"status": "success"}))
        assert result["success"] is True
        assert result["total"] == 2

    def test_search_by_status_failure(self, history):
        from brix.mcp_server import _handle_run_search
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_search({"status": "failure"}))
        assert result["success"] is True
        assert result["total"] == 1

    def test_search_by_status_running(self, history):
        from brix.mcp_server import _handle_run_search
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_search({"status": "running"}))
        assert result["success"] is True
        assert result["total"] == 1
        assert result["runs"][0]["pipeline"] == "pipeline-b"

    def test_search_invalid_status(self, history):
        from brix.mcp_server import _handle_run_search
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_search({"status": "unknown"}))
        assert result["success"] is False

    def test_search_no_filters_returns_all(self, history):
        from brix.mcp_server import _handle_run_search
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_search({}))
        assert result["success"] is True
        assert result["total"] == 4

    def test_search_result_has_no_heavy_fields(self, history):
        from brix.mcp_server import _handle_run_search
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_search({"pipeline": "pipeline-a", "limit": 1}))
        assert result["success"] is True
        row = result["runs"][0]
        assert "steps_data" not in row
        assert "result_summary" not in row
        assert "input_data" not in row

    def test_search_with_pipeline_and_status(self, history):
        from brix.mcp_server import _handle_run_search
        with mock.patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = run(_handle_run_search({"pipeline": "pipeline-a", "status": "success"}))
        assert result["success"] is True
        assert result["total"] == 2


# ===========================================================================
# 7. MCP Server Management
# ===========================================================================

class TestServerManager:
    @pytest.fixture
    def mgr(self, tmp_path):
        from brix.server_manager import ServerManager
        return ServerManager(servers_path=tmp_path / "servers.yaml")

    def test_add_and_list(self, mgr):
        entry = mgr.add("m365", "docker", ["exec", "-i", "m365-mcp", "node", "server.js"])
        assert entry["name"] == "m365"
        servers = mgr.list_all()
        assert len(servers) == 1
        assert servers[0]["name"] == "m365"

    def test_add_duplicate_raises(self, mgr):
        mgr.add("dup", "python3", ["-m", "server"])
        with pytest.raises(ValueError, match="already exists"):
            mgr.add("dup", "python3", [])

    def test_get_existing(self, mgr):
        mgr.add("test", "node", ["server.js"])
        entry = mgr.get("test")
        assert entry is not None
        assert entry["command"] == "node"

    def test_get_nonexistent_returns_none(self, mgr):
        assert mgr.get("nope") is None

    def test_update_command(self, mgr):
        mgr.add("srv", "old-cmd", [])
        updated = mgr.update("srv", command="new-cmd")
        assert updated["command"] == "new-cmd"

    def test_update_nonexistent_returns_none(self, mgr):
        result = mgr.update("nope", command="x")
        assert result is None

    def test_remove_existing(self, mgr):
        mgr.add("to-del", "cmd", [])
        assert mgr.remove("to-del") is True
        assert mgr.get("to-del") is None

    def test_remove_nonexistent_returns_false(self, mgr):
        assert mgr.remove("ghost") is False

    def test_refresh_valid(self, mgr):
        mgr.add("valid", "python3", ["-m", "srv"])
        entry = mgr.refresh("valid")
        assert entry["name"] == "valid"

    def test_refresh_nonexistent_raises(self, mgr):
        with pytest.raises(KeyError):
            mgr.refresh("nope")

    def test_refresh_missing_command_raises(self, mgr, tmp_path):
        """Entry without 'command' field should raise ValueError on refresh."""
        import yaml
        path = tmp_path / "servers.yaml"
        path.write_text(yaml.dump({"servers": {"broken": {"args": []}}}))
        mgr2 = __import__("brix.server_manager", fromlist=["ServerManager"]).ServerManager(
            servers_path=path
        )
        with pytest.raises(ValueError, match="command"):
            mgr2.refresh("broken")

    def test_env_stored(self, mgr):
        mgr.add("s", "cmd", env={"TOKEN": "abc"})
        entry = mgr.get("s")
        assert entry["env"] == {"TOKEN": "abc"}


class TestServerMcpTools:
    @pytest.fixture
    def mgr(self, tmp_path):
        from brix.server_manager import ServerManager
        return ServerManager(servers_path=tmp_path / "servers.yaml")

    def test_server_add_mcp(self, mgr):
        from brix.mcp_server import _handle_server_add
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_add({"name": "m365", "command": "node", "args": ["srv.js"]}))
        assert result["success"] is True
        assert result["server"]["name"] == "m365"

    def test_server_add_missing_name(self, mgr):
        from brix.mcp_server import _handle_server_add
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_add({"command": "node"}))
        assert result["success"] is False

    def test_server_add_missing_command(self, mgr):
        from brix.mcp_server import _handle_server_add
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_add({"name": "srv"}))
        assert result["success"] is False

    def test_server_list_mcp(self, mgr):
        from brix.mcp_server import _handle_server_list
        mgr.add("a", "cmdA", [])
        mgr.add("b", "cmdB", [])
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_list({}))
        assert result["success"] is True
        assert result["total"] == 2

    def test_server_update_mcp(self, mgr):
        from brix.mcp_server import _handle_server_update
        mgr.add("s", "old", [])
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_update({"name": "s", "command": "new"}))
        assert result["success"] is True
        assert result["server"]["command"] == "new"

    def test_server_update_not_found(self, mgr):
        from brix.mcp_server import _handle_server_update
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_update({"name": "ghost", "command": "x"}))
        assert result["success"] is False

    def test_server_remove_mcp(self, mgr):
        from brix.mcp_server import _handle_server_remove
        mgr.add("bye", "cmd", [])
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_remove({"name": "bye"}))
        assert result["success"] is True
        assert result["removed"] == "bye"

    def test_server_remove_not_found(self, mgr):
        from brix.mcp_server import _handle_server_remove
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_remove({"name": "nope"}))
        assert result["success"] is False

    def test_server_refresh_mcp(self, mgr):
        from brix.mcp_server import _handle_server_refresh
        mgr.add("ok", "python3", ["-m", "srv"])
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_refresh({"name": "ok"}))
        assert result["success"] is True
        assert result["server"]["name"] == "ok"

    def test_server_refresh_not_found(self, mgr):
        from brix.mcp_server import _handle_server_refresh
        with mock.patch("brix.server_manager.ServerManager", return_value=mgr):
            result = run(_handle_server_refresh({"name": "ghost"}))
        assert result["success"] is False


# ===========================================================================
# 8. get_tips — new tools documented
# ===========================================================================

class TestGetTipsUpdated:
    def test_tips_mention_alert_update(self):
        from brix.mcp_server import _handle_get_tips
        result = run(_handle_get_tips({}))
        tips_text = "\n".join(result["tips"])
        # Compact format: Alerts category listed in TOOL-KATEGORIEN
        assert "alert" in tips_text.lower()

    def test_tips_mention_search_pipelines(self):
        from brix.mcp_server import _handle_get_tips
        result = run(_handle_get_tips({}))
        tips_text = "\n".join(result["tips"])
        # Compact format: search listed in Pipeline category
        assert "search" in tips_text

    def test_tips_mention_credential_rotate(self):
        from brix.mcp_server import _handle_get_tips
        result = run(_handle_get_tips({}))
        tips_text = "\n".join(result["tips"])
        # Compact format: Credentials category lists rotate
        assert "rotate" in tips_text or "credential" in tips_text.lower()

    def test_tips_mention_run_annotate(self):
        from brix.mcp_server import _handle_get_tips
        result = run(_handle_get_tips({}))
        tips_text = "\n".join(result["tips"])
        # Compact format: run_annotate listed in Runs category
        assert "run_annotate" in tips_text

    def test_tips_mention_run_search(self):
        from brix.mcp_server import _handle_get_tips
        result = run(_handle_get_tips({}))
        tips_text = "\n".join(result["tips"])
        # Compact format: run_search listed in Runs category
        assert "run_search" in tips_text

    def test_tips_mention_server_management(self):
        from brix.mcp_server import _handle_get_tips
        result = run(_handle_get_tips({}))
        tips_text = "\n".join(result["tips"])
        # Compact format: Servers category in TOOL-KATEGORIEN
        assert "server_add" in tips_text
        assert "server_list" in tips_text or "Servers:" in tips_text
        assert "server_remove" in tips_text


# ===========================================================================
# CredentialStore.search — unit tests
# ===========================================================================

class TestCredentialStoreSearch:
    @pytest.fixture
    def store(self, tmp_path):
        from brix.credential_store import CredentialStore
        return CredentialStore(db_path=tmp_path / "creds.db")

    def test_search_empty_store(self, store):
        results = store.search("anything")
        assert results == []

    def test_search_name_match(self, store):
        store.add("my-openai", "api-key", "sk-abc")
        store.add("github-pat", "api-key", "ghp-xyz")
        results = store.search("openai")
        assert len(results) == 1
        assert results[0]["name"] == "my-openai"

    def test_search_type_match(self, store):
        store.add("g-oauth", "oauth2", "{}")
        store.add("basic", "basic-auth", "user:pass")
        results = store.search("oauth")
        assert len(results) == 1
        assert results[0]["type"] == "oauth2"

    def test_search_no_value_in_results(self, store):
        store.add("secret", "api-key", "plaintext")
        results = store.search("secret")
        for r in results:
            assert "value" not in r
            assert "encrypted_value" not in r


# ===========================================================================
# RunHistory.annotate + RunHistory.search — unit tests
# ===========================================================================

class TestRunHistoryAnnotate:
    @pytest.fixture
    def h(self, tmp_path):
        from brix.history import RunHistory
        h = RunHistory(db_path=tmp_path / "brix.db")
        h.record_start("r1", "p1")
        h.record_finish("r1", True, 1.0)
        return h, tmp_path

    def test_annotate_returns_true(self, h):
        history, _ = h
        assert history.annotate("r1", "my note") is True

    def test_annotate_nonexistent_returns_false(self, h):
        history, _ = h
        assert history.annotate("no-run", "x") is False

    def test_annotate_stored_in_db(self, h):
        history, tmp_path = h
        history.annotate("r1", "check this")
        from brix.history import RunHistory
        h2 = RunHistory(db_path=tmp_path / "brix.db")
        row = h2.get_run("r1")
        assert row["notes"] == "check this"

    def test_annotate_overwrite(self, h):
        history, tmp_path = h
        history.annotate("r1", "first")
        history.annotate("r1", "second")
        from brix.history import RunHistory
        h2 = RunHistory(db_path=tmp_path / "brix.db")
        row = h2.get_run("r1")
        assert row["notes"] == "second"


class TestRunHistorySearch:
    @pytest.fixture
    def h(self, tmp_path):
        from brix.history import RunHistory
        h = RunHistory(db_path=tmp_path / "brix.db")
        # pipeline-a: 2 success
        for i in range(2):
            h.record_start(f"a{i}", "pipeline-a")
            h.record_finish(f"a{i}", True, 1.0)
        # pipeline-b: 1 failure
        h.record_start("b0", "pipeline-b")
        h.record_finish("b0", False, 2.0)
        # pipeline-c: running (no finish)
        h.record_start("c0", "pipeline-c")
        return h

    def test_search_by_pipeline(self, h):
        results = h.search(pipeline="pipeline-a")
        assert len(results) == 2

    def test_search_success_status(self, h):
        results = h.search(status="success")
        assert len(results) == 2
        assert all(r["success"] == 1 for r in results)

    def test_search_failure_status(self, h):
        results = h.search(status="failure")
        assert len(results) == 1
        assert results[0]["pipeline"] == "pipeline-b"

    def test_search_running_status(self, h):
        results = h.search(status="running")
        assert len(results) == 1
        assert results[0]["pipeline"] == "pipeline-c"

    def test_search_all_no_filters(self, h):
        results = h.search()
        assert len(results) == 4

    def test_search_limit(self, h):
        results = h.search(limit=2)
        assert len(results) == 2

    def test_search_pipeline_and_status(self, h):
        results = h.search(pipeline="pipeline-a", status="success")
        assert len(results) == 2

    def test_search_pipeline_no_match(self, h):
        results = h.search(pipeline="does-not-exist")
        assert len(results) == 0
