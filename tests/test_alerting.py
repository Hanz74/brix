"""Tests for T-BRIX-V5-08: Monitoring/Alerting.

Covers:
1. AlertRule dataclass
2. AlertManager — schema creation
3. AlertManager — add_rule / list_rules / get_rule / update_rule / delete_rule
4. AlertManager — get_alert_history + _record_alert
5. check_alerts — condition: pipeline_failed
6. check_alerts — condition: pipeline_failed_consecutive:N
7. check_alerts — condition: dependency_missing
8. check_alerts — enabled=False rule is skipped
9. check_alerts — channel: log writes to stderr and records history
10. check_alerts — channel: mattermost calls webhook
11. Validation — unknown condition raises ValueError
12. Validation — unknown channel raises ValueError
13. MCP tools — brix__alert_add, brix__alert_list, brix__alert_delete, brix__alert_history
14. CLI — brix alerts list / add / delete
15. Engine integration — check_alerts called after run
"""
import asyncio
import json
import sys
import sqlite3
import unittest.mock as mock
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path):
    """AlertManager backed by a temp database."""
    from brix.alerting import AlertManager
    return AlertManager(db_path=tmp_path / "brix.db")


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    """Redirect Path.home() so all brix dirs land in temp."""
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# 1. AlertRule dataclass
# ---------------------------------------------------------------------------

class TestAlertRuleDataclass:
    def test_fields(self):
        from brix.alerting import AlertRule
        r = AlertRule(
            id="abc",
            name="test",
            condition="pipeline_failed",
            channel="log",
            config={},
            enabled=True,
            created_at="2026-01-01T00:00:00+00:00",
        )
        assert r.id == "abc"
        assert r.name == "test"
        assert r.condition == "pipeline_failed"
        assert r.channel == "log"
        assert r.config == {}
        assert r.enabled is True


# ---------------------------------------------------------------------------
# 2. Schema creation
# ---------------------------------------------------------------------------

class TestSchema:
    def test_tables_created(self, tmp_db):
        with tmp_db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "alert_rules" in tables
        assert "alert_history" in tables

    def test_db_path_parent_created(self, tmp_path):
        from brix.alerting import AlertManager
        nested = tmp_path / "a" / "b" / "brix.db"
        AlertManager(db_path=nested)
        assert nested.exists()


# ---------------------------------------------------------------------------
# 3. CRUD
# ---------------------------------------------------------------------------

class TestRuleCRUD:
    def test_add_returns_rule(self, tmp_db):
        rule = tmp_db.add_rule("my rule", "pipeline_failed", "log")
        assert rule.id
        assert rule.name == "my rule"
        assert rule.condition == "pipeline_failed"
        assert rule.channel == "log"
        assert rule.enabled is True
        assert rule.created_at

    def test_list_empty(self, tmp_db):
        assert tmp_db.list_rules() == []

    def test_list_after_add(self, tmp_db):
        tmp_db.add_rule("r1", "pipeline_failed", "log")
        tmp_db.add_rule("r2", "dependency_missing", "log")
        rules = tmp_db.list_rules()
        assert len(rules) == 2

    def test_get_rule_by_id(self, tmp_db):
        rule = tmp_db.add_rule("r", "pipeline_failed", "log")
        fetched = tmp_db.get_rule(rule.id)
        assert fetched is not None
        assert fetched.id == rule.id
        assert fetched.name == "r"

    def test_get_rule_missing(self, tmp_db):
        assert tmp_db.get_rule("nonexistent-uuid") is None

    def test_update_rule_name(self, tmp_db):
        rule = tmp_db.add_rule("old name", "pipeline_failed", "log")
        updated = tmp_db.update_rule(rule.id, name="new name")
        assert updated is not None
        assert updated.name == "new name"
        assert updated.condition == "pipeline_failed"

    def test_update_rule_enabled(self, tmp_db):
        rule = tmp_db.add_rule("r", "pipeline_failed", "log")
        updated = tmp_db.update_rule(rule.id, enabled=False)
        assert updated is not None
        assert updated.enabled is False

    def test_update_rule_missing_returns_none(self, tmp_db):
        assert tmp_db.update_rule("nonexistent", name="x") is None

    def test_delete_rule(self, tmp_db):
        rule = tmp_db.add_rule("r", "pipeline_failed", "log")
        assert tmp_db.delete_rule(rule.id) is True
        assert tmp_db.get_rule(rule.id) is None

    def test_delete_missing_returns_false(self, tmp_db):
        assert tmp_db.delete_rule("nonexistent") is False

    def test_add_rule_with_config(self, tmp_db):
        rule = tmp_db.add_rule(
            "mm rule",
            "pipeline_failed",
            "mattermost",
            config={"webhook_url": "https://example.com/hook"},
        )
        fetched = tmp_db.get_rule(rule.id)
        assert fetched.config == {"webhook_url": "https://example.com/hook"}


# ---------------------------------------------------------------------------
# 4. Alert history
# ---------------------------------------------------------------------------

class TestAlertHistory:
    def test_history_empty(self, tmp_db):
        assert tmp_db.get_alert_history() == []

    def test_history_after_alert(self, tmp_db):
        rule = tmp_db.add_rule("r", "pipeline_failed", "log")
        tmp_db._record_alert(rule, "test message", pipeline="my-pipe", run_id="run-1")
        history = tmp_db.get_alert_history()
        assert len(history) == 1
        assert history[0]["rule_id"] == rule.id
        assert history[0]["rule_name"] == "r"
        assert history[0]["pipeline"] == "my-pipe"
        assert history[0]["run_id"] == "run-1"
        assert history[0]["message"] == "test message"
        assert "fired_at" in history[0]

    def test_history_limit(self, tmp_db):
        rule = tmp_db.add_rule("r", "pipeline_failed", "log")
        for i in range(5):
            tmp_db._record_alert(rule, f"msg {i}")
        history = tmp_db.get_alert_history(limit=3)
        assert len(history) == 3

    def test_history_newest_first(self, tmp_db):
        rule = tmp_db.add_rule("r", "pipeline_failed", "log")
        tmp_db._record_alert(rule, "first")
        tmp_db._record_alert(rule, "second")
        history = tmp_db.get_alert_history()
        # Newest first
        assert history[0]["message"] == "second"
        assert history[1]["message"] == "first"


# ---------------------------------------------------------------------------
# 5. check_alerts — pipeline_failed
# ---------------------------------------------------------------------------

class TestCheckAlertsPipelineFailed:
    def _run_result(self, success, pipeline="test-pipe", run_id="run-1"):
        return {"success": success, "run_id": run_id, "pipeline": pipeline, "steps": {}}

    def test_fires_on_failure(self, tmp_db):
        tmp_db.add_rule("fail alert", "pipeline_failed", "log")
        fired = tmp_db.check_alerts(self._run_result(False))
        assert len(fired) == 1
        assert fired[0]["condition"] == "pipeline_failed"

    def test_no_fire_on_success(self, tmp_db):
        tmp_db.add_rule("fail alert", "pipeline_failed", "log")
        fired = tmp_db.check_alerts(self._run_result(True))
        assert fired == []

    def test_no_fire_when_no_rules(self, tmp_db):
        fired = tmp_db.check_alerts(self._run_result(False))
        assert fired == []

    def test_fired_alert_in_history(self, tmp_db):
        tmp_db.add_rule("fail alert", "pipeline_failed", "log")
        tmp_db.check_alerts(self._run_result(False))
        history = tmp_db.get_alert_history()
        assert len(history) == 1

    def test_disabled_rule_not_fired(self, tmp_db):
        rule = tmp_db.add_rule("fail alert", "pipeline_failed", "log")
        tmp_db.update_rule(rule.id, enabled=False)
        fired = tmp_db.check_alerts(self._run_result(False))
        assert fired == []


# ---------------------------------------------------------------------------
# 6. check_alerts — pipeline_failed_consecutive:N
# ---------------------------------------------------------------------------

class TestCheckAlertsConsecutive:
    def _seed_runs(self, tmp_db, successes: list[bool], pipeline: str = "my-pipe"):
        """Manually insert run records into the shared DB."""
        from brix.db import _now_iso
        with tmp_db._connect() as conn:
            # Ensure runs table exists (BrixDB DDL)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline TEXT NOT NULL,
                    version TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration REAL,
                    success INTEGER,
                    input_data TEXT,
                    steps_data TEXT,
                    result_summary TEXT,
                    triggered_by TEXT DEFAULT 'cli'
                )
            """)
            for i, s in enumerate(successes):
                conn.execute(
                    "INSERT OR REPLACE INTO runs (run_id, pipeline, started_at, success) VALUES (?,?,?,?)",
                    (f"run-{i}", pipeline, f"2026-01-01T00:00:0{i}+00:00", int(s)),
                )

    def test_fires_when_n_consecutive_failures(self, tmp_db):
        self._seed_runs(tmp_db, [False, False, False], pipeline="bad-pipe")
        tmp_db.add_rule("consec", "pipeline_failed_consecutive:3", "log")
        fired = tmp_db.check_alerts(
            {"success": False, "run_id": "run-3", "pipeline": "bad-pipe", "steps": {}}
        )
        assert len(fired) == 1

    def test_no_fire_when_below_threshold(self, tmp_db):
        self._seed_runs(tmp_db, [False, False], pipeline="bad-pipe")
        tmp_db.add_rule("consec", "pipeline_failed_consecutive:3", "log")
        fired = tmp_db.check_alerts(
            {"success": False, "run_id": "run-2", "pipeline": "bad-pipe", "steps": {}}
        )
        # Only 2 failures in history — consecutive count is 2 < 3
        assert fired == []

    def test_no_fire_on_success(self, tmp_db):
        self._seed_runs(tmp_db, [False, False, False], pipeline="bad-pipe")
        tmp_db.add_rule("consec", "pipeline_failed_consecutive:3", "log")
        fired = tmp_db.check_alerts(
            {"success": True, "run_id": "run-3", "pipeline": "bad-pipe", "steps": {}}
        )
        assert fired == []


# ---------------------------------------------------------------------------
# 7. check_alerts — dependency_missing
# ---------------------------------------------------------------------------

class TestCheckAlertsDependencyMissing:
    def test_fires_on_missing_dependency_error(self, tmp_db):
        tmp_db.add_rule("dep alert", "dependency_missing", "log")
        steps = {
            "step1": {"status": "error", "error_message": "Failed to install missing requirement httpx"}
        }
        fired = tmp_db.check_alerts(
            {"success": False, "run_id": "run-1", "pipeline": "p", "steps": steps}
        )
        assert len(fired) == 1

    def test_no_fire_when_no_dep_error(self, tmp_db):
        tmp_db.add_rule("dep alert", "dependency_missing", "log")
        steps = {
            "step1": {"status": "error", "error_message": "Some unrelated error"}
        }
        fired = tmp_db.check_alerts(
            {"success": False, "run_id": "run-1", "pipeline": "p", "steps": steps}
        )
        assert fired == []


# ---------------------------------------------------------------------------
# 8. Channel: log writes to stderr
# ---------------------------------------------------------------------------

class TestChannelLog:
    def test_log_writes_to_stderr(self, tmp_db, capsys):
        tmp_db.add_rule("r", "pipeline_failed", "log")
        tmp_db.check_alerts({"success": False, "run_id": "r1", "pipeline": "p", "steps": {}})
        captured = capsys.readouterr()
        assert "BRIX ALERT" in captured.err

    def test_log_records_history(self, tmp_db):
        tmp_db.add_rule("r", "pipeline_failed", "log")
        tmp_db.check_alerts({"success": False, "run_id": "r1", "pipeline": "p", "steps": {}})
        history = tmp_db.get_alert_history()
        assert len(history) == 1
        assert history[0]["channel"] == "log"


# ---------------------------------------------------------------------------
# 9. Channel: mattermost calls webhook
# ---------------------------------------------------------------------------

class TestChannelMattermost:
    def test_mattermost_posts_to_webhook(self, tmp_db):
        tmp_db.add_rule(
            "mm",
            "pipeline_failed",
            "mattermost",
            config={"webhook_url": "https://hooks.example.com/test"},
        )
        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = lambda s: s
            mock_urlopen.return_value.__exit__ = mock.Mock(return_value=False)
            fired = tmp_db.check_alerts(
                {"success": False, "run_id": "r1", "pipeline": "p", "steps": {}}
            )
        assert len(fired) == 1
        assert mock_urlopen.called

    def test_mattermost_missing_webhook_url_logs_warning(self, tmp_db, capsys):
        tmp_db.add_rule("mm", "pipeline_failed", "mattermost", config={})
        tmp_db.check_alerts({"success": False, "run_id": "r1", "pipeline": "p", "steps": {}})
        captured = capsys.readouterr()
        assert "webhook_url" in captured.err


# ---------------------------------------------------------------------------
# 10. Validation errors
# ---------------------------------------------------------------------------

class TestValidation:
    def test_invalid_condition_raises(self, tmp_db):
        with pytest.raises(ValueError, match="Unknown alert condition"):
            tmp_db.add_rule("r", "nonexistent_condition", "log")

    def test_invalid_channel_raises(self, tmp_db):
        with pytest.raises(ValueError, match="Unknown alert channel"):
            tmp_db.add_rule("r", "pipeline_failed", "email")

    def test_consecutive_condition_valid(self, tmp_db):
        rule = tmp_db.add_rule("r", "pipeline_failed_consecutive:5", "log")
        assert rule.condition == "pipeline_failed_consecutive:5"

    def test_run_hung_condition_valid(self, tmp_db):
        rule = tmp_db.add_rule("r", "run_hung", "log")
        assert rule.condition == "run_hung"


# ---------------------------------------------------------------------------
# 11. MCP tools
# ---------------------------------------------------------------------------

class TestMCPTools:
    @pytest.fixture(autouse=True)
    def patch_alert_manager(self, tmp_path, monkeypatch):
        """Redirect AlertManager to use a temp DB."""
        from brix.alerting import AlertManager as _AM
        original_init = _AM.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=tmp_path / "brix.db")

        monkeypatch.setattr(_AM, "__init__", patched_init)

    def test_alert_add_success(self):
        from brix.mcp_server import _handle_alert_add
        result = asyncio.run(_handle_alert_add({
            "name": "test alert",
            "condition": "pipeline_failed",
            "channel": "log",
        }))
        assert result["success"] is True
        assert "rule" in result
        assert result["rule"]["name"] == "test alert"
        assert result["rule"]["id"]

    def test_alert_add_missing_name(self):
        from brix.mcp_server import _handle_alert_add
        result = asyncio.run(_handle_alert_add({
            "condition": "pipeline_failed",
            "channel": "log",
        }))
        assert result["success"] is False
        assert "name" in result["error"].lower()

    def test_alert_add_invalid_condition(self):
        from brix.mcp_server import _handle_alert_add
        result = asyncio.run(_handle_alert_add({
            "name": "x",
            "condition": "bad_condition",
            "channel": "log",
        }))
        assert result["success"] is False

    def test_alert_list_empty(self):
        from brix.mcp_server import _handle_alert_list
        result = asyncio.run(_handle_alert_list({}))
        assert result["rules"] == []
        assert result["total"] == 0

    def test_alert_list_after_add(self):
        from brix.mcp_server import _handle_alert_add, _handle_alert_list
        asyncio.run(_handle_alert_add({
            "name": "r1",
            "condition": "pipeline_failed",
            "channel": "log",
        }))
        result = asyncio.run(_handle_alert_list({}))
        assert result["total"] == 1
        assert result["rules"][0]["name"] == "r1"

    def test_alert_delete_success(self):
        from brix.mcp_server import _handle_alert_add, _handle_alert_delete
        add_result = asyncio.run(_handle_alert_add({
            "name": "r",
            "condition": "pipeline_failed",
            "channel": "log",
        }))
        rule_id = add_result["rule"]["id"]
        del_result = asyncio.run(_handle_alert_delete({"id": rule_id}))
        assert del_result["success"] is True
        assert del_result["id"] == rule_id

    def test_alert_delete_missing(self):
        from brix.mcp_server import _handle_alert_delete
        result = asyncio.run(_handle_alert_delete({"id": "nonexistent-uuid"}))
        assert result["success"] is False

    def test_alert_delete_missing_id_param(self):
        from brix.mcp_server import _handle_alert_delete
        result = asyncio.run(_handle_alert_delete({}))
        assert result["success"] is False

    def test_alert_history_empty(self):
        from brix.mcp_server import _handle_alert_history
        result = asyncio.run(_handle_alert_history({}))
        assert result["history"] == []
        assert result["total"] == 0

    def test_alert_history_after_fire(self):
        from brix.mcp_server import _handle_alert_add, _handle_alert_history
        from brix.alerting import AlertManager
        asyncio.run(_handle_alert_add({
            "name": "fail alert",
            "condition": "pipeline_failed",
            "channel": "log",
        }))
        # Fire the alert directly via AlertManager
        mgr = AlertManager()
        mgr.check_alerts({"success": False, "run_id": "r1", "pipeline": "p", "steps": {}})
        result = asyncio.run(_handle_alert_history({"limit": 10}))
        assert result["total"] >= 1

    def test_alert_history_limit(self):
        from brix.mcp_server import _handle_alert_history
        result = asyncio.run(_handle_alert_history({"limit": 5}))
        assert isinstance(result["history"], list)


# ---------------------------------------------------------------------------
# 12. CLI
# ---------------------------------------------------------------------------

class TestCLI:
    @pytest.fixture(autouse=True)
    def patch_alert_manager(self, tmp_path, monkeypatch):
        from brix.alerting import AlertManager as _AM
        original_init = _AM.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=tmp_path / "brix.db")

        monkeypatch.setattr(_AM, "__init__", patched_init)

    def test_alerts_list_empty(self):
        from click.testing import CliRunner
        from brix.cli import main
        runner = CliRunner()
        result = runner.invoke(main, ["alerts", "list"])
        assert result.exit_code == 0

    def test_alerts_add(self):
        from click.testing import CliRunner
        from brix.cli import main
        runner = CliRunner()
        result = runner.invoke(main, [
            "alerts", "add",
            "--name", "my alert",
            "--condition", "pipeline_failed",
            "--channel", "log",
        ])
        assert result.exit_code == 0
        assert "added" in result.output.lower() or "added" in (result.output + "").lower()

    def test_alerts_add_and_list(self):
        from click.testing import CliRunner
        from brix.cli import main
        runner = CliRunner()
        runner.invoke(main, [
            "alerts", "add",
            "--name", "test rule",
            "--condition", "pipeline_failed",
            "--channel", "log",
        ])
        result = runner.invoke(main, ["alerts", "list"])
        assert result.exit_code == 0

    def test_alerts_delete(self):
        from click.testing import CliRunner
        from brix.cli import main
        from brix.alerting import AlertManager
        mgr = AlertManager()
        rule = mgr.add_rule("x", "pipeline_failed", "log")
        runner = CliRunner()
        result = runner.invoke(main, ["alerts", "delete", rule.id])
        assert result.exit_code == 0

    def test_alerts_delete_missing(self):
        from click.testing import CliRunner
        from brix.cli import main
        runner = CliRunner()
        result = runner.invoke(main, ["alerts", "delete", "nonexistent-uuid"])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# 13. Engine integration
# ---------------------------------------------------------------------------

class TestEngineIntegration:
    def _make_pipeline(self, name: str):
        from brix.loader import PipelineLoader
        return PipelineLoader().load_from_string(f"""
name: {name}
steps:
  - id: noop
    type: cli
    args: ["true"]
""")

    def test_check_alerts_called_after_run(self, tmp_path, monkeypatch):
        """AlertManager.check_alerts is called after every engine run."""
        called_with = []

        from brix.alerting import AlertManager as _AM

        def mock_check(self, run_result):
            called_with.append(run_result)
            return []

        monkeypatch.setattr(_AM, "check_alerts", mock_check)

        from brix.engine import PipelineEngine

        pipeline = self._make_pipeline("test-pipe")
        engine = PipelineEngine()
        asyncio.run(engine.run(pipeline, {}))

        assert len(called_with) >= 1
        result = called_with[0]
        assert "pipeline" in result
        assert result["pipeline"] == "test-pipe"

    def test_alerting_errors_dont_fail_run(self, tmp_path, monkeypatch):
        """Alerting exceptions must never propagate to the caller."""
        from brix.alerting import AlertManager as _AM

        def broken_check(self, run_result):
            raise RuntimeError("alerting broken!")

        monkeypatch.setattr(_AM, "check_alerts", broken_check)

        from brix.engine import PipelineEngine

        pipeline = self._make_pipeline("error-pipe")
        engine = PipelineEngine()
        result = asyncio.run(engine.run(pipeline, {}))
        # Run should succeed even with broken alerting
        assert result.success is True


# ---------------------------------------------------------------------------
# T-BRIX-V6-25: mcp_server_down condition
# ---------------------------------------------------------------------------

class TestMcpServerDownCondition:
    """Tests for the 'mcp_server_down:N' alert condition."""

    def test_condition_accepted_by_add_rule(self, tmp_db):
        """mcp_server_down:N is a valid condition string for add_rule."""
        rule = tmp_db.add_rule(
            "server monitor",
            "mcp_server_down:5",
            "log",
            config={"server_name": "m365"},
        )
        assert rule.condition == "mcp_server_down:5"
        assert rule.config["server_name"] == "m365"

    def test_invalid_condition_still_rejected(self, tmp_db):
        """Completely unknown conditions are still rejected."""
        with pytest.raises(ValueError, match="Unknown alert condition"):
            tmp_db.add_rule("r", "totally_unknown", "log")

    def test_condition_does_not_fire_when_no_active_pool(self, tmp_db, monkeypatch):
        """mcp_server_down returns False when _active_pool is None (no pool active)."""
        import brix.context as ctx
        monkeypatch.setattr(ctx, "_active_pool", None, raising=False)

        run_result = {"success": True, "pipeline": "my-pipe", "run_id": "r1"}
        tmp_db.add_rule("mon", "mcp_server_down:1", "log", config={"server_name": "m365"})
        # Should not fire — no active pool
        fired = tmp_db.check_alerts(run_result)
        assert fired == []

    def test_condition_does_not_fire_when_pool_is_none(self, tmp_db, monkeypatch):
        """mcp_server_down returns False when _active_pool is None."""
        import brix.context as ctx
        monkeypatch.setattr(ctx, "_active_pool", None, raising=False)

        run_result = {"success": True, "pipeline": "my-pipe", "run_id": "r1"}
        tmp_db.add_rule("mon", "mcp_server_down:1", "log", config={"server_name": "m365"})
        fired = tmp_db.check_alerts(run_result)
        assert fired == []

    def test_condition_does_not_fire_when_server_contacted_recently(self, tmp_db, monkeypatch):
        """mcp_server_down does NOT fire when server was recently contacted."""
        from datetime import datetime, timezone, timedelta

        recent_ts = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        fake_health = {
            "m365": {
                "last_contact_at": recent_ts,
                "avg_latency_ms": 50.0,
                "call_count": 3,
                "error_count": 0,
            }
        }

        fake_pool = mock.MagicMock()
        fake_pool.get_health.return_value = fake_health

        import brix.context as ctx
        monkeypatch.setattr(ctx, "_active_pool", fake_pool, raising=False)

        run_result = {"success": True, "pipeline": "my-pipe", "run_id": "r1"}
        # Threshold: 5 minutes — server was contacted 30s ago → should NOT fire
        tmp_db.add_rule("mon", "mcp_server_down:5", "log", config={"server_name": "m365"})
        fired = tmp_db.check_alerts(run_result)
        assert fired == []

    def test_condition_fires_when_server_last_contacted_too_long_ago(self, tmp_db, monkeypatch):
        """mcp_server_down fires when last_contact_at exceeds threshold."""
        from datetime import datetime, timezone, timedelta

        stale_ts = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        fake_health = {
            "m365": {
                "last_contact_at": stale_ts,
                "avg_latency_ms": 200.0,
                "call_count": 5,
                "error_count": 0,
            }
        }

        fake_pool = mock.MagicMock()
        fake_pool.get_health.return_value = fake_health

        import brix.context as ctx
        monkeypatch.setattr(ctx, "_active_pool", fake_pool, raising=False)

        run_result = {"success": True, "pipeline": "my-pipe", "run_id": "r1"}
        # Threshold: 5 minutes — server was contacted 10 min ago → SHOULD fire
        tmp_db.add_rule("mon", "mcp_server_down:5", "log", config={"server_name": "m365"})
        fired = tmp_db.check_alerts(run_result)
        assert len(fired) == 1
        assert fired[0]["condition"] == "mcp_server_down:5"

    def test_condition_fires_for_any_server_when_no_server_name_configured(self, tmp_db, monkeypatch):
        """When no server_name in config, fires if ANY server is stale."""
        from datetime import datetime, timezone, timedelta

        stale_ts = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
        fake_health = {
            "m365": {
                "last_contact_at": stale_ts,
                "avg_latency_ms": 100.0,
                "call_count": 2,
                "error_count": 0,
            }
        }

        fake_pool = mock.MagicMock()
        fake_pool.get_health.return_value = fake_health

        import brix.context as ctx
        monkeypatch.setattr(ctx, "_active_pool", fake_pool, raising=False)

        run_result = {"success": True, "pipeline": "my-pipe", "run_id": "r1"}
        # No server_name → checks all servers
        tmp_db.add_rule("mon", "mcp_server_down:5", "log", config={})
        fired = tmp_db.check_alerts(run_result)
        assert len(fired) == 1

    def test_condition_does_not_fire_for_unknown_server_name(self, tmp_db, monkeypatch):
        """Condition does NOT fire when the named server has no health data."""
        fake_pool = mock.MagicMock()
        fake_pool.get_health.return_value = {}  # no servers contacted yet

        import brix.context as ctx
        monkeypatch.setattr(ctx, "_active_pool", fake_pool, raising=False)

        run_result = {"success": True, "pipeline": "my-pipe", "run_id": "r1"}
        tmp_db.add_rule("mon", "mcp_server_down:1", "log", config={"server_name": "unknown-svc"})
        fired = tmp_db.check_alerts(run_result)
        assert fired == []

    def test_mcp_server_down_condition_in_get_tips_text(self):
        """The new condition appears in validation error messages for guidance."""
        from brix.alerting import AlertManager
        am = AlertManager.__new__(AlertManager)  # skip __init__
        try:
            am._validate_condition("bad_condition")
        except ValueError as exc:
            assert "mcp_server_down" in str(exc)
