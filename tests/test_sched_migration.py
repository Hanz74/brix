"""Tests for T-BRIX-SCHED-02: schedules.yaml -> DB trigger migration."""
import json
import pytest
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock

from brix.mcp_handlers.triggers import interval_to_cron, migrate_schedules_yaml


# ---------------------------------------------------------------------------
# interval_to_cron
# ---------------------------------------------------------------------------


class TestIntervalToCron:
    def test_hours(self):
        assert interval_to_cron("6h") == "0 */6 * * *"

    def test_single_hour(self):
        assert interval_to_cron("1h") == "0 */1 * * *"

    def test_minutes(self):
        assert interval_to_cron("30m") == "*/30 * * * *"

    def test_daily_keyword(self):
        assert interval_to_cron("daily") == "0 0 * * *"

    def test_hourly_keyword(self):
        assert interval_to_cron("hourly") == "0 * * * *"

    def test_days(self):
        assert interval_to_cron("2d") == "0 0 * * *"

    def test_24h(self):
        assert interval_to_cron("24h") == "0 0 * * *"

    def test_large_minutes(self):
        # 120m = 2h
        assert interval_to_cron("120m") == "0 */2 * * *"

    def test_invalid_fallback(self):
        assert interval_to_cron("bogus") == "0 0 * * *"

    def test_whitespace_handling(self):
        assert interval_to_cron("  6h  ") == "0 */6 * * *"


# ---------------------------------------------------------------------------
# migrate_schedules_yaml
# ---------------------------------------------------------------------------


SAMPLE_YAML = {
    "schedules": [
        {
            "name": "fints-fetch-6h",
            "pipeline": "buddy-fints-fetch",
            "interval": "6h",
            "enabled": True,
            "project": "buddy",
            "tags": ["intake", "scheduled", "fints"],
            "group": "buddy-fints",
            "description": "Alle 6h: Sparkasse-Konten per FinTS abrufen",
            "params": {"dry_run": False, "start_date": ""},
        }
    ]
}


class TestMigrateSchedulesYaml:
    def test_no_yaml_file(self, tmp_path):
        """If schedules.yaml doesn't exist, returns empty list."""
        with patch("pathlib.Path.home", return_value=tmp_path):
            result = migrate_schedules_yaml()
        assert result == []

    def test_creates_trigger_from_yaml(self, tmp_path):
        """Migration creates a schedule trigger with correct cron expression."""
        schedules_path = tmp_path / ".brix" / "schedules.yaml"
        schedules_path.parent.mkdir(parents=True, exist_ok=True)
        with open(schedules_path, "w") as f:
            yaml.dump(SAMPLE_YAML, f)

        # Mock TriggerStore
        mock_store = MagicMock()
        mock_store.get.return_value = None  # trigger doesn't exist yet
        mock_store.add.return_value = {"name": "fints-fetch-6h", "type": "schedule"}

        with patch("pathlib.Path.home", return_value=tmp_path), \
             patch("brix.triggers.store.TriggerStore", return_value=mock_store):
            results = migrate_schedules_yaml()

        assert len(results) == 1
        assert results[0]["action"] == "created"
        assert results[0]["cron"] == "0 */6 * * *"
        assert results[0]["pipeline"] == "buddy-fints-fetch"

        # Verify store.add was called with correct args
        mock_store.add.assert_called_once()
        call_kwargs = mock_store.add.call_args[1]
        assert call_kwargs["name"] == "fints-fetch-6h"
        assert call_kwargs["type"] == "schedule"
        assert call_kwargs["pipeline"] == "buddy-fints-fetch"
        assert call_kwargs["config"]["cron"] == "0 */6 * * *"
        assert call_kwargs["config"]["params"]["dry_run"] is False
        assert call_kwargs["project"] == "buddy"
        assert call_kwargs["tags"] == ["intake", "scheduled", "fints"]
        assert call_kwargs["group_name"] == "buddy-fints"

        # Description set via update
        mock_store.update.assert_called_once_with(
            "fints-fetch-6h", description="Alle 6h: Sparkasse-Konten per FinTS abrufen"
        )

    def test_skips_existing_trigger(self, tmp_path):
        """If trigger already exists in DB, migration skips it."""
        schedules_path = tmp_path / ".brix" / "schedules.yaml"
        schedules_path.parent.mkdir(parents=True, exist_ok=True)
        with open(schedules_path, "w") as f:
            yaml.dump(SAMPLE_YAML, f)

        mock_store = MagicMock()
        mock_store.get.return_value = {"name": "fints-fetch-6h"}  # already exists

        with patch("pathlib.Path.home", return_value=tmp_path), \
             patch("brix.triggers.store.TriggerStore", return_value=mock_store):
            results = migrate_schedules_yaml()

        assert len(results) == 1
        assert results[0]["action"] == "skipped"
        assert results[0]["reason"] == "already exists"
        mock_store.add.assert_not_called()

    def test_idempotent_multiple_calls(self, tmp_path):
        """Calling migrate twice: first creates, second skips."""
        schedules_path = tmp_path / ".brix" / "schedules.yaml"
        schedules_path.parent.mkdir(parents=True, exist_ok=True)
        with open(schedules_path, "w") as f:
            yaml.dump(SAMPLE_YAML, f)

        mock_store = MagicMock()
        # First call: doesn't exist
        mock_store.get.return_value = None
        mock_store.add.return_value = {"name": "fints-fetch-6h"}

        with patch("pathlib.Path.home", return_value=tmp_path), \
             patch("brix.triggers.store.TriggerStore", return_value=mock_store):
            r1 = migrate_schedules_yaml()

        assert r1[0]["action"] == "created"

        # Second call: now exists
        mock_store.get.return_value = {"name": "fints-fetch-6h"}
        with patch("pathlib.Path.home", return_value=tmp_path), \
             patch("brix.triggers.store.TriggerStore", return_value=mock_store):
            r2 = migrate_schedules_yaml()

        assert r2[0]["action"] == "skipped"


# ---------------------------------------------------------------------------
# TriggerService loads from DB
# ---------------------------------------------------------------------------


class TestTriggerServiceDbLoad:
    def test_loads_from_db(self, tmp_path):
        """TriggerService.load_triggers() loads schedule triggers from DB."""
        from brix.triggers.service import TriggerService

        mock_store = MagicMock()
        mock_store.list_all.return_value = [
            {
                "name": "test-sched",
                "type": "schedule",
                "pipeline": "my-pipeline",
                "enabled": True,
                "config": json.dumps({"cron": "0 */6 * * *", "params": {}}),
            }
        ]

        svc = TriggerService(config_path=tmp_path / "nonexistent.yaml")
        with patch("brix.triggers.store.TriggerStore", return_value=mock_store):
            svc.load_triggers()

        assert len(svc._triggers) == 1
        assert svc._triggers[0].id == "test-sched"
        assert svc._triggers[0].type == "schedule"
        assert svc._triggers[0].cron == "0 */6 * * *"

    def test_falls_back_to_yaml_if_db_empty(self, tmp_path):
        """If DB has no triggers, falls back to YAML."""
        from brix.triggers.service import TriggerService

        yaml_path = tmp_path / "triggers.yaml"
        yaml_path.write_text(yaml.dump({
            "triggers": [
                {"id": "yaml-trig", "type": "schedule", "pipeline": "p1", "cron": "0 0 * * *"}
            ]
        }))

        mock_store = MagicMock()
        mock_store.list_all.return_value = []  # DB empty

        svc = TriggerService(config_path=yaml_path)
        with patch("brix.triggers.store.TriggerStore", return_value=mock_store):
            svc.load_triggers()

        assert len(svc._triggers) == 1
        assert svc._triggers[0].id == "yaml-trig"
