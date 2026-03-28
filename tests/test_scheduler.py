"""Tests for the Brix cron scheduler (T-BRIX-V2-11)."""
import asyncio
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

import pytest
import yaml

from brix.scheduler import BrixScheduler
from brix.models import RunResult, StepStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_run_result(success: bool = True) -> RunResult:
    return RunResult(
        success=success,
        run_id="sched-run-001",
        steps={"step1": StepStatus(status="ok", duration=0.1)},
        result=None,
        duration=0.2,
    )


# ---------------------------------------------------------------------------
# parse_interval
# ---------------------------------------------------------------------------

class TestParseInterval:
    def setup_method(self):
        self.scheduler = BrixScheduler.__new__(BrixScheduler)

    def test_hours(self):
        assert self.scheduler.parse_interval("1h") == timedelta(hours=1)

    def test_hours_float(self):
        assert self.scheduler.parse_interval("0.5h") == timedelta(hours=0.5)

    def test_minutes(self):
        assert self.scheduler.parse_interval("30m") == timedelta(minutes=30)

    def test_days(self):
        assert self.scheduler.parse_interval("2d") == timedelta(days=2)

    def test_daily_alias(self):
        assert self.scheduler.parse_interval("daily") == timedelta(hours=24)

    def test_hourly_alias(self):
        assert self.scheduler.parse_interval("hourly") == timedelta(hours=1)

    def test_whitespace_stripped(self):
        assert self.scheduler.parse_interval("  6h  ") == timedelta(hours=6)

    def test_case_insensitive(self):
        assert self.scheduler.parse_interval("DAILY") == timedelta(hours=24)

    def test_invalid_returns_none(self):
        assert self.scheduler.parse_interval("unknown") is None

    def test_empty_string_returns_none(self):
        assert self.scheduler.parse_interval("") is None


# ---------------------------------------------------------------------------
# load_schedules
# ---------------------------------------------------------------------------

class TestLoadSchedules:
    def test_load_schedules_from_yaml(self, tmp_path):
        config = {
            "schedules": [
                {"pipeline": "my-pipeline", "interval": "1h", "params": {"key": "val"}},
                {"pipeline": "other-pipeline", "interval": "daily"},
            ]
        }
        config_file = tmp_path / "schedules.yaml"
        config_file.write_text(yaml.dump(config))

        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.load_schedules(config_file)

        assert len(scheduler._schedules) == 2
        assert scheduler._schedules[0]["pipeline"] == "my-pipeline"
        assert scheduler._schedules[1]["pipeline"] == "other-pipeline"

    def test_load_schedules_missing_file(self, tmp_path):
        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.load_schedules(tmp_path / "nonexistent.yaml")
        assert scheduler._schedules == []

    def test_load_schedules_empty_file(self, tmp_path):
        config_file = tmp_path / "schedules.yaml"
        config_file.write_text("")

        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.load_schedules(config_file)
        assert scheduler._schedules == []

    def test_load_schedules_no_schedules_key(self, tmp_path):
        config_file = tmp_path / "schedules.yaml"
        config_file.write_text(yaml.dump({"other_key": "value"}))

        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.load_schedules(config_file)
        assert scheduler._schedules == []


# ---------------------------------------------------------------------------
# run_once
# ---------------------------------------------------------------------------

class TestRunOnce:
    @pytest.mark.asyncio
    async def test_run_once_success(self):
        mock_store = MagicMock()
        mock_engine = MagicMock()
        mock_engine.run = AsyncMock(return_value=_make_run_result(success=True))

        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.store = mock_store
        scheduler.engine = mock_engine

        schedule = {"pipeline": "my-pipeline", "params": {"key": "value"}}
        result = await scheduler.run_once(schedule)

        assert result is True
        mock_store.load.assert_called_once_with("my-pipeline")
        mock_engine.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_once_failure(self):
        mock_store = MagicMock()
        mock_engine = MagicMock()
        mock_engine.run = AsyncMock(return_value=_make_run_result(success=False))

        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.store = mock_store
        scheduler.engine = mock_engine

        schedule = {"pipeline": "failing-pipeline", "params": {}}
        result = await scheduler.run_once(schedule)

        assert result is False

    @pytest.mark.asyncio
    async def test_run_once_pipeline_not_found(self):
        mock_store = MagicMock()
        mock_store.load.side_effect = FileNotFoundError("not found")
        mock_engine = MagicMock()

        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.store = mock_store
        scheduler.engine = mock_engine

        schedule = {"pipeline": "missing-pipeline", "params": {}}
        # Should not raise — returns False and prints error
        result = await scheduler.run_once(schedule)
        assert result is False

    @pytest.mark.asyncio
    async def test_run_once_passes_params(self):
        mock_store = MagicMock()
        mock_engine = MagicMock()
        mock_engine.run = AsyncMock(return_value=_make_run_result(success=True))

        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.store = mock_store
        scheduler.engine = mock_engine

        params = {"keywords": "Rechnung", "top": 200}
        schedule = {"pipeline": "download-attachments", "params": params}
        await scheduler.run_once(schedule)

        pipeline_obj = mock_store.load.return_value
        mock_engine.run.assert_called_once_with(pipeline_obj, params)


# ---------------------------------------------------------------------------
# start (no schedules)
# ---------------------------------------------------------------------------

class TestStart:
    @pytest.mark.asyncio
    async def test_start_with_no_schedules(self, tmp_path):
        """Scheduler exits immediately when no schedules are configured."""
        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler.store = MagicMock()
        scheduler.engine = MagicMock()
        scheduler._running = False
        scheduler._schedules = []

        # load_schedules with empty file → no schedules → returns quickly
        scheduler.load_schedules(tmp_path / "nonexistent.yaml")
        await scheduler.start()  # should return without hanging

    @pytest.mark.asyncio
    async def test_stop_sets_flag(self):
        scheduler = BrixScheduler.__new__(BrixScheduler)
        scheduler._running = True
        scheduler.stop()
        assert scheduler._running is False
