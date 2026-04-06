"""Tests for T-BRIX-TRIG-02 schedule timezone support."""
import logging
from datetime import datetime, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from brix.triggers.models import TriggerConfig
from brix.triggers.runners import ScheduleTriggerRunner
from brix.triggers.state import TriggerState


def _install_fake_datetime(mock_dt, fake_now: datetime):
    mock_dt.now.return_value = fake_now
    mock_dt.fromtimestamp = datetime.fromtimestamp
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)


@pytest.mark.asyncio
async def test_schedule_trigger_uses_configured_timezone(tmp_path):
    trigger = TriggerConfig(
        id="sched-berlin",
        type="schedule",
        pipeline="test-pipeline",
        cron="0 15 * * *",
        timezone="Europe/Berlin",
    )
    state = TriggerState(db_path=tmp_path / "test.db")
    runner = ScheduleTriggerRunner(trigger, state)
    fake_now = datetime(2026, 4, 6, 15, 2, tzinfo=ZoneInfo("Europe/Berlin"))

    with patch("brix.triggers.runners.datetime") as mock_dt:
        _install_fake_datetime(mock_dt, fake_now)
        events = await runner.poll()

    assert len(events) == 1


@pytest.mark.asyncio
async def test_schedule_trigger_defaults_to_utc(tmp_path):
    trigger = TriggerConfig(
        id="sched-utc",
        type="schedule",
        pipeline="test-pipeline",
        cron="2 13 * * *",
    )
    state = TriggerState(db_path=tmp_path / "test.db")
    runner = ScheduleTriggerRunner(trigger, state)
    fake_now = datetime(2026, 4, 6, 13, 2, tzinfo=timezone.utc)

    with patch("brix.triggers.runners.datetime") as mock_dt:
        _install_fake_datetime(mock_dt, fake_now)
        events = await runner.poll()

    assert len(events) == 1


@pytest.mark.asyncio
async def test_invalid_timezone_falls_back_to_utc_with_warning(tmp_path, caplog):
    trigger = TriggerConfig(
        id="sched-bad-tz",
        type="schedule",
        pipeline="test-pipeline",
        cron="2 13 * * *",
        timezone="Mars/Olympus",
    )
    state = TriggerState(db_path=tmp_path / "test.db")
    runner = ScheduleTriggerRunner(trigger, state)
    fake_now = datetime(2026, 4, 6, 13, 2, tzinfo=timezone.utc)

    caplog.set_level(logging.WARNING)
    with patch("brix.triggers.runners.datetime") as mock_dt:
        _install_fake_datetime(mock_dt, fake_now)
        events = await runner.poll()

    assert len(events) == 1
    assert "invalid timezone 'Mars/Olympus'" in caplog.text
    assert "falling back to UTC" in caplog.text
