"""Tests for T-BRIX-TRIG-01 schedule boundary checks and 60s polling."""
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from brix.triggers.models import TriggerConfig
from brix.triggers.runners import ScheduleTriggerRunner
from brix.triggers.service import TriggerService
from brix.triggers.state import TriggerState
def _make_trigger(cron_expr: str) -> TriggerConfig:
    return TriggerConfig(
        id="sched-1",
        type="schedule",
        pipeline="test-pipeline",
        cron=cron_expr,
        interval="5m",
    )


def _install_fake_datetime(mock_dt, fake_now: datetime):
    mock_dt.now.return_value = fake_now
    mock_dt.fromtimestamp = datetime.fromtimestamp
    mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)


@pytest.mark.asyncio
async def test_boundary_check_fires_for_missed_hour_boundary(tmp_path):
    trigger = _make_trigger("0 */3 * * *")
    state = TriggerState(db_path=tmp_path / "test.db")
    runner = ScheduleTriggerRunner(trigger, state)
    state.set_last_check("sched-1", datetime(2026, 4, 6, 14, 58, tzinfo=timezone.utc).timestamp())
    fake_now = datetime(2026, 4, 6, 15, 2, tzinfo=timezone.utc)

    with patch("brix.triggers.runners.datetime") as mock_dt:
        _install_fake_datetime(mock_dt, fake_now)
        events = await runner.poll()

    assert len(events) == 1


@pytest.mark.asyncio
async def test_boundary_check_does_not_fire_without_matching_minute(tmp_path):
    trigger = _make_trigger("0 */3 * * *")
    state = TriggerState(db_path=tmp_path / "test.db")
    runner = ScheduleTriggerRunner(trigger, state)
    state.set_last_check("sched-1", datetime(2026, 4, 6, 15, 1, tzinfo=timezone.utc).timestamp())
    fake_now = datetime(2026, 4, 6, 15, 2, tzinfo=timezone.utc)

    with patch("brix.triggers.runners.datetime") as mock_dt:
        _install_fake_datetime(mock_dt, fake_now)
        events = await runner.poll()

    assert events == []


@pytest.mark.asyncio
async def test_first_poll_checks_last_five_minutes(tmp_path):
    trigger = _make_trigger("0 */3 * * *")
    state = TriggerState(db_path=tmp_path / "test.db")
    runner = ScheduleTriggerRunner(trigger, state)
    fake_now = datetime(2026, 4, 6, 15, 2, tzinfo=timezone.utc)

    with patch("brix.triggers.runners.datetime") as mock_dt:
        _install_fake_datetime(mock_dt, fake_now)
        events = await runner.poll()

    assert len(events) == 1


@pytest.mark.asyncio
async def test_step_cron_fires_when_boundary_is_crossed(tmp_path):
    trigger = _make_trigger("*/5 * * * *")
    state = TriggerState(db_path=tmp_path / "test.db")
    runner = ScheduleTriggerRunner(trigger, state)
    state.set_last_check("sched-1", datetime(2026, 4, 6, 14, 3, tzinfo=timezone.utc).timestamp())
    fake_now = datetime(2026, 4, 6, 14, 8, tzinfo=timezone.utc)

    with patch("brix.triggers.runners.datetime") as mock_dt:
        _install_fake_datetime(mock_dt, fake_now)
        events = await runner.poll()

    assert len(events) == 1


@pytest.mark.asyncio
async def test_schedule_trigger_uses_60_second_poll(monkeypatch):
    service = TriggerService()
    trigger = TriggerConfig(
        id="sched-1",
        type="schedule",
        pipeline="test-pipeline",
        cron="* * * * *",
        interval="5m",
    )
    sleeps: list[float] = []

    async def fake_check(_trigger):
        service.stop()

    async def fake_sleep(seconds: float):
        sleeps.append(seconds)

    monkeypatch.setattr(service, "_check_trigger", fake_check)
    monkeypatch.setattr("brix.triggers.service.asyncio.sleep", fake_sleep)

    service._running = True
    await service._poll_loop(trigger)

    assert sleeps == [60.0]
