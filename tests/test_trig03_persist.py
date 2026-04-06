"""Tests for T-BRIX-TRIG-03 persistence and scheduler crash recovery."""
import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import brix.triggers.runners as runners_mod
from brix.triggers.models import TriggerConfig
from brix.triggers.service import TriggerService
from brix.triggers.store import TriggerStore


@pytest.mark.asyncio
async def test_last_fired_at_is_set_after_trigger_fires(tmp_path, monkeypatch):
    service = TriggerService()
    db_path = tmp_path / "brix.db"
    store = TriggerStore(db_path=db_path)
    store.add(
        name="sched-1",
        type="schedule",
        config={"cron": "* * * * *"},
        pipeline="test-pipeline",
        enabled=True,
    )
    trigger = TriggerConfig(id="sched-1", type="schedule", pipeline="test-pipeline", cron="* * * * *")

    runner = MagicMock()
    runner.poll = AsyncMock(return_value=[{"type": "schedule"}])
    runner.dedupe = MagicMock(return_value=[{"type": "schedule"}])
    runner.fire = AsyncMock(return_value=SimpleNamespace(run_id="run-1", success=True))

    monkeypatch.setattr("brix.triggers.store.TriggerStore", lambda: TriggerStore(db_path=db_path))
    monkeypatch.setitem(runners_mod.TRIGGER_RUNNERS, "schedule", lambda _t, _s: runner)

    await service._check_trigger(trigger)

    updated = store.get("sched-1")
    assert updated["last_fired_at"] is not None
    assert updated["last_run_id"] == "run-1"


@pytest.mark.asyncio
async def test_last_fired_at_updates_on_next_fire(tmp_path, monkeypatch):
    service = TriggerService()
    db_path = tmp_path / "brix.db"
    store = TriggerStore(db_path=db_path)
    store.add(
        name="sched-1",
        type="schedule",
        config={"cron": "* * * * *"},
        pipeline="test-pipeline",
        enabled=True,
    )
    trigger = TriggerConfig(id="sched-1", type="schedule", pipeline="test-pipeline", cron="* * * * *")

    results = [
        SimpleNamespace(run_id="run-1", success=True),
        SimpleNamespace(run_id="run-2", success=True),
    ]
    runner = MagicMock()
    runner.poll = AsyncMock(return_value=[{"type": "schedule"}])
    runner.dedupe = MagicMock(return_value=[{"type": "schedule"}])
    runner.fire = AsyncMock(side_effect=results)

    monkeypatch.setattr("brix.triggers.store.TriggerStore", lambda: TriggerStore(db_path=db_path))
    monkeypatch.setitem(runners_mod.TRIGGER_RUNNERS, "schedule", lambda _t, _s: runner)

    await service._check_trigger(trigger)
    first_fired_at = store.get("sched-1")["last_fired_at"]
    await asyncio.sleep(0.01)
    await service._check_trigger(trigger)
    updated = store.get("sched-1")

    assert updated["last_fired_at"] is not None
    assert updated["last_fired_at"] != first_fired_at
    assert updated["last_run_id"] == "run-2"


@pytest.mark.asyncio
async def test_scheduler_crash_logs_error_and_restarts(monkeypatch, caplog):
    import brix.mcp_handlers.triggers as mod

    mod._scheduler_task = None
    mod._scheduler_running = True

    started = []
    hold_restart = asyncio.Event()
    restarted = asyncio.Event()

    class FakeService:
        def __init__(self):
            self.index = len(started)

        async def start(self):
            started.append(self.index)
            if self.index == 0:
                raise RuntimeError("boom")
            restarted.set()
            await hold_restart.wait()

    async def fake_restart(delay_seconds: float = 10.0):
        assert delay_seconds == 10.0
        mod._start_scheduler_task()

    monkeypatch.setattr("brix.triggers.service.TriggerService", FakeService, raising=False)
    monkeypatch.setattr(mod, "_restart_scheduler_after_delay", fake_restart)

    caplog.set_level(logging.ERROR)
    task = mod._start_scheduler_task()

    await asyncio.wait_for(restarted.wait(), timeout=2.0)

    assert "Trigger scheduler task crashed: boom" in caplog.text
    assert len(started) >= 2
    assert mod._scheduler_task is not None
    assert mod._scheduler_task is not task

    mod._scheduler_running = False
    hold_restart.set()
    mod._scheduler_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await mod._scheduler_task
