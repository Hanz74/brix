"""TriggerService — async background polling."""
import asyncio
import yaml
from pathlib import Path
from typing import Optional

from brix.triggers.models import TriggerConfig
from brix.triggers.state import TriggerState
from brix.runners.cli import parse_timeout

TRIGGERS_CONFIG_PATH = Path.home() / ".brix" / "triggers.yaml"


class TriggerService:
    def __init__(self, config_path=None, state=None):
        self._config_path = config_path or TRIGGERS_CONFIG_PATH
        self._state = state or TriggerState()
        self._triggers: list[TriggerConfig] = []
        self._running = False

    def load_triggers(self):
        if not self._config_path.exists():
            self._triggers = []
            return
        with open(self._config_path) as f:
            data = yaml.safe_load(f) or {}
        self._triggers = [TriggerConfig(**t) for t in data.get("triggers", [])]

    async def start(self):
        self.load_triggers()
        enabled = [t for t in self._triggers if t.enabled]
        if not enabled:
            return
        self._running = True
        tasks = [self._poll_loop(t) for t in enabled]
        await asyncio.gather(*tasks)

    async def _poll_loop(self, trigger: TriggerConfig):
        interval_seconds = parse_timeout(trigger.interval)
        while self._running:
            try:
                await self._check_trigger(trigger)
            except Exception as e:
                print(f"[trigger:{trigger.id}] Error: {e}")
            await asyncio.sleep(interval_seconds)

    async def _check_trigger(self, trigger: TriggerConfig):
        from brix.triggers.runners import TRIGGER_RUNNERS

        runner_class = TRIGGER_RUNNERS.get(trigger.type)
        if not runner_class:
            print(f"[trigger:{trigger.id}] Unknown type: {trigger.type}")
            return

        runner = runner_class(trigger, self._state)
        events = await runner.poll()
        new_events = runner.dedupe(events)

        for event in new_events:
            print(f"[trigger:{trigger.id}] Firing for event")
            await runner.fire(event)

    def stop(self):
        self._running = False

    @property
    def trigger_count(self):
        return len(self._triggers)

    @property
    def enabled_count(self):
        return len([t for t in self._triggers if t.enabled])
