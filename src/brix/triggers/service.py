"""TriggerService — async background polling.

T-BRIX-SCHED-02: Now loads triggers from DB (TriggerStore) instead of
triggers.yaml.  Also runs a daily retention loop (migrated from the
deprecated scheduler.py).
"""
import asyncio
import json
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from brix.triggers.models import TriggerConfig
from brix.triggers.state import TriggerState
from brix.runners.cli import parse_timeout
from brix.app_logging import log_event

TRIGGERS_CONFIG_PATH = Path.home() / ".brix" / "triggers.yaml"


class TriggerService:
    def __init__(self, config_path=None, state=None):
        self._config_path = config_path or TRIGGERS_CONFIG_PATH
        self._state = state or TriggerState()
        self._triggers: list[TriggerConfig] = []
        self._running = False

    def load_triggers(self):
        """Load triggers from DB.  Falls back to YAML only if DB is empty."""
        self._triggers = []

        # Primary: load from DB via TriggerStore
        try:
            from brix.triggers.store import TriggerStore
            store = TriggerStore()
            rows = store.list_all()
            for row in rows:
                cfg = row.get("config") or {}
                if isinstance(cfg, str):
                    try:
                        cfg = json.loads(cfg)
                    except Exception:
                        cfg = {}
                tc = TriggerConfig(
                    id=row.get("name") or row.get("id", ""),
                    type=row.get("type", ""),
                    pipeline=row.get("pipeline", ""),
                    enabled=row.get("enabled", True),
                    params=cfg.get("params", {}),
                    interval=cfg.get("interval", "5m"),
                    cron=cfg.get("cron"),
                    timezone=cfg.get("timezone"),
                    filter=cfg if row.get("type") in ("mail", "pipeline_done") else {},
                    path=cfg.get("path"),
                    pattern=cfg.get("pattern"),
                    url=cfg.get("url"),
                    headers=cfg.get("headers", {}),
                    hash_field=cfg.get("hash_field"),
                    status=cfg.get("status"),
                    pipeline_target=cfg.get("pipeline"),
                    debounce=cfg.get("debounce"),
                )
                self._triggers.append(tc)
        except Exception:
            pass

        # Fallback: load from YAML if DB yielded nothing
        if not self._triggers and self._config_path.exists():
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
        # T-BRIX-SCHED-02: Also run retention loop (migrated from scheduler.py)
        tasks.append(self._retention_loop())
        await asyncio.gather(*tasks)

    async def _poll_loop(self, trigger: TriggerConfig):
        interval_seconds = 60.0 if trigger.type == "schedule" else parse_timeout(trigger.interval)
        while self._running:
            try:
                await self._check_trigger(trigger)
            except Exception as e:
                log_event("ERROR", "trigger", f"Trigger error: {trigger.id}: {e}", {"trigger_id": trigger.id, "error": str(e)})
                print(f"[trigger:{trigger.id}] Error: {e}")
            await asyncio.sleep(interval_seconds)

    async def _check_trigger(self, trigger: TriggerConfig):
        from brix.triggers.runners import TRIGGER_RUNNERS
        from brix.triggers.store import TriggerStore

        runner_class = TRIGGER_RUNNERS.get(trigger.type)
        if not runner_class:
            print(f"[trigger:{trigger.id}] Unknown type: {trigger.type}")
            return

        runner = runner_class(trigger, self._state)
        store = TriggerStore()
        events = await runner.poll()
        new_events = runner.dedupe(events)

        if not new_events and events:
            log_event("INFO", "trigger", f"Trigger condition not met (all deduped): {trigger.id}", {"trigger_id": trigger.id, "total_events": len(events), "new_events": 0})

        for event in new_events:
            log_event("INFO", "trigger", f"Trigger fired: {trigger.id}", {"trigger_id": trigger.id, "pipeline": trigger.pipeline})
            print(f"[trigger:{trigger.id}] Firing for event")
            fired_at = datetime.now(timezone.utc).isoformat()
            run_id = None
            status = "failure"
            try:
                result = await runner.fire(event)
                run_id = getattr(result, "run_id", None) if result is not None else None
                if result is not None and getattr(result, "success", False):
                    status = "success"
            finally:
                store.record_fired(
                    name=trigger.id,
                    run_id=run_id,
                    status=status,
                    fired_at=fired_at,
                )

    async def _retention_loop(self) -> None:
        """Run the retention policy once per day while service is active.

        Migrated from scheduler.py (T-BRIX-SCHED-02).
        """
        from brix.config import config as brix_config

        while self._running:
            await asyncio.sleep(brix_config.RETENTION_LOOP_INTERVAL_SECONDS)
            if not self._running:
                break
            try:
                from brix.db import BrixDB
                db = BrixDB()
                result = db.clean_retention()
                log_event("INFO", "trigger-service", "Retention applied", result)
            except Exception as e:
                log_event("ERROR", "trigger-service", f"Retention error: {e}", {"error": str(e)})

    def stop(self):
        self._running = False

    @property
    def trigger_count(self):
        return len(self._triggers)

    @property
    def enabled_count(self):
        return len([t for t in self._triggers if t.enabled])
