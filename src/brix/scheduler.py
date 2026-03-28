"""Simple cron scheduler for pipeline execution."""
import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Optional

import yaml

from brix.pipeline_store import PipelineStore
from brix.engine import PipelineEngine
from brix.config import config

SCHEDULES_PATH = Path.home() / ".brix" / "schedules.yaml"


class BrixScheduler:
    """Simple async scheduler for pipeline cron triggers."""

    def __init__(self, store: Optional[PipelineStore] = None):
        self.store = store or PipelineStore()
        self.engine = PipelineEngine()
        self._running = False
        self._schedules: list[dict] = []

    def load_schedules(self, path: Optional[Path] = None) -> None:
        """Load schedule config from YAML."""
        config_path = path or SCHEDULES_PATH
        if not config_path.exists():
            self._schedules = []
            return

        with open(config_path) as f:
            data = yaml.safe_load(f) or {}

        self._schedules = data.get("schedules", [])

    def parse_interval(self, interval_str: str) -> Optional[timedelta]:
        """Parse interval string: '1h', '30m', '24h', '2d', 'daily', 'hourly'."""
        interval_str = interval_str.strip().lower()
        if interval_str == "daily":
            return timedelta(hours=24)
        if interval_str == "hourly":
            return timedelta(hours=1)
        try:
            if interval_str.endswith("h"):
                return timedelta(hours=float(interval_str[:-1]))
            if interval_str.endswith("m"):
                return timedelta(minutes=float(interval_str[:-1]))
            if interval_str.endswith("d"):
                return timedelta(days=float(interval_str[:-1]))
        except ValueError:
            pass
        return None

    async def run_once(self, schedule: dict) -> bool:
        """Execute a single scheduled pipeline run. Returns True on success."""
        pipeline_name = schedule.get("pipeline")
        params = schedule.get("params", {})

        try:
            pipeline = self.store.load(pipeline_name)
            result = await self.engine.run(pipeline, params)
            return result.success
        except Exception as e:
            print(f"[scheduler] Error running {pipeline_name}: {e}")
            return False

    async def start(self) -> None:
        """Start the scheduler loop. Runs until stop() is called."""
        self.load_schedules()
        if not self._schedules:
            print("[scheduler] No schedules configured")
            return

        self._running = True
        print(f"[scheduler] Starting with {len(self._schedules)} schedules")

        tasks = []
        for schedule in self._schedules:
            interval = self.parse_interval(schedule.get("interval", "24h"))
            if interval:
                tasks.append(self._schedule_loop(schedule, interval))
            else:
                print(f"[scheduler] Invalid interval for pipeline '{schedule.get('pipeline')}', skipping")

        # Add periodic retention-policy task (runs once per day)
        tasks.append(self._retention_loop())

        if tasks:
            await asyncio.gather(*tasks)

    async def _retention_loop(self) -> None:
        """Run the retention policy once per day while the scheduler is active."""
        while self._running:
            await asyncio.sleep(config.RETENTION_LOOP_INTERVAL_SECONDS)
            if not self._running:
                break
            try:
                from brix.db import BrixDB
                db = BrixDB()
                result = db.clean_retention()
                print(
                    f"[scheduler] Retention applied: "
                    f"{result['runs_deleted_age']} runs (age), "
                    f"{result['runs_deleted_size']} runs (size), "
                    f"{result['app_log_deleted']} app_log entries. "
                    f"DB: {result['db_size_mb']} MB"
                )
            except Exception as e:
                print(f"[scheduler] Retention error: {e}")

    async def _schedule_loop(self, schedule: dict, interval: timedelta) -> None:
        """Run a pipeline on a fixed interval."""
        pipeline_name = schedule.get("pipeline", "?")
        while self._running:
            print(f"[scheduler] Running {pipeline_name}")
            await self.run_once(schedule)
            print(f"[scheduler] Next run in {interval}")
            await asyncio.sleep(interval.total_seconds())

    def stop(self) -> None:
        """Signal the scheduler to stop after current runs complete."""
        self._running = False
