"""Parallel step runner — runs sub-steps concurrently (T-BRIX-V4-06)."""
import asyncio
import time
from typing import Any

from brix.runners.base import BaseRunner


class ParallelStepRunner(BaseRunner):
    """Runs a list of sub-steps in parallel and collects their results."""

    def __init__(self, engine=None):
        self._engine = engine

    def set_engine(self, engine):
        """Set the parent engine (called by PipelineEngine)."""
        self._engine = engine

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "sub_steps": {"type": "array", "description": "Steps to run in parallel"},
                "concurrency": {"type": "integer", "description": "Max concurrent sub-steps"},
            },
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        if self._engine is None:
            return {"success": False, "error": "ParallelStepRunner not connected to engine", "duration": 0.0}

        sub_steps = getattr(step, "sub_steps", None) or []
        if not sub_steps:
            return {"success": True, "data": {}, "duration": 0.0}

        concurrency = getattr(step, "concurrency", len(sub_steps))
        semaphore = asyncio.Semaphore(max(1, concurrency))

        from brix.models import Pipeline

        async def run_sub(sub_step_data: dict) -> tuple[str, Any]:
            step_id = sub_step_data.get("id", "?")
            async with semaphore:
                try:
                    mini = Pipeline(name=f"_parallel_{step_id}", steps=[sub_step_data])
                    result = await self._engine.run(mini)
                    return step_id, result
                except Exception as e:
                    return step_id, None  # Will be treated as failure below; store exc

        # Run all sub-steps concurrently
        tasks = [run_sub(s) for s in sub_steps]
        raw = await asyncio.gather(*tasks, return_exceptions=True)

        output: dict[str, Any] = {}
        all_ok = True
        total_duration = 0.0

        for idx, r in enumerate(raw):
            step_id = sub_steps[idx].get("id", str(idx))
            if isinstance(r, Exception):
                all_ok = False
                output[step_id] = None
            else:
                sid, result = r
                if result is None:
                    all_ok = False
                    output[sid] = None
                else:
                    output[sid] = result.result
                    if not result.success:
                        all_ok = False
                    total_duration = max(total_duration, result.duration)

        duration = time.monotonic() - start
        self.report_progress(100.0, "done")
        return {"success": all_ok, "data": output, "duration": duration}
