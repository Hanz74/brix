"""Sub-pipeline runner — pipeline composition."""
import time
from pathlib import Path
from typing import Any

from brix.runners.base import BaseRunner


class PipelineRunner(BaseRunner):
    """Runs a sub-pipeline in the same asyncio event loop (D-17)."""

    def __init__(self, engine=None):
        self._engine = engine  # Set by PipelineEngine after construction

    def set_engine(self, engine):
        """Set the parent engine (called by PipelineEngine)."""
        self._engine = engine

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        pipeline_ref = getattr(step, 'pipeline', None)
        if not pipeline_ref:
            return {"success": False, "error": "Pipeline step needs 'pipeline' field", "duration": 0.0}

        if self._engine is None:
            return {"success": False, "error": "PipelineRunner not connected to engine", "duration": 0.0}

        # Resolve pipeline path
        # Try: 1) absolute path, 2) relative to ~/.brix/pipelines/, 3) relative to CWD
        pipeline_path = Path(pipeline_ref)
        if not pipeline_path.exists():
            brix_path = Path.home() / ".brix" / "pipelines" / pipeline_ref
            if brix_path.exists():
                pipeline_path = brix_path
            else:
                return {
                    "success": False,
                    "error": f"Sub-pipeline not found: {pipeline_ref}",
                    "duration": time.monotonic() - start,
                }

        try:
            # Load sub-pipeline
            sub_pipeline = self._engine.loader.load(str(pipeline_path))

            # Build sub-pipeline input from step params
            params = getattr(step, 'params', {}) or {}
            sub_input = {k: v for k, v in params.items() if not k.startswith('_')}

            # Track recursion depth
            depth = getattr(context, '_pipeline_depth', 0) + 1
            if depth > 10:
                return {
                    "success": False,
                    "error": "Maximum sub-pipeline depth (10) exceeded",
                    "duration": time.monotonic() - start,
                }

            # Run sub-pipeline (same asyncio loop, shared connections — D-17)
            sub_result = await self._engine.run(sub_pipeline, sub_input)

            duration = time.monotonic() - start

            if sub_result.success:
                return {"success": True, "data": sub_result.result, "duration": duration}
            else:
                return {
                    "success": False,
                    "error": f"Sub-pipeline failed: {sub_pipeline.name}",
                    "duration": duration,
                }

        except Exception as e:
            return {
                "success": False,
                "error": f"Sub-pipeline error: {e}",
                "duration": time.monotonic() - start,
            }
