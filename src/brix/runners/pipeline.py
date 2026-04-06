"""Sub-pipeline runner — pipeline composition."""
import time
from pathlib import Path
from typing import Any

from brix.pipeline_store import PipelineStore
from brix.runners.base import BaseRunner


class PipelineRunner(BaseRunner):
    """Runs a sub-pipeline in the same asyncio event loop (D-17)."""

    def __init__(self, engine=None):
        self._engine = engine  # Set by PipelineEngine after construction

    def set_engine(self, engine):
        """Set the parent engine (called by PipelineEngine)."""
        self._engine = engine

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "pipeline": {"type": "string", "description": "Path or name of sub-pipeline to run"},
                "params": {"type": "object", "description": "Input parameters forwarded to the sub-pipeline"},
            },
            "required": ["pipeline"],
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "any"

    def _resolve_pipeline_path(self, pipeline_ref: str) -> Path | None:
        """Resolve a pipeline reference to a disk path as a fallback."""
        pipeline_path = Path(pipeline_ref)
        if pipeline_path.exists():
            return pipeline_path

        brix_dir = Path.home() / ".brix" / "pipelines"
        brix_path = brix_dir / pipeline_ref
        brix_yaml = brix_dir / f"{pipeline_ref}.yaml"
        if brix_path.exists():
            return brix_path
        if brix_yaml.exists():
            return brix_yaml

        return None

    def _load_sub_pipeline(self, pipeline_ref: str) -> Any:
        """Load a sub-pipeline from DB first, then fall back to disk."""
        store = PipelineStore()
        try:
            return store.load(pipeline_ref)
        except FileNotFoundError:
            pipeline_path = self._resolve_pipeline_path(pipeline_ref)
            if pipeline_path is None:
                raise FileNotFoundError(
                    f"Sub-pipeline not found: {pipeline_ref} "
                    f"(searched DB, absolute path, and ~/.brix/pipelines/)"
                )
            return self._engine.loader.load(str(pipeline_path))

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        pipeline_ref = getattr(step, 'pipeline', None)
        if not pipeline_ref:
            return {"success": False, "error": "Pipeline step needs 'pipeline' field", "duration": 0.0}

        if self._engine is None:
            return {"success": False, "error": "PipelineRunner not connected to engine", "duration": 0.0}

        try:
            # Load sub-pipeline from DB first, then disk fallback for system/file refs.
            sub_pipeline = self._load_sub_pipeline(pipeline_ref)

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
                # Evaluate output_slots (T-BRIX-V6-14): named slots exposed as
                # {{ sub_step.slot_name }} downstream instead of deep result paths.
                slot_values = self._evaluate_output_slots(sub_pipeline, sub_result)
                self.report_progress(100.0, "done")
                return {
                    "success": True,
                    "data": sub_result.result,
                    "slots": slot_values,
                    "duration": duration,
                }
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

    def _evaluate_output_slots(self, sub_pipeline: Any, sub_result: Any) -> dict:
        """Evaluate pipeline.output_slots against the sub-pipeline result (T-BRIX-V6-14).

        Each slot is a Jinja2 expression evaluated against the sub-pipeline's
        final result dict.  Slot evaluation errors are silently skipped.
        """
        output_slots = getattr(sub_pipeline, "output_slots", {}) or {}
        if not output_slots:
            return {}

        from jinja2.sandbox import SandboxedEnvironment
        env = SandboxedEnvironment()

        # Build a simple context from the result
        result_data = getattr(sub_result, "result", None)
        jinja_ctx: dict = {}
        if isinstance(result_data, dict):
            jinja_ctx.update(result_data)
        jinja_ctx["result"] = result_data

        slot_values: dict = {}
        for slot_name, expression in output_slots.items():
            try:
                tmpl = env.from_string(expression)
                slot_values[slot_name] = tmpl.render(jinja_ctx)
            except Exception:
                slot_values[slot_name] = None  # Graceful degradation
        return slot_values
