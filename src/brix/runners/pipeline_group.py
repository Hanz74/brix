"""Pipeline-group runner — runs multiple sub-pipelines in parallel (T-BRIX-V6-17)."""
import asyncio
import time
from pathlib import Path
from typing import Any

from brix.runners.base import BaseRunner


class PipelineGroupRunner(BaseRunner):
    """Runs multiple named sub-pipelines concurrently with shared_params and concurrency limit."""

    def __init__(self, engine=None):
        self._engine = engine

    def set_engine(self, engine):
        """Set the parent engine (called by PipelineEngine)."""
        self._engine = engine

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "pipelines": {"type": "array", "items": {"type": "string"}, "description": "Pipeline names/paths to run"},
                "shared_params": {"type": "object", "description": "Parameters forwarded to all sub-pipelines"},
                "concurrency": {"type": "integer", "description": "Max concurrent pipelines (default 3)"},
            },
            "required": ["pipelines"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    def _resolve_pipeline_path(self, pipeline_ref: str) -> Path | None:
        """Resolve a pipeline reference to an absolute path.

        Resolution order:
        1. Absolute / relative path that exists on disk
        2. ~/.brix/pipelines/<name>
        3. ~/.brix/pipelines/<name>.yaml
        """
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

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        if self._engine is None:
            return {
                "success": False,
                "error": "PipelineGroupRunner not connected to engine",
                "duration": 0.0,
            }

        pipelines_refs: list[str] = getattr(step, "pipelines", None) or []
        if not pipelines_refs:
            return {
                "success": False,
                "error": "pipeline_group step needs 'pipelines' field with at least one entry",
                "duration": 0.0,
            }

        shared_params: dict = getattr(step, "shared_params", {}) or {}
        # concurrency from step; default 3 for pipeline_group
        concurrency: int = getattr(step, "concurrency", 3)
        concurrency = max(1, concurrency)
        semaphore = asyncio.Semaphore(concurrency)

        # Resolve Jinja2 templates inside shared_params using the current context
        resolved_shared: dict = {}
        try:
            jinja_ctx = context.to_jinja_context()
            from jinja2.sandbox import SandboxedEnvironment
            jinja_env = SandboxedEnvironment()
            for k, v in shared_params.items():
                if isinstance(v, str):
                    try:
                        resolved_shared[k] = jinja_env.from_string(v).render(jinja_ctx)
                    except Exception:
                        resolved_shared[k] = v
                else:
                    resolved_shared[k] = v
        except Exception:
            resolved_shared = dict(shared_params)

        async def run_one(pipeline_ref: str) -> tuple[str, bool, Any, str | None]:
            """Return (ref, success, result_data, error_msg)."""
            async with semaphore:
                pipeline_path = self._resolve_pipeline_path(pipeline_ref)
                if pipeline_path is None:
                    return (
                        pipeline_ref,
                        False,
                        None,
                        f"Sub-pipeline not found: {pipeline_ref} (searched absolute path and ~/.brix/pipelines/)",
                    )
                try:
                    sub_pipeline = self._engine.loader.load(str(pipeline_path))
                    sub_result = await self._engine.run(sub_pipeline, resolved_shared)
                    if sub_result.success:
                        return (pipeline_ref, True, sub_result.result, None)
                    else:
                        return (
                            pipeline_ref,
                            False,
                            None,
                            f"Sub-pipeline failed: {sub_pipeline.name}",
                        )
                except Exception as e:
                    return (pipeline_ref, False, None, f"Sub-pipeline error: {e}")

        tasks = [run_one(ref) for ref in pipelines_refs]
        raw = await asyncio.gather(*tasks, return_exceptions=True)

        results: dict[str, Any] = {}
        errors: dict[str, str] = {}
        all_ok = True

        for idx, r in enumerate(raw):
            ref = pipelines_refs[idx]
            if isinstance(r, Exception):
                all_ok = False
                results[ref] = None
                errors[ref] = str(r)
            else:
                pipeline_ref, success, data, error_msg = r
                results[pipeline_ref] = data
                if not success:
                    all_ok = False
                    errors[pipeline_ref] = error_msg or "Unknown error"

        duration = time.monotonic() - start
        self.report_progress(100.0, "done")
        return {
            "success": all_ok,
            "data": {
                "results": results,
                "errors": errors,
                "total": len(pipelines_refs),
                "succeeded": len(pipelines_refs) - len(errors),
                "failed": len(errors),
            },
            "duration": duration,
        }
