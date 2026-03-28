"""Debug tools for Brix — Step-Replay, Breakpoints, and Live Context Inspector.

Implements three MCP tool handlers (T-BRIX-V7-06):

  brix__replay_step(run_id, step_id)
      Re-execute a step in isolation using the inputs that were stored by the
      Execution Data feature (V7-04).  Returns the new result without touching
      the original run.

  brix__resume_run(run_id)
      Delete the breakpoint.json sentinel in the run workdir so the engine
      resumes execution.

  brix__inspect_context(run_id)
      Read the latest context-snapshot.json from the run workdir and return
      key → type descriptions.  Works on live (paused or running) and completed
      runs that have persisted snapshot data.

These handlers are imported by mcp_server.py via:

    from brix.debug_tools import (
        _handle_replay_step,
        _handle_resume_run,
        _handle_inspect_context,
        DEBUG_TOOLS,
    )
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _workdir_for(run_id: str) -> Path:
    from brix.context import WORKDIR_BASE
    return WORKDIR_BASE / run_id


def _load_json_file(path: Path) -> Any:
    """Return parsed JSON from *path*, or None on any error."""
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# brix__replay_step
# ---------------------------------------------------------------------------

async def _handle_replay_step(arguments: dict) -> dict:
    """Re-execute a step using its stored inputs (T-BRIX-V7-06).

    Reads the rendered_params and output stored by V7-04 (step_outputs table),
    reconstructs the step from the live pipeline definition, and runs it with
    the saved inputs.  Returns the new result without modifying the original run.

    Required parameters
    -------------------
    run_id : str
        The run whose step should be replayed.
    step_id : str
        ID of the step to replay.
    """
    run_id = (arguments.get("run_id") or "").strip()
    step_id = (arguments.get("step_id") or "").strip()

    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}
    if not step_id:
        return {"success": False, "error": "Parameter 'step_id' is required"}

    # Load stored execution data
    from brix.db import BrixDB
    db = BrixDB()
    row = db.get_step_output(run_id, step_id)
    if row is None:
        return {
            "success": False,
            "error": (
                f"No stored execution data for step '{step_id}' in run '{run_id}'. "
                "Ensure persist_output: true or BRIX_DEBUG=true was active during the original run."
            ),
        }

    stored_params = row.get("rendered_params") or {}

    # Load the run metadata to find the pipeline name
    workdir = _workdir_for(run_id)
    run_meta = _load_json_file(workdir / "run.json")
    if run_meta is None:
        return {
            "success": False,
            "error": f"run.json not found for run '{run_id}' — cannot identify pipeline",
        }
    pipeline_name = run_meta.get("pipeline", "")
    if not pipeline_name:
        return {
            "success": False,
            "error": f"Pipeline name not found in run.json for run '{run_id}'",
        }

    # Load the pipeline definition
    from brix.pipeline_store import PipelineStore
    from brix.loader import PipelineLoader
    from brix.models import Pipeline
    import yaml

    store = PipelineStore()
    try:
        yaml_path = store.get_path(pipeline_name)
        pipeline_data = yaml.safe_load(Path(yaml_path).read_text())
        pipeline = Pipeline(**pipeline_data)
    except Exception as exc:
        return {
            "success": False,
            "error": f"Could not load pipeline '{pipeline_name}': {exc}",
        }

    # Find the target step
    step = next((s for s in pipeline.steps if s.id == step_id), None)
    if step is None:
        return {
            "success": False,
            "error": f"Step '{step_id}' not found in pipeline '{pipeline_name}'",
        }

    # Build a minimal context with stored inputs and an isolated run_id
    import uuid as _uuid
    from brix.context import PipelineContext

    replay_run_id = f"replay-{run_id[:8]}-{_uuid.uuid4().hex[:8]}"
    ctx = PipelineContext(
        pipeline_input=run_meta.get("input", {}),
        run_id=replay_run_id,
    )

    # Get runner
    from brix.engine import PipelineEngine
    engine = PipelineEngine()
    runner = engine._runners.get(step.type)
    if runner is None:
        return {
            "success": False,
            "error": f"No runner registered for step type '{step.type}'",
        }

    # Build a minimal rendered-step-like object using stored params
    from brix.engine import _RenderedStep
    loader = PipelineLoader()
    # For replay we use stored_params directly — no Jinja2 re-rendering
    jinja_ctx = ctx.to_jinja_context()

    class _ReplayStep:
        """Thin wrapper that overrides rendered values with stored params."""
        def __init__(self, step: Any, stored: dict, loader: Any, jinja_ctx: dict) -> None:
            self._step = step
            self._stored = stored
            self._loader = loader
            self._jinja_ctx = jinja_ctx
            # Forward all Step attributes
            for attr in vars(step):
                if not attr.startswith("_"):
                    setattr(self, attr, getattr(step, attr))
            # Override params with stored values
            self.params = stored

        def render(self, template_str: str) -> Any:
            if not isinstance(template_str, str) or "{{" not in template_str:
                return template_str
            return self._loader.render_template(template_str, self._jinja_ctx)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._step, name)

    rendered_step = _ReplayStep(step, stored_params, loader, jinja_ctx)

    import asyncio
    import time

    from brix.mcp_pool import McpConnectionPool

    async def _run_replay() -> dict:
        async with McpConnectionPool() as pool:
            mcp_runner = engine._runners.get("mcp")
            if mcp_runner is not None and hasattr(mcp_runner, "pool"):
                mcp_runner.pool = pool
            try:
                start = time.monotonic()
                result = await runner.execute(rendered_step, ctx)
                duration = time.monotonic() - start
                result["replay_duration"] = round(duration, 4)
                return result
            finally:
                if mcp_runner is not None and hasattr(mcp_runner, "pool"):
                    mcp_runner.pool = None

    try:
        result = await _run_replay()
    except Exception as exc:
        result = {"success": False, "error": str(exc)}

    return {
        "success": result.get("success", False),
        "replay_run_id": replay_run_id,
        "step_id": step_id,
        "original_run_id": run_id,
        "used_stored_params": stored_params,
        "result": result,
    }


# ---------------------------------------------------------------------------
# brix__resume_run
# ---------------------------------------------------------------------------

async def _handle_resume_run(arguments: dict) -> dict:
    """Resume a run that is paused at a breakpoint (T-BRIX-V7-06).

    Deletes the breakpoint.json sentinel from the run workdir.  The engine
    polls this file every 2 seconds — once removed, execution continues.

    Required parameters
    -------------------
    run_id : str
        The run to resume.
    """
    run_id = (arguments.get("run_id") or "").strip()
    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}

    workdir = _workdir_for(run_id)
    breakpoint_path = workdir / "breakpoint.json"

    if not breakpoint_path.exists():
        return {
            "success": False,
            "error": (
                f"No active breakpoint found for run '{run_id}'. "
                "The run may not be paused or the run_id may be incorrect."
            ),
        }

    # Read breakpoint info before deleting
    bp_data = _load_json_file(breakpoint_path) or {}

    try:
        breakpoint_path.unlink()
    except OSError as exc:
        return {
            "success": False,
            "error": f"Could not delete breakpoint sentinel: {exc}",
        }

    return {
        "success": True,
        "run_id": run_id,
        "resumed_after_step": bp_data.get("step_id"),
        "message": "Breakpoint cleared — run will resume within ~2 seconds.",
    }


# ---------------------------------------------------------------------------
# brix__inspect_context
# ---------------------------------------------------------------------------

async def _handle_inspect_context(arguments: dict) -> dict:
    """Read the current Jinja2 context of a running (or paused) run (T-BRIX-V7-06).

    Reads the latest context-snapshot.json written by the engine before each
    step.  The snapshot contains key → type description entries so that the
    context can be inspected without exposing raw (potentially large) values.

    Required parameters
    -------------------
    run_id : str
        The run to inspect.
    """
    run_id = (arguments.get("run_id") or "").strip()
    if not run_id:
        return {"success": False, "error": "Parameter 'run_id' is required"}

    workdir = _workdir_for(run_id)

    if not workdir.exists():
        # Workdir gone — try to load step outputs from DB as fallback
        try:
            from brix.db import BrixDB
            db = BrixDB()
            db_steps = db.list_step_outputs(run_id)
        except Exception:
            db_steps = []

        if db_steps:
            db_context: dict = {}
            for row in db_steps:
                step_id_key = row.get("step_id", "unknown")
                output = row.get("output")
                if isinstance(output, dict):
                    db_context[step_id_key] = f"dict({len(output)} keys) [from DB]"
                elif isinstance(output, list):
                    db_context[step_id_key] = f"list({len(output)} items) [from DB]"
                elif output is not None:
                    db_context[step_id_key] = f"{type(output).__name__} [from DB]"
                else:
                    db_context[step_id_key] = "null [from DB]"
            return {
                "success": True,
                "run_id": run_id,
                "status": "completed (workdir cleaned up)",
                "paused_at_step": None,
                "completed_steps": list(db_context.keys()),
                "context_keys": db_context,
                "snapshot_available": False,
                "source": "db_fallback",
                "note": (
                    "Workdir not found — showing step outputs loaded from DB. "
                    "Values are intentionally omitted. Use persist_output: true on steps to capture full values."
                ),
            }

        return {
            "success": False,
            "error": (
                f"Workdir not found for run '{run_id}' and no step outputs in DB. "
                "The run may not exist or may have been cleaned up without persist_output: true."
            ),
        }

    # Read run metadata for status / current step
    run_meta = _load_json_file(workdir / "run.json") or {}
    run_status = run_meta.get("status", "unknown")
    completed_steps: list[str] = run_meta.get("completed_steps", [])

    # Check for active breakpoint
    bp_data = _load_json_file(workdir / "breakpoint.json")
    paused_at_step: str | None = bp_data.get("step_id") if bp_data else None

    # Read context snapshot
    snapshot_path = workdir / "context-snapshot.json"
    snapshot = _load_json_file(snapshot_path)

    if snapshot is None:
        # Fall back: try building summary from step output files
        snapshot = {}
        outputs_dir = workdir / "step_outputs"
        if outputs_dir.exists():
            for f in sorted(outputs_dir.glob("*.json")):
                if f.stem.endswith("_checkpoint"):
                    continue
                try:
                    data = json.loads(f.read_text())
                    if isinstance(data, dict):
                        snapshot[f.stem] = f"dict({len(data)} keys)"
                    elif isinstance(data, list):
                        snapshot[f.stem] = f"list({len(data)} items)"
                    else:
                        snapshot[f.stem] = type(data).__name__
                except (json.JSONDecodeError, OSError):
                    pass

    # Build result with value previews
    keys_detail: dict[str, str] = {}
    if isinstance(snapshot, dict):
        for key, type_desc in snapshot.items():
            keys_detail[key] = str(type_desc)

    return {
        "success": True,
        "run_id": run_id,
        "status": run_status,
        "paused_at_step": paused_at_step,
        "completed_steps": completed_steps,
        "context_keys": keys_detail,
        "snapshot_available": snapshot_path.exists(),
        "note": (
            "context_keys shows key → type description. "
            "Values are intentionally omitted to prevent large data exposure. "
            "Use persist_output: true on individual steps to capture full values."
        ),
    }


# ---------------------------------------------------------------------------
# Tool descriptor map — imported by mcp_server.py
# ---------------------------------------------------------------------------

DEBUG_TOOLS_HANDLERS: dict[str, Any] = {
    "brix__replay_step": _handle_replay_step,
    "brix__resume_run": _handle_resume_run,
    "brix__inspect_context": _handle_inspect_context,
}
