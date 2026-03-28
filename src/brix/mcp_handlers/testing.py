"""MCP handlers for Pipeline Testing: step pins and mock data (T-BRIX-DB-24)."""
from __future__ import annotations

from brix.db import BrixDB
from brix.history import RunHistory


async def _handle_pin_step_data(arguments: dict) -> dict:
    """Pin the output of a step from a past run so the engine uses it as mock data.

    If run_id is provided the step output is loaded from the run history and
    stored as the pinned data.  If run_id is omitted and ``data`` is provided
    in arguments, that data is stored directly.
    """
    pipeline_name: str = arguments.get("pipeline_name", "")
    step_id: str = arguments.get("step_id", "")
    run_id: str | None = arguments.get("run_id") or None
    inline_data = arguments.get("data")

    if not pipeline_name or not step_id:
        return {
            "success": False,
            "error": "pipeline_name and step_id are required",
        }

    db = BrixDB()

    # Resolve data: prefer run history lookup, fall back to inline data
    pinned_data = inline_data
    from_run = ""

    if run_id:
        # Load step output from run history (step_executions table)
        try:
            records = db.get_step_executions(run_id, step_id)
            if not records:
                return {
                    "success": False,
                    "error": f"Step '{step_id}' not found in run '{run_id}'",
                    "hint": "Use brix__get_step_data to list available steps for this run.",
                }
            step_record = records[-1]  # use most recent execution of this step
            output_data = step_record.get("output_data")
            pinned_data = output_data  # already deserialized by get_step_executions
            from_run = run_id
        except Exception as exc:
            return {
                "success": False,
                "error": f"Failed to load step data from run '{run_id}': {exc}",
            }

    if pinned_data is None and not run_id:
        return {
            "success": False,
            "error": "Either run_id or data must be provided",
        }

    record = db.pin_step(
        pipeline_name=pipeline_name,
        step_id=step_id,
        data=pinned_data,
        from_run=from_run,
    )
    return {
        "success": True,
        "pin": record,
        "message": (
            f"Step '{step_id}' in pipeline '{pipeline_name}' is now pinned. "
            "The engine will use the mock data instead of executing the step."
        ),
    }


async def _handle_unpin_step_data(arguments: dict) -> dict:
    """Remove a step pin so the engine executes the step normally again."""
    pipeline_name: str = arguments.get("pipeline_name", "")
    step_id: str = arguments.get("step_id", "")

    if not pipeline_name or not step_id:
        return {
            "success": False,
            "error": "pipeline_name and step_id are required",
        }

    db = BrixDB()
    deleted = db.unpin_step(pipeline_name=pipeline_name, step_id=step_id)
    if not deleted:
        return {
            "success": False,
            "error": f"No pin found for step '{step_id}' in pipeline '{pipeline_name}'",
        }
    return {
        "success": True,
        "message": f"Pin removed for step '{step_id}' in pipeline '{pipeline_name}'.",
    }


async def _handle_list_pins(arguments: dict) -> dict:
    """List all pinned steps for a pipeline."""
    pipeline_name: str = arguments.get("pipeline_name", "")

    if not pipeline_name:
        return {
            "success": False,
            "error": "pipeline_name is required",
        }

    db = BrixDB()
    pins = db.get_pins(pipeline_name=pipeline_name)
    return {
        "success": True,
        "pipeline_name": pipeline_name,
        "pins": pins,
        "count": len(pins),
    }
