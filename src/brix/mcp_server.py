"""Brix MCP Server — stdio and HTTP/SSE transports.

Exposes Brix pipeline management and execution capabilities as MCP tools.
Real implementations for Discovery (V2-05), Builder (V2-06), Execution (V2-07),
Pipeline Store (V2-08), Auto-Exposure as MCP tools (V2-09), and HTTP/SSE
transport (V2-12).
"""
import json
import asyncio
import contextlib
import logging
from pathlib import Path

import yaml

from mcp.server.lowlevel import Server, NotificationOptions
from mcp.server.stdio import stdio_server
from mcp.server.models import InitializationOptions
import mcp.types as types

logger = logging.getLogger(__name__)

from brix.bricks.registry import BrickRegistry
from brix.loader import PipelineLoader
from brix.validator import PipelineValidator
from brix.history import RunHistory
from brix.engine import PipelineEngine
from brix.pipeline_store import PipelineStore

# Shared singletons
_registry = BrickRegistry()
_loader = PipelineLoader()
_validator = PipelineValidator()
_store = PipelineStore()

# Default pipeline directory (primary save target, kept for backward compat)
PIPELINE_DIR = Path.home() / ".brix" / "pipelines"


def _pipeline_dir() -> Path:
    """Return the primary pipeline directory, creating it if needed."""
    PIPELINE_DIR.mkdir(parents=True, exist_ok=True)
    return PIPELINE_DIR


def _pipeline_path(name: str) -> Path:
    """Return the save path for a named pipeline YAML (always in pipelines_dir)."""
    return _pipeline_dir() / f"{name}.yaml"


# ---------------------------------------------------------------------------
# V2-09: Pipeline tool name helper
# ---------------------------------------------------------------------------

PIPELINE_TOOL_PREFIX = "brix__pipeline__"


def _pipeline_tool_name(pipeline_name: str) -> str:
    """Convert a pipeline name to a safe MCP tool name."""
    return PIPELINE_TOOL_PREFIX + pipeline_name.replace("-", "_").replace(" ", "_")


def _build_pipeline_tools(store: PipelineStore) -> list[types.Tool]:
    """Build MCP tool definitions for all saved pipelines (V2-09).

    Each saved pipeline becomes a tool named brix__pipeline__<name>.
    The input schema is derived from the pipeline's input parameter definitions.
    """
    tools: list[types.Tool] = []
    for info in store.list_all():
        tool_name = _pipeline_tool_name(info["name"])
        description = (
            info.get("description") or f"Run the '{info['name']}' pipeline."
        )
        # Ensure description ends with period-like sentence and has Returns info
        if "Returns" not in description and "returns" not in description:
            description = description.rstrip(".") + ". Returns run results and step outputs."

        # Build input schema from pipeline input params
        properties: dict = {}
        required: list[str] = []
        try:
            pipeline = store.load(info["name"])
            for param_name, param in pipeline.input.items():
                json_type = param.type if param.type in ("string", "integer", "boolean", "number", "array", "object") else "string"
                prop: dict = {"type": json_type}
                if param.description:
                    prop["description"] = param.description
                if param.default is not None:
                    prop["default"] = param.default
                else:
                    required.append(param_name)
                properties[param_name] = prop
        except Exception:
            pass  # pipeline with errors → empty schema, still register tool

        input_schema: dict = {"type": "object", "properties": properties, "required": required}

        tools.append(
            types.Tool(
                name=tool_name,
                description=description,
                inputSchema=input_schema,
            )
        )
    return tools


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

BRIX_TOOLS: list[types.Tool] = [
    types.Tool(
        name="brix__get_tips",
        description=(
            "Get usage tips and best practices for working with Brix. "
            "Call this first to understand how to use Brix effectively. "
            "Returns a list of tips as strings."
        ),
        inputSchema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
    types.Tool(
        name="brix__list_bricks",
        description=(
            "List all available bricks (pipeline building blocks). "
            "Use when you want to know what brick types are available. "
            "Optionally filter by category. "
            "Returns a list of brick definitions with name, type, description, and when_to_use."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Filter by category (e.g. 'http', 'cli', 'mcp', 'python'). Omit for all.",
                },
            },
            "required": [],
        },
    ),
    types.Tool(
        name="brix__search_bricks",
        description=(
            "Search bricks by keyword across name, description, and when_to_use fields. "
            "Use when you know what you want to do but not which brick to use. "
            "Returns matching brick definitions."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search keyword (e.g. 'download', 'email', 'convert').",
                },
                "category": {
                    "type": "string",
                    "description": "Optional category filter to narrow results.",
                },
            },
            "required": ["query"],
        },
    ),
    types.Tool(
        name="brix__get_brick_schema",
        description=(
            "Get the full schema for a specific brick, including all config parameters. "
            "Use before adding a brick to a pipeline to know what parameters to provide. "
            "Returns the brick's name, type, description, config_schema, and examples."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "brick_name": {
                    "type": "string",
                    "description": "Exact brick name (e.g. 'http_get', 'cli_exec', 'mcp_call').",
                },
            },
            "required": ["brick_name"],
        },
    ),
    types.Tool(
        name="brix__create_pipeline",
        description=(
            "Create a new pipeline definition, optionally with steps inline. "
            "Use this as the first step when building a pipeline — steps can be provided "
            "immediately to validate and save in a single call. "
            "Returns the pipeline_id, validated flag, step_count, and any errors."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Human-readable pipeline name (e.g. 'Download Attachments').",
                },
                "description": {
                    "type": "string",
                    "description": "What this pipeline does.",
                },
                "version": {
                    "type": "string",
                    "description": "Semantic version string (default: '1.0.0').",
                    "default": "1.0.0",
                },
                "steps": {
                    "type": "array",
                    "description": "Optional list of step dicts to include inline (Lisa P0).",
                },
                "input_schema": {
                    "type": "object",
                    "description": "JSON Schema describing pipeline input parameters.",
                },
            },
            "required": ["name"],
        },
    ),
    types.Tool(
        name="brix__get_pipeline",
        description=(
            "Get the current definition of a pipeline by name. "
            "Use to inspect pipeline structure before running or modifying it. "
            "Returns full pipeline structure with all steps and config."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "pipeline_id": {
                    "type": "string",
                    "description": "Pipeline name (used as file name in pipelines dir).",
                },
            },
            "required": ["pipeline_id"],
        },
    ),
    types.Tool(
        name="brix__add_step",
        description=(
            "Add a step to an existing pipeline. "
            "Use after brix__create_pipeline to build up the pipeline step by step. "
            "Returns the updated pipeline with the new step appended."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "pipeline_id": {
                    "type": "string",
                    "description": "Target pipeline name.",
                },
                "step_id": {
                    "type": "string",
                    "description": "Unique step identifier within the pipeline (e.g. 'fetch_emails').",
                },
                "brick": {
                    "type": "string",
                    "description": "Brick name to use for this step (e.g. 'http_get', 'mcp_call').",
                },
                "params": {
                    "type": "object",
                    "description": "Step-specific parameters. May use Jinja2 templates referencing prior steps.",
                },
                "on_error": {
                    "type": "string",
                    "enum": ["fail", "skip", "retry"],
                    "description": "Error handling strategy (default: 'fail').",
                    "default": "fail",
                },
                "parallel": {
                    "type": "boolean",
                    "description": "Whether to run this step in parallel with compatible siblings.",
                    "default": False,
                },
                "position": {
                    "type": "string",
                    "description": "Where to insert step, e.g. 'after:<step_id>'. Default: append.",
                },
            },
            "required": ["pipeline_id", "step_id", "brick"],
        },
    ),
    types.Tool(
        name="brix__remove_step",
        description=(
            "Remove a step from a pipeline by step ID. "
            "Use when you need to restructure or fix a pipeline. "
            "Returns the updated pipeline without the removed step."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "pipeline_id": {
                    "type": "string",
                    "description": "Target pipeline name.",
                },
                "step_id": {
                    "type": "string",
                    "description": "ID of the step to remove.",
                },
            },
            "required": ["pipeline_id", "step_id"],
        },
    ),
    types.Tool(
        name="brix__validate_pipeline",
        description=(
            "Validate a pipeline definition without executing it. "
            "Use before running to catch configuration errors early. "
            "Returns validation result with any errors or warnings found."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "pipeline_id": {
                    "type": "string",
                    "description": "Pipeline name to validate.",
                },
            },
            "required": ["pipeline_id"],
        },
    ),
    types.Tool(
        name="brix__run_pipeline",
        description=(
            "Execute a pipeline and return results. "
            "Use after validating the pipeline is correct. "
            "Returns run ID, status, step outputs, and final result. "
            "For long-running pipelines use brix__get_run_status to poll progress."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "pipeline_id": {
                    "type": "string",
                    "description": "Pipeline name to execute.",
                },
                "input": {
                    "type": "object",
                    "description": "Input parameters matching the pipeline's input_schema.",
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "If true, show what would happen without executing.",
                    "default": False,
                },
            },
            "required": ["pipeline_id"],
        },
    ),
    types.Tool(
        name="brix__get_run_status",
        description=(
            "Get the status and results of a pipeline run by run ID. "
            "Use to poll progress for long-running pipelines or retrieve results after completion. "
            "Returns run status, step statuses, elapsed time, and output."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "run_id": {
                    "type": "string",
                    "description": "Run ID returned by brix__run_pipeline.",
                },
            },
            "required": ["run_id"],
        },
    ),
    types.Tool(
        name="brix__get_run_history",
        description=(
            "List recent pipeline runs with status and timing. "
            "Use to see past executions and their outcomes. "
            "Returns a list of runs sorted by most recent first."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of runs to return (default: 10).",
                    "default": 10,
                },
                "pipeline_name": {
                    "type": "string",
                    "description": "Filter by pipeline name. Omit for all pipelines.",
                },
            },
            "required": [],
        },
    ),
    types.Tool(
        name="brix__list_pipelines",
        description=(
            "List all available pipeline definitions. "
            "Use to discover existing pipelines before creating new ones. "
            "Returns pipeline names, descriptions, step counts, and file paths."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "directory": {
                    "type": "string",
                    "description": "Directory to search for pipeline files (default: ~/.brix/pipelines).",
                },
            },
            "required": [],
        },
    ),
    types.Tool(
        name="brix__get_template",
        description=(
            "Get a pre-built pipeline template matching a goal description. "
            "Use before brix__create_pipeline when you want a quickstart for common patterns. "
            "Omit goal to list all available templates. "
            "Returns the template pipeline definition and customization_points."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "goal": {
                    "type": "string",
                    "description": "Description of what you want to do (e.g. 'download files from api', 'process email attachments'). Omit to list all templates.",
                },
            },
            "required": [],
        },
    ),
]


# ---------------------------------------------------------------------------
# V2-05 Discovery handlers
# ---------------------------------------------------------------------------

async def _handle_get_tips(arguments: dict) -> dict:
    """Return usage tips and best practices for Brix."""
    # Gather brick categories
    all_bricks = _registry.list_all()
    categories: dict[str, int] = {}
    for b in all_bricks:
        categories[b.category] = categories.get(b.category, 0) + 1

    category_lines = [
        f"  - {cat}: {count} brick(s)" for cat, count in sorted(categories.items())
    ]

    # List saved pipelines (from all search paths, respecting current PIPELINE_DIR)
    _tips_store = PipelineStore(pipelines_dir=PIPELINE_DIR)
    pipeline_names = [p["name"] for p in _tips_store.list_all()]

    tips = [
        "=== Brix Usage Tips ===",
        "",
        "## Available Brick Categories",
        *category_lines,
        f"  Total bricks: {len(all_bricks)}",
        "",
        "## Saved Pipelines",
        (
            "\n".join(f"  - {name}" for name in pipeline_names)
            if pipeline_names
            else "  (none saved yet — use brix__create_pipeline)"
        ),
        "",
        "## Path Convention",
        "  - Host paths must use /host/root/... prefix inside the container",
        "  - Example: /root/myfile.txt on host → /host/root/myfile.txt in pipeline",
        "",
        "## Performance Pitfalls",
        "  - NEVER pass base64-encoded file content in foreach loops (payload explosion)",
        "  - Use file paths (strings) in foreach, not file contents",
        "  - For large payloads (>100KB), use Python helper with httpx instead of http brick",
        "",
        "## Helper Script Pattern (argv + stdin)",
        "  Scripts must support both input methods:",
        "    if len(sys.argv) > 1:",
        "        params = json.loads(sys.argv[1])",
        "    elif not sys.stdin.isatty():",
        "        raw = sys.stdin.read().strip()",
        "        params = json.loads(raw) if raw else {}",
        "",
        "## Common Mistakes",
        "  - concurrency must be int (not a Jinja2 template string)",
        "  - shell=False is enforced for cli bricks (use args list, not command string)",
        "  - No container rebuild needed for pipelines/ and helpers/ (volumes are mounted)",
        "",
        "## Strategies",
        "  - targeted: use specific MCP tool → brix run → result (fast, precise)",
        "  - broad: fetch all → filter locally → process (robust, works without API search)",
        "",
        "## Workflow",
        "  1. brix__list_bricks or brix__search_bricks to find the right brick",
        "  2. brix__get_brick_schema to learn parameters",
        "  3. brix__create_pipeline with steps inline (Lisa P0: one call to create+validate)",
        "  4. brix__validate_pipeline to catch errors early",
        "  5. brix__run_pipeline to execute",
        "  6. brix__get_run_history to review results",
    ]

    return {
        "tips": tips,
        "brick_count": len(all_bricks),
        "pipeline_count": len(pipeline_names),
        "categories": list(categories.keys()),
    }


async def _handle_list_bricks(arguments: dict) -> dict:
    """List all available bricks, optionally filtered by category."""
    category = arguments.get("category")

    if category:
        bricks = _registry.list_by_category(category)
    else:
        bricks = _registry.list_all()

    return {
        "bricks": [
            {
                "name": b.name,
                "type": b.type,
                "description": b.description,
                "when_to_use": b.when_to_use,
                "category": b.category,
            }
            for b in bricks
        ],
        "total": len(bricks),
        "categories": _registry.get_categories(),
    }


async def _handle_search_bricks(arguments: dict) -> dict:
    """Search bricks by keyword."""
    query = arguments.get("query", "")
    category = arguments.get("category")

    results = _registry.search(query, category=category)

    return {
        "query": query,
        "results": [
            {
                "name": b.name,
                "type": b.type,
                "description": b.description,
                "when_to_use": b.when_to_use,
                "category": b.category,
            }
            for b in results
        ],
        "total": len(results),
    }


async def _handle_get_brick_schema(arguments: dict) -> dict:
    """Get full schema for a specific brick."""
    name = arguments.get("brick_name", "")
    brick = _registry.get(name)

    if not brick:
        return {
            "success": False,
            "error": f"Brick '{name}' not found. Use brix__list_bricks to see available bricks.",
        }

    return {
        "name": brick.name,
        "type": brick.type,
        "description": brick.description,
        "when_to_use": brick.when_to_use,
        "category": brick.category,
        "input_description": brick.input_description,
        "output_description": brick.output_description,
        "config_schema": brick.to_json_schema(),
    }


# ---------------------------------------------------------------------------
# V2-06 Builder handlers
# ---------------------------------------------------------------------------

def _load_pipeline_yaml(name: str) -> dict:
    """Load a pipeline YAML file as raw dict.

    Creates a PipelineStore with PIPELINE_DIR as primary path so that
    monkeypatching PIPELINE_DIR in tests works, while still searching
    additional paths (e.g. /app/pipelines container volume).
    """
    store = PipelineStore(pipelines_dir=PIPELINE_DIR)
    return store.load_raw(name)


def _save_pipeline_yaml(name: str, data: dict) -> None:
    """Save a pipeline dict to YAML."""
    path = _pipeline_path(name)
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def _validate_pipeline_dict(data: dict) -> dict:
    """Validate a pipeline dict using PipelineValidator. Returns validation summary."""
    try:
        pipeline = _loader.load_from_string(yaml.dump(data))
        result = _validator.validate(pipeline)
        return {
            "valid": result.is_valid,
            "errors": result.errors,
            "warnings": result.warnings,
            "checks": result.checks,
        }
    except Exception as exc:
        return {
            "valid": False,
            "errors": [str(exc)],
            "warnings": [],
            "checks": [],
        }


async def _handle_create_pipeline(arguments: dict) -> dict:
    """Create a new pipeline, optionally with inline steps."""
    name = arguments.get("name", "")
    if not name:
        return {"success": False, "error": "Pipeline 'name' is required."}

    description = arguments.get("description", "")
    version = arguments.get("version", "1.0.0")
    steps_raw = arguments.get("steps", [])
    input_schema = arguments.get("input_schema", {})

    # Build pipeline dict
    pipeline_data: dict = {
        "name": name,
        "version": version,
        "steps": steps_raw or [],
    }
    if description:
        pipeline_data["description"] = description
    if input_schema:
        pipeline_data["input"] = input_schema

    # Validate
    validation = _validate_pipeline_dict(pipeline_data)

    # Save regardless (agent can fix errors via add_step / validate)
    _save_pipeline_yaml(name, pipeline_data)

    return {
        "success": True,
        "pipeline_id": name,
        "pipeline_path": str(_pipeline_path(name)),
        "step_count": len(steps_raw or []),
        "validated": validation["valid"],
        "validation": validation,
    }


async def _handle_get_pipeline(arguments: dict) -> dict:
    """Get pipeline definition by name."""
    name = arguments.get("pipeline_id", "")
    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {"success": False, "error": str(exc)}

    steps = data.get("steps", [])
    return {
        "name": data.get("name", name),
        "version": data.get("version", "1.0.0"),
        "description": data.get("description", ""),
        "step_count": len(steps),
        "steps": steps,
        "input": data.get("input", {}),
        "credentials": data.get("credentials", {}),
        "output": data.get("output", {}),
        "pipeline_path": str(_pipeline_path(name)),
    }


async def _handle_add_step(arguments: dict) -> dict:
    """Add a step to an existing pipeline."""
    name = arguments.get("pipeline_id", "")
    step_id = arguments.get("step_id", "")
    brick = arguments.get("brick", "")

    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {"success": False, "error": str(exc)}

    # Map brick name to type (look up in registry)
    brick_def = _registry.get(brick)
    step_type = brick_def.type if brick_def else "cli"  # safe fallback

    # Build step dict
    step: dict = {"id": step_id, "type": step_type}
    if arguments.get("params"):
        step["params"] = arguments["params"]
    if arguments.get("on_error"):
        step["on_error"] = arguments["on_error"]
    if arguments.get("parallel"):
        step["parallel"] = arguments["parallel"]

    # Insert at position or append
    steps: list = data.get("steps", [])
    position = arguments.get("position", "")
    if position and position.startswith("after:"):
        after_id = position[len("after:"):]
        idx = next((i for i, s in enumerate(steps) if s.get("id") == after_id), None)
        if idx is not None:
            steps.insert(idx + 1, step)
        else:
            steps.append(step)
    else:
        steps.append(step)

    data["steps"] = steps

    # Validate and save
    validation = _validate_pipeline_dict(data)
    _save_pipeline_yaml(name, data)

    return {
        "success": True,
        "pipeline_id": name,
        "step_count": len(steps),
        "validated": validation["valid"],
        "validation": validation,
    }


async def _handle_remove_step(arguments: dict) -> dict:
    """Remove a step from a pipeline."""
    name = arguments.get("pipeline_id", "")
    step_id = arguments.get("step_id", "")

    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {"success": False, "error": str(exc)}

    steps: list = data.get("steps", [])
    original_count = len(steps)
    steps = [s for s in steps if s.get("id") != step_id]

    if len(steps) == original_count:
        return {
            "success": False,
            "error": f"Step '{step_id}' not found in pipeline '{name}'.",
        }

    data["steps"] = steps
    _save_pipeline_yaml(name, data)

    return {
        "success": True,
        "pipeline_id": name,
        "removed_step_id": step_id,
        "step_count": len(steps),
    }


async def _handle_validate_pipeline(arguments: dict) -> dict:
    """Validate a pipeline without running it."""
    name = arguments.get("pipeline_id", "")
    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {"success": False, "error": str(exc)}

    validation = _validate_pipeline_dict(data)
    return {
        "success": True,
        "pipeline_id": name,
        "valid": validation["valid"],
        "errors": validation["errors"],
        "warnings": validation["warnings"],
        "checks": validation["checks"],
    }


# ---------------------------------------------------------------------------
# V2-07 Execution handlers
# ---------------------------------------------------------------------------

async def _handle_run_pipeline(arguments: dict) -> dict:
    """Execute a pipeline and return results with dual-layer error schema."""
    name = arguments.get("pipeline_id", "")
    user_input = arguments.get("input", {})

    try:
        data = _load_pipeline_yaml(name)
    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": {
                "code": "PIPELINE_NOT_FOUND",
                "message": str(exc),
                "step_id": None,
                "recoverable": False,
                "agent_actions": ["list_pipelines", "create_pipeline"],
                "resume_command": None,
            },
        }

    try:
        pipeline_yaml = yaml.dump(data)
        pipeline = _loader.load_from_string(pipeline_yaml)
    except Exception as exc:
        return {
            "success": False,
            "error": {
                "code": "PIPELINE_PARSE_ERROR",
                "message": str(exc),
                "step_id": None,
                "recoverable": True,
                "agent_actions": ["validate_pipeline", "fix_pipeline_yaml"],
                "resume_command": f"brix__validate_pipeline({{\"pipeline_id\": \"{name}\"}})",
            },
        }

    engine = PipelineEngine()
    try:
        result = await engine.run(pipeline, user_input)
    except Exception as exc:
        return {
            "success": False,
            "error": {
                "code": "ENGINE_ERROR",
                "message": str(exc),
                "step_id": None,
                "recoverable": True,
                "agent_actions": ["retry_pipeline", "validate_pipeline"],
                "resume_command": f"brix__run_pipeline({{\"pipeline_id\": \"{name}\"}})",
            },
        }

    if result.success:
        return {
            "success": True,
            "run_id": result.run_id,
            "pipeline": name,
            "duration": round(result.duration, 2),
            "steps": {
                step_id: {
                    "status": s.status,
                    "duration": round(s.duration, 2),
                    "items": s.items,
                    "errors": s.errors,
                }
                for step_id, s in result.steps.items()
            },
            "result": result.result,
        }
    else:
        # Find the first failed step for the error report
        failed_step = next(
            (sid for sid, s in result.steps.items() if s.status == "error"),
            None,
        )
        return {
            "success": False,
            "run_id": result.run_id,
            "pipeline": name,
            "duration": round(result.duration, 2),
            "steps": {
                step_id: {
                    "status": s.status,
                    "duration": round(s.duration, 2),
                    "items": s.items,
                    "errors": s.errors,
                }
                for step_id, s in result.steps.items()
            },
            "error": {
                "code": "STEP_FAILED",
                "message": f"Pipeline failed at step: {failed_step or 'unknown'}",
                "step_id": failed_step,
                "recoverable": True,
                "agent_actions": ["retry_step", "skip_step", "abort_pipeline"],
                "resume_command": (
                    f"brix__run_pipeline({{\"pipeline_id\": \"{name}\", "
                    f"\"resume_from\": \"{failed_step}\"}})"
                    if failed_step else None
                ),
            },
        }


async def _handle_get_run_status(arguments: dict) -> dict:
    """Get the status of a specific run by run_id."""
    run_id = arguments.get("run_id", "")
    history = RunHistory()
    run = history.get_run(run_id)

    if run is None:
        return {
            "success": False,
            "error": f"Run '{run_id}' not found in history.",
        }

    # SQLite stores success as 0/1 integer — normalise to bool
    run_data = dict(run)
    if "success" in run_data:
        run_data["success"] = bool(run_data["success"])

    return {
        "success": True,
        **run_data,
    }


async def _handle_get_run_history(arguments: dict) -> dict:
    """Get recent run history."""
    limit = int(arguments.get("limit", 10))
    pipeline_name = arguments.get("pipeline_name")

    history = RunHistory()
    runs = history.get_recent(limit=limit)

    if pipeline_name:
        runs = [r for r in runs if r.get("pipeline") == pipeline_name]

    return {
        "success": True,
        "runs": runs,
        "total": len(runs),
    }


async def _handle_get_template(arguments: dict) -> dict:
    """Return a pipeline template matching the goal, or list all templates."""
    from brix.templates.catalog import get_template, list_templates

    goal = arguments.get("goal", "")
    if not goal:
        # Return all templates
        return {"templates": list_templates()}

    template = get_template(goal)
    if template:
        return {
            "name": template["name"],
            "description": template["description"],
            "customization_points": template["customization_points"],
            "pipeline": template["pipeline"],
        }

    return {"error": f"No template found for: {goal}", "available": list_templates()}


async def _handle_list_pipelines(arguments: dict) -> dict:
    """List all pipeline YAML files."""
    directory = arguments.get("directory")
    if directory:
        # Explicit directory: scan that single directory only
        search_dir = Path(directory)
        pipelines = []
        if search_dir.exists():
            for yaml_file in sorted(search_dir.glob("*.yaml")):
                try:
                    with open(yaml_file) as f:
                        data = yaml.safe_load(f) or {}
                    steps = data.get("steps", [])
                    pipelines.append({
                        "name": data.get("name", yaml_file.stem),
                        "version": data.get("version", ""),
                        "description": data.get("description", ""),
                        "step_count": len(steps),
                        "file": str(yaml_file),
                    })
                except Exception as exc:
                    pipelines.append({
                        "name": yaml_file.stem,
                        "error": str(exc),
                        "file": str(yaml_file),
                    })
        return {
            "success": True,
            "pipelines": pipelines,
            "total": len(pipelines),
            "directory": str(search_dir),
        }
    else:
        # No directory specified: use PipelineStore with current PIPELINE_DIR
        # (respects monkeypatching) and multi-path search
        store = PipelineStore(pipelines_dir=PIPELINE_DIR)
        all_pipelines = store.list_all()
        # Normalise field names to match the explicit-dir branch
        pipelines = [
            {
                "name": p["name"],
                "version": p.get("version", ""),
                "description": p.get("description", ""),
                "step_count": p.get("steps", 0),
                "file": p.get("path", ""),
            }
            for p in all_pipelines
        ]
        return {
            "success": True,
            "pipelines": pipelines,
            "total": len(pipelines),
            "directory": "multi-path",
        }


# Dispatch table — core tools only.
# Pipeline tools (brix__pipeline__*) are handled dynamically in call_tool.
_HANDLERS = {
    "brix__get_tips": _handle_get_tips,
    "brix__list_bricks": _handle_list_bricks,
    "brix__search_bricks": _handle_search_bricks,
    "brix__get_brick_schema": _handle_get_brick_schema,
    "brix__create_pipeline": _handle_create_pipeline,
    "brix__get_pipeline": _handle_get_pipeline,
    "brix__add_step": _handle_add_step,
    "brix__remove_step": _handle_remove_step,
    "brix__validate_pipeline": _handle_validate_pipeline,
    "brix__run_pipeline": _handle_run_pipeline,
    "brix__get_run_status": _handle_get_run_status,
    "brix__get_run_history": _handle_get_run_history,
    "brix__list_pipelines": _handle_list_pipelines,
    "brix__get_template": _handle_get_template,
}


# ---------------------------------------------------------------------------
# V2-09: Pipeline tool execution handler
# ---------------------------------------------------------------------------

async def _handle_pipeline_tool(pipeline_name: str, arguments: dict) -> dict:
    """Execute a named pipeline via its auto-exposed MCP tool."""
    try:
        data = _load_pipeline_yaml(pipeline_name)
    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": {
                "code": "PIPELINE_NOT_FOUND",
                "message": str(exc),
                "step_id": None,
                "recoverable": False,
                "agent_actions": ["list_pipelines", "create_pipeline"],
                "resume_command": None,
            },
        }

    try:
        pipeline_yaml = yaml.dump(data)
        pipeline = _loader.load_from_string(pipeline_yaml)
    except Exception as exc:
        return {
            "success": False,
            "error": {
                "code": "PIPELINE_PARSE_ERROR",
                "message": str(exc),
                "step_id": None,
                "recoverable": True,
                "agent_actions": ["validate_pipeline", "fix_pipeline_yaml"],
                "resume_command": f"brix__validate_pipeline({{\"pipeline_id\": \"{pipeline_name}\"}})",
            },
        }

    engine = PipelineEngine()
    try:
        result = await engine.run(pipeline, arguments)
    except Exception as exc:
        return {
            "success": False,
            "error": {
                "code": "ENGINE_ERROR",
                "message": str(exc),
                "step_id": None,
                "recoverable": True,
                "agent_actions": ["retry_pipeline", "validate_pipeline"],
                "resume_command": f"brix__pipeline__{pipeline_name.replace('-', '_')}({{}})",
            },
        }

    if result.success:
        return {
            "success": True,
            "run_id": result.run_id,
            "pipeline": pipeline_name,
            "duration": round(result.duration, 2),
            "steps": {
                step_id: {
                    "status": s.status,
                    "duration": round(s.duration, 2),
                    "items": s.items,
                    "errors": s.errors,
                }
                for step_id, s in result.steps.items()
            },
            "result": result.result,
        }
    else:
        failed_step = next(
            (sid for sid, s in result.steps.items() if s.status == "error"),
            None,
        )
        return {
            "success": False,
            "run_id": result.run_id,
            "pipeline": pipeline_name,
            "duration": round(result.duration, 2),
            "steps": {
                step_id: {
                    "status": s.status,
                    "duration": round(s.duration, 2),
                    "items": s.items,
                    "errors": s.errors,
                }
                for step_id, s in result.steps.items()
            },
            "error": {
                "code": "STEP_FAILED",
                "message": f"Pipeline failed at step: {failed_step or 'unknown'}",
                "step_id": failed_step,
                "recoverable": True,
                "agent_actions": ["retry_step", "skip_step", "abort_pipeline"],
                "resume_command": (
                    f"brix__pipeline__{pipeline_name.replace('-', '_')}({{}})"
                    if failed_step else None
                ),
            },
        }


# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------

def create_server(store: PipelineStore = None) -> Server:
    """Create and configure the Brix MCP server.

    V2-09: Pipeline tools (brix__pipeline__*) are built dynamically at
    list_tools() time so newly saved pipelines are immediately visible
    without a server restart.
    """
    server = Server("brix")
    _pipeline_store = store or _store

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        # Core tools + dynamically built pipeline tools
        pipeline_tools = _build_pipeline_tools(_pipeline_store)
        return BRIX_TOOLS + pipeline_tools

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
        # Core tool dispatch
        handler = _HANDLERS.get(name)
        if handler is not None:
            result = await handler(arguments or {})
            return [types.TextContent(type="text", text=json.dumps(result))]

        # V2-09: Dynamic pipeline tool dispatch
        if name.startswith(PIPELINE_TOOL_PREFIX):
            # Extract pipeline name from tool name
            # brix__pipeline__my_pipeline → my_pipeline → try my_pipeline and my-pipeline
            raw = name[len(PIPELINE_TOOL_PREFIX):]
            # Try exact name first, then with dashes (reverse of the _→ mapping)
            for pipeline_name in [raw, raw.replace("_", "-")]:
                if _pipeline_store.exists(pipeline_name):
                    result = await _handle_pipeline_tool(pipeline_name, arguments or {})
                    return [types.TextContent(type="text", text=json.dumps(result))]
            # Not found — return structured error
            result = {
                "success": False,
                "error": {
                    "code": "PIPELINE_NOT_FOUND",
                    "message": f"Pipeline for tool '{name}' not found.",
                    "recoverable": False,
                    "agent_actions": ["list_pipelines"],
                },
            }
            return [types.TextContent(type="text", text=json.dumps(result))]

        raise ValueError(f"Unknown tool: {name}")

    return server


async def run_mcp_server() -> None:
    """Run the Brix MCP server using stdio transport."""
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        init_options = server.create_initialization_options()
        await server.run(read_stream, write_stream, init_options)


async def run_mcp_http_server(host: str = "0.0.0.0", port: int = 8091) -> None:
    """Run Brix MCP server with HTTP/SSE transport (V2-12).

    Starts a Starlette ASGI app backed by StreamableHTTPSessionManager.
    Clients connect via:
      - GET/POST /mcp   — StreamableHTTP (recommended, supports resumability)
      - GET /sse        — legacy SSE endpoint (for older MCP clients)
      - POST /messages  — legacy POST endpoint for SSE messages

    The server listens on ``host:port`` (default 0.0.0.0:8091).
    """
    import uvicorn
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import Response
    from starlette.routing import Mount, Route
    from starlette.types import Receive, Scope, Send

    from mcp.server.sse import SseServerTransport
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

    mcp_server = create_server()

    # --- Streamable HTTP (primary, modern transport) ---
    session_manager = StreamableHTTPSessionManager(
        app=mcp_server,
        event_store=None,   # no resumability — stateful sessions only
        json_response=False,
        stateless=False,
    )

    async def handle_streamable_http(scope: Scope, receive: Receive, send: Send) -> None:
        await session_manager.handle_request(scope, receive, send)

    # --- SSE (legacy, for older MCP clients) ---
    sse_transport = SseServerTransport("/messages")

    async def handle_sse(request: Request) -> None:
        async with sse_transport.connect_sse(
            request.scope, request.receive, request._send  # type: ignore[attr-defined]
        ) as streams:
            read_stream, write_stream = streams
            init_options = mcp_server.create_initialization_options()
            await mcp_server.run(read_stream, write_stream, init_options)

    async def handle_messages(scope: Scope, receive: Receive, send: Send) -> None:
        await sse_transport.handle_post_message(scope, receive, send)

    @contextlib.asynccontextmanager
    async def lifespan(app):  # type: ignore[type-arg]
        async with session_manager.run():
            logger.info("Brix MCP HTTP server started on %s:%d", host, port)
            yield
        logger.info("Brix MCP HTTP server stopped")

    app = Starlette(
        lifespan=lifespan,
        routes=[
            # Streamable HTTP — primary transport
            Mount("/mcp", app=handle_streamable_http),
            # Legacy SSE transport
            Route("/sse", endpoint=handle_sse),
            Mount("/messages", app=handle_messages),
        ],
    )

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server_instance = uvicorn.Server(config)
    await server_instance.serve()
