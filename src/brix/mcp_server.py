"""Brix MCP Server — stdio transport.

Exposes Brix pipeline management and execution capabilities as MCP tools.
Tool implementations are stubs in this version (Wave 2 foundation).
Real logic is added in V2-05 through V2-07.
"""
import json
import asyncio

from mcp.server.lowlevel import Server, NotificationOptions
from mcp.server.stdio import stdio_server
from mcp.server.models import InitializationOptions
import mcp.types as types


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
            "Create a new empty pipeline definition. "
            "Use this as the first step when building a pipeline. "
            "Returns the pipeline ID and initial structure ready for steps to be added."
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
            "Get the current definition of a pipeline by ID or name. "
            "Use to inspect pipeline structure before running or modifying it. "
            "Returns full pipeline YAML-like structure with all steps and config."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "pipeline_id": {
                    "type": "string",
                    "description": "Pipeline ID (UUID) or pipeline name.",
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
                    "description": "Target pipeline ID.",
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
                    "description": "Target pipeline ID.",
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
                    "description": "Pipeline ID to validate.",
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
                    "description": "Pipeline ID to execute.",
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
                    "description": "Directory to search for pipeline files (default: /app/pipelines).",
                },
            },
            "required": [],
        },
    ),
]


# ---------------------------------------------------------------------------
# Stub handlers
# ---------------------------------------------------------------------------

async def _handle_get_tips(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__get_tips",
    }


async def _handle_list_bricks(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__list_bricks",
    }


async def _handle_search_bricks(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__search_bricks",
    }


async def _handle_get_brick_schema(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__get_brick_schema",
    }


async def _handle_create_pipeline(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__create_pipeline",
    }


async def _handle_get_pipeline(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__get_pipeline",
    }


async def _handle_add_step(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__add_step",
    }


async def _handle_remove_step(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__remove_step",
    }


async def _handle_validate_pipeline(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__validate_pipeline",
    }


async def _handle_run_pipeline(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__run_pipeline",
    }


async def _handle_get_run_status(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__get_run_status",
    }


async def _handle_get_run_history(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__get_run_history",
    }


async def _handle_list_pipelines(arguments: dict) -> dict:
    return {
        "status": "not_implemented",
        "tool": "brix__list_pipelines",
    }


# Dispatch table
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
}


# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------

def create_server() -> Server:
    """Create and configure the Brix MCP server."""
    server = Server("brix")

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return BRIX_TOOLS

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
        handler = _HANDLERS.get(name)
        if handler is None:
            raise ValueError(f"Unknown tool: {name}")
        result = await handler(arguments or {})
        return [types.TextContent(type="text", text=json.dumps(result))]

    return server


async def run_mcp_server() -> None:
    """Run the Brix MCP server using stdio transport."""
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        init_options = server.create_initialization_options()
        await server.run(read_stream, write_stream, init_options)
