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
import uuid as _uuid_mod
from pathlib import Path

import yaml

from mcp.server.lowlevel import Server, NotificationOptions
from mcp.server.stdio import stdio_server
from mcp.server.models import InitializationOptions
import mcp.types as types

logger = logging.getLogger(__name__)

from brix.bricks.registry import BrickRegistry
from brix.db import BrixDB
from brix.helper_registry import HelperRegistry
from brix.loader import PipelineLoader
from brix.validator import PipelineValidator
from brix.history import RunHistory
from brix.engine import PipelineEngine
from brix.mcp_pool import McpConnectionPool
from brix.pipeline_store import PipelineStore
from brix.credential_store import CredentialStore, CredentialNotFoundError, CREDENTIAL_TYPES
from brix.mcp_utils import _inject_source_param  # noqa: F401

# Import shared singletons and utilities from mcp_handlers._shared.
# These are defined there to avoid circular imports and allow sub-modules
# to share state. mcp_server.py re-exports them for backward compatibility.
from brix.mcp_handlers._shared import (
    _registry,
    _loader,
    _validator,
    _store,
    _audit_db,
    _background_runs,
    BACKGROUND_RUN_TIMEOUT_SECONDS,
    _WATCHDOG_INTERVAL_SECONDS,
    _watchdog_task,
    _background_run_watchdog,
    _ensure_watchdog,
    _pipeline_path,
)


# Default pipeline directory (primary save target, kept for backward compat)
PIPELINE_DIR = Path.home() / ".brix" / "pipelines"


# Backward-compatible BRIX_TOOLS — loaded from DB at import time.
# Falls back to empty list if DB is not yet seeded.
def _load_brix_tools_compat() -> list[types.Tool]:
    """Load BRIX_TOOLS from DB for backward compatibility."""
    try:
        db = BrixDB()
        rows = db.mcp_tool_schemas_list()
        if rows:
            import json as _json_compat
            tools: list[types.Tool] = []
            for row in rows:
                raw_schema = row.get("input_schema", "{}")
                if isinstance(raw_schema, str):
                    try:
                        input_schema = _json_compat.loads(raw_schema)
                    except Exception:
                        input_schema = {}
                else:
                    input_schema = raw_schema or {}
                tools.append(types.Tool(
                    name=row["name"],
                    description=row.get("description", ""),
                    inputSchema=input_schema,
                ))
            _inject_source_param(tools)
            return tools
    except Exception:
        pass
    return []

BRIX_TOOLS: list[types.Tool] = _load_brix_tools_compat()


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
    # Inject source parameter into dynamically built pipeline tools too
    _inject_source_param(tools)
    return tools


# ---------------------------------------------------------------------------
# Handler imports — all _handle_* functions live in brix.mcp_handlers.*
# mcp_server.py re-exports them so that existing imports and monkeypatches
# on this module continue to work.
# ---------------------------------------------------------------------------
from brix.mcp_handlers.help import (
    _handle_get_tips,
    _handle_get_help,
)
from brix.mcp_handlers.steps import (
    _handle_list_bricks,
    _handle_search_bricks,
    _handle_get_brick_schema,
    _handle_add_step,
    _handle_remove_step,
    _handle_update_step,
    _handle_get_step,
    _handle_auto_fix_step,
)
from brix.mcp_handlers.pipelines import (
    _handle_create_pipeline,
    _handle_get_pipeline,
    _handle_update_pipeline,
    _handle_delete_pipeline,
    _handle_rename_pipeline,
    _handle_validate_pipeline,
    _handle_list_pipelines,
    _handle_search_pipelines,
    _handle_get_versions,
    _handle_rollback,
    _handle_diff_versions,
    _handle_get_template,
    _handle_test_pipeline,
)
from brix.mcp_handlers.runs import (
    _handle_run_pipeline,
    _handle_get_run_status,
    _handle_diff_runs,
    _handle_get_run_errors,
    _handle_get_run_log,
    _handle_get_run_history,
    _handle_get_timeline,
    _handle_run_annotate,
    _handle_run_search,
    _handle_delete_run,
    _handle_cancel_run,
    _handle_resume_run,
    _handle_inspect_context,
    _handle_replay_step,
    _handle_get_step_data,
)
from brix.mcp_handlers.helpers import (
    _handle_create_helper,
    _handle_register_helper,
    _handle_list_helpers,
    _handle_get_helper,
    _handle_search_helpers,
    _handle_update_helper,
    _handle_delete_helper,
    _handle_rename_helper,
)
from brix.mcp_handlers.org import _handle_org
from brix.mcp_handlers.credentials import (
    _handle_credential_add,
    _handle_credential_list,
    _handle_credential_get,
    _handle_credential_update,
    _handle_credential_delete,
    _handle_credential_rotate,
    _handle_credential_search,
)
from brix.mcp_handlers.alerts import (
    _handle_alert_add,
    _handle_alert_list,
    _handle_alert_update,
    _handle_alert_delete,
    _handle_alert_history,
    _handle_get_alert_rule,
    _handle_search_alert_rules,
)
from brix.mcp_handlers.triggers import (
    _handle_trigger_add,
    _handle_trigger_list,
    _handle_trigger_get,
    _handle_trigger_update,
    _handle_trigger_delete,
    _handle_trigger_test,
    _handle_scheduler_status,
    _handle_scheduler_start,
    _handle_scheduler_stop,
    _handle_trigger_group_add,
    _handle_trigger_group_list,
    _handle_trigger_group_delete,
    _handle_trigger_group_start,
    _handle_trigger_group_stop,
    _handle_trigger_group_get,
    _handle_trigger_group_update,
    _handle_search_trigger_groups,
    _handle_search_triggers,
    _auto_start_scheduler_if_needed,
)
from brix.mcp_handlers.insights import (
    _handle_diagnose_run,
    _handle_get_insights,
    _handle_get_proactive_suggestions,
    _handle_check_resource,
    _handle_claim_resource,
    _handle_release_resource,
    _handle_db_status,
)
from brix.mcp_handlers.health import _handle_health
from brix.mcp_handlers.backup import (
    _handle_backup,
    _handle_restore,
    _handle_backup_list,
)
from brix.mcp_handlers.state import (
    _handle_save_agent_context,
    _handle_restore_agent_context,
    _handle_state_set,
    _handle_state_get,
    _handle_state_list,
    _handle_state_delete,
)
from brix.mcp_handlers.variables import (
    _handle_set_variable,
    _handle_get_variable,
    _handle_list_variables,
    _handle_delete_variable,
    _handle_store_set,
    _handle_store_get,
    _handle_store_list,
    _handle_store_delete,
    _handle_search_variables,
)
from brix.mcp_handlers.servers import (
    _handle_server_add,
    _handle_server_list,
    _handle_server_update,
    _handle_server_remove,
    _handle_server_refresh,
    _handle_server_health,
)
from brix.mcp_handlers.registry import (
    _handle_registry_add,
    _handle_registry_get,
    _handle_registry_list,
    _handle_registry_update,
    _handle_registry_delete,
    _handle_registry_search,
)
from brix.mcp_handlers.composer import (
    _handle_compose_pipeline,
    _handle_plan_pipeline,
)
from brix.mcp_handlers.connectors import (
    _handle_list_connectors,
    _handle_get_connector,
    _handle_connector_status,
)
from brix.mcp_handlers.templates import (
    _handle_list_templates,
    _handle_instantiate_template,
)
from brix.mcp_handlers.profiles import (
    _handle_create_profile,
    _handle_get_profile,
    _handle_list_profiles,
    _handle_update_profile,
    _handle_delete_profile,
    _handle_search_profiles,
)
from brix.mcp_handlers.connections import (
    _handle_connection_add,
    _handle_connection_list,
    _handle_connection_test,
    _handle_connection_delete,
    _handle_get_connection,
    _handle_update_connection,
    _handle_search_connections,
)
from brix.mcp_handlers.bricks import (
    _handle_create_brick,
    _handle_update_brick,
    _handle_delete_brick,
)
from brix.mcp_handlers.discover import (
    _handle_discover,
    _handle_list_runners,
    _handle_get_runner_info,
    _handle_list_env_config,
    _handle_list_types,
    _handle_list_namespaces,
)
from brix.migration_templates import analyze_migration as _analyze_migration_fn
from brix.mcp_handlers.testing import (
    _handle_pin_step_data,
    _handle_unpin_step_data,
    _handle_list_pins,
)
# Re-export shared utilities so that existing imports/monkeypatches on
# brix.mcp_server continue to work (e.g. from brix.mcp_server import _load_pipeline_yaml)
from brix.mcp_handlers._shared import (
    _load_pipeline_yaml,
    _save_pipeline_yaml,
    _background_runs,
    _extract_source,
    _source_summary,
    _normalize_name,
    _name_similarity,
    _description_jaccard,
    _find_similar_helpers,
    _find_similar_pipelines,
    _code_line_count,
    _find_step_recursive,
    _validate_python_code,
    _managed_helper_dir,
    _scan_pipelines_for_helper,
    _scan_pipelines_for_sub_pipeline,
    _re_module_name,
)



async def _handle_analyze_migration(arguments: dict) -> dict:
    """MCP handler for brix__analyze_migration (T-BRIX-DB-10).

    Returns migration analysis for helpers: single_brick / pipeline / not_convertible.
    """
    helper_name: str | None = arguments.get("helper_name") or None
    return _analyze_migration_fn(helper_name=helper_name)


# ---------------------------------------------------------------------------
# V8-12: Consolidated CRUD-group dispatchers
# ---------------------------------------------------------------------------

_INVALID_ACTION = "invalid_action"


async def _handle_registry(arguments: dict) -> dict:
    """Dispatcher for brix__registry — routes to individual registry handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_registry_add(arguments)
    elif action == "get":
        return await _handle_registry_get(arguments)
    elif action == "list":
        return await _handle_registry_list(arguments)
    elif action == "update":
        return await _handle_registry_update(arguments)
    elif action == "delete":
        return await _handle_registry_delete(arguments)
    elif action == "search":
        return await _handle_registry_search(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, get, list, update, delete, search."}


async def _handle_trigger(arguments: dict) -> dict:
    """Dispatcher for brix__trigger — routes to individual trigger handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_trigger_add(arguments)
    elif action == "get":
        return await _handle_trigger_get(arguments)
    elif action == "list":
        return await _handle_trigger_list(arguments)
    elif action == "update":
        return await _handle_trigger_update(arguments)
    elif action == "delete":
        return await _handle_trigger_delete(arguments)
    elif action == "test":
        return await _handle_trigger_test(arguments)
    elif action == "search":
        return await _handle_search_triggers(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, get, list, update, delete, test, search."}


async def _handle_credential(arguments: dict) -> dict:
    """Dispatcher for brix__credential — routes to individual credential handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_credential_add(arguments)
    elif action == "get":
        return await _handle_credential_get(arguments)
    elif action == "list":
        return await _handle_credential_list(arguments)
    elif action == "update":
        return await _handle_credential_update(arguments)
    elif action == "delete":
        return await _handle_credential_delete(arguments)
    elif action == "rotate":
        return await _handle_credential_rotate(arguments)
    elif action == "search":
        return await _handle_credential_search(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, get, list, update, delete, rotate, search."}


async def _handle_alert(arguments: dict) -> dict:
    """Dispatcher for brix__alert — routes to individual alert handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_alert_add(arguments)
    elif action == "get":
        return await _handle_get_alert_rule(arguments)
    elif action == "list":
        return await _handle_alert_list(arguments)
    elif action == "update":
        return await _handle_alert_update(arguments)
    elif action == "delete":
        return await _handle_alert_delete(arguments)
    elif action == "history":
        return await _handle_alert_history(arguments)
    elif action == "search":
        return await _handle_search_alert_rules(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, get, list, update, delete, history, search."}


async def _handle_server(arguments: dict) -> dict:
    """Dispatcher for brix__server — routes to individual server handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_server_add(arguments)
    elif action == "list":
        return await _handle_server_list(arguments)
    elif action == "update":
        return await _handle_server_update(arguments)
    elif action == "remove":
        return await _handle_server_remove(arguments)
    elif action == "health":
        return await _handle_server_health(arguments)
    elif action == "refresh":
        return await _handle_server_refresh(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, list, update, remove, health, refresh."}


async def _handle_state(arguments: dict) -> dict:
    """Dispatcher for brix__state — routes to individual state handlers."""
    action = arguments.get("action", "")
    if action == "get":
        return await _handle_state_get(arguments)
    elif action == "set":
        return await _handle_state_set(arguments)
    elif action == "list":
        return await _handle_state_list(arguments)
    elif action == "delete":
        return await _handle_state_delete(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: get, set, list, delete."}


async def _handle_trigger_group(arguments: dict) -> dict:
    """Dispatcher for brix__trigger_group — routes to individual trigger group handlers."""
    action = arguments.get("action", "")
    if action == "add":
        return await _handle_trigger_group_add(arguments)
    elif action == "get":
        return await _handle_trigger_group_get(arguments)
    elif action == "list":
        return await _handle_trigger_group_list(arguments)
    elif action == "update":
        return await _handle_trigger_group_update(arguments)
    elif action == "search":
        return await _handle_search_trigger_groups(arguments)
    elif action == "start":
        return await _handle_trigger_group_start(arguments)
    elif action == "stop":
        return await _handle_trigger_group_stop(arguments)
    elif action == "delete":
        return await _handle_trigger_group_delete(arguments)
    else:
        return {"success": False, "error": f"Unknown action '{action}'. Valid actions: add, get, list, update, search, start, stop, delete."}


# Dispatch table — core tools only.
# Pipeline tools (brix__pipeline__*) are handled dynamically in call_tool.
_HANDLERS = {
    "brix__get_tips": _handle_get_tips,
    "brix__get_help": _handle_get_help,
    "brix__list_bricks": _handle_list_bricks,
    "brix__search_bricks": _handle_search_bricks,
    "brix__get_brick_schema": _handle_get_brick_schema,
    "brix__create_pipeline": _handle_create_pipeline,
    "brix__get_pipeline": _handle_get_pipeline,
    "brix__add_step": _handle_add_step,
    "brix__remove_step": _handle_remove_step,
    "brix__update_step": _handle_update_step,
    "brix__update_pipeline": _handle_update_pipeline,
    "brix__validate_pipeline": _handle_validate_pipeline,
    "brix__run_pipeline": _handle_run_pipeline,
    "brix__get_run_status": _handle_get_run_status,
    "brix__diff_runs": _handle_diff_runs,
    "brix__get_run_errors": _handle_get_run_errors,
    "brix__get_run_log": _handle_get_run_log,
    "brix__get_run_history": _handle_get_run_history,
    "brix__get_timeline": _handle_get_timeline,  # T-BRIX-V7-07
    "brix__list_pipelines": _handle_list_pipelines,
    "brix__get_template": _handle_get_template,
    "brix__create_helper": _handle_create_helper,
    "brix__register_helper": _handle_register_helper,
    "brix__list_helpers": _handle_list_helpers,
    "brix__get_helper": _handle_get_helper,
    "brix__search_helpers": _handle_search_helpers,
    "brix__update_helper": _handle_update_helper,
    "brix__delete_pipeline": _handle_delete_pipeline,
    "brix__get_step": _handle_get_step,
    "brix__delete_helper": _handle_delete_helper,
    "brix__delete_run": _handle_delete_run,
    "brix__cancel_run": _handle_cancel_run,
    # Credential Store — consolidated (V8-12)
    "brix__credential": _handle_credential,
    # Object Versioning (T-BRIX-V5-07)
    "brix__get_versions": _handle_get_versions,
    "brix__rollback": _handle_rollback,
    "brix__diff_versions": _handle_diff_versions,
    # Alerting — consolidated (V8-12)
    "brix__alert": _handle_alert,
    # T-BRIX-V5-11 — CRUD gap fillers
    "brix__search_pipelines": _handle_search_pipelines,
    "brix__run_annotate": _handle_run_annotate,
    "brix__run_search": _handle_run_search,
    # MCP Server Management — consolidated (V8-12)
    "brix__server": _handle_server,
    # Trigger CRUD — consolidated (V8-12)
    "brix__trigger": _handle_trigger,
    "brix__scheduler_status": _handle_scheduler_status,
    "brix__scheduler_start": _handle_scheduler_start,
    "brix__scheduler_stop": _handle_scheduler_stop,
    # Trigger Groups — consolidated (V8-12)
    "brix__trigger_group": _handle_trigger_group,
    # Rename + Test Pipeline (T-BRIX-V5-13)
    "brix__rename_pipeline": _handle_rename_pipeline,
    "brix__rename_helper": _handle_rename_helper,
    "brix__test_pipeline": _handle_test_pipeline,
    # Agent Intelligence (T-BRIX-V6-07 / V6-08 / V6-09)
    "brix__diagnose_run": _handle_diagnose_run,
    "brix__auto_fix_step": _handle_auto_fix_step,
    "brix__get_insights": _handle_get_insights,
    "brix__get_proactive_suggestions": _handle_get_proactive_suggestions,
    # Agent State (T-BRIX-V6-10 / V6-11 / V6-12)
    "brix__save_agent_context": _handle_save_agent_context,
    "brix__restore_agent_context": _handle_restore_agent_context,
    "brix__claim_resource": _handle_claim_resource,
    "brix__check_resource": _handle_check_resource,
    "brix__release_resource": _handle_release_resource,
    # Blackboard — consolidated (V8-12)
    "brix__state": _handle_state,
    # Managed Variables (T-BRIX-DB-13)
    "brix__set_variable": _handle_set_variable,
    "brix__get_variable": _handle_get_variable,
    "brix__list_variables": _handle_list_variables,
    "brix__delete_variable": _handle_delete_variable,
    # T-BRIX-CRUD-01: variable search
    "brix__search_variables": _handle_search_variables,
    # Persistent Data Store (T-BRIX-DB-13)
    "brix__store_set": _handle_store_set,
    "brix__store_get": _handle_store_get,
    "brix__store_list": _handle_store_list,
    "brix__store_delete": _handle_store_delete,
    # Debug tools: Step-Replay, Breakpoints, Live Context Inspector (T-BRIX-V7-06)
    **{k: v for k, v in __import__("brix.debug_tools", fromlist=["DEBUG_TOOLS_HANDLERS"]).DEBUG_TOOLS_HANDLERS.items()},
    # Registry System — consolidated (V8-12)
    "brix__registry": _handle_registry,
    # Intent-to-Pipeline Assembly (T-BRIX-V8-01)
    "brix__compose_pipeline": _handle_compose_pipeline,
    # Formalized Reason Phase (T-BRIX-V8-02)
    "brix__plan_pipeline": _handle_plan_pipeline,
    # Source-Connector-Abstraktion (T-BRIX-V8-04)
    "brix__list_connectors": _handle_list_connectors,
    "brix__get_connector": _handle_get_connector,
    "brix__connector_status": _handle_connector_status,
    # Pipeline-Templates: Parametrisierte Blueprints (T-BRIX-V8-08)
    "brix__list_templates": _handle_list_templates,
    "brix__instantiate_template": _handle_instantiate_template,
    # Named DB-Connections (T-BRIX-DB-05b)
    "brix__connection_add": _handle_connection_add,
    "brix__connection_list": _handle_connection_list,
    "brix__connection_test": _handle_connection_test,
    "brix__connection_delete": _handle_connection_delete,
    # T-BRIX-CRUD-01: new connection handlers
    "brix__connection_get": _handle_get_connection,
    "brix__connection_update": _handle_update_connection,
    "brix__connection_search": _handle_search_connections,
    # Custom Bricks (T-BRIX-DB-20)
    "brix__create_brick": _handle_create_brick,
    "brix__update_brick": _handle_update_brick,
    "brix__delete_brick": _handle_delete_brick,
    # Run-Persistenz: Step Execution Data (T-BRIX-DB-07)
    "brix__get_step_data": _handle_get_step_data,
    # Universal Registry (T-BRIX-DB-19)
    "brix__discover": _handle_discover,
    "brix__list_runners": _handle_list_runners,
    "brix__get_runner_info": _handle_get_runner_info,
    "brix__list_env_config": _handle_list_env_config,
    "brix__list_types": _handle_list_types,
    "brix__list_namespaces": _handle_list_namespaces,
    # Helper Migration Analysis (T-BRIX-DB-10)
    "brix__analyze_migration": _handle_analyze_migration,
    # DB Schema Status (T-BRIX-DB-27)
    "brix__db_status": _handle_db_status,
    # Health Check — Gesamt-Status (T-BRIX-DB-25)
    "brix__health": _handle_health,
    # Backup / Restore (T-BRIX-DB-28)
    "brix__backup": _handle_backup,
    "brix__restore": _handle_restore,
    "brix__backup_list": _handle_backup_list,
    # Pipeline Testing — Step Pins / Mock Data (T-BRIX-DB-24)
    "brix__pin_step_data": _handle_pin_step_data,
    "brix__unpin_step_data": _handle_unpin_step_data,
    "brix__list_pins": _handle_list_pins,
    # Org Registry — project/tag/group definitions (T-BRIX-ORG-02)
    "brix__org": _handle_org,
    # T-BRIX-CRUD-01: Profiles CRUD + search
    "brix__create_profile": _handle_create_profile,
    "brix__get_profile": _handle_get_profile,
    "brix__list_profiles": _handle_list_profiles,
    "brix__update_profile": _handle_update_profile,
    "brix__delete_profile": _handle_delete_profile,
    "brix__search_profiles": _handle_search_profiles,
    # T-BRIX-CRUD-01: standalone alert get + search
    "brix__get_alert_rule": _handle_get_alert_rule,
    "brix__search_alert_rules": _handle_search_alert_rules,
    # T-BRIX-CRUD-01: standalone trigger group get/update/search + trigger search
    "brix__trigger_group_get": _handle_trigger_group_get,
    "brix__trigger_group_update": _handle_trigger_group_update,
    "brix__search_trigger_groups": _handle_search_trigger_groups,
    "brix__search_triggers": _handle_search_triggers,
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
# DB-First MCP tool schema loader (T-BRIX-DB-06)
# ---------------------------------------------------------------------------

def _load_mcp_tools_from_db() -> list[types.Tool]:
    """Load MCP tool schemas from DB (DB-First — no code fallback)."""
    try:
        db = BrixDB()
        rows = db.mcp_tool_schemas_list()
        if rows:
            import json as _json
            tools: list[types.Tool] = []
            for row in rows:
                raw_schema = row.get("input_schema", "{}")
                if isinstance(raw_schema, str):
                    try:
                        input_schema = _json.loads(raw_schema)
                    except Exception:
                        input_schema = {}
                else:
                    input_schema = raw_schema or {}
                tools.append(types.Tool(
                    name=row["name"],
                    description=row.get("description", ""),
                    inputSchema=input_schema,
                ))
            # Inject source param into DB-loaded tools too
            _inject_source_param(tools)
            return tools
    except Exception:
        pass
    # DB empty or error — return empty list (seed_if_empty should populate)
    logger.warning("No MCP tool schemas found in DB — tools list will be empty until seeded")
    return []


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
        # Core tools — from DB if available, else from code
        core_tools = _load_mcp_tools_from_db()
        # Dynamically built pipeline tools
        pipeline_tools = _build_pipeline_tools(_pipeline_store)
        return core_tools + pipeline_tools

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
    # T-BRIX-DB-06: Seed DB tables on first start
    try:
        from brix.seed import seed_if_empty
        _seed_db = BrixDB()
        seed_if_empty(_seed_db)
    except Exception as _seed_err:
        logger.warning("DB seeding failed (non-fatal): %s", _seed_err)

    # T-BRIX-INT-01: Run integrity checks after seeding
    try:
        from brix.integrity import run_integrity_checks
        _integrity_db = BrixDB()
        _integrity_result = run_integrity_checks(_integrity_db)
        if not _integrity_result["ok"]:
            _n = len(_integrity_result["issues"])
            logger.warning(
                "integrity: %d issue(s) detected at startup (see logs above)", _n
            )
    except Exception as _int_err:
        logger.warning("integrity checks failed (non-fatal): %s", _int_err)

    server = create_server()
    # T-BRIX-V6-BUG-01: Auto-start scheduler if enabled triggers exist
    await _auto_start_scheduler_if_needed()
    async with stdio_server() as (read_stream, write_stream):
        # T-BRIX-V6-05: declare claude/channel experimental capability
        init_options = server.create_initialization_options(
            experimental_capabilities={"claude/channel": {}}
        )
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
            # T-BRIX-V6-05: declare claude/channel experimental capability
            init_options = mcp_server.create_initialization_options(
                experimental_capabilities={"claude/channel": {}}
            )
            await mcp_server.run(read_stream, write_stream, init_options)

    async def handle_messages(scope: Scope, receive: Receive, send: Send) -> None:
        await sse_transport.handle_post_message(scope, receive, send)

    @contextlib.asynccontextmanager
    async def lifespan(app):  # type: ignore[type-arg]
        async with session_manager.run():
            logger.info("Brix MCP HTTP server started on %s:%d", host, port)
            # T-BRIX-V6-BUG-01: Auto-start scheduler if enabled triggers exist
            await _auto_start_scheduler_if_needed()
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
