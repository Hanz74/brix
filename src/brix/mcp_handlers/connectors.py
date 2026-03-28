"""Connector MCP handler module (T-BRIX-V8-04).

Handlers for brix__list_connectors, brix__get_connector, brix__connector_status.
"""
from __future__ import annotations

from brix.connectors import (
    CONNECTOR_REGISTRY,
    _get_registry,
    get_connector,
    list_connectors,
    connector_status,
)


async def _handle_list_connectors(arguments: dict) -> dict:
    """List all available source connectors.

    Returns a summary list with name, type, description, required_mcp_server,
    and parameter count for each connector.
    """
    type_filter = arguments.get("type_filter")

    connectors = list_connectors(type_filter=type_filter or None)

    items = []
    for c in connectors:
        items.append({
            "name": c.name,
            "type": c.type,
            "description": c.description,
            "required_mcp_server": c.required_mcp_server,
            "required_mcp_tools": c.required_mcp_tools,
            "parameter_count": len(c.parameters),
            "required_parameter_count": sum(1 for p in c.parameters if p.required),
            "related_pipelines": c.related_pipelines,
            "related_helpers": c.related_helpers,
        })

    return {
        "success": True,
        "count": len(items),
        "type_filter": type_filter,
        "connectors": items,
    }


async def _handle_get_connector(arguments: dict) -> dict:
    """Get full details of a connector including output schema and parameters."""
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    connector = get_connector(name)
    if connector is None:
        available = sorted(_get_registry().keys())
        return {
            "success": False,
            "error": f"Connector '{name}' not found.",
            "available_connectors": available,
        }

    params = [
        {
            "name": p.name,
            "type": p.type,
            "description": p.description,
            "required": p.required,
            "default": p.default,
        }
        for p in connector.parameters
    ]

    return {
        "success": True,
        "connector": {
            "name": connector.name,
            "type": connector.type,
            "description": connector.description,
            "required_mcp_server": connector.required_mcp_server,
            "required_mcp_tools": connector.required_mcp_tools,
            "output_schema": connector.output_schema,
            "parameters": params,
            "related_pipelines": connector.related_pipelines,
            "related_helpers": connector.related_helpers,
        },
    }


async def _handle_connector_status(arguments: dict) -> dict:
    """Check whether a connector's dependencies (MCP server, credentials) are available."""
    name = arguments.get("name", "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    status = connector_status(name)
    return {"success": True, **status}
