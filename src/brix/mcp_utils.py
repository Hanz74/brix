"""MCP utility functions — extracted from mcp_tools_schema.py during DB-First migration.

Contains the _inject_source_param utility and _SOURCE_SCHEMA_PROPERTY that are
needed by the MCP server regardless of whether tool schemas come from DB or code.
"""

_SOURCE_SCHEMA_PROPERTY: dict = {
    "type": "object",
    "description": (
        "Optional caller identity — expected on every MCP call. "
        "Fields: session (pipeline/session name), model (LLM model id), "
        "agent (agent name). Used for audit logging. "
        "Example: {\"session\": \"buddy-session\", \"model\": \"opus\", \"agent\": \"agent-alpha\"}"
    ),
    "properties": {
        "session": {"type": "string", "description": "Pipeline or session name that triggered this call."},
        "model": {"type": "string", "description": "LLM model identifier (e.g. 'opus', 'sonnet')."},
        "agent": {"type": "string", "description": "Agent name (e.g. 'agent-alpha')."},
    },
    "additionalProperties": True,
}


def _inject_source_param(tools: list) -> list:
    """Add the optional 'source' parameter to every tool's inputSchema."""
    for tool in tools:
        schema = tool.inputSchema
        if isinstance(schema, dict):
            props = schema.setdefault("properties", {})
            if "source" not in props:
                props["source"] = _SOURCE_SCHEMA_PROPERTY
    return tools
