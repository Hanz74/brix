"""Brick schema definitions — Pydantic models that export to JSON Schema."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class BrickParam(BaseModel):
    """A single parameter in a brick's config schema."""

    type: str  # "string", "integer", "boolean", "array", "object"
    description: str = ""
    default: Any = None
    required: bool = False
    enum: list[Any] | None = None


class BrickSchema(BaseModel):
    """Schema definition for a brick type."""

    name: str
    type: str  # "http", "cli", "python", "mcp", "filter", "transform", "file", "pipeline"
    description: str
    when_to_use: str  # Agent-optimiert: wann diesen Brick nutzen
    when_NOT_to_use: str = ""  # Agent-optimiert: wann diesen Brick NICHT nutzen
    category: str = "general"  # für search_bricks Filterung
    aliases: list[str] = []  # Suchbegriffe (deutsch + englisch)
    input_type: str = ""   # Was als Input erwartet wird (z.B. "none", "list[email]")
    output_type: str = ""  # Was als Output kommt (z.B. "list[email]")
    config_schema: dict[str, BrickParam] = {}
    input_description: str = ""
    output_description: str = ""
    examples: list[dict] = []  # Beispielkonfigurationen mit "goal" + "config"
    related_connector: str = ""  # Verknüpfung zu einem Connector

    # Brick-First Engine fields (T-BRIX-DB-05c)
    runner: str = ""  # Which runner executes this brick (e.g. "python", "http", "mcp")
    system: bool = False  # System bricks are built-in and cannot be deleted
    namespace: str = ""  # Logical namespace: "flow", "db", "source", "action", "extract", "script", "http", "mcp"

    # Brick-Inheritance (T-BRIX-DB-23): name of the parent brick this brick extends.
    # At get() time, child fields are merged over the parent schema — child values override.
    extends: str | None = None

    def to_json_schema(self) -> dict:
        """Export config as JSON Schema for MCP tool parameter definition."""
        properties: dict[str, Any] = {}
        required: list[str] = []

        for param_name, param in self.config_schema.items():
            prop: dict[str, Any] = {
                "type": param.type,
                "description": param.description,
            }
            if param.default is not None:
                prop["default"] = param.default
            if param.enum:
                prop["enum"] = param.enum
            properties[param_name] = prop
            if param.required:
                required.append(param_name)

        schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
        }
        if required:
            schema["required"] = required
        return schema
