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
    category: str = "general"  # für search_bricks Filterung
    config_schema: dict[str, BrickParam] = {}
    input_description: str = ""
    output_description: str = ""

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
