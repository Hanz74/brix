"""Test that brix__org tool schema in seed-data.json has all handler parameters (T-BRIX-BUG-14)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def org_tool_schema():
    """Load the brix__org tool schema from seed-data.json."""
    seed = Path(__file__).resolve().parent.parent / "seed-data.json"
    data = json.loads(seed.read_text())
    tools = data.get("mcp_tool_schemas") or []
    for t in tools:
        if t["name"] == "brix__org":
            return t["input_schema"]
    pytest.fail("brix__org not found in seed-data.json")


EXPECTED_PARAMS = {"action", "type", "name", "description", "pipelines"}


def test_org_schema_has_all_handler_params(org_tool_schema):
    """Every parameter read by _handle_org must appear in the tool schema."""
    props = set(org_tool_schema.get("properties", {}).keys())
    missing = EXPECTED_PARAMS - props
    assert not missing, f"Missing parameters in brix__org schema: {missing}"


def test_org_schema_action_required(org_tool_schema):
    """action should be listed as required."""
    assert "action" in org_tool_schema.get("required", [])


def test_org_schema_action_has_enum(org_tool_schema):
    """action should have an enum constraint."""
    action = org_tool_schema["properties"]["action"]
    assert set(action.get("enum", [])) == {"create", "list", "delete", "seed"}


def test_org_schema_type_has_enum(org_tool_schema):
    """type should have an enum constraint."""
    typ = org_tool_schema["properties"]["type"]
    assert set(typ.get("enum", [])) == {"project", "tag", "group"}
