"""Tests for T-BRIX-BRICK-GROUP: group parameter on brick MCP tools.

Uses the session-level isolated DB from conftest.py (BRIX_DB_PATH env var).
"""
import pytest
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_create_args(name="custom.group_test_brick", **kwargs):
    return {
        "name": name,
        "runner": "python",
        "description": "A test brick for group tests",
        "namespace": "custom",
        "category": "custom",
        **kwargs,
    }


def _get_db():
    """Return a BrixDB using the current BRIX_DB_PATH (test-isolated)."""
    from brix.db import BrixDB
    return BrixDB()


def _fresh_registry():
    """Return a BrickRegistry backed by the current isolated DB."""
    from brix.bricks.registry import BrickRegistry
    db = _get_db()
    return BrickRegistry(db=db)


# ---------------------------------------------------------------------------
# create_brick with group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_brick_with_group():
    """create_brick sets group_name in DB when group parameter is provided."""
    from brix.mcp_handlers.bricks import _handle_create_brick

    args = _make_create_args(name="custom.group_create_test", group="my-group")
    result = await _handle_create_brick(args)

    assert result["success"] is True
    assert result.get("group") == "my-group"

    db = _get_db()
    row = db.brick_definitions_get("custom.group_create_test")
    assert row is not None
    assert row.get("group_name") == "my-group"


@pytest.mark.asyncio
async def test_create_brick_without_group():
    """create_brick without group leaves group_name empty."""
    from brix.mcp_handlers.bricks import _handle_create_brick

    args = _make_create_args(name="custom.group_create_nogroup")
    result = await _handle_create_brick(args)

    assert result["success"] is True
    assert "group" not in result

    db = _get_db()
    row = db.brick_definitions_get("custom.group_create_nogroup")
    assert row is not None
    assert row.get("group_name", "") == ""


# ---------------------------------------------------------------------------
# update_brick with group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_brick_group():
    """update_brick changes group_name in DB."""
    from brix.mcp_handlers.bricks import _handle_create_brick, _handle_update_brick

    brick_name = "custom.group_update_test"
    await _handle_create_brick(_make_create_args(name=brick_name, group="initial-group"))

    result = await _handle_update_brick({"name": brick_name, "group": "new-group"})
    assert result["success"] is True
    assert result.get("group") == "new-group"

    db = _get_db()
    row = db.brick_definitions_get(brick_name)
    assert row is not None
    assert row.get("group_name") == "new-group"


@pytest.mark.asyncio
async def test_update_brick_preserves_existing_group():
    """update_brick without group preserves the existing group_name."""
    from brix.mcp_handlers.bricks import _handle_create_brick, _handle_update_brick

    brick_name = "custom.group_preserve_test"
    await _handle_create_brick(_make_create_args(name=brick_name, group="keep-this"))

    # Update description only — no group arg
    result = await _handle_update_brick({
        "name": brick_name,
        "description": "Updated description",
    })
    assert result["success"] is True

    db = _get_db()
    row = db.brick_definitions_get(brick_name)
    assert row is not None
    assert row.get("group_name") == "keep-this"


# ---------------------------------------------------------------------------
# list_bricks includes group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_bricks_includes_group():
    """list_bricks response includes 'group' field for bricks with group_name set."""
    from brix.mcp_handlers.bricks import _handle_create_brick
    from brix.mcp_handlers.steps import _handle_list_bricks

    brick_name = "custom.group_list_test"
    await _handle_create_brick(_make_create_args(name=brick_name, group="listed-group"))

    # Use fresh registry with the test DB so the brick is visible
    registry = _fresh_registry()
    with patch("brix.mcp_handlers.steps._registry", registry):
        result = await _handle_list_bricks({})

    assert result["success"] is True
    found = next((b for b in result["bricks"] if b["name"] == brick_name), None)
    assert found is not None, f"Brick '{brick_name}' should appear in list_bricks"
    assert found.get("group") == "listed-group"


@pytest.mark.asyncio
async def test_list_bricks_group_empty_by_default():
    """list_bricks returns empty group for bricks without group_name."""
    from brix.mcp_handlers.bricks import _handle_create_brick
    from brix.mcp_handlers.steps import _handle_list_bricks

    brick_name = "custom.group_list_empty_test"
    await _handle_create_brick(_make_create_args(name=brick_name))

    registry = _fresh_registry()
    with patch("brix.mcp_handlers.steps._registry", registry):
        result = await _handle_list_bricks({})

    found = next((b for b in result["bricks"] if b["name"] == brick_name), None)
    assert found is not None
    assert found.get("group", "") == ""


# ---------------------------------------------------------------------------
# search_bricks includes group
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_bricks_includes_group():
    """search_bricks response includes 'group' field."""
    from brix.mcp_handlers.bricks import _handle_create_brick
    from brix.mcp_handlers.steps import _handle_search_bricks

    brick_name = "custom.group_searchable_brick"
    await _handle_create_brick(_make_create_args(
        name=brick_name,
        description="A uniquely searchable brick for group test xyz123",
        group="search-group",
    ))

    registry = _fresh_registry()
    with patch("brix.mcp_handlers.steps._registry", registry):
        result = await _handle_search_bricks({"query": "uniquely searchable"})

    assert result["success"] is True
    found = next((b for b in result["bricks"] if b["name"] == brick_name), None)
    assert found is not None, f"Brick '{brick_name}' should appear in search_bricks"
    assert found.get("group") == "search-group"
