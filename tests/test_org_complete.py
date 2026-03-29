"""Comprehensive tests for project/tags/group across ALL entities.

Tests that create, get, list, and update consistently handle org fields
for: helpers, variables, triggers, brick_definitions, connections, profiles.
"""
from __future__ import annotations

import json
import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    """Isolated BrixDB with all migrations applied."""
    d = BrixDB(db_path=tmp_path / "org_complete_test.db")
    run_pending_migrations(d)
    return d


@pytest.fixture
def patch_db(tmp_path, monkeypatch, db):
    """Patch global DB path so handlers use the test DB."""
    import brix.db as db_mod
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", db.db_path)
    return db


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_helper_create_with_org(tmp_path, monkeypatch, patch_db):
    """create_helper with project/tags/group persists and returns them."""
    from brix.mcp_handlers.helpers import _handle_create_helper

    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    result = await _handle_create_helper({
        "name": "org_helper",
        "code": "print('hello')",
        "description": "test helper",
        "project": "buddy",
        "tags": ["email", "import"],
        "group": "intake",
    })

    assert result["success"] is True
    assert result.get("project") == "buddy"
    # No MISSING PROJECT warning expected
    warnings = result.get("warnings", [])
    assert not any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_helper_create_without_project_warns(tmp_path, monkeypatch, patch_db):
    """create_helper without project should warn."""
    from brix.mcp_handlers.helpers import _handle_create_helper

    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    result = await _handle_create_helper({
        "name": "no_project_helper",
        "code": "print('hello')",
        "description": "test",
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_helper_get_returns_org(tmp_path, monkeypatch, patch_db):
    """get_helper returns project/tags/group."""
    from brix.mcp_handlers.helpers import _handle_create_helper, _handle_get_helper

    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    await _handle_create_helper({
        "name": "get_org_helper",
        "code": "print('hello')",
        "description": "test",
        "project": "cody",
        "tags": ["tool"],
        "group": "dev",
    })

    result = await _handle_get_helper({"name": "get_org_helper"})
    assert result["success"] is True
    helper = result["helper"]
    assert helper["project"] == "cody"
    assert helper["tags"] == ["tool"]
    assert helper["group"] == "dev"


@pytest.mark.asyncio
async def test_helper_list_returns_org(tmp_path, monkeypatch, patch_db):
    """list_helpers returns project/tags/group per item."""
    from brix.mcp_handlers.helpers import _handle_create_helper, _handle_list_helpers

    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    await _handle_create_helper({
        "name": "list_org_helper",
        "code": "print('hello')",
        "description": "test",
        "project": "mailpilot",
        "tags": ["ml"],
    })

    result = await _handle_list_helpers({})
    assert result["success"] is True
    helpers = result["helpers"]
    h = next((h for h in helpers if h["name"] == "list_org_helper"), None)
    assert h is not None
    assert h["project"] == "mailpilot"
    assert h["tags"] == ["ml"]


@pytest.mark.asyncio
async def test_helper_update_org(tmp_path, monkeypatch, patch_db):
    """update_helper can change project/tags/group."""
    from brix.mcp_handlers.helpers import (
        _handle_create_helper,
        _handle_update_helper,
        _handle_get_helper,
    )

    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    await _handle_create_helper({
        "name": "update_org_helper",
        "code": "print('v1')",
        "description": "test",
        "project": "old",
    })

    result = await _handle_update_helper({
        "name": "update_org_helper",
        "project": "new_project",
        "tags": ["updated"],
        "group": "new_group",
    })
    assert result["success"] is True
    assert "project" in result["updated_fields"]
    assert "tags" in result["updated_fields"]
    assert "group" in result["updated_fields"]

    # Verify persistence
    get_result = await _handle_get_helper({"name": "update_org_helper"})
    helper = get_result["helper"]
    assert helper["project"] == "new_project"
    assert helper["tags"] == ["updated"]
    assert helper["group"] == "new_group"


# ---------------------------------------------------------------------------
# VARIABLES
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_variable_set_with_org(patch_db):
    """set_variable with project/tags/group persists them."""
    from brix.mcp_handlers.variables import _handle_set_variable

    result = await _handle_set_variable({
        "name": "org_var",
        "value": "test_value",
        "project": "buddy",
        "tags": ["config"],
        "group": "settings",
    })

    assert result["set"] is True
    assert result.get("project") == "buddy"
    assert result.get("tags") == ["config"]
    assert result.get("group") == "settings"
    # No MISSING PROJECT warning expected
    warnings = result.get("warnings", [])
    assert not any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_variable_set_without_project_warns(patch_db):
    """set_variable without project should warn."""
    from brix.mcp_handlers.variables import _handle_set_variable

    result = await _handle_set_variable({
        "name": "no_project_var",
        "value": "val",
    })

    assert result["set"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_variable_get_returns_org(patch_db):
    """get_variable returns project/tags/group."""
    from brix.mcp_handlers.variables import _handle_set_variable, _handle_get_variable

    await _handle_set_variable({
        "name": "get_org_var",
        "value": "val",
        "project": "cody",
        "tags": ["secret"],
        "group": "auth",
    })

    result = await _handle_get_variable({"name": "get_org_var"})
    assert result["found"] is True
    assert result["project"] == "cody"
    assert result["tags"] == ["secret"]
    assert result["group"] == "auth"


@pytest.mark.asyncio
async def test_variable_list_returns_org(patch_db):
    """list_variables returns project/tags/group per item."""
    from brix.mcp_handlers.variables import _handle_set_variable, _handle_list_variables

    await _handle_set_variable({
        "name": "list_org_var",
        "value": "val",
        "project": "mailpilot",
        "tags": ["env"],
    })

    result = await _handle_list_variables({})
    variables = result["variables"]
    v = next((v for v in variables if v["name"] == "list_org_var"), None)
    assert v is not None
    assert v["project"] == "mailpilot"
    assert v["tags"] == ["env"]


@pytest.mark.asyncio
async def test_variable_list_filter_by_project(patch_db):
    """list_variables with project filter works."""
    from brix.mcp_handlers.variables import _handle_set_variable, _handle_list_variables

    await _handle_set_variable({"name": "filter_var_a", "value": "a", "project": "alpha"})
    await _handle_set_variable({"name": "filter_var_b", "value": "b", "project": "beta"})

    result = await _handle_list_variables({"project": "alpha"})
    variables = result["variables"]
    names = [v["name"] for v in variables]
    assert "filter_var_a" in names
    assert "filter_var_b" not in names


# ---------------------------------------------------------------------------
# TRIGGERS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_add_with_org(patch_db):
    """trigger_add with project/tags/group persists them."""
    from brix.mcp_handlers.triggers import _handle_trigger_add

    result = await _handle_trigger_add({
        "name": "org_trigger",
        "type": "file",
        "pipeline": "test-pipe",
        "config": {"path": "/tmp"},
        "project": "buddy",
        "tags": ["watcher"],
        "group": "intake",
    })

    assert result["success"] is True
    trigger = result["trigger"]
    assert trigger["project"] == "buddy"
    assert trigger["tags"] == ["watcher"]
    assert trigger["group_name"] == "intake"


@pytest.mark.asyncio
async def test_trigger_add_without_project_warns(patch_db):
    """trigger_add without project should warn."""
    from brix.mcp_handlers.triggers import _handle_trigger_add

    result = await _handle_trigger_add({
        "name": "no_project_trigger",
        "type": "file",
        "pipeline": "test-pipe",
        "config": {},
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_trigger_get_returns_org(patch_db):
    """trigger_get returns project/tags/group."""
    from brix.mcp_handlers.triggers import _handle_trigger_add, _handle_trigger_get

    await _handle_trigger_add({
        "name": "get_org_trigger",
        "type": "file",
        "pipeline": "test-pipe",
        "config": {},
        "project": "cody",
        "tags": ["monitor"],
    })

    result = await _handle_trigger_get({"name": "get_org_trigger"})
    assert result["success"] is True
    trigger = result["trigger"]
    assert trigger["project"] == "cody"
    assert trigger["tags"] == ["monitor"]


@pytest.mark.asyncio
async def test_trigger_list_returns_org(patch_db):
    """trigger_list returns project/tags/group per item."""
    from brix.mcp_handlers.triggers import _handle_trigger_add, _handle_trigger_list

    await _handle_trigger_add({
        "name": "list_org_trigger",
        "type": "file",
        "pipeline": "test-pipe",
        "config": {},
        "project": "mailpilot",
        "tags": ["event"],
    })

    result = await _handle_trigger_list({})
    triggers = result["triggers"]
    t = next((t for t in triggers if t["name"] == "list_org_trigger"), None)
    assert t is not None
    assert t["project"] == "mailpilot"
    assert t["tags"] == ["event"]


@pytest.mark.asyncio
async def test_trigger_update_org(patch_db):
    """trigger_update can change project/tags/group."""
    from brix.mcp_handlers.triggers import (
        _handle_trigger_add,
        _handle_trigger_update,
        _handle_trigger_get,
    )

    await _handle_trigger_add({
        "name": "update_org_trigger",
        "type": "file",
        "pipeline": "test-pipe",
        "config": {},
        "project": "old",
    })

    result = await _handle_trigger_update({
        "name": "update_org_trigger",
        "project": "new_project",
        "tags": ["updated"],
        "group": "new_group",
    })
    assert result["success"] is True

    # Verify persistence
    get_result = await _handle_trigger_get({"name": "update_org_trigger"})
    trigger = get_result["trigger"]
    assert trigger["project"] == "new_project"
    assert trigger["tags"] == ["updated"]
    assert trigger["group_name"] == "new_group"


# ---------------------------------------------------------------------------
# BRICK DEFINITIONS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_brick_create_with_tags(patch_db, monkeypatch):
    """create_brick with tags persists them."""
    from brix.mcp_handlers.bricks import _handle_create_brick

    # Mock runner validation
    monkeypatch.setattr(
        "brix.mcp_handlers.bricks._get_valid_runners",
        lambda: {"python", "http", "cli"},
    )
    # Mock registry refresh
    import brix.mcp_handlers._shared as _shared_mod
    from unittest.mock import MagicMock
    mock_reg = MagicMock()
    monkeypatch.setattr(_shared_mod, "_registry", mock_reg)

    result = await _handle_create_brick({
        "name": "org_brick",
        "runner": "python",
        "description": "test brick",
        "namespace": "buddy",
        "tags": ["transform", "data"],
    })

    assert result["success"] is True
    assert result.get("tags") == ["transform", "data"]
    # No MISSING PROJECT warning expected (namespace acts as project for bricks)
    warnings = result.get("warnings", [])
    assert not any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_brick_create_without_namespace_warns(patch_db, monkeypatch):
    """create_brick without namespace should warn MISSING PROJECT."""
    from brix.mcp_handlers.bricks import _handle_create_brick

    monkeypatch.setattr(
        "brix.mcp_handlers.bricks._get_valid_runners",
        lambda: {"python", "http", "cli"},
    )
    import brix.mcp_handlers._shared as _shared_mod
    from unittest.mock import MagicMock
    mock_reg = MagicMock()
    monkeypatch.setattr(_shared_mod, "_registry", mock_reg)

    result = await _handle_create_brick({
        "name": "no_ns_brick",
        "runner": "python",
        "description": "test brick",
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_brick_list_returns_tags(patch_db, monkeypatch):
    """list_bricks returns org_tags per item from DB."""
    from brix.mcp_handlers.bricks import _handle_create_brick

    monkeypatch.setattr(
        "brix.mcp_handlers.bricks._get_valid_runners",
        lambda: {"python", "http", "cli"},
    )
    import brix.mcp_handlers._shared as _shared_mod
    from unittest.mock import MagicMock
    mock_reg = MagicMock()
    monkeypatch.setattr(_shared_mod, "_registry", mock_reg)

    await _handle_create_brick({
        "name": "list_tag_brick",
        "runner": "python",
        "description": "test",
        "namespace": "test",
        "tags": ["special"],
    })

    # Verify via DB directly
    db = BrixDB()
    row = db.brick_definitions_get("list_tag_brick")
    assert row is not None
    assert row["org_tags"] == ["special"]


@pytest.mark.asyncio
async def test_brick_update_tags(patch_db, monkeypatch):
    """update_brick can change tags."""
    from brix.mcp_handlers.bricks import _handle_create_brick, _handle_update_brick

    monkeypatch.setattr(
        "brix.mcp_handlers.bricks._get_valid_runners",
        lambda: {"python", "http", "cli"},
    )
    import brix.mcp_handlers._shared as _shared_mod
    from unittest.mock import MagicMock
    mock_reg = MagicMock()
    monkeypatch.setattr(_shared_mod, "_registry", mock_reg)

    await _handle_create_brick({
        "name": "update_tag_brick",
        "runner": "python",
        "description": "test",
        "namespace": "test",
        "tags": ["old"],
    })

    result = await _handle_update_brick({
        "name": "update_tag_brick",
        "tags": ["new_tag", "updated"],
    })
    assert result["success"] is True
    assert result.get("tags") == ["new_tag", "updated"]

    # Verify via DB
    db = BrixDB()
    row = db.brick_definitions_get("update_tag_brick")
    assert row["org_tags"] == ["new_tag", "updated"]


# ---------------------------------------------------------------------------
# CONNECTIONS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connection_add_with_org(patch_db, monkeypatch):
    """connection_add with project/tags/group persists them."""
    from brix.mcp_handlers.connections import _handle_connection_add

    result = await _handle_connection_add({
        "name": "org_conn",
        "dsn": "sqlite:///tmp/test.db",
        "driver": "sqlite",
        "description": "test",
        "project": "buddy",
        "tags": ["database"],
        "group": "data",
    })

    assert result["success"] is True
    assert result.get("project") == "buddy"
    assert result.get("tags") == ["database"]
    assert result.get("group") == "data"


@pytest.mark.asyncio
async def test_connection_add_without_project_warns(patch_db, monkeypatch):
    """connection_add without project should warn."""
    from brix.mcp_handlers.connections import _handle_connection_add

    result = await _handle_connection_add({
        "name": "no_project_conn",
        "dsn": "sqlite:///tmp/test2.db",
        "driver": "sqlite",
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_connection_list_returns_org(patch_db, monkeypatch):
    """connection_list returns project/tags/group per item."""
    from brix.mcp_handlers.connections import _handle_connection_add, _handle_connection_list

    await _handle_connection_add({
        "name": "list_org_conn",
        "dsn": "sqlite:///tmp/test3.db",
        "driver": "sqlite",
        "project": "mailpilot",
        "tags": ["sql"],
    })

    result = await _handle_connection_list({})
    assert result["success"] is True
    connections = result["connections"]
    c = next((c for c in connections if c["name"] == "list_org_conn"), None)
    assert c is not None
    assert c["project"] == "mailpilot"
    assert c["tags"] == ["sql"]


# ---------------------------------------------------------------------------
# PROFILES
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_profile_create_with_org(patch_db):
    """create_profile with project/tags/group persists them."""
    from brix.mcp_handlers.profiles import _handle_create_profile

    result = await _handle_create_profile({
        "name": "org_profile",
        "config": {"timeout": 30},
        "description": "test profile",
        "project": "buddy",
        "tags": ["config"],
        "group": "defaults",
    })

    assert result["success"] is True
    assert result.get("project") == "buddy"
    assert result.get("tags") == ["config"]


@pytest.mark.asyncio
async def test_profile_create_without_project_warns(patch_db):
    """create_profile without project should warn."""
    from brix.mcp_handlers.profiles import _handle_create_profile

    result = await _handle_create_profile({
        "name": "no_project_profile",
        "config": {"x": 1},
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_profile_get_returns_org(patch_db):
    """get_profile returns project/tags/group."""
    from brix.mcp_handlers.profiles import _handle_create_profile, _handle_get_profile

    await _handle_create_profile({
        "name": "get_org_profile",
        "config": {"y": 2},
        "project": "cody",
        "tags": ["dev"],
        "group": "testing",
    })

    result = await _handle_get_profile({"name": "get_org_profile"})
    assert result["success"] is True
    assert result["project"] == "cody"
    assert result["tags"] == ["dev"]
    assert result["group_name"] == "testing"


@pytest.mark.asyncio
async def test_profile_list_returns_org(patch_db):
    """list_profiles returns project/tags/group per item."""
    from brix.mcp_handlers.profiles import _handle_create_profile, _handle_list_profiles

    await _handle_create_profile({
        "name": "list_org_profile",
        "config": {"z": 3},
        "project": "mailpilot",
        "tags": ["prod"],
    })

    result = await _handle_list_profiles({})
    assert result["success"] is True
    profiles = result["profiles"]
    p = next((p for p in profiles if p["name"] == "list_org_profile"), None)
    assert p is not None
    assert p["project"] == "mailpilot"
    assert p["tags"] == ["prod"]


@pytest.mark.asyncio
async def test_profile_update_org(patch_db):
    """update_profile can change project/tags/group."""
    from brix.mcp_handlers.profiles import (
        _handle_create_profile,
        _handle_update_profile,
        _handle_get_profile,
    )

    await _handle_create_profile({
        "name": "update_org_profile",
        "config": {"a": 1},
        "project": "old",
    })

    result = await _handle_update_profile({
        "name": "update_org_profile",
        "project": "new_project",
        "tags": ["updated"],
        "group": "new_group",
    })
    assert result["success"] is True

    # Verify persistence
    get_result = await _handle_get_profile({"name": "update_org_profile"})
    assert get_result["project"] == "new_project"
    assert get_result["tags"] == ["updated"]
    assert get_result["group_name"] == "new_group"


# ---------------------------------------------------------------------------
# DB-level migration tests
# ---------------------------------------------------------------------------


def test_migrations_add_all_org_columns(db):
    """Verify all org columns exist after migrations."""
    import sqlite3

    with db._connect() as conn:
        # Helpers
        assert db._column_exists(conn, "helpers", "project")
        assert db._column_exists(conn, "helpers", "tags")
        assert db._column_exists(conn, "helpers", "group_name")
        # Variables
        assert db._column_exists(conn, "variables", "project")
        assert db._column_exists(conn, "variables", "tags")
        assert db._column_exists(conn, "variables", "group_name")
        # Triggers
        assert db._column_exists(conn, "triggers", "project")
        assert db._column_exists(conn, "triggers", "tags")
        assert db._column_exists(conn, "triggers", "group_name")
        # Brick definitions
        assert db._column_exists(conn, "brick_definitions", "org_tags")
        assert db._column_exists(conn, "brick_definitions", "project")
        assert db._column_exists(conn, "brick_definitions", "group_name")
        # Connections
        assert db._column_exists(conn, "connections", "project")
        assert db._column_exists(conn, "connections", "tags")
        assert db._column_exists(conn, "connections", "group_name")
        # Profiles
        assert db._column_exists(conn, "profiles", "project")
        assert db._column_exists(conn, "profiles", "tags")
        assert db._column_exists(conn, "profiles", "group_name")
