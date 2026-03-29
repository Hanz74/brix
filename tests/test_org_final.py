"""Comprehensive org-field tests for ALL 15 entity types.

Tests that create/add, get, and list consistently handle project/tags/group
for: pipelines, helpers, variables, triggers, trigger_groups, brick_definitions,
connections, profiles, alert_rules, and all 6 registry types.
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
    d = BrixDB(db_path=tmp_path / "org_final_test.db")
    run_pending_migrations(d)
    return d


@pytest.fixture
def patch_db(tmp_path, monkeypatch, db):
    """Patch global DB path so handlers use the test DB."""
    import brix.db as db_mod
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", db.db_path)
    return db


# ---------------------------------------------------------------------------
# 1. PIPELINES
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_create_without_project_warns(tmp_path, monkeypatch, patch_db):
    """create_pipeline without project should warn MISSING PROJECT."""
    from brix.mcp_handlers.pipelines import _handle_create_pipeline
    monkeypatch.setattr(
        "brix.mcp_handlers._shared._pipeline_dir",
        lambda: tmp_path / "pipelines",
    )
    (tmp_path / "pipelines").mkdir(exist_ok=True)

    result = await _handle_create_pipeline({"name": "test-pipe-no-proj", "steps": []})
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)
    assert any("MISSING DESCRIPTION" in w for w in warnings)


@pytest.mark.asyncio
async def test_pipeline_get_returns_org(tmp_path, monkeypatch, patch_db):
    """get_pipeline returns project/tags/group."""
    from brix.mcp_handlers.pipelines import _handle_create_pipeline, _handle_get_pipeline
    monkeypatch.setattr(
        "brix.mcp_handlers._shared._pipeline_dir",
        lambda: tmp_path / "pipelines",
    )
    (tmp_path / "pipelines").mkdir(exist_ok=True)

    await _handle_create_pipeline({
        "name": "org-get-pipe",
        "steps": [],
        "description": "test",
        "project": "buddy",
        "tags": ["import"],
        "group": "intake",
    })

    result = await _handle_get_pipeline({"pipeline_id": "org-get-pipe"})
    assert result.get("project") == "buddy"
    assert result.get("tags") == ["import"]
    assert result.get("group") == "intake"


@pytest.mark.asyncio
async def test_pipeline_list_returns_org(tmp_path, monkeypatch, patch_db):
    """list_pipelines returns project/tags/group per item."""
    from brix.mcp_handlers.pipelines import _handle_create_pipeline, _handle_list_pipelines
    monkeypatch.setattr(
        "brix.mcp_handlers._shared._pipeline_dir",
        lambda: tmp_path / "pipelines",
    )
    (tmp_path / "pipelines").mkdir(exist_ok=True)

    await _handle_create_pipeline({
        "name": "org-list-pipe",
        "steps": [],
        "description": "test",
        "project": "cody",
        "tags": ["ci"],
    })

    result = await _handle_list_pipelines({})
    pipelines = result["pipelines"]
    p = next((p for p in pipelines if p["name"] == "org-list-pipe"), None)
    assert p is not None
    assert p["project"] == "cody"
    assert p["tags"] == ["ci"]


# ---------------------------------------------------------------------------
# 2. HELPERS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_helper_create_without_project_warns(tmp_path, monkeypatch, patch_db):
    from brix.mcp_handlers.helpers import _handle_create_helper
    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    result = await _handle_create_helper({
        "name": "no_proj_helper",
        "code": "print('hello')",
    })
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)
    assert any("MISSING DESCRIPTION" in w for w in warnings)


@pytest.mark.asyncio
async def test_helper_get_returns_org(tmp_path, monkeypatch, patch_db):
    from brix.mcp_handlers.helpers import _handle_create_helper, _handle_get_helper
    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    await _handle_create_helper({
        "name": "org_get_h",
        "code": "print('hi')",
        "description": "test",
        "project": "buddy",
        "tags": ["util"],
        "group": "tools",
    })

    result = await _handle_get_helper({"name": "org_get_h"})
    helper = result["helper"]
    assert helper["project"] == "buddy"
    assert helper["tags"] == ["util"]
    assert helper["group"] == "tools"


@pytest.mark.asyncio
async def test_helper_list_returns_org(tmp_path, monkeypatch, patch_db):
    from brix.mcp_handlers.helpers import _handle_create_helper, _handle_list_helpers
    monkeypatch.setattr(
        "brix.mcp_handlers._shared._managed_helper_dir",
        lambda: tmp_path / "helpers",
    )
    (tmp_path / "helpers").mkdir(exist_ok=True)

    await _handle_create_helper({
        "name": "org_list_h",
        "code": "print('hi')",
        "description": "test",
        "project": "mailpilot",
        "tags": ["ml"],
    })

    result = await _handle_list_helpers({})
    helpers = result["helpers"]
    h = next((h for h in helpers if h["name"] == "org_list_h"), None)
    assert h is not None
    assert h["project"] == "mailpilot"
    assert h["tags"] == ["ml"]


# ---------------------------------------------------------------------------
# 3. VARIABLES
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_variable_create_without_project_warns(patch_db):
    from brix.mcp_handlers.variables import _handle_set_variable

    result = await _handle_set_variable({"name": "no_proj_var", "value": "x"})
    assert result["set"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)
    assert any("MISSING DESCRIPTION" in w for w in warnings)


@pytest.mark.asyncio
async def test_variable_get_returns_org(patch_db):
    from brix.mcp_handlers.variables import _handle_set_variable, _handle_get_variable

    await _handle_set_variable({
        "name": "org_get_var",
        "value": "v",
        "description": "test",
        "project": "cody",
        "tags": ["env"],
        "group": "config",
    })

    result = await _handle_get_variable({"name": "org_get_var"})
    assert result["project"] == "cody"
    assert result["tags"] == ["env"]
    assert result["group"] == "config"


@pytest.mark.asyncio
async def test_variable_list_returns_org(patch_db):
    from brix.mcp_handlers.variables import _handle_set_variable, _handle_list_variables

    await _handle_set_variable({
        "name": "org_list_var",
        "value": "v",
        "description": "test",
        "project": "buddy",
        "tags": ["secret"],
    })

    result = await _handle_list_variables({})
    variables = result["variables"]
    v = next((v for v in variables if v["name"] == "org_list_var"), None)
    assert v is not None
    assert v["project"] == "buddy"
    assert v["tags"] == ["secret"]


# ---------------------------------------------------------------------------
# 4. TRIGGERS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_create_without_project_warns(patch_db):
    from brix.mcp_handlers.triggers import _handle_trigger_add

    result = await _handle_trigger_add({
        "name": "no_proj_trig",
        "type": "file",
        "pipeline": "test-pipe",
        "config": {},
    })
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_trigger_get_returns_org(patch_db):
    from brix.mcp_handlers.triggers import _handle_trigger_add, _handle_trigger_get

    await _handle_trigger_add({
        "name": "org_get_trig",
        "type": "file",
        "pipeline": "test-pipe",
        "config": {},
        "project": "cody",
        "tags": ["watch"],
    })

    result = await _handle_trigger_get({"name": "org_get_trig"})
    trigger = result["trigger"]
    assert trigger["project"] == "cody"
    assert trigger["tags"] == ["watch"]


@pytest.mark.asyncio
async def test_trigger_list_returns_org(patch_db):
    from brix.mcp_handlers.triggers import _handle_trigger_add, _handle_trigger_list

    await _handle_trigger_add({
        "name": "org_list_trig",
        "type": "file",
        "pipeline": "test-pipe",
        "config": {},
        "project": "buddy",
        "tags": ["event"],
    })

    result = await _handle_trigger_list({})
    triggers = result["triggers"]
    t = next((t for t in triggers if t["name"] == "org_list_trig"), None)
    assert t is not None
    assert t["project"] == "buddy"
    assert t["tags"] == ["event"]


# ---------------------------------------------------------------------------
# 5. TRIGGER GROUPS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_group_add_without_project_warns(patch_db):
    from brix.mcp_handlers.triggers import _handle_trigger_group_add

    result = await _handle_trigger_group_add({
        "name": "no_proj_tg",
        "triggers": ["t1"],
    })
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)
    assert any("MISSING DESCRIPTION" in w for w in warnings)


@pytest.mark.asyncio
async def test_trigger_group_add_with_org(patch_db):
    from brix.mcp_handlers.triggers import _handle_trigger_group_add

    result = await _handle_trigger_group_add({
        "name": "org_tg",
        "triggers": ["t1"],
        "description": "test group",
        "project": "buddy",
        "tags": ["batch"],
        "group": "intake",
    })
    assert result["success"] is True
    group = result["group"]
    assert group["project"] == "buddy"
    assert group["tags"] == ["batch"]
    assert group["group_name"] == "intake"
    # No MISSING PROJECT warning
    warnings = result.get("warnings", [])
    assert not any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_trigger_group_list_returns_org(patch_db):
    from brix.mcp_handlers.triggers import _handle_trigger_group_add, _handle_trigger_group_list

    await _handle_trigger_group_add({
        "name": "org_list_tg",
        "triggers": ["t1"],
        "description": "test",
        "project": "cody",
        "tags": ["monitor"],
    })

    result = await _handle_trigger_group_list({})
    groups = result["groups"]
    g = next((g for g in groups if g["name"] == "org_list_tg"), None)
    assert g is not None
    assert g["project"] == "cody"
    assert g["tags"] == ["monitor"]


# ---------------------------------------------------------------------------
# 6. BRICK DEFINITIONS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_brick_create_without_namespace_warns(patch_db, monkeypatch):
    from brix.mcp_handlers.bricks import _handle_create_brick
    monkeypatch.setattr(
        "brix.mcp_handlers.bricks._get_valid_runners",
        lambda: {"python", "http", "cli"},
    )
    import brix.mcp_handlers._shared as _shared_mod
    from unittest.mock import MagicMock
    monkeypatch.setattr(_shared_mod, "_registry", MagicMock())

    result = await _handle_create_brick({
        "name": "no_ns_brick_f",
        "runner": "python",
        "description": "test",
    })
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_brick_create_with_tags(patch_db, monkeypatch):
    from brix.mcp_handlers.bricks import _handle_create_brick
    monkeypatch.setattr(
        "brix.mcp_handlers.bricks._get_valid_runners",
        lambda: {"python", "http", "cli"},
    )
    import brix.mcp_handlers._shared as _shared_mod
    from unittest.mock import MagicMock
    monkeypatch.setattr(_shared_mod, "_registry", MagicMock())

    result = await _handle_create_brick({
        "name": "org_brick_f",
        "runner": "python",
        "description": "test",
        "namespace": "buddy",
        "tags": ["transform"],
    })
    assert result["success"] is True
    assert result.get("tags") == ["transform"]


@pytest.mark.asyncio
async def test_brick_list_returns_org(patch_db, monkeypatch):
    """list_bricks returns org fields from DB."""
    from brix.mcp_handlers.bricks import _handle_create_brick
    from brix.mcp_handlers.steps import _handle_list_bricks
    monkeypatch.setattr(
        "brix.mcp_handlers.bricks._get_valid_runners",
        lambda: {"python", "http", "cli"},
    )
    import brix.mcp_handlers._shared as _shared_mod
    from brix.bricks.schema import BrickSchema
    from unittest.mock import MagicMock

    # Create brick in DB
    db = BrixDB()
    db.brick_definitions_upsert({
        "name": "list_org_brick_f",
        "runner": "python",
        "namespace": "buddy",
        "category": "custom",
        "description": "test",
        "when_to_use": "",
        "when_NOT_to_use": "",
        "aliases": [],
        "input_type": "*",
        "output_type": "*",
        "config_schema": {},
        "examples": [],
        "related_connector": "",
        "system": False,
        "org_tags": ["data"],
        "project": "buddy",
        "group_name": "tools",
    })

    # Create a real BrickSchema for the mock registry
    mock_brick = BrickSchema(
        name="list_org_brick_f",
        type="list_org_brick_f",
        description="test",
        when_to_use="",
        category="custom",
    )
    # Use a mock registry that returns our brick
    class FakeRegistry:
        def list_all(self):
            return [mock_brick]
        def list_by_category(self, cat):
            return [mock_brick]
        def get_categories(self):
            return ["custom"]
    fake_reg = FakeRegistry()
    monkeypatch.setattr(_shared_mod, "_registry", fake_reg)
    # Also patch the local reference in steps module
    import brix.mcp_handlers.steps as _steps_mod
    monkeypatch.setattr(_steps_mod, "_registry", fake_reg)

    result = await _handle_list_bricks({})
    bricks = result["bricks"]
    b = next((b for b in bricks if b["name"] == "list_org_brick_f"), None)
    assert b is not None
    assert b["project"] == "buddy"
    assert b["tags"] == ["data"]
    assert b["group"] == "tools"


# ---------------------------------------------------------------------------
# 7. CONNECTIONS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connection_add_without_project_warns(patch_db):
    from brix.mcp_handlers.connections import _handle_connection_add

    result = await _handle_connection_add({
        "name": "no_proj_conn_f",
        "dsn": "sqlite:///tmp/test_f.db",
        "driver": "sqlite",
    })
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)
    assert any("MISSING DESCRIPTION" in w for w in warnings)


@pytest.mark.asyncio
async def test_connection_list_returns_org(patch_db):
    from brix.mcp_handlers.connections import _handle_connection_add, _handle_connection_list

    await _handle_connection_add({
        "name": "org_list_conn_f",
        "dsn": "sqlite:///tmp/test_f2.db",
        "driver": "sqlite",
        "description": "test",
        "project": "buddy",
        "tags": ["db"],
    })

    result = await _handle_connection_list({})
    connections = result["connections"]
    c = next((c for c in connections if c["name"] == "org_list_conn_f"), None)
    assert c is not None
    assert c["project"] == "buddy"
    assert c["tags"] == ["db"]


# ---------------------------------------------------------------------------
# 8. PROFILES
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_profile_create_without_project_warns(patch_db):
    from brix.mcp_handlers.profiles import _handle_create_profile

    result = await _handle_create_profile({
        "name": "no_proj_prof_f",
        "config": {"x": 1},
    })
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)
    assert any("MISSING DESCRIPTION" in w for w in warnings)


@pytest.mark.asyncio
async def test_profile_get_returns_org(patch_db):
    from brix.mcp_handlers.profiles import _handle_create_profile, _handle_get_profile

    await _handle_create_profile({
        "name": "org_get_prof_f",
        "config": {"y": 2},
        "description": "test",
        "project": "cody",
        "tags": ["dev"],
        "group": "testing",
    })

    result = await _handle_get_profile({"name": "org_get_prof_f"})
    assert result["success"] is True
    assert result["project"] == "cody"
    assert result["tags"] == ["dev"]


@pytest.mark.asyncio
async def test_profile_list_returns_org(patch_db):
    from brix.mcp_handlers.profiles import _handle_create_profile, _handle_list_profiles

    await _handle_create_profile({
        "name": "org_list_prof_f",
        "config": {"z": 3},
        "description": "test",
        "project": "mailpilot",
        "tags": ["prod"],
    })

    result = await _handle_list_profiles({})
    profiles = result["profiles"]
    p = next((p for p in profiles if p["name"] == "org_list_prof_f"), None)
    assert p is not None
    assert p["project"] == "mailpilot"
    assert p["tags"] == ["prod"]


# ---------------------------------------------------------------------------
# 9. ALERT RULES
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_alert_add_without_project_warns(patch_db):
    from brix.mcp_handlers.alerts import _handle_alert_add

    result = await _handle_alert_add({
        "name": "no_proj_alert",
        "condition": "run_failed",
        "channel": "log",
    })
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)
    assert any("MISSING DESCRIPTION" in w for w in warnings)
    assert any("HINT" in w for w in warnings)


@pytest.mark.asyncio
async def test_alert_add_with_org(patch_db):
    from brix.mcp_handlers.alerts import _handle_alert_add

    result = await _handle_alert_add({
        "name": "org_alert",
        "condition": "run_failed",
        "channel": "log",
        "description": "test alert",
        "project": "buddy",
        "tags": ["cost"],
        "group": "monitoring",
    })
    assert result["success"] is True
    rule = result["rule"]
    assert rule["project"] == "buddy"
    assert rule["tags"] == ["cost"]
    assert rule["group"] == "monitoring"
    # No MISSING PROJECT warning
    warnings = result.get("warnings", [])
    assert not any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
async def test_alert_list_returns_org(patch_db):
    from brix.mcp_handlers.alerts import _handle_alert_add, _handle_alert_list

    await _handle_alert_add({
        "name": "org_list_alert",
        "condition": "run_failed",
        "channel": "log",
        "description": "test",
        "project": "cody",
        "tags": ["watch"],
    })

    result = await _handle_alert_list({})
    rules = result["rules"]
    r = next((r for r in rules if r["name"] == "org_list_alert"), None)
    assert r is not None
    assert r["project"] == "cody"
    assert r["tags"] == ["watch"]


@pytest.mark.asyncio
async def test_alert_update_org(patch_db):
    from brix.mcp_handlers.alerts import _handle_alert_add, _handle_alert_update

    add_result = await _handle_alert_add({
        "name": "update_org_alert",
        "condition": "run_failed",
        "channel": "log",
        "description": "test",
        "project": "old",
    })
    rule_id = add_result["rule"]["id"]

    result = await _handle_alert_update({
        "id": rule_id,
        "project": "new_project",
        "tags": ["updated"],
        "group": "new_group",
    })
    assert result["success"] is True
    assert result["rule"]["project"] == "new_project"
    assert result["rule"]["tags"] == ["updated"]
    assert result["rule"]["group"] == "new_group"


# ---------------------------------------------------------------------------
# 10-15. REGISTRY (6 types — all use same handler)
# ---------------------------------------------------------------------------


REGISTRY_TYPES = [
    "best_practices",
    "error_patterns",
    "lessons_learned",
    "patterns",
    "schemas",
    "templates",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_type", REGISTRY_TYPES)
async def test_registry_add_without_project_warns(patch_db, registry_type):
    from brix.mcp_handlers.registry import _handle_registry_add

    result = await _handle_registry_add({
        "registry_type": registry_type,
        "name": f"no_proj_{registry_type}",
        "content": {"data": "test"},
    })
    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings)
    assert any("MISSING DESCRIPTION" in w for w in warnings)


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_type", REGISTRY_TYPES)
async def test_registry_add_with_project(patch_db, registry_type):
    from brix.mcp_handlers.registry import _handle_registry_add

    result = await _handle_registry_add({
        "registry_type": registry_type,
        "name": f"org_{registry_type}",
        "content": {"data": "test"},
        "description": "test entry",
        "tags": ["important"],
        "project": "buddy",
        "group": "knowledge",
    })
    assert result["success"] is True
    assert result.get("project") == "buddy"
    assert result.get("group") == "knowledge"
    warnings = result.get("warnings", [])
    assert not any("MISSING PROJECT" in w for w in warnings)


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_type", REGISTRY_TYPES)
async def test_registry_get_returns_org(patch_db, registry_type):
    from brix.mcp_handlers.registry import _handle_registry_add, _handle_registry_get

    await _handle_registry_add({
        "registry_type": registry_type,
        "name": f"get_org_{registry_type}",
        "content": {"data": "test"},
        "description": "test",
        "tags": ["cat"],
        "project": "cody",
        "group": "dev",
    })

    result = await _handle_registry_get({
        "registry_type": registry_type,
        "name_or_id": f"get_org_{registry_type}",
    })
    assert result["success"] is True
    entry = result["entry"]
    assert entry["project"] == "cody"
    assert entry["group"] == "dev"
    assert entry["tags"] == ["cat"]


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_type", REGISTRY_TYPES)
async def test_registry_list_returns_org(patch_db, registry_type):
    from brix.mcp_handlers.registry import _handle_registry_add, _handle_registry_list

    await _handle_registry_add({
        "registry_type": registry_type,
        "name": f"list_org_{registry_type}",
        "content": {"data": "test"},
        "description": "test",
        "tags": ["tag1"],
        "project": "mailpilot",
        "group": "infra",
    })

    result = await _handle_registry_list({"registry_type": registry_type})
    assert result["success"] is True
    entries = result["entries"]
    e = next((e for e in entries if e["name"] == f"list_org_{registry_type}"), None)
    assert e is not None
    assert e["project"] == "mailpilot"
    assert e["group"] == "infra"
    assert e["tags"] == ["tag1"]
