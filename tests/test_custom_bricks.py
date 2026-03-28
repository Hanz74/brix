"""Tests for Custom Bricks CRUD and reference integrity — T-BRIX-DB-20.

Covers:
- create_brick with valid runner → OK
- create_brick with invalid runner → error
- create_brick duplicate name → error
- create_brick missing required params → error
- update_brick system brick → error
- update_brick custom brick → OK
- update_brick invalid runner → error
- update_brick non-existent → error
- delete_brick system brick → error
- delete_brick custom brick without references → OK
- delete_brick custom brick with references, force=false → error with list
- delete_brick custom brick with references, force=true → OK
- delete_brick non-existent → error
- check_references for pipeline → trigger list
- check_references for connection → pipeline list
- check_references for brick → pipeline list
- BrickRegistry is updated on create/update/delete
- DB persistence (brick survives registry reload)
- MCP handler integration tests (async)
"""
from __future__ import annotations

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path):
    """BrixDB backed by a temp file."""
    from brix.db import BrixDB
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def pipeline_dir(tmp_path):
    """Temporary pipeline directory."""
    d = tmp_path / "pipelines"
    d.mkdir()
    return d


@pytest.fixture(autouse=True)
def patch_pipeline_dir(pipeline_dir, monkeypatch):
    """Redirect _pipeline_dir() to our temp dir so no real files are touched."""
    import brix.mcp_server as mcp_mod
    monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", pipeline_dir)


@pytest.fixture
def valid_runners():
    """Return a fixed set of valid runners for testing."""
    return {"python", "http", "cli", "mcp", "filter", "transform"}


@pytest.fixture(autouse=True)
def patch_discover_runners(valid_runners, monkeypatch):
    """Patch discover_runners so tests don't depend on actual runner modules."""
    mock_runners = {r: MagicMock() for r in valid_runners}
    monkeypatch.setattr(
        "brix.mcp_handlers.bricks._get_valid_runners",
        lambda: valid_runners,
    )


@pytest.fixture(autouse=True)
def patch_db(tmp_db, monkeypatch):
    """Patch BrixDB() constructor in bricks handler to use tmp_db."""
    # bricks.py does `from brix.db import BrixDB` at module level,
    # so we patch the name in the bricks module namespace
    import brix.mcp_handlers.bricks as bricks_mod
    monkeypatch.setattr(bricks_mod, "BrixDB", lambda **kwargs: tmp_db)
    # Also mock write_audit_entry on the tmp_db so audit logging is silent
    tmp_db.write_audit_entry = MagicMock()


@pytest.fixture
def brick_registry():
    """Fresh BrickRegistry (no DB, no builtins override)."""
    from brix.bricks.registry import BrickRegistry
    registry = BrickRegistry()
    return registry


@pytest.fixture(autouse=True)
def patch_registry(brick_registry, monkeypatch):
    """Patch the shared _registry used by both _shared and bricks modules."""
    import brix.mcp_handlers._shared as shared_mod
    monkeypatch.setattr(shared_mod, "_registry", brick_registry)


# ---------------------------------------------------------------------------
# Helper to create a custom brick in DB
# ---------------------------------------------------------------------------

def _insert_custom_brick(db, name: str, runner: str = "python", system: bool = False):
    """Insert a brick_definition directly into DB."""
    db.brick_definitions_upsert({
        "name": name,
        "runner": runner,
        "namespace": "",
        "category": "custom",
        "description": f"Test brick {name}",
        "when_to_use": "",
        "when_NOT_to_use": "",
        "aliases": [],
        "input_type": "*",
        "output_type": "*",
        "config_schema": {},
        "examples": [],
        "related_connector": "",
        "system": system,
    })


def _insert_system_brick(db, name: str, runner: str = "python"):
    """Insert a system brick_definition (system=True)."""
    _insert_custom_brick(db, name, runner, system=True)


# ---------------------------------------------------------------------------
# Tests: create_brick
# ---------------------------------------------------------------------------

class TestCreateBrick:
    """brix__create_brick handler tests."""

    @pytest.mark.asyncio
    async def test_create_brick_valid_runner_ok(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_create_brick
        result = await _handle_create_brick({
            "name": "my_extractor",
            "runner": "python",
            "description": "Extracts things.",
        })
        assert result["success"] is True
        assert result["created_brick"] == "my_extractor"
        # Verify DB persistence
        row = tmp_db.brick_definitions_get("my_extractor")
        assert row is not None
        assert row["runner"] == "python"
        assert bool(row["system"]) is False

    @pytest.mark.asyncio
    async def test_create_brick_invalid_runner_error(self):
        from brix.mcp_handlers.bricks import _handle_create_brick
        result = await _handle_create_brick({
            "name": "bad_brick",
            "runner": "nonexistent_runner_xyz",
            "description": "This should fail.",
        })
        assert result["success"] is False
        assert "runner" in result["error"].lower() or "nonexistent_runner_xyz" in result["error"]

    @pytest.mark.asyncio
    async def test_create_brick_missing_name_error(self):
        from brix.mcp_handlers.bricks import _handle_create_brick
        result = await _handle_create_brick({
            "runner": "python",
            "description": "No name given.",
        })
        assert result["success"] is False
        assert "name" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_create_brick_missing_runner_error(self):
        from brix.mcp_handlers.bricks import _handle_create_brick
        result = await _handle_create_brick({
            "name": "brick_no_runner",
            "description": "No runner given.",
        })
        assert result["success"] is False
        assert "runner" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_create_brick_missing_description_error(self):
        from brix.mcp_handlers.bricks import _handle_create_brick
        result = await _handle_create_brick({
            "name": "brick_no_desc",
            "runner": "python",
        })
        assert result["success"] is False
        assert "description" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_create_brick_duplicate_error(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_create_brick
        _insert_custom_brick(tmp_db, "existing_brick")
        result = await _handle_create_brick({
            "name": "existing_brick",
            "runner": "python",
            "description": "Duplicate.",
        })
        assert result["success"] is False
        assert "already exists" in result["error"]

    @pytest.mark.asyncio
    async def test_create_brick_with_all_fields(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_create_brick
        result = await _handle_create_brick({
            "name": "full_brick",
            "runner": "http",
            "description": "Full brick with all fields.",
            "input_type": "list[str]",
            "output_type": "list[dict]",
            "aliases": ["full", "komplett"],
            "when_to_use": "Use always.",
            "when_NOT_to_use": "Never skip.",
            "namespace": "action",
            "category": "integration",
            "config_defaults": {"url": "https://example.com"},
        })
        assert result["success"] is True
        row = tmp_db.brick_definitions_get("full_brick")
        assert row["runner"] == "http"
        assert row["namespace"] == "action"
        assert row["category"] == "integration"

    @pytest.mark.asyncio
    async def test_create_brick_registers_in_registry(self, tmp_db, brick_registry):
        from brix.mcp_handlers.bricks import _handle_create_brick
        result = await _handle_create_brick({
            "name": "registry_brick",
            "runner": "cli",
            "description": "Test registry update.",
        })
        assert result["success"] is True
        brick = brick_registry.get("registry_brick")
        assert brick is not None
        assert brick.runner == "cli"


# ---------------------------------------------------------------------------
# Tests: update_brick
# ---------------------------------------------------------------------------

class TestUpdateBrick:
    """brix__update_brick handler tests."""

    @pytest.mark.asyncio
    async def test_update_system_brick_error(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_update_brick
        _insert_system_brick(tmp_db, "sys_brick")
        result = await _handle_update_brick({
            "name": "sys_brick",
            "description": "Try to update system brick.",
        })
        assert result["success"] is False
        assert "system" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_update_custom_brick_ok(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_update_brick
        _insert_custom_brick(tmp_db, "my_brick")
        result = await _handle_update_brick({
            "name": "my_brick",
            "description": "Updated description.",
            "when_to_use": "Use for updates.",
        })
        assert result["success"] is True
        row = tmp_db.brick_definitions_get("my_brick")
        assert row["description"] == "Updated description."
        assert row["when_to_use"] == "Use for updates."

    @pytest.mark.asyncio
    async def test_update_brick_invalid_runner_error(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_update_brick
        _insert_custom_brick(tmp_db, "my_brick2")
        result = await _handle_update_brick({
            "name": "my_brick2",
            "runner": "invalid_runner_abc",
        })
        assert result["success"] is False
        assert "runner" in result["error"].lower() or "invalid_runner_abc" in result["error"]

    @pytest.mark.asyncio
    async def test_update_brick_not_found_error(self):
        from brix.mcp_handlers.bricks import _handle_update_brick
        result = await _handle_update_brick({
            "name": "nonexistent_brick_xyz",
            "description": "Update nonexistent.",
        })
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_update_brick_missing_name_error(self):
        from brix.mcp_handlers.bricks import _handle_update_brick
        result = await _handle_update_brick({
            "description": "No name provided.",
        })
        assert result["success"] is False
        assert "name" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_update_brick_refreshes_registry(self, tmp_db, brick_registry):
        from brix.mcp_handlers.bricks import _handle_update_brick, _handle_create_brick
        # Create first
        await _handle_create_brick({
            "name": "upd_brick",
            "runner": "python",
            "description": "Original.",
        })
        # Update
        result = await _handle_update_brick({
            "name": "upd_brick",
            "description": "Updated in registry.",
        })
        assert result["success"] is True
        brick = brick_registry.get("upd_brick")
        assert brick is not None
        assert brick.description == "Updated in registry."


# ---------------------------------------------------------------------------
# Tests: delete_brick
# ---------------------------------------------------------------------------

class TestDeleteBrick:
    """brix__delete_brick handler tests."""

    @pytest.mark.asyncio
    async def test_delete_system_brick_error(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_delete_brick
        _insert_system_brick(tmp_db, "sys_del_brick")
        result = await _handle_delete_brick({"name": "sys_del_brick"})
        assert result["success"] is False
        assert "system" in result["error"].lower() or "löschbar" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_delete_custom_brick_no_refs_ok(self, tmp_db, brick_registry):
        from brix.mcp_handlers.bricks import _handle_delete_brick
        _insert_custom_brick(tmp_db, "del_brick")
        # No pipelines reference it
        with patch("brix.mcp_handlers.bricks._scan_pipelines_for_brick", return_value=[]):
            result = await _handle_delete_brick({"name": "del_brick"})
        assert result["success"] is True
        assert result["deleted_brick"] == "del_brick"
        # Verify removed from DB
        assert tmp_db.brick_definitions_get("del_brick") is None

    @pytest.mark.asyncio
    async def test_delete_custom_brick_with_refs_no_force_error(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_delete_brick
        _insert_custom_brick(tmp_db, "ref_brick")
        with patch(
            "brix.mcp_handlers.bricks._scan_pipelines_for_brick",
            return_value=["pipeline_a", "pipeline_b"],
        ):
            result = await _handle_delete_brick({"name": "ref_brick", "force": False})
        assert result["success"] is False
        assert "references" in result
        assert "pipeline_a" in str(result["references"])
        assert "pipeline_b" in str(result["references"])

    @pytest.mark.asyncio
    async def test_delete_custom_brick_with_refs_force_ok(self, tmp_db):
        from brix.mcp_handlers.bricks import _handle_delete_brick
        _insert_custom_brick(tmp_db, "force_del_brick")
        with patch(
            "brix.mcp_handlers.bricks._scan_pipelines_for_brick",
            return_value=["pipeline_a"],
        ):
            result = await _handle_delete_brick({"name": "force_del_brick", "force": True})
        assert result["success"] is True
        assert tmp_db.brick_definitions_get("force_del_brick") is None

    @pytest.mark.asyncio
    async def test_delete_brick_not_found_error(self):
        from brix.mcp_handlers.bricks import _handle_delete_brick
        result = await _handle_delete_brick({"name": "nonexistent_xyz"})
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_delete_brick_missing_name_error(self):
        from brix.mcp_handlers.bricks import _handle_delete_brick
        result = await _handle_delete_brick({})
        assert result["success"] is False
        assert "name" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_delete_brick_removes_from_registry(self, tmp_db, brick_registry):
        from brix.mcp_handlers.bricks import _handle_create_brick, _handle_delete_brick
        await _handle_create_brick({
            "name": "to_delete",
            "runner": "python",
            "description": "Will be deleted.",
        })
        assert brick_registry.get("to_delete") is not None
        with patch("brix.mcp_handlers.bricks._scan_pipelines_for_brick", return_value=[]):
            result = await _handle_delete_brick({"name": "to_delete"})
        assert result["success"] is True
        assert brick_registry.get("to_delete") is None


# ---------------------------------------------------------------------------
# Tests: check_references utility
# ---------------------------------------------------------------------------

class TestCheckReferences:
    """check_references() utility function."""

    def test_check_references_pipeline_no_triggers(self):
        from brix.mcp_handlers.bricks import check_references
        with patch("brix.mcp_handlers.bricks._get_triggers_for_pipeline", return_value=[]):
            refs = check_references("pipeline", "my_pipeline")
        assert refs == []

    def test_check_references_pipeline_with_triggers(self):
        from brix.mcp_handlers.bricks import check_references
        with patch(
            "brix.mcp_handlers.bricks._get_triggers_for_pipeline",
            return_value=["trigger_a", "trigger_b"],
        ):
            refs = check_references("pipeline", "my_pipeline")
        assert len(refs) == 2
        assert "trigger: trigger_a" in refs
        assert "trigger: trigger_b" in refs

    def test_check_references_connection_with_pipelines(self):
        from brix.mcp_handlers.bricks import check_references
        with patch(
            "brix.mcp_handlers.bricks._scan_pipelines_for_connection",
            return_value=["pipe_x", "pipe_y"],
        ):
            refs = check_references("connection", "my_conn")
        assert len(refs) == 2
        assert "pipeline: pipe_x" in refs
        assert "pipeline: pipe_y" in refs

    def test_check_references_connection_no_pipelines(self):
        from brix.mcp_handlers.bricks import check_references
        with patch("brix.mcp_handlers.bricks._scan_pipelines_for_connection", return_value=[]):
            refs = check_references("connection", "empty_conn")
        assert refs == []

    def test_check_references_brick_with_pipelines(self):
        from brix.mcp_handlers.bricks import check_references
        with patch(
            "brix.mcp_handlers.bricks._scan_pipelines_for_brick",
            return_value=["pipe_a"],
        ):
            refs = check_references("brick", "my_brick")
        assert refs == ["pipeline: pipe_a"]

    def test_check_references_brick_no_pipelines(self):
        from brix.mcp_handlers.bricks import check_references
        with patch("brix.mcp_handlers.bricks._scan_pipelines_for_brick", return_value=[]):
            refs = check_references("brick", "unused_brick")
        assert refs == []

    def test_check_references_unknown_entity_type(self):
        from brix.mcp_handlers.bricks import check_references
        refs = check_references("unknown_type", "something")
        assert refs == []


# ---------------------------------------------------------------------------
# Tests: DB persistence
# ---------------------------------------------------------------------------

class TestDbPersistence:
    """Brick definitions survive DB reload."""

    def test_brick_persists_after_db_reload(self, tmp_path):
        from brix.db import BrixDB
        db_path = tmp_path / "persist_test.db"

        db1 = BrixDB(db_path=db_path)
        db1.brick_definitions_upsert({
            "name": "persistent_brick",
            "runner": "python",
            "description": "Persists.",
            "system": False,
        })

        # Reload DB
        db2 = BrixDB(db_path=db_path)
        row = db2.brick_definitions_get("persistent_brick")
        assert row is not None
        assert row["runner"] == "python"
        assert bool(row["system"]) is False

    def test_system_brick_preserved_on_reload(self, tmp_path):
        from brix.db import BrixDB
        db_path = tmp_path / "sys_test.db"

        db1 = BrixDB(db_path=db_path)
        db1.brick_definitions_upsert({
            "name": "builtin_thing",
            "runner": "filter",
            "description": "Built-in.",
            "system": True,
        })

        db2 = BrixDB(db_path=db_path)
        row = db2.brick_definitions_get("builtin_thing")
        assert row is not None
        assert bool(row["system"]) is True

    def test_brick_delete_from_db(self, tmp_path):
        from brix.db import BrixDB
        db_path = tmp_path / "del_test.db"
        db = BrixDB(db_path=db_path)
        db.brick_definitions_upsert({
            "name": "to_del",
            "runner": "python",
            "description": "Delete me.",
            "system": False,
        })
        assert db.brick_definitions_get("to_del") is not None
        deleted = db.brick_definitions_delete("to_del")
        assert deleted is True
        assert db.brick_definitions_get("to_del") is None

    def test_delete_nonexistent_brick_returns_false(self, tmp_path):
        from brix.db import BrixDB
        db = BrixDB(db_path=tmp_path / "empty.db")
        result = db.brick_definitions_delete("no_such_brick")
        assert result is False
