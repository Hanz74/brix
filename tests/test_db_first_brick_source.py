"""T-BRIX-DBF-02: Verify DB is sole source of truth for brick definitions.

Tests:
  1. Editing a brick via DB survives a simulated container restart
  2. Runner discovery works without builtins.py being imported
  3. Seed only populates empty brick_definition table
  4. Startup sync no longer overwrites DB brick edits
"""
from __future__ import annotations

import pytest
import sys


@pytest.fixture
def fresh_db(tmp_path):
    """Create a fresh BrixDB instance with a temporary database."""
    from brix.db import BrixDB
    db_path = tmp_path / "test_brix.db"
    return BrixDB(db_path=db_path)


class TestDBIsSoleSourceOfTruth:
    """DB-edited bricks must survive startup sync (simulated restart)."""

    def test_edited_brick_survives_startup_sync(self, fresh_db):
        """Edit a brick in DB, run startup_sync, verify edit persists."""
        from brix.seed import seed_if_empty
        from brix.startup_sync import run_startup_sync

        # Seed DB with initial brick data
        seed_if_empty(fresh_db)
        assert fresh_db.brick_definitions_count() > 0

        # Edit a brick directly in DB (simulating MCP edit)
        custom_description = "CUSTOM EDIT — user-modified via MCP"
        with fresh_db._connect() as conn:
            conn.execute(
                "UPDATE brick_definition SET description = ? WHERE name = ?",
                (custom_description, "script.python"),
            )

        # Verify edit took effect
        with fresh_db._connect() as conn:
            row = conn.execute(
                "SELECT description FROM brick_definition WHERE name = ?",
                ("script.python",),
            ).fetchone()
            assert row[0] == custom_description

        # Run startup sync (simulates container restart)
        run_startup_sync(fresh_db)

        # Verify the custom edit was NOT overwritten
        with fresh_db._connect() as conn:
            row = conn.execute(
                "SELECT description FROM brick_definition WHERE name = ?",
                ("script.python",),
            ).fetchone()
            assert row[0] == custom_description, (
                "startup_sync overwrote user-edited brick description — "
                "DB should be sole source of truth"
            )

    def test_new_brick_via_db_survives_startup_sync(self, fresh_db):
        """A custom brick added via DB must survive startup sync."""
        from brix.seed import seed_if_empty
        from brix.startup_sync import run_startup_sync

        seed_if_empty(fresh_db)

        # Add a custom brick via DB
        fresh_db.brick_definitions_upsert({
            "name": "custom.my_brick",
            "runner": "python",
            "namespace": "custom",
            "category": "custom",
            "description": "My custom brick",
            "system": 0,
        })

        count_before = fresh_db.brick_definitions_count()

        # Run startup sync
        run_startup_sync(fresh_db)

        # Custom brick must still exist
        count_after = fresh_db.brick_definitions_count()
        assert count_after >= count_before

        with fresh_db._connect() as conn:
            row = conn.execute(
                "SELECT description FROM brick_definition WHERE name = ?",
                ("custom.my_brick",),
            ).fetchone()
            assert row is not None, "Custom brick was deleted by startup_sync"
            assert row[0] == "My custom brick"


class TestRunnerDiscoveryIndependent:
    """Runner discovery must work without importing builtins.py."""

    def test_discover_runners_without_builtins_import(self):
        """discover_runners() scans brix.runners package, not builtins.py."""
        from brix.runners.base import discover_runners

        runners = discover_runners()

        # Must find the core runners
        assert "python" in runners, "PythonRunner not discovered"
        assert "http" in runners, "HttpRunner not discovered"
        assert "cli" in runners, "CliRunner not discovered"
        assert "filter" in runners, "FilterRunner not discovered"
        assert "transform" in runners, "TransformRunner not discovered"
        assert "mcp" in runners, "McpRunner not discovered"

    def test_discover_runners_does_not_import_builtins(self):
        """Verify discover_runners() does not trigger import of builtins module."""
        from brix.runners.base import discover_runners

        # Remove builtins from sys.modules cache if present
        builtins_key = "brix.bricks.builtins"
        was_loaded = builtins_key in sys.modules
        saved = sys.modules.pop(builtins_key, None)

        try:
            runners = discover_runners()
            assert len(runners) > 0, "No runners discovered"
            # builtins should NOT have been imported as a side effect
            assert builtins_key not in sys.modules, (
                "discover_runners() imported brix.bricks.builtins — "
                "runner discovery must be independent of brick definitions"
            )
        finally:
            # Restore module state
            if was_loaded and saved is not None:
                sys.modules[builtins_key] = saved


class TestSeedOnlyPopulatesEmpty:
    """Seed must only populate when brick_definition table is empty."""

    def test_seed_skips_when_bricks_exist(self, fresh_db):
        """seed_if_empty must skip brick seeding if table already has data."""
        from brix.seed import seed_if_empty

        # First seed — populates
        counts1 = seed_if_empty(fresh_db)
        assert counts1["brick_definitions"] > 0

        # Edit a brick
        custom_desc = "USER EDIT MUST SURVIVE"
        with fresh_db._connect() as conn:
            conn.execute(
                "UPDATE brick_definition SET description = ? WHERE name = ?",
                (custom_desc, "script.python"),
            )

        # Second seed — must skip
        counts2 = seed_if_empty(fresh_db)
        assert counts2["brick_definitions"] == 0, (
            "seed_if_empty seeded bricks again even though table was not empty"
        )

        # Verify edit persists
        with fresh_db._connect() as conn:
            row = conn.execute(
                "SELECT description FROM brick_definition WHERE name = ?",
                ("script.python",),
            ).fetchone()
            assert row[0] == custom_desc

    def test_seed_populates_empty_table(self, fresh_db):
        """seed_if_empty must populate brick_definition when table is empty."""
        from brix.seed import seed_if_empty

        assert fresh_db.brick_definitions_count() == 0
        counts = seed_if_empty(fresh_db)
        assert counts["brick_definitions"] > 0
        assert fresh_db.brick_definitions_count() > 0


class TestRegistryReadsFromDB:
    """BrickRegistry must read exclusively from DB, not from code."""

    def test_registry_loads_from_db_after_seed(self, fresh_db):
        """After seeding, BrickRegistry loads bricks from DB."""
        from brix.seed import seed_if_empty
        from brix.bricks.registry import BrickRegistry

        seed_if_empty(fresh_db)
        reg = BrickRegistry(db=fresh_db)

        assert reg.count > 0
        # Verify a known brick is present
        brick = reg.get("script.python")
        assert brick is not None
        assert brick.description

    def test_registry_empty_without_db(self):
        """Registry without DB should be empty (no code fallback)."""
        from brix.bricks.registry import BrickRegistry

        reg = BrickRegistry(db=None)
        assert reg.count == 0

    def test_registry_reflects_db_edits(self, fresh_db):
        """Edits made directly in DB should be visible via a fresh BrickRegistry."""
        from brix.seed import seed_if_empty
        from brix.bricks.registry import BrickRegistry

        seed_if_empty(fresh_db)

        # Edit brick in DB
        custom_desc = "REGISTRY TEST EDIT"
        with fresh_db._connect() as conn:
            conn.execute(
                "UPDATE brick_definition SET description = ? WHERE name = ?",
                (custom_desc, "script.python"),
            )

        # Fresh registry instance should see the edit
        reg = BrickRegistry(db=fresh_db)
        brick = reg.get("script.python")
        assert brick is not None
        assert brick.description == custom_desc


class TestStartupSyncBrickRepair:
    """_sync_builtin_bricks repairs known brick registry gaps."""

    def test_sync_builtin_bricks_repairs_missing_rows(self, fresh_db):
        """_sync_builtin_bricks repairs a partially populated registry."""
        from brix.startup_sync import _sync_builtin_bricks

        fresh_db.brick_definitions_upsert({"name": "action.notify", "runner": "notify"})
        fresh_db.brick_definitions_upsert({"name": "file_read", "runner": "file"})
        fresh_db.brick_definitions_upsert({"name": "file_write", "runner": "file"})

        result = _sync_builtin_bricks(fresh_db)
        assert result >= 4
        assert fresh_db.brick_definitions_get("db.exec") is not None
        assert fresh_db.brick_definitions_get("action.queue") is not None
        assert fresh_db.brick_definitions_get("action.emit") is not None
        assert fresh_db.brick_definitions_get("file.read_base64") is not None
        assert fresh_db.brick_definitions_get("file_read")["runner"] == "file_read"
        assert fresh_db.brick_definitions_get("file_write")["runner"] == "file_write"

    def test_startup_sync_keeps_seeded_bricks_healthy(self, fresh_db):
        """run_startup_sync preserves healthy seeded brick rows."""
        from brix.seed import seed_if_empty
        from brix.startup_sync import run_startup_sync

        seed_if_empty(fresh_db)
        summary = run_startup_sync(fresh_db)
        assert summary["bricks_synced"] >= 0
        assert fresh_db.brick_definitions_get("file_read")["runner"] == "file_read"
        assert fresh_db.brick_definitions_get("file_write")["runner"] == "file_write"
        assert fresh_db.brick_definitions_get("db.exec") is not None
