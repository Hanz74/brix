"""Tests for startup_sync — T-BRIX-INTEGRITY-01."""
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from brix.db import BrixDB
from brix.startup_sync import (
    _is_test_helper,
    _scan_helper_files,
    run_startup_sync,
    _sync_helpers,
    _sync_pipelines,
    _backfill_descriptions,
    _detect_orphans,
    _cleanup_test_artifacts,
)


@pytest.fixture
def sync_db(tmp_path):
    """Return a BrixDB backed by a temporary database file."""
    return BrixDB(db_path=tmp_path / "sync_test.db")


@pytest.fixture
def helper_dir(tmp_path):
    """Create a temporary helper directory with some .py files."""
    d = tmp_path / "helpers"
    d.mkdir()
    (d / "real_helper.py").write_text("# real helper\n")
    (d / "another_util.py").write_text("# another util\n")
    return d


# ---------------------------------------------------------------------------
# Unit tests: _is_test_helper
# ---------------------------------------------------------------------------

class TestIsTestHelper:
    def test_known_test_names(self):
        assert _is_test_helper("my-helper") is True
        assert _is_test_helper("my_helper") is True
        assert _is_test_helper("no-project-helper") is True
        assert _is_test_helper("org_helper") is True
        assert _is_test_helper("with-project-helper") is True
        assert _is_test_helper("test_mcp_output") is True

    def test_prefix_patterns(self):
        assert _is_test_helper("debug_foo") is True
        assert _is_test_helper("test_something") is True
        assert _is_test_helper("xtest_bar") is True
        assert _is_test_helper("mock_helper") is True

    def test_real_helpers_not_matched(self):
        assert _is_test_helper("download_attachments") is False
        assert _is_test_helper("parse_invoice") is False
        assert _is_test_helper("email_utils") is False


# ---------------------------------------------------------------------------
# Integration: helper sync
# ---------------------------------------------------------------------------

class TestHelperSync:
    def test_registers_unregistered_helpers(self, sync_db, helper_dir):
        with patch("brix.startup_sync._HELPER_SEARCH_PATHS", [helper_dir]):
            count = _sync_helpers(sync_db)

        assert count == 2
        names = {h["name"] for h in sync_db.list_helpers()}
        assert "real_helper" in names
        assert "another_util" in names

    def test_skips_already_registered(self, sync_db, helper_dir):
        # Pre-register one helper
        sync_db.upsert_helper(name="real_helper", script_path="/some/path.py")

        with patch("brix.startup_sync._HELPER_SEARCH_PATHS", [helper_dir]):
            count = _sync_helpers(sync_db)

        # Only the new one should be registered
        assert count == 1

    def test_skips_test_helpers(self, sync_db, tmp_path):
        d = tmp_path / "helpers"
        d.mkdir()
        (d / "debug_foo.py").write_text("# test artifact\n")
        (d / "test_bar.py").write_text("# test artifact\n")
        (d / "legit_helper.py").write_text("# real\n")

        with patch("brix.startup_sync._HELPER_SEARCH_PATHS", [d]):
            count = _sync_helpers(sync_db)

        assert count == 1
        names = {h["name"] for h in sync_db.list_helpers()}
        assert "legit_helper" in names
        assert "debug_foo" not in names


# ---------------------------------------------------------------------------
# Integration: pipeline sync
# ---------------------------------------------------------------------------

class TestPipelineSync:
    def test_sync_pipelines_noop_when_all_rows_migrated(self, sync_db):
        sync_db.upsert_pipeline(name="done-pipe", path="/done.yaml")
        row = sync_db.get_pipeline("done-pipe")
        assert row is not None

        with sync_db._connect() as conn:
            conn.execute(
                "UPDATE pipeline SET migration_status='v71_complete' WHERE id=?",
                (row["id"],),
            )

        count = _sync_pipelines(sync_db)

        assert count == 0

    def test_sync_pipelines_normalizes_non_migrated_rows(self, sync_db):
        sync_db.upsert_pipeline(
            name="legacy-pipe",
            path="/legacy.yaml",
            yaml_content="""
name: legacy-pipe
steps:
  - id: normalize-me
    type: flow.set
    values:
      value: 1
""".strip(),
        )

        count = _sync_pipelines(sync_db)
        row = sync_db.get_pipeline("legacy-pipe")

        assert count == 1
        assert row is not None
        assert row["migration_status"] == "v71_complete"
        assert sync_db.get_steps(row["id"])[0]["id"] == "normalize-me"

    def test_sync_pipelines_does_not_scan_disk(self, sync_db, monkeypatch):
        sync_db.upsert_pipeline(name="db-only-pipe", path="/db-only.yaml")
        row = sync_db.get_pipeline("db-only-pipe")
        assert row is not None
        with sync_db._connect() as conn:
            conn.execute(
                "UPDATE pipeline SET migration_status='v71_complete' WHERE id=?",
                (row["id"],),
            )

        def _boom():
            raise AssertionError("pipeline disk scan should not run")

        monkeypatch.setattr("brix.startup_sync._scan_pipeline_files", _boom)
        count = _sync_pipelines(sync_db)

        assert count == 0


# ---------------------------------------------------------------------------
# Integration: description backfill
# ---------------------------------------------------------------------------

class TestDescriptionBackfill:
    def test_backfill_descriptions_is_noop(self, sync_db):
        sync_db.upsert_pipeline(
            name="bp1",
            path="/p.yaml",
            yaml_content="name: bp1\ndescription: Filled from YAML\nsteps: []\n",
        )

        count = _backfill_descriptions(sync_db)
        assert count == 0


# ---------------------------------------------------------------------------
# Integration: orphan detection
# ---------------------------------------------------------------------------

class TestOrphanDetection:
    def test_detects_orphan_triggers(self, sync_db):
        # Create a trigger pointing to a non-existent pipeline
        import sqlite3
        with sync_db._connect() as conn:
            try:
                conn.execute(
                    """INSERT INTO triggers (id, name, type, pipeline, created_at, updated_at)
                       VALUES ('t1', 'orphan_trig', 'cron', 'nonexistent_pipeline', '2024-01-01', '2024-01-01')"""
                )
            except sqlite3.OperationalError:
                pytest.skip("triggers table not available")

        result = _detect_orphans(sync_db)
        assert result["triggers"] >= 1

    def test_detects_orphan_helpers(self, sync_db):
        sync_db.upsert_helper(
            name="ghost_helper",
            script_path="/nonexistent/path/ghost.py",
        )
        result = _detect_orphans(sync_db)
        assert result["helpers"] >= 1


# ---------------------------------------------------------------------------
# Integration: test artifact cleanup
# ---------------------------------------------------------------------------

class TestArtifactCleanup:
    def test_cleans_test_files(self, tmp_path):
        # Create the .brix/helpers structure that _cleanup_test_artifacts expects
        d = tmp_path / ".brix" / "helpers"
        d.mkdir(parents=True)
        (d / "debug_foo.py").write_text("# test\n")
        (d / "my_helper.py").write_text("# test\n")
        (d / "real_helper.py").write_text("# keep\n")

        with patch.object(Path, "home", return_value=tmp_path):
            count = _cleanup_test_artifacts()

        assert count == 2
        assert (d / "real_helper.py").exists()
        assert not (d / "debug_foo.py").exists()
        assert not (d / "my_helper.py").exists()

    def test_no_cleanup_outside_brix_helpers(self, tmp_path):
        """Ensure /app/helpers/ is NOT cleaned."""
        d = tmp_path / "app_helpers"
        d.mkdir()
        (d / "debug_foo.py").write_text("# test\n")

        # No .brix/helpers dir exists under tmp_path
        with patch.object(Path, "home", return_value=tmp_path):
            count = _cleanup_test_artifacts()

        # Nothing in ~/.brix/helpers/ so count=0
        assert count == 0


# ---------------------------------------------------------------------------
# Integration: full run_startup_sync (idempotent)
# ---------------------------------------------------------------------------

class TestRunStartupSync:
    def test_full_sync_registers_helpers_and_normalizes_pipelines(self, sync_db, helper_dir):
        """Startup sync registers disk helpers and normalizes non-migrated pipelines."""
        # Pre-seed a non-migrated pipeline in DB (as if from a previous import)
        sync_db.upsert_pipeline(
            name="unmigrated-pipe",
            path="/unmigrated.yaml",
            yaml_content="name: unmigrated-pipe\nsteps:\n  - id: s1\n    type: flow.set\n    values:\n      x: 1\n",
        )

        with patch("brix.startup_sync._HELPER_SEARCH_PATHS", [helper_dir]):
            result = run_startup_sync(sync_db)

        # Helpers registered from disk
        assert result["helpers_registered"] == 2
        # Pipeline normalized from yaml_content to step rows
        assert result["pipelines_normalized"] >= 1
        # Verify the pipeline is now migrated
        row = sync_db.get_pipeline("unmigrated-pipe")
        assert row["migration_status"] == "v71_complete"
        # Verify step rows exist
        steps = sync_db.get_steps(row["id"])
        assert len(steps) == 1
        assert len(steps) >= 1  # step row created

    def test_idempotent(self, sync_db, helper_dir):
        """Running startup sync twice produces no duplicate data."""
        sync_db.upsert_pipeline(
            name="idem-pipe",
            path="/idem.yaml",
            yaml_content="name: idem-pipe\nsteps:\n  - id: s1\n    type: flow.set\n    values:\n      v: 1\n",
        )

        with patch("brix.startup_sync._HELPER_SEARCH_PATHS", [helper_dir]):
            result1 = run_startup_sync(sync_db)
            result2 = run_startup_sync(sync_db)

        # First run registers helpers and normalizes
        assert result1["helpers_registered"] == 2
        assert result1["pipelines_normalized"] >= 1
        # Second run finds nothing new
        assert result2["helpers_registered"] == 0
        assert result2["pipelines_normalized"] == 0
