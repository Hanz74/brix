"""Tests for startup_sync — T-BRIX-INTEGRITY-01."""
import os
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from brix.db import BrixDB
from brix.startup_sync import (
    _is_test_helper,
    _scan_helper_files,
    _scan_pipeline_files,
    _extract_description_from_yaml,
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


@pytest.fixture
def pipeline_dir(tmp_path):
    """Create a temporary pipeline directory with some YAML files."""
    d = tmp_path / "pipelines"
    d.mkdir()
    (d / "my-pipeline.yaml").write_text(yaml.dump({
        "name": "my-pipeline",
        "description": "A test pipeline for sync",
        "steps": [{"id": "s1", "type": "flow.transform", "params": {"expression": "item"}}],
    }))
    (d / "no-desc.yaml").write_text(yaml.dump({
        "name": "no-desc",
        "steps": [{"id": "s1", "type": "flow.transform", "params": {"expression": "item"}}],
    }))
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
# Unit tests: _extract_description_from_yaml
# ---------------------------------------------------------------------------

class TestExtractDescription:
    def test_extracts_description(self):
        yaml_text = yaml.dump({"name": "p1", "description": "My pipeline desc"})
        assert _extract_description_from_yaml(yaml_text) == "My pipeline desc"

    def test_no_description(self):
        yaml_text = yaml.dump({"name": "p1", "steps": []})
        assert _extract_description_from_yaml(yaml_text) == ""

    def test_invalid_yaml(self):
        assert _extract_description_from_yaml(":::invalid") == ""


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
    def test_imports_unregistered_pipelines(self, sync_db, pipeline_dir):
        with patch("brix.startup_sync._PIPELINE_SEARCH_PATHS", [pipeline_dir]):
            count = _sync_pipelines(sync_db)

        assert count == 2
        names = {p["name"] for p in sync_db.list_pipelines()}
        assert "my-pipeline" in names
        assert "no-desc" in names

    def test_skips_already_registered(self, sync_db, pipeline_dir):
        sync_db.upsert_pipeline(name="my-pipeline", path="/old/path.yaml")

        with patch("brix.startup_sync._PIPELINE_SEARCH_PATHS", [pipeline_dir]):
            count = _sync_pipelines(sync_db)

        assert count == 1  # only no-desc

    def test_skips_test_pipelines(self, sync_db, tmp_path):
        d = tmp_path / "pipelines"
        d.mkdir()
        (d / "test_foo.yaml").write_text(yaml.dump({"name": "test_foo", "steps": []}))
        (d / "real_pipe.yaml").write_text(yaml.dump({
            "name": "real_pipe",
            "description": "A real pipeline",
            "steps": [{"id": "s1", "type": "flow.transform"}],
        }))

        with patch("brix.startup_sync._PIPELINE_SEARCH_PATHS", [d]):
            count = _sync_pipelines(sync_db)

        assert count == 1
        names = {p["name"] for p in sync_db.list_pipelines()}
        assert "real_pipe" in names
        assert "test_foo" not in names


# ---------------------------------------------------------------------------
# Integration: description backfill
# ---------------------------------------------------------------------------

class TestDescriptionBackfill:
    def test_backfills_from_yaml_content(self, sync_db):
        yaml_content = yaml.dump({"name": "bp1", "description": "Filled from YAML"})
        sync_db.upsert_pipeline(name="bp1", path="/p.yaml", yaml_content=yaml_content)
        # Ensure description is empty
        import sqlite3
        with sync_db._connect() as conn:
            if sync_db._column_exists(conn, "pipeline", "description"):
                conn.execute("UPDATE pipeline SET description='' WHERE name='bp1'")

        count = _backfill_descriptions(sync_db)
        assert count == 1

        p = sync_db.get_pipeline("bp1")
        assert p.get("description") == "Filled from YAML"

    def test_skips_already_described(self, sync_db):
        yaml_content = yaml.dump({"name": "bp2", "description": "Original"})
        sync_db.upsert_pipeline(name="bp2", path="/p.yaml", yaml_content=yaml_content)
        import sqlite3
        with sync_db._connect() as conn:
            if sync_db._column_exists(conn, "pipeline", "description"):
                conn.execute("UPDATE pipeline SET description='Already set' WHERE name='bp2'")

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
    def test_full_sync(self, sync_db, helper_dir, pipeline_dir):
        with patch("brix.startup_sync._HELPER_SEARCH_PATHS", [helper_dir]), \
             patch("brix.startup_sync._PIPELINE_SEARCH_PATHS", [pipeline_dir]):
            result = run_startup_sync(sync_db)

        assert result["helpers_registered"] == 2
        assert result["pipelines_imported"] == 2

    def test_idempotent(self, sync_db, helper_dir, pipeline_dir):
        with patch("brix.startup_sync._HELPER_SEARCH_PATHS", [helper_dir]), \
             patch("brix.startup_sync._PIPELINE_SEARCH_PATHS", [pipeline_dir]):
            result1 = run_startup_sync(sync_db)
            result2 = run_startup_sync(sync_db)

        # First run registers things
        assert result1["helpers_registered"] == 2
        assert result1["pipelines_imported"] == 2

        # Second run finds nothing new
        assert result2["helpers_registered"] == 0
        assert result2["pipelines_imported"] == 0
