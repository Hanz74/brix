"""Tests for T-BRIX-V5-07: Object Versioning — Rollback for Pipelines + Helpers.

Covers:
1. DB layer — record_object_version, get_object_versions, get_object_version,
   trim_object_versions, cleanup_all_versions
2. PipelineStore.save() archives old version before overwrite
3. HelperRegistry.register() archives old entry (code) on update
4. HelperRegistry.update() archives on script change
5. MCP tools: brix__get_versions, brix__rollback, brix__diff_versions
6. brix clean --versions CLI flag
"""
import asyncio
import json
import pytest
from pathlib import Path
from unittest.mock import patch

import yaml

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore
from brix.helper_registry import HelperRegistry
from brix.mcp_server import (
    _handle_get_versions,
    _handle_rollback,
    _handle_diff_versions,
    PIPELINE_DIR,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path):
    """BrixDB backed by a temp file."""
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def tmp_pipeline_dir(tmp_path, monkeypatch):
    """Redirect PIPELINE_DIR to a temp directory."""
    pd = tmp_path / "pipelines"
    pd.mkdir()
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", pd)
    monkeypatch.setattr("brix.pipeline_store.DEFAULT_PIPELINES_DIR", pd)
    return pd


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    """Redirect Path.home() to tmp_path so all brix dirs land in temp."""
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# 1. DB layer
# ---------------------------------------------------------------------------

class TestBrixDBVersioning:
    def test_record_and_retrieve(self, tmp_db):
        vid = tmp_db.record_object_version("pipeline", "my-pipe", {"steps": []})
        rows = tmp_db.get_object_versions("pipeline", "my-pipe")
        assert len(rows) == 1
        assert rows[0]["version_id"] == vid
        assert rows[0]["name"] == "my-pipe"
        assert rows[0]["type"] == "pipeline"

    def test_get_object_version_by_id(self, tmp_db):
        vid = tmp_db.record_object_version("pipeline", "p", {"v": 1})
        record = tmp_db.get_object_version(vid)
        assert record is not None
        assert record["version_id"] == vid

    def test_get_object_version_missing(self, tmp_db):
        assert tmp_db.get_object_version("nonexistent-uuid") is None

    def test_versions_ordered_newest_first(self, tmp_db):
        for i in range(3):
            tmp_db.record_object_version("pipeline", "ordered", {"i": i})
        rows = tmp_db.get_object_versions("pipeline", "ordered")
        # newest first — created_at strings are ISO-8601 which sort lexicographically
        assert rows == sorted(rows, key=lambda r: r["created_at"], reverse=True)

    def test_trim_removes_oldest(self, tmp_db):
        for i in range(12):
            tmp_db.record_object_version("pipeline", "trim-me", {"i": i})
        deleted = tmp_db.trim_object_versions("pipeline", "trim-me", keep=10)
        assert deleted == 2
        remaining = tmp_db.get_object_versions("pipeline", "trim-me")
        assert len(remaining) == 10

    def test_trim_noop_below_limit(self, tmp_db):
        for i in range(5):
            tmp_db.record_object_version("pipeline", "few", {"i": i})
        deleted = tmp_db.trim_object_versions("pipeline", "few", keep=10)
        assert deleted == 0
        assert len(tmp_db.get_object_versions("pipeline", "few")) == 5

    def test_cleanup_all_versions(self, tmp_db):
        for i in range(15):
            tmp_db.record_object_version("pipeline", "a", {"i": i})
        for i in range(12):
            tmp_db.record_object_version("helper", "b", {"i": i})
        total = tmp_db.cleanup_all_versions(keep=10)
        assert total == 7  # 5 from a + 2 from b
        assert len(tmp_db.get_object_versions("pipeline", "a")) == 10
        assert len(tmp_db.get_object_versions("helper", "b")) == 10


# ---------------------------------------------------------------------------
# 2. PipelineStore archives on save
# ---------------------------------------------------------------------------

class TestPipelineStoreVersioning:
    def test_first_save_no_version_created(self, tmp_path):
        db = BrixDB(db_path=tmp_path / "brix.db")
        store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
        store.save({"name": "my-pipe", "steps": []}, "my-pipe")
        # First save: nothing archived (no prior version)
        rows = db.get_object_versions("pipeline", "my-pipe")
        assert len(rows) == 0

    def test_second_save_archives_old_version(self, tmp_path):
        db = BrixDB(db_path=tmp_path / "brix.db")
        store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
        store.save({"name": "p", "steps": [], "version": "1.0.0"}, "p")
        store.save({"name": "p", "steps": [{"id": "s1"}], "version": "1.0.1"}, "p")
        rows = db.get_object_versions("pipeline", "p")
        assert len(rows) == 1
        content = json.loads(rows[0]["content"])
        assert content.get("version") == "1.0.0"

    def test_multiple_saves_retain_last_10(self, tmp_path):
        db = BrixDB(db_path=tmp_path / "brix.db")
        store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
        # First save — no archive
        store.save({"name": "p", "version": "0.0.0", "steps": []}, "p")
        # Next 12 saves — each archives the previous
        for i in range(1, 13):
            store.save({"name": "p", "version": f"1.0.{i}", "steps": []}, "p")
        rows = db.get_object_versions("pipeline", "p")
        assert len(rows) == 10


# ---------------------------------------------------------------------------
# 3 & 4. HelperRegistry archives on register/update
# ---------------------------------------------------------------------------

class TestHelperRegistryVersioning:
    def test_first_register_no_version(self, tmp_path):
        db = BrixDB(db_path=tmp_path / "brix.db")
        reg_path = tmp_path / "registry.yaml"
        registry = HelperRegistry(registry_path=reg_path, db=db)
        script = tmp_path / "helper.py"
        script.write_text("print('hello')")
        registry.register("my-helper", str(script))
        rows = db.get_object_versions("helper", "my-helper")
        assert len(rows) == 0

    def test_re_register_archives_old_code(self, tmp_path):
        db = BrixDB(db_path=tmp_path / "brix.db")
        reg_path = tmp_path / "registry.yaml"
        registry = HelperRegistry(registry_path=reg_path, db=db)
        script_v1 = tmp_path / "helper_v1.py"
        script_v1.write_text("print('v1')")
        registry.register("my-helper", str(script_v1))
        # Re-register with a different script path (new v2 script)
        script_v2 = tmp_path / "helper_v2.py"
        script_v2.write_text("print('v2')")
        registry.register("my-helper", str(script_v2))
        rows = db.get_object_versions("helper", "my-helper")
        assert len(rows) == 1
        content = json.loads(rows[0]["content"])
        assert "v1" in content.get("code", "")

    def test_update_with_new_script_archives(self, tmp_path):
        db = BrixDB(db_path=tmp_path / "brix.db")
        reg_path = tmp_path / "registry.yaml"
        registry = HelperRegistry(registry_path=reg_path, db=db)
        script_v1 = tmp_path / "helper_v1.py"
        script_v1.write_text("print('original')")
        registry.register("upd-helper", str(script_v1))
        script_v2 = tmp_path / "helper_v2.py"
        script_v2.write_text("print('updated')")
        registry.update("upd-helper", script=str(script_v2))
        rows = db.get_object_versions("helper", "upd-helper")
        assert len(rows) == 1
        content = json.loads(rows[0]["content"])
        assert "original" in content.get("code", "")

    def test_update_no_script_change_no_archive(self, tmp_path):
        """Updating description only must NOT archive (no script change)."""
        db = BrixDB(db_path=tmp_path / "brix.db")
        reg_path = tmp_path / "registry.yaml"
        registry = HelperRegistry(registry_path=reg_path, db=db)
        script = tmp_path / "helper.py"
        script.write_text("pass")
        registry.register("desc-helper", str(script), description="old desc")
        # Update only description — same script path
        registry.update("desc-helper", description="new desc")
        rows = db.get_object_versions("helper", "desc-helper")
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# 5. MCP tools
# ---------------------------------------------------------------------------

@pytest.fixture
def isolated_brix_env(tmp_path, monkeypatch):
    """Set up a fully isolated brix environment with consistent tmp_path.

    Patches brix.db.BRIX_DB_PATH and mcp_server.PIPELINE_DIR so that ALL
    BrixDB() instances (in handlers AND in stores) land in the same temp db.
    """
    brix_dir = tmp_path / ".brix"
    brix_dir.mkdir(parents=True, exist_ok=True)
    db_path = brix_dir / "brix.db"
    pd = brix_dir / "pipelines"
    pd.mkdir(parents=True, exist_ok=True)

    # Patch the module-level constant so BrixDB() with no args uses our temp db
    monkeypatch.setattr("brix.db.BRIX_DB_PATH", db_path)
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", pd)

    db = BrixDB(db_path=db_path)
    return {"tmp_path": tmp_path, "pipeline_dir": pd, "db": db}


class TestMCPGetVersions:
    def test_returns_empty_for_unknown(self, isolated_brix_env):
        result = asyncio.get_event_loop().run_until_complete(
            _handle_get_versions({"type": "pipeline", "name": "nonexistent"})
        )
        assert result["success"] is True
        assert result["versions"] == []
        assert result["count"] == 0

    def test_invalid_type(self, isolated_brix_env):
        result = asyncio.get_event_loop().run_until_complete(
            _handle_get_versions({"type": "unknown", "name": "x"})
        )
        assert result["success"] is False

    def test_missing_name(self, isolated_brix_env):
        result = asyncio.get_event_loop().run_until_complete(
            _handle_get_versions({"type": "pipeline", "name": ""})
        )
        assert result["success"] is False

    def test_lists_versions_after_saves(self, isolated_brix_env):
        env = isolated_brix_env
        store = PipelineStore(pipelines_dir=env["pipeline_dir"], db=env["db"])
        store.save({"name": "versioned", "version": "1.0.0", "steps": []}, "versioned")
        store.save({"name": "versioned", "version": "1.0.1", "steps": []}, "versioned")

        result = asyncio.get_event_loop().run_until_complete(
            _handle_get_versions({"type": "pipeline", "name": "versioned"})
        )
        assert result["success"] is True
        assert result["count"] == 1
        v = result["versions"][0]
        assert "version_id" in v
        assert "created_at" in v
        assert "size" in v


class TestMCPRollback:
    def test_rollback_missing_version(self, isolated_brix_env):
        result = asyncio.get_event_loop().run_until_complete(
            _handle_rollback({"type": "pipeline", "name": "p", "version_id": "no-such-vid"})
        )
        assert result["success"] is False
        assert "not found" in result["error"]

    def test_rollback_pipeline(self, isolated_brix_env):
        env = isolated_brix_env
        store = PipelineStore(pipelines_dir=env["pipeline_dir"], db=env["db"])
        store.save({"name": "rollpipe", "version": "1.0.0", "steps": []}, "rollpipe")
        store.save({"name": "rollpipe", "version": "2.0.0", "steps": [{"id": "s"}]}, "rollpipe")

        rows = env["db"].get_object_versions("pipeline", "rollpipe")
        assert len(rows) == 1
        vid = rows[0]["version_id"]

        result = asyncio.get_event_loop().run_until_complete(
            _handle_rollback({"type": "pipeline", "name": "rollpipe", "version_id": vid})
        )
        assert result["success"] is True
        assert result["version_id"] == vid

        # Verify live YAML is now the v1.0.0 content
        restored = store.load_raw("rollpipe")
        assert restored.get("version") == "1.0.0"

    def test_rollback_invalid_type(self, isolated_brix_env):
        result = asyncio.get_event_loop().run_until_complete(
            _handle_rollback({"type": "bad", "name": "x", "version_id": "y"})
        )
        assert result["success"] is False

    def test_rollback_wrong_name_mismatch(self, isolated_brix_env):
        env = isolated_brix_env
        store = PipelineStore(pipelines_dir=env["pipeline_dir"], db=env["db"])
        store.save({"name": "pipe-a", "version": "1.0.0", "steps": []}, "pipe-a")
        store.save({"name": "pipe-a", "version": "2.0.0", "steps": []}, "pipe-a")
        rows = env["db"].get_object_versions("pipeline", "pipe-a")
        vid = rows[0]["version_id"]

        # Try rollback with wrong name
        result = asyncio.get_event_loop().run_until_complete(
            _handle_rollback({"type": "pipeline", "name": "pipe-b", "version_id": vid})
        )
        assert result["success"] is False


class TestMCPDiffVersions:
    def test_diff_two_archived_versions(self, isolated_brix_env):
        env = isolated_brix_env
        store = PipelineStore(pipelines_dir=env["pipeline_dir"], db=env["db"])
        # Save three times: v1, v2, v3 — creates archives for v1 and v2
        store.save({"name": "diffpipe", "version": "1.0.0", "steps": []}, "diffpipe")
        store.save({"name": "diffpipe", "version": "2.0.0", "steps": []}, "diffpipe")
        store.save({"name": "diffpipe", "version": "3.0.0", "steps": []}, "diffpipe")

        rows = env["db"].get_object_versions("pipeline", "diffpipe")
        assert len(rows) == 2
        vid_a = rows[1]["version_id"]  # older (v1.0.0)
        vid_b = rows[0]["version_id"]  # newer (v2.0.0)

        result = asyncio.get_event_loop().run_until_complete(
            _handle_diff_versions({"type": "pipeline", "name": "diffpipe",
                                    "version_id_a": vid_a, "version_id_b": vid_b})
        )
        assert result["success"] is True
        assert result["changed"] is True
        assert "diff" in result

    def test_diff_with_current(self, isolated_brix_env):
        env = isolated_brix_env
        store = PipelineStore(pipelines_dir=env["pipeline_dir"], db=env["db"])
        store.save({"name": "curdiff", "version": "1.0.0", "steps": []}, "curdiff")
        store.save({"name": "curdiff", "version": "2.0.0", "steps": []}, "curdiff")

        rows = env["db"].get_object_versions("pipeline", "curdiff")
        vid = rows[0]["version_id"]

        result = asyncio.get_event_loop().run_until_complete(
            _handle_diff_versions({"type": "pipeline", "name": "curdiff",
                                    "version_id_a": vid, "version_id_b": "current"})
        )
        assert result["success"] is True
        assert "diff" in result

    def test_diff_identical_returns_no_differences(self, isolated_brix_env):
        env = isolated_brix_env
        store = PipelineStore(pipelines_dir=env["pipeline_dir"], db=env["db"])
        store.save({"name": "same", "version": "1.0.0", "steps": []}, "same")
        store.save({"name": "same", "version": "1.0.0", "steps": []}, "same")

        rows = env["db"].get_object_versions("pipeline", "same")
        vid = rows[0]["version_id"]

        # Compare archived version against itself
        result = asyncio.get_event_loop().run_until_complete(
            _handle_diff_versions({"type": "pipeline", "name": "same",
                                    "version_id_a": vid, "version_id_b": vid})
        )
        assert result["success"] is True
        assert result["changed"] is False
        assert "(no differences)" in result["diff"]

    def test_diff_missing_version_id(self, isolated_brix_env):
        result = asyncio.get_event_loop().run_until_complete(
            _handle_diff_versions({"type": "pipeline", "name": "x",
                                    "version_id_a": "", "version_id_b": "y"})
        )
        assert result["success"] is False

    def test_diff_nonexistent_version(self, isolated_brix_env):
        result = asyncio.get_event_loop().run_until_complete(
            _handle_diff_versions({"type": "pipeline", "name": "x",
                                    "version_id_a": "no-vid-a", "version_id_b": "no-vid-b"})
        )
        assert result["success"] is False


# ---------------------------------------------------------------------------
# 6. brix clean --versions CLI
# ---------------------------------------------------------------------------

class TestCleanVersionsCLI:
    def test_clean_versions_flag(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        runner = CliRunner()
        # BrixDB is imported inside the function, so patch the module it comes from
        with patch("brix.db.BrixDB") as MockDB:
            instance = MockDB.return_value
            instance.cleanup_all_versions.return_value = 5
            result = runner.invoke(main, ["clean", "--versions"])

        assert result.exit_code == 0
        instance.cleanup_all_versions.assert_called_once_with(keep=10)

    def test_clean_versions_dry_run(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        runner = CliRunner()
        with patch("brix.db.BrixDB") as MockDB:
            result = runner.invoke(main, ["clean", "--versions", "--dry-run"])

        assert result.exit_code == 0
        MockDB.return_value.cleanup_all_versions.assert_not_called()

    def test_clean_versions_custom_keep(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        runner = CliRunner()
        with patch("brix.db.BrixDB") as MockDB:
            instance = MockDB.return_value
            instance.cleanup_all_versions.return_value = 3
            result = runner.invoke(main, ["clean", "--versions", "--keep", "5"])

        assert result.exit_code == 0
        instance.cleanup_all_versions.assert_called_once_with(keep=5)
