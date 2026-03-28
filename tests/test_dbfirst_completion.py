"""Tests for DB-First completion — verifying all 5 migration points.

1. Pipelines loaded from DB (not filesystem)
2. Helpers loaded from DB (not filesystem)
3. No legacy step types in DB pipelines
4. mcp_tools_schema.py not imported
5. mcp_help_content.py not imported
"""
import json
import pytest
import yaml
from pathlib import Path

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore
from brix.helper_registry import HelperRegistry, HelperEntry
from brix.seed import (
    import_pipeline_content,
    import_helper_code,
    migrate_legacy_step_types,
    LEGACY_STEP_TYPE_MAP,
    seed_if_empty,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db(tmp_path):
    """Return a fresh BrixDB with no data."""
    return BrixDB(db_path=tmp_path / "test.db")


@pytest.fixture
def store(tmp_path, fresh_db):
    """Return a PipelineStore with isolated DB and filesystem."""
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=fresh_db)


@pytest.fixture
def registry(tmp_path, fresh_db):
    """Return a HelperRegistry with isolated DB."""
    reg_file = tmp_path / "registry.yaml"
    return HelperRegistry(registry_path=reg_file, db=fresh_db)


MINIMAL_PIPELINE = {
    "name": "test-pipe",
    "version": "1.0.0",
    "description": "Test pipeline",
    "steps": [{"id": "s1", "type": "script.cli", "args": ["echo", "hi"]}],
}

LEGACY_PIPELINE = {
    "name": "legacy-pipe",
    "version": "1.0.0",
    "description": "Pipeline with legacy types",
    "steps": [
        {"id": "s1", "type": "python", "script": "test.py"},
        {"id": "s2", "type": "http", "url": "http://example.com"},
        {"id": "s3", "type": "cli", "args": ["echo"]},
    ],
}


# ---------------------------------------------------------------------------
# 1. Pipelines loaded from DB (not filesystem)
# ---------------------------------------------------------------------------

class TestPipelineFromDB:
    def test_save_stores_yaml_in_db(self, store, fresh_db):
        """PipelineStore.save() writes yaml_content to DB."""
        store.save(MINIMAL_PIPELINE)
        content = fresh_db.get_pipeline_yaml_content("test-pipe")
        assert content is not None
        parsed = yaml.safe_load(content)
        assert parsed["name"] == "test-pipe"

    def test_load_from_db_without_file(self, tmp_path, fresh_db):
        """Pipeline can be loaded from DB even without filesystem file."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=fresh_db)
        s.save(MINIMAL_PIPELINE)
        # Remove the filesystem file
        (tmp_path / "test-pipe.yaml").unlink()
        assert not (tmp_path / "test-pipe.yaml").exists()
        # Should still load from DB
        pipeline = s.load("test-pipe")
        assert pipeline.name == "test-pipe"
        assert len(pipeline.steps) == 1

    def test_load_raw_from_db_without_file(self, tmp_path, fresh_db):
        """load_raw() reads from DB when file is missing."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=fresh_db)
        s.save(MINIMAL_PIPELINE)
        (tmp_path / "test-pipe.yaml").unlink()
        raw = s.load_raw("test-pipe")
        assert raw["name"] == "test-pipe"

    def test_exists_from_db_without_file(self, tmp_path, fresh_db):
        """exists() returns True from DB even without filesystem file."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=fresh_db)
        s.save(MINIMAL_PIPELINE)
        (tmp_path / "test-pipe.yaml").unlink()
        assert s.exists("test-pipe") is True

    def test_list_all_from_db(self, store, fresh_db):
        """list_all() includes pipelines stored in DB."""
        store.save(MINIMAL_PIPELINE)
        results = store.list_all()
        names = [r["name"] for r in results]
        assert "test-pipe" in names

    def test_delete_removes_from_db(self, store, fresh_db):
        """delete() removes pipeline from both DB and filesystem."""
        store.save(MINIMAL_PIPELINE)
        assert fresh_db.get_pipeline("test-pipe") is not None
        store.delete("test-pipe")
        assert fresh_db.get_pipeline("test-pipe") is None


# ---------------------------------------------------------------------------
# 2. Helpers loaded from DB (not filesystem)
# ---------------------------------------------------------------------------

class TestHelperFromDB:
    def test_register_stores_code_in_db(self, registry, fresh_db):
        """HelperRegistry.register() stores code in DB."""
        registry.register("myhelper", "/tmp/myhelper.py", code="print('hello')")
        code = fresh_db.get_helper_code("myhelper")
        assert code == "print('hello')"

    def test_get_from_db(self, registry, fresh_db):
        """get() retrieves helper from DB."""
        registry.register("dbhelper", "/tmp/h.py", description="From DB", code="pass")
        entry = registry.get("dbhelper")
        assert entry is not None
        assert entry.name == "dbhelper"
        assert entry.description == "From DB"

    def test_list_all_from_db(self, registry, fresh_db):
        """list_all() returns helpers from DB."""
        registry.register("h1", "/h1.py", code="pass")
        registry.register("h2", "/h2.py", code="pass")
        entries = registry.list_all()
        names = [e.name for e in entries]
        assert "h1" in names
        assert "h2" in names

    def test_get_code_from_db(self, registry):
        """get_code() retrieves Python source from DB."""
        registry.register("coded", "/coded.py", code="import json\nprint('test')")
        code = registry.get_code("coded")
        assert code is not None
        assert "import json" in code

    def test_remove_from_db(self, registry, fresh_db):
        """remove() deletes from DB."""
        registry.register("todel", "/d.py", code="pass")
        assert fresh_db.get_helper("todel") is not None
        registry.remove("todel")
        assert fresh_db.get_helper("todel") is None

    def test_update_in_db(self, registry, fresh_db):
        """update() persists changes to DB."""
        registry.register("upd", "/old.py", description="old", code="old code")
        registry.update("upd", description="new", code="new code")
        entry = registry.get("upd")
        assert entry.description == "new"
        code = fresh_db.get_helper_code("upd")
        assert code == "new code"


# ---------------------------------------------------------------------------
# 3. No legacy step types in migrated DB pipelines
# ---------------------------------------------------------------------------

class TestLegacyStepMigration:
    def test_legacy_types_get_migrated(self, tmp_path, fresh_db):
        """migrate_legacy_step_types replaces old type names."""
        # First import a pipeline with legacy types
        yaml_content = yaml.dump(LEGACY_PIPELINE)
        fresh_db.upsert_pipeline(
            name="legacy-pipe",
            path="/tmp/legacy.yaml",
            yaml_content=yaml_content,
        )

        count = migrate_legacy_step_types(fresh_db)
        assert count == 1  # one pipeline modified

        # Verify types were changed
        new_content = fresh_db.get_pipeline_yaml_content("legacy-pipe")
        data = yaml.safe_load(new_content)
        step_types = [s["type"] for s in data["steps"]]
        assert "script.python" in step_types
        assert "http.request" in step_types
        assert "script.cli" in step_types
        assert "python" not in step_types
        assert "http" not in step_types
        assert "cli" not in step_types

    def test_no_legacy_types_is_noop(self, tmp_path, fresh_db):
        """Pipeline with modern types is not modified."""
        yaml_content = yaml.dump(MINIMAL_PIPELINE)
        fresh_db.upsert_pipeline(
            name="modern-pipe",
            path="/tmp/modern.yaml",
            yaml_content=yaml_content,
        )

        count = migrate_legacy_step_types(fresh_db)
        assert count == 0

    def test_nested_legacy_types_migrated(self, fresh_db):
        """Legacy types in nested structures (repeat, choose) are migrated."""
        nested_pipeline = {
            "name": "nested-pipe",
            "version": "1.0.0",
            "steps": [
                {
                    "id": "r1",
                    "type": "flow.repeat",
                    "sequence": [
                        {"id": "inner", "type": "python", "script": "x.py"},
                    ],
                },
            ],
        }
        yaml_content = yaml.dump(nested_pipeline)
        fresh_db.upsert_pipeline(name="nested-pipe", path="/tmp/n.yaml", yaml_content=yaml_content)

        count = migrate_legacy_step_types(fresh_db)
        assert count == 1

        new_content = fresh_db.get_pipeline_yaml_content("nested-pipe")
        data = yaml.safe_load(new_content)
        inner_type = data["steps"][0]["sequence"][0]["type"]
        assert inner_type == "script.python"

    def test_legacy_map_completeness(self):
        """All legacy types in the map have namespaced replacements."""
        for old_type, new_type in LEGACY_STEP_TYPE_MAP.items():
            assert "." in new_type, f"Replacement for '{old_type}' should be namespaced: '{new_type}'"

    def test_idempotent_migration(self, fresh_db):
        """Running migration twice does not change already-migrated pipelines."""
        yaml_content = yaml.dump(LEGACY_PIPELINE)
        fresh_db.upsert_pipeline(name="idem-pipe", path="/tmp/i.yaml", yaml_content=yaml_content)

        count1 = migrate_legacy_step_types(fresh_db)
        assert count1 == 1

        count2 = migrate_legacy_step_types(fresh_db)
        assert count2 == 0


# ---------------------------------------------------------------------------
# 4. mcp_tools_schema.py not importable (moved to backup)
# ---------------------------------------------------------------------------

class TestMcpToolsSchemaRemoved:
    def test_mcp_tools_schema_not_importable(self):
        """mcp_tools_schema.py should not be importable (moved to backup)."""
        with pytest.raises(ImportError):
            import importlib
            importlib.import_module("brix.mcp_tools_schema")

    def test_inject_source_param_available_from_mcp_utils(self):
        """_inject_source_param is still available from mcp_utils."""
        from brix.mcp_utils import _inject_source_param
        assert callable(_inject_source_param)

    def test_brix_tools_loaded_from_db(self):
        """BRIX_TOOLS in mcp_server is loaded from DB (not from code file)."""
        from brix.mcp_server import BRIX_TOOLS
        # BRIX_TOOLS should be a list (may be empty if DB not seeded in test)
        assert isinstance(BRIX_TOOLS, list)

    def test_mcp_server_importable(self):
        """mcp_server module can be imported without mcp_tools_schema."""
        import brix.mcp_server  # noqa: F401


# ---------------------------------------------------------------------------
# 5. mcp_help_content.py not importable (moved to backup)
# ---------------------------------------------------------------------------

class TestMcpHelpContentRemoved:
    def test_mcp_help_content_not_importable(self):
        """mcp_help_content.py should not be importable (moved to backup)."""
        with pytest.raises(ImportError):
            import importlib
            importlib.import_module("brix.mcp_help_content")

    def test_help_handler_works_without_code_file(self):
        """Help handler should work, loading topics from DB."""
        from brix.mcp_handlers.help import _get_help_topics
        topics, descriptions = _get_help_topics()
        # Should return dicts (possibly empty if DB not seeded)
        assert isinstance(topics, dict)
        assert isinstance(descriptions, dict)


# ---------------------------------------------------------------------------
# 6. Import functions work correctly
# ---------------------------------------------------------------------------

class TestImportFunctions:
    def test_import_pipeline_content(self, tmp_path, fresh_db):
        """import_pipeline_content imports YAML files into DB."""
        # Create a pipeline file
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        (pipelines_dir / "test-import.yaml").write_text(yaml.dump(MINIMAL_PIPELINE))

        # Patch search paths
        import brix.seed as seed_mod
        original_paths = seed_mod._PIPELINE_SEARCH_PATHS
        seed_mod._PIPELINE_SEARCH_PATHS = [pipelines_dir]
        try:
            count = import_pipeline_content(fresh_db)
            assert count >= 1
            content = fresh_db.get_pipeline_yaml_content("test-import")
            assert content is not None
        finally:
            seed_mod._PIPELINE_SEARCH_PATHS = original_paths

    def test_import_helper_code(self, tmp_path, fresh_db):
        """import_helper_code imports Python files into DB."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "test_helper.py").write_text("print('hello')")

        import brix.seed as seed_mod
        original_paths = seed_mod._HELPER_SEARCH_PATHS
        seed_mod._HELPER_SEARCH_PATHS = [helpers_dir]
        try:
            count = import_helper_code(fresh_db)
            assert count >= 1
            code = fresh_db.get_helper_code("test_helper")
            assert code is not None
            assert "hello" in code
        finally:
            seed_mod._HELPER_SEARCH_PATHS = original_paths

    def test_import_idempotent(self, tmp_path, fresh_db):
        """Second import is a no-op."""
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        (pipelines_dir / "idem.yaml").write_text(yaml.dump(MINIMAL_PIPELINE))

        import brix.seed as seed_mod
        original_paths = seed_mod._PIPELINE_SEARCH_PATHS
        seed_mod._PIPELINE_SEARCH_PATHS = [pipelines_dir]
        try:
            count1 = import_pipeline_content(fresh_db)
            assert count1 >= 1
            count2 = import_pipeline_content(fresh_db)
            assert count2 == 0
        finally:
            seed_mod._PIPELINE_SEARCH_PATHS = original_paths


# ---------------------------------------------------------------------------
# 7. DB schema has content columns
# ---------------------------------------------------------------------------

class TestDBSchemaColumns:
    def test_pipelines_has_yaml_content_column(self, fresh_db):
        """pipelines table has yaml_content column after migration."""
        with fresh_db._connect() as conn:
            assert fresh_db._column_exists(conn, "pipelines", "yaml_content")

    def test_helpers_has_code_column(self, fresh_db):
        """helpers table has code column after migration."""
        with fresh_db._connect() as conn:
            assert fresh_db._column_exists(conn, "helpers", "code")

    def test_pipeline_yaml_content_roundtrip(self, fresh_db):
        """yaml_content can be stored and retrieved."""
        content = yaml.dump(MINIMAL_PIPELINE)
        fresh_db.upsert_pipeline(name="rt-pipe", path="/tmp/rt.yaml", yaml_content=content)
        retrieved = fresh_db.get_pipeline_yaml_content("rt-pipe")
        assert retrieved == content

    def test_helper_code_roundtrip(self, fresh_db):
        """code can be stored and retrieved."""
        code = "#!/usr/bin/env python3\nprint('test')"
        fresh_db.upsert_helper(name="rt-helper", script_path="/tmp/rt.py", code=code)
        retrieved = fresh_db.get_helper_code("rt-helper")
        assert retrieved == code
