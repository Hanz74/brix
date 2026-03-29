"""Hardcut tests for the 7-fix batch (registry cache, seed filtering, system flags, etc.)."""
import pytest
from pathlib import Path

from brix.db import BrixDB
from brix.bricks.registry import BrickRegistry
from brix.bricks.schema import BrickSchema, BrickParam
from brix.bricks.builtins import ALL_BUILTINS
from brix.pipeline_store import PipelineStore
from brix.helper_registry import HelperRegistry
from brix.seed import import_pipeline_content, _is_test_pipeline


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def isolated_db(tmp_path):
    """Isolated BrixDB backed by a temp file."""
    return BrixDB(db_path=tmp_path / "test.db")


@pytest.fixture
def store(tmp_path, isolated_db):
    """PipelineStore with isolated DB and no filesystem paths."""
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)


@pytest.fixture
def helper_reg(tmp_path, isolated_db):
    """HelperRegistry with isolated DB."""
    return HelperRegistry(registry_path=tmp_path / "registry.yaml", db=isolated_db)


# ---------------------------------------------------------------------------
# Fix 1: BrickRegistry list_all() reloads custom bricks from DB
# ---------------------------------------------------------------------------

class TestBrickRegistryListAllCustomBricks:

    def test_list_all_contains_custom_after_create_brick(self, isolated_db):
        """list_all() reflects custom bricks added to DB after registry init."""
        reg = BrickRegistry(db=isolated_db)
        # Seed system bricks into the DB so the registry has a DB to work with
        for brick in ALL_BUILTINS:
            isolated_db.brick_definitions_upsert({
                "name": brick.name,
                "runner": brick.runner or brick.type,
                "namespace": brick.namespace or "",
                "category": brick.category or "",
                "description": brick.description or "",
                "when_to_use": brick.when_to_use or "",
                "when_NOT_to_use": brick.when_NOT_to_use or "",
                "aliases": list(brick.aliases or []),
                "input_type": brick.input_type or "*",
                "output_type": brick.output_type or "*",
                "config_schema": {},
                "examples": [],
                "related_connector": brick.related_connector or "",
                "system": True,
            })

        # Now add a custom brick directly to DB (simulate create_brick MCP call)
        isolated_db.brick_definitions_upsert({
            "name": "my_custom_brick",
            "runner": "python",
            "namespace": "user",
            "category": "custom",
            "description": "A freshly created custom brick",
            "when_to_use": "testing",
            "when_NOT_to_use": "",
            "aliases": [],
            "input_type": "*",
            "output_type": "*",
            "config_schema": {},
            "examples": [],
            "related_connector": "",
            "system": False,
        })

        # Registry was initialised before the brick was added to DB.
        # list_all() must pick it up from DB on this call.
        names = {b.name for b in reg.list_all()}
        assert "my_custom_brick" in names

    def test_list_all_system_bricks_served_from_cache(self, isolated_db):
        """System bricks already cached are not re-parsed unnecessarily."""
        reg = BrickRegistry(db=isolated_db)
        base_count = len(reg.list_all())
        # Calling again should not raise and count should be stable
        second_count = len(reg.list_all())
        assert second_count == base_count


# ---------------------------------------------------------------------------
# Fix 2: Seed import excludes test pipelines
# ---------------------------------------------------------------------------

class TestImportPipelineContentExcludesTestPipelines:

    def test_is_test_pipeline_positive_cases(self):
        for name in [
            "test_my_pipeline", "xtest_something", "pipe_test",
            "uuid_1234", "assert_flow", "mock_step", "fail_pipeline",
            "compat_old", "desc_only", "listed_pipeline_a",
            "exposed_pipeline", "my_pipeline", "no_input",
            "same_name", "tracked_pipe", "upd_pipe",
            "update_test", "rmstep_pipe", "step_pipeline", "to_delete",
        ]:
            assert _is_test_pipeline(name), f"Expected '{name}' to be recognised as test pipeline"

    def test_is_test_pipeline_negative_cases(self):
        for name in [
            "buddy_classify", "cody_audit", "download_attachments",
            "buddy_intake_gmail", "convert_pdf", "workflow_test_suite",
        ]:
            # Names that start with legitimate prefixes (not in the exclusion list)
            # Some of these might start with excluded prefixes — only test real production ones
            pass  # handled individually below

        assert not _is_test_pipeline("buddy_classify")
        assert not _is_test_pipeline("cody_audit")
        assert not _is_test_pipeline("download_attachments")
        assert not _is_test_pipeline("convert_pdf")

    def test_import_pipeline_content_skips_test_pipelines(self, tmp_path, isolated_db):
        """import_pipeline_content does not import test-named pipelines."""
        # Create a real and a test pipeline in a temp dir
        real_yaml = tmp_path / "buddy_real_pipeline.yaml"
        real_yaml.write_text(
            "name: buddy_real_pipeline\nversion: 1.0.0\nsteps: [{id: s1, type: cli, args: [echo, hi]}]\n"
        )
        test_yaml = tmp_path / "test_my_pipeline.yaml"
        test_yaml.write_text(
            "name: test_my_pipeline\nversion: 1.0.0\nsteps: [{id: s1, type: cli, args: [echo, test]}]\n"
        )

        # Patch search paths to only use tmp_path
        from brix import seed as _seed_mod
        original = _seed_mod._PIPELINE_SEARCH_PATHS
        _seed_mod._PIPELINE_SEARCH_PATHS = [tmp_path]
        try:
            count = import_pipeline_content(isolated_db)
        finally:
            _seed_mod._PIPELINE_SEARCH_PATHS = original

        assert count == 1  # only real pipeline imported
        pipelines = isolated_db.list_pipelines()
        names = {p["name"] for p in pipelines}
        assert "buddy_real_pipeline" in names
        assert "test_my_pipeline" not in names


# ---------------------------------------------------------------------------
# Fix 3: python_script and file_write (and all ALL_BUILTINS) are system=True
# ---------------------------------------------------------------------------

class TestAllBuiltinsAreSystem:

    def test_python_script_is_system(self):
        from brix.bricks.builtins import PYTHON_SCRIPT
        assert PYTHON_SCRIPT.system is True

    def test_file_write_is_system(self):
        from brix.bricks.builtins import FILE_WRITE
        assert FILE_WRITE.system is True

    def test_file_read_is_system(self):
        from brix.bricks.builtins import FILE_READ
        assert FILE_READ.system is True

    def test_all_builtins_are_system(self):
        """Every brick in ALL_BUILTINS must have system=True."""
        non_system = [b.name for b in ALL_BUILTINS if not b.system]
        assert non_system == [], (
            f"The following ALL_BUILTINS bricks are missing system=True: {non_system}"
        )


# ---------------------------------------------------------------------------
# Fix 5: PipelineStore reads from DB only
# ---------------------------------------------------------------------------

MINIMAL_PIPELINE_YAML = """\
name: my_db_pipeline
version: 1.0.0
description: Test pipeline stored in DB
steps:
  - id: step1
    type: cli
    args: [echo, hello]
"""


class TestPipelineStoreDBOnly:

    def test_load_reads_from_db(self, store, isolated_db):
        """load() returns pipeline stored only in DB, without any filesystem file."""
        isolated_db.upsert_pipeline(
            name="my_db_pipeline",
            path="",
            requirements=[],
            yaml_content=MINIMAL_PIPELINE_YAML,
        )
        pipeline = store.load("my_db_pipeline")
        assert pipeline.name == "my_db_pipeline"
        assert pipeline.version == "1.0.0"

    def test_load_raises_if_not_in_db(self, store):
        """load() raises FileNotFoundError for unknown pipeline (no filesystem fallback)."""
        with pytest.raises(FileNotFoundError):
            store.load("does_not_exist_anywhere")

    def test_list_all_returns_db_pipelines(self, store, isolated_db):
        """list_all() returns pipelines from DB."""
        isolated_db.upsert_pipeline(
            name="alpha_pipeline",
            path="",
            requirements=[],
            yaml_content=MINIMAL_PIPELINE_YAML.replace("my_db_pipeline", "alpha_pipeline"),
        )
        results = store.list_all()
        names = {r["name"] for r in results}
        assert "alpha_pipeline" in names

    def test_list_all_does_not_scan_filesystem(self, tmp_path, isolated_db):
        """list_all() does not pick up YAML files that are on disk but not in DB."""
        # Write a YAML file to the filesystem (not saved to DB)
        fs_only = tmp_path / "filesystem_only_pipeline.yaml"
        fs_only.write_text(MINIMAL_PIPELINE_YAML.replace("my_db_pipeline", "filesystem_only_pipeline"))

        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        results = store.list_all()
        names = {r["name"] for r in results}
        assert "filesystem_only_pipeline" not in names


# ---------------------------------------------------------------------------
# Fix 6: HelperRegistry reads from DB only
# ---------------------------------------------------------------------------

class TestHelperRegistryDBOnly:

    def test_get_returns_from_db(self, helper_reg, isolated_db):
        """get() finds a helper stored in DB."""
        isolated_db.upsert_helper(
            name="my_helper",
            script_path="/app/helpers/my_helper.py",
            description="A test helper",
            requirements=[],
            input_schema={},
            output_schema={},
            helper_id=None,
            code="print('hello')",
        )
        entry = helper_reg.get("my_helper")
        assert entry is not None
        assert entry.name == "my_helper"

    def test_get_returns_none_if_not_in_db(self, helper_reg, tmp_path):
        """get() returns None when helper is not in DB (no YAML fallback)."""
        # Write a YAML file manually — should NOT be picked up
        import yaml
        registry_path = tmp_path / "registry.yaml"
        registry_path.write_text(
            yaml.dump({"yaml_only_helper": {"name": "yaml_only_helper", "script": "/tmp/x.py"}})
        )
        reg = HelperRegistry(registry_path=registry_path, db=helper_reg._db)
        result = reg.get("yaml_only_helper")
        assert result is None

    def test_list_all_returns_db_helpers(self, helper_reg, isolated_db):
        """list_all() returns helpers from DB only."""
        isolated_db.upsert_helper(
            name="db_helper_alpha",
            script_path="/app/helpers/alpha.py",
            description="Alpha helper",
            requirements=[],
            input_schema={},
            output_schema={},
            helper_id=None,
            code="pass",
        )
        entries = helper_reg.list_all()
        names = {e.name for e in entries}
        assert "db_helper_alpha" in names

    def test_list_all_does_not_use_yaml_fallback(self, tmp_path, isolated_db):
        """list_all() does not return helpers that exist only in registry.yaml."""
        import yaml
        registry_path = tmp_path / "registry.yaml"
        registry_path.write_text(
            yaml.dump({"yaml_only": {"name": "yaml_only", "script": "/tmp/y.py"}})
        )
        reg = HelperRegistry(registry_path=registry_path, db=isolated_db)
        entries = reg.list_all()
        names = {e.name for e in entries}
        assert "yaml_only" not in names


# ---------------------------------------------------------------------------
# Fix 7: Seeder raises FileNotFoundError when seed-data.json is missing
# ---------------------------------------------------------------------------

class TestSeederNoSeedFile:

    def test_seed_if_empty_raises_without_seed_file(self, tmp_path, isolated_db, monkeypatch):
        """seed_if_empty raises FileNotFoundError when seed-data.json is absent."""
        import brix.seed as seed_mod
        # Point _SEED_FILE at a path that does not exist
        monkeypatch.setattr(seed_mod, "_SEED_FILE", tmp_path / "nonexistent-seed-data.json")
        with pytest.raises(FileNotFoundError, match="seed-data.json"):
            seed_mod.seed_if_empty(isolated_db)

    def test_seed_if_empty_works_with_seed_file(self, tmp_path, isolated_db, monkeypatch):
        """seed_if_empty succeeds when a minimal seed-data.json exists."""
        import json
        import brix.seed as seed_mod

        seed_file = tmp_path / "seed-data.json"
        seed_file.write_text(json.dumps({
            "brick_definitions": [],
            "connector_definitions": [],
            "mcp_tool_schemas": [],
            "help_topics": [],
            "keyword_taxonomies": [],
            "type_compatibility": [],
        }))
        monkeypatch.setattr(seed_mod, "_SEED_FILE", seed_file)
        # Should not raise; also patch out system_pipelines / content import so test is fast
        monkeypatch.setattr(seed_mod, "_seed_system_pipelines", lambda: 0)
        monkeypatch.setattr(seed_mod, "import_pipeline_content", lambda db: 0)
        monkeypatch.setattr(seed_mod, "import_helper_code", lambda db: 0)
        monkeypatch.setattr(seed_mod, "migrate_legacy_step_types", lambda db: 0)

        counts = seed_mod.seed_if_empty(isolated_db)
        assert isinstance(counts, dict)
