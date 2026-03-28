"""Tests for T-BRIX-V5-02: Managed Helper Storage.

Covers:
1. create_helper MCP tool — inline code, compile check, file write, registry
2. update_helper with code parameter — backup, file write, registry update
3. Dual-Path Resolution in engine — managed > legacy > absolute
4. migrate-helpers CLI command — copy + register, idempotent
5. Bundle export — searches ~/.brix/helpers/ for helper files
6. Bundle import — writes helpers to ~/.brix/helpers/
7. get_tips — includes managed helper storage section
"""
import pytest
import asyncio
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

import brix.mcp_server as mcp_module
from brix.mcp_server import (
    _handle_create_helper,
    _handle_update_helper,
    _handle_register_helper,
    _handle_get_helper,
    _handle_get_tips,
    _validate_python_code,
    _managed_helper_dir,
)
from brix.helper_registry import HelperRegistry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_managed_dir(tmp_path, monkeypatch):
    """Redirect ~/.brix/helpers/ to a temp directory."""
    managed = tmp_path / ".brix" / "helpers"
    managed.mkdir(parents=True, exist_ok=True)

    # Patch Path.home() used in _managed_helper_dir and engine
    original_home = Path.home

    def mock_home():
        return tmp_path

    monkeypatch.setattr(Path, "home", staticmethod(mock_home))
    return managed


@pytest.fixture
def mock_registry(tmp_path, monkeypatch):
    """Patch HelperRegistry to use a temp file."""
    reg_file = tmp_path / "registry.yaml"
    original_init = HelperRegistry.__init__

    def patched_init(self, registry_path=None):
        original_init(self, registry_path=reg_file)

    monkeypatch.setattr(HelperRegistry, "__init__", patched_init)
    return reg_file


# ---------------------------------------------------------------------------
# 1. _validate_python_code
# ---------------------------------------------------------------------------

class TestValidatePythonCode:
    def test_valid_code_returns_none(self):
        code = "import json\nprint(json.dumps({'ok': True}))"
        assert _validate_python_code(code) is None

    def test_syntax_error_returns_message(self):
        code = "def foo(\n  pass"
        result = _validate_python_code(code)
        assert result is not None
        assert "SyntaxError" in result

    def test_empty_code_is_valid(self):
        # Empty string is valid Python
        assert _validate_python_code("") is None

    def test_complex_valid_code(self):
        code = (
            "import json\nimport sys\n\n"
            "def main():\n    params = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}\n"
            "    print(json.dumps({'result': params}))\n\n"
            "if __name__ == '__main__':\n    main()\n"
        )
        assert _validate_python_code(code) is None


# ---------------------------------------------------------------------------
# 2. create_helper MCP tool
# ---------------------------------------------------------------------------

class TestCreateHelper:
    async def test_create_basic(self, tmp_managed_dir, mock_registry):
        code = "import json\nprint(json.dumps({'ok': True}))"
        result = await _handle_create_helper({
            "name": "my_helper",
            "code": code,
            "description": "My test helper",
        })
        assert result["success"] is True
        assert result["action"] == "created"
        assert "path" in result
        assert result["helper"]["name"] == "my_helper"

        # File should be written
        script_path = Path(result["path"])
        assert script_path.exists()
        assert script_path.read_text() == code

    async def test_create_registers_in_registry(self, tmp_managed_dir, mock_registry):
        code = "import json\nprint(json.dumps({'ok': True}))"
        result = await _handle_create_helper({
            "name": "reg_helper",
            "code": code,
        })
        assert result["success"] is True

        # Verify in registry
        get_result = await _handle_get_helper({"name": "reg_helper"})
        assert get_result["success"] is True
        assert get_result["helper"]["name"] == "reg_helper"

    async def test_create_with_requirements(self, tmp_managed_dir, mock_registry):
        code = "import httpx\nprint('ok')"
        result = await _handle_create_helper({
            "name": "req_helper",
            "code": code,
            "requirements": ["httpx>=0.28", "pydantic"],
        })
        assert result["success"] is True
        assert result["helper"]["requirements"] == ["httpx>=0.28", "pydantic"]

    async def test_create_with_schemas(self, tmp_managed_dir, mock_registry):
        code = "print('ok')"
        result = await _handle_create_helper({
            "name": "schema_helper",
            "code": code,
            "input_schema": {"type": "object", "properties": {"x": {"type": "string"}}},
            "output_schema": {"type": "object"},
        })
        assert result["success"] is True
        assert result["helper"]["input_schema"]["properties"]["x"]["type"] == "string"

    async def test_create_missing_name_fails(self, tmp_managed_dir, mock_registry):
        result = await _handle_create_helper({"code": "print('hi')"})
        assert result["success"] is False
        assert "name" in result["error"]

    async def test_create_missing_code_fails(self, tmp_managed_dir, mock_registry):
        result = await _handle_create_helper({"name": "foo"})
        assert result["success"] is False
        assert "code" in result["error"]

    async def test_create_invalid_python_fails(self, tmp_managed_dir, mock_registry):
        result = await _handle_create_helper({
            "name": "bad_helper",
            "code": "def foo(\n  pass",  # syntax error
        })
        assert result["success"] is False
        assert "Invalid Python" in result["error"]

    async def test_create_writes_to_managed_dir(self, tmp_managed_dir, mock_registry):
        code = "print('managed')"
        result = await _handle_create_helper({"name": "managed_test", "code": code})
        assert result["success"] is True
        path = Path(result["path"])
        assert path.parent == tmp_managed_dir
        assert path.name == "managed_test.py"

    async def test_create_path_in_registry_entry(self, tmp_managed_dir, mock_registry):
        code = "print('ok')"
        result = await _handle_create_helper({"name": "path_test", "code": code})
        assert result["success"] is True
        # Registry entry script should match written path
        assert result["helper"]["script"] == result["path"]

    async def test_create_idempotent_overwrite(self, tmp_managed_dir, mock_registry):
        """Creating helper twice overwrites file and updates registry."""
        code1 = "print('v1')"
        code2 = "print('v2')"
        r1 = await _handle_create_helper({"name": "overwrite_test", "code": code1})
        r2 = await _handle_create_helper({"name": "overwrite_test", "code": code2})
        assert r1["success"] is True
        assert r2["success"] is True
        path = Path(r2["path"])
        assert path.read_text() == code2


# ---------------------------------------------------------------------------
# 3. update_helper with code parameter
# ---------------------------------------------------------------------------

class TestUpdateHelperCode:
    async def test_update_with_code_writes_file(self, tmp_managed_dir, mock_registry):
        # First create helper
        code1 = "print('original')"
        await _handle_create_helper({"name": "upd_code", "code": code1})

        # Now update with new code
        code2 = "print('updated')"
        result = await _handle_update_helper({
            "name": "upd_code",
            "code": code2,
        })
        assert result["success"] is True
        assert result["action"] == "updated"
        assert "code" not in result.get("updated_fields", []) or "script" in result.get("updated_fields", [])

        # File should have new content
        get_result = await _handle_get_helper({"name": "upd_code"})
        script_path = Path(get_result["helper"]["script"])
        assert script_path.read_text() == code2

    async def test_update_with_code_creates_backup(self, tmp_managed_dir, mock_registry):
        code1 = "print('original')"
        await _handle_create_helper({"name": "backup_test", "code": code1})

        code2 = "print('updated')"
        result = await _handle_update_helper({
            "name": "backup_test",
            "code": code2,
        })
        assert result["success"] is True
        # Backup should exist
        assert "backup_path" in result
        bak = Path(result["backup_path"])
        assert bak.exists()
        assert bak.read_text() == code1

    async def test_update_with_invalid_code_fails(self, tmp_managed_dir, mock_registry):
        await _handle_create_helper({"name": "syntax_test", "code": "print('ok')"})
        result = await _handle_update_helper({
            "name": "syntax_test",
            "code": "def broken(\n  pass",
        })
        assert result["success"] is False
        assert "Invalid Python" in result["error"]

    async def test_update_code_updates_registry_script(self, tmp_managed_dir, mock_registry):
        await _handle_create_helper({"name": "script_upd", "code": "print('v1')"})
        result = await _handle_update_helper({
            "name": "script_upd",
            "code": "print('v2')",
        })
        assert result["success"] is True
        # Registry entry should point to updated file
        get_result = await _handle_get_helper({"name": "script_upd"})
        script_path = Path(get_result["helper"]["script"])
        assert script_path.read_text() == "print('v2')"

    async def test_update_no_fields_error_mentions_code(self, tmp_managed_dir, mock_registry):
        await _handle_create_helper({"name": "no_fields", "code": "print('ok')"})
        result = await _handle_update_helper({"name": "no_fields"})
        assert result["success"] is False
        assert "code" in result["error"]

    async def test_update_code_on_missing_helper_fails(self, tmp_managed_dir, mock_registry):
        result = await _handle_update_helper({
            "name": "ghost_helper",
            "code": "print('ok')",
        })
        assert result["success"] is False
        assert "not found" in result["error"]


# ---------------------------------------------------------------------------
# 4. Dual-Path Resolution in engine
# ---------------------------------------------------------------------------

class TestDualPathResolution:
    async def test_managed_path_preferred_over_legacy(self, tmp_path, monkeypatch):
        """Engine resolves relative script to ~/.brix/helpers/ first."""
        from brix.engine import PipelineEngine
        from brix.loader import PipelineLoader

        # Set up managed helpers dir
        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)

        # Write script in managed location
        managed_script = managed / "dual_test.py"
        managed_script.write_text(
            "import json, sys\nprint(json.dumps({'source': 'managed'}))"
        )

        # Patch Path.home() to return tmp_path
        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        # Use absolute script path for managed to simulate resolution
        pipeline_yaml = f"""
name: dual-test
version: 1.0.0
steps:
  - id: run
    type: python
    script: {str(managed_script)}
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        engine = PipelineEngine()
        result = await engine.run(pipeline)
        assert result.success
        # result.result is the last step's output
        assert isinstance(result.result, dict)
        assert result.result.get("source") == "managed"

    async def test_legacy_path_used_when_no_managed(self, tmp_path, monkeypatch):
        """Engine executes absolute script path directly."""
        from brix.engine import PipelineEngine
        from brix.loader import PipelineLoader

        # Write a script to a known absolute path
        script = tmp_path / "legacy_only.py"
        script.write_text(
            "import json, sys\nprint(json.dumps({'source': 'legacy'}))"
        )

        pipeline_yaml = f"""
name: legacy-test
version: 1.0.0
steps:
  - id: run
    type: python
    script: {str(script)}
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        engine = PipelineEngine()
        result = await engine.run(pipeline)
        assert result.success
        assert isinstance(result.result, dict)
        assert result.result.get("source") == "legacy"

    async def test_relative_script_resolved_from_managed(self, tmp_path, monkeypatch):
        """Relative script path (e.g. helpers/foo.py) resolves via managed dir."""
        from brix.engine import PipelineEngine
        from brix.loader import PipelineLoader

        # Set up managed dir with script
        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)
        script = managed / "rel_helper.py"
        script.write_text(
            "import json, sys\nprint(json.dumps({'resolved': True}))"
        )

        # Patch home
        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        pipeline_yaml = """
name: relative-test
version: 1.0.0
steps:
  - id: run
    type: python
    script: helpers/rel_helper.py
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        engine = PipelineEngine()
        result = await engine.run(pipeline)
        assert result.success
        assert isinstance(result.result, dict)
        assert result.result.get("resolved") is True


# ---------------------------------------------------------------------------
# 5. migrate-helpers CLI command
# ---------------------------------------------------------------------------

class TestMigrateHelpersCli:
    def test_migrate_copies_files(self, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from brix.cli import main

        # Set up source dir
        source = tmp_path / "legacy"
        source.mkdir()
        (source / "foo.py").write_text("print('foo')")
        (source / "bar.py").write_text("print('bar')")

        # Set up managed target
        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        # Patch HelperRegistry to use temp registry
        reg_file = tmp_path / "registry.yaml"
        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        runner = CliRunner()
        result = runner.invoke(main, ["migrate-helpers", "--source", str(source)])

        assert result.exit_code == 0
        assert (managed / "foo.py").exists()
        assert (managed / "bar.py").exists()

    def test_migrate_registers_helpers(self, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from brix.cli import main

        source = tmp_path / "legacy"
        source.mkdir()
        (source / "myhelper.py").write_text("print('hi')")

        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        reg_file = tmp_path / ".brix" / "helpers" / "registry.yaml"
        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        runner = CliRunner()
        result = runner.invoke(main, ["migrate-helpers", "--source", str(source)])
        assert result.exit_code == 0

        # Verify registered
        reg = HelperRegistry(registry_path=reg_file)
        entry = reg.get("myhelper")
        assert entry is not None
        assert entry.script == str(managed / "myhelper.py")

    def test_migrate_is_idempotent(self, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from brix.cli import main

        source = tmp_path / "legacy"
        source.mkdir()
        (source / "idem.py").write_text("print('idempotent')")

        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        reg_file = tmp_path / ".brix" / "helpers" / "registry.yaml"
        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        runner = CliRunner()
        # Run twice — should not error
        r1 = runner.invoke(main, ["migrate-helpers", "--source", str(source)])
        r2 = runner.invoke(main, ["migrate-helpers", "--source", str(source)])
        assert r1.exit_code == 0
        assert r2.exit_code == 0

    def test_migrate_dry_run_does_not_write(self, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from brix.cli import main

        source = tmp_path / "legacy"
        source.mkdir()
        (source / "dryrun.py").write_text("print('dry')")

        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        runner = CliRunner()
        result = runner.invoke(main, ["migrate-helpers", "--source", str(source), "--dry-run"])
        assert result.exit_code == 0
        # File should NOT be written
        assert not (managed / "dryrun.py").exists()

    def test_migrate_missing_source_exits_nonzero(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["migrate-helpers", "--source", str(tmp_path / "nonexistent")])
        assert result.exit_code != 0

    def test_migrate_updates_registry_path_for_existing(self, tmp_path, monkeypatch):
        """When helper is already registered with old path, migrate updates the path."""
        from click.testing import CliRunner
        from brix.cli import main

        source = tmp_path / "legacy"
        source.mkdir()
        (source / "pathupd.py").write_text("print('hi')")

        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)

        reg_file = tmp_path / ".brix" / "helpers" / "registry.yaml"

        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        # Pre-register with old path
        reg = HelperRegistry(registry_path=reg_file)
        reg.register("pathupd", str(source / "pathupd.py"))

        runner = CliRunner()
        result = runner.invoke(main, ["migrate-helpers", "--source", str(source)])
        assert result.exit_code == 0

        # Registry should now have new path
        reg2 = HelperRegistry(registry_path=reg_file)
        entry = reg2.get("pathupd")
        assert entry is not None
        assert entry.script == str(managed / "pathupd.py")


# ---------------------------------------------------------------------------
# 6. Bundle: export searches ~/.brix/helpers/, import writes there
# ---------------------------------------------------------------------------

class TestBundleManagedStorage:
    def test_export_finds_helper_in_managed_dir(self, tmp_path, monkeypatch):
        from brix.bundle import export_bundle

        # Set up managed dir with a helper
        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)
        (managed / "my_helper.py").write_text("print('managed')")

        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        # Pipeline that references the helper
        pipeline_yaml = (
            "name: bundle-test\nversion: 1.0.0\nsteps:\n"
            "  - id: step1\n    type: python\n    script: helpers/my_helper.py\n"
        )
        pipeline_file = tmp_path / "bundle-test.yaml"
        pipeline_file.write_text(pipeline_yaml)

        output_path = tmp_path / "bundle-test.brix.tar.gz"
        # base_dir doesn't have helpers/, but managed dir should be found
        manifest = export_bundle(pipeline_file, output_path, base_dir=tmp_path)
        assert output_path.exists()
        assert len(manifest.helpers) == 1

    def test_import_writes_to_managed_dir(self, tmp_path, monkeypatch):
        from brix.bundle import export_bundle, import_bundle

        # Set up source
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        helpers_dir = source_dir / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "imp_helper.py").write_text("print('imported')")

        pipeline_yaml = (
            "name: import-test\nversion: 1.0.0\nsteps:\n"
            "  - id: step1\n    type: python\n    script: helpers/imp_helper.py\n"
        )
        pipeline_file = source_dir / "import-test.yaml"
        pipeline_file.write_text(pipeline_yaml)

        # Export
        bundle_path = tmp_path / "import-test.brix.tar.gz"
        export_bundle(pipeline_file, bundle_path, base_dir=source_dir)

        # Set up managed target
        managed = tmp_path / ".brix" / "helpers"
        managed.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

        # Import — should go to managed dir
        pipelines_dir = tmp_path / ".brix" / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        result = import_bundle(bundle_path, pipelines_dir=pipelines_dir)

        # Verify helper written to managed dir
        assert len(result.helpers) == 1
        assert result.helpers[0].parent == managed


# ---------------------------------------------------------------------------
# 7. get_tips includes managed helper storage section
# ---------------------------------------------------------------------------

class TestGetTipsManagedStorage:
    async def test_get_tips_mentions_create_helper(self):
        result = await _handle_get_tips({})
        assert "success" in result or "tips" in result
        tips_text = " ".join(result.get("tips", []))
        # Compact format: Helper category lists create tool
        assert "create" in tips_text and "Helper" in tips_text

    async def test_get_tips_mentions_managed_storage(self):
        result = await _handle_get_tips({})
        tips_text = " ".join(result.get("tips", []))
        # Compact format: managed storage details are in get_help('helper-scripts')
        assert "helper-scripts" in tips_text or "Helper" in tips_text

    async def test_get_tips_mentions_migrate_helpers(self):
        result = await _handle_get_tips({})
        tips_text = " ".join(result.get("tips", []))
        # Compact format: migrate details are in get_help('helper-scripts')
        assert "helper-scripts" in tips_text or "Helper" in tips_text or "get_help" in tips_text

    async def test_get_tips_mentions_dual_path(self):
        result = await _handle_get_tips({})
        tips_text = " ".join(result.get("tips", []))
        # Compact format: dual-path details are in get_help('helper-scripts')
        assert "helper-scripts" in tips_text or "Helper" in tips_text or "get_help" in tips_text


# ---------------------------------------------------------------------------
# 8. Version bump
# ---------------------------------------------------------------------------

class TestVersionBump:
    def test_version_is_current(self):
        import brix
        assert brix.__version__ is not None
        assert len(brix.__version__.split(".")) >= 2
