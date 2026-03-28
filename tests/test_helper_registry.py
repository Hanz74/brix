"""Tests for the Brix HelperRegistry (T-BRIX-V4-BUG-12).

Covers:
- HelperEntry dataclass
- HelperRegistry CRUD: register, get, list_all, search, update, remove
- MCP tool handlers: register_helper, list_helpers, get_helper, search_helpers, update_helper
- Pipeline integration: step.helper resolved to script path in engine
- Validator: helper references checked against registry
"""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from brix.helper_registry import HelperEntry, HelperRegistry, REGISTRY_PATH


# ---------------------------------------------------------------------------
# HelperEntry tests
# ---------------------------------------------------------------------------

class TestHelperEntry:
    def test_defaults(self):
        entry = HelperEntry(name="foo", script="/app/helpers/foo.py")
        assert entry.name == "foo"
        assert entry.script == "/app/helpers/foo.py"
        assert entry.description == ""
        assert entry.requirements == []
        assert entry.input_schema == {}
        assert entry.output_schema == {}

    def test_to_dict(self):
        entry = HelperEntry(
            name="bar",
            script="/tmp/bar.py",
            description="Does bar things",
            requirements=["httpx"],
            input_schema={"type": "object", "properties": {"x": {"type": "string"}}},
            output_schema={"type": "object"},
        )
        d = entry.to_dict()
        assert d["name"] == "bar"
        assert d["script"] == "/tmp/bar.py"
        assert d["description"] == "Does bar things"
        assert d["requirements"] == ["httpx"]
        assert "x" in d["input_schema"]["properties"]

    def test_from_dict_roundtrip(self):
        original = HelperEntry(
            name="baz",
            script="/tmp/baz.py",
            description="baz helper",
            requirements=["pyyaml>=6"],
            input_schema={"type": "object"},
            output_schema={"type": "array"},
        )
        restored = HelperEntry.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.script == original.script
        assert restored.description == original.description
        assert restored.requirements == original.requirements
        assert restored.input_schema == original.input_schema
        assert restored.output_schema == original.output_schema

    def test_from_dict_minimal(self):
        entry = HelperEntry.from_dict({"name": "x", "script": "/tmp/x.py"})
        assert entry.name == "x"
        assert entry.requirements == []
        assert entry.input_schema == {}


# ---------------------------------------------------------------------------
# HelperRegistry CRUD tests
# ---------------------------------------------------------------------------

@pytest.fixture
def isolated_db(tmp_path):
    """Return a BrixDB backed by a temporary database file."""
    from brix.db import BrixDB
    return BrixDB(db_path=tmp_path / "test.db")


@pytest.fixture
def registry(tmp_path, isolated_db):
    """Return a HelperRegistry backed by a temporary file and isolated DB."""
    reg_file = tmp_path / "registry.yaml"
    return HelperRegistry(registry_path=reg_file, db=isolated_db)


class TestHelperRegistryCRUD:
    def test_register_and_get(self, registry):
        entry = registry.register("parse", "/app/helpers/parse.py", description="Parse invoices")
        assert entry.name == "parse"
        assert entry.script == "/app/helpers/parse.py"

        fetched = registry.get("parse")
        assert fetched is not None
        assert fetched.name == "parse"
        assert fetched.script == "/app/helpers/parse.py"
        assert fetched.description == "Parse invoices"

    def test_get_missing_returns_none(self, registry):
        assert registry.get("nonexistent") is None

    def test_register_with_full_fields(self, registry):
        entry = registry.register(
            name="fetch",
            script="/app/helpers/fetch.py",
            description="Fetch data",
            requirements=["httpx>=0.28", "pydantic"],
            input_schema={"type": "object", "properties": {"url": {"type": "string"}}},
            output_schema={"type": "object", "properties": {"data": {"type": "array"}}},
        )
        assert entry.requirements == ["httpx>=0.28", "pydantic"]
        assert "url" in entry.input_schema.get("properties", {})

        # Verify persistence
        fetched = registry.get("fetch")
        assert fetched.requirements == ["httpx>=0.28", "pydantic"]

    def test_register_overwrites_existing(self, registry):
        registry.register("myhel", "/old/path.py", description="old")
        registry.register("myhel", "/new/path.py", description="new")
        entry = registry.get("myhel")
        assert entry.script == "/new/path.py"
        assert entry.description == "new"

    def test_list_all_empty(self, registry):
        assert registry.list_all() == []

    def test_list_all_sorted(self, registry):
        registry.register("zebra", "/z.py")
        registry.register("apple", "/a.py")
        registry.register("mango", "/m.py")
        names = [e.name for e in registry.list_all()]
        assert names == ["apple", "mango", "zebra"]

    def test_search_by_name(self, registry):
        registry.register("parse_invoice", "/parse.py", description="Parse invoices")
        registry.register("fetch_mail", "/fetch.py", description="Fetch emails")
        results = registry.search("parse")
        assert len(results) == 1
        assert results[0].name == "parse_invoice"

    def test_search_by_description(self, registry):
        registry.register("tool_a", "/a.py", description="Handles email processing")
        registry.register("tool_b", "/b.py", description="Processes PDF files")
        results = registry.search("email")
        assert len(results) == 1
        assert results[0].name == "tool_a"

    def test_search_case_insensitive(self, registry):
        registry.register("MyHelper", "/h.py", description="Does Something Special")
        results = registry.search("something special")
        assert len(results) == 1

    def test_search_no_match(self, registry):
        registry.register("alpha", "/a.py", description="Alpha helper")
        results = registry.search("zzznomatch")
        assert results == []

    def test_update_script(self, registry):
        registry.register("upd", "/old.py", description="original")
        updated = registry.update("upd", script="/new.py")
        assert updated.script == "/new.py"
        assert updated.description == "original"  # unchanged

        # Persisted
        fetched = registry.get("upd")
        assert fetched.script == "/new.py"

    def test_update_multiple_fields(self, registry):
        registry.register("multi", "/m.py", description="old desc", requirements=[])
        updated = registry.update("multi", description="new desc", requirements=["requests"])
        assert updated.description == "new desc"
        assert updated.requirements == ["requests"]

    def test_update_missing_raises(self, registry):
        with pytest.raises(KeyError, match="nonexistent"):
            registry.update("nonexistent", script="/x.py")

    def test_remove_existing(self, registry):
        registry.register("todel", "/d.py")
        removed = registry.remove("todel")
        assert removed is True
        assert registry.get("todel") is None

    def test_remove_missing_returns_false(self, registry):
        assert registry.remove("nosuchhelper") is False

    def test_persistence_across_instances(self, tmp_path, isolated_db):
        reg_file = tmp_path / "reg.yaml"
        r1 = HelperRegistry(registry_path=reg_file, db=isolated_db)
        r1.register("persistent", "/p.py", description="Should survive")

        r2 = HelperRegistry(registry_path=reg_file, db=isolated_db)
        entry = r2.get("persistent")
        assert entry is not None
        assert entry.description == "Should survive"


# ---------------------------------------------------------------------------
# MCP handler tests
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_registry(tmp_path, monkeypatch):
    """Patch HelperRegistry to use a temporary file and isolated DB in all handler imports."""
    from brix.db import BrixDB

    reg_file = tmp_path / "registry.yaml"
    test_db = BrixDB(db_path=tmp_path / "test.db")

    original_init = HelperRegistry.__init__

    def patched_init(self, registry_path=None, db=None):
        original_init(self, registry_path=reg_file, db=test_db)

    monkeypatch.setattr(HelperRegistry, "__init__", patched_init)
    return reg_file


class TestMcpHandlerRegisterHelper:
    async def test_register_success(self, mock_registry):
        from brix.mcp_server import _handle_register_helper
        result = await _handle_register_helper({
            "name": "myhel",
            "script": "/app/helpers/myhel.py",
            "description": "My helper",
        })
        assert result["success"] is True
        assert result["action"] == "registered"
        assert result["helper"]["name"] == "myhel"
        assert result["helper"]["script"] == "/app/helpers/myhel.py"

    async def test_register_missing_name(self, mock_registry):
        from brix.mcp_server import _handle_register_helper
        result = await _handle_register_helper({"script": "/foo.py"})
        assert result["success"] is False
        assert "name" in result["error"]

    async def test_register_missing_script(self, mock_registry):
        from brix.mcp_server import _handle_register_helper
        result = await _handle_register_helper({"name": "foo"})
        assert result["success"] is False
        assert "script" in result["error"]

    async def test_register_with_requirements(self, mock_registry):
        from brix.mcp_server import _handle_register_helper
        result = await _handle_register_helper({
            "name": "req_helper",
            "script": "/helpers/req.py",
            "requirements": ["httpx>=0.28", "pandas"],
        })
        assert result["success"] is True
        assert result["helper"]["requirements"] == ["httpx>=0.28", "pandas"]


class TestMcpHandlerListHelpers:
    async def test_list_empty(self, mock_registry):
        from brix.mcp_server import _handle_list_helpers
        result = await _handle_list_helpers({})
        assert result["success"] is True
        assert result["helpers"] == []
        assert result["total"] == 0

    async def test_list_with_entries(self, mock_registry):
        from brix.mcp_server import _handle_register_helper, _handle_list_helpers
        await _handle_register_helper({"name": "a", "script": "/a.py"})
        await _handle_register_helper({"name": "b", "script": "/b.py"})
        result = await _handle_list_helpers({})
        assert result["total"] == 2
        names = [h["name"] for h in result["helpers"]]
        assert "a" in names
        assert "b" in names


class TestMcpHandlerGetHelper:
    async def test_get_existing(self, mock_registry):
        from brix.mcp_server import _handle_register_helper, _handle_get_helper
        await _handle_register_helper({
            "name": "target",
            "script": "/target.py",
            "description": "Target helper",
        })
        result = await _handle_get_helper({"name": "target"})
        assert result["success"] is True
        assert result["helper"]["name"] == "target"
        assert result["helper"]["description"] == "Target helper"

    async def test_get_missing(self, mock_registry):
        from brix.mcp_server import _handle_get_helper
        result = await _handle_get_helper({"name": "nonexistent"})
        assert result["success"] is False
        assert "nonexistent" in result["error"]


class TestMcpHandlerSearchHelpers:
    async def test_search_finds_match(self, mock_registry):
        from brix.mcp_server import _handle_register_helper, _handle_search_helpers
        await _handle_register_helper({"name": "invoice_parser", "script": "/inv.py", "description": "Parses PDF invoices"})
        await _handle_register_helper({"name": "mail_fetcher", "script": "/mail.py", "description": "Fetches emails"})
        result = await _handle_search_helpers({"query": "invoice"})
        assert result["success"] is True
        assert result["total"] == 1
        assert result["helpers"][0]["name"] == "invoice_parser"

    async def test_search_no_results(self, mock_registry):
        from brix.mcp_server import _handle_register_helper, _handle_search_helpers
        await _handle_register_helper({"name": "alpha", "script": "/a.py"})
        result = await _handle_search_helpers({"query": "zzznomatch"})
        assert result["success"] is True
        assert result["total"] == 0


class TestMcpHandlerUpdateHelper:
    async def test_update_description(self, mock_registry):
        from brix.mcp_server import _handle_register_helper, _handle_update_helper
        await _handle_register_helper({"name": "upd", "script": "/u.py", "description": "old"})
        result = await _handle_update_helper({
            "name": "upd",
            "action": "update",
            "description": "new description",
        })
        assert result["success"] is True
        assert result["action"] == "updated"
        assert "description" in result["updated_fields"]
        assert result["helper"]["description"] == "new description"

    async def test_remove_helper(self, mock_registry):
        from brix.mcp_server import _handle_register_helper, _handle_update_helper, _handle_get_helper
        await _handle_register_helper({"name": "todel", "script": "/del.py"})
        result = await _handle_update_helper({"name": "todel", "action": "remove"})
        assert result["success"] is True
        assert result["action"] == "removed"
        # Verify actually removed
        get_result = await _handle_get_helper({"name": "todel"})
        assert get_result["success"] is False

    async def test_remove_missing(self, mock_registry):
        from brix.mcp_server import _handle_update_helper
        result = await _handle_update_helper({"name": "ghost", "action": "remove"})
        assert result["success"] is False

    async def test_update_missing_helper(self, mock_registry):
        from brix.mcp_server import _handle_update_helper
        result = await _handle_update_helper({
            "name": "nosuchhelper",
            "description": "whatever",
        })
        assert result["success"] is False

    async def test_update_no_fields(self, mock_registry):
        from brix.mcp_server import _handle_register_helper, _handle_update_helper
        await _handle_register_helper({"name": "nofields", "script": "/n.py"})
        result = await _handle_update_helper({"name": "nofields"})
        assert result["success"] is False
        assert "No fields" in result["error"]


# ---------------------------------------------------------------------------
# Pipeline integration: step.helper resolved in engine
# ---------------------------------------------------------------------------

class TestEngineHelperResolution:
    async def test_helper_resolves_to_script(self, tmp_path, monkeypatch):
        """Engine resolves step.helper to script path from registry."""
        import sys
        from brix.helper_registry import HelperRegistry
        from brix.engine import PipelineEngine
        from brix.loader import PipelineLoader

        # Setup registry with temp file
        reg_file = tmp_path / "reg.yaml"
        reg = HelperRegistry(registry_path=reg_file)

        # Create a minimal echo helper script
        helper_script = tmp_path / "echo_helper.py"
        helper_script.write_text(
            "import json, sys\n"
            "params = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}\n"
            "print(json.dumps({'echoed': params}))\n"
        )
        reg.register("echo_test", str(helper_script))

        # Patch HelperRegistry to use our temp registry
        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        pipeline_yaml = f"""
name: helper-test
version: 1.0.0
steps:
  - id: run_helper
    type: python
    helper: echo_test
    params:
      x: "hello"
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.success
        assert "run_helper" in result.steps
        assert result.steps["run_helper"].status == "ok"

    async def test_helper_not_found_fails_run(self, tmp_path, monkeypatch):
        """Engine fails the run when step.helper is not in registry."""
        from brix.helper_registry import HelperRegistry
        from brix.engine import PipelineEngine
        from brix.loader import PipelineLoader

        reg_file = tmp_path / "empty_reg.yaml"

        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        pipeline_yaml = """
name: missing-helper-test
version: 1.0.0
steps:
  - id: boom
    type: python
    helper: nonexistent_helper
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert not result.success


# ---------------------------------------------------------------------------
# Validator integration
# ---------------------------------------------------------------------------

class TestValidatorHelperCheck:
    def test_valid_helper_reference(self, tmp_path, monkeypatch):
        """Validator passes when helper exists in registry."""
        from brix.helper_registry import HelperRegistry
        from brix.validator import PipelineValidator
        from brix.loader import PipelineLoader

        reg_file = tmp_path / "reg.yaml"
        reg = HelperRegistry(registry_path=reg_file)
        reg.register("my_helper", "/some/script.py", description="A helper")

        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        pipeline_yaml = """
name: valid-helper
version: 1.0.0
steps:
  - id: use_helper
    type: python
    helper: my_helper
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        validator = PipelineValidator()
        result = validator.validate(pipeline)

        assert result.is_valid
        assert any("my_helper" in c for c in result.checks)

    def test_missing_helper_reference_is_error(self, tmp_path, monkeypatch):
        """Validator adds an error when helper is not registered."""
        from brix.helper_registry import HelperRegistry
        from brix.validator import PipelineValidator
        from brix.loader import PipelineLoader

        reg_file = tmp_path / "empty_reg.yaml"

        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        pipeline_yaml = """
name: missing-helper
version: 1.0.0
steps:
  - id: use_missing
    type: python
    helper: ghost_helper
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        validator = PipelineValidator()
        result = validator.validate(pipeline)

        assert not result.is_valid
        assert any("ghost_helper" in e for e in result.errors)

    def test_schema_mismatch_is_warning(self, tmp_path, monkeypatch):
        """Validator warns when step params don't match helper input_schema."""
        from brix.helper_registry import HelperRegistry
        from brix.validator import PipelineValidator
        from brix.loader import PipelineLoader

        reg_file = tmp_path / "reg.yaml"
        reg = HelperRegistry(registry_path=reg_file)
        reg.register(
            "strict_helper",
            "/strict.py",
            input_schema={
                "type": "object",
                "properties": {"expected_param": {"type": "string"}},
            },
        )

        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        pipeline_yaml = """
name: schema-mismatch
version: 1.0.0
steps:
  - id: use_strict
    type: python
    helper: strict_helper
    params:
      unexpected_param: "value"
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        validator = PipelineValidator()
        result = validator.validate(pipeline)

        # Valid (no error), but warns about mismatch
        assert result.is_valid
        assert any("unexpected_param" in w for w in result.warnings)

    def test_jinja2_params_not_warned(self, tmp_path, monkeypatch):
        """Jinja2-template params are not flagged for schema mismatch."""
        from brix.helper_registry import HelperRegistry
        from brix.validator import PipelineValidator
        from brix.loader import PipelineLoader

        reg_file = tmp_path / "reg.yaml"
        reg = HelperRegistry(registry_path=reg_file)
        reg.register(
            "templated_helper",
            "/templated.py",
            input_schema={
                "type": "object",
                "properties": {"x": {"type": "string"}},
            },
        )

        original_init = HelperRegistry.__init__

        def patched_init(self, registry_path=None):
            original_init(self, registry_path=reg_file)

        monkeypatch.setattr(HelperRegistry, "__init__", patched_init)

        pipeline_yaml = """
name: jinja2-params
version: 1.0.0
steps:
  - id: use_templated
    type: python
    helper: templated_helper
    params:
      dynamic_param: "{{ input.something }}"
"""
        loader = PipelineLoader()
        pipeline = loader.load_from_string(pipeline_yaml)

        validator = PipelineValidator()
        result = validator.validate(pipeline)

        # No warning for Jinja2 params
        assert not any("dynamic_param" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Step model: helper field
# ---------------------------------------------------------------------------

class TestStepHelperField:
    def test_step_has_helper_field(self):
        from brix.models import Step
        step = Step(id="s", type="python", helper="my_helper")
        assert step.helper == "my_helper"

    def test_step_helper_defaults_to_none(self):
        from brix.models import Step
        step = Step(id="s", type="python", script="/s.py")
        assert step.helper is None

    def test_step_helper_and_script_coexist(self):
        """helper and script can both be set (script takes precedence at runtime)."""
        from brix.models import Step
        step = Step(id="s", type="python", helper="foo", script="/explicit.py")
        assert step.helper == "foo"
        assert step.script == "/explicit.py"
