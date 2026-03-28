"""Tests for the Brick-First Engine — T-BRIX-DB-05c.

Covers:
- New dot-notation brick names resolve to the correct runner
- Old flat names resolve via LEGACY_ALIASES with a DeprecationWarning
- Unknown names return None from _resolve_runner
- All system bricks have a runner field set
- All runners have at least one system brick covering them
- BrickSchema fields: runner, system, namespace
- system bricks cannot be unregistered
"""

import warnings

import pytest

from brix.engine import PipelineEngine, LEGACY_ALIASES
from brix.bricks.builtins import SYSTEM_BRICKS, ALL_BUILTINS
from brix.bricks.registry import BrickRegistry
from brix.bricks.schema import BrickSchema
from brix.models import Step


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_engine() -> PipelineEngine:
    """Return a PipelineEngine instance (no I/O performed)."""
    return PipelineEngine()


# ---------------------------------------------------------------------------
# BrickSchema new fields
# ---------------------------------------------------------------------------

class TestBrickSchemaFields:
    def test_runner_field_default_empty(self):
        brick = BrickSchema(
            name="test.brick",
            type="python",
            description="Test",
            when_to_use="Test",
        )
        assert brick.runner == ""

    def test_system_field_default_false(self):
        brick = BrickSchema(
            name="test.brick",
            type="python",
            description="Test",
            when_to_use="Test",
        )
        assert brick.system is False

    def test_namespace_field_default_empty(self):
        brick = BrickSchema(
            name="test.brick",
            type="python",
            description="Test",
            when_to_use="Test",
        )
        assert brick.namespace == ""

    def test_fields_can_be_set(self):
        brick = BrickSchema(
            name="test.brick",
            type="python",
            description="Test",
            when_to_use="Test",
            runner="python",
            system=True,
            namespace="script",
        )
        assert brick.runner == "python"
        assert brick.system is True
        assert brick.namespace == "script"


# ---------------------------------------------------------------------------
# System bricks
# ---------------------------------------------------------------------------

class TestSystemBricks:
    def test_all_system_bricks_have_runner_field(self):
        for brick in SYSTEM_BRICKS:
            assert brick.runner, f"System brick '{brick.name}' is missing runner field"

    def test_all_system_bricks_have_system_flag(self):
        for brick in SYSTEM_BRICKS:
            assert brick.system is True, f"System brick '{brick.name}' should have system=True"

    def test_all_system_bricks_have_namespace(self):
        for brick in SYSTEM_BRICKS:
            assert brick.namespace, f"System brick '{brick.name}' is missing namespace"

    def test_system_bricks_use_dot_notation_names(self):
        for brick in SYSTEM_BRICKS:
            assert "." in brick.name, (
                f"System brick '{brick.name}' should use dot-notation (e.g. 'db.query')"
            )

    def test_system_bricks_included_in_all_builtins(self):
        builtin_names = {b.name for b in ALL_BUILTINS}
        for brick in SYSTEM_BRICKS:
            assert brick.name in builtin_names, (
                f"System brick '{brick.name}' is missing from ALL_BUILTINS"
            )

    def test_known_system_brick_db_query(self):
        registry = BrickRegistry()
        brick = registry.get("db.query")
        assert brick is not None
        assert brick.runner == "db_query"
        assert brick.namespace == "db"
        assert brick.system is True

    def test_known_system_brick_script_python(self):
        registry = BrickRegistry()
        brick = registry.get("script.python")
        assert brick is not None
        assert brick.runner == "python"
        assert brick.namespace == "script"

    def test_known_system_brick_mcp_call(self):
        registry = BrickRegistry()
        brick = registry.get("mcp.call")
        assert brick is not None
        assert brick.runner == "mcp"

    def test_known_system_brick_action_notify(self):
        registry = BrickRegistry()
        brick = registry.get("action.notify")
        assert brick is not None
        assert brick.runner == "notify"
        assert brick.namespace == "action"


# ---------------------------------------------------------------------------
# BrickRegistry: system bricks cannot be deleted
# ---------------------------------------------------------------------------

class TestRegistrySystemBricks:
    def test_cannot_unregister_system_brick(self):
        registry = BrickRegistry()
        with pytest.raises(ValueError, match="system brick"):
            registry.unregister("db.query")

    def test_can_unregister_non_system_brick(self):
        registry = BrickRegistry()
        # Register a custom non-system brick and then remove it — should not raise
        custom = BrickSchema(
            name="custom.test",
            type="python",
            description="Custom",
            when_to_use="Custom",
            system=False,
        )
        registry.register(custom)
        registry.unregister("custom.test")
        assert registry.get("custom.test") is None


# ---------------------------------------------------------------------------
# Engine runner resolution
# ---------------------------------------------------------------------------

class TestEngineResolveRunner:
    def test_new_name_db_query_resolves(self):
        engine = make_engine()
        runner = engine._resolve_runner("db.query")
        assert runner is not None, "db.query should resolve to db_query runner"

    def test_new_name_script_python_resolves(self):
        engine = make_engine()
        runner = engine._resolve_runner("script.python")
        assert runner is not None

    def test_new_name_http_request_resolves(self):
        engine = make_engine()
        runner = engine._resolve_runner("http.request")
        assert runner is not None

    def test_new_name_mcp_call_resolves(self):
        engine = make_engine()
        runner = engine._resolve_runner("mcp.call")
        assert runner is not None

    def test_new_name_flow_filter_resolves(self):
        engine = make_engine()
        runner = engine._resolve_runner("flow.filter")
        assert runner is not None

    def test_old_name_python_resolves_with_deprecation(self):
        engine = make_engine()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner = engine._resolve_runner("python")
        assert runner is not None, "Old 'python' should still resolve"
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1, "Should emit DeprecationWarning for old step type"
        assert "python" in str(deprecation_warnings[0].message)

    def test_old_name_http_resolves_with_deprecation(self):
        engine = make_engine()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner = engine._resolve_runner("http")
        assert runner is not None
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1

    def test_old_name_mcp_resolves_with_deprecation(self):
        engine = make_engine()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner = engine._resolve_runner("mcp")
        assert runner is not None
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1

    def test_unknown_name_returns_none(self):
        engine = make_engine()
        runner = engine._resolve_runner("completely.unknown.step.type")
        assert runner is None

    def test_another_unknown_name_returns_none(self):
        engine = engine = make_engine()
        runner = engine._resolve_runner("nonexistent")
        assert runner is None

    def test_all_legacy_aliases_resolve(self):
        """Every entry in LEGACY_ALIASES must produce a valid runner."""
        engine = make_engine()
        for old_name, new_name in LEGACY_ALIASES.items():
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                runner = engine._resolve_runner(old_name)
            assert runner is not None, (
                f"LEGACY_ALIAS '{old_name}' → '{new_name}' did not resolve to a runner"
            )


# ---------------------------------------------------------------------------
# LEGACY_ALIASES completeness
# ---------------------------------------------------------------------------

class TestLegacyAliases:
    def test_legacy_aliases_map_to_system_brick_names(self):
        """All values in LEGACY_ALIASES must be names of system bricks."""
        registry = BrickRegistry()
        for old_name, new_name in LEGACY_ALIASES.items():
            brick = registry.get(new_name)
            assert brick is not None, (
                f"LEGACY_ALIAS value '{new_name}' (from '{old_name}') not found in registry"
            )
            assert brick.system is True, (
                f"LEGACY_ALIAS target '{new_name}' should be a system brick"
            )

    def test_all_runners_covered_by_at_least_one_system_brick(self):
        """Every flat runner name referenced in system bricks must also appear in _runners."""
        engine = make_engine()
        runner_names = set(engine._runners.keys())
        for brick in SYSTEM_BRICKS:
            assert brick.runner in runner_names, (
                f"System brick '{brick.name}' references runner '{brick.runner}' "
                f"which is not registered in engine._runners"
            )


# ---------------------------------------------------------------------------
# Step model: new type names are valid
# ---------------------------------------------------------------------------

class TestStepModelNewTypes:
    def test_step_with_dot_notation_type_db_query(self):
        step = Step(id="s1", type="db.query")
        assert step.type == "db.query"

    def test_step_with_dot_notation_type_script_python(self):
        step = Step(id="s2", type="script.python", script="/tmp/test.py")
        assert step.type == "script.python"

    def test_step_with_dot_notation_type_mcp_call(self):
        step = Step(id="s3", type="mcp.call", server="my_server", tool="my_tool")
        assert step.type == "mcp.call"

    def test_step_with_old_type_python_still_valid(self):
        step = Step(id="s4", type="python", script="/tmp/test.py")
        assert step.type == "python"

    def test_step_with_old_type_http_still_valid(self):
        step = Step(id="s5", type="http", url="https://example.com")
        assert step.type == "http"
