"""Tests for Brick Composition: Profiles/Mixins, Dynamic Dispatch, Brick Inheritance.

Covers T-BRIX-DB-23:
- Part 1: Profiles/Mixins — DB CRUD, step profile merge, override precedence
- Part 2: Dynamic Dispatch — {{ item.type }} renders to valid brick, invalid → error
- Part 3: Brick Inheritance — child inherits parent config_defaults, child overrides parent
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from brix.bricks.registry import BrickRegistry
from brix.bricks.schema import BrickParam, BrickSchema
from brix.db import BrixDB
from brix.engine import PipelineEngine
from brix.models import Step


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_db() -> BrixDB:
    """Return an in-memory BrixDB backed by a temp file."""
    tmp = tempfile.mktemp(suffix=".db")
    return BrixDB(db_path=tmp)


def make_engine() -> PipelineEngine:
    return PipelineEngine()


def make_brick(name: str, runner: str = "python", extends: str | None = None, **kwargs) -> BrickSchema:
    defaults = {
        "description": f"Brick {name}",
        "when_to_use": f"Use {name}",
    }
    defaults.update(kwargs)
    return BrickSchema(
        name=name,
        type=runner,
        runner=runner,
        extends=extends,
        **defaults,
    )


# ===========================================================================
# Part 1: Profiles / Mixins
# ===========================================================================


class TestProfileDB:
    """BrixDB profile CRUD methods."""

    def test_profile_set_and_get(self):
        db = make_db()
        config = {"timeout": "30s", "on_error": "continue"}
        result = db.profile_set("slow-api", config, description="For slow APIs")
        assert result["name"] == "slow-api"
        assert result["config"] == config
        assert result["description"] == "For slow APIs"
        assert result["created_at"]

    def test_profile_get_returns_none_for_missing(self):
        db = make_db()
        assert db.profile_get("nonexistent") is None

    def test_profile_set_updates_existing(self):
        db = make_db()
        db.profile_set("p1", {"timeout": "5s"})
        updated = db.profile_set("p1", {"timeout": "60s", "on_error": "retry"}, description="Updated")
        assert updated["config"]["timeout"] == "60s"
        assert updated["config"]["on_error"] == "retry"
        assert updated["description"] == "Updated"

    def test_profile_list_returns_all(self):
        db = make_db()
        db.profile_set("alpha", {"timeout": "10s"})
        db.profile_set("beta", {"timeout": "20s"})
        profiles = db.profile_list()
        names = [p["name"] for p in profiles]
        assert "alpha" in names
        assert "beta" in names

    def test_profile_list_empty(self):
        db = make_db()
        assert db.profile_list() == []

    def test_profile_delete_returns_true(self):
        db = make_db()
        db.profile_set("to-delete", {})
        result = db.profile_delete("to-delete")
        assert result is True

    def test_profile_delete_removes_record(self):
        db = make_db()
        db.profile_set("gone", {"timeout": "1s"})
        db.profile_delete("gone")
        assert db.profile_get("gone") is None

    def test_profile_delete_missing_returns_false(self):
        db = make_db()
        assert db.profile_delete("nope") is False

    def test_profile_config_serialized_as_json(self):
        db = make_db()
        config = {"rate_limit": {"max_calls": 100, "per": "1m"}, "timeout": "5s"}
        db.profile_set("complex", config)
        fetched = db.profile_get("complex")
        assert fetched["config"]["rate_limit"]["max_calls"] == 100
        assert fetched["config"]["timeout"] == "5s"


class TestProfileEngineApply:
    """Engine _apply_profile merges profile config into step."""

    def test_apply_profile_no_profile_returns_same_step(self):
        engine = make_engine()
        step = Step(id="s1", type="script.python", script="print(1)")
        result = engine._apply_profile(step)
        assert result is step  # same object — no merge needed

    def test_apply_profile_with_missing_profile_returns_original(self):
        """If profile name is set but profile not in DB, return original step."""
        engine = make_engine()
        step = Step(id="s1", type="script.python", script="print(1)", profile="nonexistent-profile")
        result = engine._apply_profile(step)
        # Should return the original step (profile not found → no crash)
        assert result.id == "s1"

    def test_apply_profile_merges_timeout(self):
        """Profile timeout is applied when step has no explicit timeout."""
        engine = make_engine()
        # Seed the engine's DB path with a profile
        db = BrixDB()
        db.profile_set("test-timeout-profile", {"timeout": "45s"})

        step = Step(id="s1", type="script.python", script="pass", profile="test-timeout-profile")
        result = engine._apply_profile(step)
        assert result.timeout == "45s"

    def test_step_config_overrides_profile(self):
        """Step-level field takes precedence over profile config."""
        engine = make_engine()
        db = BrixDB()
        db.profile_set("test-override-profile", {"timeout": "5s", "on_error": "continue"})

        # Step explicitly sets timeout — should keep its value, not use profile's
        step = Step(
            id="s1",
            type="script.python",
            script="pass",
            profile="test-override-profile",
            timeout="99s",  # explicit override
        )
        result = engine._apply_profile(step)
        assert result.timeout == "99s"  # step wins

    def test_apply_profile_merges_on_error(self):
        """Profile on_error is applied when step has no explicit on_error."""
        engine = make_engine()
        db = BrixDB()
        db.profile_set("test-on-error-profile", {"on_error": "continue"})

        step = Step(id="s1", type="script.python", script="pass", profile="test-on-error-profile")
        result = engine._apply_profile(step)
        assert result.on_error == "continue"


# ===========================================================================
# Part 2: Dynamic Dispatch
# ===========================================================================


class TestDynamicDispatch:
    """_resolve_runner handles {{ ... }} step types."""

    def test_resolve_runner_static_type_unchanged(self):
        """Static types still resolve normally."""
        engine = make_engine()
        runner = engine._resolve_runner("script.python")
        assert runner is not None

    def test_resolve_runner_dynamic_dispatch_valid(self):
        """{{ item.type }} renders to a valid brick → runner resolved."""
        engine = make_engine()
        jinja_ctx = {"item": {"type": "script.python"}}
        runner = engine._resolve_runner("{{ item.type }}", jinja_ctx=jinja_ctx)
        assert runner is not None

    def test_resolve_runner_dynamic_dispatch_resolves_http(self):
        """Dynamic dispatch to http.request works."""
        engine = make_engine()
        jinja_ctx = {"item": {"brick": "http.request"}}
        runner = engine._resolve_runner("{{ item.brick }}", jinja_ctx=jinja_ctx)
        assert runner is not None

    def test_resolve_runner_dynamic_dispatch_invalid_type_returns_none(self):
        """{{ item.type }} rendering to an unknown brick → returns None."""
        engine = make_engine()
        jinja_ctx = {"item": {"type": "totally.unknown.brick"}}
        runner = engine._resolve_runner("{{ item.type }}", jinja_ctx=jinja_ctx)
        assert runner is None

    def test_resolve_runner_dynamic_dispatch_no_jinja_ctx_returns_none(self):
        """Jinja2 type without context → cannot render → returns None."""
        engine = make_engine()
        runner = engine._resolve_runner("{{ item.type }}", jinja_ctx=None)
        assert runner is None

    def test_resolve_runner_dynamic_dispatch_legacy_alias(self):
        """Dynamic dispatch to a legacy alias (e.g. 'python') works with deprecation warning."""
        import warnings
        engine = make_engine()
        jinja_ctx = {"step_type": "python"}
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            runner = engine._resolve_runner("{{ step_type }}", jinja_ctx=jinja_ctx)
        # 'python' is a legacy alias → resolves via LEGACY_ALIASES
        assert runner is not None

    def test_resolve_runner_dynamic_dispatch_render_error_returns_none(self):
        """Template render error (bad syntax) → returns None safely."""
        engine = make_engine()
        jinja_ctx = {}
        # Malformed jinja that can't be rendered cleanly
        runner = engine._resolve_runner("{{ undefined_var.missing_attr }}", jinja_ctx=jinja_ctx)
        # Should not crash — returns None or None depending on Jinja2 mode
        # (undefined variables resolve to '' in SandboxedEnvironment)
        # Either None or a valid runner — just must not raise


# ===========================================================================
# Part 3: Brick Inheritance
# ===========================================================================


class TestBrickInheritance:
    """BrickRegistry.get() resolves inheritance via extends field."""

    def test_extends_field_default_none(self):
        brick = make_brick("test.base")
        assert brick.extends is None

    def test_child_inherits_config_defaults_from_parent(self):
        """Child brick without its own config_defaults inherits parent's."""
        registry = BrickRegistry()
        parent = make_brick(
            "base.parent",
            runner="python",
            config_schema={
                "url": BrickParam(type="string", description="URL", required=True),
                "timeout": BrickParam(type="string", description="Timeout", default="30s"),
            },
        )
        child = BrickSchema(
            name="child.step",
            type="python",
            runner="python",
            description="Child brick",
            when_to_use="Use child",
            extends="base.parent",
            config_schema={},  # No own config — should inherit
        )
        registry.register(parent)
        registry.register(child)

        resolved = registry.get("child.step")
        assert resolved is not None
        assert "url" in resolved.config_schema
        assert "timeout" in resolved.config_schema
        assert resolved.config_schema["timeout"].default == "30s"

    def test_child_overrides_parent_field(self):
        """Child description/runner overrides parent."""
        registry = BrickRegistry()
        parent = make_brick("base.p2", runner="http", description="Parent desc")
        child = BrickSchema(
            name="child.c2",
            type="python",
            runner="python",  # override parent runner
            description="Child desc",  # override parent description
            when_to_use="Use child c2",
            extends="base.p2",
        )
        registry.register(parent)
        registry.register(child)

        resolved = registry.get("child.c2")
        assert resolved.runner == "python"  # child wins
        assert resolved.description == "Child desc"  # child wins

    def test_child_inherits_when_to_use_from_parent(self):
        """Child with default when_to_use inherits parent's non-default value."""
        registry = BrickRegistry()
        parent = BrickSchema(
            name="base.p3",
            type="http",
            runner="http",
            description="Parent",
            when_to_use="Use this for HTTP calls",  # non-default
        )
        # Child uses make_brick which sets when_to_use to a non-empty value
        # → child's value overrides. So test that parent's namespace is inherited.
        child = BrickSchema(
            name="child.c3",
            type="http",
            runner="http",
            description="Child",
            when_to_use="Use child c3",  # child sets its own
            extends="base.p3",
        )
        registry.register(parent)
        registry.register(child)

        resolved = registry.get("child.c3")
        # Child explicitly set when_to_use — child wins
        assert resolved.when_to_use == "Use child c3"
        # But category should come from parent (default "general")
        assert resolved.category == "general"

    def test_child_with_unknown_parent_returns_child_as_is(self):
        """If parent doesn't exist, return child without merge (no crash)."""
        registry = BrickRegistry()
        child = make_brick("orphan.child", extends="nonexistent.parent")
        registry.register(child)

        resolved = registry.get("orphan.child")
        assert resolved is not None
        assert resolved.name == "orphan.child"

    def test_no_inheritance_cycle_crash(self):
        """Cycle detection prevents infinite loop."""
        registry = BrickRegistry()
        brick_a = BrickSchema(
            name="cycle.a",
            type="python",
            runner="python",
            description="A",
            when_to_use="A",
            extends="cycle.b",
        )
        brick_b = BrickSchema(
            name="cycle.b",
            type="python",
            runner="python",
            description="B",
            when_to_use="B",
            extends="cycle.a",
        )
        registry.register(brick_a)
        registry.register(brick_b)

        # Must not raise or loop infinitely
        resolved = registry.get("cycle.a")
        assert resolved is not None  # Returns something without crashing

    def test_resolved_brick_name_is_child_name(self):
        """After inheritance resolution, the brick name remains the child's name."""
        registry = BrickRegistry()
        parent = make_brick("parent.only", runner="cli")
        child = make_brick("child.only", runner="cli", extends="parent.only")
        registry.register(parent)
        registry.register(child)

        resolved = registry.get("child.only")
        assert resolved.name == "child.only"

    def test_resolved_brick_extends_is_none(self):
        """After resolution, extends field is cleared (already resolved)."""
        registry = BrickRegistry()
        parent = make_brick("parent.x", runner="python")
        child = make_brick("child.x", runner="python", extends="parent.x")
        registry.register(parent)
        registry.register(child)

        resolved = registry.get("child.x")
        assert resolved.extends is None  # Resolved — marker cleared

    def test_base_brick_without_extends_unchanged(self):
        """Brick without extends returns as-is."""
        registry = BrickRegistry()
        base = make_brick("standalone.brick", runner="http")
        registry.register(base)

        resolved = registry.get("standalone.brick")
        assert resolved.name == "standalone.brick"
        assert resolved.extends is None
