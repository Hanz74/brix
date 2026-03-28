"""Tests for BaseRunner abstract interface and discover_runners() (T-BRIX-DB-15)."""

import pytest

from brix.runners.base import BaseRunner, discover_runners


# ---------------------------------------------------------------------------
# Helpers — minimal concrete implementations for testing
# ---------------------------------------------------------------------------


class _MinimalRunner(BaseRunner):
    """Fully compliant concrete runner used in positive tests."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "required_field": {"type": "string"},
            },
            "required": ["required_field"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step, context) -> dict:
        self.report_progress(100.0, "done")
        return {"success": True, "data": {}, "duration": 0.0}


# ---------------------------------------------------------------------------
# Abstract-class enforcement
# ---------------------------------------------------------------------------


def test_base_runner_cannot_be_instantiated():
    """BaseRunner is abstract and raises TypeError on direct instantiation."""
    with pytest.raises(TypeError):
        BaseRunner()  # type: ignore[abstract]


def test_subclass_missing_config_schema_raises():
    """A concrete subclass that omits config_schema() cannot be instantiated."""
    with pytest.raises(TypeError):

        class _BadRunner(BaseRunner):
            def input_type(self) -> str:
                return "none"

            def output_type(self) -> str:
                return "none"

            async def execute(self, step, context) -> dict:
                return {"success": True, "data": None, "duration": 0.0}

        _BadRunner()


def test_subclass_missing_input_type_raises():
    """A concrete subclass that omits input_type() cannot be instantiated."""
    with pytest.raises(TypeError):

        class _BadRunner(BaseRunner):
            def config_schema(self) -> dict:
                return {}

            def output_type(self) -> str:
                return "none"

            async def execute(self, step, context) -> dict:
                return {"success": True, "data": None, "duration": 0.0}

        _BadRunner()


def test_subclass_missing_output_type_raises():
    """A concrete subclass that omits output_type() cannot be instantiated."""
    with pytest.raises(TypeError):

        class _BadRunner(BaseRunner):
            def config_schema(self) -> dict:
                return {}

            def input_type(self) -> str:
                return "none"

            async def execute(self, step, context) -> dict:
                return {"success": True, "data": None, "duration": 0.0}

        _BadRunner()


def test_subclass_with_all_methods_ok():
    """A fully implemented subclass can be instantiated without error."""
    runner = _MinimalRunner()
    assert runner is not None


# ---------------------------------------------------------------------------
# config_schema / validate_config
# ---------------------------------------------------------------------------


def test_validate_config_required_field_present():
    """validate_config returns empty list when all required fields are present."""
    runner = _MinimalRunner()
    errors = runner.validate_config({"required_field": "hello"})
    assert errors == []


def test_validate_config_required_field_missing():
    """validate_config reports missing required field as an error string."""
    runner = _MinimalRunner()
    errors = runner.validate_config({})
    assert len(errors) == 1
    assert "required_field" in errors[0]


def test_validate_config_extra_fields_allowed():
    """validate_config does not complain about extra (unknown) fields."""
    runner = _MinimalRunner()
    errors = runner.validate_config({"required_field": "x", "extra": 42})
    assert errors == []


def test_config_schema_returns_dict():
    """config_schema() must return a plain dict."""
    runner = _MinimalRunner()
    schema = runner.config_schema()
    assert isinstance(schema, dict)
    assert schema.get("type") == "object"


# ---------------------------------------------------------------------------
# report_progress
# ---------------------------------------------------------------------------


def test_report_progress_stores_state():
    """report_progress() stores progress in _progress attribute."""
    runner = _MinimalRunner()
    assert runner._progress is None
    runner.report_progress(50.0, "halfway", done=5, total=10)
    assert runner._progress == {"pct": 50.0, "msg": "halfway", "done": 5, "total": 10}


def test_report_progress_default_args():
    """report_progress() works with only pct argument."""
    runner = _MinimalRunner()
    runner.report_progress(100.0)
    assert runner._progress["pct"] == 100.0
    assert runner._progress["msg"] == ""
    assert runner._progress["done"] == 0
    assert runner._progress["total"] == 0


async def test_execute_calls_report_progress():
    """execute() calls report_progress() so _progress is set after the call."""
    runner = _MinimalRunner()
    assert runner._progress is None
    await runner.execute(object(), context=None)
    assert runner._progress is not None
    assert runner._progress["pct"] == 100.0


# ---------------------------------------------------------------------------
# input_type / output_type
# ---------------------------------------------------------------------------


def test_input_output_types_return_strings():
    """input_type() and output_type() return non-empty strings."""
    runner = _MinimalRunner()
    assert isinstance(runner.input_type(), str)
    assert isinstance(runner.output_type(), str)
    assert runner.input_type() != ""
    assert runner.output_type() != ""


# ---------------------------------------------------------------------------
# discover_runners
# ---------------------------------------------------------------------------


def test_discover_runners_returns_dict():
    """discover_runners() returns a non-empty dict."""
    registry = discover_runners()
    assert isinstance(registry, dict)
    assert len(registry) > 0


def test_discover_runners_finds_core_runners():
    """discover_runners() finds all well-known core runners by step type."""
    registry = discover_runners()
    expected = {
        "cli", "python", "http", "mcp",
        "filter", "transform", "set", "choose",
        "parallel", "repeat", "notify", "approval",
        "validate", "pipeline", "pipeline_group", "specialist",
    }
    missing = expected - set(registry.keys())
    assert not missing, f"Missing runners in registry: {missing}"


def test_discover_runners_values_are_base_runner_subclasses():
    """Every class in the registry is a subclass of BaseRunner."""
    registry = discover_runners()
    for step_type, runner_cls in registry.items():
        assert issubclass(runner_cls, BaseRunner), (
            f"Runner for '{step_type}' ({runner_cls.__name__}) is not a BaseRunner subclass"
        )


def test_discover_runners_classes_are_concrete():
    """Every class in the registry can be instantiated (is not abstract)."""
    import inspect
    registry = discover_runners()
    for step_type, runner_cls in registry.items():
        assert not inspect.isabstract(runner_cls), (
            f"Runner for '{step_type}' ({runner_cls.__name__}) is still abstract"
        )


def test_all_discovered_runners_implement_interface():
    """All discovered runners implement config_schema, input_type, output_type."""
    registry = discover_runners()
    for step_type, runner_cls in registry.items():
        # Instantiate with minimal args (engine-requiring runners accept engine=None)
        try:
            runner = runner_cls()
        except TypeError:
            runner = runner_cls(engine=None)

        schema = runner.config_schema()
        assert isinstance(schema, dict), (
            f"{runner_cls.__name__}.config_schema() did not return a dict"
        )
        assert isinstance(runner.input_type(), str), (
            f"{runner_cls.__name__}.input_type() did not return a str"
        )
        assert isinstance(runner.output_type(), str), (
            f"{runner_cls.__name__}.output_type() did not return a str"
        )


# ---------------------------------------------------------------------------
# Progress warning simulation (engine-level behaviour is tested elsewhere;
# here we verify that _progress starts as None and is set after the call)
# ---------------------------------------------------------------------------


def test_no_progress_before_execute():
    """A fresh runner has _progress == None before execute() is called."""
    runner = _MinimalRunner()
    assert runner._progress is None


def test_progress_set_after_execute():
    """After execute(), _progress is not None for a compliant runner."""
    import asyncio
    runner = _MinimalRunner()
    asyncio.get_event_loop().run_until_complete(runner.execute(None, None))
    assert runner._progress is not None
