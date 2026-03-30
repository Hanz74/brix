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


# ---------------------------------------------------------------------------
# validate_config per-runner overrides (T-BRIX-STD-03)
# ---------------------------------------------------------------------------


class TestValidateConfigPerRunner:
    """Test validate_config() overrides on concrete runners."""

    def test_filter_where_must_be_string(self):
        from brix.runners.filter import FilterRunner
        runner = FilterRunner()
        errors = runner.validate_config({"where": 123})
        assert any("'where' must be a string" in e for e in errors)

    def test_filter_valid_config(self):
        from brix.runners.filter import FilterRunner
        runner = FilterRunner()
        errors = runner.validate_config({"where": "{{ item.ok }}"})
        assert errors == []

    def test_transform_expression_must_be_string(self):
        from brix.runners.transform import TransformRunner
        runner = TransformRunner()
        errors = runner.validate_config({"expression": ["not", "a", "string"]})
        assert any("'expression' must be a string" in e for e in errors)

    def test_aggregate_operations_must_be_dict(self):
        from brix.runners.aggregate import AggregateRunner
        runner = AggregateRunner()
        errors = runner.validate_config({"group_by": "{{ item.x }}", "operations": "wrong"})
        assert any("'operations' must be a dict" in e for e in errors)

    def test_aggregate_group_by_must_be_string(self):
        from brix.runners.aggregate import AggregateRunner
        runner = AggregateRunner()
        errors = runner.validate_config({"group_by": 42, "operations": {"count": {"op": "count"}}})
        assert any("'group_by' must be a string" in e for e in errors)

    def test_aggregate_valid_config(self):
        from brix.runners.aggregate import AggregateRunner
        runner = AggregateRunner()
        errors = runner.validate_config({"group_by": "{{ item.x }}", "operations": {"count": {"op": "count"}}})
        assert errors == []

    def test_switch_cases_must_be_dict(self):
        from brix.runners.switch import SwitchRunner
        runner = SwitchRunner()
        errors = runner.validate_config({"field": "{{ x }}", "cases": ["a", "b"]})
        assert any("'cases' must be a dict" in e for e in errors)

    def test_switch_field_must_be_string(self):
        from brix.runners.switch import SwitchRunner
        runner = SwitchRunner()
        errors = runner.validate_config({"field": 99, "cases": {"a": "step_a"}})
        assert any("'field' must be a string" in e for e in errors)

    def test_db_query_connection_must_be_string(self):
        from brix.runners.db_query import DbQueryRunner
        runner = DbQueryRunner()
        errors = runner.validate_config({"connection": 123, "query": "SELECT 1"})
        assert any("'connection' must be a string" in e for e in errors)

    def test_db_query_query_must_be_string(self):
        from brix.runners.db_query import DbQueryRunner
        runner = DbQueryRunner()
        errors = runner.validate_config({"connection": "mydb", "query": 456})
        assert any("'query' must be a string" in e for e in errors)

    def test_db_upsert_table_must_be_string(self):
        from brix.runners.db_upsert import DbUpsertRunner
        runner = DbUpsertRunner()
        errors = runner.validate_config({"connection": "mydb", "table": ["wrong"]})
        assert any("'table' must be a string" in e for e in errors)

    def test_llm_batch_system_prompt_must_be_string(self):
        from brix.runners.llm_batch import LlmBatchRunner
        runner = LlmBatchRunner()
        errors = runner.validate_config({"system_prompt": 42, "user_template": "x"})
        assert any("'system_prompt' must be a string" in e for e in errors)

    def test_llm_batch_output_schema_must_be_dict(self):
        from brix.runners.llm_batch import LlmBatchRunner
        runner = LlmBatchRunner()
        errors = runner.validate_config({"system_prompt": "x", "user_template": "y", "output_schema": "not_dict"})
        assert any("'output_schema' must be a dict" in e for e in errors)

    def test_source_connector_must_be_string(self):
        from brix.runners.source import SourceRunner
        runner = SourceRunner()
        errors = runner.validate_config({"connector": ["wrong"]})
        assert any("'connector' must be a string" in e for e in errors)

    def test_approval_on_timeout_must_be_valid(self):
        from brix.runners.approval import ApprovalRunner
        runner = ApprovalRunner()
        errors = runner.validate_config({"message": "ok", "on_timeout": "crash"})
        assert any("'on_timeout' must be 'stop' or 'continue'" in e for e in errors)

    def test_cli_needs_args_or_command(self):
        from brix.runners.cli import CliRunner
        runner = CliRunner()
        errors = runner.validate_config({})
        assert any("'args' or 'command' must be provided" in e for e in errors)

    def test_cli_args_must_be_list(self):
        from brix.runners.cli import CliRunner
        runner = CliRunner()
        errors = runner.validate_config({"args": "not-a-list"})
        assert any("'args' must be a list" in e for e in errors)

    def test_cli_valid_config(self):
        from brix.runners.cli import CliRunner
        runner = CliRunner()
        errors = runner.validate_config({"args": ["echo", "hello"]})
        assert errors == []

    def test_specialist_extract_must_be_list(self):
        from brix.runners.specialist import SpecialistRunner
        runner = SpecialistRunner()
        errors = runner.validate_config({"extract": "wrong"})
        assert any("'extract' must be a list" in e for e in errors)

    def test_validate_rules_must_be_list(self):
        from brix.runners.validate import ValidateRunner
        runner = ValidateRunner()
        errors = runner.validate_config({"rules": "wrong"})
        assert any("'rules' must be a list" in e for e in errors)

    def test_all_runners_validate_config_callable(self):
        """Every discovered runner's validate_config returns a list."""
        registry = discover_runners()
        for step_type, runner_cls in registry.items():
            try:
                runner = runner_cls()
            except TypeError:
                runner = runner_cls(engine=None)
            result = runner.validate_config({})
            assert isinstance(result, list), (
                f"{runner_cls.__name__}.validate_config({{}}) did not return a list"
            )
