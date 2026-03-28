"""Tests for T-BRIX-V6-13 + V6-14 + V6-15 + V6-16.

Covers:
- V6-13: Schema-Contracts (Step input_schema/output_schema, inter-step checks, context warning)
- V6-14: Pipeline-Composition output_slots (model + runner evaluation)
- V6-15: Data quality gates — validate runner
- V6-16: Pipeline linting rules (default rules + custom rules.yaml)
"""

import asyncio
import warnings
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from brix.models import Pipeline, Step
from brix.validator import PipelineValidator, ValidationResult
from brix.context import PipelineContext
from brix.runners.validate import ValidateRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_pipeline(**kwargs) -> Pipeline:
    defaults = {
        "name": "test-pipeline",
        "steps": [{"id": "step1", "type": "python", "script": "run.py"}],
    }
    defaults.update(kwargs)
    return Pipeline.model_validate(defaults)


def make_pipeline_with_steps(steps_raw: list[dict]) -> Pipeline:
    return Pipeline.model_validate({"name": "test-pipeline", "steps": steps_raw})


def run_async(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ===========================================================================
# V6-13: Schema-Contracts — models.py
# ===========================================================================


class TestSchemaContractModel:
    def test_step_has_input_schema_default(self):
        step = Step.model_validate({"id": "s1", "type": "python", "script": "run.py"})
        assert step.input_schema == {}

    def test_step_has_output_schema_default(self):
        step = Step.model_validate({"id": "s1", "type": "python", "script": "run.py"})
        assert step.output_schema == {}

    def test_step_accepts_input_schema(self):
        step = Step.model_validate({
            "id": "s1",
            "type": "python",
            "script": "run.py",
            "input_schema": {"name": "string", "count": "integer"},
        })
        assert step.input_schema == {"name": "string", "count": "integer"}

    def test_step_accepts_output_schema(self):
        step = Step.model_validate({
            "id": "s1",
            "type": "python",
            "script": "run.py",
            "output_schema": {"result": "list", "status": "string"},
        })
        assert step.output_schema == {"result": "list", "status": "string"}


# ===========================================================================
# V6-13: Schema-Contracts — validator.py
# ===========================================================================


class TestSchemaContractValidator:
    def test_compatible_schemas_pass(self):
        """Step B's input_schema fields all exist in step A's output_schema."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "extract",
                "type": "python",
                "script": "extract.py",
                "output_schema": {"name": "string", "amount": "float", "date": "string"},
            },
            {
                "id": "process",
                "type": "python",
                "script": "process.py",
                "input_schema": {"name": "string", "amount": "float"},
                "params": {"data": "{{ extract.output }}"},
            },
        ])
        v = PipelineValidator()
        result = v.validate(pipeline)
        assert result.is_valid
        checks_text = " ".join(result.checks)
        assert "compatible" in checks_text

    def test_incompatible_schemas_warn(self):
        """Step B's input_schema has fields missing from step A's output_schema."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "fetch",
                "type": "python",
                "script": "fetch.py",
                "output_schema": {"name": "string"},
            },
            {
                "id": "transform",
                "type": "python",
                "script": "transform.py",
                "input_schema": {"name": "string", "missing_field": "integer"},
                "params": {"data": "{{ fetch.output }}"},
            },
        ])
        v = PipelineValidator()
        result = v.validate(pipeline)
        warnings_text = " ".join(result.warnings)
        assert "missing_field" in warnings_text
        assert "T-BRIX-V6-13" in warnings_text

    def test_no_schema_no_check(self):
        """Steps without schemas don't trigger schema-contract checks."""
        pipeline = make_pipeline_with_steps([
            {"id": "a", "type": "python", "script": "a.py"},
            {
                "id": "b",
                "type": "python",
                "script": "b.py",
                "params": {"x": "{{ a.output }}"},
            },
        ])
        v = PipelineValidator()
        result = v.validate(pipeline)
        assert not any("T-BRIX-V6-13" in w for w in result.warnings)

    def test_only_output_schema_no_downstream_check(self):
        """Upstream step has output_schema, but downstream has no input_schema — no warning."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "producer",
                "type": "python",
                "script": "prod.py",
                "output_schema": {"value": "string"},
            },
            {"id": "consumer", "type": "python", "script": "cons.py"},
        ])
        v = PipelineValidator()
        result = v.validate(pipeline)
        assert not any("T-BRIX-V6-13" in w for w in result.warnings)


# ===========================================================================
# V6-13: Schema-Contracts — context.py output validation
# ===========================================================================


class TestContextOutputSchemaValidation:
    def _make_ctx(self):
        return PipelineContext(pipeline_input={})

    def test_set_output_with_schema_no_warning_when_fields_present(self):
        ctx = self._make_ctx()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ctx.set_output("step1", {"name": "Alice", "amount": 42}, output_schema={"name": "string", "amount": "float"})
        schema_warns = [w for w in caught if "missing schema fields" in str(w.message)]
        assert len(schema_warns) == 0

    def test_set_output_with_schema_warns_on_missing_field(self):
        ctx = self._make_ctx()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ctx.set_output("step1", {"name": "Alice"}, output_schema={"name": "string", "amount": "float"})
        schema_warns = [w for w in caught if "missing schema fields" in str(w.message)]
        assert len(schema_warns) == 1
        assert "amount" in str(schema_warns[0].message)
        assert "step1" in str(schema_warns[0].message)

    def test_set_output_with_schema_warns_non_dict_output(self):
        ctx = self._make_ctx()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ctx.set_output("step1", [1, 2, 3], output_schema={"result": "list"})
        type_warns = [w for w in caught if "not a dict" in str(w.message)]
        assert len(type_warns) == 1

    def test_set_output_without_schema_no_warning(self):
        ctx = self._make_ctx()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ctx.set_output("step1", {"anything": "goes"})
        schema_warns = [w for w in caught if "schema" in str(w.message).lower()]
        assert len(schema_warns) == 0

    def test_validate_output_schema_empty_schema_noop(self):
        ctx = self._make_ctx()
        # Should not raise
        ctx.validate_output_schema("step1", {"a": 1}, {})


# ===========================================================================
# V6-14: Pipeline-Composition output_slots — model
# ===========================================================================


class TestOutputSlotsModel:
    def test_pipeline_output_slots_default_empty(self):
        p = make_pipeline()
        assert p.output_slots == {}

    def test_pipeline_accepts_output_slots(self):
        p = Pipeline.model_validate({
            "name": "test",
            "output_slots": {
                "total": "{{ result.count }}",
                "status": "ok",
            },
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        })
        assert "total" in p.output_slots
        assert p.output_slots["status"] == "ok"


# ===========================================================================
# V6-14: Pipeline-Composition output_slots — runner
# ===========================================================================


class TestOutputSlotsRunner:
    def test_evaluate_output_slots_simple(self):
        """output_slots are evaluated against sub-pipeline result."""
        from brix.runners.pipeline import PipelineRunner

        runner = PipelineRunner()

        sub_pipeline = MagicMock()
        sub_pipeline.output_slots = {
            "total": "{{ total }}",
            "label": "processed",
        }

        sub_result = MagicMock()
        sub_result.result = {"total": 42, "items": []}

        slots = runner._evaluate_output_slots(sub_pipeline, sub_result)
        assert slots["total"] == "42"
        assert slots["label"] == "processed"

    def test_evaluate_output_slots_empty(self):
        from brix.runners.pipeline import PipelineRunner

        runner = PipelineRunner()
        sub_pipeline = MagicMock()
        sub_pipeline.output_slots = {}
        sub_result = MagicMock()
        sub_result.result = {}

        slots = runner._evaluate_output_slots(sub_pipeline, sub_result)
        assert slots == {}

    def test_evaluate_output_slots_error_returns_none(self):
        """A broken Jinja2 expression returns None for that slot (graceful degradation)."""
        from brix.runners.pipeline import PipelineRunner

        runner = PipelineRunner()
        sub_pipeline = MagicMock()
        sub_pipeline.output_slots = {"broken": "{{ undefined_var.deeply.nested }}"}
        sub_result = MagicMock()
        sub_result.result = {}

        slots = runner._evaluate_output_slots(sub_pipeline, sub_result)
        # Should not raise, broken slot is None
        assert "broken" in slots

    def test_evaluate_output_slots_no_attribute(self):
        """Sub-pipeline without output_slots attribute returns empty dict."""
        from brix.runners.pipeline import PipelineRunner

        runner = PipelineRunner()
        sub_pipeline = MagicMock(spec=[])  # no attributes
        sub_result = MagicMock()
        sub_result.result = {}

        slots = runner._evaluate_output_slots(sub_pipeline, sub_result)
        assert slots == {}


# ===========================================================================
# V6-15: Data Quality Gates — validate runner
# ===========================================================================


class TestValidateRunner:
    def _make_step(self, rules=None):
        step = MagicMock()
        step.id = "quality_check"
        step.rules = rules or []
        return step

    def _make_context(self, outputs=None):
        ctx = MagicMock()
        ctx.to_jinja_context.return_value = outputs or {}
        return ctx

    def test_no_rules_passes(self):
        runner = ValidateRunner()
        step = self._make_step(rules=[])
        ctx = self._make_context()
        result = run_async(runner.execute(step, ctx))
        assert result["success"] is True
        assert result["data"]["violations"] == []

    def test_rule_passes_when_ratio_met(self):
        """100% of items have non-empty 'name' — min_ratio=0.8 is met."""
        runner = ValidateRunner()
        step = self._make_step(rules=[{
            "field": "{{ item.name }}",
            "min_ratio": 0.8,
            "of": "{{ items }}",
            "on_fail": "stop",
        }])
        ctx = self._make_context({"items": [
            {"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}
        ]})
        result = run_async(runner.execute(step, ctx))
        assert result["success"] is True
        assert result["data"]["violations"] == []

    def test_rule_fails_on_stop(self):
        """Only 1/3 items have non-empty name — min_ratio=0.8 not met → stop."""
        runner = ValidateRunner()
        step = self._make_step(rules=[{
            "field": "{{ item.name }}",
            "min_ratio": 0.8,
            "of": "{{ items }}",
            "on_fail": "stop",
        }])
        ctx = self._make_context({"items": [
            {"name": "Alice"}, {"name": ""}, {"name": ""}
        ]})
        result = run_async(runner.execute(step, ctx))
        assert result["success"] is False
        assert "quality gate failed" in result["error"]
        assert len(result["data"]["violations"]) == 1

    def test_rule_warns_on_warn(self):
        """Only 1/3 items have non-empty name — on_fail=warn → success but warning recorded."""
        runner = ValidateRunner()
        step = self._make_step(rules=[{
            "field": "{{ item.name }}",
            "min_ratio": 0.8,
            "of": "{{ items }}",
            "on_fail": "warn",
        }])
        ctx = self._make_context({"items": [
            {"name": "Alice"}, {"name": ""}, {"name": ""}
        ]})
        result = run_async(runner.execute(step, ctx))
        assert result["success"] is True
        assert len(result["data"]["warnings"]) == 1

    def test_rule_passes_trivially_on_empty_list(self):
        """Empty 'of' list → rule passes trivially."""
        runner = ValidateRunner()
        step = self._make_step(rules=[{
            "field": "{{ item.name }}",
            "min_ratio": 1.0,
            "of": "{{ items }}",
            "on_fail": "stop",
        }])
        ctx = self._make_context({"items": []})
        result = run_async(runner.execute(step, ctx))
        assert result["success"] is True

    def test_multiple_rules_all_pass(self):
        """Multiple rules all passing → success."""
        runner = ValidateRunner()
        step = self._make_step(rules=[
            {"field": "{{ item.name }}", "min_ratio": 1.0, "of": "{{ items }}", "on_fail": "stop"},
            {"field": "{{ item.value }}", "min_ratio": 1.0, "of": "{{ items }}", "on_fail": "stop"},
        ])
        ctx = self._make_context({"items": [
            {"name": "Alice", "value": 100},
            {"name": "Bob", "value": 200},
        ]})
        result = run_async(runner.execute(step, ctx))
        assert result["success"] is True

    def test_multiple_rules_one_stop_fails(self):
        """Two rules: first passes, second fails with on_fail=stop → stop."""
        runner = ValidateRunner()
        step = self._make_step(rules=[
            {"field": "{{ item.name }}", "min_ratio": 0.5, "of": "{{ items }}", "on_fail": "warn"},
            {"field": "{{ item.value }}", "min_ratio": 1.0, "of": "{{ items }}", "on_fail": "stop"},
        ])
        ctx = self._make_context({"items": [
            {"name": "Alice", "value": 0},
            {"name": "", "value": 0},
        ]})
        result = run_async(runner.execute(step, ctx))
        assert result["success"] is False

    def test_validate_step_type_in_model(self):
        """'validate' is a valid step type in the Step model."""
        step = Step.model_validate({
            "id": "quality",
            "type": "validate",
            "rules": [{"field": "{{ item.x }}", "min_ratio": 0.9, "of": "{{ data }}", "on_fail": "warn"}],
        })
        assert step.type == "validate"
        assert step.rules[0]["min_ratio"] == 0.9

    def test_validate_step_in_pipeline(self):
        """Pipeline with a validate step parses and validates correctly."""
        p = Pipeline.model_validate({
            "name": "quality-pipeline",
            "steps": [
                {"id": "fetch", "type": "python", "script": "fetch.py"},
                {
                    "id": "check",
                    "type": "validate",
                    "rules": [
                        {
                            "field": "{{ item.status }}",
                            "min_ratio": 0.95,
                            "of": "{{ fetch.output.items }}",
                            "on_fail": "warn",
                        }
                    ],
                },
            ],
        })
        v = PipelineValidator()
        result = v.validate(p)
        assert result.is_valid


# ===========================================================================
# V6-16: Pipeline Linting Rules
# ===========================================================================


class TestLintRules:
    def test_default_lint_rules_loaded(self):
        """PipelineValidator loads default lint rules."""
        v = PipelineValidator()
        rules = v._load_lint_rules()
        assert len(rules) >= 3
        ids = [r.get("id") for r in rules]
        assert "max-mcp-concurrency" in ids
        assert "no-base64-foreach" in ids
        assert "progress-on-long-timeout" in ids

    def test_custom_lint_rules_injected(self):
        """Custom lint rules can be injected directly for testing."""
        custom = [{"id": "my-rule", "check": "max_concurrency", "type": "http", "max": 3, "severity": "warning"}]
        v = PipelineValidator(lint_rules=custom)
        rules = v._load_lint_rules()
        assert rules == custom

    def test_lint_mcp_concurrency_warn(self):
        """MCP step with parallel + concurrency > 5 triggers lint warning."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "bulk_call",
                "type": "mcp",
                "server": "my-server",
                "tool": "list-items",
                "foreach": "{{ input.ids }}",
                "parallel": True,
                "concurrency": 10,
            },
        ])
        v = PipelineValidator()
        result = v.validate(pipeline)
        warnings_text = " ".join(result.warnings)
        assert "concurrency" in warnings_text.lower()
        assert "bulk_call" in warnings_text

    def test_lint_mcp_concurrency_ok(self):
        """MCP step with concurrency <= 5 does NOT trigger lint warning."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "small_call",
                "type": "mcp",
                "server": "my-server",
                "tool": "list-items",
                "foreach": "{{ input.ids }}",
                "parallel": True,
                "concurrency": 5,
            },
        ])
        v = PipelineValidator(lint_rules=[{
            "id": "max-mcp-concurrency",
            "check": "max_concurrency",
            "type": "mcp",
            "max": 5,
            "severity": "warning",
        }])
        result = v.validate(pipeline)
        assert not any("max-mcp-concurrency" in w for w in result.warnings)

    def test_lint_no_base64_foreach_warn(self):
        """foreach step with base64 in params triggers lint warning."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "process_files",
                "type": "python",
                "script": "process.py",
                "foreach": "{{ input.files }}",
                "params": {"file_base64": "{{ item.base64_content }}"},
            },
        ])
        v = PipelineValidator()
        result = v.validate(pipeline)
        warnings_text = " ".join(result.warnings)
        assert "base64" in warnings_text.lower()
        assert "process_files" in warnings_text

    def test_lint_no_base64_no_foreach_no_warn(self):
        """base64 in params of a non-foreach step is fine."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "single_call",
                "type": "http",
                "url": "https://example.com/upload",
                "params": {"file_base64": "data"},
            },
        ])
        v = PipelineValidator(lint_rules=[{
            "id": "no-base64-foreach",
            "check": "no_base64_foreach",
            "severity": "warning",
        }])
        result = v.validate(pipeline)
        assert not any("no-base64-foreach" in w for w in result.warnings)

    def test_lint_progress_on_long_timeout_warn(self):
        """Step with timeout=5m and progress:false triggers lint warning."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "long_step",
                "type": "python",
                "script": "long.py",
                "timeout": "5m",
                "progress": False,
            },
        ])
        v = PipelineValidator()
        result = v.validate(pipeline)
        warnings_text = " ".join(result.warnings)
        assert "progress" in warnings_text.lower()
        assert "long_step" in warnings_text

    def test_lint_progress_on_long_timeout_ok_when_progress_true(self):
        """Step with timeout=5m and progress:true does NOT trigger lint warning."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "long_step",
                "type": "python",
                "script": "long.py",
                "timeout": "5m",
                "progress": True,
            },
        ])
        v = PipelineValidator(lint_rules=[{
            "id": "progress-on-long-timeout",
            "check": "progress_on_long_timeout",
            "timeout_threshold_seconds": 60,
            "severity": "warning",
        }])
        result = v.validate(pipeline)
        assert not any("progress-on-long-timeout" in w for w in result.warnings)

    def test_lint_progress_short_timeout_no_warn(self):
        """Step with timeout=30s (below threshold) does not trigger lint warning."""
        pipeline = make_pipeline_with_steps([
            {
                "id": "fast_step",
                "type": "python",
                "script": "fast.py",
                "timeout": "30s",
                "progress": False,
            },
        ])
        v = PipelineValidator()
        result = v.validate(pipeline)
        assert not any("fast_step" in w and "progress" in w for w in result.warnings)

    def test_lint_custom_rule_from_file(self, tmp_path, monkeypatch):
        """Custom lint_rules.yaml is loaded and merged with defaults."""
        lint_rules_path = tmp_path / "lint_rules.yaml"
        lint_rules_path.write_text(
            "rules:\n"
            "  - id: custom-rule\n"
            "    check: max_concurrency\n"
            "    type: http\n"
            "    max: 2\n"
            "    severity: warning\n"
        )
        monkeypatch.setenv("HOME", str(tmp_path))
        (tmp_path / ".brix").mkdir(exist_ok=True)
        (tmp_path / ".brix" / "lint_rules.yaml").write_text(
            "rules:\n"
            "  - id: custom-rule\n"
            "    check: max_concurrency\n"
            "    type: http\n"
            "    max: 2\n"
            "    severity: warning\n"
        )
        v = PipelineValidator()
        rules = v._load_lint_rules()
        custom_ids = [r.get("id") for r in rules]
        assert "custom-rule" in custom_ids
        # Defaults are also present
        assert "max-mcp-concurrency" in custom_ids

    def test_parse_timeout_seconds(self):
        """_parse_timeout_seconds handles s/m/h suffixes."""
        v = PipelineValidator()
        assert v._parse_timeout_seconds("30s") == 30.0
        assert v._parse_timeout_seconds("5m") == 300.0
        assert v._parse_timeout_seconds("2h") == 7200.0
        assert v._parse_timeout_seconds("120") == 120.0
        assert v._parse_timeout_seconds("bad") is None
        assert v._parse_timeout_seconds("") is None
