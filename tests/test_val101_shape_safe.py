"""Tests for T-BRIX-VAL-101 shape-safe validator access."""

from unittest.mock import patch

import pytest

from brix.models import Pipeline, Step
from brix.validator import PipelineValidator, StepAnalysis, ValidationContext


def _step(step_id: str, type: str = "flow.set", **kwargs) -> Step:
    return Step(id=step_id, type=type, **kwargs)


def _pipeline(steps: list[Step], **kwargs) -> Pipeline:
    return Pipeline(name="val101-pipeline", steps=steps, **kwargs)


def _noop(self, *args, **kwargs):
    pass


def test_step_analysis_with_dict_params():
    analysis = StepAnalysis.from_step(
        _step("set-values", params={"alpha": 1, "beta": "{{ input.name }}"}, config={"persist": True}),
        index=0,
    )

    assert analysis.effective_type == "flow.set"
    assert analysis.normalized_params == {"alpha": 1, "beta": "{{ input.name }}"}
    assert analysis.normalized_config == {"persist": True}
    assert analysis.param_get("alpha") == 1
    assert analysis.param_get("missing", "fallback") == "fallback"
    assert analysis.param_values() == [1, "{{ input.name }}"]
    assert analysis.param_items() == [("alpha", 1), ("beta", "{{ input.name }}")]
    assert analysis.config_values() == [True]


def test_step_analysis_with_list_params():
    analysis = StepAnalysis.from_step(
        _step("foreach-step", type="flow.transform", params=["a", "{{ source.output }}", 3]),
        index=1,
    )

    assert analysis.normalized_params == {}
    assert analysis.param_get("missing", "fallback") == "fallback"
    assert analysis.param_values() == ["a", "{{ source.output }}", 3]
    assert analysis.param_items() == [(0, "a"), (1, "{{ source.output }}"), (2, 3)]


def test_step_analysis_with_none_params():
    analysis = StepAnalysis.from_step(_step("empty", params=None), index=2)

    assert analysis.normalized_params == {}
    assert analysis.param_values() == []
    assert analysis.param_items() == []
    assert analysis.param_get("missing") is None


def test_validation_context_builds_correctly():
    pipeline = _pipeline(
        [
            _step("first", params={"x": 1}),
            _step("second", type="db.query", params=["{{ first.output }}"]),
        ]
    )

    ctx = ValidationContext.from_pipeline(pipeline)

    assert [analysis.step.id for analysis in ctx.steps] == ["first", "second"]
    assert set(ctx.step_map) == {"first", "second"}
    assert ctx.known_step_ids == {"first", "second"}
    assert ctx.pipeline_metadata["name"] == "val101-pipeline"
    assert ctx.pipeline_metadata["version"] == pipeline.version
    assert ctx.step_map["second"].effective_type == "db.query"


@pytest.fixture
def _patch_heavy_checks():
    heavy_checks = [
        "_check_sub_pipeline_existence",
        "_check_connection_existence",
        "_check_brick_config_schema",
        "_check_jinja_ast",
    ]
    patches = [patch.object(PipelineValidator, name, _noop) for name in heavy_checks]
    for item in patches:
        item.start()
    yield
    for item in patches:
        item.stop()


def test_existing_checks_do_not_crash_with_list_params(_patch_heavy_checks):
    pipeline = _pipeline(
        [
            _step("source", type="db.query", config={"query": "SELECT 1", "connection": "main"}),
            _step(
                "consumer",
                type="flow.set",
                params=["{{ source.output }}", "base64-payload"],
                foreach="{{ source.output }}",
            ),
        ]
    )

    result = PipelineValidator(lint_rules=[]).validate(pipeline, level="standard")

    assert isinstance(result.errors, list)
    assert isinstance(result.warnings, list)


def test_validate_quick_only_runs_core_checks(monkeypatch):
    pipeline = _pipeline([_step("first"), _step("second", params={"value": "{{ first.output }}"})])
    validator = PipelineValidator(lint_rules=[])
    calls: list[str] = []

    def make_recorder(name: str):
        def recorder(self, ctx, result, *args, **kwargs):
            calls.append(name)

        return recorder

    monkeypatch.setattr(PipelineValidator, "run_core_checks", make_recorder("core"))
    monkeypatch.setattr(PipelineValidator, "run_schema_checks", make_recorder("schema"))
    monkeypatch.setattr(PipelineValidator, "run_reference_checks", make_recorder("reference"))
    monkeypatch.setattr(PipelineValidator, "run_flow_checks", make_recorder("flow"))
    monkeypatch.setattr(PipelineValidator, "run_lint_checks", make_recorder("lint"))
    monkeypatch.setattr(PipelineValidator, "run_deep_checks", make_recorder("deep"))

    result = validator.validate(pipeline, level="quick")

    assert result.is_valid
    assert calls == ["core"]


def test_legacy_mcp_and_mcp_call_validate_identically(monkeypatch):
    monkeypatch.setattr(PipelineValidator, "_check_deprecated_step_types", _noop)

    legacy_pipeline = _pipeline(
        [
            _step(
                "call-tool",
                type="mcp",
                server="demo-server",
                tool="demo-tool",
                params={"query": "hello"},
            )
        ]
    )
    brick_pipeline = _pipeline(
        [
            _step(
                "call-tool",
                type="mcp.call",
                server="demo-server",
                tool="demo-tool",
                params={"query": "hello"},
            )
        ]
    )

    legacy_result = PipelineValidator(lint_rules=[]).validate(legacy_pipeline, level="standard")
    brick_result = PipelineValidator(lint_rules=[]).validate(brick_pipeline, level="standard")

    assert legacy_result.errors == brick_result.errors
    assert legacy_result.warnings == brick_result.warnings
    assert legacy_result.infos == brick_result.infos
    assert legacy_result.checks == brick_result.checks
