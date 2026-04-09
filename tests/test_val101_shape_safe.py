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
