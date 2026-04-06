"""Focused tests for actionable preflight validation hints."""

from brix.models import Pipeline
from brix.validator import PipelineValidator


def make_pipeline(steps: list[dict]) -> Pipeline:
    return Pipeline.model_validate({"name": "preflight-hints", "steps": steps})


def test_deprecated_type_warning_includes_replacement_hint():
    pipeline = make_pipeline(
        [{"id": "transform_step", "type": "transform", "params": {"expression": "{{ input.value }}"}}]
    )

    result = PipelineValidator().validate(pipeline)

    assert any('type "transform" is deprecated' in warning for warning in result.warnings)
    assert any('Use "flow.transform" instead' in warning for warning in result.warnings)


def test_missing_required_field_hint_includes_schema_reference(monkeypatch):
    pipeline = make_pipeline(
        [{"id": "query_step", "type": "db.query", "params": {"connection": "main"}}]
    )

    class FakeBrick:
        runner = ""

        def to_json_schema(self):
            return {
                "type": "object",
                "properties": {
                    "connection": {"type": "string"},
                    "query": {"type": "string"},
                },
                "required": ["connection", "query"],
            }

    class FakeRegistry:
        def get(self, name):
            return FakeBrick() if name == "db.query" else None

        def list_all(self):
            return []

    monkeypatch.setattr("brix.validator.BrickRegistry", FakeRegistry)

    result = PipelineValidator().validate(pipeline)

    assert any('missing required field "query"' in warning for warning in result.warnings)
    assert any('get_brick_schema(name="db.query") shows required fields.' in warning for warning in result.warnings)


def test_config_vs_params_misplacement_hint_explains_correct_location():
    pipeline = make_pipeline(
        [{"id": "py_step", "type": "script.python", "params": {"helper": "demo_helper"}}]
    )

    result = PipelineValidator().validate(pipeline)

    assert any('"helper" found in params but should be in config' in warning for warning in result.warnings)
    assert any('get_brick_schema(name="script.python") shows config structure.' in warning for warning in result.warnings)
