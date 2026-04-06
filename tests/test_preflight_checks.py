"""Tests for E-BRIX-PREFLIGHT validation checks."""

from unittest import mock

from brix.models import Pipeline
from brix.validator import PipelineValidator


def make_pipeline(steps: list[dict]) -> Pipeline:
    return Pipeline.model_validate({"name": "preflight-test", "steps": steps})


def test_brick_schema_missing_required_field_warns(monkeypatch):
    pipeline = make_pipeline(
        [{"id": "step1", "type": "python", "params": {"foo": "bar"}, "script": "run.py"}]
    )

    class FakeBrick:
        def to_json_schema(self):
            return {
                "type": "object",
                "properties": {"script": {"type": "string"}},
                "required": ["script", "required_field"],
            }

        runner = ""

    class FakeRegistry:
        def get(self, name):
            return FakeBrick() if name == "script.python" else None

        def list_all(self):
            return []

    monkeypatch.setattr("brix.validator.BrickRegistry", FakeRegistry)

    result = PipelineValidator().validate(pipeline)

    assert result.is_valid
    assert any("does not match schema" in warning for warning in result.warnings)
    assert any("required_field" in warning for warning in result.warnings)


def test_jinja_unknown_name_warns():
    pipeline = make_pipeline(
        [
            {"id": "step1", "type": "python", "script": "run.py"},
            {
                "id": "step2",
                "type": "python",
                "script": "other.py",
                "params": {"value": "{{ mystery_name.output }}"},
            },
        ]
    )

    result = PipelineValidator().validate(pipeline)

    assert result.is_valid
    assert any("unknown name 'mystery_name'" in warning for warning in result.warnings)


def test_jinja_syntax_error_is_error():
    pipeline = make_pipeline(
        [{"id": "step1", "type": "python", "script": "run.py", "when": "{{ input.foo "}]
    )

    result = PipelineValidator().validate(pipeline)

    assert not result.is_valid
    assert any("Jinja2 syntax error" in error for error in result.errors)


def test_missing_sub_pipeline_is_error(monkeypatch):
    pipeline = make_pipeline(
        [{"id": "sub", "type": "pipeline", "pipeline": "does-not-exist"}]
    )

    load_mock = mock.Mock(side_effect=FileNotFoundError("not found"))
    monkeypatch.setattr("brix.validator.PipelineStore.load", load_mock)

    result = PipelineValidator().validate(pipeline)

    assert not result.is_valid
    assert any("sub-pipeline 'does-not-exist' does not exist" in error for error in result.errors)


def test_missing_connection_is_error(monkeypatch):
    pipeline = make_pipeline(
        [
            {
                "id": "query",
                "type": "db_query",
                "params": {"connection": "missing_conn", "query": "SELECT 1"},
            }
        ]
    )

    class FakeManager:
        def __init__(self, db):
            self.db = db

        def list(self):
            return []

    monkeypatch.setattr("brix.validator.ConnectionManager", FakeManager)

    result = PipelineValidator().validate(pipeline)

    assert not result.is_valid
    assert any("Connection 'missing_conn' does not exist" in error for error in result.errors)


def test_known_step_refs_and_brix_globals_do_not_warn():
    pipeline = make_pipeline(
        [
            {
                "id": "fetch",
                "type": "python",
                "script": "fetch.py",
                "params": {"seed": "{{ input.limit | default(5) }}"},
            },
            {
                "id": "process",
                "type": "python",
                "script": "process.py",
                "when": "{{ true }}",
                "foreach": "{{ fetch.output.items | default([]) }}",
                "params": {
                    "data": "{{ fetch.output.value }}",
                    "fallback": "{{ last_output | default('') }}",
                    "token": "{{ var.api_token | default('') }}",
                    "cache_key": "{{ store.result_key | default(uuid4()) }}",
                    "stamp": "{{ now() }}",
                    "namespace_name": "{{ namespace() }}",
                },
            },
        ]
    )

    result = PipelineValidator().validate(pipeline)

    assert result.is_valid
    assert not any("references unknown name" in warning for warning in result.warnings)
    assert not any("Jinja2 template" in warning for warning in result.warnings)
