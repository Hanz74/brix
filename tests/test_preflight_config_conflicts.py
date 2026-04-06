"""Tests for preflight config vs top-level conflict warnings."""

from brix.models import Pipeline
from brix.validator import PipelineValidator


def make_pipeline(steps: list[dict]) -> Pipeline:
    return Pipeline.model_validate({"name": "preflight-config-conflicts", "steps": steps})


def test_config_method_overrides_default_get_without_warning(monkeypatch):
    pipeline = make_pipeline(
        [
            {
                "id": "fetch",
                "type": "http",
                "method": "GET",
                "config": {"method": "POST"},
            }
        ]
    )

    monkeypatch.setattr(PipelineValidator, "_resolve_step_schema", lambda self, step: None)

    result = PipelineValidator().validate(pipeline)

    assert not any("config.method" in warning for warning in result.warnings)


def test_config_method_conflict_with_non_default_toplevel_warns(monkeypatch):
    pipeline = make_pipeline(
        [
            {
                "id": "fetch",
                "type": "http",
                "method": "PUT",
                "config": {"method": "POST"},
            }
        ]
    )

    monkeypatch.setattr(PipelineValidator, "_resolve_step_schema", lambda self, step: None)

    result = PipelineValidator().validate(pipeline)

    assert any("Step fetch: config.method='POST' differs from step.method='PUT'" in warning for warning in result.warnings)
    assert any("Config takes precedence after merge." in warning for warning in result.warnings)
    assert any('get_brick_schema(name="http")' in warning for warning in result.warnings)


def test_config_helper_with_null_toplevel_does_not_warn(monkeypatch):
    pipeline = make_pipeline(
        [
            {
                "id": "run_helper",
                "type": "python",
                "script": "run.py",
                "helper": None,
                "config": {"helper": "my_helper"},
            }
        ]
    )

    monkeypatch.setattr(PipelineValidator, "_resolve_step_schema", lambda self, step: None)

    result = PipelineValidator().validate(pipeline)

    assert not any("config.helper" in warning for warning in result.warnings)
