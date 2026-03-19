"""Tests for brix.loader module."""

import json
import os
import tempfile

import pytest

from brix.loader import PipelineLoader
from brix.models import Pipeline

# ---------------------------------------------------------------------------
# Load tests
# ---------------------------------------------------------------------------


def test_load_from_string_minimal():
    """Minimal valid pipeline YAML loads correctly."""
    yaml_str = """
name: test
steps:
  - id: s1
    type: python
    script: run.py
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_str)
    assert pipeline.name == "test"
    assert len(pipeline.steps) == 1
    assert pipeline.steps[0].id == "s1"


def test_load_from_string_full():
    """Full pipeline YAML with all fields parses correctly."""
    yaml_str = """
name: full-test
version: "1.0.0"
description: Full test pipeline
input:
  query:
    type: str
    default: "test"
credentials:
  token:
    env: BRIX_CRED_TOKEN
error_handling:
  on_error: continue
steps:
  - id: fetch
    type: mcp
    server: m365
    tool: list-mail
    params:
      filter: "{{ input.query }}"
  - id: process
    type: python
    script: helpers/process.py
    foreach: "{{ fetch.output }}"
    parallel: true
    concurrency: 5
output:
  result: "{{ process.output }}"
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_str)
    assert pipeline.version == "1.0.0"
    assert "query" in pipeline.input
    assert pipeline.error_handling.on_error == "continue"
    assert len(pipeline.steps) == 2


def test_load_from_file():
    """Load pipeline from an actual YAML file on disk."""
    yaml_content = """
name: file-test
steps:
  - id: s1
    type: cli
    args: ["echo", "hello"]
"""
    loader = PipelineLoader()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        pipeline = loader.load(path)
        assert pipeline.name == "file-test"
    finally:
        os.unlink(path)


def test_load_invalid_yaml():
    """Malformed YAML raises an exception."""
    loader = PipelineLoader()
    with pytest.raises(Exception):
        loader.load_from_string("name: [invalid yaml {{")


# ---------------------------------------------------------------------------
# render_template tests
# ---------------------------------------------------------------------------


def test_render_template_simple():
    """Simple variable substitution works."""
    loader = PipelineLoader()
    result = loader.render_template("Hello {{ name }}", {"name": "World"})
    assert result == "Hello World"


def test_render_template_nested():
    """Nested context access works."""
    loader = PipelineLoader()
    result = loader.render_template("{{ input.query }}", {"input": {"query": "test"}})
    assert result == "test"


def test_render_template_default_filter():
    """Jinja2 default filter provides a fallback for missing variables (D-16)."""
    loader = PipelineLoader()
    result = loader.render_template("{{ missing | default('fallback') }}", {})
    assert result == "fallback"


def test_render_template_default_list():
    """Default filter with an empty-list sentinel."""
    loader = PipelineLoader()
    result = loader.render_template("{{ items | default([]) }}", {})
    assert result == "[]"


def test_render_template_sandbox_blocks_imports():
    """SandboxedEnvironment blocks dangerous dunder access (D-13)."""
    loader = PipelineLoader()
    with pytest.raises(Exception):
        loader.render_template("{{ ''.__class__.__mro__[1].__subclasses__() }}", {})


# ---------------------------------------------------------------------------
# render_value tests
# ---------------------------------------------------------------------------


def test_render_value_plain_string():
    """Plain string without {{ }} passes through unchanged."""
    loader = PipelineLoader()
    assert loader.render_value("hello", {}) == "hello"


def test_render_value_template_string():
    """String with {{ }} is rendered."""
    loader = PipelineLoader()
    assert loader.render_value("{{ name }}", {"name": "World"}) == "World"


def test_render_value_json_result():
    """Template that renders to valid JSON is parsed to the native type."""
    loader = PipelineLoader()
    ctx = {"data": json.dumps({"key": "value"})}
    result = loader.render_value("{{ data }}", ctx)
    assert result == {"key": "value"}


def test_render_value_dict():
    """Dict values are recursively rendered."""
    loader = PipelineLoader()
    result = loader.render_value({"a": "{{ x }}", "b": "plain"}, {"x": "rendered"})
    assert result == {"a": "rendered", "b": "plain"}


def test_render_value_list():
    """List values are recursively rendered."""
    loader = PipelineLoader()
    result = loader.render_value(["{{ x }}", "plain"], {"x": "rendered"})
    assert result == ["rendered", "plain"]


def test_render_value_non_string_int():
    """Integer passes through unchanged."""
    loader = PipelineLoader()
    assert loader.render_value(42, {}) == 42


def test_render_value_non_string_bool():
    """Boolean passes through unchanged."""
    loader = PipelineLoader()
    assert loader.render_value(True, {}) is True


def test_render_value_non_string_none():
    """None passes through unchanged."""
    loader = PipelineLoader()
    assert loader.render_value(None, {}) is None


# ---------------------------------------------------------------------------
# evaluate_condition tests
# ---------------------------------------------------------------------------


def test_evaluate_condition_true():
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ flag }}", {"flag": True}) is True


def test_evaluate_condition_false():
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ flag }}", {"flag": False}) is False


def test_evaluate_condition_empty_string():
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ val }}", {"val": ""}) is False


def test_evaluate_condition_none_value():
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ val }}", {"val": None}) is False


def test_evaluate_condition_missing_var():
    """Missing variable renders to empty string → falsy (D-16 / Undefined)."""
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ missing }}", {}) is False


def test_evaluate_condition_no_condition_empty():
    """Empty condition string → always execute."""
    loader = PipelineLoader()
    assert loader.evaluate_condition("", {}) is True


def test_evaluate_condition_no_condition_none():
    """None condition → always execute."""
    loader = PipelineLoader()
    assert loader.evaluate_condition(None, {}) is True


# ---------------------------------------------------------------------------
# resolve_foreach tests
# ---------------------------------------------------------------------------


def test_resolve_foreach_list():
    loader = PipelineLoader()
    result = loader.resolve_foreach("{{ items }}", {"items": [1, 2, 3]})
    assert result == [1, 2, 3]


def test_resolve_foreach_json_string():
    """A template rendering to a JSON list string is parsed correctly."""
    loader = PipelineLoader()
    result = loader.resolve_foreach("{{ data }}", {"data": json.dumps([1, 2, 3])})
    assert result == [1, 2, 3]


def test_resolve_foreach_not_list_raises():
    """Non-list resolution raises ValueError with descriptive message."""
    loader = PipelineLoader()
    with pytest.raises(ValueError, match="did not resolve to a list"):
        loader.resolve_foreach("{{ val }}", {"val": "not-a-list"})
