"""Tests for brix.context.PipelineContext."""

import os

import pytest

from brix.context import PipelineContext
from brix.loader import PipelineLoader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIMPLE_PIPELINE_YAML = """
name: test-pipeline
input:
  greeting:
    type: string
    default: hello
  count:
    type: integer
    default: 3
credentials:
  api_key:
    env: TEST_API_KEY
steps:
  - id: step1
    type: cli
    args: ["echo", "hi"]
"""


def _load_pipeline(yaml_str: str):
    loader = PipelineLoader()
    return loader.load_from_string(yaml_str)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_context_creation():
    """PipelineContext can be instantiated with explicit input/credentials."""
    ctx = PipelineContext(
        pipeline_input={"foo": "bar"},
        credentials={"key": "secret"},
    )
    assert ctx.input == {"foo": "bar"}
    assert ctx.credentials == {"key": "secret"}
    assert ctx.step_outputs == {}
    assert ctx.run_id.startswith("run-")
    assert len(ctx.run_id) == len("run-") + 12


def test_context_run_id_unique():
    """Each context gets a unique run_id."""
    ctx1 = PipelineContext()
    ctx2 = PipelineContext()
    assert ctx1.run_id != ctx2.run_id


def test_context_from_pipeline_with_defaults():
    """from_pipeline resolves defaults from pipeline input definition."""
    pipeline = _load_pipeline(SIMPLE_PIPELINE_YAML)
    ctx = PipelineContext.from_pipeline(pipeline)
    assert ctx.input["greeting"] == "hello"
    assert ctx.input["count"] == 3


def test_context_from_pipeline_with_user_input():
    """User input overrides pipeline defaults."""
    pipeline = _load_pipeline(SIMPLE_PIPELINE_YAML)
    ctx = PipelineContext.from_pipeline(pipeline, user_input={"greeting": "world"})
    assert ctx.input["greeting"] == "world"
    assert ctx.input["count"] == 3  # still default


def test_context_credentials_from_env(monkeypatch):
    """Credentials are resolved from environment variables."""
    monkeypatch.setenv("TEST_API_KEY", "my-secret-key")
    pipeline = _load_pipeline(SIMPLE_PIPELINE_YAML)
    ctx = PipelineContext.from_pipeline(pipeline)
    assert ctx.credentials["api_key"] == "my-secret-key"


def test_context_credentials_missing_env():
    """Missing env var results in empty string credential (not an error)."""
    # Ensure env var is not set
    os.environ.pop("TEST_API_KEY", None)
    pipeline = _load_pipeline(SIMPLE_PIPELINE_YAML)
    ctx = PipelineContext.from_pipeline(pipeline)
    assert ctx.credentials["api_key"] == ""


def test_context_set_get_output():
    """set_output stores value, get_output retrieves it."""
    ctx = PipelineContext()
    ctx.set_output("step_a", {"result": 42})
    assert ctx.get_output("step_a") == {"result": 42}


def test_context_get_output_missing_returns_none():
    """get_output returns None for unknown step IDs."""
    ctx = PipelineContext()
    assert ctx.get_output("nonexistent") is None


def test_context_to_jinja_context():
    """to_jinja_context returns correct structure with input, credentials, step outputs."""
    ctx = PipelineContext(
        pipeline_input={"name": "alice"},
        credentials={"token": "abc123"},
    )
    ctx.set_output("fetch", ["item1", "item2"])
    ctx.set_output("transform", {"count": 2})

    jinja_ctx = ctx.to_jinja_context()

    assert jinja_ctx["input"] == {"name": "alice"}
    assert jinja_ctx["credentials"] == {"token": "abc123"}
    assert jinja_ctx["fetch"] == {"output": ["item1", "item2"]}
    assert jinja_ctx["transform"] == {"output": {"count": 2}}
    assert "item" not in jinja_ctx


def test_context_to_jinja_context_with_item():
    """to_jinja_context includes 'item' key when item is provided."""
    ctx = PipelineContext(pipeline_input={"x": 1})
    jinja_ctx = ctx.to_jinja_context(item="current-item")
    assert jinja_ctx["item"] == "current-item"


def test_context_to_jinja_context_item_none_excluded():
    """item=None should NOT add 'item' key to the context."""
    ctx = PipelineContext()
    jinja_ctx = ctx.to_jinja_context(item=None)
    assert "item" not in jinja_ctx
