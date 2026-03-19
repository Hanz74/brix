"""Tests for PipelineValidator."""
import os
from pathlib import Path

import pytest

from brix.models import Pipeline, Step
from brix.cache import SchemaCache
from brix.validator import PipelineValidator, ValidationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_pipeline(**kwargs) -> Pipeline:
    """Build a minimal valid Pipeline, merging extra kwargs."""
    defaults = {
        "name": "test-pipeline",
        "steps": [{"id": "step1", "type": "python", "script": "run.py"}],
    }
    defaults.update(kwargs)
    return Pipeline.model_validate(defaults)


def make_pipeline_with_steps(steps_raw: list[dict]) -> Pipeline:
    return Pipeline.model_validate({"name": "test-pipeline", "steps": steps_raw})


# ---------------------------------------------------------------------------
# ValidationResult basics
# ---------------------------------------------------------------------------

def test_validation_result_valid_when_no_errors():
    r = ValidationResult()
    assert r.is_valid is True


def test_validation_result_invalid_when_errors():
    r = ValidationResult()
    r.add_error("something wrong")
    assert r.is_valid is False


# ---------------------------------------------------------------------------
# T-BRIX-22 validator tests
# ---------------------------------------------------------------------------

def test_validate_valid_pipeline():
    pipeline = make_pipeline()
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert result.is_valid
    assert any("unique" in c for c in result.checks)
    assert any("valid" in c for c in result.checks)


def test_validate_duplicate_step_ids():
    """Duplicate IDs are detected even if Pydantic doesn't catch them."""
    pipeline = make_pipeline_with_steps([
        {"id": "step1", "type": "python", "script": "a.py"},
        {"id": "step1", "type": "python", "script": "b.py"},
    ])
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert not result.is_valid
    assert any("Duplicate" in e for e in result.errors)


def test_validate_mcp_missing_server():
    """MCP step without server is flagged.

    Because models.py already enforces server+tool on mcp steps at parse time,
    we inject a step directly after construction to bypass that validator.
    """
    pipeline = make_pipeline()
    # Inject an mcp step with server=None by bypassing model_validator
    bad_step = object.__new__(Step)
    bad_step.__dict__.update(
        {
            "id": "mcp1",
            "type": "mcp",
            "server": None,
            "tool": "some-tool",
            "script": None,
            "url": None,
            "method": "GET",
            "headers": None,
            "body": None,
            "command": None,
            "args": None,
            "shell": False,
            "pipeline": None,
            "params": None,
            "foreach": None,
            "parallel": False,
            "concurrency": 10,
            "when": None,
            "on_error": None,
            "timeout": None,
        }
    )
    pipeline.steps.append(bad_step)

    v = PipelineValidator()
    result = v.validate(pipeline)
    assert not result.is_valid
    assert any("needs 'server'" in e for e in result.errors)


def test_validate_mcp_missing_tool():
    """MCP step without tool is flagged (bypassing model_validator)."""
    pipeline = make_pipeline()
    bad_step = object.__new__(Step)
    bad_step.__dict__.update(
        {
            "id": "mcp2",
            "type": "mcp",
            "server": "my-server",
            "tool": None,
            "script": None,
            "url": None,
            "method": "GET",
            "headers": None,
            "body": None,
            "command": None,
            "args": None,
            "shell": False,
            "pipeline": None,
            "params": None,
            "foreach": None,
            "parallel": False,
            "concurrency": 10,
            "when": None,
            "on_error": None,
            "timeout": None,
        }
    )
    pipeline.steps.append(bad_step)

    v = PipelineValidator()
    result = v.validate(pipeline)
    assert not result.is_valid
    assert any("needs 'tool'" in e for e in result.errors)


def test_validate_script_not_found(tmp_path):
    """Script path that does not exist generates an error."""
    pipeline = make_pipeline_with_steps([
        {"id": "step1", "type": "python", "script": "nonexistent_script.py"},
    ])
    v = PipelineValidator()
    result = v.validate(pipeline, pipeline_dir=tmp_path)
    assert not result.is_valid
    assert any("Script not found" in e for e in result.errors)


def test_validate_script_found(tmp_path):
    """Script path that exists passes."""
    script = tmp_path / "run.py"
    script.write_text("# ok\n")
    pipeline = make_pipeline_with_steps([
        {"id": "step1", "type": "python", "script": "run.py"},
    ])
    v = PipelineValidator()
    result = v.validate(pipeline, pipeline_dir=tmp_path)
    assert result.is_valid
    assert any("Script exists" in c for c in result.checks)


def test_validate_credential_not_set(monkeypatch):
    """Missing env var for credential emits a warning."""
    monkeypatch.delenv("MY_TOKEN", raising=False)
    pipeline = Pipeline.model_validate(
        {
            "name": "cred-pipeline",
            "credentials": {"token": {"env": "MY_TOKEN"}},
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        }
    )
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert result.is_valid  # warnings don't invalidate
    assert any("NOT SET" in w for w in result.warnings)


def test_validate_credential_set(monkeypatch):
    """Present env var for credential emits a check (not warning)."""
    monkeypatch.setenv("MY_TOKEN", "secret-value")
    pipeline = Pipeline.model_validate(
        {
            "name": "cred-pipeline",
            "credentials": {"token": {"env": "MY_TOKEN"}},
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        }
    )
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert result.is_valid
    assert any("MY_TOKEN" in c and "set" in c for c in result.checks)


def test_validate_when_without_default():
    """A step that uses output of a conditional step without | default warns."""
    pipeline = Pipeline.model_validate(
        {
            "name": "when-pipeline",
            "steps": [
                {"id": "fetch", "type": "python", "script": "fetch.py", "when": "{{ input.do_fetch }}"},
                {
                    "id": "process",
                    "type": "python",
                    "script": "process.py",
                    "params": {"data": "{{ fetch.output }}"},
                },
            ],
        }
    )
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert any("without | default()" in w for w in result.warnings)


def test_validate_when_with_default_no_warning():
    """A step that uses | default() on a conditional step output does not warn."""
    pipeline = Pipeline.model_validate(
        {
            "name": "when-pipeline",
            "steps": [
                {"id": "fetch", "type": "python", "script": "fetch.py", "when": "{{ input.do_fetch }}"},
                {
                    "id": "process",
                    "type": "python",
                    "script": "process.py",
                    "params": {"data": "{{ fetch.output | default([]) }}"},
                },
            ],
        }
    )
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert not any("without | default()" in w for w in result.warnings)


def test_validate_future_step_reference():
    """Referencing a step that comes later in the pipeline is an error."""
    pipeline = Pipeline.model_validate(
        {
            "name": "forward-ref-pipeline",
            "steps": [
                {
                    "id": "step_a",
                    "type": "python",
                    "script": "a.py",
                    "params": {"x": "{{ step_b.output }}"},
                },
                {"id": "step_b", "type": "python", "script": "b.py"},
            ],
        }
    )
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert not result.is_valid
    assert any("future step" in e for e in result.errors)


def test_validate_mcp_tool_not_in_cache(tmp_path):
    """MCP tool not in cached schema emits a warning."""
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("my-server", [{"name": "known-tool"}])

    # Build a valid MCP step via model_validate (server+tool required)
    pipeline = Pipeline.model_validate(
        {
            "name": "mcp-pipeline",
            "steps": [{"id": "s1", "type": "mcp", "server": "my-server", "tool": "unknown-tool"}],
        }
    )
    v = PipelineValidator(cache=cache)
    result = v.validate(pipeline)
    assert any("not in cached schema" in w for w in result.warnings)


def test_validate_output_bad_reference():
    """Output referencing a non-existent step ID warns."""
    pipeline = Pipeline.model_validate(
        {
            "name": "out-pipeline",
            "steps": [{"id": "step1", "type": "python", "script": "run.py"}],
            "output": {"result": "{{ ghost.output }}"},
        }
    )
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert any("may reference non-existent step" in w for w in result.warnings)
