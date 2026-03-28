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


# ---------------------------------------------------------------------------
# T-BRIX-V4-21: validate_input_params
# ---------------------------------------------------------------------------

def test_validate_input_params_all_present():
    """All required params provided → valid result."""
    pipeline = Pipeline.model_validate(
        {
            "name": "input-pipeline",
            "input": {
                "folder": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
            },
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        }
    )
    v = PipelineValidator()
    result = v.validate_input_params(pipeline, {"folder": "Inbox"})
    assert result.is_valid
    assert result.errors == []


def test_validate_input_params_missing_required():
    """Required param without default and absent from user_input → error."""
    pipeline = Pipeline.model_validate(
        {
            "name": "input-pipeline",
            "input": {
                "folder": {"type": "string"},
                "limit": {"type": "integer"},  # no default → required
            },
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        }
    )
    v = PipelineValidator()
    result = v.validate_input_params(pipeline, {"folder": "Inbox"})
    assert not result.is_valid
    assert any("limit" in e for e in result.errors)


def test_validate_input_params_optional_with_default_not_required():
    """Param with a default is not required even when absent."""
    pipeline = Pipeline.model_validate(
        {
            "name": "input-pipeline",
            "input": {
                "limit": {"type": "integer", "default": 50},
            },
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        }
    )
    v = PipelineValidator()
    result = v.validate_input_params(pipeline, {})
    assert result.is_valid


def test_validate_input_params_empty_input_no_required():
    """No required params defined → always valid regardless of user input."""
    pipeline = Pipeline.model_validate(
        {
            "name": "no-input-pipeline",
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        }
    )
    v = PipelineValidator()
    result = v.validate_input_params(pipeline, {})
    assert result.is_valid


def test_validate_input_params_multiple_missing():
    """Multiple missing required params are all reported."""
    pipeline = Pipeline.model_validate(
        {
            "name": "multi-input-pipeline",
            "input": {
                "source": {"type": "string"},
                "dest": {"type": "string"},
                "count": {"type": "integer"},
            },
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        }
    )
    v = PipelineValidator()
    result = v.validate_input_params(pipeline, {})
    assert not result.is_valid
    assert len(result.errors) == 3
    param_names = {"source", "dest", "count"}
    for name in param_names:
        assert any(name in e for e in result.errors)


# ---------------------------------------------------------------------------
# T-BRIX-V4-21: MCP step required params validation
# ---------------------------------------------------------------------------

def test_validate_mcp_required_params_missing(tmp_path):
    """MCP tool required params that are absent from step params → warning."""
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("my-server", [
        {
            "name": "send-mail",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to": {"type": "string"},
                    "subject": {"type": "string"},
                    "body": {"type": "string"},
                },
                "required": ["to", "subject"],
            },
        }
    ])
    pipeline = Pipeline.model_validate(
        {
            "name": "mcp-param-pipeline",
            "steps": [
                {
                    "id": "send",
                    "type": "mcp",
                    "server": "my-server",
                    "tool": "send-mail",
                    "params": {"body": "Hello"},
                }
            ],
        }
    )
    v = PipelineValidator(cache=cache)
    result = v.validate(pipeline)
    assert any("requires param 'to'" in w for w in result.warnings)
    assert any("requires param 'subject'" in w for w in result.warnings)


def test_validate_mcp_required_params_all_provided(tmp_path):
    """MCP tool required params all present → no warning about missing params."""
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("my-server", [
        {
            "name": "send-mail",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to": {"type": "string"},
                    "subject": {"type": "string"},
                },
                "required": ["to", "subject"],
            },
        }
    ])
    pipeline = Pipeline.model_validate(
        {
            "name": "mcp-param-pipeline",
            "steps": [
                {
                    "id": "send",
                    "type": "mcp",
                    "server": "my-server",
                    "tool": "send-mail",
                    "params": {"to": "test@example.com", "subject": "Hi"},
                }
            ],
        }
    )
    v = PipelineValidator(cache=cache)
    result = v.validate(pipeline)
    assert not any("requires param" in w for w in result.warnings)


def test_validate_mcp_no_required_in_schema_no_warning(tmp_path):
    """MCP tool with no 'required' in schema → no missing-param warning."""
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("my-server", [
        {
            "name": "list-items",
            "inputSchema": {
                "type": "object",
                "properties": {"limit": {"type": "integer"}},
            },
        }
    ])
    pipeline = Pipeline.model_validate(
        {
            "name": "mcp-no-req-pipeline",
            "steps": [
                {"id": "lst", "type": "mcp", "server": "my-server", "tool": "list-items"}
            ],
        }
    )
    v = PipelineValidator(cache=cache)
    result = v.validate(pipeline)
    assert not any("requires param" in w for w in result.warnings)


def test_validate_mcp_no_cache_no_warning():
    """No cache available → MCP required-param check is silently skipped."""
    pipeline = Pipeline.model_validate(
        {
            "name": "mcp-nocache-pipeline",
            "steps": [
                {"id": "lst", "type": "mcp", "server": "unknown-server", "tool": "some-tool"}
            ],
        }
    )
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert not any("requires param" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Proactive Hints (T-BRIX-V5-03)
# ---------------------------------------------------------------------------

def test_validator_warns_when_and_else_of_on_same_step():
    """A step with both 'when' and 'else_of' triggers a warning."""
    pipeline = make_pipeline_with_steps([
        {"id": "step_a", "type": "python", "script": "a.py"},
        {
            "id": "step_b",
            "type": "python",
            "script": "b.py",
            "when": "{{ input.flag }}",
            "else_of": "step_a",
        },
    ])
    v = PipelineValidator()
    result = v.validate(pipeline)
    warning_texts = " ".join(result.warnings)
    assert "when" in warning_texts and "else_of" in warning_texts
    assert "step_b" in warning_texts


def test_validator_no_warn_when_only_when():
    """A step with only 'when' (no else_of) should NOT trigger the mutual-exclusion warning."""
    pipeline = make_pipeline_with_steps([
        {
            "id": "conditional_step",
            "type": "python",
            "script": "run.py",
            "when": "{{ input.flag }}",
        },
    ])
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert not any("mutually exclusive" in w for w in result.warnings)


def test_validator_no_warn_when_only_else_of():
    """A step with only 'else_of' (no when) should NOT trigger the mutual-exclusion warning."""
    pipeline = make_pipeline_with_steps([
        {"id": "step_a", "type": "python", "script": "a.py", "when": "{{ input.flag }}"},
        {"id": "step_b", "type": "python", "script": "b.py", "else_of": "step_a"},
    ])
    v = PipelineValidator()
    result = v.validate(pipeline)
    assert not any("mutually exclusive" in w for w in result.warnings)


def test_validator_warns_on_error_continue_http():
    """on_error: continue on an http step triggers the retry hint."""
    pipeline = make_pipeline_with_steps([
        {
            "id": "fetch",
            "type": "http",
            "url": "https://example.com/api",
            "on_error": "continue",
        },
    ])
    v = PipelineValidator()
    result = v.validate(pipeline)
    warning_texts = " ".join(result.warnings)
    assert "on_error" in warning_texts or "retry" in warning_texts
    assert "fetch" in warning_texts


def test_validator_warns_on_error_continue_mcp():
    """on_error: continue on an mcp step triggers the retry hint."""
    pipeline = make_pipeline_with_steps([
        {
            "id": "call_tool",
            "type": "mcp",
            "server": "my-server",
            "tool": "my-tool",
            "on_error": "continue",
        },
    ])
    v = PipelineValidator()
    result = v.validate(pipeline)
    warning_texts = " ".join(result.warnings)
    assert "on_error" in warning_texts or "retry" in warning_texts
    assert "call_tool" in warning_texts


def test_validator_no_warn_on_error_continue_python():
    """on_error: continue on a python step should NOT trigger the retry hint (not transient)."""
    pipeline = make_pipeline_with_steps([
        {
            "id": "run_script",
            "type": "python",
            "script": "run.py",
            "on_error": "continue",
        },
    ])
    v = PipelineValidator()
    result = v.validate(pipeline)
    # Should NOT have the http/mcp specific retry hint
    assert not any(
        ("on_error" in w and "retry" in w and "run_script" in w)
        for w in result.warnings
    )


def test_validator_no_warn_on_error_retry():
    """on_error: retry on an http step should NOT trigger the continue hint."""
    pipeline = make_pipeline_with_steps([
        {
            "id": "fetch",
            "type": "http",
            "url": "https://example.com/api",
            "on_error": "retry",
        },
    ])
    v = PipelineValidator()
    result = v.validate(pipeline)
    # Should not warn about on_error: continue since it's already retry
    assert not any(
        ("on_error" in w and "continue" in w and "fetch" in w)
        for w in result.warnings
    )
