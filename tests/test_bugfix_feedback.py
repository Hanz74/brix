"""Tests for 3 bug fixes (feedback batch).

Bug 1: input.* references in output templates should not trigger
       "may reference non-existent step" warning.

Bug 2: TriggerConfig.enabled should accept string "false"/"true" and
       coerce to bool instead of raising a Pydantic validation error.

Bug 3: progress-on-long-timeout lint should NOT fire for MCP steps
       because external MCP servers don't support Brix progress events.
"""
import pytest

from brix.models import Pipeline
from brix.validator import PipelineValidator
from brix.triggers.models import TriggerConfig


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


def _no_progress_rules() -> list:
    """Return only the progress lint rule so tests are isolated."""
    return [
        {
            "id": "progress-on-long-timeout",
            "check": "progress_on_long_timeout",
            "timeout_threshold_seconds": 60,
            "severity": "warning",
        }
    ]


# ---------------------------------------------------------------------------
# Bug 1: input.* references in output templates
# ---------------------------------------------------------------------------

class TestInputReferenceInOutput:
    def test_input_ref_no_warning(self):
        """{{ input.task_id }} in output must NOT trigger step-reference warning."""
        pipeline = Pipeline.model_validate({
            "name": "test",
            "input": {"task_id": {"type": "string", "description": "task id"}},
            "steps": [{"id": "step1", "type": "python", "script": "run.py"}],
            "output": {"result_id": "{{ input.task_id }}"},
        })
        v = PipelineValidator(lint_rules=[])
        result = v.validate(pipeline)
        step_ref_warnings = [
            w for w in result.warnings
            if "may reference non-existent step" in w
        ]
        assert step_ref_warnings == [], (
            f"Got unexpected step-ref warning(s): {step_ref_warnings}"
        )

    def test_real_missing_step_still_warns(self):
        """A genuine missing step reference in output MUST still produce a warning."""
        pipeline = Pipeline.model_validate({
            "name": "test",
            "steps": [{"id": "step1", "type": "python", "script": "run.py"}],
            "output": {"result": "{{ ghost_step.output }}"},
        })
        v = PipelineValidator(lint_rules=[])
        result = v.validate(pipeline)
        step_ref_warnings = [
            w for w in result.warnings
            if "may reference non-existent step" in w
        ]
        assert step_ref_warnings, "Expected warning for non-existent step reference in output"

    def test_known_step_ref_no_warning(self):
        """A valid step reference in output must NOT produce a warning."""
        pipeline = Pipeline.model_validate({
            "name": "test",
            "steps": [{"id": "step1", "type": "python", "script": "run.py"}],
            "output": {"result": "{{ step1.output }}"},
        })
        v = PipelineValidator(lint_rules=[])
        result = v.validate(pipeline)
        step_ref_warnings = [
            w for w in result.warnings
            if "may reference non-existent step" in w
        ]
        assert step_ref_warnings == []


# ---------------------------------------------------------------------------
# Bug 2: TriggerConfig.enabled string coercion
# ---------------------------------------------------------------------------

class TestTriggerEnabledCoercion:
    def _base_config(self) -> dict:
        return {
            "id": "t1",
            "type": "mail",
            "pipeline": "my-pipeline",
        }

    def test_enabled_string_false_is_coerced(self):
        """enabled='false' (string) must be coerced to False (bool)."""
        cfg = TriggerConfig(**{**self._base_config(), "enabled": "false"})
        assert cfg.enabled is False

    def test_enabled_string_true_is_coerced(self):
        """enabled='true' (string) must be coerced to True (bool)."""
        cfg = TriggerConfig(**{**self._base_config(), "enabled": "true"})
        assert cfg.enabled is True

    def test_enabled_string_False_uppercase_coerced(self):
        """Case-insensitive: 'False' → False."""
        cfg = TriggerConfig(**{**self._base_config(), "enabled": "False"})
        assert cfg.enabled is False

    def test_enabled_bool_false_unchanged(self):
        """Native bool False passes through unchanged."""
        cfg = TriggerConfig(**{**self._base_config(), "enabled": False})
        assert cfg.enabled is False

    def test_enabled_bool_true_unchanged(self):
        """Native bool True passes through unchanged."""
        cfg = TriggerConfig(**{**self._base_config(), "enabled": True})
        assert cfg.enabled is True

    def test_enabled_default_is_true(self):
        """Default enabled value is True."""
        cfg = TriggerConfig(**self._base_config())
        assert cfg.enabled is True


# ---------------------------------------------------------------------------
# Bug 3: progress lint should be suppressed for MCP steps
# ---------------------------------------------------------------------------

class TestProgressLintMcpExclusion:
    def test_mcp_step_with_long_timeout_no_progress_warning(self):
        """MCP steps with long timeout must NOT produce a progress lint warning."""
        pipeline = Pipeline.model_validate({
            "name": "test",
            "steps": [
                {
                    "id": "call_llm",
                    "type": "mcp",
                    "server": "openai",
                    "tool": "chat",
                    "timeout": "120s",
                }
            ],
        })
        v = PipelineValidator(lint_rules=_no_progress_rules())
        result = v.validate(pipeline)
        progress_warnings = [
            w for w in result.warnings
            if "progress" in w.lower() and "call_llm" in w
        ]
        assert progress_warnings == [], (
            f"MCP step should not get progress warning, but got: {progress_warnings}"
        )

    def test_python_step_with_long_timeout_still_warns(self):
        """Python steps with long timeout and no progress MUST still warn."""
        pipeline = Pipeline.model_validate({
            "name": "test",
            "steps": [
                {
                    "id": "crunch",
                    "type": "python",
                    "script": "run.py",
                    "timeout": "120s",
                }
            ],
        })
        v = PipelineValidator(lint_rules=_no_progress_rules())
        result = v.validate(pipeline)
        progress_warnings = [
            w for w in result.warnings
            if "progress" in w.lower() and "crunch" in w
        ]
        assert progress_warnings, (
            "Python step with long timeout should get progress warning"
        )

    def test_mcp_step_with_progress_true_no_warning(self):
        """MCP step with progress:true should obviously not warn (no regression)."""
        pipeline = Pipeline.model_validate({
            "name": "test",
            "steps": [
                {
                    "id": "call_llm",
                    "type": "mcp",
                    "server": "openai",
                    "tool": "chat",
                    "timeout": "120s",
                    "progress": True,
                }
            ],
        })
        v = PipelineValidator(lint_rules=_no_progress_rules())
        result = v.validate(pipeline)
        progress_warnings = [
            w for w in result.warnings
            if "progress" in w.lower() and "call_llm" in w
        ]
        assert progress_warnings == []
