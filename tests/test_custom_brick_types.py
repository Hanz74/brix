"""Tests for T-BRIX-CUSTOM-TYPES: Step.type accepts custom brick names.

Covers:
- Step with custom brick type (e.g. 'extract.iban') validates without Pydantic error
- Step with built-in type (e.g. 'db.query') still works
- Pipeline with custom brick type passes validate_pipeline (no errors on type field)
- Empty/whitespace-only type is rejected
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from brix.models import Step, Pipeline


# ---------------------------------------------------------------------------
# Step model — type field validation
# ---------------------------------------------------------------------------


def test_step_custom_type_accepted():
    """Step with custom brick type 'extract.iban' must not raise ValidationError."""
    step = Step(id="s1", type="extract.iban")
    assert step.type == "extract.iban"


def test_step_builtin_type_accepted():
    """Step with built-in type 'db.query' still works."""
    step = Step(id="s1", type="db.query")
    assert step.type == "db.query"


def test_step_dot_notation_custom_types():
    """Various custom dot-notation types are accepted."""
    for custom_type in ["extract.iban", "action.custom", "my_company.send_invoice", "x.y.z"]:
        step = Step(id="s1", type=custom_type)
        assert step.type == custom_type


def test_step_legacy_flat_types_accepted():
    """Legacy flat types (without dot) are accepted — deprecated but valid.

    Note: 'mcp' type requires server+tool; skip it here as that's a content
    validation rule, not a type-acceptance issue.
    """
    for legacy_type in ["python", "http", "filter", "transform", "db_query"]:
        step = Step(id="s1", type=legacy_type)
        assert step.type == legacy_type


def test_step_type_whitespace_stripped():
    """Leading/trailing whitespace is stripped from type."""
    step = Step(id="s1", type="  extract.iban  ")
    assert step.type == "extract.iban"


def test_step_type_empty_string_rejected():
    """Empty string type raises ValidationError."""
    with pytest.raises(ValidationError):
        Step(id="s1", type="")


def test_step_type_whitespace_only_rejected():
    """Whitespace-only type raises ValidationError."""
    with pytest.raises(ValidationError):
        Step(id="s1", type="   ")


# ---------------------------------------------------------------------------
# Pipeline with custom brick type — validate_pipeline
# ---------------------------------------------------------------------------


def _make_pipeline_with_step_type(step_type: str) -> Pipeline:
    """Helper: build a minimal Pipeline with one step of the given type."""
    return Pipeline(
        name="test-pipeline",
        steps=[
            Step(id="step1", type=step_type),
        ],
    )


def test_pipeline_custom_brick_type_no_pydantic_error():
    """Pipeline with custom brick type 'extract.iban' parses without Pydantic error."""
    pipeline = _make_pipeline_with_step_type("extract.iban")
    assert pipeline.steps[0].type == "extract.iban"


def test_pipeline_builtin_type_no_pydantic_error():
    """Pipeline with built-in type 'db.query' parses without Pydantic error."""
    pipeline = _make_pipeline_with_step_type("db.query")
    assert pipeline.steps[0].type == "db.query"


def test_step_conflict_key_list_accepted():
    """db.upsert steps may use a composite conflict_key list."""
    step = Step(id="upsert1", type="db.upsert", conflict_key=["person_name", "event_type"])
    assert step.conflict_key == ["person_name", "event_type"]


def test_pipeline_custom_brick_type_passes_validate_pipeline():
    """validate_pipeline on a pipeline with custom brick type does not error on type."""
    from brix.validator import PipelineValidator

    pipeline = _make_pipeline_with_step_type("extract.iban")
    validator = PipelineValidator(lint_rules=[])  # skip loading lint rules from disk
    result = validator.validate(pipeline, level="quick")

    # Validation must not report errors about the step type being unknown
    type_errors = [e for e in result.errors if "type" in e.lower() and "extract.iban" in e]
    assert not type_errors, f"Unexpected type errors: {type_errors}"


def test_pipeline_builtin_type_passes_validate_pipeline():
    """validate_pipeline on a pipeline with built-in type does not error on type."""
    from brix.validator import PipelineValidator

    pipeline = _make_pipeline_with_step_type("db.query")
    validator = PipelineValidator(lint_rules=[])
    result = validator.validate(pipeline, level="quick")

    type_errors = [e for e in result.errors if "type" in e.lower() and "db.query" in e]
    assert not type_errors, f"Unexpected type errors: {type_errors}"
