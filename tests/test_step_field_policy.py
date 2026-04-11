"""Tests for runner-specific top-level Step field migration policy."""

from __future__ import annotations

from brix.materialize import materialize_step
from brix.models import Pipeline, Step
from brix.step_field_policy import (
    explicit_runner_specific_fields,
    get_field_migration_policy,
    is_runner_specific_top_level_field,
    list_field_migration_policies,
)
from brix.validator import PipelineValidator


def test_policy_catalog_identifies_runner_specific_fields() -> None:
    fields = {policy.field for policy in list_field_migration_policies()}

    assert {"query", "connection", "server", "tool", "script", "pipeline"}.issubset(fields)
    assert is_runner_specific_top_level_field("query") is True
    assert is_runner_specific_top_level_field("when") is False
    assert get_field_migration_policy("query").canonical_home == "config.query"  # type: ignore[union-attr]


def test_explicit_runner_specific_fields_ignore_pydantic_defaults() -> None:
    step = Step(id="load", type="db.query", config={"connection": "main", "query": "SELECT 1"})

    assert explicit_runner_specific_fields(step) == {}


def test_explicit_runner_specific_fields_capture_compatibility_inputs() -> None:
    step = Step(id="load", type="db.query", connection="main", query="SELECT 1")

    assert explicit_runner_specific_fields(step) == {
        "connection": "main",
        "query": "SELECT 1",
    }


def test_explicit_runner_specific_fields_ignore_db_mirrored_defaults_and_config() -> None:
    step = Step(
        id="load",
        type="db.query",
        method="GET",
        approval_timeout="24h",
        on_timeout="stop",
        max_iterations=100,
        fetch_all_pages=False,
        persist=False,
        connection="main",
        query="SELECT 1",
        config={"connection": "main", "query": "SELECT 1"},
    )

    assert explicit_runner_specific_fields(step) == {}


def test_materialized_step_surfaces_runner_specific_policy_flags() -> None:
    materialized = materialize_step(Step(id="call", type="mcp.call", server="cody", tool="get_tips"))

    assert materialized.policy_flags["has_runner_specific_top_level_fields"] is True
    assert materialized.provenance["runner_specific_top_level_fields"] == ("server", "tool")


def test_validator_informs_on_runner_specific_top_level_fields() -> None:
    result = PipelineValidator().validate(
        Pipeline(
            name="field-policy",
            steps=[
                Step(id="load", type="db.query", connection="main", query="SELECT 1"),
            ],
        ),
        level="standard",
    )

    assert any(finding.code == "RUNNER_TOP_LEVEL_FIELD_COMPAT" for finding in result.findings)
    assert any("config.query" in info for info in result.infos)


def test_validator_does_not_warn_when_runner_fields_are_in_config() -> None:
    result = PipelineValidator().validate(
        Pipeline(
            name="field-policy",
            steps=[
                Step(id="load", type="db.query", config={"connection": "main", "query": "SELECT 1"}),
            ],
        ),
        level="standard",
    )

    assert not any(finding.code == "RUNNER_TOP_LEVEL_FIELD_COMPAT" for finding in result.findings)


def test_validator_ignores_db_mirrored_runner_defaults() -> None:
    result = PipelineValidator().validate(
        Pipeline(
            name="field-policy",
            steps=[
                Step(
                    id="load",
                    type="db.query",
                    method="GET",
                    approval_timeout="24h",
                    on_timeout="stop",
                    max_iterations=100,
                    fetch_all_pages=False,
                    persist=False,
                    connection="main",
                    query="SELECT 1",
                    config={"connection": "main", "query": "SELECT 1"},
                ),
            ],
        ),
        level="standard",
    )

    assert not any(finding.code == "RUNNER_TOP_LEVEL_FIELD_COMPAT" for finding in result.findings)
