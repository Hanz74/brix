"""Drift detection integrity checks for legacy flat step type usage."""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.integrity import run_integrity_checks
from brix.mcp_handlers.help import _handle_get_tips
from brix.migrations import run_pending_migrations
from brix.pipeline_store import PipelineStore
from brix.bricks.registry import BrickRegistry, _row_to_brick


@pytest.fixture
def db(tmp_path):
    """Return an isolated DB with all migrations applied."""
    database = BrixDB(db_path=tmp_path / "drift_detection.db")
    run_pending_migrations(database)
    return database


def test_integrity_detects_help_and_pipeline_legacy_types(tmp_path, db):
    """Legacy flat step type names in help topics and pipeline steps are flagged."""
    db.help_topics_upsert(
        {
            "name": "legacy-help",
            "title": "Legacy Help",
            "content": (
                '{"id": "fetch", "type": "http"}\n'
                '{"id": "script", "type": "python"}\n'
                'Modern docs should use brick-specific step names instead.'
            ),
            "category": "docs",
        }
    )

    store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db)
    store.save(
        {
            "name": "legacy-pipeline",
            "description": "Pipeline with a legacy flat step type",
            "project": "utility",
            "tags": ["one-shot"],
            "steps": [
                {
                    "id": "s1",
                    "type": "python",
                    "script": "print('hello')",
                }
            ],
        }
    )

    result = run_integrity_checks(db)

    help_issue = next(issue for issue in result["issues"] if issue["code"] == "HELP_LEGACY_TYPE")
    assert help_issue["severity"] == "warning"
    assert "legacy-help:http,python" in help_issue["topics"]

    pipeline_issue = next(issue for issue in result["issues"] if issue["code"] == "PIPELINE_LEGACY_TYPE")
    assert pipeline_issue["severity"] == "warning"
    assert "legacy-pipeline/s1:python" in pipeline_issue["steps"]

    semantic_issue = next(issue for issue in result["issues"] if issue["code"] == "SEMANTIC_RAW_EFFECTIVE_DRIFT")
    assert semantic_issue["severity"] == "warning"
    assert "legacy-pipeline/s1:script.python:legacy_alias:python" in semantic_issue["steps"]


def test_integrity_detects_raw_effective_config_precedence_drift(tmp_path, db):
    """Config-vs-top-level conflicts are visible as semantic parity drift."""
    store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db)
    store.save(
        {
            "name": "config-drift-pipeline",
            "project": "utility",
            "tags": ["one-shot"],
            "steps": [
                {
                    "id": "fetch",
                    "type": "http.request",
                    "url": "https://top-level.example",
                    "config": {
                        "url": "https://config.example",
                    },
                }
            ],
        }
    )

    result = run_integrity_checks(db)

    semantic_issue = next(issue for issue in result["issues"] if issue["code"] == "SEMANTIC_RAW_EFFECTIVE_DRIFT")
    assert "config-drift-pipeline/fetch:http.request:config_precedence:url" in semantic_issue["steps"]


def test_integrity_detects_materialized_step_schema_mismatch(tmp_path, db):
    """Brick schema is checked against the materialized effective step shape."""
    db.brick_definitions_upsert({
        "name": "custom.required",
        "runner": "python",
        "namespace": "custom",
        "category": "custom",
        "description": "Requires a field",
        "when_to_use": "testing schema drift",
        "when_NOT_to_use": "",
        "aliases": [],
        "input_type": "*",
        "output_type": "*",
        "config_schema": {
            "required_field": {
                "type": "string",
                "description": "Required test field",
                "required": True,
            }
        },
        "examples": [],
        "related_connector": "",
        "system": False,
    })

    store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db)
    store.save(
        {
            "name": "schema-drift-pipeline",
            "project": "utility",
            "tags": ["one-shot"],
            "steps": [{"id": "custom", "type": "custom.required", "config": {}}],
        }
    )

    result = run_integrity_checks(db)

    schema_issue = next(issue for issue in result["issues"] if issue["code"] == "SEMANTIC_SCHEMA_MISMATCH")
    assert schema_issue["severity"] == "warning"
    assert "schema-drift-pipeline/custom:custom.required:required_field:required" in schema_issue["steps"]


def test_integrity_ignores_dynamic_jinja_values_for_schema_enum(tmp_path, db):
    """Dynamic Jinja enum values should not surface as semantic schema mismatch."""
    store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db)
    store.save(
        {
            "name": "jinja-enum-pipeline",
            "project": "utility",
            "tags": ["one-shot"],
            "steps": [
                {
                    "id": "notify",
                    "type": "action.notify",
                    "params": {
                        "channel": "{{ var.notification_conditional | default('slack') }}",
                        "message": "Hello",
                    },
                }
            ],
        }
    )

    result = run_integrity_checks(db)

    mismatches = [issue for issue in result["issues"] if issue["code"] == "SEMANTIC_SCHEMA_MISMATCH"]
    assert all("jinja-enum-pipeline/notify:action.notify:channel:enum" not in issue["steps"] for issue in mismatches)


@pytest.mark.asyncio
async def test_get_tips_surfaces_help_legacy_type_integrity_issue(tmp_path, db, monkeypatch):
    db.help_topics_upsert(
        {
            "name": "legacy-help",
            "title": "Legacy Help",
            "content": '{"id": "fetch", "type": "http"}',
            "category": "docs",
        }
    )

    monkeypatch.setattr("brix.db.BRIX_DB_PATH", db.db_path)
    monkeypatch.setattr("brix.mcp_handlers.help._pipeline_dir", lambda: tmp_path)

    result = await _handle_get_tips({})

    tips_text = "\n".join(result["tips"])
    assert "[HELP_LEGACY_TYPE]" in tips_text


def test_row_to_brick_accepts_legacy_python_literal_payloads():
    brick = _row_to_brick(
        {
            "name": "legacy.literal",
            "runner": "python",
            "namespace": "legacy",
            "category": "custom",
            "description": "Legacy literal payload brick",
            "when_to_use": "Testing",
            "when_NOT_to_use": "",
            "aliases": "['legacy', 'literal']",
            "config_schema": "{'path': 'string', 'mode': {'type': 'string', 'enum': ['copy', 'move']}}",
            "examples": "[{'config': {'path': '/tmp/x'}}]",
            "input_type": "none",
            "output_type": "dict",
            "system": 0,
        }
    )

    assert brick.aliases == ["legacy", "literal"]
    assert brick.config_schema["path"].type == "string"
    assert brick.config_schema["mode"].enum == ["copy", "move"]
    assert isinstance(brick.examples, list)


def test_brick_registry_lists_legacy_literal_custom_bricks(tmp_path, db):
    db.brick_definitions_upsert(
        {
            "name": "legacy.literal",
            "runner": "python",
            "namespace": "legacy",
            "category": "custom",
            "description": "Legacy literal payload brick",
            "when_to_use": "Testing",
            "when_NOT_to_use": "",
            "aliases": ["legacy"],
            "input_type": "none",
            "output_type": "dict",
            "config_schema": {"path": "string"},
            "examples": [{"config": {"path": "/tmp/x"}}],
            "system": False,
        }
    )
    with db._connect() as conn:  # type: ignore[attr-defined]
        conn.execute(
            "UPDATE brick_definition SET aliases=?, config_schema=?, examples=? WHERE name=?",
            (
                "['legacy']",
                "{'path': 'string'}",
                "[{'config': {'path': '/tmp/x'}}]",
                "legacy.literal",
            ),
        )
        conn.commit()

    registry = BrickRegistry(db=db)
    listed = {brick.name: brick for brick in registry.list_all()}

    assert "legacy.literal" in listed
    assert listed["legacy.literal"].config_schema["path"].type == "string"
