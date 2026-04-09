"""Drift detection integrity checks for legacy flat step type usage."""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.integrity import run_integrity_checks
from brix.migrations import run_pending_migrations
from brix.pipeline_store import PipelineStore


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
