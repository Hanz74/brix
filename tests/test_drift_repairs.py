"""Regression tests for drift repairs."""
from __future__ import annotations

import logging

import pytest

from brix.db import BrixDB
from brix.integrity import run_integrity_checks
from brix.migrations import run_pending_migrations
from brix.pipeline_store import PipelineStore


@pytest.fixture
def drift_db(tmp_path):
    """Return an isolated DB with all migrations applied."""
    db = BrixDB(db_path=tmp_path / "drift_repairs.db")
    run_pending_migrations(db)
    return db


def test_pipeline_saved_with_zero_steps_warns_and_is_flagged(tmp_path, drift_db, caplog):
    """Saving a 0-step pipeline warns immediately and is surfaced by integrity."""
    store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=drift_db)

    with caplog.at_level(logging.WARNING):
        store.save(
            {
                "name": "zero-step-pipe",
                "description": "Pipeline with no steps",
                "steps": [],
            }
        )

    assert "saved with 0 steps" in caplog.text

    result = run_integrity_checks(drift_db)
    no_step_rows = next(issue for issue in result["issues"] if issue["code"] == "NO_STEP_ROWS")
    assert "zero-step-pipe" in no_step_rows["pipelines"]
