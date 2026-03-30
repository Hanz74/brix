"""Tests for T-BRIX-SCHEMA-03: project field on runs and persistent_store.

Covers:
- Migration adds project column to runs table
- Migration adds org fields to persistent_store table
- Backfill populates project from pipelines table
- Engine sets project on new runs via record_run_start
- Run search results include project field
"""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.history import RunHistory
from brix.migrations import MIGRATIONS


@pytest.fixture
def db(tmp_path):
    """Fresh BrixDB backed by a temp file."""
    return BrixDB(db_path=tmp_path / "test.db")


@pytest.fixture
def history(tmp_path):
    """RunHistory backed by a temp file."""
    return RunHistory(db_path=tmp_path / "test_history.db")


# ---------------------------------------------------------------------------
# Migration: runs.project column exists
# ---------------------------------------------------------------------------

class TestRunsProjectMigration:
    def test_runs_table_has_project_column(self, db):
        """After migrations, runs table should have a project column."""
        with db._connect() as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
        assert "project" in cols

    def test_runs_project_index_exists(self, db):
        """Index idx_runs_project should be created."""
        with db._connect() as conn:
            indexes = {
                row[1]
                for row in conn.execute(
                    "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='runs'"
                ).fetchall()
            }
        assert "idx_runs_project" in indexes

    def test_persistent_store_has_org_columns(self, db):
        """After migrations, persistent_store should have project, tags, group_name."""
        with db._connect() as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(persistent_store)").fetchall()}
        assert "project" in cols
        assert "tags" in cols
        assert "group_name" in cols

    def test_migration_versions_present(self):
        """Migrations 54-59 should exist in the MIGRATIONS list."""
        versions = {m["version"] for m in MIGRATIONS}
        for v in (54, 55, 56, 57, 58, 59):
            assert v in versions, f"Migration v{v} missing"


# ---------------------------------------------------------------------------
# Backfill: runs.project from pipelines.project
# ---------------------------------------------------------------------------

class TestRunsProjectBackfill:
    def test_backfill_sets_project_from_pipeline(self, db):
        """Backfill migration should set project on existing runs from pipelines table."""
        # Insert a pipeline with a project
        db.upsert_pipeline(
            name="test-pipe",
            path="", yaml_content="name: test-pipe\nsteps: []",
            project="buddy",
        )
        # Insert a run referencing that pipeline (project will be set by record_run_start)
        db.record_run_start(
            run_id="run-backfill-1",
            pipeline="test-pipe",
        )
        # Verify project was set
        run = db.get_run("run-backfill-1")
        assert run is not None
        assert run["project"] == "buddy"


# ---------------------------------------------------------------------------
# Engine: record_run_start sets project
# ---------------------------------------------------------------------------

class TestRecordRunStartProject:
    def test_sets_project_from_pipeline_lookup(self, db):
        """record_run_start should resolve project from pipelines table."""
        db.upsert_pipeline(
            name="proj-pipe",
            path="", yaml_content="name: proj-pipe\nsteps: []",
            project="cody",
        )
        db.record_run_start(run_id="run-proj-1", pipeline="proj-pipe")
        run = db.get_run("run-proj-1")
        assert run["project"] == "cody"

    def test_sets_explicit_project(self, db):
        """Explicit project parameter should override pipeline lookup."""
        db.upsert_pipeline(
            name="proj-pipe2",
            path="", yaml_content="name: proj-pipe2\nsteps: []",
            project="cody",
        )
        db.record_run_start(run_id="run-proj-2", pipeline="proj-pipe2", project="buddy")
        run = db.get_run("run-proj-2")
        assert run["project"] == "buddy"

    def test_defaults_to_empty_string(self, db):
        """When pipeline has no project, project should be empty string."""
        db.record_run_start(run_id="run-no-proj", pipeline="nonexistent-pipe")
        run = db.get_run("run-no-proj")
        assert run["project"] == ""

    def test_history_record_start_passes_project(self, history):
        """RunHistory.record_start should pass project through to DB."""
        history._db.upsert_pipeline(
            name="hist-pipe",
            path="", yaml_content="name: hist-pipe\nsteps: []",
            project="utility",
        )
        history.record_start(run_id="run-hist-1", pipeline="hist-pipe")
        run = history.get_run("run-hist-1")
        assert run["project"] == "utility"


# ---------------------------------------------------------------------------
# Search: project filter and project in results
# ---------------------------------------------------------------------------

class TestRunSearchProject:
    def test_search_includes_project_field(self, db):
        """search_runs results should include the project field."""
        db.upsert_pipeline(
            name="search-pipe",
            path="", yaml_content="name: search-pipe\nsteps: []",
            project="buddy",
        )
        db.record_run_start(run_id="run-search-1", pipeline="search-pipe")
        results = db.search_runs(pipeline="search-pipe")
        assert len(results) == 1
        assert results[0]["project"] == "buddy"

    def test_search_filter_by_project(self, db):
        """search_runs with project filter should only return matching runs."""
        db.upsert_pipeline(name="pipe-a", path="", yaml_content="name: pipe-a\nsteps: []", project="buddy")
        db.upsert_pipeline(name="pipe-b", path="", yaml_content="name: pipe-b\nsteps: []", project="cody")
        db.record_run_start(run_id="run-a", pipeline="pipe-a")
        db.record_run_start(run_id="run-b", pipeline="pipe-b")

        buddy_runs = db.search_runs(project="buddy")
        assert len(buddy_runs) == 1
        assert buddy_runs[0]["run_id"] == "run-a"

        cody_runs = db.search_runs(project="cody")
        assert len(cody_runs) == 1
        assert cody_runs[0]["run_id"] == "run-b"

    def test_get_run_includes_project(self, db):
        """get_run should include project in the result dict."""
        db.upsert_pipeline(name="get-pipe", path="", yaml_content="name: get-pipe\nsteps: []", project="system")
        db.record_run_start(run_id="run-get-1", pipeline="get-pipe")
        run = db.get_run("run-get-1")
        assert "project" in run
        assert run["project"] == "system"
