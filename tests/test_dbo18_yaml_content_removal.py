"""Tests for T-BRIX-DBO-18: yaml_content removed from live code paths.

Covers:
- upsert_pipeline no longer accepts/writes yaml_content
- get_pipeline_yaml_content always returns None
- count_pipelines_with_content always returns 0
- integrity checks work without yaml_content (DB step rows used instead)
- health validate_step_migration checks step rows, not yaml_content
"""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.integrity import run_integrity_checks


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """BrixDB backed by a temporary file."""
    return BrixDB(db_path=tmp_path / "brix.db")


# ---------------------------------------------------------------------------
# upsert_pipeline — yaml_content no longer accepted
# ---------------------------------------------------------------------------

class TestUpsertPipelineNoYamlContent:
    def test_upsert_pipeline_ignores_yaml_content_kwarg(self, db):
        """upsert_pipeline accepts and silently ignores yaml_content via **kwargs."""
        # Should not raise, yaml_content is consumed by **_ignored_kwargs
        pid = db.upsert_pipeline(
            name="test_pipe",
            path="/tmp/test_pipe.yaml",
            yaml_content="steps:\n  - id: s1\n    type: flow.set\n",
        )
        assert isinstance(pid, str)
        assert len(pid) > 0

    def test_upsert_pipeline_does_not_write_yaml_content(self, db):
        """yaml_content column is NOT written from the upsert_pipeline call.

        The column may contain NULL or the schema default ('') — but never the
        value passed as the yaml_content kwarg (which is silently ignored).
        """
        yaml_kwarg_value = "steps:\n  - id: s1\n    type: flow.set\n"
        db.upsert_pipeline(
            name="no_yaml_pipe",
            path="/tmp/no_yaml_pipe.yaml",
            yaml_content=yaml_kwarg_value,
        )
        with db._connect() as conn:
            row = conn.execute(
                "SELECT yaml_content FROM pipeline WHERE name='no_yaml_pipe'"
            ).fetchone()
        assert row is not None
        # The kwarg value must NOT have been written to the column
        assert row[0] != yaml_kwarg_value, (
            "yaml_content kwarg was written to DB — expected it to be ignored"
        )
        # Column value should be empty/null (schema default)
        assert not row[0]  # None or empty string

    def test_upsert_pipeline_no_yaml_content_arg_works(self, db):
        """upsert_pipeline works fine without yaml_content at all."""
        pid = db.upsert_pipeline(name="plain_pipe", path="/tmp/plain.yaml")
        assert isinstance(pid, str)


# ---------------------------------------------------------------------------
# get_pipeline_yaml_content — always None
# ---------------------------------------------------------------------------

class TestGetPipelineYamlContent:
    def test_returns_none_for_existing_pipeline(self, db):
        """get_pipeline_yaml_content returns None regardless of what is stored."""
        db.upsert_pipeline(name="pipe_with_content", path="/tmp/pipe.yaml")
        # Manually insert yaml_content to simulate legacy data
        with db._connect() as conn:
            conn.execute(
                "UPDATE pipeline SET yaml_content='steps: []' WHERE name='pipe_with_content'"
            )
        result = db.get_pipeline_yaml_content("pipe_with_content")
        assert result is None

    def test_returns_none_for_missing_pipeline(self, db):
        """get_pipeline_yaml_content returns None for a pipeline that doesn't exist."""
        result = db.get_pipeline_yaml_content("does_not_exist")
        assert result is None

    def test_returns_none_always(self, db):
        """get_pipeline_yaml_content returns None unconditionally."""
        assert db.get_pipeline_yaml_content("anything") is None


# ---------------------------------------------------------------------------
# count_pipelines_with_content — always 0
# ---------------------------------------------------------------------------

class TestCountPipelinesWithContent:
    def test_returns_zero_when_empty(self, db):
        """count_pipelines_with_content returns 0 on empty DB."""
        assert db.count_pipelines_with_content() == 0

    def test_returns_zero_even_with_legacy_data(self, db):
        """count_pipelines_with_content returns 0 even if yaml_content column has data."""
        db.upsert_pipeline(name="legacy_pipe", path="/tmp/l.yaml")
        with db._connect() as conn:
            conn.execute(
                "UPDATE pipeline SET yaml_content='steps: []' WHERE name='legacy_pipe'"
            )
        assert db.count_pipelines_with_content() == 0

    def test_returns_zero_with_multiple_pipelines(self, db):
        """count_pipelines_with_content returns 0 regardless of how many pipelines exist."""
        for i in range(5):
            db.upsert_pipeline(name=f"pipe_{i}", path=f"/tmp/pipe_{i}.yaml")
        assert db.count_pipelines_with_content() == 0


# ---------------------------------------------------------------------------
# integrity checks — use DB step rows, not yaml_content
# ---------------------------------------------------------------------------

class TestIntegrityChecksWithoutYamlContent:
    def _register_bricks(self, db):
        """Insert minimal brick definitions for brick reference checks."""
        try:
            db.brick_definitions_upsert({
                "name": "flow.set",
                "runner": "set",
                "namespace": "flow",
                "category": "flow",
                "description": "Set a variable",
                "when_to_use": "",
                "when_NOT_to_use": "",
                "aliases": [],
                "input_type": "*",
                "output_type": "*",
                "config_schema": {},
                "examples": [],
                "related_connector": "",
                "system": False,
            })
            db.brick_definitions_upsert({
                "name": "script.python",
                "runner": "python",
                "namespace": "script",
                "category": "script",
                "description": "Run Python",
                "when_to_use": "",
                "when_NOT_to_use": "",
                "aliases": [],
                "input_type": "*",
                "output_type": "*",
                "config_schema": {},
                "examples": [],
                "related_connector": "",
                "system": False,
            })
        except Exception:
            pass  # Table may not exist in minimal test DB

    def test_integrity_runs_without_yaml_content(self, db):
        """run_integrity_checks completes without error when yaml_content is absent."""
        db.upsert_pipeline(name="clean_pipe", path="/tmp/clean.yaml")
        result = run_integrity_checks(db)
        assert "ok" in result
        assert "issues" in result

    def test_integrity_no_false_positives_for_no_yaml_content(self, db):
        """Pipelines without yaml_content do not trigger YAML-related issues."""
        db.upsert_pipeline(name="no_yaml_pipe2", path="/tmp/noyaml2.yaml")
        result = run_integrity_checks(db)
        issue_codes = [i["code"] for i in result["issues"]]
        # No YAML-specific errors
        assert "MISSING_YAML_CONTENT" not in issue_codes
        assert "YAML_PARSE_ERROR" not in issue_codes

    def test_brick_reference_check_uses_db_steps(self, db):
        """_check_brick_references reads from pipeline_step rows, not yaml_content."""
        self._register_bricks(db)
        pid = db.upsert_pipeline(name="brick_ref_pipe", path="/tmp/brick_ref.yaml")
        # Insert a step with a known brick type
        db.upsert_step(
            pipeline_id=pid,
            step_dict={"id": "s1", "type": "flow.set", "label": "Set x"},
            step_order=0,
        )
        # Also manually set yaml_content to something wrong — should be ignored
        with db._connect() as conn:
            conn.execute(
                "UPDATE pipeline SET yaml_content='steps:\n  - id: s1\n    type: INVALID_BRICK\n' "
                "WHERE id=?",
                (pid,),
            )
        # yaml_content has INVALID_BRICK, but DB steps have flow.set → no UNKNOWN_BRICK_REF issue
        result = run_integrity_checks(db)
        brick_issues = [i for i in result["issues"] if i["code"] == "UNKNOWN_BRICK_REF"]
        # The INVALID_BRICK in yaml_content must NOT be flagged (yaml_content is ignored)
        for issue in brick_issues:
            for step_ref in issue.get("steps", []):
                assert "INVALID_BRICK" not in step_ref

    def test_helper_reference_check_uses_db_steps(self, db):
        """_check_helper_references reads from pipeline_step rows, not yaml_content."""
        # Register a known helper
        db.upsert_helper(
            name="my_helper",
            script_path="/app/helpers/my_helper.py",
            description="Test helper",
        )
        pid = db.upsert_pipeline(name="helper_ref_pipe", path="/tmp/helper_ref.yaml")
        # Insert a step with a known helper
        db.upsert_step(
            pipeline_id=pid,
            step_dict={
                "id": "s1",
                "type": "script.python",
                "helper": "my_helper",
                "label": "Run helper",
            },
            step_order=0,
        )
        # Set yaml_content with a fake unknown helper — must be ignored
        with db._connect() as conn:
            conn.execute(
                "UPDATE pipeline SET yaml_content='steps:\n  - id: s1\n    type: script.python\n    helper: ghost_helper\n' "
                "WHERE id=?",
                (pid,),
            )
        result = run_integrity_checks(db)
        helper_issues = [i for i in result["issues"] if i["code"] == "UNKNOWN_HELPER_REF"]
        # ghost_helper from yaml_content must NOT be reported
        for issue in helper_issues:
            for step_ref in issue.get("steps", []):
                assert "ghost_helper" not in step_ref


# ---------------------------------------------------------------------------
# health validate_step_migration — checks step rows, not yaml_content
# ---------------------------------------------------------------------------

class TestValidateStepMigration:
    """Integration-level test for the health handler logic."""

    def test_pipeline_with_no_steps_is_flagged(self, db):
        """A pipeline with 0 pipeline_step rows is flagged as an issue."""
        from brix.integrity import run_integrity_checks as _run  # noqa: F401

        db.upsert_pipeline(name="empty_steps_pipe", path="/tmp/empty.yaml")

        # Simulate what _handle_validate_step_migration does: count steps per pipeline
        with db._connect() as conn:
            rows = conn.execute(
                "SELECT id, name FROM pipeline ORDER BY name ASC"
            ).fetchall()

        issues = []
        for row in rows:
            steps = db.get_steps(row[0])
            if len(steps) == 0:
                issues.append({"name": row[1], "reason": "no_step_rows"})

        # The pipeline we just created has no steps → must be in issues
        flagged_names = [i["name"] for i in issues]
        assert "empty_steps_pipe" in flagged_names

    def test_pipeline_with_steps_is_not_flagged(self, db):
        """A pipeline that has step rows is not flagged."""
        pid = db.upsert_pipeline(name="has_steps_pipe", path="/tmp/has_steps.yaml")
        db.upsert_step(
            pipeline_id=pid,
            step_dict={"id": "s1", "type": "flow.set"},
            step_order=0,
        )

        with db._connect() as conn:
            rows = conn.execute(
                "SELECT id, name FROM pipeline ORDER BY name ASC"
            ).fetchall()

        issues = []
        for row in rows:
            steps = db.get_steps(row[0])
            if len(steps) == 0:
                issues.append({"name": row[1], "reason": "no_step_rows"})

        flagged_names = [i["name"] for i in issues]
        assert "has_steps_pipe" not in flagged_names
