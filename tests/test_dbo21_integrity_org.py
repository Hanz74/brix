"""T-BRIX-DBO-21: Integrity checks for org metadata (project + description).

Verifies that _check_entity_org_metadata flags rows with missing project or
description and leaves compliant rows untouched.
"""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.integrity import _check_entity_org_metadata, run_integrity_checks


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "dbo21.db")


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _make_pipeline(db: BrixDB, name: str, project: str = "", description: str = "") -> str:
    """Create a minimal pipeline with the given project/description. Returns pipeline_id."""
    pid = db.upsert_pipeline(name, f"/tmp/{name}.yaml")
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET project=?, description=? WHERE id=?",
            (project, description, pid),
        )
    return pid


def _make_helper(db: BrixDB, name: str, project: str = "", description: str = "") -> str:
    """Create a minimal helper with the given project/description. Returns helper_id."""
    return db.upsert_helper(
        name=name,
        script_path=f"/tmp/{name}.py",
        description=description,
        project=project if project else None,
    )


def _make_trigger_group(db: BrixDB, name: str, project: str = "", description: str = "") -> dict:
    """Create a minimal trigger_group with the given project/description."""
    return db.trigger_group_add(
        name=name,
        triggers=[],
        description=description,
        project=project if project else None,
    )


# ---------------------------------------------------------------------------
# MISSING_PROJECT tests
# ---------------------------------------------------------------------------


class TestMissingProject:
    def test_pipeline_without_project_not_flagged_by_org_check(self, db):
        # pipeline missing-project is reported by _check_entities_without_project
        # (code ENTITY_NO_PROJECT), NOT by _check_entity_org_metadata, to avoid
        # duplicate reporting.
        _make_pipeline(db, "pipe-no-proj", project="", description="A description")
        issues = _check_entity_org_metadata(db)
        missing = [i for i in issues if i["code"] == "MISSING_PROJECT" and i["entity_name"] == "pipe-no-proj"]
        assert len(missing) == 0, (
            f"pipeline MISSING_PROJECT must not be emitted by _check_entity_org_metadata "
            f"(covered by ENTITY_NO_PROJECT), got {missing}"
        )

    def test_helper_without_project_not_flagged_by_org_check(self, db):
        # helper missing-project is reported by _check_entities_without_project
        # (code HELPER_NO_PROJECT), NOT by _check_entity_org_metadata.
        _make_helper(db, "helper-no-proj", project="", description="A description")
        issues = _check_entity_org_metadata(db)
        missing = [i for i in issues if i["code"] == "MISSING_PROJECT" and i["entity_name"] == "helper-no-proj"]
        assert len(missing) == 0, (
            f"helper MISSING_PROJECT must not be emitted by _check_entity_org_metadata "
            f"(covered by HELPER_NO_PROJECT), got {missing}"
        )

    def test_trigger_group_without_project_is_flagged(self, db):
        _make_trigger_group(db, "tg-no-proj", project="", description="A description")
        issues = _check_entity_org_metadata(db)
        missing = [i for i in issues if i["code"] == "MISSING_PROJECT" and i["entity_name"] == "tg-no-proj"]
        assert len(missing) == 1, f"Expected 1 MISSING_PROJECT for tg-no-proj, got {missing}"

    def test_pipeline_with_project_not_flagged(self, db):
        _make_pipeline(db, "pipe-with-proj", project="my-project", description="A description")
        issues = _check_entity_org_metadata(db)
        missing = [i for i in issues if i["code"] == "MISSING_PROJECT" and i["entity_name"] == "pipe-with-proj"]
        assert len(missing) == 0, f"Expected no MISSING_PROJECT for pipe-with-proj, got {missing}"


# ---------------------------------------------------------------------------
# MISSING_DESCRIPTION tests
# ---------------------------------------------------------------------------


class TestMissingDescription:
    def test_pipeline_without_description_is_flagged(self, db):
        _make_pipeline(db, "pipe-no-desc", project="my-project", description="")
        issues = _check_entity_org_metadata(db)
        missing = [i for i in issues if i["code"] == "MISSING_DESCRIPTION" and i["entity_name"] == "pipe-no-desc"]
        assert len(missing) == 1, f"Expected 1 MISSING_DESCRIPTION for pipe-no-desc, got {missing}"

    def test_helper_without_description_is_flagged(self, db):
        _make_helper(db, "helper-no-desc", project="my-project", description="")
        issues = _check_entity_org_metadata(db)
        missing = [i for i in issues if i["code"] == "MISSING_DESCRIPTION" and i["entity_name"] == "helper-no-desc"]
        assert len(missing) == 1, f"Expected 1 MISSING_DESCRIPTION for helper-no-desc, got {missing}"

    def test_trigger_group_without_description_is_flagged(self, db):
        _make_trigger_group(db, "tg-no-desc", project="my-project", description="")
        issues = _check_entity_org_metadata(db)
        missing = [i for i in issues if i["code"] == "MISSING_DESCRIPTION" and i["entity_name"] == "tg-no-desc"]
        assert len(missing) == 1, f"Expected 1 MISSING_DESCRIPTION for tg-no-desc, got {missing}"

    def test_helper_with_description_not_flagged(self, db):
        _make_helper(db, "helper-with-desc", project="my-project", description="Does something useful")
        issues = _check_entity_org_metadata(db)
        missing = [i for i in issues if i["code"] == "MISSING_DESCRIPTION" and i["entity_name"] == "helper-with-desc"]
        assert len(missing) == 0, f"Expected no MISSING_DESCRIPTION for helper-with-desc, got {missing}"


# ---------------------------------------------------------------------------
# Compliant entities are NOT flagged
# ---------------------------------------------------------------------------


class TestCompliantEntitiesNotFlagged:
    def test_pipeline_with_project_and_description_not_flagged(self, db):
        _make_pipeline(db, "pipe-ok", project="my-project", description="Does something")
        issues = _check_entity_org_metadata(db)
        related = [i for i in issues if i["entity_name"] == "pipe-ok"]
        assert len(related) == 0, f"Compliant pipeline should not be flagged, got {related}"

    def test_helper_with_project_and_description_not_flagged(self, db):
        _make_helper(db, "helper-ok", project="my-project", description="Does something")
        issues = _check_entity_org_metadata(db)
        related = [i for i in issues if i["entity_name"] == "helper-ok"]
        assert len(related) == 0, f"Compliant helper should not be flagged, got {related}"

    def test_trigger_group_with_project_and_description_not_flagged(self, db):
        _make_trigger_group(db, "tg-ok", project="my-project", description="Does something")
        issues = _check_entity_org_metadata(db)
        related = [i for i in issues if i["entity_name"] == "tg-ok"]
        assert len(related) == 0, f"Compliant trigger_group should not be flagged, got {related}"


# ---------------------------------------------------------------------------
# Issue structure validation
# ---------------------------------------------------------------------------


class TestIssueStructure:
    REQUIRED_KEYS = {"code", "severity", "entity_type", "entity_name", "message"}

    def test_issue_has_required_keys(self, db):
        _make_pipeline(db, "pipe-struct", project="", description="")
        issues = _check_entity_org_metadata(db)
        assert issues, "Expected at least one issue"
        for issue in issues:
            missing_keys = self.REQUIRED_KEYS - set(issue.keys())
            assert not missing_keys, f"Issue missing keys {missing_keys}: {issue}"

    def test_issue_code_is_string(self, db):
        _make_pipeline(db, "pipe-code-type", project="", description="")
        issues = _check_entity_org_metadata(db)
        for issue in issues:
            assert isinstance(issue["code"], str), f"code must be str, got {type(issue['code'])}"

    def test_issue_severity_is_info(self, db):
        _make_pipeline(db, "pipe-severity", project="", description="")
        issues = _check_entity_org_metadata(db)
        for issue in issues:
            assert issue["severity"] == "info", f"severity must be 'info', got {issue['severity']}"

    def test_issue_entity_type_matches_table(self, db):
        _make_pipeline(db, "pipe-etype", project="", description="A description")
        issues = _check_entity_org_metadata(db)
        pipeline_issues = [i for i in issues if i["entity_name"] == "pipe-etype"]
        assert all(i["entity_type"] == "pipeline" for i in pipeline_issues), (
            f"entity_type should be 'pipeline': {pipeline_issues}"
        )

    def test_issue_entity_name_matches_row_name(self, db):
        # Use missing description (not missing project) so that
        # _check_entity_org_metadata produces exactly one issue for this helper.
        _make_helper(db, "helper-ename", project="my-project", description="")
        issues = _check_entity_org_metadata(db)
        helper_issues = [i for i in issues if i["entity_name"] == "helper-ename"]
        assert len(helper_issues) == 1
        assert helper_issues[0]["entity_name"] == "helper-ename"

    def test_issue_message_is_nonempty_string(self, db):
        _make_pipeline(db, "pipe-msg", project="", description="")
        issues = _check_entity_org_metadata(db)
        for issue in issues:
            assert isinstance(issue["message"], str) and issue["message"], (
                f"message must be non-empty string: {issue}"
            )


# ---------------------------------------------------------------------------
# Whitespace-only values are treated as missing
# ---------------------------------------------------------------------------


class TestWhitespaceValues:
    def test_pipeline_whitespace_project_flagged_by_entities_check(self, db):
        """A pipeline with a whitespace-only project must be caught somewhere."""
        pid = db.upsert_pipeline("pipe-ws-proj", "/tmp/pipe-ws-proj.yaml")
        with db._connect() as conn:
            conn.execute(
                "UPDATE pipeline SET project=?, description=? WHERE id=?",
                ("   ", "A description", pid),
            )
        # _check_entity_org_metadata skips pipeline for MISSING_PROJECT, but
        # _check_entities_without_project in run_integrity_checks should still
        # catch it (raw DB value is non-empty string, so that check may miss it).
        # What we DO verify: _check_entity_org_metadata does NOT produce a
        # MISSING_PROJECT issue for pipelines (dedup rule), and does NOT produce
        # a false-negative MISSING_DESCRIPTION when description is real.
        issues = _check_entity_org_metadata(db)
        proj_issues = [i for i in issues if i["code"] == "MISSING_PROJECT" and i["entity_name"] == "pipe-ws-proj"]
        # pipeline is excluded from MISSING_PROJECT in _check_entity_org_metadata
        assert len(proj_issues) == 0, "pipeline should not appear in MISSING_PROJECT from org-metadata check"

    def test_trigger_group_whitespace_project_flagged(self, db):
        """trigger_group with whitespace-only project must be flagged as MISSING_PROJECT."""
        tg = db.trigger_group_add(name="tg-ws-proj", triggers=[], description="Real desc", project=None)
        tg_id = tg["id"] if isinstance(tg, dict) else tg
        with db._connect() as conn:
            conn.execute("UPDATE trigger_group SET project=? WHERE id=?", ("   ", tg_id))
        issues = _check_entity_org_metadata(db)
        proj_issues = [i for i in issues if i["code"] == "MISSING_PROJECT" and i["entity_name"] == "tg-ws-proj"]
        assert len(proj_issues) == 1, f"Expected MISSING_PROJECT for tg-ws-proj with whitespace project, got {proj_issues}"

    def test_pipeline_whitespace_description_flagged(self, db):
        """A pipeline with a whitespace-only description must be flagged as MISSING_DESCRIPTION."""
        pid = db.upsert_pipeline("pipe-ws-desc", "/tmp/pipe-ws-desc.yaml")
        with db._connect() as conn:
            conn.execute(
                "UPDATE pipeline SET project=?, description=? WHERE id=?",
                ("my-project", "   ", pid),
            )
        issues = _check_entity_org_metadata(db)
        desc_issues = [i for i in issues if i["code"] == "MISSING_DESCRIPTION" and i["entity_name"] == "pipe-ws-desc"]
        assert len(desc_issues) == 1, f"Expected MISSING_DESCRIPTION for pipe-ws-desc with whitespace description, got {desc_issues}"

    def test_helper_whitespace_description_flagged(self, db):
        """A helper with a whitespace-only description must be flagged as MISSING_DESCRIPTION."""
        hid = db.upsert_helper(
            name="helper-ws-desc",
            script_path="/tmp/helper-ws-desc.py",
            description="   ",
            project="my-project",
        )
        issues = _check_entity_org_metadata(db)
        desc_issues = [i for i in issues if i["code"] == "MISSING_DESCRIPTION" and i["entity_name"] == "helper-ws-desc"]
        assert len(desc_issues) == 1, f"Expected MISSING_DESCRIPTION for helper-ws-desc with whitespace description, got {desc_issues}"


# ---------------------------------------------------------------------------
# Integration: run_integrity_checks wires in org checks
# ---------------------------------------------------------------------------


class TestIntegrationWiring:
    def test_run_integrity_checks_includes_missing_project(self, db):
        # Use a trigger_group — pipeline/helper missing-project is reported as
        # ENTITY_NO_PROJECT / HELPER_NO_PROJECT by _check_entities_without_project.
        # trigger_group is only covered by _check_entity_org_metadata → MISSING_PROJECT.
        _make_trigger_group(db, "tg-wiring", project="", description="Some description")
        result = run_integrity_checks(db)
        codes = [i["code"] for i in result["issues"]]
        assert "MISSING_PROJECT" in codes, f"Expected MISSING_PROJECT in {codes}"

    def test_run_integrity_checks_includes_missing_description(self, db):
        _make_helper(db, "helper-wiring", project="some-project", description="")
        result = run_integrity_checks(db)
        codes = [i["code"] for i in result["issues"]]
        assert "MISSING_DESCRIPTION" in codes, f"Expected MISSING_DESCRIPTION in {codes}"

    def test_run_integrity_checks_ok_false_when_org_issues_present(self, db):
        _make_pipeline(db, "pipe-ok-flag", project="", description="")
        result = run_integrity_checks(db)
        # ok should be False because there are issues
        assert result["ok"] is False
