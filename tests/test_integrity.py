"""Tests for DB integrity checks — T-BRIX-INT-01."""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations
from brix.integrity import run_integrity_checks


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Isolated BrixDB with all migrations applied."""
    d = BrixDB(db_path=tmp_path / "integrity_test.db")
    run_pending_migrations(d)
    return d


# ---------------------------------------------------------------------------
# 1. Clean DB → ok=True, no issues
# ---------------------------------------------------------------------------

def test_empty_db_is_ok(db):
    result = run_integrity_checks(db)
    assert result["ok"] is True
    assert result["issues"] == []
    assert result["auto_fixed"] == []


# ---------------------------------------------------------------------------
# 2. Pipeline without normalized step rows → reported as DB issue
# ---------------------------------------------------------------------------

def test_pipeline_without_step_rows_reported(db):
    db.upsert_pipeline(name="no-step-rows-pipe", path="/tmp/no-step-rows-pipe.yaml")

    result = run_integrity_checks(db)

    codes = [i["code"] for i in result["issues"]]
    assert "NO_STEP_ROWS" in codes
    assert "PIPELINE_NO_YAML" not in codes
    assert result["ok"] is False


# ---------------------------------------------------------------------------
# 3. File mirror does not suppress missing DB step rows
# ---------------------------------------------------------------------------

def test_pipeline_file_mirror_does_not_autofix_db_truth(tmp_path, monkeypatch, db):
    import brix.integrity as int_mod

    yaml_dir = tmp_path / "pipelines"
    yaml_dir.mkdir()
    pipe_file = yaml_dir / "mirror-only-pipe.yaml"
    pipe_file.write_text(
        "name: mirror-only-pipe\nsteps:\n  - id: s1\n    type: script.python\n    script: pass\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(int_mod, "_PIPELINE_SEARCH_PATHS", [yaml_dir])
    db.upsert_pipeline(name="mirror-only-pipe", path=str(pipe_file))

    result = run_integrity_checks(db)

    codes = [i["code"] for i in result["issues"]]
    assert "NO_STEP_ROWS" in codes
    assert not any("mirror-only-pipe" in f for f in result["auto_fixed"]), result
    assert db.get_pipeline_yaml_content("mirror-only-pipe") is None


# ---------------------------------------------------------------------------
# 4. Test pipeline in DB → auto-deleted
# ---------------------------------------------------------------------------

def test_test_pipeline_auto_deleted(db):
    # Insert a test pipeline (prefixed with "test")
    db.upsert_pipeline(
        name="test-my-bad-pipe",
        path="/tmp/test-my-bad-pipe.yaml",
        yaml_content="name: test-my-bad-pipe\nsteps: []\n",
    )

    result = run_integrity_checks(db)

    assert any("test-my-bad-pipe" in f for f in result["auto_fixed"]), result
    # Pipeline should be gone from DB
    assert db.get_pipeline("test-my-bad-pipe") is None


# ---------------------------------------------------------------------------
# 5. Pipeline with project='' → ENTITY_NO_PROJECT reported
# ---------------------------------------------------------------------------

def test_entity_without_project_reported(db):
    db.upsert_pipeline(
        name="untagged-pipe",
        path="/tmp/untagged.yaml",
        yaml_content="name: untagged-pipe\nsteps:\n  - id: s1\n    type: mcp.call\n    server: x\n    tool: y\n",
    )

    result = run_integrity_checks(db)

    codes = [i["code"] for i in result["issues"]]
    assert "ENTITY_NO_PROJECT" in codes


# ---------------------------------------------------------------------------
# 6. Orphaned deprecated_usage → auto-deleted
# ---------------------------------------------------------------------------

def test_orphaned_deprecated_usage_auto_deleted(db):
    # Record a deprecated usage for a pipeline that doesn't exist
    db.record_deprecated_usage(
        pipeline_name="ghost-pipeline",
        step_id="s1",
        old_type="python",
        new_type="script.python",
    )

    result = run_integrity_checks(db)

    assert any("ghost-pipeline" in f for f in result["auto_fixed"]), result
    # Should be gone now
    remaining = db.get_deprecated_usage()
    assert not any(e["pipeline_name"] == "ghost-pipeline" for e in remaining)


# ---------------------------------------------------------------------------
# 7. Orphaned deprecated_usage for existing pipeline → kept
# ---------------------------------------------------------------------------

def test_non_orphaned_deprecated_usage_kept(db):
    pipeline_id = db.upsert_pipeline(
        name="real-pipeline",
        path="/tmp/real.yaml",
        yaml_content="name: real-pipeline\nsteps:\n  - id: s1\n    type: script.python\n    script: pass\n",
    )
    db.upsert_step(
        pipeline_id=pipeline_id,
        step_dict={"id": "s1", "type": "script.python", "script": "pass"},
        step_order=0,
    )
    db.record_deprecated_usage(
        pipeline_name="real-pipeline",
        step_id="s1",
        old_type="python",
        new_type="script.python",
    )

    result = run_integrity_checks(db)

    # Should NOT be deleted
    remaining = db.get_deprecated_usage()
    assert any(e["pipeline_name"] == "real-pipeline" for e in remaining)
    # And no auto_fixed for this
    assert not any("real-pipeline" in f for f in result["auto_fixed"])


# ---------------------------------------------------------------------------
# 8. Unknown helper ref in DB step row → issue reported
# ---------------------------------------------------------------------------

def test_unknown_helper_ref_reported(db):
    # Add a known helper
    db.upsert_helper(
        name="known-helper",
        script_path="/tmp/known.py",
        description="",
        requirements=[],
        input_schema={},
        output_schema={},
    )
    pipeline_id = db.upsert_pipeline(
        name="bad-helper-pipe",
        path="/tmp/bad-helper.yaml",
    )
    db.upsert_step(
        pipeline_id=pipeline_id,
        step_dict={"id": "s1", "type": "script.python", "helper": "nonexistent-helper"},
        step_order=0,
    )

    result = run_integrity_checks(db)

    codes = [i["code"] for i in result["issues"]]
    assert "UNKNOWN_HELPER_REF" in codes


# ---------------------------------------------------------------------------
# 9. Valid pipeline with known helper → no UNKNOWN_HELPER_REF issue
# ---------------------------------------------------------------------------

def test_known_helper_ref_no_issue(db):
    db.upsert_helper(
        name="my-helper",
        script_path="/tmp/my-helper.py",
        description="",
        requirements=[],
        input_schema={},
        output_schema={},
    )
    pipeline_id = db.upsert_pipeline(
        name="good-helper-pipe",
        path="/tmp/good.yaml",
        project="myproject",
    )
    db.upsert_step(
        pipeline_id=pipeline_id,
        step_dict={"id": "s1", "type": "script.python", "helper": "my-helper"},
        step_order=0,
    )

    result = run_integrity_checks(db)

    codes = [i["code"] for i in result["issues"]]
    assert "UNKNOWN_HELPER_REF" not in codes


# ---------------------------------------------------------------------------
# 10. Return structure always has required keys
# ---------------------------------------------------------------------------

def test_result_structure(db):
    result = run_integrity_checks(db)
    assert "ok" in result
    assert "issues" in result
    assert "auto_fixed" in result
    assert isinstance(result["ok"], bool)
    assert isinstance(result["issues"], list)
    assert isinstance(result["auto_fixed"], list)
