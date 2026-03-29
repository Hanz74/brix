"""Tests for DB integrity checks — T-BRIX-INT-01."""
from __future__ import annotations

import pytest
import yaml

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
# 2. Pipeline without yaml_content AND no disk file → reported as issue
# ---------------------------------------------------------------------------

def test_pipeline_without_yaml_content_reported(tmp_path, db):
    # Insert a pipeline with no yaml_content
    db.upsert_pipeline(name="no-yaml-pipe", path="/tmp/no-yaml-pipe.yaml")

    result = run_integrity_checks(db)

    # Should have PIPELINE_NO_YAML issue
    codes = [i["code"] for i in result["issues"]]
    assert "PIPELINE_NO_YAML" in codes
    # ok=False because there's an unresolvable missing yaml
    assert result["ok"] is False


# ---------------------------------------------------------------------------
# 3. Pipeline without yaml_content but disk file exists → auto-fix
# ---------------------------------------------------------------------------

def test_pipeline_without_yaml_content_autofix(tmp_path, monkeypatch, db):
    import brix.integrity as int_mod

    # Create YAML on disk
    yaml_dir = tmp_path / "pipelines"
    yaml_dir.mkdir()
    pipe_file = yaml_dir / "myfix-pipe.yaml"
    pipe_file.write_text(
        "name: myfix-pipe\nsteps:\n  - id: s1\n    type: script.python\n    script: pass\n",
        encoding="utf-8",
    )

    # Monkeypatch search paths to point to our tmp dir
    monkeypatch.setattr(int_mod, "_PIPELINE_SEARCH_PATHS", [yaml_dir])

    # Insert pipeline without yaml_content
    db.upsert_pipeline(name="myfix-pipe", path=str(pipe_file))

    result = run_integrity_checks(db)

    # Should have been auto-fixed
    assert any("myfix-pipe" in f for f in result["auto_fixed"]), result
    # And yaml_content should now exist in DB
    content = db.get_pipeline_yaml_content("myfix-pipe")
    assert content is not None
    assert "myfix-pipe" in content


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
    db.upsert_pipeline(
        name="real-pipeline",
        path="/tmp/real.yaml",
        yaml_content="name: real-pipeline\nsteps:\n  - id: s1\n    type: script.python\n    script: pass\n",
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
# 8. Unknown helper ref in pipeline YAML → issue reported
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
    # Add pipeline referencing an unknown helper
    db.upsert_pipeline(
        name="bad-helper-pipe",
        path="/tmp/bad-helper.yaml",
        yaml_content=(
            "name: bad-helper-pipe\n"
            "steps:\n"
            "  - id: s1\n"
            "    type: script.python\n"
            "    helper: nonexistent-helper\n"
        ),
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
    db.upsert_pipeline(
        name="good-helper-pipe",
        path="/tmp/good.yaml",
        yaml_content=(
            "name: good-helper-pipe\n"
            "steps:\n"
            "  - id: s1\n"
            "    type: script.python\n"
            "    helper: my-helper\n"
        ),
        project="myproject",
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
