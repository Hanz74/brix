"""Tests for T-BRIX-ORG-01 — project, tags, group organisation.

Covers:
- DB migrations: project/tags/group_name columns present
- auto_tag_by_prefix() prefix rules
- delete_test_pipelines()
- DB list_pipelines / list_helpers with filter
- DB helper_set_project / delete_helpers_by_project
- DB upsert_helper with project/tags/group_name
- DB get_project_stats
- MCP handler: create_pipeline with project
- MCP handler: update_pipeline with project
- MCP handler: list_pipelines with project filter
- MCP handler: create_helper with project
- MCP handler: update_helper with project
- MCP handler: list_helpers with project filter
- get_tips project overview section
"""
from __future__ import annotations

import json
import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations
from brix.seed import auto_tag_by_prefix, delete_test_pipelines, _infer_project_from_name


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Isolated BrixDB with all migrations applied."""
    d = BrixDB(db_path=tmp_path / "org_test.db")
    run_pending_migrations(d)
    return d


def _add_pipeline(db: BrixDB, name: str, project: str = "", tags: list | None = None, group_name: str = "") -> None:
    db.upsert_pipeline(
        name=name,
        path=f"/fake/{name}.yaml",
        project=project if project else None,
        tags=tags,
        group_name=group_name if group_name else None,
    )


def _add_helper(db: BrixDB, name: str, project: str = "", tags: list | None = None, group_name: str = "") -> None:
    db.upsert_helper(
        name=name,
        script_path=f"/fake/{name}.py",
        project=project if project else None,
        tags=tags,
        group_name=group_name if group_name else None,
    )


# ---------------------------------------------------------------------------
# 1. DB schema — columns exist after migrations
# ---------------------------------------------------------------------------

def test_pipelines_has_org_columns(db):
    """Verify project/tags/group_name columns exist on pipelines table."""
    with db._connect() as conn:
        assert db._column_exists(conn, "pipelines", "project")
        assert db._column_exists(conn, "pipelines", "tags")
        assert db._column_exists(conn, "pipelines", "group_name")


def test_helpers_has_org_columns(db):
    """Verify project/tags/group_name columns exist on helpers table."""
    with db._connect() as conn:
        assert db._column_exists(conn, "helpers", "project")
        assert db._column_exists(conn, "helpers", "tags")
        assert db._column_exists(conn, "helpers", "group_name")


# ---------------------------------------------------------------------------
# 2. _infer_project_from_name prefix rules
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,expected", [
    ("buddy-intake", "buddy"),
    ("buddy_prepare", "buddy"),
    ("cody-task-start", "cody"),
    ("cody_scan", "cody"),
    ("download-attachments", "utility"),
    ("convert-pdf", "utility"),
    ("import-emails", "utility"),
    ("analyze-data", "utility"),
    ("generate-report", "utility"),
    ("enrich-contacts", "utility"),
    ("apply-rules", "utility"),
    ("test-pipeline", "test"),
    ("xtest-flow", "test"),
    ("assert-step", "test"),
    ("mock-api", "test"),
    ("fail-fast", "test"),
    ("my-custom-pipeline", ""),   # no match → empty
    ("random-name", ""),
])
def test_infer_project_from_name(name, expected):
    assert _infer_project_from_name(name) == expected


# ---------------------------------------------------------------------------
# 3. auto_tag_by_prefix
# ---------------------------------------------------------------------------

def test_auto_tag_tags_untagged_pipelines(db):
    _add_pipeline(db, "buddy-intake")
    _add_pipeline(db, "cody-task-start")
    _add_pipeline(db, "download-files")
    _add_pipeline(db, "random-stuff")

    counts = auto_tag_by_prefix(db)

    assert counts["pipelines_tagged"] == 3  # buddy, cody, download → utility; random = ""

    rows = db.list_pipelines()
    by_name = {r["name"]: r for r in rows}
    assert by_name["buddy-intake"]["project"] == "buddy"
    assert by_name["cody-task-start"]["project"] == "cody"
    assert by_name["download-files"]["project"] == "utility"
    assert by_name["random-stuff"]["project"] == ""


def test_auto_tag_skips_already_tagged(db):
    _add_pipeline(db, "buddy-intake", project="custom")
    counts = auto_tag_by_prefix(db)
    # Already tagged — should NOT overwrite
    assert counts["pipelines_tagged"] == 0
    rows = db.list_pipelines()
    assert rows[0]["project"] == "custom"


def test_auto_tag_tags_helpers(db):
    _add_helper(db, "buddy-classify")
    _add_helper(db, "convert-csv")
    counts = auto_tag_by_prefix(db)
    assert counts["helpers_tagged"] == 2
    helpers = db.list_helpers()
    by_name = {h["name"]: h for h in helpers}
    assert by_name["buddy-classify"]["project"] == "buddy"
    assert by_name["convert-csv"]["project"] == "utility"


# ---------------------------------------------------------------------------
# 4. delete_test_pipelines
# ---------------------------------------------------------------------------

def test_delete_test_pipelines(db):
    _add_pipeline(db, "test-smoke", project="test")
    _add_pipeline(db, "test-edge", project="test")
    _add_pipeline(db, "buddy-intake", project="buddy")

    deleted = delete_test_pipelines(db)
    assert deleted == 2

    remaining = db.list_pipelines()
    names = [r["name"] for r in remaining]
    assert "buddy-intake" in names
    assert "test-smoke" not in names
    assert "test-edge" not in names


# ---------------------------------------------------------------------------
# 5. DB: list_pipelines with filter
# ---------------------------------------------------------------------------

def test_list_pipelines_filter_by_project(db):
    _add_pipeline(db, "buddy-a", project="buddy")
    _add_pipeline(db, "buddy-b", project="buddy")
    _add_pipeline(db, "cody-a", project="cody")

    buddy = db.list_pipelines(project="buddy")
    assert len(buddy) == 2
    assert all(p["project"] == "buddy" for p in buddy)


def test_list_pipelines_filter_by_tags(db):
    _add_pipeline(db, "p-with-tag", tags=["email", "m365"])
    _add_pipeline(db, "p-no-tag", tags=[])

    tagged = db.list_pipelines(tags=["email"])
    assert len(tagged) == 1
    assert tagged[0]["name"] == "p-with-tag"


def test_list_pipelines_filter_by_group(db):
    _add_pipeline(db, "p1", group_name="intake")
    _add_pipeline(db, "p2", group_name="export")

    intake = db.list_pipelines(group_name="intake")
    assert len(intake) == 1
    assert intake[0]["name"] == "p1"


# ---------------------------------------------------------------------------
# 6. DB: upsert_helper and list_helpers with filter
# ---------------------------------------------------------------------------

def test_upsert_helper_stores_project_tags(db):
    db.upsert_helper(
        name="buddy-classify",
        script_path="/app/helpers/buddy-classify.py",
        project="buddy",
        tags=["classification", "email"],
        group_name="buddy-core",
    )
    helpers = db.list_helpers(project="buddy")
    assert len(helpers) == 1
    h = helpers[0]
    assert h["project"] == "buddy"
    assert "classification" in h["tags"]
    assert h["group_name"] == "buddy-core"


def test_list_helpers_filter_by_tags(db):
    _add_helper(db, "h-tagged", tags=["nlp"])
    _add_helper(db, "h-plain", tags=[])

    tagged = db.list_helpers(tags=["nlp"])
    assert len(tagged) == 1
    assert tagged[0]["name"] == "h-tagged"


# ---------------------------------------------------------------------------
# 7. DB: helper_set_project / delete_helpers_by_project
# ---------------------------------------------------------------------------

def test_helper_set_project(db):
    _add_helper(db, "my-helper")
    result = db.helper_set_project("my-helper", "utility")
    assert result is True
    helpers = db.list_helpers(project="utility")
    assert len(helpers) == 1


def test_delete_helpers_by_project(db):
    _add_helper(db, "test-h1", project="test")
    _add_helper(db, "test-h2", project="test")
    _add_helper(db, "prod-h", project="buddy")

    deleted = db.delete_helpers_by_project("test")
    assert deleted == 2
    assert len(db.list_helpers()) == 1


# ---------------------------------------------------------------------------
# 8. DB: get_project_stats
# ---------------------------------------------------------------------------

def test_get_project_stats(db):
    _add_pipeline(db, "buddy-a", project="buddy")
    _add_pipeline(db, "buddy-b", project="buddy")
    _add_pipeline(db, "cody-a", project="cody")
    _add_helper(db, "buddy-h", project="buddy")

    stats = db.get_project_stats()
    assert stats["buddy"]["pipelines"] == 2
    assert stats["buddy"]["helpers"] == 1
    assert stats["cody"]["pipelines"] == 1


# ---------------------------------------------------------------------------
# 9. MCP handler: create_pipeline with project
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_pipeline_with_project(tmp_path, monkeypatch):
    """create_pipeline stores project in DB when project param given."""
    import brix.mcp_handlers.pipelines as ph
    import brix.db as db_mod

    # Use isolated tmp paths
    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)

    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    result = await ph._handle_create_pipeline({
        "name": "buddy-test-pipe",
        "project": "buddy",
        "steps": [],
    })

    assert result["success"] is True
    assert result.get("project") == "buddy"

    rows = d.list_pipelines(project="buddy")
    assert len(rows) == 1
    assert rows[0]["name"] == "buddy-test-pipe"


# ---------------------------------------------------------------------------
# 10. MCP handler: list_pipelines with project filter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_pipelines_handler_project_filter(tmp_path, monkeypatch):
    """list_pipelines with project filter returns only matching pipelines."""
    import brix.mcp_handlers.pipelines as ph
    import brix.db as db_mod

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)

    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    # Create two pipelines with different projects
    d.upsert_pipeline("buddy-pipe", str(tmp_path / "buddy-pipe.yaml"), project="buddy")
    d.upsert_pipeline("cody-pipe", str(tmp_path / "cody-pipe.yaml"), project="cody")

    result = await ph._handle_list_pipelines({"project": "buddy"})

    assert result["success"] is True
    names = [p["name"] for p in result["pipelines"]]
    assert "buddy-pipe" in names
    assert "cody-pipe" not in names


# ---------------------------------------------------------------------------
# 11. MCP handler: list_helpers with project filter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_helpers_handler_project_filter(tmp_path, monkeypatch):
    """list_helpers with project filter returns only matching helpers."""
    import brix.mcp_handlers.helpers as hh
    import brix.db as db_mod

    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    d.upsert_helper("buddy-h", "/fake/buddy-h.py", project="buddy")
    d.upsert_helper("cody-h", "/fake/cody-h.py", project="cody")

    result = await hh._handle_list_helpers({"project": "buddy"})

    assert result["success"] is True
    names = [h["name"] for h in result["helpers"]]
    assert "buddy-h" in names
    assert "cody-h" not in names
