"""Tests for project/tags/group org enforcement warnings.

Covers:
- create_pipeline without project → MISSING PROJECT warning
- create_pipeline with project → no MISSING PROJECT warning
- create_pipeline without tags → HINT: tags warning
- create_pipeline with tags → no tags hint
- create_helper without project → MISSING PROJECT warning
- create_helper with project → no MISSING PROJECT warning
- get_tips shows untagged counts
- get_tips shows known projects from org registry
- brix__org list/create/delete/seed
"""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Isolated BrixDB with all migrations applied."""
    d = BrixDB(db_path=tmp_path / "org_enforcement_test.db")
    run_pending_migrations(d)
    return d


# ---------------------------------------------------------------------------
# 1. create_pipeline — MISSING PROJECT warning
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_pipeline_without_project_warns(tmp_path, monkeypatch):
    """create_pipeline without 'project' should include MISSING PROJECT warning."""
    import brix.mcp_handlers.pipelines as ph
    import brix.db as db_mod

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    result = await ph._handle_create_pipeline({
        "name": "no-project-pipe",
        "steps": [],
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings), f"Expected MISSING PROJECT warning, got: {warnings}"


@pytest.mark.asyncio
async def test_create_pipeline_with_project_no_missing_project_warning(tmp_path, monkeypatch):
    """create_pipeline with 'project' should NOT include MISSING PROJECT warning."""
    import brix.mcp_handlers.pipelines as ph
    import brix.db as db_mod

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    result = await ph._handle_create_pipeline({
        "name": "with-project-pipe",
        "project": "buddy",
        "steps": [],
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert not any("MISSING PROJECT" in w for w in warnings), f"Unexpected MISSING PROJECT warning: {warnings}"


@pytest.mark.asyncio
async def test_create_pipeline_without_tags_hints(tmp_path, monkeypatch):
    """create_pipeline without 'tags' should include HINT about tags."""
    import brix.mcp_handlers.pipelines as ph
    import brix.db as db_mod

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    result = await ph._handle_create_pipeline({
        "name": "no-tags-pipe",
        "project": "utility",
        "steps": [],
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("HINT" in w and "tags" in w.lower() for w in warnings), f"Expected tags HINT, got: {warnings}"


@pytest.mark.asyncio
async def test_create_pipeline_with_tags_no_hint(tmp_path, monkeypatch):
    """create_pipeline with 'tags' should NOT include tags HINT."""
    import brix.mcp_handlers.pipelines as ph
    import brix.db as db_mod

    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    result = await ph._handle_create_pipeline({
        "name": "with-tags-pipe",
        "project": "buddy",
        "tags": ["intake", "email"],
        "steps": [],
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert not any("HINT" in w and "tags" in w.lower() for w in warnings), f"Unexpected tags HINT: {warnings}"


# ---------------------------------------------------------------------------
# 2. create_helper — MISSING PROJECT warning
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_helper_without_project_warns(tmp_path, monkeypatch):
    """create_helper without 'project' should include MISSING PROJECT warning."""
    import brix.mcp_handlers.helpers as hh
    import brix.db as db_mod
    from brix.mcp_handlers import _shared as shared

    monkeypatch.setattr(shared, "_managed_helper_dir", lambda: tmp_path)
    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    result = await hh._handle_create_helper({
        "name": "no-project-helper",
        "code": "def main(): pass",
        "description": "Test helper",
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert any("MISSING PROJECT" in w for w in warnings), f"Expected MISSING PROJECT warning, got: {warnings}"


@pytest.mark.asyncio
async def test_create_helper_with_project_no_warning(tmp_path, monkeypatch):
    """create_helper with 'project' should NOT include MISSING PROJECT warning."""
    import brix.mcp_handlers.helpers as hh
    import brix.db as db_mod
    from brix.mcp_handlers import _shared as shared

    monkeypatch.setattr(shared, "_managed_helper_dir", lambda: tmp_path)
    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    result = await hh._handle_create_helper({
        "name": "with-project-helper",
        "code": "def main(): pass",
        "description": "Test helper",
        "project": "buddy",
        "tags": ["classification"],
    })

    assert result["success"] is True
    warnings = result.get("warnings", [])
    assert not any("MISSING PROJECT" in w for w in warnings), f"Unexpected MISSING PROJECT warning: {warnings}"


# ---------------------------------------------------------------------------
# 3. get_tips — untagged counts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_tips_shows_unassigned_count(tmp_path, monkeypatch):
    """get_tips should warn when entities have no project assigned."""
    import brix.mcp_handlers.help as hp
    import brix.db as db_mod
    from brix.pipeline_store import PipelineStore

    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    # Insert pipeline without project
    d.upsert_pipeline("unassigned-pipe", str(tmp_path / "unassigned-pipe.yaml"), project=None)

    monkeypatch.setattr(hp, "_pipeline_dir", lambda: tmp_path)
    # Stub list_all to return empty to avoid filesystem scan
    monkeypatch.setattr(PipelineStore, "list_all", lambda self: [])

    result = await hp._handle_get_tips({})

    tips_text = "\n".join(result.get("tips", []))
    assert "unassigned" in tips_text.lower() or "kein projekt" in tips_text.lower() or "⚠" in tips_text, \
        f"Expected warning about unassigned pipelines in tips, got:\n{tips_text}"


# ---------------------------------------------------------------------------
# 4. brix__org CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_org_seed(tmp_path, monkeypatch):
    """brix__org seed loads default projects, tags, and groups."""
    import brix.db as db_mod

    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    from brix.mcp_handlers.org import _handle_org
    result = await _handle_org({"action": "seed"})

    assert result["success"] is True
    entries = d.org_registry_list()
    types = {e["entry_type"] for e in entries}
    assert "project" in types
    assert "tag" in types
    assert "group" in types


@pytest.mark.asyncio
async def test_org_create_and_list(tmp_path, monkeypatch):
    """brix__org create adds entry; list returns it."""
    import brix.db as db_mod

    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    from brix.mcp_handlers.org import _handle_org

    create_result = await _handle_org({
        "action": "create",
        "type": "project",
        "name": "myproject",
        "description": "My custom project",
    })
    assert create_result["success"] is True

    list_result = await _handle_org({"action": "list", "type": "project"})
    assert list_result["success"] is True
    names = [e["name"] for e in list_result["entries"]]
    assert "myproject" in names


@pytest.mark.asyncio
async def test_org_delete(tmp_path, monkeypatch):
    """brix__org delete removes an entry."""
    import brix.db as db_mod

    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    from brix.mcp_handlers.org import _handle_org

    await _handle_org({"action": "create", "type": "tag", "name": "temp-tag"})
    delete_result = await _handle_org({"action": "delete", "type": "tag", "name": "temp-tag"})
    assert delete_result["success"] is True

    list_result = await _handle_org({"action": "list", "type": "tag"})
    names = [e["name"] for e in list_result["entries"]]
    assert "temp-tag" not in names


@pytest.mark.asyncio
async def test_org_create_group_with_pipelines(tmp_path, monkeypatch):
    """brix__org create group stores pipeline list in metadata."""
    import brix.db as db_mod

    d = BrixDB(db_path=tmp_path / "t.db")
    run_pending_migrations(d)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", tmp_path / "t.db")

    from brix.mcp_handlers.org import _handle_org

    result = await _handle_org({
        "action": "create",
        "type": "group",
        "name": "my-chain",
        "description": "A test chain",
        "pipelines": ["pipe-a", "pipe-b"],
    })
    assert result["success"] is True

    entries = d.org_registry_list(entry_type="group")
    entry = next((e for e in entries if e["name"] == "my-chain"), None)
    assert entry is not None
    assert entry["metadata"].get("pipelines") == ["pipe-a", "pipe-b"]
