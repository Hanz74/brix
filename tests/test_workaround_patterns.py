from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.loader import PipelineLoader
from brix.migrations import run_pending_migrations
from brix.validator import PipelineValidator
from brix.workaround_patterns import ensure_default_workaround_patterns


@pytest.fixture
def db(tmp_path, monkeypatch):
    import brix.db as db_mod
    import brix.workaround_patterns as wp_mod

    database = BrixDB(db_path=tmp_path / "workaround_patterns.db")
    run_pending_migrations(database)
    monkeypatch.setattr(db_mod, "BRIX_DB_PATH", database.db_path)
    monkeypatch.setattr(wp_mod, "BrixDB", lambda: database)
    return database


def test_migration_seeds_default_workaround_patterns(db):
    patterns = db.registry_list("patterns")
    names = {entry["name"] for entry in patterns}
    assert "db_query_used_for_dml" in names
    assert "runner_fields_outside_config" in names
    assert "helper_without_brick_justification" in names


def test_validator_surfaces_known_workaround_pattern_for_db_query_dml(db):
    ensure_default_workaround_patterns(db)
    pipeline = PipelineLoader().load_from_string(
        """
name: workaround-db-query
steps:
  - id: save
    type: db.query
    query: "UPDATE documents SET status='done'"
        """
    )

    result = PipelineValidator().validate(pipeline)

    assert any(f.code == "DB_QUERY_DML" for f in result.findings)
    workaround = next(
        f
        for f in result.findings
        if f.code == "KNOWN_WORKAROUND_PATTERN"
        and (f.suggestion or {}).get("source_finding") == "DB_QUERY_DML"
    )
    assert workaround.step_id == "save"
    assert workaround.suggestion == {
        "kind": "review_workaround_pattern",
        "pattern": "db_query_used_for_dml",
        "source_finding": "DB_QUERY_DML",
    }


def test_validator_surfaces_known_workaround_pattern_for_runner_field_compat(db):
    ensure_default_workaround_patterns(db)
    pipeline = PipelineLoader().load_from_string(
        """
name: workaround-top-level
steps:
  - id: fetch
    type: cli
    args: ["echo", "hi"]
        """
    )

    result = PipelineValidator().validate(pipeline)

    assert any(f.code == "RUNNER_TOP_LEVEL_FIELD_COMPAT" for f in result.findings)
    workaround = [f for f in result.findings if f.code == "KNOWN_WORKAROUND_PATTERN"]
    assert workaround
    assert any(f.step_id == "fetch" for f in workaround)


@pytest.mark.asyncio
async def test_get_tips_includes_workaround_patterns_section(db, monkeypatch):
    import brix.mcp_handlers.help as help_mod

    monkeypatch.setattr(help_mod._registry, "list_all", lambda: [])
    monkeypatch.setattr(help_mod.PipelineStore, "list_all", lambda self: [])

    result = await help_mod._handle_get_tips({})

    assert any("WORKAROUND PATTERNS" in line for line in result["tips"])
    assert any("db_query_used_for_dml" in line for line in result["tips"])
