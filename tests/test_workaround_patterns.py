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


def test_validator_requires_workaround_annotation_metadata(db):
    ensure_default_workaround_patterns(db)
    pipeline = PipelineLoader().load_from_string(
        """
name: workaround-metadata-missing
steps:
  - id: save
    type: db.query
    query: "UPDATE documents SET status='done'"
        """
    )

    result = PipelineValidator().validate(pipeline)

    finding = next(f for f in result.findings if f.code == "WORKAROUND_ANNOTATION_MISSING")
    assert "replacement_plan" in finding.message
    assert finding.suggestion == {
        "kind": "update_pipeline_metadata",
        "missing_fields": ["owner", "replacement_plan", "expiry_condition"],
        "patterns": ["db_query_used_for_dml"],
    }


@pytest.mark.asyncio
async def test_create_pipeline_with_workaround_requires_annotation_metadata(db, monkeypatch):
    import brix.mcp_handlers.pipelines as ph
    import brix.mcp_server as mcp_server

    tmp_path = db.db_path.parent / "pipelines"
    tmp_path.mkdir(exist_ok=True)
    monkeypatch.setattr(ph, "_pipeline_dir", lambda: tmp_path)
    monkeypatch.setattr(mcp_server, "PIPELINE_DIR", tmp_path)

    blocked = await ph._handle_create_pipeline(
        {
            "name": "hmk-workaround-blocked",
            "steps": [
                {
                    "id": "save",
                    "type": "db.query",
                    "query": "UPDATE documents SET status='done'",
                }
            ],
        }
    )
    assert blocked["success"] is False
    assert blocked["workaround_policy"]["missing_fields"] == ["owner", "replacement_plan", "expiry_condition"]

    allowed = await ph._handle_create_pipeline(
        {
            "name": "hmk-workaround-annotated",
            "steps": [
                {
                    "id": "save",
                    "type": "db.query",
                    "query": "UPDATE documents SET status='done'",
                }
            ],
            "owner": "team-brix",
            "replacement_plan": "replace db.query DML with db.exec after migration completes",
            "expiry_condition": "remove after HMK persistence brick rollout",
        }
    )
    assert allowed["success"] is True


@pytest.mark.asyncio
async def test_get_tips_includes_workaround_patterns_section(db, monkeypatch):
    import brix.mcp_handlers.help as help_mod

    monkeypatch.setattr(help_mod._registry, "list_all", lambda: [])
    monkeypatch.setattr(help_mod.PipelineStore, "list_all", lambda self: [])

    result = await help_mod._handle_get_tips({})

    assert any("WORKAROUND PATTERNS" in line for line in result["tips"])
    assert any("db_query_used_for_dml" in line for line in result["tips"])
