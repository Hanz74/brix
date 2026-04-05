from __future__ import annotations

import yaml

import pytest

from brix.db import BrixDB
from brix.migrations import _normalize_pipeline_steps_v71
from brix.startup_sync import _migrate_pipeline_steps


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "db_only_migration.db")


def _insert_pipeline(db: BrixDB, name: str, raw: dict | None = None, *, yaml_content: str | None = None) -> str:
    content = yaml_content if yaml_content is not None else yaml.dump(raw or {}, sort_keys=False)
    return db.upsert_pipeline(name=name, path=f"/tmp/{name}.yaml", yaml_content=content)


def _pipeline_row(db: BrixDB, name: str) -> dict:
    row = db.get_pipeline(name)
    assert row is not None
    return row


def test_pipeline_with_three_steps_creates_three_pipeline_step_rows(db):
    raw = {
        "name": "three-step-pipeline",
        "version": "2.1.0",
        "steps": [
            {"id": "s1", "type": "flow.set", "values": {"a": 1}},
            {"id": "s2", "type": "flow.set", "values": {"b": 2}},
            {"id": "s3", "type": "flow.set", "values": {"c": 3}},
        ],
    }
    pipeline_id = _insert_pipeline(db, "three-step-pipeline", raw)

    _normalize_pipeline_steps_v71(db)

    steps = db.get_steps(pipeline_id)
    assert [step["id"] for step in steps] == ["s1", "s2", "s3"]
    assert _pipeline_row(db, "three-step-pipeline")["migration_status"] == "v71_complete"


def test_pipeline_with_credentials_creates_pipeline_credential_rows(db):
    raw = {
        "name": "credential-pipeline",
        "steps": [],
        "credentials": {
            "api_key": "API_KEY",
            "oauth": {"env": "OAUTH_TOKEN", "refresh": {"type": "oauth2"}},
        },
    }
    pipeline_id = _insert_pipeline(db, "credential-pipeline", raw)

    _normalize_pipeline_steps_v71(db)

    credentials = db.get_pipeline_credentials(pipeline_id)
    assert credentials == [
        {"pipeline_id": pipeline_id, "name": "api_key", "env": "API_KEY", "refresh": None},
        {
            "pipeline_id": pipeline_id,
            "name": "oauth",
            "env": "OAUTH_TOKEN",
            "refresh": {"type": "oauth2"},
        },
    ]


def test_pipeline_with_inputs_creates_pipeline_input_rows(db):
    raw = {
        "name": "input-pipeline",
        "steps": [],
        "input": {
            "folder": {"type": "string", "default": "inbox", "description": "Folder name"},
            "limit": {"type": "integer", "default": 10},
        },
    }
    pipeline_id = _insert_pipeline(db, "input-pipeline", raw)

    _normalize_pipeline_steps_v71(db)

    inputs = db.get_pipeline_inputs(pipeline_id)
    assert inputs == [
        {
            "pipeline_id": pipeline_id,
            "name": "folder",
            "type": "string",
            "default": "inbox",
            "description": "Folder name",
        },
        {
            "pipeline_id": pipeline_id,
            "name": "limit",
            "type": "integer",
            "default": 10,
            "description": None,
        },
    ]


def test_normalization_is_idempotent(db):
    raw = {
        "name": "idempotent-pipeline",
        "description": "normalize once",
        "steps": [{"id": "fetch", "type": "flow.set", "values": {"ok": True}}],
        "credentials": {"token": "API_TOKEN"},
        "input": {"limit": {"type": "integer", "default": 5}},
    }
    pipeline_id = _insert_pipeline(db, "idempotent-pipeline", raw)

    _normalize_pipeline_steps_v71(db)
    first_steps = db.get_steps(pipeline_id)
    first_credentials = db.get_pipeline_credentials(pipeline_id)
    first_inputs = db.get_pipeline_inputs(pipeline_id)
    first_status = _pipeline_row(db, "idempotent-pipeline")["migration_status"]

    _normalize_pipeline_steps_v71(db)

    assert db.get_steps(pipeline_id) == first_steps
    assert db.get_pipeline_credentials(pipeline_id) == first_credentials
    assert db.get_pipeline_inputs(pipeline_id) == first_inputs
    assert _pipeline_row(db, "idempotent-pipeline")["migration_status"] == first_status == "v71_complete"


def test_invalid_yaml_marks_failed_and_does_not_block_other_pipelines(db):
    _insert_pipeline(
        db,
        "broken-pipeline",
        yaml_content="name: broken-pipeline\nsteps: [\n",
    )
    good_raw = {
        "name": "good-pipeline",
        "steps": [{"id": "ok", "type": "flow.set", "values": {"done": True}}],
    }
    good_id = _insert_pipeline(db, "good-pipeline", good_raw)

    _normalize_pipeline_steps_v71(db)

    assert _pipeline_row(db, "broken-pipeline")["migration_status"] == "v71_failed"
    assert _pipeline_row(db, "good-pipeline")["migration_status"] == "v71_complete"
    assert [step["id"] for step in db.get_steps(good_id)] == ["ok"]


def test_pipeline_step_count_matches_yaml_step_count(db):
    raw = {
        "name": "step-count-pipeline",
        "steps": [
            {"id": "a", "type": "flow.set", "values": {"v": 1}},
            {"id": "b", "type": "flow.set", "values": {"v": 2}},
            {"id": "c", "type": "flow.set", "values": {"v": 3}},
            {"id": "d", "type": "flow.set", "values": {"v": 4}},
        ],
    }
    pipeline_id = _insert_pipeline(db, "step-count-pipeline", raw)

    _normalize_pipeline_steps_v71(db)

    expected_count = len((yaml.safe_load(db.get_pipeline_yaml_content("step-count-pipeline")) or {})["steps"])
    assert len(db.get_steps(pipeline_id)) == expected_count


def test_startup_sync_normalizes_imported_pipeline_rows(db):
    raw = {
        "name": "startup-pipeline",
        "steps": [{"id": "seed", "type": "flow.set", "values": {"x": 1}}],
    }
    pipeline_id = _insert_pipeline(db, "startup-pipeline", raw)

    migrated = _migrate_pipeline_steps(db)

    assert migrated == 1
    assert [step["id"] for step in db.get_steps(pipeline_id)] == ["seed"]
    assert _pipeline_row(db, "startup-pipeline")["migration_status"] == "v71_complete"
