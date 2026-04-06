from __future__ import annotations

import json

from brix.db import BrixDB
from brix.seed import seed_if_empty


def _seeded_db(tmp_path) -> BrixDB:
    db = BrixDB(db_path=tmp_path / "dbo16.db")
    seed_if_empty(db)
    return db


def test_file_read_brick_uses_file_read_runner(tmp_path):
    db = _seeded_db(tmp_path)
    row = db.brick_definitions_get("file_read")
    assert row is not None
    assert row["runner"] == "file_read"


def test_file_write_brick_uses_file_write_runner(tmp_path):
    db = _seeded_db(tmp_path)
    row = db.brick_definitions_get("file_write")
    assert row is not None
    assert row["runner"] == "file_write"


def test_db_exec_brick_definition_exists(tmp_path):
    db = _seeded_db(tmp_path)
    row = db.brick_definitions_get("db.exec")
    assert row is not None
    assert row["runner"] == "db_exec"


def test_action_queue_brick_definition_exists(tmp_path):
    db = _seeded_db(tmp_path)
    row = db.brick_definitions_get("action.queue")
    assert row is not None
    assert row["runner"] == "queue"


def test_action_emit_brick_definition_exists(tmp_path):
    db = _seeded_db(tmp_path)
    row = db.brick_definitions_get("action.emit")
    assert row is not None
    assert row["runner"] == "emit"


def test_action_notify_schema_matches_runner_fields(tmp_path):
    db = _seeded_db(tmp_path)
    row = db.brick_definitions_get("action.notify")
    assert row is not None
    config_schema = row["config_schema"]
    if isinstance(config_schema, str):
        config_schema = json.loads(config_schema)
    assert set(config_schema) == {"channel", "to", "message"}
    assert config_schema["message"]["required"] is True
