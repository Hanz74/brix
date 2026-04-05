from __future__ import annotations

import sqlite3

import pytest

from brix.db import BrixDB


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "db_only_schema.db")


def _table_columns(conn: sqlite3.Connection, table: str) -> dict[str, str]:
    return {row[1]: row[2] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_db_only_tables_exist_after_init(db):
    with db._connect() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    assert "pipeline_step" in tables
    assert "pipeline_credential" in tables
    assert "pipeline_input" in tables


def test_db_only_column_types_spot_check(db):
    with db._connect() as conn:
        pipeline_step_cols = _table_columns(conn, "pipeline_step")
        pipeline_credential_cols = _table_columns(conn, "pipeline_credential")
        pipeline_input_cols = _table_columns(conn, "pipeline_input")
        pipeline_cols = _table_columns(conn, "pipeline")

    assert pipeline_step_cols["id"] == "TEXT"
    assert pipeline_step_cols["pipeline_id"] == "TEXT"
    assert pipeline_step_cols["step_key"] == "TEXT"
    assert pipeline_step_cols["step_type"] == "TEXT"
    assert pipeline_step_cols["enabled"] == "INTEGER"
    assert pipeline_step_cols["headers_json"] == "TEXT"
    assert pipeline_step_cols["sub_pipeline"] == "TEXT"
    assert pipeline_step_cols["notify_to"] == "TEXT"
    assert pipeline_step_cols["when_expr"] == "TEXT"
    assert pipeline_step_cols["until_expr"] == "TEXT"
    assert pipeline_step_cols["foreach_expr"] == "TEXT"
    assert pipeline_step_cols["depends_on_json"] == "TEXT"
    assert pipeline_step_cols["unwrap_json"] == "INTEGER"

    assert pipeline_credential_cols["pipeline_id"] == "TEXT"
    assert pipeline_credential_cols["alias"] == "TEXT"
    assert pipeline_credential_cols["env_ref"] == "TEXT"
    assert pipeline_credential_cols["refresh_json"] == "TEXT"

    assert pipeline_input_cols["pipeline_id"] == "TEXT"
    assert pipeline_input_cols["input_key"] == "TEXT"
    assert pipeline_input_cols["type"] == "TEXT"
    assert pipeline_input_cols["default_json"] == "TEXT"

    assert pipeline_cols["migration_status"] == "TEXT"


def test_pipeline_delete_cascades_to_step_rows(db):
    pipeline_id = db.upsert_pipeline("cascade-pipeline", "/tmp/cascade-pipeline.yaml")

    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO pipeline_step (
                id, pipeline_id, step_key, parent_step_id, container, position,
                step_type, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            ("step-row-1", pipeline_id, "step-1", None, "steps", 0, "flow.set"),
        )
        before = conn.execute(
            "SELECT COUNT(*) FROM pipeline_step WHERE pipeline_id = ?",
            (pipeline_id,),
        ).fetchone()[0]
        assert before == 1

    db.delete_pipeline("cascade-pipeline")

    with db._connect() as conn:
        after = conn.execute(
            "SELECT COUNT(*) FROM pipeline_step WHERE pipeline_id = ?",
            (pipeline_id,),
        ).fetchone()[0]

    assert after == 0


def test_pipeline_step_indexes_exist(db):
    with db._connect() as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='pipeline_step'"
            ).fetchall()
        }

    assert "idx_pipeline_step_pipeline_id" in indexes
    assert "idx_pipeline_step_parent_container_position" in indexes
    assert "idx_pipeline_step_pipeline_step_key" in indexes


def test_pipeline_has_migration_status_column(db):
    with db._connect() as conn:
        pipeline_cols = _table_columns(conn, "pipeline")

    assert "migration_status" in pipeline_cols
