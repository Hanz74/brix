from __future__ import annotations

import yaml

import pytest

from brix.db import BrixDB, step_dict_to_row
from brix.migrations import _normalize_pipeline_steps_v71


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "dbo10_column_fix.db")


def _insert_pipeline(db: BrixDB, name: str, raw: dict) -> str:
    return db.upsert_pipeline(
        name=name,
        path=f"/tmp/{name}.yaml",
        yaml_content=yaml.dump(raw, sort_keys=False),
    )


def test_step_dict_to_row_skips_unknown_key():
    row = step_dict_to_row({
        "id": "stopper",
        "type": "stop",
        "foobar": "ignored",
    })

    assert row["step_key"] == "stopper"
    assert row["step_type"] == "stop"
    assert "foobar" not in row


def test_step_dict_to_row_maps_success_alias():
    row = step_dict_to_row({
        "id": "stopper",
        "type": "stop",
        "success": True,
    })

    assert row["success_on_stop"] == 1
    assert "success" not in row


def test_step_dict_to_row_keeps_success_on_stop_canonical():
    row = step_dict_to_row({
        "id": "stopper",
        "type": "stop",
        "success_on_stop": False,
    })

    assert row["success_on_stop"] == 0


def test_upsert_step_accepts_raw_success_alias(db):
    pipeline_id = db.upsert_pipeline("alias-pipeline", "/tmp/alias-pipeline.yaml")

    db.upsert_step(
        pipeline_id,
        {
            "id": "stopper",
            "type": "flow.stop",
            "message": "done",
            "success": True,
        },
        step_order=0,
    )

    with db._connect() as conn:
        stored = conn.execute(
            "SELECT success_on_stop FROM pipeline_step WHERE pipeline_id=? AND step_key=?",
            (pipeline_id, "stopper"),
        ).fetchone()

    assert stored is not None
    assert stored[0] == 1
    assert db.get_step_by_id(pipeline_id, "stopper")["success_on_stop"] is True


def test_migration_accepts_yaml_success_alias(db):
    pipeline_id = _insert_pipeline(
        db,
        "migrate-success-pipeline",
        {
            "name": "migrate-success-pipeline",
            "steps": [
                {
                    "id": "stopper",
                    "type": "flow.stop",
                    "message": "finished",
                    "success": True,
                }
            ],
        },
    )

    _normalize_pipeline_steps_v71(db)

    migrated = db.get_step_by_id(pipeline_id, "stopper")
    pipeline_row = db.get_pipeline("migrate-success-pipeline")

    assert migrated is not None
    assert migrated["success_on_stop"] is True
    assert pipeline_row is not None
    assert pipeline_row["migration_status"] == "v71_complete"
