from __future__ import annotations

import sqlite3

import pytest

from brix.db import BrixDB, step_dict_to_row, step_row_to_dict


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "dbo15.db")


def test_merge_step_save_load_preserves_inputs_mode_key(db):
    pipeline_id = db.upsert_pipeline("dbo15-merge", "/tmp/dbo15-merge.yaml")
    step = {
        "id": "merge-users",
        "type": "merge",
        "inputs": ["users", "orders"],
        "mode": "lookup",
        "key": "user_id",
    }

    db.upsert_step(pipeline_id, step, step_order=0)

    loaded = db.get_step_by_id(pipeline_id, "merge-users")

    assert loaded is not None
    assert loaded["inputs"] == ["users", "orders"]
    assert loaded["mode"] == "lookup"
    assert loaded["key"] == "user_id"


def test_switch_step_save_load_preserves_field_cases_default(db):
    pipeline_id = db.upsert_pipeline("dbo15-switch", "/tmp/dbo15-switch.yaml")
    step = {
        "id": "route",
        "type": "switch",
        "field": "{{ item.status }}",
        "cases": {"approved": "step_approve", "rejected": "step_reject"},
        "default": "step_fallback",
    }

    db.upsert_step(pipeline_id, step, step_order=0)

    loaded = db.get_step_by_id(pipeline_id, "route")

    assert loaded is not None
    assert loaded["field"] == "{{ item.status }}"
    assert loaded["cases"] == {"approved": "step_approve", "rejected": "step_reject"}
    assert loaded["default"] == "step_fallback"


def test_error_handler_step_save_load_preserves_try_and_handler(db):
    pipeline_id = db.upsert_pipeline("dbo15-error-handler", "/tmp/dbo15-error-handler.yaml")
    step = {
        "id": "safe-fetch",
        "type": "error_handler",
        "try_step": "fetch_data",
        "handler_step": "fallback_data",
    }

    db.upsert_step(pipeline_id, step, step_order=0)

    loaded = db.get_step_by_id(pipeline_id, "safe-fetch")

    assert loaded is not None
    assert loaded["try_step"] == "fetch_data"
    assert loaded["handler_step"] == "fallback_data"


def test_repeat_step_save_load_preserves_delay(db):
    pipeline_id = db.upsert_pipeline("dbo15-repeat", "/tmp/dbo15-repeat.yaml")
    step = {
        "id": "poll",
        "type": "repeat",
        "delay": 1.5,
    }

    db.upsert_step(pipeline_id, step, step_order=0)

    loaded = db.get_step_by_id(pipeline_id, "poll")

    assert loaded is not None
    assert loaded["delay"] == pytest.approx(1.5)


@pytest.mark.parametrize(
    ("step", "expected"),
    [
        (
            {
                "id": "merge-users",
                "type": "merge",
                "inputs": ["users", "orders"],
                "mode": "lookup",
                "key": "user_id",
            },
            {
                "inputs": ["users", "orders"],
                "mode": "lookup",
                "key": "user_id",
            },
        ),
        (
            {
                "id": "route",
                "type": "switch",
                "field": "{{ item.status }}",
                "cases": {"approved": "step_approve"},
                "default": "step_fallback",
            },
            {
                "field": "{{ item.status }}",
                "cases": {"approved": "step_approve"},
                "default": "step_fallback",
            },
        ),
        (
            {
                "id": "safe-fetch",
                "type": "error_handler",
                "try_step": "fetch_data",
                "handler_step": "fallback_data",
            },
            {
                "try_step": "fetch_data",
                "handler_step": "fallback_data",
            },
        ),
        (
            {
                "id": "poll",
                "type": "repeat",
                "delay": 2.25,
            },
            {
                "delay": 2.25,
            },
        ),
    ],
)
def test_step_dict_row_roundtrip_preserves_runner_fields(step, expected):
    row = step_dict_to_row(step)
    roundtrip = step_row_to_dict(row)

    for key, value in expected.items():
        if isinstance(value, float):
            assert roundtrip[key] == pytest.approx(value)
        else:
            assert roundtrip[key] == value


def test_migration_adds_missing_pipeline_step_columns(db):
    with db._connect() as conn:
        conn.row_factory = sqlite3.Row
        columns = {
            row["name"]: row["type"]
            for row in conn.execute("PRAGMA table_info(pipeline_step)").fetchall()
        }

    assert columns["inputs_json"] == "TEXT"
    assert columns["merge_mode"] == "TEXT"
    assert columns["merge_key"] == "TEXT"
    assert columns["switch_field"] == "TEXT"
    assert columns["cases_json"] == "TEXT"
    assert columns["switch_default"] == "TEXT"
    assert columns["try_step"] == "TEXT"
    assert columns["handler_step"] == "TEXT"
    assert columns["delay"] == "REAL"
