from __future__ import annotations

from brix.models import Step
from brix.db import merge_step_config_into_params, step_dict_to_row, step_row_to_dict


def test_step_accepts_list_params_for_db_exec():
    step = Step(
        id="insert_user",
        type="db.exec",
        connection="main",
        query="INSERT INTO users (name) VALUES (?)",
        params=["Alice"],
    )

    assert step.params == ["Alice"]


def test_merge_step_config_into_params_sets_list_params_directly():
    step = {
        "id": "insert_user",
        "type": "db.exec",
        "config": {
            "connection": "main",
            "query": "INSERT INTO users (name) VALUES (?)",
            "params": ["Alice"],
        },
        "params": {"stale": "value"},
    }

    result = merge_step_config_into_params(step)

    assert result["params"] == ["Alice"]


def test_step_row_to_dict_preserves_list_config_params_for_db_exec():
    row = step_dict_to_row(
        {
            "id": "insert_user",
            "type": "db.exec",
            "config": {
                "connection": "main",
                "query": "INSERT INTO users (name) VALUES (?)",
                "params": ["Alice"],
            },
            "params": None,
        }
    )

    step = step_row_to_dict(row)

    assert step["config"]["params"] == ["Alice"]
    assert step["params"] == ["Alice"]


def test_merge_step_config_into_params_keeps_dict_merge_behavior():
    step = {
        "id": "insert_user",
        "type": "db.exec",
        "config": {
            "connection": "main",
            "query": "INSERT INTO users (name) VALUES (:name)",
            "params": {"name": "Alice"},
        },
        "params": {"existing": 1},
    }

    result = merge_step_config_into_params(step)

    assert result["params"] == {"existing": 1, "name": "Alice"}


def test_merge_step_config_into_params_keeps_existing_params_when_config_params_is_none():
    step = {
        "id": "insert_user",
        "type": "db.exec",
        "config": {
            "connection": "main",
            "query": "INSERT INTO users DEFAULT VALUES",
            "params": None,
        },
        "params": {"existing": 1},
    }

    result = merge_step_config_into_params(step)

    assert result["params"] == {"existing": 1}
