from __future__ import annotations

import copy
from unittest.mock import patch

import pytest
import yaml

from brix.db import BrixDB
from brix.loader import PipelineLoader
from brix.pipeline_store import PipelineStore
from brix.validator import PipelineValidator


def _noop(self, *args, **kwargs) -> None:
    return None


_HEAVY_CHECKS = [
    "_check_sub_pipeline_existence",
    "_check_connection_existence",
    "_check_brick_config_schema",
    "_check_jinja_ast",
]


@pytest.fixture(autouse=True)
def _patch_heavy_checks():
    patches = [patch.object(PipelineValidator, name, _noop) for name in _HEAVY_CHECKS]
    for current in patches:
        current.start()
    yield
    for current in patches:
        current.stop()


@pytest.fixture
def isolated_db(tmp_path):
    return BrixDB(db_path=tmp_path / "val106.db")


@pytest.fixture
def store(tmp_path, isolated_db):
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)


def _validate_snapshot(pipeline) -> dict[str, object]:
    result = PipelineValidator().validate(pipeline, level="standard")
    return {
        "is_valid": result.is_valid,
        "errors": result.errors,
        "warnings": result.warnings,
        "infos": result.infos,
        "checks": result.checks,
    }


def test_db_roundtrip_pipeline_validates(store):
    pipeline_data = {
        "name": "val106-basic",
        "steps": [
            {
                "id": "set_value",
                "type": "flow.set",
                "values": {"answer": 42},
            }
        ],
    }

    store.save(copy.deepcopy(pipeline_data))
    loaded = store.load("val106-basic")

    result = PipelineValidator().validate(loaded, level="standard")

    assert result.is_valid
    assert result.errors == []


def test_db_roundtrip_promotes_config_params_for_validation(store):
    pipeline_data = {
        "name": "val106-config-params",
        "steps": [
            {
                "id": "fetch",
                "type": "db.query",
                "config": {
                    "connection": "analytics",
                    "query": "SELECT * FROM users WHERE status = :status",
                    "params": {"status": "active"},
                },
            }
        ],
    }

    store.save(copy.deepcopy(pipeline_data))
    loaded = store.load("val106-config-params")

    step = loaded.steps[0]
    result = PipelineValidator().validate(loaded, level="standard")

    assert step.connection == "analytics"
    assert step.query == "SELECT * FROM users WHERE status = :status"
    assert step.params == {"status": "active"}
    assert result.is_valid
    assert result.errors == []


def test_db_roundtrip_keeps_db_exec_list_params_without_crash(store):
    pipeline_data = {
        "name": "val106-db-exec-list",
        "steps": [
            {
                "id": "insert_user",
                "type": "db.exec",
                "connection": "main",
                "query": "INSERT INTO users (name) VALUES (?)",
                "params": ["Alice"],
            }
        ],
    }

    store.save(copy.deepcopy(pipeline_data))
    loaded = store.load("val106-db-exec-list")

    result = PipelineValidator().validate(loaded, level="standard")

    assert loaded.steps[0].params == ["Alice"]
    assert result.is_valid
    assert result.errors == []


def test_yaml_loaded_and_db_loaded_pipelines_validate_identically(store):
    pipeline_data = {
        "name": "val106-compare",
        "steps": [
            {
                "id": "fetch",
                "type": "db.query",
                "config": {
                    "connection": "analytics",
                    "query": "SELECT * FROM users WHERE status = :status",
                    "params": {"status": "active"},
                },
            },
            {
                "id": "insert_audit",
                "type": "db.exec",
                "config": {
                    "connection": "main",
                    "query": "INSERT INTO audit_log (status) VALUES (?)",
                    "params": ["active"],
                },
            },
        ],
    }

    yaml_loaded = PipelineLoader().load_from_string(yaml.safe_dump(pipeline_data))
    store.save(copy.deepcopy(pipeline_data))
    db_loaded = store.load("val106-compare")

    assert _validate_snapshot(yaml_loaded) == _validate_snapshot(db_loaded)

