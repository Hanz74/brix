from __future__ import annotations

import sqlite3

import pytest

from brix.db import BrixDB, step_dict_to_row, step_row_to_dict
from brix.models import Step
from brix.runners.db_query import DbQueryRunner


class _DummyContext:
    def to_jinja_context(self) -> dict:
        return {}


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "dbo13.db")


def test_step_row_to_dict_merges_config_into_params_for_non_specialist():
    row = step_dict_to_row(
        {
            "id": "fetch",
            "type": "db.query",
            "config": {"connection": "main", "query": "SELECT 1"},
            "params": None,
        }
    )

    step = step_row_to_dict(row)

    assert step["config"] == {"connection": "main", "query": "SELECT 1"}
    assert step["params"] == {"connection": "main", "query": "SELECT 1"}


def test_step_from_db_row_populates_params_from_config():
    row = step_dict_to_row(
        {
            "id": "fetch",
            "type": "db.query",
            "config": {"connection": "main", "query": "SELECT 1"},
            "params": None,
        }
    )

    step = Step.from_db_row(row)

    assert step.config == {"connection": "main", "query": "SELECT 1"}
    assert step.params == {"connection": "main", "query": "SELECT 1"}


def test_step_row_to_dict_parses_raw_config_json_before_merge():
    row = {
        "step_key": "fetch",
        "step_type": "db.query",
        "config_json": '{"connection":"main","query":"SELECT 1"}',
        "params_json": None,
    }

    step = step_row_to_dict(row)

    assert step["config"] == {"connection": "main", "query": "SELECT 1"}
    assert step["params"] == {"connection": "main", "query": "SELECT 1"}


def test_step_row_to_dict_prefers_config_json_over_stale_config_field():
    row = {
        "step_key": "fetch",
        "step_type": "db.query",
        "config_json": '{"connection":"main","query":"SELECT 1"}',
        "config": None,
        "params_json": None,
    }

    step = step_row_to_dict(row)

    assert step["config"] == {"connection": "main", "query": "SELECT 1"}
    assert step["params"] == {"connection": "main", "query": "SELECT 1"}


def test_specialist_step_keeps_config_separate_from_params():
    row = step_dict_to_row(
        {
            "id": "extract",
            "type": "extract.specialist",
            "config": {"input_field": "text", "extract": []},
            "params": None,
        }
    )

    step_dict = step_row_to_dict(row)
    step = Step.from_db_row(row)

    assert step_dict["config"] == {"input_field": "text", "extract": []}
    assert step_dict.get("params") is None
    assert step.config == {"input_field": "text", "extract": []}
    assert step.params is None


def test_pipeline_to_dict_and_step_from_db_row_preserve_db_query_config(db):
    pipeline_id = db.upsert_pipeline("dbo13-pipeline", "/tmp/dbo13-pipeline.yaml")
    db.upsert_step(
        pipeline_id,
        {
            "id": "fetch",
            "type": "db.query",
            "config": {
                "connection": "analytics",
                "query": "SELECT 1 AS one",
            },
            "params": None,
        },
        step_order=0,
    )

    pipeline_dict = db.pipeline_to_dict(pipeline_id)
    assert pipeline_dict is not None

    raw_step = pipeline_dict["steps"][0]
    with db._connect() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM pipeline_step WHERE pipeline_id=? AND step_key=?",
            (pipeline_id, "fetch"),
        ).fetchone()
    assert row is not None
    step = Step.from_db_row(dict(row))

    assert raw_step["params"] == {
        "connection": "analytics",
        "query": "SELECT 1 AS one",
    }
    assert step.params == {
        "connection": "analytics",
        "query": "SELECT 1 AS one",
    }


@pytest.mark.asyncio
async def test_db_query_runner_reads_connection_and_query_from_params_config_merge():
    row = step_dict_to_row(
        {
            "id": "fetch",
            "type": "db.query",
            "config": {
                "connection": "analytics",
                "query": "SELECT 1 AS one",
            },
            "params": None,
        }
    )
    step = Step.from_db_row(row)
    runner = DbQueryRunner()

    captured: dict[str, object] = {}

    def _resolve_connection(connection_ref: str, context: object) -> tuple[str, str]:
        captured["connection"] = connection_ref
        return "sqlite", ":memory:"

    def _run_query(driver: str, dsn: str, query: str, params: dict | None) -> list[dict]:
        captured["driver"] = driver
        captured["dsn"] = dsn
        captured["query"] = query
        captured["params"] = params
        return [{"one": 1}]

    runner._resolve_connection = _resolve_connection  # type: ignore[method-assign]
    runner._run_query = _run_query  # type: ignore[method-assign]

    result = await runner.execute(step, _DummyContext())

    assert result["success"] is True
    assert result["metadata"]["row_count"] == 1
    assert captured == {
        "connection": "analytics",
        "driver": "sqlite",
        "dsn": ":memory:",
        "query": "SELECT 1 AS one",
        "params": None,
    }
