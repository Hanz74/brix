from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from unittest.mock import patch

from brix.context import PipelineContext
from brix.runners.db_query import DbQueryRunner


class _Step:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


async def test_db_query_serializes_date_output():
    runner = DbQueryRunner()
    step = _Step(connection="postgresql://example", query="SELECT 1")
    row = {"created_on": dt.date(2026, 4, 6)}

    with patch.object(DbQueryRunner, "_resolve_connection", return_value=("postgresql", "dsn")):
        with patch.object(DbQueryRunner, "_run_query", return_value=[row]):
            result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"] == [{"created_on": "2026-04-06"}]
    json.dumps(result["data"])


async def test_db_query_serializes_datetime_output():
    runner = DbQueryRunner()
    step = _Step(connection="postgresql://example", query="SELECT 1")
    value = dt.datetime(2026, 4, 6, 12, 30, 45, tzinfo=dt.timezone.utc)

    with patch.object(DbQueryRunner, "_resolve_connection", return_value=("postgresql", "dsn")):
        with patch.object(DbQueryRunner, "_run_query", return_value=[{"created_at": value}]):
            result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"][0]["created_at"] == value.isoformat()
    json.dumps(result["data"])


async def test_db_query_serializes_decimal_output():
    runner = DbQueryRunner()
    step = _Step(connection="postgresql://example", query="SELECT 1")

    with patch.object(DbQueryRunner, "_resolve_connection", return_value=("postgresql", "dsn")):
        with patch.object(DbQueryRunner, "_run_query", return_value=[{"amount": Decimal("19.95")}]):
            result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"] == [{"amount": 19.95}]
    json.dumps(result["data"])


async def test_db_query_serializes_mixed_row_output():
    runner = DbQueryRunner()
    step = _Step(connection="postgresql://example", query="SELECT 1")
    row = {
        "id": 7,
        "name": "invoice",
        "created_on": dt.date(2026, 4, 6),
        "created_at": dt.datetime(2026, 4, 6, 12, 30, 45),
        "amount": Decimal("10.50"),
    }

    with patch.object(DbQueryRunner, "_resolve_connection", return_value=("postgresql", "dsn")):
        with patch.object(DbQueryRunner, "_run_query", return_value=[row]):
            result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"] == [{
        "id": 7,
        "name": "invoice",
        "created_on": "2026-04-06",
        "created_at": "2026-04-06T12:30:45",
        "amount": 10.5,
    }]
    json.dumps(result["data"])


def test_pipeline_context_sanitizes_outputs_for_json_persistence(tmp_path):
    context = PipelineContext(workdir=tmp_path / "run")
    output = {
        "items": [{
            "created_on": dt.date(2026, 4, 6),
            "created_at": dt.datetime(2026, 4, 6, 12, 30, 45, tzinfo=dt.timezone.utc),
            "amount": Decimal("3.14"),
        }]
    }

    context.set_output("db_step", output)

    assert context.get_output("db_step") == {
        "items": [{
            "created_on": "2026-04-06",
            "created_at": "2026-04-06T12:30:45+00:00",
            "amount": 3.14,
        }]
    }
    persisted = json.loads((context.workdir / "step_outputs" / "db_step.json").read_text())
    assert persisted == context.get_output("db_step")
