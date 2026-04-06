from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

from brix.runners.queue import QueueRunner
from brix.runners.validate import ValidateRunner


class _Step:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __getattr__(self, name):
        return None


class _Context:
    def __init__(self, last_output=None, pipeline_name="test_pipeline"):
        self.last_output = last_output
        self.pipeline_name = pipeline_name


def _mock_brix_db(db_instance):
    return patch("brix.runners.queue.BrixDB", MagicMock(return_value=db_instance))


@pytest.mark.asyncio
async def test_rf06_validate_runner_handles_none_context_gracefully():
    runner = ValidateRunner()
    step = SimpleNamespace(
        id="quality_check",
        rules=[
            {
                "field": "{{ item.name }}",
                "min_ratio": 1.0,
                "of": "{{ items }}",
                "on_fail": "stop",
            }
        ],
        timeout=None,
    )

    result = await runner.execute(step, None)

    assert result["success"] is True
    assert result["data"]["violations"] == []
    assert result["data"]["warnings"] == []


@pytest.mark.asyncio
async def test_rf07_queue_collect_until_string_is_coerced_to_int(tmp_path):
    from brix.db import BrixDB

    db = BrixDB(db_path=tmp_path / "queue.db")
    runner = QueueRunner()
    step = _Step(queue_name="q_rf07", collect_until="10", collect_for=None)
    ctx = _Context(last_output={"value": 1})

    with _mock_brix_db(db):
        result = await runner.execute(step, ctx)

    assert result["success"] is True
    assert result["data"]["waiting"] is True
    assert result["data"]["buffered"] == 1
    assert result["data"]["threshold"] == 10
