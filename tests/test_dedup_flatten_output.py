from __future__ import annotations

import asyncio

from brix.runners.dedup import DedupRunner
from brix.runners.flatten import FlattenRunner


class _Step:
    def __init__(self, **kwargs):
        self.params = kwargs


def test_dedup_declares_list_output_type():
    assert DedupRunner().output_type() == "list[dict]"


def test_dedup_execute_returns_list_in_data():
    runner = DedupRunner()
    step = _Step(
        input=[
            {"id": 1, "name": "Alice"},
            {"id": 1, "name": "Alice dup"},
            {"id": 2, "name": "Bob"},
        ],
        key="{{ item.id }}",
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert result["data"] == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
    assert result["items_count"] == 2
    assert result["original_count"] == 3
    assert result["removed_count"] == 1


def test_flatten_declares_list_output_type():
    assert FlattenRunner().output_type() == "list"


def test_flatten_execute_returns_list_in_data():
    runner = FlattenRunner()
    step = _Step(input=[["a", "b"], ["c"]], depth=1)

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert result["data"] == ["a", "b", "c"]
    assert result["items_count"] == 3
