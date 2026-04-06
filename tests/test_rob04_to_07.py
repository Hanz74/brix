"""Regression tests for ROB-04 through ROB-07."""

import asyncio
from types import SimpleNamespace

from brix.bricks.builtins import FILTER
from brix.runners.dedup import DedupRunner
from brix.runners.repeat import RepeatRunner
from brix.runners.transform import TransformRunner


def _run(coro):
    return asyncio.run(coro)


def test_rob04_dedup_validate_config_without_key_or_field_returns_error():
    runner = DedupRunner()

    errors = runner.validate_config({"input": [{"id": 1}]})

    assert errors
    assert any("key" in err or "field" in err for err in errors)


def test_rob05_filter_builtin_uses_flow_filter_type():
    assert FILTER.type == "flow.filter"


def test_rob06_repeat_with_empty_sequence_returns_early_without_running_engine():
    class DummyEngine:
        def __init__(self):
            self.calls = 0
            self._mcp_pool = None

        async def run(self, *args, **kwargs):
            self.calls += 1
            raise AssertionError("repeat should not invoke engine.run() for an empty sequence")

    runner = RepeatRunner(engine=DummyEngine())
    step = SimpleNamespace(sequence=[], timeout=None)

    result = _run(runner.execute(step, context=SimpleNamespace()))

    assert result["success"] is True
    assert result["iterations"] == 0
    assert "empty sequence" in result["message"].lower()
    assert runner._engine.calls == 0


def test_rob07_transform_runner_supports_now_global():
    runner = TransformRunner()
    step = SimpleNamespace(params={"expression": "{{ now().isoformat() }}"}, timeout=None)

    result = _run(runner.execute(step, context=SimpleNamespace()))

    assert result["success"] is True
    assert isinstance(result["data"], str)
    assert "T" in result["data"]
