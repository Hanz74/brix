from types import SimpleNamespace

import pytest

from brix.models import RunResult
from brix.runners.filter import FilterRunner
from brix.runners.parallel_runner import ParallelStepRunner
from brix.runners.repeat import RepeatRunner


class _DummyContext:
    input = {}

    def to_jinja_context(self):
        return {}

    def set_output(self, key, value):
        return None


@pytest.mark.asyncio
async def test_rob01_filter_runner_no_input_defaults_to_empty_list():
    runner = FilterRunner()
    step = SimpleNamespace(
        params={"expression": "{{ item }}", "where": "{{ item.active }}"},
        timeout=None,
    )

    result = await runner.execute(step, context=None)

    assert "input" not in runner.config_schema().get("required", [])
    assert result["success"] is True
    assert result["data"] == []
    assert result["items_count"] == 0


class _ExplodingParallelEngine:
    async def run(self, pipeline):
        raise RuntimeError(f"parallel boom for {pipeline.name}")


@pytest.mark.asyncio
async def test_rob02_parallel_runner_preserves_substep_exception_message():
    runner = ParallelStepRunner(engine=_ExplodingParallelEngine())
    step = SimpleNamespace(
        sub_steps=[{"id": "explode", "type": "cli", "args": ["false"]}],
        concurrency=1,
    )

    result = await runner.execute(step, _DummyContext())

    assert result["success"] is False
    assert result["data"]["explode"]["success"] is False
    assert "parallel boom" in result["data"]["explode"]["error"]


class _ExplodingRepeatEngine:
    _mcp_pool = None

    async def run(self, pipeline, _inherit_input=None, mcp_pool=None):
        raise RuntimeError("repeat boom")


@pytest.mark.asyncio
async def test_rob03_repeat_runner_preserves_substep_exception_message():
    runner = RepeatRunner(engine=_ExplodingRepeatEngine())
    step = SimpleNamespace(
        sequence=[{"id": "explode", "type": "cli", "args": ["false"]}],
        until=None,
        while_condition=None,
        max_iterations=1,
        delay=0,
        timeout=None,
    )

    result = await runner.execute(step, _DummyContext())

    assert result["success"] is False
    assert result["iterations"] == 1
    assert "repeat boom" in result["error"]
