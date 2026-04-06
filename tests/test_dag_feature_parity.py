"""Parity tests for features present in sequential execution but missing in DAG."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from brix.db import BrixDB
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.runners.base import BaseRunner, _StubRunnerMixin


def _load(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


class _CountingRunner(_StubRunnerMixin, BaseRunner):
    def __init__(self, data="ok"):
        self.calls = 0
        self._data = data

    async def execute(self, step, context) -> dict:
        self.calls += 1
        return {"success": True, "data": self._data}


@pytest.fixture
def _isolate_workdir(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")
    return tmp_path


@pytest.mark.asyncio
async def test_dag_pause_before_waits_for_resume(monkeypatch, _isolate_workdir):
    pipeline = _load("""
name: dag-pause-before
steps:
  - id: prep
    type: set
    values:
      ready: true
  - id: paused
    type: python
    script: dummy.py
    pause_before: true
    depends_on: [prep]
""")
    engine = PipelineEngine()
    runner = _CountingRunner(data="ran-after-resume")
    engine.register_runner("python", runner)

    wait_mock = AsyncMock()
    monkeypatch.setattr(engine, "_wait_for_breakpoint_resume", wait_mock)

    result = await engine.run(pipeline)

    assert result.success is True
    assert runner.calls == 1
    wait_mock.assert_awaited_once()
    assert wait_mock.await_args.args[1] == "paused"
    assert result.steps["paused"].status == "ok"


@pytest.mark.asyncio
async def test_dag_test_mode_intercepts_db_upsert(_isolate_workdir):
    pipeline = _load("""
name: dag-test-mode
test_mode: true
steps:
  - id: prep
    type: set
    values:
      x: 1
  - id: upsert
    type: db_upsert
    params:
      table: demo_table
      data:
        x: "{{ steps.prep.x }}"
    depends_on: [prep]
""")
    engine = PipelineEngine()

    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["upsert"].status == "ok"
    assert result.steps["upsert"].reason == "test_mode_dry"
    assert result.result == {"test_mode": True, "dry": True, "step_id": "upsert"}


@pytest.mark.asyncio
async def test_dag_pin_mock_short_circuits_runner(_isolate_workdir):
    pipeline_name = "dag-pin-mock"
    pipeline = _load(f"""
name: {pipeline_name}
steps:
  - id: prep
    type: set
    values:
      x: 1
  - id: pinned
    type: python
    script: dummy.py
    depends_on: [prep]
""")
    engine = PipelineEngine()
    runner = _CountingRunner(data="should-not-run")
    engine.register_runner("python", runner)

    db = BrixDB()
    db.pin_step(pipeline_name, "pinned", {"mocked": True, "value": 42})
    try:
        result = await engine.run(pipeline)
    finally:
        db.unpin_step(pipeline_name, "pinned")

    assert result.success is True
    assert runner.calls == 0
    assert result.steps["pinned"].status == "ok"
    assert result.steps["pinned"].reason == "pin_mock"
    assert result.result == {"mocked": True, "value": 42}


@pytest.mark.asyncio
async def test_dag_cache_hit_skips_runner(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr("brix.context.CACHE_BASE", tmp_path / "cache")

    pipeline = _load("""
name: dag-cache
steps:
  - id: prep
    type: set
    values:
      x: 1
  - id: cached
    type: python
    script: dummy.py
    cache: true
    depends_on: [prep]
""")
    engine = PipelineEngine()
    runner = _CountingRunner(data="computed-result")
    engine.register_runner("python", runner)

    first = await engine.run(pipeline)
    second = await engine.run(pipeline)

    assert first.success is True
    assert second.success is True
    assert runner.calls == 1
    assert second.steps["cached"].status == "ok"
    assert second.steps["cached"].reason == "cache_hit"
    assert second.result == "computed-result"
