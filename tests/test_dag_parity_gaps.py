"""Xfail tests documenting DAG parity gaps slated for ENG-06."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from brix.db import BrixDB
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.runners.base import BaseRunner, _StubRunnerMixin


XFAIL_REASON = "DAG parity gap \u2014 will be fixed by ENG-06"


def _load(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


@pytest.fixture
def _isolate_workdir(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")
    return tmp_path


class _ValidateConfigRunner(_StubRunnerMixin, BaseRunner):
    def validate_config(self, config: dict) -> list[str]:
        if not config.get("required"):
            return ["required must be set"]
        return []

    async def execute(self, step, context) -> dict:
        self.report_progress(100.0, "done")
        return {"success": True, "data": {"validated": True}}


class _EchoItemRunner(_StubRunnerMixin, BaseRunner):
    async def execute(self, step, context) -> dict:
        self.report_progress(100.0, "done")
        return {"success": True, "data": step.params.get("value")}


class _ResourceRunner(_StubRunnerMixin, BaseRunner):
    async def execute(self, step, context) -> dict:
        self.report_progress(100.0, "done")
        return {"success": True, "data": {"ok": True}}


class _SuccessRunner(_StubRunnerMixin, BaseRunner):
    async def execute(self, step, context) -> dict:
        self.report_progress(100.0, "done")
        return {"success": True, "data": {"step": step.id}}


class _FailRunner(_StubRunnerMixin, BaseRunner):
    async def execute(self, step, context) -> dict:
        self.report_progress(100.0, "failed")
        return {"success": False, "error": f"boom-{step.id}"}


class _LlmUsageRunner(_StubRunnerMixin, BaseRunner):
    async def execute(self, step, context) -> dict:
        self.report_progress(100.0, "done")
        return {
            "success": True,
            "data": {
                "text": "ok",
                "llm_usage": {
                    "model": "gpt-4o",
                    "input_tokens": 1000,
                    "output_tokens": 1000,
                },
            },
        }


class _NoProgressRunner(_StubRunnerMixin, BaseRunner):
    async def execute(self, step, context) -> dict:
        return {"success": True, "data": {"ok": True}}


class _CancelWriterRunner(_StubRunnerMixin, BaseRunner):
    async def execute(self, step, context) -> dict:
        self.report_progress(100.0, "wrote-cancel")
        (context.workdir / "cancel_requested.json").write_text(json.dumps({"reason": "test"}))
        return {"success": True, "data": {"cancelled": True}}


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_validate_config(_isolate_workdir):
    pipeline = _load("""
name: dag-validate-config
steps:
  - id: prep
    type: set
    values:
      ready: true
  - id: invalid
    type: custom
    params:
      required: false
    depends_on: [prep]
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _ValidateConfigRunner())

    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["invalid"].status == "error"
    assert "validation" in (result.steps["invalid"].error_message or "").lower()


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_foreach(_isolate_workdir):
    pipeline = _load("""
name: dag-foreach
steps:
  - id: prep
    type: set
    values:
      ready: true
  - id: fanout
    type: custom
    foreach: "{{ [1, 2, 3] | tojson }}"
    params:
      value: "{{ item }}"
    depends_on: [prep]
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _EchoItemRunner())

    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["fanout"].items == 3
    assert result.result["summary"]["total"] == 3


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_resource_usage(_isolate_workdir):
    pipeline = _load("""
name: dag-resource-usage
steps:
  - id: prep
    type: set
    values:
      ready: true
  - id: measured
    type: custom
    depends_on: [prep]
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _ResourceRunner())

    result = await engine.run(pipeline)

    assert result.success is True
    assert isinstance(result.steps["measured"].resource_usage, dict)
    assert result.steps["measured"].resource_usage["rss_mb"] >= 0


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_step_records(_isolate_workdir):
    pipeline = _load("""
name: dag-step-records
steps:
  - id: prep
    type: set
    values:
      ready: true
  - id: worker
    type: custom
    depends_on: [prep]
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _SuccessRunner())

    result = await engine.run(pipeline)
    records = BrixDB().get_step_executions(result.run_id)

    assert result.success is True
    assert {record["step_id"] for record in records} == {"prep", "worker"}


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_saga_compensation(_isolate_workdir):
    executed: list[str] = []

    class _TrackingRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context) -> dict:
            executed.append(step.id)
            self.report_progress(100.0, step.id)
            if step.id == "boom":
                return {"success": False, "error": "boom"}
            return {"success": True, "data": {"step": step.id}}

    pipeline = _load("""
name: dag-saga-compensation
steps:
  - id: prep
    type: custom
    compensate:
      id: undo-prep
      type: custom
    on_error: stop
  - id: boom
    type: custom
    depends_on: [prep]
    on_error: stop
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _TrackingRunner())

    result = await engine.run(pipeline)

    assert result.success is False
    assert "undo-prep" in executed


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_cost_accumulation(_isolate_workdir):
    pipeline = _load("""
name: dag-cost-accumulation
steps:
  - id: prep
    type: set
    values:
      ready: true
  - id: llm_step
    type: custom
    depends_on: [prep]
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _LlmUsageRunner())

    result = await engine.run(pipeline)

    assert result.success is True
    assert result.model_dump().get("cost_usd", 0) > 0


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_progress_compliance(_isolate_workdir, caplog):
    pipeline = _load("""
name: dag-progress-compliance
steps:
  - id: prep
    type: set
    values:
      ready: true
  - id: worker
    type: custom
    depends_on: [prep]
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _NoProgressRunner())

    with caplog.at_level(logging.WARNING):
        result = await engine.run(pipeline)

    assert result.success is True
    assert any("did not call report_progress()" in record.message for record in caplog.records)


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_context_snapshot(_isolate_workdir):
    pipeline = _load("""
name: dag-context-snapshot
steps:
  - id: prep
    type: set
    values:
      ready: true
  - id: worker
    type: custom
    depends_on: [prep]
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _SuccessRunner())

    result = await engine.run(pipeline, keep_workdir=True)
    snapshot_path = Path(_isolate_workdir) / "runs" / result.run_id / "context-snapshot.json"

    assert result.success is True
    assert snapshot_path.exists()
    assert "prep" in snapshot_path.read_text()


@pytest.mark.asyncio
@pytest.mark.xfail(reason=XFAIL_REASON)
async def test_dag_cancel_check(_isolate_workdir):
    calls: list[str] = []

    class _TrackingRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context) -> dict:
            calls.append(step.id)
            self.report_progress(100.0, step.id)
            if step.id == "prep":
                (context.workdir / "cancel_requested.json").write_text(json.dumps({"reason": "test"}))
            return {"success": True, "data": {"step": step.id}}

    pipeline = _load("""
name: dag-cancel-check
steps:
  - id: prep
    type: custom
  - id: worker
    type: custom
    depends_on: [prep]
""")
    engine = PipelineEngine()
    engine.register_runner("custom", _TrackingRunner())

    result = await engine.run(pipeline, keep_workdir=True)

    assert result.success is False
    assert calls == ["prep"]
