from __future__ import annotations

import pytest

import brix.context as context_mod
from brix.engine import PipelineEngine
from brix.models import Pipeline, Step
from brix.validator import PipelineValidator


def _validate(pipeline: Pipeline):
    return PipelineValidator().validate(pipeline, level="standard")


@pytest.fixture(autouse=True)
def _use_tmp_run_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(context_mod, "WORKDIR_BASE", tmp_path / "runs")


@pytest.mark.asyncio
async def test_permissive_pipeline_legacy_type_warns_only():
    pipeline = Pipeline(
        name="permissive-legacy",
        steps=[Step(id="s1", type="set", values={"ok": True})],
        policy_level="permissive",
    )

    validation = _validate(pipeline)
    assert validation.errors == []
    assert any('type "set" is deprecated' in warning for warning in validation.warnings)

    result = await PipelineEngine().run(pipeline)
    assert result.success is True
    assert any("Step type 'set' is deprecated" in warning for warning in result.deprecation_warnings)


@pytest.mark.asyncio
async def test_strict_pipeline_legacy_type_errors():
    pipeline = Pipeline(
        name="strict-legacy",
        steps=[Step(id="s1", type="set", values={"ok": True})],
        policy_level="strict",
    )

    validation = _validate(pipeline)
    assert any('type "set" is not allowed under strict policy' in error for error in validation.errors)

    result = await PipelineEngine().run(pipeline)
    assert result.success is False
    assert result.steps["s1"].status == "error"
    assert "legacy alias" in (result.steps["s1"].error_message or "")


@pytest.mark.asyncio
async def test_locked_pipeline_non_brick_type_errors():
    pipeline = Pipeline(
        name="locked-non-brick",
        steps=[Step(id="s1", type="queue", queue_name="jobs", collect_until=1)],
        policy_level="locked",
    )

    validation = _validate(pipeline)
    assert any('type "queue" is not allowed under locked policy' in error for error in validation.errors)

    result = await PipelineEngine().run(pipeline)
    assert result.success is False
    assert result.steps["s1"].status == "error"
    assert "policy_level=locked" in (result.steps["s1"].error_message or "")
