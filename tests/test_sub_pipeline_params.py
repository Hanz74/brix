from types import SimpleNamespace

import pytest

from brix.runners.pipeline import PipelineRunner
from brix.runners.pipeline_group import PipelineGroupRunner


class _DummyEngine:
    def __init__(self):
        self.calls = []

    async def run(self, pipeline, user_input):
        self.calls.append((pipeline, user_input))
        return SimpleNamespace(success=True, result={"ok": True})


class _DummyContext:
    def __init__(self):
        self._pipeline_depth = 0

    def to_jinja_context(self):
        return {}


@pytest.mark.asyncio
async def test_sub_pipeline_receives_only_user_params():
    engine = _DummyEngine()
    runner = PipelineRunner(engine=engine)
    runner._load_sub_pipeline = lambda pipeline_ref: SimpleNamespace(name=pipeline_ref, output_slots={})

    payload = {"item": {"id": 123}, "batch_ref": "batch-1"}
    step = SimpleNamespace(
        pipeline="sub-name",
        params={
            "pipeline": "sub-name",
            "params": payload,
        },
    )

    result = await runner.execute(step, _DummyContext())

    assert result["success"] is True
    assert len(engine.calls) == 1
    _, sub_input = engine.calls[0]
    assert sub_input == payload
    assert sub_input["item"]["id"] == 123
    assert "params" not in sub_input
    assert "pipeline" not in sub_input


@pytest.mark.asyncio
async def test_pipeline_group_receives_only_user_params():
    engine = _DummyEngine()
    runner = PipelineGroupRunner(engine=engine)
    runner._load_sub_pipeline = lambda pipeline_ref: SimpleNamespace(name=pipeline_ref)

    payload = {"item": {"id": 456}, "batch_ref": "batch-2"}
    step = SimpleNamespace(
        pipelines=["sub-name"],
        shared_params={
            "pipelines": ["sub-name"],
            "params": payload,
        },
        concurrency=1,
    )

    result = await runner.execute(step, _DummyContext())

    assert result["success"] is True
    assert len(engine.calls) == 1
    _, sub_input = engine.calls[0]
    assert sub_input == payload
    assert sub_input["item"]["id"] == 456
    assert "params" not in sub_input
    assert "pipelines" not in sub_input
