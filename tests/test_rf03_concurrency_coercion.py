import asyncio
from types import SimpleNamespace

import pytest

from brix.runners.parallel_runner import ParallelStepRunner
from brix.runners.pipeline_group import PipelineGroupRunner


_MISSING = object()


class _DummyContext:
    def to_jinja_context(self):
        return {}


class _DummyRunResult:
    def __init__(self, *, success=True, result=None, duration=0.0):
        self.success = success
        self.result = result if result is not None else {}
        self.duration = duration


class _DummyEngine:
    async def run(self, pipeline, params=None):
        return _DummyRunResult(result={"pipeline": getattr(pipeline, "name", "dummy")})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runner_cls", "step_factory", "expected_default"),
    [
        (
            ParallelStepRunner,
            lambda concurrency=_MISSING: SimpleNamespace(
                sub_steps=[{"id": "a", "type": "cli", "args": ["echo", "a"]}] * 4,
                **(
                    {}
                    if concurrency is _MISSING
                    else {"concurrency": concurrency}
                ),
            ),
            4,
        ),
        (
            PipelineGroupRunner,
            lambda concurrency=_MISSING: SimpleNamespace(
                pipelines=["pipe-a", "pipe-b"],
                shared_params={},
                **(
                    {}
                    if concurrency is _MISSING
                    else {"concurrency": concurrency}
                ),
            ),
            3,
        ),
    ],
)
async def test_rf03_concurrency_string_is_coerced_to_int(
    monkeypatch, runner_cls, step_factory, expected_default
):
    captured = []
    real_semaphore = asyncio.Semaphore

    def capture_semaphore(value):
        captured.append(value)
        return real_semaphore(value)

    monkeypatch.setattr(asyncio, "Semaphore", capture_semaphore)

    runner = runner_cls(engine=_DummyEngine())
    if isinstance(runner, PipelineGroupRunner):
        monkeypatch.setattr(
            runner,
            "_load_sub_pipeline",
            lambda ref: SimpleNamespace(name=ref),
        )

    result = await runner.execute(step_factory("4"), _DummyContext())

    assert result["success"] is True
    assert captured[0] == 4

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runner_cls", "step_factory", "expected_default"),
    [
        (
            ParallelStepRunner,
            lambda concurrency=_MISSING: SimpleNamespace(
                sub_steps=[{"id": "a", "type": "cli", "args": ["echo", "a"]}] * 4,
                **(
                    {}
                    if concurrency is _MISSING
                    else {"concurrency": concurrency}
                ),
            ),
            4,
        ),
        (
            PipelineGroupRunner,
            lambda concurrency=_MISSING: SimpleNamespace(
                pipelines=["pipe-a", "pipe-b"],
                shared_params={},
                **(
                    {}
                    if concurrency is _MISSING
                    else {"concurrency": concurrency}
                ),
            ),
            3,
        ),
    ],
)
async def test_rf03_concurrency_default_is_used(
    monkeypatch, runner_cls, step_factory, expected_default
):
    captured = []
    real_semaphore = asyncio.Semaphore

    def capture_semaphore(value):
        captured.append(value)
        return real_semaphore(value)

    monkeypatch.setattr(asyncio, "Semaphore", capture_semaphore)

    runner = runner_cls(engine=_DummyEngine())
    if isinstance(runner, PipelineGroupRunner):
        monkeypatch.setattr(
            runner,
            "_load_sub_pipeline",
            lambda ref: SimpleNamespace(name=ref),
        )

    result = await runner.execute(step_factory(), _DummyContext())

    assert result["success"] is True
    assert captured[0] == expected_default


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runner_cls", "step_factory", "message"),
    [
        (
            ParallelStepRunner,
            lambda: SimpleNamespace(
                sub_steps=[{"id": "a", "type": "cli", "args": ["echo", "a"]}],
                concurrency="abc",
            ),
            "Invalid concurrency value for parallel step",
        ),
        (
            PipelineGroupRunner,
            lambda: SimpleNamespace(
                pipelines=["pipe-a"],
                shared_params={},
                concurrency="abc",
            ),
            "Invalid concurrency value for pipeline_group step",
        ),
    ],
)
async def test_rf03_invalid_concurrency_string_raises_clean_error(
    monkeypatch, runner_cls, step_factory, message
):
    runner = runner_cls(engine=_DummyEngine())
    if isinstance(runner, PipelineGroupRunner):
        monkeypatch.setattr(
            runner,
            "_load_sub_pipeline",
            lambda ref: SimpleNamespace(name=ref),
        )

    with pytest.raises(ValueError, match=message):
        await runner.execute(step_factory(), _DummyContext())
