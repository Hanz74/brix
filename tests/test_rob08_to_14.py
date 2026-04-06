from types import SimpleNamespace
from unittest.mock import patch

import pytest

from brix.runners.aggregate import AggregateRunner
from brix.runners.choose import ChooseRunner
from brix.runners.filter import FilterRunner
from brix.runners.pipeline_group import PipelineGroupRunner


class _DummyContext:
    def to_jinja_context(self):
        return {}


class _PipelineGroupContext:
    def to_jinja_context(self):
        return {}


class _DummyEngine:
    def __init__(self):
        self.calls = []

    async def run(self, pipeline, shared_params):
        self.calls.append((pipeline, shared_params))
        return SimpleNamespace(success=True, result={"ok": True})


@pytest.mark.asyncio
async def test_rob08_pipeline_group_shared_params_error_is_logged():
    runner = PipelineGroupRunner(engine=_DummyEngine())
    runner._load_sub_pipeline = lambda pipeline_ref: SimpleNamespace(name=pipeline_ref)
    step = SimpleNamespace(
        pipelines=["subpipe"],
        shared_params={"bad": "{{ missing["},
        concurrency=1,
    )

    with patch("brix.runners.pipeline_group.logger") as mock_logger:
        result = await runner.execute(step, _PipelineGroupContext())

    assert result["success"] is True
    mock_logger.warning.assert_called()
    assert "pipeline_group shared_params render error" in mock_logger.warning.call_args[0][0]


@pytest.mark.asyncio
async def test_rob09_filter_expression_error_detail_preserved_in_result():
    runner = FilterRunner()
    step = SimpleNamespace(
        params={
            "input": [{"name": "ok"}],
            "where": "{{ item.missing.attribute }}",
        },
        timeout=None,
    )

    result = await runner.execute(step, _DummyContext())

    assert result["success"] is True
    assert result["data"] == []
    assert result["skipped_errors"] == 1
    assert "first_expression_error" in result
    assert isinstance(result["first_expression_error"], str)
    assert result["first_expression_error"]


def test_rob11_choose_runner_validate_config_without_choices_returns_error():
    runner = ChooseRunner()

    errors = runner.validate_config({})

    assert errors
    assert any("choices" in err for err in errors)


@pytest.mark.asyncio
async def test_rob12_aggregate_error_fallback_is_logged():
    runner = AggregateRunner()
    step = SimpleNamespace(
        params={
            "input": [{"name": "a"}],
            "group_by": "{{ item.missing.attribute }}",
            "operations": {"count": {"op": "count"}},
        },
        timeout=None,
    )

    with patch("brix.runners.aggregate.logger") as mock_logger:
        result = await runner.execute(step, _DummyContext())

    assert result["success"] is True
    assert "__error__" in result["data"]
    mock_logger.warning.assert_called()
    assert "falling back to __error__" in mock_logger.warning.call_args[0][0]
