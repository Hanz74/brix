from __future__ import annotations

from types import SimpleNamespace

import pytest

from brix.runners.base import _coerce_bool
from brix.runners.keyword_filter import KeywordFilterRunner
from brix.runners.repeat import RepeatRunner
from brix.runners.set import SetRunner
from brix.runners.switch import SwitchRunner


class _Context:
    def __init__(self, data: dict | None = None, pipeline_name: str = "") -> None:
        self._data = data or {}
        self.pipeline_name = pipeline_name

    def to_jinja_context(self) -> dict:
        return dict(self._data)


class _FakeRunResult:
    def __init__(self, success: bool = True, result=None) -> None:
        self.success = success
        self.result = result
        self.steps = {}


class _FakeRepeatEngine:
    def __init__(self) -> None:
        self.calls = 0
        self._last_step_outputs = {}
        self._mcp_pool = None

    async def run(self, pipeline, _inherit_input=None, mcp_pool=None):
        self.calls += 1
        return _FakeRunResult(success=True, result={"iteration": self.calls})


@pytest.mark.asyncio
async def test_set_persist_uses_json(monkeypatch):
    stored: list[tuple[str, str, str]] = []

    class _FakeDB:
        def store_set(self, key: str, value: str, pipeline_name: str) -> None:
            stored.append((key, value, pipeline_name))

    monkeypatch.setattr("brix.db.BrixDB", _FakeDB)

    runner = SetRunner()
    step = SimpleNamespace(values={"payload": {"a": 1}}, persist=True, params={})
    context = _Context(pipeline_name="demo-pipeline")

    result = await runner.execute(step, context)

    assert result["success"] is True
    assert stored == [("payload", '{"a": 1}', "demo-pipeline")]


@pytest.mark.asyncio
async def test_repeat_string_max_iterations_coerced():
    runner = RepeatRunner(engine=_FakeRepeatEngine())
    step = SimpleNamespace(
        sequence=[{"id": "noop", "type": "set", "values": {"x": 1}}],
        until=None,
        while_condition=None,
        max_iterations="2",
        delay=0,
        timeout=None,
    )

    result = await runner.execute(step, _Context())

    assert result["success"] is True
    assert result["iterations"] == 2


def test_coerce_bool_handles_common_values():
    assert _coerce_bool("false") is False
    assert _coerce_bool("true") is True
    assert _coerce_bool(True) is True


@pytest.mark.asyncio
async def test_switch_integer_case_key_matches_string_value():
    runner = SwitchRunner()
    step = SimpleNamespace(field="{{ input.status }}", cases={1: "step-one"}, default=None, timeout=None)
    context = _Context({"input": {"status": "1"}})

    result = await runner.execute(step, context)

    assert result["success"] is True
    assert result["data"]["matched_case"] == "1"
    assert result["data"]["target_step"] == "step-one"


@pytest.mark.asyncio
async def test_keyword_filter_handles_integer_keyword():
    runner = KeywordFilterRunner()
    step = SimpleNamespace(
        params={
            "input": [{"text": "Invoice 42 ready"}, {"text": "Nothing here"}],
            "fields": ["text"],
            "keywords": [42],
            "mode": "any",
            "case_sensitive": False,
        }
    )

    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"]["count"] == 1
    assert result["data"]["items"][0]["text"] == "Invoice 42 ready"
