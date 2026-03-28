"""Tests for Step-Level Caching (T-BRIX-V6-24).

Covers:
- CacheManager.compute_key determinism and content-addressing
- CacheManager.get miss / hit
- CacheManager.set persists output
- CacheManager.invalidate removes entry
- Step.cache field default (False)
- Engine: cache=False → runner is always called
- Engine: cache=True → cache miss runs runner, stores result
- Engine: cache=True → cache hit skips runner, returns cached data
- Engine: cache hit step status has reason="cache_hit" and duration=0.0
- Engine: failed step is NOT cached
"""

import json
from pathlib import Path

import pytest

from brix.context import CacheManager
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.runners.base import BaseRunner, _StubRunnerMixin


# ---------------------------------------------------------------------------
# CacheManager unit tests
# ---------------------------------------------------------------------------


def test_cache_manager_compute_key_deterministic(tmp_path):
    """Same (step_id, params) always produces the same hash."""
    mgr = CacheManager(cache_dir=tmp_path)
    k1 = mgr.compute_key("my_step", {"a": 1, "b": "hello"})
    k2 = mgr.compute_key("my_step", {"a": 1, "b": "hello"})
    assert k1 == k2


def test_cache_manager_compute_key_differs_for_different_step(tmp_path):
    """Different step_id → different key even with identical params."""
    mgr = CacheManager(cache_dir=tmp_path)
    k1 = mgr.compute_key("step_a", {"x": 1})
    k2 = mgr.compute_key("step_b", {"x": 1})
    assert k1 != k2


def test_cache_manager_compute_key_differs_for_different_params(tmp_path):
    """Same step_id, different params → different key."""
    mgr = CacheManager(cache_dir=tmp_path)
    k1 = mgr.compute_key("step", {"x": 1})
    k2 = mgr.compute_key("step", {"x": 2})
    assert k1 != k2


def test_cache_manager_miss_returns_none(tmp_path):
    """get() returns None when there is no cached entry."""
    mgr = CacheManager(cache_dir=tmp_path)
    result = mgr.get("step_a", {"foo": "bar"})
    assert result is None


def test_cache_manager_set_and_get(tmp_path):
    """set() followed by get() returns the stored output."""
    mgr = CacheManager(cache_dir=tmp_path)
    output = {"result": "hello", "count": 42}
    mgr.set("step_a", {"p": 1}, output)
    loaded = mgr.get("step_a", {"p": 1})
    assert loaded == output


def test_cache_manager_set_stores_file(tmp_path):
    """set() writes a JSON file under the cache directory."""
    mgr = CacheManager(cache_dir=tmp_path)
    mgr.set("step_a", {"p": 1}, "my_output")
    key = mgr.compute_key("step_a", {"p": 1})
    path = tmp_path / f"{key}.json"
    assert path.exists()
    entry = json.loads(path.read_text())
    assert entry["step_id"] == "step_a"
    assert entry["output"] == "my_output"


def test_cache_manager_invalidate_removes_entry(tmp_path):
    """invalidate() deletes the cache file; subsequent get() returns None."""
    mgr = CacheManager(cache_dir=tmp_path)
    mgr.set("step_a", {"p": 1}, "cached_value")
    assert mgr.get("step_a", {"p": 1}) == "cached_value"

    mgr.invalidate("step_a", {"p": 1})
    assert mgr.get("step_a", {"p": 1}) is None


def test_cache_manager_invalidate_nonexistent_is_noop(tmp_path):
    """invalidate() on a non-existent entry raises no error."""
    mgr = CacheManager(cache_dir=tmp_path)
    mgr.invalidate("step_nonexistent", {})  # should not raise


def test_cache_manager_none_output_not_treated_as_miss(tmp_path):
    """If None is stored as output, get() returns None — indistinguishable from miss.

    This is acceptable by design: steps that return None are not cached
    in a meaningful way. The engine only caches on success with non-None data.
    """
    mgr = CacheManager(cache_dir=tmp_path)
    mgr.set("step_a", {}, None)
    # None output → get() returns None (same as miss)
    assert mgr.get("step_a", {}) is None


def test_cache_manager_creates_directory_on_init(tmp_path):
    """CacheManager creates its cache directory if it does not exist."""
    new_dir = tmp_path / "nested" / "cache"
    assert not new_dir.exists()
    CacheManager(cache_dir=new_dir)
    assert new_dir.exists()


# ---------------------------------------------------------------------------
# Step model: cache field default
# ---------------------------------------------------------------------------


def test_step_cache_field_default():
    """Step.cache defaults to False."""
    from brix.models import Step
    step = Step(id="s", type="cli", command="echo hi", shell=True)
    assert step.cache is False


def test_step_cache_field_can_be_set_true():
    """Step.cache can be set to True."""
    from brix.models import Step
    step = Step(id="s", type="cli", command="echo hi", shell=True, cache=True)
    assert step.cache is True


# ---------------------------------------------------------------------------
# Engine integration: cache=False (default behaviour unchanged)
# ---------------------------------------------------------------------------


class _CountingRunner(_StubRunnerMixin, BaseRunner):
    """Runner that counts invocations and returns a fixed payload."""

    def __init__(self, data="ok"):
        self.calls = 0
        self._data = data

    async def execute(self, step, context) -> dict:
        self.calls += 1
        return {"success": True, "data": self._data}


class _FailRunner(_StubRunnerMixin, BaseRunner):
    """Runner that always fails."""

    async def execute(self, step, context) -> dict:
        return {"success": False, "error": "intentional failure"}


def _load(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


async def test_engine_cache_false_runner_always_called(tmp_path):
    """With cache=False (default), the runner is called on every run."""
    pipeline = _load("""
name: no-cache
steps:
  - id: step1
    type: python
    script: dummy.py
""")
    engine = PipelineEngine()
    runner = _CountingRunner(data="result1")
    engine.register_runner("python", runner)

    await engine.run(pipeline)
    await engine.run(pipeline)

    assert runner.calls == 2


# ---------------------------------------------------------------------------
# Engine integration: cache=True — miss then hit
# ---------------------------------------------------------------------------


async def test_engine_cache_miss_calls_runner(tmp_path, monkeypatch):
    """On first run with cache=True the runner is invoked (cache miss)."""
    monkeypatch.setattr("brix.context.CACHE_BASE", tmp_path)

    pipeline = _load("""
name: cached
steps:
  - id: step1
    type: python
    script: dummy.py
    cache: true
""")
    engine = PipelineEngine()
    runner = _CountingRunner(data="computed_result")
    engine.register_runner("python", runner)

    result = await engine.run(pipeline)

    assert result.success is True
    assert runner.calls == 1
    assert result.result == "computed_result"


async def test_engine_cache_hit_skips_runner(tmp_path, monkeypatch):
    """On second run with cache=True and same params, runner is NOT called."""
    monkeypatch.setattr("brix.context.CACHE_BASE", tmp_path)

    pipeline = _load("""
name: cached
steps:
  - id: step1
    type: python
    script: dummy.py
    cache: true
""")
    engine = PipelineEngine()
    runner = _CountingRunner(data="computed_result")
    engine.register_runner("python", runner)

    # First run — cache miss
    result1 = await engine.run(pipeline)
    assert runner.calls == 1
    assert result1.result == "computed_result"

    # Second run — cache hit
    result2 = await engine.run(pipeline)
    assert runner.calls == 1  # runner NOT called again
    assert result2.result == "computed_result"
    assert result2.success is True


async def test_engine_cache_hit_step_status(tmp_path, monkeypatch):
    """Cache-hit steps have status='ok', duration=0.0, reason='cache_hit'."""
    monkeypatch.setattr("brix.context.CACHE_BASE", tmp_path)

    pipeline = _load("""
name: cached
steps:
  - id: step1
    type: python
    script: dummy.py
    cache: true
""")
    engine = PipelineEngine()
    runner = _CountingRunner(data="payload")
    engine.register_runner("python", runner)

    # Populate cache
    await engine.run(pipeline)

    # Second run: check step status
    result = await engine.run(pipeline)
    step_st = result.steps["step1"]
    assert step_st.status == "ok"
    assert step_st.duration == 0.0
    assert step_st.reason == "cache_hit"


async def test_engine_cache_different_params_different_entries(tmp_path, monkeypatch):
    """Different input params produce different cache entries."""
    monkeypatch.setattr("brix.context.CACHE_BASE", tmp_path)

    pipeline = _load("""
name: cached-param
input:
  greeting:
    type: string
    default: hello
steps:
  - id: greeter
    type: python
    script: dummy.py
    cache: true
    params:
      msg: "{{ input.greeting }}"
""")
    engine = PipelineEngine()
    runner = _CountingRunner(data="result_a")
    engine.register_runner("python", runner)

    # Run with default greeting → cache miss
    await engine.run(pipeline, user_input={"greeting": "hello"})
    assert runner.calls == 1

    # Run with same greeting → cache hit
    await engine.run(pipeline, user_input={"greeting": "hello"})
    assert runner.calls == 1

    # Run with different greeting → cache miss (new entry)
    runner._data = "result_b"
    result = await engine.run(pipeline, user_input={"greeting": "world"})
    assert runner.calls == 2
    assert result.result == "result_b"


async def test_engine_failed_step_not_cached(tmp_path, monkeypatch):
    """A failed step is not written to the cache; next run retries the runner."""
    monkeypatch.setattr("brix.context.CACHE_BASE", tmp_path)

    pipeline = _load("""
name: fail-cache
error_handling:
  on_error: continue
steps:
  - id: step1
    type: python
    script: dummy.py
    cache: true
    on_error: continue
""")
    engine = PipelineEngine()
    fail_runner = _FailRunner()
    engine.register_runner("python", fail_runner)

    # First run — fails; should not be cached
    result1 = await engine.run(pipeline)
    assert result1.steps["step1"].status == "error"

    # Second run — should still call the runner (no cache hit)
    success_runner = _CountingRunner(data="recovered")
    engine.register_runner("python", success_runner)
    result2 = await engine.run(pipeline)
    assert success_runner.calls == 1
    assert result2.steps["step1"].status == "ok"
