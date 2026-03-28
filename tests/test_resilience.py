"""Tests for T-BRIX-DB-21: Circuit Breaker, Rate Limiter, Brick Cache, Saga."""
from __future__ import annotations

import asyncio
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from brix.db import BrixDB
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.models import Step
from brix.resilience import (
    BrickCache,
    CircuitBreaker,
    RateLimiter,
    SagaTracker,
    parse_duration,
)
from brix.runners.base import BaseRunner, _StubRunnerMixin


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _db(tmp_path: Path) -> BrixDB:
    return BrixDB(tmp_path / "brix.db")


def _load(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


class _CountingRunner(_StubRunnerMixin, BaseRunner):
    """Returns success on each call, counting invocations."""

    def __init__(self, data="ok"):
        self.calls = 0
        self._data = data

    async def execute(self, step, context) -> dict:
        self.calls += 1
        return {"success": True, "data": self._data}


class _FailRunner(_StubRunnerMixin, BaseRunner):
    """Always returns failure."""

    async def execute(self, step, context) -> dict:
        return {"success": False, "error": "intentional failure"}


class _FailThenSucceedRunner(_StubRunnerMixin, BaseRunner):
    """Fails the first N times, then succeeds."""

    def __init__(self, fail_times: int = 3, data="recovered"):
        self.calls = 0
        self.fail_times = fail_times
        self._data = data

    async def execute(self, step, context) -> dict:
        self.calls += 1
        if self.calls <= self.fail_times:
            return {"success": False, "error": f"fail #{self.calls}"}
        return {"success": True, "data": self._data}


# ---------------------------------------------------------------------------
# parse_duration
# ---------------------------------------------------------------------------


def test_parse_duration_seconds():
    assert parse_duration("30s") == 30.0
    assert parse_duration("1s") == 1.0


def test_parse_duration_minutes():
    assert parse_duration("10m") == 600.0
    assert parse_duration("1m") == 60.0


def test_parse_duration_hours():
    assert parse_duration("1h") == 3600.0
    assert parse_duration("2h") == 7200.0


def test_parse_duration_days():
    assert parse_duration("1d") == 86400.0


def test_parse_duration_bare_number():
    assert parse_duration("60") == 60.0


def test_parse_duration_float():
    assert parse_duration("1.5m") == 90.0


def test_parse_duration_int_passthrough():
    assert parse_duration(120) == 120.0


def test_parse_duration_invalid():
    with pytest.raises(ValueError):
        parse_duration("fast")


# ---------------------------------------------------------------------------
# DB: circuit_breaker_state
# ---------------------------------------------------------------------------


def test_db_cb_get_empty(tmp_path):
    db = _db(tmp_path)
    assert db.cb_get("my_brick") is None


def test_db_cb_upsert_and_get(tmp_path):
    db = _db(tmp_path)
    db.cb_upsert("my_brick", 2, "2026-01-01T10:00:00+00:00", None)
    state = db.cb_get("my_brick")
    assert state is not None
    assert state["failure_count"] == 2
    assert state["cooldown_until"] is None


def test_db_cb_reset(tmp_path):
    db = _db(tmp_path)
    db.cb_upsert("my_brick", 5, "2026-01-01T10:00:00+00:00", "2099-01-01T00:00:00+00:00")
    db.cb_reset("my_brick")
    state = db.cb_get("my_brick")
    assert state["failure_count"] == 0
    assert state["cooldown_until"] is None


# ---------------------------------------------------------------------------
# DB: rate_limiter_state
# ---------------------------------------------------------------------------


def test_db_rl_get_empty(tmp_path):
    db = _db(tmp_path)
    assert db.rl_get_timestamps("my_brick") == []


def test_db_rl_set_and_get(tmp_path):
    db = _db(tmp_path)
    ts = ["2026-01-01T10:00:00+00:00", "2026-01-01T10:00:01+00:00"]
    db.rl_set_timestamps("my_brick", ts)
    result = db.rl_get_timestamps("my_brick")
    assert result == ts


# ---------------------------------------------------------------------------
# DB: brick_cache
# ---------------------------------------------------------------------------


def test_db_bcache_miss(tmp_path):
    db = _db(tmp_path)
    assert db.bcache_get("nonexistent") is None


def test_db_bcache_hit(tmp_path):
    db = _db(tmp_path)
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    db.bcache_set("key1", {"result": 42}, future)
    out = db.bcache_get("key1")
    assert out == {"result": 42}


def test_db_bcache_expired(tmp_path):
    db = _db(tmp_path)
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    db.bcache_set("key_old", {"result": 99}, past)
    assert db.bcache_get("key_old") is None


def test_db_bcache_purge_expired(tmp_path):
    db = _db(tmp_path)
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    db.bcache_set("old", "x", past)
    db.bcache_set("new", "y", future)
    deleted = db.bcache_purge_expired()
    assert deleted == 1
    assert db.bcache_get("new") is not None


# ---------------------------------------------------------------------------
# CircuitBreaker unit tests
# ---------------------------------------------------------------------------


def test_cb_no_state_allows_execution(tmp_path):
    db = _db(tmp_path)
    cb = CircuitBreaker("brick_a", {"max_failures": 3, "cooldown": "10m"}, db)
    assert cb.pre_check(None) is None


def test_cb_failures_below_threshold_no_cooldown(tmp_path):
    db = _db(tmp_path)
    cb = CircuitBreaker("brick_b", {"max_failures": 3, "cooldown": "10m"}, db)
    cb.on_failure()
    cb.on_failure()
    state = db.cb_get("brick_b")
    assert state["failure_count"] == 2
    assert state["cooldown_until"] is None
    assert cb.pre_check(None) is None  # Not yet open


def test_cb_trips_after_max_failures(tmp_path):
    db = _db(tmp_path)
    cb = CircuitBreaker("brick_c", {"max_failures": 3, "cooldown": "10m"}, db)
    cb.on_failure()
    cb.on_failure()
    cb.on_failure()
    state = db.cb_get("brick_c")
    assert state["failure_count"] == 3
    assert state["cooldown_until"] is not None
    # Circuit should be open now
    result = cb.pre_check(None)
    assert result is not None
    assert result["success"] is False
    assert "OPEN" in result["error"]


def test_cb_skips_when_open(tmp_path):
    db = _db(tmp_path)
    # Manually set cooldown_until in the future
    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    db.cb_upsert("brick_d", 5, None, future)
    cb = CircuitBreaker("brick_d", {"max_failures": 3, "cooldown": "10m"}, db)
    result = cb.pre_check(None)
    assert result is not None
    assert result["success"] is False


def test_cb_allows_after_cooldown(tmp_path):
    db = _db(tmp_path)
    # Cooldown already expired
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    db.cb_upsert("brick_e", 5, None, past)
    cb = CircuitBreaker("brick_e", {"max_failures": 3, "cooldown": "10m"}, db)
    result = cb.pre_check(None)
    assert result is None  # Half-open: allow attempt


def test_cb_reset_on_success(tmp_path):
    db = _db(tmp_path)
    cb = CircuitBreaker("brick_f", {"max_failures": 3, "cooldown": "10m"}, db)
    cb.on_failure()
    cb.on_failure()
    cb.on_success()
    state = db.cb_get("brick_f")
    assert state["failure_count"] == 0
    assert state["cooldown_until"] is None


def test_cb_fallback_returns_previous_output(tmp_path):
    db = _db(tmp_path)
    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    db.cb_upsert("brick_g", 5, None, future)
    cb = CircuitBreaker("brick_g", {"max_failures": 3, "cooldown": "5m", "fallback": "prev_step"}, db)

    # Mock context
    mock_ctx = MagicMock()
    mock_ctx.get_output.return_value = {"fallback": True}
    result = cb.pre_check(mock_ctx)
    assert result is not None
    assert result["success"] is True
    assert result["data"] == {"fallback": True}


# ---------------------------------------------------------------------------
# RateLimiter unit tests
# ---------------------------------------------------------------------------


def test_rl_allows_calls_under_limit(tmp_path):
    db = _db(tmp_path)
    rl = RateLimiter("brick_rl", {"max_calls": 5, "per": "1m"}, db)
    assert rl.wait_seconds() == 0.0


def test_rl_blocks_when_limit_reached(tmp_path):
    db = _db(tmp_path)
    rl = RateLimiter("brick_rl2", {"max_calls": 2, "per": "60s"}, db)
    # Record 2 calls (max) right now
    rl.record_call()
    rl.record_call()
    # Third call should need to wait
    wait = rl.wait_seconds()
    assert wait > 0.0


def test_rl_prunes_expired_timestamps(tmp_path):
    db = _db(tmp_path)
    # Set old timestamps (2 minutes ago — outside 1m window)
    old = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    db.rl_set_timestamps("brick_rl3", [old, old])
    rl = RateLimiter("brick_rl3", {"max_calls": 2, "per": "1m"}, db)
    # Old timestamps pruned → no wait
    assert rl.wait_seconds() == 0.0


def test_rl_record_increments(tmp_path):
    db = _db(tmp_path)
    rl = RateLimiter("brick_rl4", {"max_calls": 10, "per": "1m"}, db)
    rl.record_call()
    rl.record_call()
    ts = db.rl_get_timestamps("brick_rl4")
    assert len(ts) == 2


# ---------------------------------------------------------------------------
# BrickCache unit tests
# ---------------------------------------------------------------------------


def test_bcache_miss_on_fresh_db(tmp_path):
    db = _db(tmp_path)
    bc = BrickCache({"key": "test_key", "ttl": "1h"}, db)
    assert bc.get("test_key") is None


def test_bcache_hit_after_set(tmp_path):
    db = _db(tmp_path)
    bc = BrickCache({"key": "k", "ttl": "1h"}, db)
    bc.set("k", {"value": 123})
    result = bc.get("k")
    assert result == {"value": 123}


def test_bcache_expired_returns_none(tmp_path):
    db = _db(tmp_path)
    bc = BrickCache({"key": "k", "ttl": "1s"}, db)
    # Manually set an expired entry
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    from brix.resilience import _make_cache_key
    db.bcache_set(_make_cache_key("k"), {"old": True}, past)
    assert bc.get("k") is None


def test_bcache_different_keys_independent(tmp_path):
    db = _db(tmp_path)
    bc = BrickCache({"key": "k", "ttl": "1h"}, db)
    bc.set("key_a", "data_a")
    bc.set("key_b", "data_b")
    assert bc.get("key_a") == "data_a"
    assert bc.get("key_b") == "data_b"


# ---------------------------------------------------------------------------
# SagaTracker unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_saga_tracker_empty_no_error():
    tracker = SagaTracker()
    # Should not raise when nothing to compensate
    await tracker.run_compensations(None, None, None)


@pytest.mark.asyncio
async def test_saga_compensation_runs_in_reverse():
    """Compensations execute in reverse order (3, 2, 1)."""
    tracker = SagaTracker()
    executed_order = []

    class _TrackingRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            executed_order.append(step.id)
            return {"success": True, "data": f"comp_{step.id}"}

    # Record 3 steps with valid step dicts (type=set is a valid no-op type)
    tracker.record("step1", {"id": "comp1", "type": "set", "values": {}})
    tracker.record("step2", {"id": "comp2", "type": "set", "values": {}})
    tracker.record("step3", {"id": "comp3", "type": "set", "values": {}})

    # Mock engine with runners for set type
    mock_engine = MagicMock()
    mock_engine.loader = PipelineLoader()
    tracking_runner = _TrackingRunner()
    mock_engine._resolve_runner.return_value = tracking_runner

    mock_ctx = MagicMock()
    mock_ctx.to_jinja_context.return_value = {}

    await tracker.run_compensations(mock_ctx, mock_engine, MagicMock())
    assert executed_order == ["comp3", "comp2", "comp1"]


@pytest.mark.asyncio
async def test_saga_compensation_error_does_not_raise():
    """Compensation failures are logged but don't propagate."""
    tracker = SagaTracker()
    tracker.record("step1", {"id": "comp1", "type": "set", "values": {}})

    mock_engine = MagicMock()
    mock_engine.loader = PipelineLoader()

    class _FailingCompRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            return {"success": False, "error": "comp failed"}

    mock_engine._resolve_runner.return_value = _FailingCompRunner()
    mock_ctx = MagicMock()
    mock_ctx.to_jinja_context.return_value = {}

    # Should not raise
    await tracker.run_compensations(mock_ctx, mock_engine, MagicMock())


# ---------------------------------------------------------------------------
# Engine integration: Circuit Breaker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_cb_trips_after_max_failures(tmp_path):
    """After 3 failures the circuit opens and the step is skipped."""
    pipeline = _load("""
name: cb-test
steps:
  - id: my_step
    type: python
    script: dummy.py
    circuit_breaker:
      max_failures: 3
      cooldown: 10m
    on_error: continue
""")
    engine = PipelineEngine()
    fail_runner = _FailRunner()
    engine.register_runner("python", fail_runner)

    db = _db(tmp_path)
    # Pre-populate 3 failures + cooldown
    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    db.cb_upsert("my_step", 3, None, future)

    # Patch BrixDB at the resilience module level (where it's imported)
    with patch("brix.resilience.BrixDB", return_value=db):
        result = await engine.run(pipeline)

    # Step should be skipped (circuit open), not errored
    assert "my_step" in result.steps
    status = result.steps["my_step"]
    assert status.status == "skipped"


@pytest.mark.asyncio
async def test_engine_cb_fallback_returns_data(tmp_path):
    """When circuit is open and fallback is set, returns fallback output."""
    pipeline = _load("""
name: cb-fallback
steps:
  - id: prev_step
    type: set
    values:
      cached: fallback_value
  - id: my_step
    type: python
    script: dummy.py
    circuit_breaker:
      max_failures: 3
      cooldown: 10m
      fallback: prev_step
    on_error: continue
""")
    engine = PipelineEngine()
    fail_runner = _FailRunner()
    engine.register_runner("python", fail_runner)

    db = _db(tmp_path)
    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    db.cb_upsert("my_step", 3, None, future)

    with patch("brix.resilience.BrixDB", return_value=db):
        result = await engine.run(pipeline)

    assert result.steps["my_step"].status == "ok"
    assert result.steps["my_step"].reason == "circuit_breaker_fallback"


# ---------------------------------------------------------------------------
# Engine integration: Rate Limiter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_rl_waits_when_limit_reached(tmp_path):
    """Rate limiter makes the engine wait when max_calls exceeded."""
    pipeline = _load("""
name: rl-test
steps:
  - id: step1
    type: python
    script: dummy.py
    rate_limit:
      max_calls: 1
      per: 60s
""")
    engine = PipelineEngine()
    runner = _CountingRunner("ok")
    engine.register_runner("python", runner)

    db = _db(tmp_path)
    # Fill the window with 1 timestamp (max_calls=1), so next call must wait
    recent = datetime.now(timezone.utc).isoformat()
    db.rl_set_timestamps("step1", [recent])

    sleep_calls: list = []

    async def mock_sleep(secs):
        sleep_calls.append(secs)

    with patch("brix.resilience.BrixDB", return_value=db), \
         patch("brix.engine.asyncio.sleep", side_effect=mock_sleep):
        result = await engine.run(pipeline)

    # sleep was called at least once for rate limiting
    assert any(s > 0 for s in sleep_calls), "Expected at least one rate-limit sleep"


# ---------------------------------------------------------------------------
# Engine integration: Brick Cache
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_brick_cache_hit_skips_runner(tmp_path):
    """On cache hit, runner is NOT called."""
    pipeline = _load("""
name: bc-test
steps:
  - id: step1
    type: python
    script: dummy.py
    cache:
      key: my_static_key
      ttl: 1h
""")
    engine = PipelineEngine()
    runner = _CountingRunner("fresh")
    engine.register_runner("python", runner)

    db = _db(tmp_path)
    # Pre-populate cache
    from brix.resilience import _make_cache_key
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    db.bcache_set(_make_cache_key("my_static_key"), {"cached": True}, future)

    with patch("brix.resilience.BrixDB", return_value=db):
        result = await engine.run(pipeline)

    assert runner.calls == 0
    assert result.steps["step1"].reason == "cache_hit"


@pytest.mark.asyncio
async def test_engine_brick_cache_miss_runs_runner(tmp_path):
    """On cache miss, runner IS called and result is stored."""
    pipeline = _load("""
name: bc-miss
steps:
  - id: step1
    type: python
    script: dummy.py
    cache:
      key: miss_key
      ttl: 1h
""")
    engine = PipelineEngine()
    runner = _CountingRunner("fresh_data")
    engine.register_runner("python", runner)

    db = _db(tmp_path)

    with patch("brix.resilience.BrixDB", return_value=db):
        result = await engine.run(pipeline)

    assert runner.calls == 1
    assert result.success is True


@pytest.mark.asyncio
async def test_engine_brick_cache_expired_runs_runner(tmp_path):
    """On expired cache, runner IS called."""
    pipeline = _load("""
name: bc-expired
steps:
  - id: step1
    type: python
    script: dummy.py
    cache:
      key: exp_key
      ttl: 1h
""")
    engine = PipelineEngine()
    runner = _CountingRunner("fresh")
    engine.register_runner("python", runner)

    db = _db(tmp_path)
    from brix.resilience import _make_cache_key
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    db.bcache_set(_make_cache_key("exp_key"), {"old": True}, past)

    with patch("brix.resilience.BrixDB", return_value=db):
        result = await engine.run(pipeline)

    assert runner.calls == 1


# ---------------------------------------------------------------------------
# Engine integration: Saga
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_saga_compensates_on_failure(tmp_path):
    """When step 4 fails, compensate steps 3, 2, 1 run in reverse."""
    executed = []

    class _TrackingSetRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            executed.append(step.id)
            return {"success": True, "data": f"ok_{step.id}"}

    class _FailRunner2(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            executed.append(step.id)
            return {"success": False, "error": "fail"}

    pipeline = _load("""
name: saga-test
steps:
  - id: step1
    type: set
    values:
      x: 1
    compensate:
      id: comp1
      type: set
      values:
        undo: 1
  - id: step2
    type: set
    values:
      x: 2
    compensate:
      id: comp2
      type: set
      values:
        undo: 2
  - id: step3
    type: set
    values:
      x: 3
    compensate:
      id: comp3
      type: set
      values:
        undo: 3
  - id: step4
    type: python
    script: dummy.py
""")
    engine = PipelineEngine()
    tracking_runner = _TrackingSetRunner()
    engine.register_runner("set", tracking_runner)
    engine.register_runner("python", _FailRunner2())

    result = await engine.run(pipeline)

    # step4 failed → pipeline aborted
    assert not result.success
    # Compensations ran in reverse order: comp3, comp2, comp1
    comp_calls = [e for e in executed if e.startswith("comp")]
    assert comp_calls == ["comp3", "comp2", "comp1"]


@pytest.mark.asyncio
async def test_engine_saga_no_compensate_without_compensate_field(tmp_path):
    """Steps without compensate field are not tracked by saga."""
    pipeline = _load("""
name: saga-no-comp
steps:
  - id: step1
    type: set
    values:
      x: 1
  - id: step2
    type: python
    script: dummy.py
""")

    class _FailRunnerNoComp(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            return {"success": False, "error": "fail"}

    engine = PipelineEngine()
    engine.register_runner("python", _FailRunnerNoComp())

    # Should not raise; no compensations to run
    result = await engine.run(pipeline)
    assert not result.success


# ---------------------------------------------------------------------------
# Backward compatibility: cache: true (bool) still works
# ---------------------------------------------------------------------------


def test_step_cache_bool_true_still_valid():
    """Legacy cache: true (bool) is still accepted."""
    step = Step(id="s", type="cli", command="echo hi", shell=True, cache=True)
    assert step.cache is True


def test_step_cache_dict_accepted():
    """New cache: dict form is accepted."""
    step = Step(id="s", type="cli", command="echo hi", shell=True,
                cache={"key": "{{ input.id }}", "ttl": "30m"})
    assert isinstance(step.cache, dict)
    assert step.cache["ttl"] == "30m"


def test_step_cache_none_accepted():
    """cache: None is accepted."""
    step = Step(id="s", type="cli", command="echo hi", shell=True, cache=None)
    assert step.cache is None


def test_step_circuit_breaker_field():
    step = Step(id="s", type="cli", command="echo hi", shell=True,
                circuit_breaker={"max_failures": 5, "cooldown": "5m"})
    assert step.circuit_breaker["max_failures"] == 5


def test_step_rate_limit_field():
    step = Step(id="s", type="cli", command="echo hi", shell=True,
                rate_limit={"max_calls": 10, "per": "1m"})
    assert step.rate_limit["max_calls"] == 10


def test_step_compensate_field():
    step = Step(id="s", type="cli", command="echo hi", shell=True,
                compensate={"id": "undo_s", "type": "cli", "command": "echo undo", "shell": True})
    assert step.compensate["id"] == "undo_s"
