"""Tests for T-BRIX-DB-22: Advanced Flow — Queue/Buffer, Event-Emit/Subscribe,
Streaming (experimental), and Debounce on Triggers.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step stand-in for tests."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getattr__(self, name):
        return None


class _Context:
    """Minimal context stand-in for tests."""

    def __init__(self, last_output=None, pipeline_name="test_pipeline"):
        self.last_output = last_output
        self.pipeline_name = pipeline_name


# ---------------------------------------------------------------------------
# Import guards — ensure new modules are importable
# ---------------------------------------------------------------------------


def test_queue_runner_importable():
    from brix.runners.queue import QueueRunner
    assert QueueRunner is not None


def test_emit_runner_importable():
    from brix.runners.emit import EmitRunner
    assert EmitRunner is not None


def test_debounce_module_importable():
    from brix.triggers.debounce import record_event, is_ready_to_fire, clear_state, get_state
    assert callable(record_event)
    assert callable(is_ready_to_fire)
    assert callable(clear_state)
    assert callable(get_state)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_db(tmp_path):
    """Return a BrixDB instance backed by a temp file."""
    from brix.db import BrixDB
    db_file = tmp_path / "test.db"
    db = BrixDB(db_path=db_file)
    return db


@pytest.fixture()
def queue_runner():
    from brix.runners.queue import QueueRunner
    return QueueRunner()


@pytest.fixture()
def emit_runner():
    from brix.runners.emit import EmitRunner
    return EmitRunner()


def _mock_brix_db(module_path: str, db_instance):
    """Return a patch context manager that replaces BrixDB() calls in module_path."""
    mock_cls = MagicMock(return_value=db_instance)
    return patch(module_path, mock_cls)


# ---------------------------------------------------------------------------
# Queue Runner — count-based flush
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_count_threshold_waiting(tmp_db, queue_runner):
    """Items below threshold → waiting=True, items stored in DB."""
    step = _Step(queue_name="q1", collect_until=3, collect_for=None, flush_to=None)
    ctx = _Context(last_output={"value": 1})

    with _mock_brix_db("brix.runners.queue.BrixDB", tmp_db):
        result = await queue_runner.execute(step, ctx)

    assert result["success"] is True
    assert result["data"]["waiting"] is True
    assert result["data"]["buffered"] == 1
    assert result["data"]["threshold"] == 3

    # Confirm item is in DB
    from brix.runners.queue import _ensure_queue_buffer_table, _get_buffer
    _ensure_queue_buffer_table(tmp_db)
    row = _get_buffer(tmp_db, "q1")
    assert row is not None
    items = json.loads(row["items"])
    assert len(items) == 1


@pytest.mark.asyncio
async def test_queue_count_threshold_flush(tmp_db, queue_runner):
    """Exactly N items → flushed, buffer cleared."""
    from brix.runners.queue import _ensure_queue_buffer_table, _upsert_buffer, _get_buffer

    _ensure_queue_buffer_table(tmp_db)
    # Pre-fill 2 items
    _upsert_buffer(tmp_db, "q_flush", [{"v": 1}, {"v": 2}], datetime.now(timezone.utc).isoformat(), "p")

    step = _Step(queue_name="q_flush", collect_until=3, collect_for=None, flush_to="next_step")
    ctx = _Context(last_output={"v": 3})

    with _mock_brix_db("brix.runners.queue.BrixDB", tmp_db):
        result = await queue_runner.execute(step, ctx)

    assert result["success"] is True
    data = result["data"]
    assert data["waiting"] is False
    assert data["flushed"] == 3
    assert len(data["items"]) == 3

    # Buffer should be cleared
    row = _get_buffer(tmp_db, "q_flush")
    assert row is None


@pytest.mark.asyncio
async def test_queue_accumulates_across_calls(tmp_db, queue_runner):
    """Multiple calls accumulate items until threshold."""
    from brix.runners.queue import _ensure_queue_buffer_table, _get_buffer
    _ensure_queue_buffer_table(tmp_db)

    result = None
    for i in range(1, 4):
        step = _Step(queue_name="q_accum", collect_until=4, collect_for=None)
        ctx = _Context(last_output={"i": i})
        with _mock_brix_db("brix.runners.queue.BrixDB", tmp_db):
            result = await queue_runner.execute(step, ctx)

    # After 3 items: still waiting
    assert result["data"]["waiting"] is True
    assert result["data"]["buffered"] == 3

    # 4th call should flush
    step = _Step(queue_name="q_accum", collect_until=4, collect_for=None)
    ctx = _Context(last_output={"i": 4})
    with _mock_brix_db("brix.runners.queue.BrixDB", tmp_db):
        result = await queue_runner.execute(step, ctx)

    assert result["data"]["waiting"] is False
    assert result["data"]["flushed"] == 4


@pytest.mark.asyncio
async def test_queue_no_item_just_checks(tmp_db, queue_runner):
    """If no input item, runner still checks threshold without adding."""
    from brix.runners.queue import _ensure_queue_buffer_table
    _ensure_queue_buffer_table(tmp_db)

    step = _Step(queue_name="q_empty", collect_until=5)
    ctx = _Context(last_output=None)

    with _mock_brix_db("brix.runners.queue.BrixDB", tmp_db):
        result = await queue_runner.execute(step, ctx)

    assert result["success"] is True
    assert result["data"]["buffered"] == 0
    assert result["data"]["waiting"] is True


# ---------------------------------------------------------------------------
# Queue Runner — time-based flush
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_time_based_waiting(tmp_db, queue_runner):
    """collect_for with future expiry → waiting=True."""
    from brix.runners.queue import _ensure_queue_buffer_table
    _ensure_queue_buffer_table(tmp_db)

    step = _Step(queue_name="q_time", collect_for="1h", collect_until=None)
    ctx = _Context(last_output={"x": 1})

    with _mock_brix_db("brix.runners.queue.BrixDB", tmp_db):
        result = await queue_runner.execute(step, ctx)

    assert result["success"] is True
    assert result["data"]["waiting"] is True


@pytest.mark.asyncio
async def test_queue_time_based_flush(tmp_db, queue_runner):
    """collect_for with past expiry → flush immediately."""
    from brix.runners.queue import _ensure_queue_buffer_table, _upsert_buffer

    _ensure_queue_buffer_table(tmp_db)
    # created_at in the distant past
    old_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    _upsert_buffer(tmp_db, "q_timeflush", [{"a": 1}], old_time, "p")

    step = _Step(queue_name="q_timeflush", collect_for="1h", collect_until=None)
    ctx = _Context(last_output={"a": 2})

    with _mock_brix_db("brix.runners.queue.BrixDB", tmp_db):
        result = await queue_runner.execute(step, ctx)

    assert result["data"]["waiting"] is False
    assert result["data"]["flushed"] == 2


# ---------------------------------------------------------------------------
# Queue Runner — config schema and metadata
# ---------------------------------------------------------------------------


def test_queue_runner_config_schema(queue_runner):
    schema = queue_runner.config_schema()
    assert "queue_name" in schema["properties"]
    assert "collect_until" in schema["properties"]
    assert "collect_for" in schema["properties"]


def test_queue_runner_io_types(queue_runner):
    assert queue_runner.input_type() == "any"
    assert queue_runner.output_type() == "dict"


# ---------------------------------------------------------------------------
# Emit Runner — event emission
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_emit_stores_event_in_db(tmp_db, emit_runner):
    """Emitting an event inserts a row into event_bus."""
    from brix.runners.emit import _ensure_event_bus_table, peek_events

    step = _Step(event="order.created", data={"amount": 99})
    ctx = _Context()

    with _mock_brix_db("brix.runners.emit.BrixDB", tmp_db):
        result = await emit_runner.execute(step, ctx)

    assert result["success"] is True
    assert result["data"]["event_name"] == "order.created"
    assert "event_id" in result["data"]

    # Confirm it's in DB
    _ensure_event_bus_table(tmp_db)
    events = peek_events(tmp_db, "order.created")
    assert len(events) == 1
    assert events[0]["data"] == {"amount": 99}
    assert events[0]["consumed"] is False


@pytest.mark.asyncio
async def test_emit_multiple_events(tmp_db, emit_runner):
    """Multiple emissions create multiple rows."""
    from brix.runners.emit import _ensure_event_bus_table, peek_events

    for i in range(3):
        step = _Step(event="batch.item", data={"n": i})
        ctx = _Context()
        with _mock_brix_db("brix.runners.emit.BrixDB", tmp_db):
            await emit_runner.execute(step, ctx)

    _ensure_event_bus_table(tmp_db)
    events = peek_events(tmp_db, "batch.item")
    assert len(events) == 3


@pytest.mark.asyncio
async def test_emit_consume_marks_consumed(tmp_db, emit_runner):
    """consume_events marks events as consumed."""
    from brix.runners.emit import _ensure_event_bus_table, consume_events, peek_events

    step = _Step(event="user.signup", data={"email": "a@b.com"})
    ctx = _Context()
    with _mock_brix_db("brix.runners.emit.BrixDB", tmp_db):
        await emit_runner.execute(step, ctx)

    _ensure_event_bus_table(tmp_db)
    consumed = consume_events(tmp_db, "user.signup")
    assert len(consumed) == 1
    assert consumed[0]["data"]["email"] == "a@b.com"

    # Second consume should return empty
    consumed2 = consume_events(tmp_db, "user.signup")
    assert len(consumed2) == 0

    # peek with consumed=True should show the event
    all_events = peek_events(tmp_db, "user.signup", consumed=True)
    assert len(all_events) == 1
    assert all_events[0]["consumed"] is True


@pytest.mark.asyncio
async def test_emit_missing_event_name_returns_error(tmp_db, emit_runner):
    """EmitRunner without 'event' config returns error."""
    step = _Step(event=None, data={"x": 1})
    ctx = _Context()
    with _mock_brix_db("brix.runners.emit.BrixDB", tmp_db):
        result = await emit_runner.execute(step, ctx)
    assert result["success"] is False
    assert "event" in result["error"].lower()


def test_emit_runner_config_schema(emit_runner):
    schema = emit_runner.config_schema()
    assert "event" in schema["properties"]
    assert "data" in schema["properties"]
    assert "event" in schema.get("required", [])


def test_emit_runner_io_types(emit_runner):
    assert emit_runner.input_type() == "any"
    assert emit_runner.output_type() == "dict"


# ---------------------------------------------------------------------------
# Debounce — trigger-level quiet-period logic
# ---------------------------------------------------------------------------


def test_debounce_record_event_creates_state(tmp_db):
    from brix.triggers.debounce import record_event, get_state

    state = record_event(tmp_db, "trigger-x", "5m")
    assert state["trigger_name"] == "trigger-x"
    assert "last_event_at" in state
    assert "scheduled_at" in state

    persisted = get_state(tmp_db, "trigger-x")
    assert persisted is not None
    assert persisted["trigger_name"] == "trigger-x"


def test_debounce_not_ready_before_window(tmp_db):
    from brix.triggers.debounce import record_event, is_ready_to_fire

    record_event(tmp_db, "trigger-wait", "5m")
    # Window is 5m from now — should NOT be ready yet
    assert is_ready_to_fire(tmp_db, "trigger-wait") is False


def test_debounce_ready_after_window(tmp_db):
    from brix.triggers.debounce import is_ready_to_fire, _ensure_table

    _ensure_table(tmp_db)
    # Manually insert state with scheduled_at in the past
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    with tmp_db._connect() as conn:
        conn.execute(
            "INSERT INTO debounce_state (trigger_name, last_event_at, scheduled_at) VALUES (?, ?, ?)",
            ("trigger-past", past, past),
        )

    assert is_ready_to_fire(tmp_db, "trigger-past") is True


def test_debounce_clear_state(tmp_db):
    from brix.triggers.debounce import record_event, clear_state, get_state

    record_event(tmp_db, "trigger-clear", "1m")
    clear_state(tmp_db, "trigger-clear")
    assert get_state(tmp_db, "trigger-clear") is None


def test_debounce_reset_on_new_event(tmp_db):
    """New event resets the scheduled_at to now + debounce_seconds."""
    from brix.triggers.debounce import record_event, get_state

    state1 = record_event(tmp_db, "trigger-reset", "5m")
    scheduled1 = datetime.fromisoformat(state1["scheduled_at"])

    time.sleep(0.05)  # tiny sleep to ensure time advances

    state2 = record_event(tmp_db, "trigger-reset", "5m")
    scheduled2 = datetime.fromisoformat(state2["scheduled_at"])

    # scheduled_at should be strictly later after reset
    assert scheduled2 >= scheduled1


def test_debounce_no_state_not_ready(tmp_db):
    from brix.triggers.debounce import is_ready_to_fire

    # No state at all → not ready
    assert is_ready_to_fire(tmp_db, "trigger-nonexistent") is False


def test_debounce_three_events_single_fire(tmp_db):
    """3 rapid events should only result in 1 ready-to-fire after window."""
    from brix.triggers.debounce import record_event, is_ready_to_fire, get_state, _ensure_table

    _ensure_table(tmp_db)
    for _ in range(3):
        record_event(tmp_db, "trigger-burst", "1m")

    # Not ready yet (window = 1m from now)
    assert is_ready_to_fire(tmp_db, "trigger-burst") is False

    # Simulate time passing: set scheduled_at to past
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    with tmp_db._connect() as conn:
        conn.execute(
            "UPDATE debounce_state SET scheduled_at = ? WHERE trigger_name = ?",
            (past, "trigger-burst"),
        )

    # Now should be ready
    assert is_ready_to_fire(tmp_db, "trigger-burst") is True
    # State still exists (caller clears it)
    assert get_state(tmp_db, "trigger-burst") is not None


# ---------------------------------------------------------------------------
# TriggerConfig — debounce field present
# ---------------------------------------------------------------------------


def test_trigger_config_has_debounce_field():
    from brix.triggers.models import TriggerConfig

    cfg = TriggerConfig(id="t1", type="mail", pipeline="my_pipeline", debounce="5m")
    assert cfg.debounce == "5m"


def test_trigger_config_debounce_optional():
    from brix.triggers.models import TriggerConfig

    cfg = TriggerConfig(id="t2", type="file", pipeline="my_pipeline")
    assert cfg.debounce is None


# ---------------------------------------------------------------------------
# Step model — new fields present
# ---------------------------------------------------------------------------


def test_step_model_has_queue_fields():
    from brix.models import Step

    s = Step(id="s1", type="queue", queue_name="myq", collect_until=5)
    assert s.queue_name == "myq"
    assert s.collect_until == 5
    assert s.collect_for is None
    assert s.flush_to is None


def test_step_model_has_emit_fields():
    from brix.models import Step

    s = Step(id="s2", type="emit", event="my.event", data={"k": "v"})
    assert s.event == "my.event"
    assert s.data == {"k": "v"}


def test_step_model_stream_field_default_false():
    from brix.models import Step

    s = Step(id="s3", type="python", script="echo.py")
    assert s.stream is False


def test_step_model_stream_field_settable():
    from brix.models import Step

    s = Step(id="s4", type="python", script="echo.py", stream=True)
    assert s.stream is True


# ---------------------------------------------------------------------------
# Engine registration — new runners are available
# ---------------------------------------------------------------------------


def test_engine_has_queue_runner():
    from brix.engine import PipelineEngine
    engine = PipelineEngine()
    assert "queue" in engine._runners


def test_engine_has_emit_runner():
    from brix.engine import PipelineEngine
    engine = PipelineEngine()
    assert "emit" in engine._runners
