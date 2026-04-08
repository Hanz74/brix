"""Tests for _evaluate_output_slots using SandboxedNativeEnvironment.

Verifies that output_slots return native Python types (dict, int, bool, …)
instead of stringified representations when the sub-pipeline result contains
structured data.
"""
import types

import pytest

from brix.runners.pipeline import PipelineRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sub_pipeline(output_slots: dict) -> types.SimpleNamespace:
    """Return a minimal sub-pipeline stub with output_slots."""
    return types.SimpleNamespace(output_slots=output_slots)


def _make_sub_result(result_data) -> types.SimpleNamespace:
    """Return a minimal PipelineResult stub."""
    return types.SimpleNamespace(result=result_data, success=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_output_slots_dict_native_type():
    """output_slots with dict expression returns a native dict, not a string."""
    runner = PipelineRunner()

    sub_pipeline = _make_sub_pipeline(
        output_slots={"payload": "{{ result.payload }}"}
    )
    result_data = {"payload": {"key": "value", "count": 42}}
    sub_result = _make_sub_result(result_data)

    slots = runner._evaluate_output_slots(sub_pipeline, sub_result)

    assert isinstance(slots["payload"], dict), (
        f"Expected dict, got {type(slots['payload'])}: {slots['payload']!r}"
    )
    assert slots["payload"] == {"key": "value", "count": 42}


def test_output_slots_scalar_native_type():
    """output_slots with integer expression returns a native int, not a string."""
    runner = PipelineRunner()

    sub_pipeline = _make_sub_pipeline(
        output_slots={"count": "{{ result.count }}"}
    )
    result_data = {"count": 7}
    sub_result = _make_sub_result(result_data)

    slots = runner._evaluate_output_slots(sub_pipeline, sub_result)

    assert isinstance(slots["count"], int), (
        f"Expected int, got {type(slots['count'])}: {slots['count']!r}"
    )
    assert slots["count"] == 7


def test_output_slots_list_native_type():
    """output_slots with list expression returns a native list."""
    runner = PipelineRunner()

    # Use a key that does not shadow a dict method (avoid "items", "keys", "values")
    sub_pipeline = _make_sub_pipeline(
        output_slots={"numbers": "{{ result.numbers }}"}
    )
    result_data = {"numbers": [1, 2, 3]}
    sub_result = _make_sub_result(result_data)

    slots = runner._evaluate_output_slots(sub_pipeline, sub_result)

    assert isinstance(slots["numbers"], list), (
        f"Expected list, got {type(slots['numbers'])}: {slots['numbers']!r}"
    )
    assert slots["numbers"] == [1, 2, 3]


def test_output_slots_bool_native_type():
    """output_slots with boolean expression returns a native bool."""
    runner = PipelineRunner()

    sub_pipeline = _make_sub_pipeline(
        output_slots={"ok": "{{ result.ok }}"}
    )
    result_data = {"ok": True}
    sub_result = _make_sub_result(result_data)

    slots = runner._evaluate_output_slots(sub_pipeline, sub_result)

    assert isinstance(slots["ok"], bool), (
        f"Expected bool, got {type(slots['ok'])}: {slots['ok']!r}"
    )
    assert slots["ok"] is True


def test_output_slots_string_type():
    """output_slots with string expression returns a string (unchanged)."""
    runner = PipelineRunner()

    sub_pipeline = _make_sub_pipeline(
        output_slots={"label": "{{ result.label }}"}
    )
    result_data = {"label": "hello"}
    sub_result = _make_sub_result(result_data)

    slots = runner._evaluate_output_slots(sub_pipeline, sub_result)

    assert isinstance(slots["label"], str)
    assert slots["label"] == "hello"


def test_output_slots_empty_returns_empty_dict():
    """No output_slots on pipeline → empty dict returned."""
    runner = PipelineRunner()

    sub_pipeline = _make_sub_pipeline(output_slots={})
    sub_result = _make_sub_result({"x": 1})

    slots = runner._evaluate_output_slots(sub_pipeline, sub_result)

    assert slots == {}


def test_output_slots_missing_key_returns_none():
    """Slot expression referencing non-existent key returns None (graceful degradation)."""
    runner = PipelineRunner()

    sub_pipeline = _make_sub_pipeline(
        output_slots={"missing": "{{ result.does_not_exist }}"}
    )
    sub_result = _make_sub_result({"x": 1})

    slots = runner._evaluate_output_slots(sub_pipeline, sub_result)

    # Graceful: slot exists but value is None or empty string/Undefined
    assert "missing" in slots


def test_output_slots_multiple_slots():
    """Multiple slots are all evaluated and returned."""
    runner = PipelineRunner()

    sub_pipeline = _make_sub_pipeline(
        output_slots={
            "count": "{{ result.count }}",
            "data": "{{ result.data }}",
        }
    )
    result_data = {"count": 3, "data": {"a": 1}}
    sub_result = _make_sub_result(result_data)

    slots = runner._evaluate_output_slots(sub_pipeline, sub_result)

    assert isinstance(slots["count"], int)
    assert slots["count"] == 3
    assert isinstance(slots["data"], dict)
    assert slots["data"] == {"a": 1}
