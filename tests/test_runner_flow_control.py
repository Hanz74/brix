"""Tests for flow-control runners: Switch, Merge, ErrorHandler, Wait (T-BRIX-DB-17)."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from brix.runners.switch import SwitchRunner
from brix.runners.merge import MergeRunner, _merge_append, _merge_zip, _merge_lookup
from brix.runners.error_handler import ErrorHandlerRunner
from brix.runners.wait import WaitRunner
from brix.runners.base import BaseRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step object that mirrors how the engine passes config."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _Context:
    """Minimal pipeline context with Jinja2 context support and outputs."""

    def __init__(self, jinja_ctx: dict | None = None, outputs: dict | None = None):
        self._jinja_ctx = jinja_ctx or {}
        self._outputs: dict = outputs or {}

    def to_jinja_context(self) -> dict:
        return dict(self._jinja_ctx)

    def get_output(self, step_id: str):
        return self._outputs.get(step_id)


# ---------------------------------------------------------------------------
# SwitchRunner
# ---------------------------------------------------------------------------


class TestSwitchRunnerSchema:
    def test_config_schema_structure(self):
        runner = SwitchRunner()
        schema = runner.config_schema()
        assert schema["type"] == "object"
        assert "field" in schema["properties"]
        assert "cases" in schema["properties"]
        assert "default" in schema["properties"]
        assert "field" in schema["required"]
        assert "cases" in schema["required"]

    def test_input_type(self):
        assert SwitchRunner().input_type() == "any"

    def test_output_type(self):
        assert SwitchRunner().output_type() == "dict"

    def test_inherits_base_runner(self):
        assert issubclass(SwitchRunner, BaseRunner)

    def test_report_progress_called(self):
        runner = SwitchRunner()
        assert runner._progress is None  # not yet called


class TestSwitchRunnerMatch:
    async def test_exact_match(self):
        runner = SwitchRunner()
        ctx = _Context(jinja_ctx={"item": {"status": "approved"}})
        step = _Step(
            field="{{ item.status }}",
            cases={"approved": "step_approve", "rejected": "step_reject"},
        )
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert result["data"]["matched_case"] == "approved"
        assert result["data"]["target_step"] == "step_approve"
        assert result["data"]["evaluated_value"] == "approved"

    async def test_second_case_match(self):
        runner = SwitchRunner()
        ctx = _Context(jinja_ctx={"item": {"status": "rejected"}})
        step = _Step(
            field="{{ item.status }}",
            cases={"approved": "step_approve", "rejected": "step_reject"},
        )
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert result["data"]["matched_case"] == "rejected"
        assert result["data"]["target_step"] == "step_reject"

    async def test_default_used_when_no_match(self):
        runner = SwitchRunner()
        ctx = _Context(jinja_ctx={"item": {"status": "unknown"}})
        step = _Step(
            field="{{ item.status }}",
            cases={"approved": "step_approve"},
            default="step_fallback",
        )
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert result["data"]["matched_case"] is None
        assert result["data"]["target_step"] == "step_fallback"

    async def test_no_match_no_default_returns_error(self):
        runner = SwitchRunner()
        ctx = _Context(jinja_ctx={"item": {"status": "unknown"}})
        step = _Step(
            field="{{ item.status }}",
            cases={"approved": "step_approve"},
        )
        result = await runner.execute(step, ctx)

        assert result["success"] is False
        assert "no case matched" in result["error"]
        assert "unknown" in result["error"]

    async def test_missing_field_config_returns_error(self):
        runner = SwitchRunner()
        step = _Step(cases={"a": "step_a"})
        result = await runner.execute(step, None)

        assert result["success"] is False
        assert "field" in result["error"]

    async def test_report_progress_set_after_match(self):
        runner = SwitchRunner()
        ctx = _Context(jinja_ctx={"val": "x"})
        step = _Step(field="{{ val }}", cases={"x": "step_x"})
        await runner.execute(step, ctx)

        assert runner._progress is not None
        assert runner._progress["pct"] == 100.0

    async def test_static_field_value(self):
        """Non-template static value matches correctly."""
        runner = SwitchRunner()
        step = _Step(field="hello", cases={"hello": "step_hello"})
        result = await runner.execute(step, None)

        assert result["success"] is True
        assert result["data"]["target_step"] == "step_hello"

    async def test_duration_in_result(self):
        runner = SwitchRunner()
        ctx = _Context(jinja_ctx={"v": "a"})
        step = _Step(field="{{ v }}", cases={"a": "step_a"})
        result = await runner.execute(step, ctx)

        assert "duration" in result
        assert result["duration"] >= 0.0


# ---------------------------------------------------------------------------
# MergeRunner
# ---------------------------------------------------------------------------


class TestMergeRunnerSchema:
    def test_config_schema_structure(self):
        runner = MergeRunner()
        schema = runner.config_schema()
        assert schema["type"] == "object"
        assert "inputs" in schema["properties"]
        assert "mode" in schema["properties"]
        assert "key" in schema["properties"]
        assert "inputs" in schema["required"]

    def test_input_type(self):
        assert MergeRunner().input_type() == "any"

    def test_output_type(self):
        assert MergeRunner().output_type() == "list[dict]"

    def test_inherits_base_runner(self):
        assert issubclass(MergeRunner, BaseRunner)


class TestMergeRunnerAppend:
    async def test_append_two_lists(self):
        ctx = _Context(
            outputs={
                "step_a": [{"id": 1}, {"id": 2}],
                "step_b": [{"id": 3}],
            }
        )
        runner = MergeRunner()
        step = _Step(inputs=["step_a", "step_b"], mode="append")
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert len(result["data"]) == 3
        ids = [item["id"] for item in result["data"]]
        assert ids == [1, 2, 3]

    async def test_append_single_list(self):
        ctx = _Context(outputs={"step_a": [{"x": 1}, {"x": 2}]})
        runner = MergeRunner()
        step = _Step(inputs=["step_a"], mode="append")
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert len(result["data"]) == 2

    async def test_append_empty_inputs(self):
        runner = MergeRunner()
        step = _Step(inputs=[], mode="append")
        result = await runner.execute(step, None)

        assert result["success"] is True
        assert result["data"] == []

    async def test_append_default_mode(self):
        """mode defaults to 'append' when not specified."""
        ctx = _Context(outputs={"step_a": [{"v": 1}], "step_b": [{"v": 2}]})
        runner = MergeRunner()
        step = _Step(inputs=["step_a", "step_b"])
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert len(result["data"]) == 2


class TestMergeRunnerZip:
    async def test_zip_equal_length(self):
        ctx = _Context(
            outputs={
                "step_users": [{"user_id": 1, "name": "Alice"}, {"user_id": 2, "name": "Bob"}],
                "step_scores": [{"score": 90}, {"score": 75}],
            }
        )
        runner = MergeRunner()
        step = _Step(inputs=["step_users", "step_scores"], mode="zip")
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert len(result["data"]) == 2
        assert result["data"][0]["name"] == "Alice"
        assert result["data"][0]["score"] == 90
        assert result["data"][1]["name"] == "Bob"
        assert result["data"][1]["score"] == 75

    async def test_zip_unequal_length_takes_longer(self):
        ctx = _Context(
            outputs={
                "step_a": [{"a": 1}, {"a": 2}, {"a": 3}],
                "step_b": [{"b": 10}],
            }
        )
        runner = MergeRunner()
        step = _Step(inputs=["step_a", "step_b"], mode="zip")
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        # zip extends to the longer list
        assert len(result["data"]) == 3
        assert result["data"][0]["a"] == 1
        assert result["data"][0]["b"] == 10
        assert "b" not in result["data"][1]  # no match from step_b


class TestMergeRunnerLookup:
    async def test_lookup_basic_join(self):
        ctx = _Context(
            outputs={
                "step_users": [
                    {"user_id": 1, "name": "Alice"},
                    {"user_id": 2, "name": "Bob"},
                ],
                "step_orders": [
                    {"user_id": 1, "order_count": 5},
                    {"user_id": 2, "order_count": 3},
                ],
            }
        )
        runner = MergeRunner()
        step = _Step(inputs=["step_users", "step_orders"], mode="lookup", key="user_id")
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert len(result["data"]) == 2
        alice = next(r for r in result["data"] if r["name"] == "Alice")
        assert alice["order_count"] == 5

    async def test_lookup_missing_key_in_right_returns_base_only(self):
        ctx = _Context(
            outputs={
                "step_users": [{"user_id": 1, "name": "Alice"}, {"user_id": 3, "name": "Carol"}],
                "step_orders": [{"user_id": 1, "order_count": 5}],
            }
        )
        runner = MergeRunner()
        step = _Step(inputs=["step_users", "step_orders"], mode="lookup", key="user_id")
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        carol = next(r for r in result["data"] if r["name"] == "Carol")
        assert "order_count" not in carol

    async def test_lookup_missing_key_config_returns_error(self):
        runner = MergeRunner()
        step = _Step(inputs=["a", "b"], mode="lookup")
        result = await runner.execute(step, _Context())

        assert result["success"] is False
        assert "key" in result["error"]

    async def test_report_progress_called(self):
        ctx = _Context(outputs={"step_a": [{"x": 1}]})
        runner = MergeRunner()
        step = _Step(inputs=["step_a"])
        await runner.execute(step, ctx)

        assert runner._progress is not None
        assert runner._progress["pct"] == 100.0


# ---------------------------------------------------------------------------
# ErrorHandlerRunner
# ---------------------------------------------------------------------------


class TestErrorHandlerRunnerSchema:
    def test_config_schema_structure(self):
        runner = ErrorHandlerRunner()
        schema = runner.config_schema()
        assert schema["type"] == "object"
        assert "try_step" in schema["properties"]
        assert "handler_step" in schema["properties"]
        assert "try_step" in schema["required"]

    def test_input_type(self):
        assert ErrorHandlerRunner().input_type() == "any"

    def test_output_type(self):
        assert ErrorHandlerRunner().output_type() == "dict"

    def test_inherits_base_runner(self):
        assert issubclass(ErrorHandlerRunner, BaseRunner)

    def test_no_engine_returns_error(self):
        runner = ErrorHandlerRunner()
        result = asyncio.get_event_loop().run_until_complete(
            runner.execute(_Step(try_step="s"), None)
        )
        assert result["success"] is False
        assert "not connected to engine" in result["error"]


class TestErrorHandlerRunnerSuccessPath:
    async def test_try_step_succeeds(self):
        """When try_step succeeds, handler is not called."""
        mock_engine = MagicMock()
        mock_engine._find_step = MagicMock(return_value=_Step(id="fetch"))
        mock_engine._execute_step = AsyncMock(
            return_value={"success": True, "data": {"value": 42}}
        )

        runner = ErrorHandlerRunner(engine=mock_engine)
        step = _Step(try_step="fetch", handler_step="fallback")
        result = await runner.execute(step, None)

        assert result["success"] is True
        assert result["data"]["success"] is True
        assert result["data"]["result"] == {"value": 42}
        assert result["data"]["used_handler"] is False
        assert result["data"]["error"] is None
        mock_engine._execute_step.assert_called_once()

    async def test_try_step_fails_handler_runs(self):
        """When try_step fails, handler_step is executed."""
        mock_engine = MagicMock()
        mock_engine._find_step = MagicMock(return_value=_Step(id="step"))

        call_count = 0

        async def fake_execute(step_obj, ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"success": False, "error": "connection refused"}
            return {"success": True, "data": {"fallback": "data"}}

        mock_engine._execute_step = fake_execute

        runner = ErrorHandlerRunner(engine=mock_engine)
        step = _Step(try_step="fetch", handler_step="fallback")
        result = await runner.execute(step, None)

        assert result["success"] is True
        assert result["data"]["success"] is False
        assert result["data"]["used_handler"] is True
        assert result["data"]["error"] == "connection refused"
        assert result["data"]["result"] == {"fallback": "data"}
        assert call_count == 2

    async def test_try_fails_no_handler(self):
        """When try_step fails and no handler configured, returns error details."""
        mock_engine = MagicMock()
        mock_engine._find_step = MagicMock(return_value=_Step(id="step"))
        mock_engine._execute_step = AsyncMock(
            return_value={"success": False, "error": "timeout"}
        )

        runner = ErrorHandlerRunner(engine=mock_engine)
        step = _Step(try_step="fetch")
        result = await runner.execute(step, None)

        assert result["success"] is True
        assert result["data"]["success"] is False
        assert result["data"]["used_handler"] is False
        assert result["data"]["error"] == "timeout"

    async def test_handler_also_fails(self):
        """When both try and handler fail, overall success=False."""
        mock_engine = MagicMock()
        mock_engine._find_step = MagicMock(return_value=_Step(id="step"))
        mock_engine._execute_step = AsyncMock(
            return_value={"success": False, "error": "all broken"}
        )

        runner = ErrorHandlerRunner(engine=mock_engine)
        step = _Step(try_step="fetch", handler_step="fallback")
        result = await runner.execute(step, None)

        assert result["success"] is False
        assert "handler step also failed" in result["error"]

    async def test_missing_try_step_config(self):
        mock_engine = MagicMock()
        runner = ErrorHandlerRunner(engine=mock_engine)
        step = _Step(handler_step="fallback")
        result = await runner.execute(step, None)

        assert result["success"] is False
        assert "try_step" in result["error"]

    async def test_report_progress_called(self):
        mock_engine = MagicMock()
        mock_engine._find_step = MagicMock(return_value=_Step(id="s"))
        mock_engine._execute_step = AsyncMock(
            return_value={"success": True, "data": None}
        )

        runner = ErrorHandlerRunner(engine=mock_engine)
        step = _Step(try_step="s", handler_step="h")
        await runner.execute(step, None)

        assert runner._progress is not None
        assert runner._progress["pct"] == 100.0


# ---------------------------------------------------------------------------
# WaitRunner
# ---------------------------------------------------------------------------


class TestWaitRunnerSchema:
    def test_config_schema_structure(self):
        runner = WaitRunner()
        schema = runner.config_schema()
        assert schema["type"] == "object"
        assert "seconds" in schema["properties"]
        assert "until" in schema["properties"]
        assert "poll_interval" in schema["properties"]
        assert "timeout" in schema["properties"]

    def test_input_type(self):
        assert WaitRunner().input_type() == "none"

    def test_output_type(self):
        assert WaitRunner().output_type() == "dict"

    def test_inherits_base_runner(self):
        assert issubclass(WaitRunner, BaseRunner)


class TestWaitRunnerSeconds:
    async def test_wait_zero_seconds(self):
        runner = WaitRunner()
        step = _Step(seconds=0)
        result = await runner.execute(step, None)

        assert result["success"] is True
        assert result["data"]["timed_out"] is False
        assert result["data"]["condition_met"] is None
        assert result["data"]["waited_seconds"] >= 0.0

    async def test_wait_small_duration(self):
        """Wait 0.05s — should complete quickly."""
        runner = WaitRunner()
        step = _Step(seconds=0.05)
        result = await runner.execute(step, None)

        assert result["success"] is True
        assert result["data"]["waited_seconds"] >= 0.05
        assert result["data"]["timed_out"] is False

    async def test_wait_seconds_no_context_needed(self):
        """seconds-based wait works without context."""
        runner = WaitRunner()
        step = _Step(seconds=0.01)
        result = await runner.execute(step, None)

        assert result["success"] is True

    async def test_report_progress_called_seconds(self):
        runner = WaitRunner()
        step = _Step(seconds=0.01)
        await runner.execute(step, None)

        assert runner._progress is not None
        assert runner._progress["pct"] == 100.0


class TestWaitRunnerUntil:
    async def test_until_condition_already_true(self):
        """Condition is true on the first check — resolves immediately."""
        ctx = _Context(jinja_ctx={"status": "ready"})
        runner = WaitRunner()
        step = _Step(until="{{ status == 'ready' }}", poll_interval=0.01, timeout=5.0)
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert result["data"]["condition_met"] is True
        assert result["data"]["timed_out"] is False

    async def test_until_condition_becomes_true_after_polls(self):
        """Condition becomes true after 2 polls."""
        poll_calls = [0]

        ctx = _Context()

        original_evaluate = None

        from brix.loader import PipelineLoader

        def patched_evaluate(self_loader, condition, jinja_ctx):
            poll_calls[0] += 1
            return poll_calls[0] >= 2

        with patch.object(PipelineLoader, "evaluate_condition", patched_evaluate):
            runner = WaitRunner()
            step = _Step(until="{{ flag }}", poll_interval=0.01, timeout=5.0)
            result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert result["data"]["condition_met"] is True
        assert result["data"]["poll_count"] >= 1

    async def test_until_timeout(self):
        """Condition never becomes true — times out."""
        ctx = _Context(jinja_ctx={"flag": False})
        runner = WaitRunner()
        step = _Step(until="{{ flag }}", poll_interval=0.01, timeout=0.05)
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert result["data"]["timed_out"] is True
        assert result["data"]["condition_met"] is False

    async def test_until_expression_error(self):
        """Broken Jinja2 expression surfaces as error."""
        from brix.loader import PipelineLoader

        def broken_evaluate(self_loader, condition, jinja_ctx):
            raise ValueError("template syntax error")

        with patch.object(PipelineLoader, "evaluate_condition", broken_evaluate):
            runner = WaitRunner()
            ctx = _Context()
            step = _Step(until="{{ broken }}", poll_interval=0.01, timeout=5.0)
            result = await runner.execute(step, ctx)

        assert result["success"] is False
        assert "until expression error" in result["error"]

    async def test_no_config_noop(self):
        """No seconds and no until — returns immediately without error."""
        runner = WaitRunner()
        step = _Step()
        result = await runner.execute(step, None)

        assert result["success"] is True
        assert result["data"]["waited_seconds"] == 0.0

    async def test_report_progress_called_until(self):
        ctx = _Context(jinja_ctx={"done": True})
        runner = WaitRunner()
        step = _Step(until="{{ done }}", poll_interval=0.01, timeout=5.0)
        await runner.execute(step, ctx)

        assert runner._progress is not None
        assert runner._progress["pct"] == 100.0


# ---------------------------------------------------------------------------
# Helper function unit tests
# ---------------------------------------------------------------------------


class TestMergeHelpers:
    def test_merge_append_basic(self):
        result = _merge_append([[{"a": 1}], [{"b": 2}, {"b": 3}]])
        assert len(result) == 3

    def test_merge_append_empty(self):
        assert _merge_append([]) == []

    def test_merge_append_single(self):
        assert _merge_append([[{"x": 1}]]) == [{"x": 1}]

    def test_merge_zip_basic(self):
        result = _merge_zip([[{"a": 1}, {"a": 2}], [{"b": 10}, {"b": 20}]])
        assert result[0] == {"a": 1, "b": 10}
        assert result[1] == {"a": 2, "b": 20}

    def test_merge_zip_unequal(self):
        result = _merge_zip([[{"a": 1}, {"a": 2}], [{"b": 10}]])
        assert len(result) == 2
        assert result[0]["b"] == 10
        assert "b" not in result[1]

    def test_merge_zip_empty(self):
        assert _merge_zip([]) == []

    def test_merge_lookup_basic(self):
        base = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        extra = [{"id": 1, "score": 99}, {"id": 2, "score": 50}]
        result = _merge_lookup([base, extra], "id")
        assert result[0]["score"] == 99
        assert result[1]["score"] == 50

    def test_merge_lookup_no_match(self):
        base = [{"id": 1, "name": "A"}]
        extra = [{"id": 99, "score": 0}]
        result = _merge_lookup([base, extra], "id")
        assert "score" not in result[0]

    def test_merge_lookup_preserves_base_fields(self):
        base = [{"id": 1, "name": "Alice"}]
        extra = [{"id": 1, "dept": "Eng"}]
        result = _merge_lookup([base, extra], "id")
        assert result[0]["name"] == "Alice"
        assert result[0]["dept"] == "Eng"
