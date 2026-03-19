"""Tests for brix.runners modules."""

import pytest

from brix.runners import base
from brix.runners.base import BaseRunner
from brix.runners.cli import CliRunner, parse_timeout


def test_base_runner_module_exists():
    """Base runner module is importable and has a docstring."""
    assert base.__doc__ is not None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step stand-in for tests."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# BaseRunner — abstract behaviour
# ---------------------------------------------------------------------------


def test_base_runner_is_abstract():
    """BaseRunner cannot be instantiated directly (it's abstract)."""
    with pytest.raises(TypeError):
        BaseRunner()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# parse_timeout
# ---------------------------------------------------------------------------


def test_parse_timeout():
    assert parse_timeout("30s") == 30.0
    assert parse_timeout("5m") == 300.0
    assert parse_timeout("1h") == 3600.0
    assert parse_timeout("2h") == 7200.0
    assert parse_timeout("90s") == 90.0
    assert parse_timeout("0.5m") == 30.0


# ---------------------------------------------------------------------------
# CliRunner
# ---------------------------------------------------------------------------


async def test_cli_runner_args_success():
    """echo via args list succeeds and returns the echoed string."""
    runner = CliRunner()
    step = _Step(args=["echo", "hello"])
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == "hello"
    assert result["duration"] >= 0.0


async def test_cli_runner_command_success():
    """echo via shell command succeeds."""
    runner = CliRunner()
    step = _Step(command="echo hello")
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == "hello"


async def test_cli_runner_json_output():
    """Command that writes JSON to stdout returns parsed dict."""
    runner = CliRunner()
    step = _Step(args=["python3", "-c", 'import json, sys; print(json.dumps({"key": "value"}))'])
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == {"key": "value"}


async def test_cli_runner_nonzero_exit():
    """Command exiting with non-zero code returns success=False."""
    runner = CliRunner()
    step = _Step(command="exit 1")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "error" in result


async def test_cli_runner_timeout():
    """Command exceeding the timeout is killed and returns success=False."""
    runner = CliRunner()
    step = _Step(args=["sleep", "10"], timeout="1s")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "Timeout" in result["error"]
    assert result["duration"] < 5.0  # much less than 10s


async def test_cli_runner_no_args_no_command():
    """Step without 'args' or 'command' returns a descriptive error."""
    runner = CliRunner()
    step = _Step()  # no args, no command
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "args" in result["error"] or "command" in result["error"]
    assert result["duration"] == 0.0
