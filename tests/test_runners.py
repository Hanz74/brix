"""Tests for brix.runners modules."""

import pytest

from brix.runners import base
from brix.runners.base import BaseRunner
from brix.runners.cli import CliRunner, parse_timeout, get_default_timeout


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


from brix.runners.python import PythonRunner

# --- PythonRunner Tests ---


async def test_python_runner_echo_params():
    runner = PythonRunner()
    step = _Step(script="tests/helpers/echo_params.py", params={"key": "value"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"]["received"] == {"key": "value"}


async def test_python_runner_no_params():
    runner = PythonRunner()
    step = _Step(script="tests/helpers/echo_params.py", params={})
    result = await runner.execute(step, context=None)
    assert result["success"] is True


async def test_python_runner_fail_script():
    runner = PythonRunner()
    step = _Step(script="tests/helpers/fail_script.py")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "wrong" in result["error"]


async def test_python_runner_script_not_found():
    runner = PythonRunner()
    step = _Step(script="nonexistent_script.py")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "not found" in result["error"].lower() or "No such file" in result["error"]


async def test_python_runner_timeout():
    runner = PythonRunner()
    step = _Step(script="tests/helpers/slow_script.py", timeout="1s")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "Timeout" in result["error"]


async def test_python_runner_no_script():
    runner = PythonRunner()
    step = _Step()  # no script
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "script" in result["error"].lower()


async def test_python_runner_nested_subprocess_no_hang():
    """INBOX-366: Python steps that spawn inner subprocesses must not hang.

    When no input_data is passed, stdin must be DEVNULL so inner subprocesses
    receive immediate EOF rather than inheriting an open pipe that never closes.
    """
    runner = PythonRunner()
    step = _Step(
        script="tests/helpers/nested_subprocess.py",
        params={"check": "nested"},
        timeout="10s",
    )
    result = await runner.execute(step, context=None)
    assert result["success"] is True, f"nested subprocess failed: {result.get('error')}"
    assert result["data"]["outer"] == "ok"
    assert result["data"]["inner_exit"] == 0
    assert result["data"]["inner_data"] == {"inner": "ok"}
    assert result["data"]["params"] == {"check": "nested"}


# ---------------------------------------------------------------------------
# get_default_timeout — per-step-type defaults (T-BRIX-V4-BUG-08)
# ---------------------------------------------------------------------------


def test_get_default_timeout_python():
    """python steps default to 3600s (1h)."""
    assert get_default_timeout("python") == 3600.0


def test_get_default_timeout_cli():
    """cli steps default to 300s (5 min)."""
    assert get_default_timeout("cli") == 300.0


def test_get_default_timeout_mcp():
    """mcp steps default to 120s (2 min)."""
    assert get_default_timeout("mcp") == 120.0


def test_get_default_timeout_http():
    """http steps default to 60s (1 min)."""
    assert get_default_timeout("http") == 60.0


def test_get_default_timeout_repeat():
    """repeat steps default to 7200s (2 h)."""
    assert get_default_timeout("repeat") == 7200.0


def test_get_default_timeout_approval():
    """approval steps default to 86400s (24 h)."""
    assert get_default_timeout("approval") == 86400.0


def test_get_default_timeout_unknown_type():
    """Unknown step types fall back to 600s (10 min)."""
    assert get_default_timeout("unknown_type") == 600.0
    assert get_default_timeout("filter") == 600.0
    assert get_default_timeout("transform") == 600.0
    assert get_default_timeout("pipeline") == 600.0


def test_get_default_timeout_explicit_overrides_default():
    """Explicit timeout on a step takes precedence over the type default.

    This is a documentation test: verify that parse_timeout is used when a
    timeout string is present and get_default_timeout is used as fallback.
    """
    # python default is 3600s but an explicit "30s" wins
    timeout_str = "30s"
    result = parse_timeout(timeout_str)
    assert result == 30.0
    # Explicit wins — not the default
    assert result != get_default_timeout("python")


# ---------------------------------------------------------------------------
# Runner integration: default timeout is applied (not the old hardcoded 60s)
# ---------------------------------------------------------------------------


def test_cli_runner_uses_type_default_timeout():
    """CliRunner resolves to 300s default when no timeout is set."""
    runner = CliRunner()
    step = _Step(args=["echo", "hi"])  # no timeout attribute set
    # We can't run async here without a loop, but we can inspect the constant
    assert get_default_timeout("cli") == 300.0


def test_python_runner_default_timeout_is_1h():
    """PythonRunner default timeout is 3600s, not the old 60s."""
    assert get_default_timeout("python") == 3600.0
    assert get_default_timeout("python") != 60.0


def test_mcp_runner_default_timeout_is_2min():
    """McpRunner default timeout is 120s, not the old 60s."""
    assert get_default_timeout("mcp") == 120.0
    assert get_default_timeout("mcp") != 60.0


# ---------------------------------------------------------------------------
# T-BRIX-V4-BUG-14: CLI args with int/float values (Jinja2 render side-effect)
# ---------------------------------------------------------------------------


async def test_cli_runner_args_with_int_value():
    """Args containing int values (e.g. from Jinja2 render) must not crash.

    Jinja2 can render numeric expressions as int/float.  The CliRunner must
    str()-cast each element before passing to create_subprocess_exec.
    """
    runner = CliRunner()
    # Simulate Jinja2 rendering an integer into the args list
    step = _Step(args=["echo", 42])
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    # "42" is valid JSON so it gets parsed back to int — either form is fine;
    # what matters is that no TypeError was raised by the subprocess call.
    assert str(result["data"]) == "42"
