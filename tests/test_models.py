"""Tests for brix.models module."""

import pytest
from pydantic import ValidationError

from brix import models
from brix.models import (
    CredentialRef,
    ErrorConfig,
    ForeachItem,
    ForeachResult,
    ForeachSummary,
    InputParam,
    Pipeline,
    RetryConfig,
    RunResult,
    ServerConfig,
    Step,
    StepResult,
    StepStatus,
)


# ---------------------------------------------------------------------------
# Module sanity
# ---------------------------------------------------------------------------


def test_models_module_exists():
    """Module is importable and has a docstring."""
    assert models.__doc__ is not None


# ---------------------------------------------------------------------------
# InputParam
# ---------------------------------------------------------------------------


def test_input_param_minimal():
    p = InputParam(type="str")
    assert p.type == "str"
    assert p.default is None
    assert p.description is None


def test_input_param_full():
    p = InputParam(type="int", default=42, description="The answer")
    assert p.default == 42
    assert p.description == "The answer"


# ---------------------------------------------------------------------------
# CredentialRef
# ---------------------------------------------------------------------------


def test_credential_ref():
    ref = CredentialRef(env="BRIX_CRED_TOKEN")
    assert ref.env == "BRIX_CRED_TOKEN"


def test_credential_ref_missing_env():
    with pytest.raises(ValidationError):
        CredentialRef()


# ---------------------------------------------------------------------------
# RetryConfig
# ---------------------------------------------------------------------------


def test_retry_config_defaults():
    r = RetryConfig()
    assert r.max == 3
    assert r.backoff == "exponential"


def test_retry_config_custom():
    r = RetryConfig(max=5, backoff="linear")
    assert r.max == 5
    assert r.backoff == "linear"


def test_retry_config_invalid_backoff():
    with pytest.raises(ValidationError):
        RetryConfig(backoff="random")


# ---------------------------------------------------------------------------
# ErrorConfig
# ---------------------------------------------------------------------------


def test_error_config_defaults():
    e = ErrorConfig()
    assert e.on_error == "stop"
    assert e.retry is None


def test_error_config_with_retry():
    e = ErrorConfig(on_error="retry", retry=RetryConfig(max=5))
    assert e.on_error == "retry"
    assert e.retry.max == 5


def test_error_config_invalid_on_error():
    with pytest.raises(ValidationError):
        ErrorConfig(on_error="explode")


# ---------------------------------------------------------------------------
# Step — basic construction
# ---------------------------------------------------------------------------


def test_step_python():
    s = Step(id="run", type="python", script="helpers/run.py")
    assert s.type == "python"
    assert s.script == "helpers/run.py"
    assert s.parallel is False
    assert s.concurrency == 10


def test_step_http_defaults():
    s = Step(id="call", type="http", url="https://example.com")
    assert s.method == "GET"
    assert s.headers is None
    assert s.body is None


def test_step_http_full():
    s = Step(
        id="post",
        type="http",
        url="https://api.example.com/v1/data",
        method="POST",
        headers={"Content-Type": "application/json"},
        body={"key": "value"},
    )
    assert s.method == "POST"
    assert s.headers["Content-Type"] == "application/json"


def test_step_cli_args_list():
    s = Step(id="conv", type="cli", args=["markitdown", "{{ item.path }}"])
    assert s.shell is False
    assert s.args == ["markitdown", "{{ item.path }}"]


def test_step_cli_shell_command():
    s = Step(id="proc", type="cli", command="cat file.txt | grep pattern", shell=True)
    assert s.shell is True
    assert s.command == "cat file.txt | grep pattern"


def test_step_mcp():
    s = Step(id="fetch", type="mcp", server="m365", tool="list-mail-messages")
    assert s.server == "m365"
    assert s.tool == "list-mail-messages"


def test_step_pipeline():
    s = Step(id="sub", type="pipeline", pipeline="sub_pipeline.yaml")
    assert s.pipeline == "sub_pipeline.yaml"


def test_step_common_fields():
    s = Step(
        id="s1",
        type="python",
        script="run.py",
        foreach="{{ items }}",
        parallel=True,
        concurrency=5,
        when="{{ condition }}",
        on_error="continue",
        timeout="30s",
        params={"key": "val"},
    )
    assert s.foreach == "{{ items }}"
    assert s.parallel is True
    assert s.concurrency == 5
    assert s.when == "{{ condition }}"
    assert s.on_error == "continue"
    assert s.timeout == "30s"
    assert s.params == {"key": "val"}


# ---------------------------------------------------------------------------
# Step — validators
# ---------------------------------------------------------------------------


def test_step_concurrency_zero_raises():
    with pytest.raises(ValidationError):
        Step(id="s", type="python", script="run.py", concurrency=0)


def test_step_concurrency_negative_raises():
    with pytest.raises(ValidationError):
        Step(id="s", type="python", script="run.py", concurrency=-1)


def test_step_mcp_missing_server_raises():
    with pytest.raises(ValidationError):
        Step(id="s", type="mcp", tool="some-tool")


def test_step_mcp_missing_tool_raises():
    with pytest.raises(ValidationError):
        Step(id="s", type="mcp", server="m365")


def test_step_mcp_both_missing_raises():
    with pytest.raises(ValidationError):
        Step(id="s", type="mcp")


def test_step_cli_shell_true_no_command_raises():
    with pytest.raises(ValidationError):
        Step(id="s", type="cli", shell=True)


def test_step_cli_shell_false_no_args_no_command_raises():
    with pytest.raises(ValidationError):
        Step(id="s", type="cli")


def test_step_cli_shell_false_with_command_ok():
    """shell=False with command (to be split) should be valid."""
    s = Step(id="s", type="cli", command="markitdown file.pdf")
    assert s.command == "markitdown file.pdf"
    assert s.shell is False


def test_step_invalid_type_raises():
    with pytest.raises(ValidationError):
        Step(id="s", type="unknown_type")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def test_pipeline_minimal():
    step = Step(id="s1", type="python", script="run.py")
    p = Pipeline(name="my-pipeline", steps=[step])
    assert p.name == "my-pipeline"
    assert p.version == "0.1.0"
    assert len(p.steps) == 1
    assert p.input == {}
    assert p.credentials == {}
    assert isinstance(p.error_handling, ErrorConfig)
    assert p.output is None


def test_pipeline_full():
    steps = [
        Step(id="fetch", type="mcp", server="m365", tool="list-mail-messages"),
        Step(id="save", type="cli", args=["cp", "{{ fetch.output }}", "/tmp/"]),
    ]
    p = Pipeline(
        name="fetch-and-save",
        version="1.0.0",
        description="Fetches and saves",
        brix_version=">=0.1.0",
        input={"query": InputParam(type="str", description="Search query")},
        credentials={"token": CredentialRef(env="BRIX_CRED_TOKEN")},
        error_handling=ErrorConfig(on_error="continue"),
        steps=steps,
        output={"files": "{{ save.output }}"},
    )
    assert p.version == "1.0.0"
    assert "query" in p.input
    assert "token" in p.credentials
    assert p.error_handling.on_error == "continue"
    assert p.output == {"files": "{{ save.output }}"}
    assert len(p.steps) == 2


def test_pipeline_empty_steps_raises():
    with pytest.raises(ValidationError):
        Pipeline(name="empty", steps=[])


def test_pipeline_steps_required():
    with pytest.raises(ValidationError):
        Pipeline(name="no-steps")


# ---------------------------------------------------------------------------
# StepResult
# ---------------------------------------------------------------------------


def test_step_result_success():
    r = StepResult(success=True, data={"files": ["/tmp/a.pdf"]}, duration=1.23)
    assert r.success is True
    assert r.data == {"files": ["/tmp/a.pdf"]}
    assert r.duration == 1.23
    assert r.error is None
    assert r.items_count is None


def test_step_result_error():
    r = StepResult(success=False, error="Connection refused", duration=0.5)
    assert r.success is False
    assert r.error == "Connection refused"


def test_step_result_with_items_count():
    r = StepResult(success=True, data=[], duration=2.0, items_count=5)
    assert r.items_count == 5


def test_step_result_defaults():
    r = StepResult(success=True)
    assert r.data is None
    assert r.duration == 0.0


# ---------------------------------------------------------------------------
# ForeachResult
# ---------------------------------------------------------------------------


def test_foreach_result_mixed():
    result = ForeachResult(
        items=[
            ForeachItem(success=True, data={"path": "/tmp/file1.pdf"}),
            ForeachItem(success=True, data={"path": "/tmp/file2.pdf"}),
            ForeachItem(
                success=False,
                error="404 Not Found",
                input={"url": "https://example.com/missing"},
            ),
        ],
        summary=ForeachSummary(total=3, succeeded=2, failed=1),
    )
    assert len(result.items) == 3
    assert result.items[0].success is True
    assert result.items[0].data == {"path": "/tmp/file1.pdf"}
    assert result.items[2].success is False
    assert result.items[2].error == "404 Not Found"
    assert result.items[2].input == {"url": "https://example.com/missing"}
    assert result.summary.total == 3
    assert result.summary.succeeded == 2
    assert result.summary.failed == 1


def test_foreach_result_all_success():
    result = ForeachResult(
        items=[ForeachItem(success=True, data=i) for i in range(5)],
        summary=ForeachSummary(total=5, succeeded=5, failed=0),
    )
    assert result.summary.failed == 0


def test_foreach_result_empty():
    result = ForeachResult(
        items=[],
        summary=ForeachSummary(total=0, succeeded=0, failed=0),
    )
    assert result.items == []


# ---------------------------------------------------------------------------
# StepStatus
# ---------------------------------------------------------------------------


def test_step_status_ok():
    s = StepStatus(status="ok", duration=1.5)
    assert s.status == "ok"
    assert s.duration == 1.5
    assert s.items is None
    assert s.errors is None
    assert s.reason is None


def test_step_status_error():
    s = StepStatus(status="error", duration=0.2, errors=1)
    assert s.status == "error"
    assert s.errors == 1


def test_step_status_skipped():
    s = StepStatus(status="skipped", duration=0.0, reason="when condition was false")
    assert s.status == "skipped"
    assert s.reason == "when condition was false"


def test_step_status_with_items():
    s = StepStatus(status="ok", duration=3.0, items=42)
    assert s.items == 42


def test_step_status_invalid_status():
    with pytest.raises(ValidationError):
        StepStatus(status="pending", duration=0.0)


# ---------------------------------------------------------------------------
# RunResult
# ---------------------------------------------------------------------------


def test_run_result():
    rr = RunResult(
        success=True,
        run_id="run-abc123",
        steps={
            "fetch": StepStatus(status="ok", duration=1.0, items=3),
            "save": StepStatus(status="ok", duration=0.5),
        },
        result={"files": ["/tmp/a.pdf", "/tmp/b.pdf"]},
        duration=1.5,
    )
    assert rr.success is True
    assert rr.run_id == "run-abc123"
    assert "fetch" in rr.steps
    assert rr.steps["fetch"].items == 3
    assert rr.duration == 1.5


def test_run_result_failed():
    rr = RunResult(
        success=False,
        run_id="run-fail-001",
        steps={
            "fetch": StepStatus(status="ok", duration=0.8),
            "process": StepStatus(status="error", duration=0.1, errors=1),
        },
        result=None,
        duration=0.9,
    )
    assert rr.success is False
    assert rr.steps["process"].status == "error"


def test_run_result_empty_steps():
    rr = RunResult(
        success=True,
        run_id="run-trivial",
        steps={},
        result=None,
        duration=0.0,
    )
    assert rr.steps == {}


# ---------------------------------------------------------------------------
# ServerConfig
# ---------------------------------------------------------------------------


def test_server_config_minimal():
    sc = ServerConfig(name="m365", command="docker")
    assert sc.name == "m365"
    assert sc.command == "docker"
    assert sc.args == []
    assert sc.env == {}
    assert sc.tools_prefix is None


def test_server_config_full():
    sc = ServerConfig(
        name="m365",
        command="docker",
        args=["exec", "-i", "m365-mcp", "node", "/app/index.js"],
        env={"NODE_ENV": "production"},
        tools_prefix="m365",
    )
    assert sc.args == ["exec", "-i", "m365-mcp", "node", "/app/index.js"]
    assert sc.env["NODE_ENV"] == "production"
    assert sc.tools_prefix == "m365"


def test_server_config_missing_name():
    with pytest.raises(ValidationError):
        ServerConfig(command="docker")


def test_server_config_missing_command():
    with pytest.raises(ValidationError):
        ServerConfig(name="m365")


# ---------------------------------------------------------------------------
# Edge cases and cross-model scenarios
# ---------------------------------------------------------------------------


def test_pipeline_with_all_step_types():
    steps = [
        Step(id="py", type="python", script="run.py"),
        Step(id="http", type="http", url="https://example.com", method="POST"),
        Step(id="cli", type="cli", args=["echo", "hello"]),
        Step(id="mcp_step", type="mcp", server="m365", tool="list-mail-messages"),
        Step(id="sub", type="pipeline", pipeline="sub.yaml"),
    ]
    p = Pipeline(name="mixed", steps=steps)
    assert len(p.steps) == 5


def test_step_on_error_overrides_pipeline():
    """Each step can override the pipeline-level error handling."""
    step = Step(id="risky", type="python", script="risky.py", on_error="continue")
    p = Pipeline(
        name="p",
        steps=[step],
        error_handling=ErrorConfig(on_error="stop"),
    )
    assert p.error_handling.on_error == "stop"
    assert p.steps[0].on_error == "continue"


def test_foreach_item_success_without_data():
    item = ForeachItem(success=True)
    assert item.data is None
    assert item.error is None
    assert item.input is None


def test_foreach_item_failure_with_input():
    item = ForeachItem(success=False, error="timeout", input={"url": "https://x.com"})
    assert item.success is False
    assert item.error == "timeout"
    assert item.input["url"] == "https://x.com"


def test_pipeline_serialization_roundtrip():
    """Pipeline model can be serialized to dict and back."""
    step = Step(id="s1", type="python", script="run.py")
    p = Pipeline(name="roundtrip", steps=[step])
    data = p.model_dump()
    p2 = Pipeline.model_validate(data)
    assert p2.name == p.name
    assert p2.steps[0].id == "s1"


# ---------------------------------------------------------------------------
# T-BRIX-V4-17: groups field on Pipeline model
# ---------------------------------------------------------------------------


def test_pipeline_groups_default_empty():
    """Pipeline.groups defaults to an empty dict."""
    step = Step(id="s1", type="python", script="run.py")
    p = Pipeline(name="no-groups", steps=[step])
    assert p.groups == {}


def test_pipeline_groups_stored():
    """Pipeline.groups stores named step group dicts."""
    step = Step(id="s1", type="python", script="run.py")
    groups = {
        "auth": [{"id": "login", "type": "python", "script": "helpers/login.py"}]
    }
    p = Pipeline(name="with-groups", steps=[step], groups=groups)
    assert "auth" in p.groups
    assert p.groups["auth"][0]["id"] == "login"


def test_pipeline_groups_roundtrip():
    """Pipeline with groups round-trips through model_dump/model_validate."""
    step = Step(id="s1", type="python", script="run.py")
    groups = {"setup": [{"id": "init", "type": "python", "script": "run.py"}]}
    p = Pipeline(name="roundtrip-groups", steps=[step], groups=groups)
    data = p.model_dump()
    p2 = Pipeline.model_validate(data)
    assert p2.groups == groups
