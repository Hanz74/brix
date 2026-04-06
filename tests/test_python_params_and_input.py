from types import SimpleNamespace

import pytest

from brix.models import RunResult
from brix.mcp_handlers import runs as run_handlers
from brix.runners.python import PythonRunner


def _minimal_pipeline_data(name: str) -> dict:
    return {
        "name": name,
        "version": "1.0.0",
        "steps": [
            {
                "id": "noop",
                "type": "set",
                "values": {"ok": True},
            }
        ],
    }


@pytest.mark.asyncio
async def test_python_runner_passes_only_user_params_to_subprocess(tmp_path):
    script = tmp_path / "echo_params.py"
    script.write_text(
        "import json, sys\n"
        "print(json.dumps(json.loads(sys.argv[1])))\n"
    )

    runner = PythonRunner()
    step = {
        "id": "helper_step",
        "type": "python",
        "config": {
            "helper": "example_helper",
            "script": str(script),
            "params": {"foo": "alpha", "bar": 2},
        },
    }

    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"] == {"foo": "alpha", "bar": 2}
    assert "helper" not in result["data"]
    assert "script" not in result["data"]


def _pipeline_run_result() -> RunResult:
    return RunResult(
        success=True,
        run_id="run-test",
        steps={},
        result={"ok": True},
        duration=0.01,
    )


@pytest.mark.asyncio
async def test_pipeline_without_input_definition_accepts_all_params_without_warning(monkeypatch):
    async def fake_run(self, pipeline, user_input, **kwargs):
        return _pipeline_run_result()

    monkeypatch.setattr(
        run_handlers,
        "_load_pipeline_yaml",
        _minimal_pipeline_data,
    )
    monkeypatch.setattr(run_handlers.PipelineEngine, "run", fake_run)
    monkeypatch.setattr(
        run_handlers,
        "_audit_db",
        SimpleNamespace(write_audit_entry=lambda **kwargs: None),
    )

    result = await run_handlers._handle_run_pipeline(
        {"pipeline_id": "no-input-schema", "input": {"foo": "x", "bar": 3}}
    )

    assert result["success"] is True
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_pipeline_with_input_definition_warns_only_for_unknown_params(monkeypatch):
    async def fake_run(self, pipeline, user_input, **kwargs):
        return _pipeline_run_result()

    monkeypatch.setattr(
        run_handlers,
        "_load_pipeline_yaml",
        lambda name: {
            **_minimal_pipeline_data(name),
            "input": {
                "known": {
                    "type": "string",
                    "default": "",
                }
            },
        },
    )
    monkeypatch.setattr(run_handlers.PipelineEngine, "run", fake_run)
    monkeypatch.setattr(
        run_handlers,
        "_audit_db",
        SimpleNamespace(write_audit_entry=lambda **kwargs: None),
    )

    result = await run_handlers._handle_run_pipeline(
        {"pipeline_id": "with-input-schema", "input": {"known": "ok", "extra": "ignored"}}
    )

    assert result["success"] is True
    assert result["warnings"] == ["Unknown input parameters (ignored): extra"]
