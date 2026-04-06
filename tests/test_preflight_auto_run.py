from uuid import uuid4

import pytest

from brix.mcp_server import _handle_create_pipeline, _handle_run_pipeline


@pytest.mark.asyncio
async def test_run_pipeline_blocked_when_quick_preflight_fails(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")

    pipeline_name = f"preflight-invalid-{uuid4().hex[:8]}"

    await _handle_create_pipeline(
        {
            "name": pipeline_name,
            "steps": [
                {"id": "call_helper", "type": "python", "helper": "missing_helper_xyz", "params": {}},
            ],
        }
    )

    result = await _handle_run_pipeline({"pipeline_id": pipeline_name})

    assert result["success"] is False
    assert result["error"]["code"] == "PREFLIGHT_FAILED"
    assert result["errors"]
    assert any("missing_helper_xyz" in msg for msg in result["errors"])
    assert "run_id" not in result


@pytest.mark.asyncio
async def test_run_pipeline_runs_normally_when_quick_preflight_passes(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")

    pipeline_name = f"preflight-valid-{uuid4().hex[:8]}"

    await _handle_create_pipeline(
        {
            "name": pipeline_name,
            "steps": [
                {"id": "greet", "type": "cli", "args": ["echo", "hello brix"]},
            ],
        }
    )

    result = await _handle_run_pipeline({"pipeline_id": pipeline_name})

    assert result["success"] is True
    assert "run_id" in result
    assert result["steps"]["greet"]["status"] == "ok"
