from __future__ import annotations

import pytest

from brix.pipeline_store import PipelineStore


@pytest.mark.asyncio
async def test_test_pipeline_loads_db_only_pipeline(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr("brix.context.CACHE_BASE", tmp_path / "cache")

    store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
    store.save(
        {
            "name": "db-only-test-pipeline",
            "steps": [
                {
                    "id": "fetch",
                    "type": "mcp",
                    "server": "fake",
                    "tool": "fake-tool",
                }
            ],
        }
    )

    assert not (tmp_path / "db-only-test-pipeline.yaml").exists()

    from brix.mcp_server import _handle_test_pipeline

    result = await _handle_test_pipeline(
        {
            "name": "db-only-test-pipeline",
            "mocks": {"fetch": {"result": "ok"}},
            "assertions": {"fetch": [{"status": "ok"}]},
        }
    )

    assert result["success"] is True, result
    assert result["pipeline"] == "db-only-test-pipeline"
    assert result["summary"]["steps_passed"] == 1
    assert result["summary"]["steps_total"] == 1
    assert result["summary"]["assertions_passed"] == 1
    assert result["steps"]["fetch"]["status"] == "ok"
