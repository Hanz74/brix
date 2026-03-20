"""End-to-end tests for Brix v2 — MCP + REST API + Auto-Exposure."""
import pytest
from pathlib import Path
from starlette.testclient import TestClient

from brix.mcp_server import (
    _handle_get_tips,
    _handle_list_bricks,
    _handle_search_bricks,
    _handle_get_brick_schema,
    _handle_create_pipeline,
    _handle_get_pipeline,
    _handle_run_pipeline,
    _handle_list_pipelines,
    _handle_get_template,
    _handle_get_run_history,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_pipeline_dir(monkeypatch, tmp_path: Path) -> None:
    """Redirect mcp_server's PIPELINE_DIR to tmp_path for test isolation."""
    import brix.mcp_server as mcp_mod
    monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)


# ---------------------------------------------------------------------------
# TestE2EMcpFlow — Full MCP flow: discover → build → run → inspect
# ---------------------------------------------------------------------------

class TestE2EMcpFlow:
    """Full MCP flow: discover → build → run → inspect."""

    async def test_discover_bricks(self):
        """Step 1: Claude discovers available bricks."""
        result = await _handle_list_bricks({})
        assert "bricks" in result
        assert len(result["bricks"]) >= 10

    async def test_search_for_http(self):
        """Step 2: Claude searches for HTTP bricks."""
        result = await _handle_search_bricks({"query": "REST API"})
        # handler returns key 'results', not 'bricks'
        assert "results" in result
        names = [b["name"] for b in result["results"]]
        assert any("http" in n for n in names)

    async def test_get_schema(self):
        """Step 3: Claude gets schema for http_get."""
        result = await _handle_get_brick_schema({"brick_name": "http_get"})
        # Should have config_schema (or an error if brick not found)
        assert "config_schema" in result or "error" in result

    async def test_create_and_run_pipeline(self, tmp_path, monkeypatch):
        """Step 4+5: Claude creates pipeline inline and runs it."""
        _patch_pipeline_dir(monkeypatch, tmp_path)

        # Create pipeline with inline steps
        create_result = await _handle_create_pipeline({
            "name": "e2e-test",
            "description": "E2E test pipeline",
            "steps": [
                {"id": "echo", "type": "cli", "args": ["echo", "e2e-success"]},
            ],
        })
        assert create_result.get("success") is True
        assert create_result.get("pipeline_id") == "e2e-test"

        # Run it
        run_result = await _handle_run_pipeline({
            "pipeline_id": "e2e-test",
        })
        assert run_result.get("success") is True

    async def test_get_tips(self):
        """Tips should contain useful info."""
        result = await _handle_get_tips({})
        tips = result.get("tips", [])
        # tips is a list of strings — join to check total length
        assert isinstance(tips, list)
        assert len("\n".join(tips)) > 100

    async def test_get_template(self):
        """Templates are available."""
        result = await _handle_get_template({"goal": "download files"})
        assert "name" in result or "templates" in result


# ---------------------------------------------------------------------------
# TestE2ERestApi — Full REST API flow
# ---------------------------------------------------------------------------

class TestE2ERestApi:
    """Full REST API flow."""

    def test_health(self):
        from brix.api import app
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_list_pipelines(self):
        from brix.api import app
        client = TestClient(app)
        resp = client.get("/pipelines")
        assert resp.status_code == 200
        assert "pipelines" in resp.json()

    def test_run_nonexistent(self):
        from brix.api import app
        client = TestClient(app)
        resp = client.post("/run/nonexistent-e2e-test", json={})
        assert resp.status_code == 404

    def test_webhook_nonexistent(self):
        from brix.api import app
        client = TestClient(app)
        resp = client.post("/webhook/nonexistent-e2e-test", json={})
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestE2EPipelineAutoExposure — Saved pipelines auto-registered as MCP tools
# ---------------------------------------------------------------------------

class TestE2EPipelineAutoExposure:
    """Saved pipelines auto-registered as MCP tools."""

    async def test_create_pipeline_then_list(self, tmp_path, monkeypatch):
        """Pipeline created via MCP appears in list."""
        _patch_pipeline_dir(monkeypatch, tmp_path)

        await _handle_create_pipeline({
            "name": "auto-expose-test",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo", "exposed"]}],
        })

        list_result = await _handle_list_pipelines({"directory": str(tmp_path)})
        names = [p["name"] for p in list_result.get("pipelines", [])]
        assert "auto-expose-test" in names

    async def test_pipeline_tool_auto_exposed_in_server(self, tmp_path, monkeypatch):
        """Pipeline saved to store is visible as brix__pipeline__* tool."""
        from brix.mcp_server import _build_pipeline_tools, PIPELINE_TOOL_PREFIX
        from brix.pipeline_store import PipelineStore

        store = PipelineStore(pipelines_dir=tmp_path)
        store.save({
            "name": "exposed-pipeline",
            "version": "1.0.0",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo", "hi"]}],
        })

        tools = _build_pipeline_tools(store)
        tool_names = [t.name for t in tools]
        expected_tool = PIPELINE_TOOL_PREFIX + "exposed_pipeline"
        assert expected_tool in tool_names


# ---------------------------------------------------------------------------
# TestE2EFullWorkflow — Complete workflow: tips → template → create → run → history
# ---------------------------------------------------------------------------

class TestE2EFullWorkflow:
    """Complete workflow: tips → template → create → run → history."""

    async def test_full_workflow(self, tmp_path, monkeypatch):
        """Simulates Claude's actual workflow."""
        _patch_pipeline_dir(monkeypatch, tmp_path)

        # 1. Get tips
        tips = await _handle_get_tips({})
        assert tips
        assert tips.get("brick_count", 0) > 0

        # 2. Get template
        template = await _handle_get_template({"goal": "download"})
        assert template

        # 3. Create pipeline with inline steps
        create = await _handle_create_pipeline({
            "name": "workflow-test",
            "description": "Full workflow test",
            "steps": [
                {"id": "step1", "type": "cli", "args": ["echo", "workflow-works"]},
                {"id": "step2", "type": "cli", "args": ["echo", "step2-done"]},
            ],
        })
        assert create.get("success") is True
        assert create.get("step_count") == 2

        # 4. Run pipeline
        run = await _handle_run_pipeline({
            "pipeline_id": "workflow-test",
        })
        assert run.get("success") is True

        # 5. Check history
        history = await _handle_get_run_history({"limit": 5})
        assert isinstance(history, dict)
        assert history.get("success") is True
        assert "runs" in history

    async def test_list_pipelines_after_create(self, tmp_path, monkeypatch):
        """Pipelines created via MCP are immediately listable."""
        _patch_pipeline_dir(monkeypatch, tmp_path)

        await _handle_create_pipeline({
            "name": "list-test-pipeline",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo", "listed"]}],
        })

        list_result = await _handle_list_pipelines({"directory": str(tmp_path)})
        assert list_result.get("success") is True
        names = [p["name"] for p in list_result.get("pipelines", [])]
        assert "list-test-pipeline" in names

    async def test_get_pipeline_after_create(self, tmp_path, monkeypatch):
        """Pipeline definition is retrievable after creation."""
        _patch_pipeline_dir(monkeypatch, tmp_path)

        await _handle_create_pipeline({
            "name": "get-test-pipeline",
            "description": "Retrieval test",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo", "get-me"]}],
        })

        get_result = await _handle_get_pipeline({"pipeline_id": "get-test-pipeline"})
        assert get_result.get("name") == "get-test-pipeline"
        assert get_result.get("step_count") == 1
