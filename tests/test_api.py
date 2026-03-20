"""Tests for the Brix REST API (T-BRIX-V2-10)."""
import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from starlette.testclient import TestClient

from brix.api import app
from brix.models import RunResult, StepStatus


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    """TestClient without authentication."""
    with patch.dict(os.environ, {"BRIX_API_KEY": ""}):
        # Re-create app reference so API_KEY module var is unset for these tests
        from brix import api as api_module
        original = api_module.API_KEY
        api_module.API_KEY = ""
        yield TestClient(app)
        api_module.API_KEY = original


@pytest.fixture
def authed_client():
    """TestClient with API key authentication enabled."""
    from brix import api as api_module
    original = api_module.API_KEY
    api_module.API_KEY = "test-secret"
    yield TestClient(app)
    api_module.API_KEY = original


def _make_run_result(success: bool = True) -> RunResult:
    """Helper to build a RunResult for mocking."""
    return RunResult(
        success=success,
        run_id="test-run-123",
        steps={"step1": StepStatus(status="ok", duration=0.1)},
        result={"output": "hello"},
        duration=0.5,
    )


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data

    def test_health_no_auth_required(self, authed_client):
        """Health endpoint is always public, even when API key is configured."""
        resp = authed_client.get("/health")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# List pipelines
# ---------------------------------------------------------------------------

class TestListPipelines:
    def test_list_pipelines_returns_list(self, client):
        with patch("brix.api.PipelineStore") as MockStore:
            MockStore.return_value.list_all.return_value = []
            resp = client.get("/pipelines")
        assert resp.status_code == 200
        assert "pipelines" in resp.json()

    def test_list_pipelines_with_items(self, client):
        pipelines = [
            {"name": "my-pipeline", "version": "1.0", "description": "", "steps": 3, "path": "/x"}
        ]
        with patch("brix.api.PipelineStore") as MockStore:
            MockStore.return_value.list_all.return_value = pipelines
            resp = client.get("/pipelines")
        assert resp.status_code == 200
        assert resp.json()["pipelines"] == pipelines

    def test_list_pipelines_requires_auth(self, authed_client):
        resp = authed_client.get("/pipelines")
        assert resp.status_code == 401

    def test_list_pipelines_auth_success(self, authed_client):
        with patch("brix.api.PipelineStore") as MockStore:
            MockStore.return_value.list_all.return_value = []
            resp = authed_client.get("/pipelines", headers={"X-API-Key": "test-secret"})
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Run pipeline
# ---------------------------------------------------------------------------

class TestRunPipeline:
    def test_run_pipeline_not_found(self, client):
        with patch("brix.api.PipelineStore") as MockStore:
            MockStore.return_value.load.side_effect = FileNotFoundError("not found")
            resp = client.post("/run/nonexistent", json={})
        assert resp.status_code == 404
        assert "not found" in resp.json()["error"].lower()

    def test_run_pipeline_success(self, client):
        mock_result = _make_run_result(success=True)
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.PipelineEngine") as MockEngine:
            MockStore.return_value.load.return_value = MagicMock()
            MockEngine.return_value.run = AsyncMock(return_value=mock_result)
            resp = client.post("/run/my-pipeline", json={"param": "value"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["run_id"] == "test-run-123"

    def test_run_pipeline_failure_returns_500(self, client):
        mock_result = _make_run_result(success=False)
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.PipelineEngine") as MockEngine:
            MockStore.return_value.load.return_value = MagicMock()
            MockEngine.return_value.run = AsyncMock(return_value=mock_result)
            resp = client.post("/run/failing-pipeline", json={})
        assert resp.status_code == 500
        assert resp.json()["success"] is False

    def test_run_pipeline_empty_body(self, client):
        """POST with no JSON body should work (empty params)."""
        mock_result = _make_run_result(success=True)
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.PipelineEngine") as MockEngine:
            MockStore.return_value.load.return_value = MagicMock()
            MockEngine.return_value.run = AsyncMock(return_value=mock_result)
            resp = client.post("/run/my-pipeline")
        assert resp.status_code == 200

    def test_run_pipeline_requires_auth(self, authed_client):
        resp = authed_client.post("/run/my-pipeline", json={})
        assert resp.status_code == 401

    def test_run_pipeline_auth_success(self, authed_client):
        mock_result = _make_run_result(success=True)
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.PipelineEngine") as MockEngine:
            MockStore.return_value.load.return_value = MagicMock()
            MockEngine.return_value.run = AsyncMock(return_value=mock_result)
            resp = authed_client.post(
                "/run/my-pipeline",
                json={},
                headers={"X-API-Key": "test-secret"},
            )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Run status
# ---------------------------------------------------------------------------

class TestRunStatus:
    def test_get_run_status_not_found(self, client):
        with patch("brix.api.RunHistory") as MockHistory:
            MockHistory.return_value.get_run.return_value = None
            resp = client.get("/status/unknown-run-id")
        assert resp.status_code == 404

    def test_get_run_status_found(self, client):
        run_data = {
            "run_id": "abc123",
            "pipeline": "my-pipeline",
            "success": 1,
            "duration": 1.5,
        }
        with patch("brix.api.RunHistory") as MockHistory:
            MockHistory.return_value.get_run.return_value = run_data
            resp = client.get("/status/abc123")
        assert resp.status_code == 200
        assert resp.json()["run_id"] == "abc123"

    def test_get_run_status_requires_auth(self, authed_client):
        resp = authed_client.get("/status/abc123")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Webhook endpoint (T-BRIX-V2-11)
# ---------------------------------------------------------------------------

class TestWebhook:
    def test_webhook_not_found(self, client):
        with patch("brix.api.PipelineStore") as MockStore:
            MockStore.return_value.load.side_effect = FileNotFoundError("not found")
            resp = client.post("/webhook/nonexistent", json={})
        assert resp.status_code == 404

    def test_webhook_success(self, client):
        mock_result = _make_run_result(success=True)
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.PipelineEngine") as MockEngine:
            MockStore.return_value.load.return_value = MagicMock()
            MockEngine.return_value.run = AsyncMock(return_value=mock_result)
            resp = client.post("/webhook/my-pipeline", json={"event": "push"})
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_webhook_with_secret_valid(self, client):
        mock_result = _make_run_result(success=True)
        env = {"BRIX_WEBHOOK_SECRET_MY_PIPELINE": "mysecret"}
        with patch.dict(os.environ, env), \
             patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.PipelineEngine") as MockEngine:
            MockStore.return_value.load.return_value = MagicMock()
            MockEngine.return_value.run = AsyncMock(return_value=mock_result)
            resp = client.post(
                "/webhook/my-pipeline",
                json={},
                headers={"X-Webhook-Secret": "mysecret"},
            )
        assert resp.status_code == 200

    def test_webhook_with_secret_invalid(self, client):
        env = {"BRIX_WEBHOOK_SECRET_MY_PIPELINE": "mysecret"}
        with patch.dict(os.environ, env):
            resp = client.post(
                "/webhook/my-pipeline",
                json={},
                headers={"X-Webhook-Secret": "wrong"},
            )
        assert resp.status_code == 403

    def test_webhook_requires_global_auth(self, authed_client):
        resp = authed_client.post("/webhook/my-pipeline", json={})
        assert resp.status_code == 401
