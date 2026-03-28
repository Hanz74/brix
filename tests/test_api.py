"""Tests for the Brix REST API (T-BRIX-V2-10)."""
import json
import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from starlette.testclient import TestClient

from brix.api import app, _sse_event, _TERMINAL_STATUSES
from brix.context import PipelineContext
from brix.models import RunResult, StepStatus


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    """TestClient without authentication — simulates localhost (no API key required)."""
    from brix import api as api_module
    original = api_module.API_KEY
    api_module.API_KEY = ""
    # TestClient uses 'testclient' as the remote host, which is not 127.0.0.1.
    # Patch _is_localhost to return True so the no-key localhost bypass works as expected.
    with patch("brix.api._is_localhost", return_value=True):
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
# Webhook endpoint (T-BRIX-V2-11 / T-BRIX-V5-06)
# ---------------------------------------------------------------------------

def _make_pipeline_mock(input_params: dict | None = None):
    """Build a mock Pipeline object with optional input schema."""
    mock = MagicMock()
    if input_params is None:
        mock.input = {}
    else:
        # input_params: {name: (type, default)} — default=None means required
        from brix.models import InputParam
        mock.input = {
            name: InputParam(type=typ, default=default)
            for name, (typ, default) in input_params.items()
        }
    return mock


class TestWebhook:
    def test_webhook_not_found(self, client):
        with patch("brix.api.PipelineStore") as MockStore:
            MockStore.return_value.load.side_effect = FileNotFoundError("not found")
            resp = client.post("/webhook/nonexistent", json={})
        assert resp.status_code == 404

    def test_webhook_async_response(self, client):
        """Webhook always returns 202 + run_id (async mode)."""
        mock_pipeline = _make_pipeline_mock()
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = mock_pipeline
            resp = client.post("/webhook/my-pipeline", json={"event": "push"})
        assert resp.status_code == 202
        data = resp.json()
        assert "run_id" in data
        assert data["status"] == "started"

    def test_webhook_with_secret_valid(self, client):
        """Correct X-Webhook-Secret header is accepted."""
        mock_pipeline = _make_pipeline_mock()
        env = {"BRIX_WEBHOOK_SECRET_MY_PIPELINE": "mysecret"}
        with patch.dict(os.environ, env), \
             patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = mock_pipeline
            resp = client.post(
                "/webhook/my-pipeline",
                json={},
                headers={"X-Webhook-Secret": "mysecret"},
            )
        assert resp.status_code == 202

    def test_webhook_with_secret_invalid(self, client):
        """Wrong X-Webhook-Secret is rejected with 403."""
        env = {"BRIX_WEBHOOK_SECRET_MY_PIPELINE": "mysecret"}
        with patch.dict(os.environ, env):
            resp = client.post(
                "/webhook/my-pipeline",
                json={},
                headers={"X-Webhook-Secret": "wrong"},
            )
        assert resp.status_code == 403

    def test_webhook_requires_global_auth(self, authed_client):
        """Without per-pipeline secret, global API key is required."""
        resp = authed_client.post("/webhook/my-pipeline", json={})
        assert resp.status_code == 401

    # ------------------------------------------------------------------
    # T-BRIX-V5-06 — Auth fallback: BRIX_API_KEY accepted alongside secret
    # ------------------------------------------------------------------

    def test_webhook_api_key_fallback_when_secret_set(self, client):
        """X-API-Key is accepted as fallback when pipeline secret does not match."""
        import brix.api as api_module
        mock_pipeline = _make_pipeline_mock()
        env = {"BRIX_WEBHOOK_SECRET_MY_PIPELINE": "mysecret"}
        original = api_module.API_KEY
        api_module.API_KEY = "global-key"
        try:
            with patch.dict(os.environ, env), \
                 patch("brix.api.PipelineStore") as MockStore, \
                 patch("brix.api.asyncio.create_task"):
                MockStore.return_value.load.return_value = mock_pipeline
                resp = client.post(
                    "/webhook/my-pipeline",
                    json={},
                    headers={"X-API-Key": "global-key"},
                )
        finally:
            api_module.API_KEY = original
        assert resp.status_code == 202

    def test_webhook_api_key_fallback_wrong_key_rejected(self):
        """Wrong X-API-Key does NOT bypass webhook secret."""
        import brix.api as api_module
        env = {"BRIX_WEBHOOK_SECRET_MY_PIPELINE": "mysecret"}
        original = api_module.API_KEY
        api_module.API_KEY = "global-key"
        try:
            with patch.dict(os.environ, env), \
                 patch("brix.api._is_localhost", return_value=True):
                client = TestClient(app)
                resp = client.post(
                    "/webhook/my-pipeline",
                    json={},
                    headers={"X-API-Key": "wrong-key"},
                )
        finally:
            api_module.API_KEY = original
        assert resp.status_code == 403

    # ------------------------------------------------------------------
    # T-BRIX-V5-06 — Payload validation
    # ------------------------------------------------------------------

    def test_webhook_payload_validation_missing_required(self, client):
        """Missing required parameter → 400 Bad Request."""
        mock_pipeline = _make_pipeline_mock({"folder": ("string", None)})
        with patch("brix.api.PipelineStore") as MockStore:
            MockStore.return_value.load.return_value = mock_pipeline
            resp = client.post("/webhook/my-pipeline", json={})
        assert resp.status_code == 400
        data = resp.json()
        assert data["error"] == "Payload validation failed"
        assert any("folder" in d for d in data["details"])

    def test_webhook_payload_validation_with_default_ok(self, client):
        """Parameters with defaults are optional — body without them passes."""
        mock_pipeline = _make_pipeline_mock({"folder": ("string", "inbox")})
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = mock_pipeline
            resp = client.post("/webhook/my-pipeline", json={})
        assert resp.status_code == 202

    def test_webhook_payload_validation_required_provided(self, client):
        """Required parameter is provided in body — passes validation."""
        mock_pipeline = _make_pipeline_mock({"folder": ("string", None)})
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = mock_pipeline
            resp = client.post("/webhook/my-pipeline", json={"folder": "inbox"})
        assert resp.status_code == 202

    def test_webhook_no_input_schema_no_validation(self, client):
        """Pipeline with no input schema — any body passes."""
        mock_pipeline = _make_pipeline_mock()  # empty input
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = mock_pipeline
            resp = client.post("/webhook/my-pipeline", json={"anything": "goes"})
        assert resp.status_code == 202

    # ------------------------------------------------------------------
    # T-BRIX-V5-06 — Idempotency
    # ------------------------------------------------------------------

    def test_webhook_idempotency_first_call(self, client, tmp_path):
        """First call with X-Idempotency-Key returns 202 and stores run_id."""
        mock_pipeline = _make_pipeline_mock()
        db_path = tmp_path / "idem.db"
        with patch("brix.api._IDEMPOTENCY_DB", db_path), \
             patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = mock_pipeline
            resp = client.post(
                "/webhook/my-pipeline",
                json={},
                headers={"X-Idempotency-Key": "unique-key-001"},
            )
        assert resp.status_code == 202
        data = resp.json()
        assert "run_id" in data
        assert data["status"] == "started"

    def test_webhook_idempotency_duplicate_returns_original_run_id(self, client, tmp_path):
        """Second call with same X-Idempotency-Key returns the first run_id."""
        mock_pipeline = _make_pipeline_mock()
        db_path = tmp_path / "idem.db"
        with patch("brix.api._IDEMPOTENCY_DB", db_path), \
             patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = mock_pipeline
            resp1 = client.post(
                "/webhook/my-pipeline",
                json={},
                headers={"X-Idempotency-Key": "unique-key-002"},
            )
            resp2 = client.post(
                "/webhook/my-pipeline",
                json={},
                headers={"X-Idempotency-Key": "unique-key-002"},
            )
        assert resp1.status_code == 202
        assert resp2.status_code == 200
        assert resp2.json()["status"] == "duplicate"
        assert resp2.json()["run_id"] == resp1.json()["run_id"]

    def test_webhook_no_idempotency_key_always_new_run(self, client):
        """Without X-Idempotency-Key, each call starts a fresh run."""
        mock_pipeline = _make_pipeline_mock()
        with patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = mock_pipeline
            resp1 = client.post("/webhook/my-pipeline", json={})
            resp2 = client.post("/webhook/my-pipeline", json={})
        assert resp1.status_code == 202
        assert resp2.status_code == 202
        assert resp1.json()["run_id"] != resp2.json()["run_id"]


# ---------------------------------------------------------------------------
# Approve endpoint (T-BRIX-V4-12)
# ---------------------------------------------------------------------------


class TestApproveEndpoint:
    def test_approve_endpoint_success(self, client, tmp_path):
        """POST /approve/{run_id} with an existing approval_pending.json returns success."""
        import json
        from unittest.mock import patch

        run_id = "test-approve-run"
        approval_file = tmp_path / run_id / "approval_pending.json"
        approval_file.parent.mkdir(parents=True)
        approval_file.write_text(json.dumps({
            "step_id": "wait",
            "message": "Approve me",
            "requested_at": 0,
            "status": "pending",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path):
            resp = client.post(
                f"/approve/{run_id}",
                json={"action": "approved", "by": "tester", "reason": "looks good"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["run_id"] == run_id
        assert data["status"] == "approved"

        # Verify file was updated
        updated = json.loads(approval_file.read_text())
        assert updated["status"] == "approved"
        assert updated["approved_by"] == "tester"

    def test_approve_endpoint_reject(self, client, tmp_path):
        """POST /approve/{run_id} with action=rejected records rejection."""
        import json
        from unittest.mock import patch

        run_id = "test-reject-run"
        approval_file = tmp_path / run_id / "approval_pending.json"
        approval_file.parent.mkdir(parents=True)
        approval_file.write_text(json.dumps({
            "step_id": "wait",
            "message": "Approve me",
            "requested_at": 0,
            "status": "pending",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path):
            resp = client.post(
                f"/approve/{run_id}",
                json={"action": "rejected", "by": "reviewer", "reason": "not ready"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "rejected"

    def test_approve_no_pending(self, client, tmp_path):
        """POST /approve/{run_id} when no approval file exists returns 404."""
        from unittest.mock import patch

        with patch("brix.api.WORKDIR_BASE", tmp_path):
            resp = client.post("/approve/nonexistent-run", json={})

        assert resp.status_code == 404
        assert "pending" in resp.json()["error"].lower()

    def test_approve_requires_auth(self, authed_client):
        """POST /approve/{run_id} requires API key when auth is configured."""
        resp = authed_client.post("/approve/some-run", json={})
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Security tests (T-BRIX-V5-SEC-01)
# ---------------------------------------------------------------------------


class TestSecurityFix01DefaultDeny:
    """No API key configured → only localhost is allowed, remote gets 401."""

    def _make_remote_client(self):
        """Return a TestClient that appears to come from a remote IP."""
        from brix import api as api_module
        original = api_module.API_KEY
        api_module.API_KEY = ""
        # TestClient uses a loopback address by default; we override via scope
        client = TestClient(app, base_url="http://testserver", raise_server_exceptions=True)
        client._original_api_key = original
        return client

    def test_no_api_key_localhost_allowed(self):
        """Requests from 127.0.0.1 are accepted when no API key is configured."""
        from brix import api as api_module
        original = api_module.API_KEY
        api_module.API_KEY = ""
        try:
            # Default TestClient connects from 127.0.0.1 (testclient scope)
            client = TestClient(app)
            with patch("brix.api.PipelineStore") as MockStore:
                MockStore.return_value.list_all.return_value = []
                resp = client.get("/pipelines")
            # TestClient uses scope client host = "testclient" by default,
            # which is not localhost — so we patch _is_localhost directly
            # to confirm the localhost branch permits access.
            with patch("brix.api._is_localhost", return_value=True), \
                 patch("brix.api.PipelineStore") as MockStore:
                MockStore.return_value.list_all.return_value = []
                resp = client.get("/pipelines")
            assert resp.status_code == 200
        finally:
            api_module.API_KEY = original

    def test_no_api_key_remote_denied(self):
        """Requests from a remote IP are rejected (401) when no API key is configured."""
        from brix import api as api_module
        original = api_module.API_KEY
        api_module.API_KEY = ""
        try:
            client = TestClient(app)
            with patch("brix.api._is_localhost", return_value=False):
                resp = client.get("/pipelines")
            assert resp.status_code == 401
        finally:
            api_module.API_KEY = original

    def test_no_api_key_webhook_remote_denied(self):
        """Webhook from remote IP is rejected when no API key is configured."""
        from brix import api as api_module
        original = api_module.API_KEY
        api_module.API_KEY = ""
        try:
            client = TestClient(app)
            with patch("brix.api._is_localhost", return_value=False):
                resp = client.post("/webhook/my-pipeline", json={})
            assert resp.status_code == 401
        finally:
            api_module.API_KEY = original


class TestSecurityFix02TimingAttack:
    """Webhook secret comparison uses hmac.compare_digest (not ==)."""

    def test_webhook_secret_uses_hmac_compare_digest(self):
        """Verify that hmac.compare_digest is called for webhook secret validation."""
        import brix.api as api_module
        env = {"BRIX_WEBHOOK_SECRET_MY_PIPELINE": "supersecret"}

        # Patch _is_localhost so the no-key guard passes for this test
        with patch.dict(os.environ, env), \
             patch("brix.api._is_localhost", return_value=True), \
             patch("brix.api.hmac.compare_digest", wraps=api_module.hmac.compare_digest) as mock_cd, \
             patch("brix.api.PipelineStore") as MockStore, \
             patch("brix.api.asyncio.create_task"):
            MockStore.return_value.load.return_value = _make_pipeline_mock()
            # No key set — use localhost bypass so only webhook secret validation fires
            original = api_module.API_KEY
            api_module.API_KEY = ""
            try:
                client = TestClient(app)
                resp = client.post(
                    "/webhook/my-pipeline",
                    json={},
                    headers={"X-Webhook-Secret": "supersecret"},
                )
            finally:
                api_module.API_KEY = original

        # compare_digest must have been called at least once for the secret
        assert mock_cd.called, "hmac.compare_digest was not called for webhook secret comparison"

    def test_api_key_uses_hmac_compare_digest(self):
        """Verify that hmac.compare_digest is called for API key validation."""
        import brix.api as api_module
        mock_result = _make_run_result(success=True)

        with patch("brix.api.hmac.compare_digest", wraps=api_module.hmac.compare_digest) as mock_cd, \
             patch("brix.api.PipelineStore") as MockStore:
            MockStore.return_value.list_all.return_value = []
            original = api_module.API_KEY
            api_module.API_KEY = "my-key"
            try:
                client = TestClient(app)
                resp = client.get("/pipelines", headers={"X-API-Key": "my-key"})
            finally:
                api_module.API_KEY = original

        assert mock_cd.called, "hmac.compare_digest was not called for API key comparison"
        assert resp.status_code == 200

    def test_wrong_api_key_rejected(self):
        """Wrong API key is rejected even if lengths match (timing-safe)."""
        from brix import api as api_module
        original = api_module.API_KEY
        api_module.API_KEY = "correct-key-value"
        try:
            client = TestClient(app)
            resp = client.get("/pipelines", headers={"X-API-Key": "wrong-key-value"})
        finally:
            api_module.API_KEY = original
        assert resp.status_code == 401


class TestSecurityFix03CredentialsNotPersisted:
    """Credentials must not appear in run.json written by save_run_metadata."""

    def test_save_run_metadata_excludes_credentials(self, tmp_path):
        """run.json does not contain credentials after save_run_metadata."""
        import json
        ctx = PipelineContext(
            pipeline_input={"param": "value"},
            credentials={"api_key": "super-secret-token", "password": "hunter2"},
            workdir=tmp_path,
        )
        ctx.run_id = "test-run-sec"
        ctx.save_run_metadata("my-pipeline", status="running")

        run_json = json.loads((tmp_path / "run.json").read_text())

        assert "credentials" not in run_json, "credentials key must not be persisted to run.json"
        assert "super-secret-token" not in json.dumps(run_json), "credential value leaked into run.json"
        assert "hunter2" not in json.dumps(run_json), "credential value leaked into run.json"

    def test_save_run_metadata_preserves_input(self, tmp_path):
        """run.json still contains pipeline input after fix."""
        import json
        ctx = PipelineContext(
            pipeline_input={"folder": "inbox", "limit": 10},
            credentials={"token": "secret"},
            workdir=tmp_path,
        )
        ctx.run_id = "test-run-input"
        ctx.save_run_metadata("test-pipeline", status="completed")

        run_json = json.loads((tmp_path / "run.json").read_text())

        assert run_json["input"] == {"folder": "inbox", "limit": 10}
        assert run_json["pipeline"] == "test-pipeline"
        assert run_json["status"] == "completed"


# ---------------------------------------------------------------------------
# SSE streaming endpoint (T-BRIX-V6-23)
# ---------------------------------------------------------------------------


def _parse_sse(body: str) -> list[dict]:
    """Parse SSE body into list of {event, data} dicts."""
    events = []
    for block in body.split("\n\n"):
        block = block.strip()
        if not block:
            continue
        ev = {}
        for line in block.splitlines():
            if line.startswith("event: "):
                ev["event"] = line[len("event: "):]
            elif line.startswith("data: "):
                ev["data"] = json.loads(line[len("data: "):])
        if ev:
            events.append(ev)
    return events


class TestSseEvent:
    """Unit tests for the _sse_event helper."""

    def test_sse_event_format(self):
        result = _sse_event("progress", {"run_id": "r1", "pct": 50})
        assert result.startswith("event: progress\n")
        assert "data: " in result
        assert result.endswith("\n\n")
        payload = json.loads(result.split("data: ", 1)[1].strip())
        assert payload["run_id"] == "r1"
        assert payload["pct"] == 50

    def test_sse_event_done(self):
        result = _sse_event("done", {"run_id": "r2", "status": "success"})
        assert "event: done\n" in result

    def test_terminal_statuses_set(self):
        assert "success" in _TERMINAL_STATUSES
        assert "failed" in _TERMINAL_STATUSES
        assert "running" not in _TERMINAL_STATUSES


class TestStreamRunEndpoint:
    """Integration tests for GET /stream/{run_id} (T-BRIX-V6-23)."""

    def test_stream_requires_auth(self, authed_client):
        """Without API key, SSE endpoint returns 401 error event."""
        resp = authed_client.get("/stream/some-run-id")
        assert resp.status_code == 401
        events = _parse_sse(resp.text)
        assert any(e["event"] == "error" for e in events)
        assert any("Unauthorized" in e["data"].get("error", "") for e in events)

    def test_stream_run_not_found(self, client, tmp_path):
        """Non-existent run_id emits error event."""
        with patch("brix.api.WORKDIR_BASE", tmp_path):
            resp = client.get("/stream/no-such-run")
        assert resp.status_code == 200
        events = _parse_sse(resp.text)
        assert any(e["event"] == "error" for e in events)

    def test_stream_emits_connected_event(self, client, tmp_path):
        """A run workdir that finishes immediately emits 'connected' then 'done'."""
        run_id = "run-sse-test-01"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        # Write a terminal run.json immediately
        (run_dir / "run.json").write_text(json.dumps({
            "run_id": run_id,
            "pipeline": "test",
            "status": "success",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path), \
             patch("brix.api._SSE_POLL_INTERVAL", 0.01):
            resp = client.get(f"/stream/{run_id}")

        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers["content-type"]
        events = _parse_sse(resp.text)

        event_types = [e["event"] for e in events]
        assert "connected" in event_types
        assert "done" in event_types

    def test_stream_connected_event_has_run_id(self, client, tmp_path):
        """The 'connected' event carries the run_id."""
        run_id = "run-sse-test-02"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "run.json").write_text(json.dumps({
            "run_id": run_id,
            "status": "success",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path), \
             patch("brix.api._SSE_POLL_INTERVAL", 0.01):
            resp = client.get(f"/stream/{run_id}")

        events = _parse_sse(resp.text)
        connected = next(e for e in events if e["event"] == "connected")
        assert connected["data"]["run_id"] == run_id

    def test_stream_emits_status_event(self, client, tmp_path):
        """A 'status' event is emitted with current run state."""
        run_id = "run-sse-test-03"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "run.json").write_text(json.dumps({
            "run_id": run_id,
            "pipeline": "my-pipe",
            "status": "success",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path), \
             patch("brix.api._SSE_POLL_INTERVAL", 0.01):
            resp = client.get(f"/stream/{run_id}")

        events = _parse_sse(resp.text)
        status_events = [e for e in events if e["event"] == "status"]
        assert len(status_events) >= 1
        assert status_events[0]["data"]["run_id"] == run_id
        assert status_events[0]["data"]["status"] == "success"

    def test_stream_emits_progress_event_when_step_progress_exists(self, client, tmp_path):
        """A 'progress' event is emitted when step_progress.json is present."""
        run_id = "run-sse-test-04"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        step_progress = {"fetch": {"processed": 5, "total": 10, "percent": 50.0}}
        (run_dir / "step_progress.json").write_text(json.dumps(step_progress))
        (run_dir / "run.json").write_text(json.dumps({
            "run_id": run_id,
            "status": "success",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path), \
             patch("brix.api._SSE_POLL_INTERVAL", 0.01):
            resp = client.get(f"/stream/{run_id}")

        events = _parse_sse(resp.text)
        progress_events = [e for e in events if e["event"] == "progress"]
        assert len(progress_events) >= 1
        assert "step_progress" in progress_events[0]["data"]
        assert progress_events[0]["data"]["step_progress"]["fetch"]["percent"] == 50.0

    def test_stream_done_event_on_failed_run(self, client, tmp_path):
        """A run with status 'failed' still gets a 'done' event."""
        run_id = "run-sse-test-05"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "run.json").write_text(json.dumps({
            "run_id": run_id,
            "status": "failed",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path), \
             patch("brix.api._SSE_POLL_INTERVAL", 0.01):
            resp = client.get(f"/stream/{run_id}")

        events = _parse_sse(resp.text)
        done = next((e for e in events if e["event"] == "done"), None)
        assert done is not None
        assert done["data"]["status"] == "failed"

    def test_stream_no_duplicate_progress_events(self, client, tmp_path):
        """Progress event is NOT repeated when step_progress.json is unchanged."""
        run_id = "run-sse-test-06"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        step_progress = {"fetch": {"processed": 3, "total": 10, "percent": 30.0}}
        (run_dir / "step_progress.json").write_text(json.dumps(step_progress))
        (run_dir / "run.json").write_text(json.dumps({
            "run_id": run_id,
            "status": "success",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path), \
             patch("brix.api._SSE_POLL_INTERVAL", 0.01):
            resp = client.get(f"/stream/{run_id}")

        events = _parse_sse(resp.text)
        progress_events = [e for e in events if e["event"] == "progress"]
        # Step progress doesn't change between ticks — should be emitted only once
        assert len(progress_events) == 1

    def test_stream_auth_with_api_key(self, authed_client, tmp_path):
        """Authenticated client can access the stream endpoint."""
        run_id = "run-sse-auth-test"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "run.json").write_text(json.dumps({
            "run_id": run_id,
            "status": "success",
        }))

        with patch("brix.api.WORKDIR_BASE", tmp_path), \
             patch("brix.api._SSE_POLL_INTERVAL", 0.01):
            resp = authed_client.get(
                f"/stream/{run_id}",
                headers={"X-API-Key": "test-secret"},
            )

        assert resp.status_code == 200
        events = _parse_sse(resp.text)
        assert any(e["event"] == "connected" for e in events)
