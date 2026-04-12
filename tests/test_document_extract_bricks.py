from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from brix.bricks.builtins import ALL_BUILTINS
from brix.db import BrixDB
from brix.engine import PipelineEngine
from brix.history import RunHistory
from brix.loader import PipelineLoader
from brix.migrations import MIGRATIONS, _register_document_extract_bricks_v90, run_pending_migrations
from brix.models import Step
from brix.runners.document_extract import (
    DocumentPrepareExtractablePayloadRunner,
    ExtractDocumentWithDaigestrRunner,
)


@pytest.fixture(autouse=True)
def _disable_async_daigestr_jobs(monkeypatch):
    monkeypatch.setenv("BRIX_DAIGESTR_USE_ASYNC_JOBS", "false")


def test_prepare_extractable_payload_reads_file_and_base64_encodes(tmp_path):
    file_path = tmp_path / "invoice.pdf"
    file_path.write_bytes(b"pdf-content")
    runner = DocumentPrepareExtractablePayloadRunner()
    step = SimpleNamespace(
        config={
            "file_bytes_path": str(file_path),
            "mime_type": "application/pdf",
            "language": "de",
            "include_base64": True,
            "metadata": {"source": "hmk"},
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["file_bytes_path"] == str(file_path)
    assert data["base64"]
    assert data["mime_type"] == "application/pdf"
    assert data["metadata"]["source"] == "hmk"


def test_extract_document_with_daigestr_normalizes_response(monkeypatch, tmp_path):
    file_path = tmp_path / "invoice.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "receipt",
                    "quality_score": 0.91,
                    "template_used": "receipt",
                },
                "normalized": {"vendor_name": "REWE"},
                "markdown": "# test",
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            assert url.endswith("/v1/convert")
            assert json["auto_extract"] is True
            assert json["mode"] == "default"
            assert json["retry_on_low_quality"] is True
            assert json["quality_retry_threshold"] == 0.75
            assert json["quality_retry_mode"] == "full"
            assert json["base64"]
            assert json["content"]
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = SimpleNamespace(
        config={
            "file_bytes_path": str(file_path),
            "language": "de",
            "metadata": {"source": "hmk"},
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["normalized"]["vendor_name"] == "REWE"
    assert data["document_type"] == "receipt"
    assert data["quality_score"] == 0.91
    assert data["_meta"]["template"] == "receipt"
    assert data["attempt_history"] == [
        {
            "attempt": 1,
            "attempt_count": 1,
            "mode": "default",
            "quality_score": 0.91,
            "request_id": None,
            "status": "completed",
        }
    ]


def test_extract_document_with_daigestr_fails_when_runtime_payload_reports_unsuccessful_result(monkeypatch, tmp_path):
    file_path = tmp_path / "timeout.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def json(self):
            return {
                "success": False,
                "error": {
                    "code": "TIMEOUT",
                    "message": "Timeout nach 300 Sekunden bei convert_auto",
                },
                "meta": {
                    "request_id": "req-timeout",
                    "initial_mode": "default",
                    "final_mode": "default",
                    "retry_applied": False,
                    "retry_threshold_used": 0.75,
                },
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = SimpleNamespace(config={"file_bytes_path": str(file_path)}, timeout=None)

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is False
    assert result["error"]["error_type"] == "external_job_runtime_error"
    assert result["error"]["error"] == "Timeout nach 300 Sekunden bei convert_auto"
    assert result["error"]["external_job"]["request_id"] == "req-timeout"


def test_prepare_extractable_payload_prefers_step_params(tmp_path):
    file_path = tmp_path / "params.pdf"
    file_path.write_bytes(b"pdf-content")
    runner = DocumentPrepareExtractablePayloadRunner()
    step = Step(
        id="prepare",
        type="document.prepare_extractable_payload",
        params={
            "file_bytes_path": str(file_path),
            "mime_type": "application/pdf",
            "include_base64": True,
            "metadata": {"source": "params"},
        },
    )

    result = asyncio.run(runner.execute(step, context=SimpleNamespace(last_output={"file_bytes_path": "ignored"})))

    assert result["success"] is True
    data = result["data"]
    assert data["file_bytes_path"] == str(file_path)
    assert data["mime_type"] == "application/pdf"
    assert data["metadata"]["source"] == "params"
    assert data["base64"]


def test_extract_document_with_daigestr_prefers_step_params(monkeypatch, tmp_path):
    file_path = tmp_path / "params.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "invoice",
                    "quality_score": 0.77,
                },
                "normalized": {"vendor_name": "Params Vendor"},
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            assert url.endswith("/v1/convert")
            assert json["auto_extract"] is True
            assert json["filename"] == "params.pdf"
            assert json["mode"] == "default"
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={
            "file_bytes_path": str(file_path),
            "filename": "params.pdf",
            "language": "de",
            "metadata": {"source": "params"},
        },
    )

    result = asyncio.run(runner.execute(step, context=SimpleNamespace(last_output={"file_bytes_path": "ignored"})))

    assert result["success"] is True
    data = result["data"]
    assert data["normalized"]["vendor_name"] == "Params Vendor"
    assert data["document_type"] == "invoice"
    assert data["quality_score"] == 0.77


def test_extract_document_with_daigestr_prefers_explicit_base_url_and_endpoint(monkeypatch, tmp_path):
    file_path = tmp_path / "override.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"normalized": {"vendor_name": "Override"}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["url"] = url
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_URL", "http://daigestr:9999")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={
            "file_bytes_path": str(file_path),
            "endpoint": "/v1/extract",
        },
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert captured["url"] == "http://daigestr:9999/v1/extract"
    assert captured["json"]["auto_extract"] is True


def test_extract_document_with_daigestr_uses_env_default_endpoint(monkeypatch, tmp_path):
    file_path = tmp_path / "env-default.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"normalized": {"vendor_name": "Env Default"}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["url"] = url
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_URL", "http://daigestr:9999")
    monkeypatch.setenv("BRIX_DAIGESTR_CONVERT_ENDPOINT", "/custom/convert")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={"file_bytes_path": str(file_path)},
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert captured["url"] == "http://daigestr:9999/custom/convert"


def test_extract_document_with_daigestr_uses_retry_env_defaults(monkeypatch, tmp_path):
    file_path = tmp_path / "retry-defaults.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"normalized": {"vendor_name": "Retry Defaults"}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_MODE", "default")
    monkeypatch.setenv("BRIX_DAIGESTR_RETRY_ON_LOW_QUALITY", "true")
    monkeypatch.setenv("BRIX_DAIGESTR_QUALITY_RETRY_THRESHOLD", "0.75")
    monkeypatch.setenv("BRIX_DAIGESTR_QUALITY_RETRY_MODE", "full")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={"file_bytes_path": str(file_path)},
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert captured["json"]["mode"] == "default"
    assert captured["json"]["retry_on_low_quality"] is True
    assert captured["json"]["quality_retry_threshold"] == 0.75
    assert captured["json"]["quality_retry_mode"] == "full"


def test_extract_document_with_daigestr_prefers_explicit_retry_settings(monkeypatch, tmp_path):
    file_path = tmp_path / "retry-explicit.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"normalized": {"vendor_name": "Retry Explicit"}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={
            "file_bytes_path": str(file_path),
            "mode": "default",
            "retry_on_low_quality": True,
            "quality_retry_threshold": 0.8,
            "quality_retry_mode": "full",
        },
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert captured["json"]["mode"] == "default"
    assert captured["json"]["retry_on_low_quality"] is True
    assert captured["json"]["quality_retry_threshold"] == 0.8
    assert captured["json"]["quality_retry_mode"] == "full"


def test_extract_document_with_daigestr_uses_async_job_contract_when_enabled(monkeypatch, tmp_path):
    file_path = tmp_path / "async-job.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, list[tuple[str, str]]] = {"calls": []}
    updates: list[dict] = []
    poll_state = {"count": 0}

    class FakeResponse:
        def __init__(self, payload: dict, *, status_code: int = 200):
            self._payload = payload
            self.status_code = status_code
            self.is_error = status_code >= 400

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["calls"].append(("POST", url))
            if url.endswith("/v1/convert/async"):
                return FakeResponse({"job_id": "job-123", "status": "queued"})
            raise AssertionError(f"unexpected post url: {url}")

        async def get(self, url):
            captured["calls"].append(("GET", url))
            if url.endswith("/v1/health"):
                return FakeResponse({"status": "ok", "version": "13.6.2"})
            if url.endswith("/v1/tips"):
                return FakeResponse(
                    {
                        "response_contract": {
                            "job_progress_endpoints": {
                                "start": "POST /v1/convert/async returns {job_id, status}.",
                                "status": "GET /v1/jobs/{id} returns canonical progress under progress.",
                                "result": "GET /v1/jobs/{id}/result returns the final ConvertResponse after completion.",
                            },
                            "job_progress_fields": {
                                "progress.status": "status",
                                "progress.job_id": "job id",
                            },
                        }
                    }
                )
            if url.endswith("/v1/jobs/job-123"):
                poll_state["count"] += 1
                return FakeResponse(
                    {
                        "status": "completed" if poll_state["count"] > 1 else "processing",
                        "progress": {
                            "job_id": "job-123",
                            "request_id": "req-async",
                            "current_stage": "extract",
                            "message": "processing",
                            "percent": 55,
                            "attempt_number": 1,
                            "attempt_count": 2,
                            "attempt_mode": "default",
                        },
                    }
                )
            if url.endswith("/v1/jobs/job-123/result"):
                return FakeResponse(
                    {
                        "meta": {
                            "job_id": "job-123",
                            "request_id": "req-async",
                            "document_type": "invoice",
                            "template_used": "invoice",
                            "quality_score": 0.88,
                            "attempt_number": 2,
                            "attempt_count": 2,
                            "attempt_mode": "full",
                            "retry_applied": True,
                            "retry_reason": "low_quality",
                            "initial_mode": "default",
                            "final_mode": "full",
                            "initial_quality_score": 0.61,
                            "final_quality_score": 0.88,
                        },
                        "normalized": {"invoice_number": "A-1"},
                    }
                )
            raise AssertionError(f"unexpected get url: {url}")

    class FakeContext:
        def update_step_progress(self, step_id, payload):
            updates.append({"step_id": step_id, **payload})

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_USE_ASYNC_JOBS", "true")
    monkeypatch.setenv("BRIX_DAIGESTR_JOB_POLL_INTERVAL_SECONDS", "0")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={"file_bytes_path": str(file_path)},
    )

    result = asyncio.run(runner.execute(step, context=FakeContext()))

    assert result["success"] is True
    assert result["data"]["document_type"] == "invoice"
    assert result["data"]["_meta"]["template"] == "invoice"
    assert result["data"]["_meta"]["request_id"] == "req-async"
    assert result["data"]["_meta"]["final_mode"] == "full"
    assert result["data"]["_meta"]["attempt_mode"] == "full"
    assert any(call == ("POST", "http://daigestr:8081/v1/convert/async") for call in captured["calls"])
    assert any(call == ("GET", "http://daigestr:8081/v1/jobs/job-123") for call in captured["calls"])
    assert any(call == ("GET", "http://daigestr:8081/v1/jobs/job-123/result") for call in captured["calls"])
    assert any(update.get("job_id") == "job-123" for update in updates)


def test_extract_document_with_daigestr_falls_back_to_sync_when_async_job_id_missing(monkeypatch, tmp_path):
    file_path = tmp_path / "async-fallback.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: list[tuple[str, str]] = []

    class FakeResponse:
        def __init__(self, payload: dict, *, status_code: int = 200):
            self._payload = payload
            self.status_code = status_code
            self.is_error = status_code >= 400

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured.append(("POST", url))
            if url.endswith("/v1/convert"):
                return FakeResponse({"meta": {"document_type": "receipt", "quality_score": 0.8}, "normalized": {"vendor_name": "Fallback"}})
            raise AssertionError(f"unexpected post url: {url}")

        async def get(self, url):
            captured.append(("GET", url))
            if url.endswith("/v1/health"):
                return FakeResponse({"status": "ok", "version": "13.6.2"})
            if url.endswith("/v1/tips"):
                return FakeResponse({"response_contract": {}})
            raise AssertionError(f"unexpected get url: {url}")

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_USE_ASYNC_JOBS", "true")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={"file_bytes_path": str(file_path)},
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert result["data"]["normalized"]["vendor_name"] == "Fallback"
    assert captured == [
        ("GET", "http://daigestr:8081/v1/health"),
        ("GET", "http://daigestr:8081/v1/tips"),
        ("POST", "http://daigestr:8081/v1/convert"),
    ]


def test_extract_document_with_daigestr_fails_fast_when_async_is_explicitly_required(monkeypatch, tmp_path):
    file_path = tmp_path / "async-required.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: list[tuple[str, str]] = []

    class FakeResponse:
        def __init__(self, payload: dict, *, status_code: int = 200):
            self._payload = payload
            self.status_code = status_code
            self.is_error = status_code >= 400

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            captured.append(("GET", url))
            if url.endswith("/v1/health"):
                return FakeResponse({"status": "ok", "version": "13.5.0"})
            if url.endswith("/v1/tips"):
                return FakeResponse({"response_contract": {}})
            raise AssertionError(f"unexpected get url: {url}")

        async def post(self, url, json, headers):
            raise AssertionError(f"unexpected post url: {url}")

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_USE_ASYNC_JOBS", "false")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={"file_bytes_path": str(file_path), "use_async_jobs": True},
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is False
    assert result["error"]["error_type"] == "external_job_capability_error"
    assert result["error"]["external_job"]["service_capabilities"]["supports_async_jobs"] is False
    assert captured == [
        ("GET", "http://daigestr:8081/v1/health"),
        ("GET", "http://daigestr:8081/v1/tips"),
    ]


def test_document_extract_bricks_are_registered_in_builtins():
    names = {brick.name for brick in ALL_BUILTINS}
    assert "document.prepare_extractable_payload" in names
    assert "extract.document_with_daigestr" in names
    prepare = next(brick for brick in ALL_BUILTINS if brick.name == "document.prepare_extractable_payload")
    assert prepare.input_type == "remote_download_result"


def test_migration_registers_document_extract_bricks(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")

    _register_document_extract_bricks_v90(db)

    assert db.brick_definitions_get("document.prepare_extractable_payload") is not None
    assert db.brick_definitions_get("extract.document_with_daigestr") is not None


def test_extract_document_with_daigestr_uses_meta_as_canonical_contract(monkeypatch, tmp_path):
    file_path = tmp_path / "meta-contract.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "bank_statement",
                    "document_type_confidence": 0.99,
                    "template_used": "bank_statement",
                    "quality_score": 0.52,
                    "quality_grade": "medium",
                    "retry_applied": True,
                    "retry_reason": "low_quality",
                    "initial_mode": "default",
                    "final_mode": "full",
                    "initial_quality_score": 0.52,
                    "final_quality_score": 0.8045,
                    "retry_threshold_used": 0.75,
                    "request_id": "req-123",
                    "attempt_number": 2,
                    "attempt_count": 2,
                    "attempt_mode": "full",
                    "pipeline_steps": ["ocr", "dual_pass_validation"],
                },
                "extracted": {"iban": "DE62..."},
                "normalized": {"iban_normalized": "DE62..."},
                "markdown": "# statement",
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["document_type"] == "bank_statement"
    assert data["quality_score"] == 0.52
    assert data["_quality_score"] == 0.52
    assert data["_meta"]["template"] == "bank_statement"
    assert data["_meta"]["final_quality_score"] == 0.8045
    assert data["_meta"]["pipeline_steps"] == ["ocr", "dual_pass_validation"]
    assert data["attempt_history"] == [
        {
            "attempt": 1,
            "attempt_count": 2,
            "mode": "default",
            "quality_score": 0.52,
            "request_id": "req-123",
            "retry_reason": "low_quality",
            "retry_threshold_used": 0.75,
            "status": "retry_triggered",
        },
        {
            "attempt": 2,
            "attempt_count": 2,
            "mode": "full",
            "quality_score": 0.8045,
            "request_id": "req-123",
            "retry_reason": "low_quality",
            "retry_threshold_used": 0.75,
            "status": "completed",
        },
    ]


def test_extract_document_with_daigestr_updates_progress_with_attempt_metadata(monkeypatch, tmp_path):
    file_path = tmp_path / "attempt-progress.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "invoice",
                    "template_used": "invoice",
                    "quality_score": 0.81,
                    "request_id": "req-456",
                    "attempt_number": 2,
                    "attempt_count": 2,
                    "attempt_mode": "full",
                    "retry_applied": True,
                    "retry_reason": "low_quality",
                },
                "normalized": {"invoice_number": "R-1"},
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    updates: list[dict] = []

    class FakeContext:
        def update_step_progress(self, step_id, payload):
            updates.append({"step_id": step_id, **payload})

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=FakeContext()))

    assert result["success"] is True
    assert updates[0]["stage"] == "request"
    assert updates[0]["attempt_mode"] == "default"
    assert updates[1]["stage"] == "result"
    assert updates[1]["attempt_number"] == 2
    assert updates[1]["attempt_count"] == 2
    assert updates[1]["attempt_mode"] == "full"
    assert updates[1]["retry_applied"] is True
    assert updates[1]["retry_reason"] == "low_quality"
    assert updates[1]["request_id"] == "req-456"


def test_extract_document_with_daigestr_persists_replay_artifacts(monkeypatch, tmp_path):
    file_path = tmp_path / "artifact.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "receipt",
                    "template_used": "receipt",
                    "quality_score": 0.81,
                    "request_id": "req-artifact",
                },
                "normalized": {"vendor_name": "Artifact Vendor"},
                "markdown": "# artifact",
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    class FakeContext:
        def __init__(self, workdir):
            self.workdir = workdir

        def update_step_progress(self, step_id, payload):
            return None

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=FakeContext(tmp_path)))

    assert result["success"] is True
    artifacts = result["data"]["artifacts"]
    assert artifacts["request_path"] == "external_job_artifacts/extract/request.json"
    assert artifacts["response_path"] == "external_job_artifacts/extract/response.json"
    assert artifacts["attempt_history_path"] == "external_job_artifacts/extract/attempt_history.json"
    assert artifacts["markdown_path"] == "external_job_artifacts/extract/markdown.md"

    request_json = json.loads((tmp_path / artifacts["request_path"]).read_text())
    response_json = json.loads((tmp_path / artifacts["response_path"]).read_text())
    attempt_history = json.loads((tmp_path / artifacts["attempt_history_path"]).read_text())
    markdown = (tmp_path / artifacts["markdown_path"]).read_text()

    assert "base64" not in request_json
    assert "content" not in request_json
    assert request_json["base64_bytes"] > 0
    assert request_json["content_bytes"] > 0
    assert response_json["markdown_path"] == artifacts["markdown_path"]
    assert response_json["markdown_chars"] == len("# artifact")
    assert attempt_history[0]["request_id"] == "req-artifact"
    assert markdown == "# artifact"


def test_extract_document_with_daigestr_surfaces_structured_http_failure(monkeypatch, tmp_path):
    file_path = tmp_path / "http-failure.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        is_error = True
        status_code = 502
        text = '{"meta":{"request_id":"req-fail"}}'

        def json(self):
            return {
                "meta": {
                    "request_id": "req-fail",
                    "retry_applied": True,
                    "retry_reason": "low_quality",
                    "initial_mode": "default",
                    "final_mode": "full",
                    "initial_quality_score": 0.41,
                    "final_quality_score": 0.44,
                    "attempt_number": 2,
                    "attempt_count": 2,
                    "attempt_mode": "full",
                    "retry_threshold_used": 0.75,
                },
                "warnings": ["upstream retry exhausted"],
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    class FakeContext:
        def __init__(self, workdir):
            self.workdir = workdir
            self.progress_updates: list[dict] = []

        def update_step_progress(self, step_id, payload):
            self.progress_updates.append({"step_id": step_id, **payload})

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    context = FakeContext(tmp_path)
    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=context))

    assert result["success"] is False
    error = result["error"]
    assert error["error"] == "Daigestr request failed with HTTP 502"
    assert error["error_type"] == "external_job_http_error"
    assert error["external_job"]["request_id"] == "req-fail"
    assert error["external_job"]["retry_reason"] == "low_quality"
    assert error["external_job"]["attempt_history"] == [
        {
            "attempt": 1,
            "attempt_count": 2,
            "mode": "default",
            "quality_score": 0.41,
            "request_id": "req-fail",
            "retry_reason": "low_quality",
            "retry_threshold_used": 0.75,
            "status": "retry_triggered",
        },
        {
            "attempt": 2,
            "attempt_count": 2,
            "mode": "full",
            "quality_score": 0.44,
            "request_id": "req-fail",
            "retry_reason": "low_quality",
            "retry_threshold_used": 0.75,
            "status": "completed",
        },
    ]
    assert error["external_job"]["artifacts"]["response_path"] == "external_job_artifacts/extract/response.json"
    assert context.progress_updates[-1]["stage"] == "error"
    assert context.progress_updates[-1]["retry_state"] == "failed"


def test_extract_document_with_daigestr_replays_persisted_fixture(monkeypatch, tmp_path):
    file_path = tmp_path / "replay-fixture.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("live HTTP call must not happen during fixture replay")

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={
            "file_bytes_path": str(file_path),
            "replay_fixture_path": str(
                (Path.cwd() / "tests/fixtures/daigestr/12513-hmk-202402-b-04-11-24-10-19-microsoft-lens-pdf.json")
            ),
        },
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert result["data"]["document_type"] == "receipt"
    assert result["data"]["_meta"]["template"] == "receipt"
    assert result["data"]["replay"]["source_type"] == "fixture"
    assert result["data"]["raw"]["meta"]["replay"]["source_type"] == "fixture"


def test_extract_document_with_daigestr_replays_response_artifact_relative_to_workdir(monkeypatch, tmp_path):
    file_path = tmp_path / "replay-artifact.pdf"
    file_path.write_bytes(b"pdf-content")
    artifact_dir = tmp_path / "external_job_artifacts" / "extract"
    artifact_dir.mkdir(parents=True)
    response_path = artifact_dir / "response.json"
    response_path.write_text(
        json.dumps(
            {
                "meta": {
                    "document_type": "invoice",
                    "template_used": "invoice",
                    "quality_score": 0.81,
                    "request_id": "req-replay-artifact",
                },
                "normalized": {"invoice_number": "R-2"},
            }
        )
    )

    class FakeClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("live HTTP call must not happen during artifact replay")

    class FakeContext:
        def __init__(self, workdir):
            self.workdir = workdir

        def update_step_progress(self, step_id, payload):
            return None

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={
            "file_bytes_path": str(file_path),
            "replay_response_path": "external_job_artifacts/extract/response.json",
        },
    )

    result = asyncio.run(runner.execute(step, context=FakeContext(tmp_path)))

    assert result["success"] is True
    assert result["data"]["document_type"] == "invoice"
    assert result["data"]["normalized"]["invoice_number"] == "R-2"
    assert result["data"]["replay"]["source_type"] == "artifact"
    assert result["data"]["replay"]["source_path"] == str(response_path)


def test_extract_document_with_daigestr_resume_uses_completed_response_artifact(monkeypatch, tmp_path):
    file_path = tmp_path / "resume-complete.pdf"
    file_path.write_bytes(b"pdf-content")
    artifact_dir = tmp_path / "external_job_artifacts" / "extract"
    artifact_dir.mkdir(parents=True)
    response_path = artifact_dir / "response.json"
    response_path.write_text(
        json.dumps(
            {
                "meta": {
                    "document_type": "receipt",
                    "template_used": "receipt",
                    "quality_score": 0.88,
                    "request_id": "req-resume",
                },
                "normalized": {"vendor_name": "Resume Vendor"},
            }
        )
    )

    class FakeClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("resume with response artifact must not hit live HTTP")

    class FakeContext:
        def __init__(self, workdir):
            self.workdir = workdir
            self._resume_from = "run-resume"

        def update_step_progress(self, step_id, payload):
            return None

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=FakeContext(tmp_path)))

    assert result["success"] is True
    assert result["data"]["normalized"]["vendor_name"] == "Resume Vendor"
    assert result["data"]["replay"]["source_type"] == "resume_artifact"
    assert result["data"]["replay"]["source_path"] == str(response_path)


def test_extract_document_with_daigestr_resume_restarts_when_only_partial_artifacts_exist(monkeypatch, tmp_path):
    file_path = tmp_path / "resume-partial.pdf"
    file_path.write_bytes(b"pdf-content")
    artifact_dir = tmp_path / "external_job_artifacts" / "extract"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "request.json").write_text(json.dumps({"mode": "default"}))

    class FakeResponse:
        is_error = False

        def json(self):
            return {
                "meta": {
                    "document_type": "invoice",
                    "template_used": "invoice",
                    "quality_score": 0.77,
                    "request_id": "req-restart",
                },
                "normalized": {"invoice_number": "R-3"},
            }

    calls = {"count": 0}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            calls["count"] += 1
            return FakeResponse()

    class FakeContext:
        def __init__(self, workdir):
            self.workdir = workdir
            self._resume_from = "run-resume"

        def update_step_progress(self, step_id, payload):
            return None

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=FakeContext(tmp_path)))

    assert result["success"] is True
    assert calls["count"] == 1
    assert result["data"]["normalized"]["invoice_number"] == "R-3"
    assert result["data"]["replay"] is None


@pytest.mark.asyncio
async def test_engine_persists_structured_daigestr_failure_history(monkeypatch, tmp_path):
    file_path = tmp_path / "engine-http-failure.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        is_error = True
        status_code = 502
        text = '{"meta":{"request_id":"req-engine"}}'

        def json(self):
            return {
                "meta": {
                    "request_id": "req-engine",
                    "retry_applied": True,
                    "retry_reason": "low_quality",
                    "initial_mode": "default",
                    "final_mode": "full",
                    "initial_quality_score": 0.51,
                    "final_quality_score": 0.55,
                    "attempt_number": 2,
                    "attempt_count": 2,
                    "attempt_mode": "full",
                    "retry_threshold_used": 0.75,
                }
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    import brix.context as context_mod

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    pipeline = PipelineLoader().load_from_string(
        f"""
name: structured-daigestr-failure
steps:
  - id: extract
    type: extract.document_with_daigestr
    params:
      file_bytes_path: "{file_path}"
"""
    )

    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["extract"].status == "error"
    assert result.steps["extract"].error_message == "Daigestr request failed with HTTP 502"
    assert result.steps["extract"].error_detail["error_type"] == "external_job_http_error"
    assert result.steps["extract"].error_detail["external_job"]["attempt_history"][0]["status"] == "retry_triggered"

    history = RunHistory()
    errors = history.get_run_errors(run_id=result.run_id)

    assert len(errors) == 1
    assert errors[0]["error_detail"]["external_job"]["request_id"] == "req-engine"
    assert errors[0]["error_detail"]["external_job"]["attempt_history"][1]["mode"] == "full"


@pytest.mark.asyncio
async def test_engine_marks_daigestr_runtime_error_payload_as_failed_step(monkeypatch, tmp_path):
    file_path = tmp_path / "engine-runtime-timeout.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        is_error = False

        def json(self):
            return {
                "success": False,
                "error": {
                    "code": "TIMEOUT",
                    "message": "Timeout nach 300 Sekunden bei convert_auto",
                },
                "meta": {
                    "request_id": "req-engine-timeout",
                    "initial_mode": "default",
                    "final_mode": "default",
                    "retry_applied": False,
                    "retry_threshold_used": 0.75,
                },
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    import brix.context as context_mod

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    pipeline = PipelineLoader().load_from_string(
        f"""
name: runtime-daigestr-timeout
steps:
  - id: extract
    type: extract.document_with_daigestr
    params:
      file_bytes_path: "{file_path}"
"""
    )

    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps["extract"].status == "error"
    assert result.steps["extract"].error_message == "Timeout nach 300 Sekunden bei convert_auto"
    assert result.steps["extract"].error_detail["error_type"] == "external_job_runtime_error"
    assert result.steps["extract"].error_detail["external_job"]["request_id"] == "req-engine-timeout"


@pytest.mark.asyncio
async def test_engine_resume_reuses_completed_external_response_artifact(monkeypatch, tmp_path):
    file_path = tmp_path / "resume-engine-complete.pdf"
    file_path.write_bytes(b"pdf-content")
    run_id = "run-resume-external-complete"

    import brix.context as context_mod

    run_dir = tmp_path / "runs" / run_id
    artifact_dir = run_dir / "external_job_artifacts" / "extract"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "pipeline": "resume-complete",
                "input": {"seed": "value"},
                "status": "failed",
                "completed_steps": [],
            }
        )
    )
    response_path = artifact_dir / "response.json"
    response_path.write_text(
        json.dumps(
            {
                "meta": {
                    "document_type": "invoice",
                    "template_used": "invoice",
                    "quality_score": 0.83,
                    "request_id": "req-resume-engine",
                },
                "normalized": {"invoice_number": "R-RESUME"},
            }
        )
    )

    class FakeClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("resume with completed response.json must not call upstream")

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    pipeline = PipelineLoader().load_from_string(
        f"""
name: resume-complete
steps:
  - id: extract
    type: extract.document_with_daigestr
    params:
      file_bytes_path: "{file_path}"
"""
    )

    engine = PipelineEngine()
    result = await engine.run(pipeline, run_id=run_id)

    assert result.success is True
    assert result.steps["extract"].status == "ok"
    assert result.result["normalized"]["invoice_number"] == "R-RESUME"
    assert result.result["replay"]["source_type"] == "resume_artifact"
    assert result.result["replay"]["source_path"] == str(response_path)


@pytest.mark.asyncio
async def test_engine_resume_restarts_external_step_when_only_partial_artifacts_exist(monkeypatch, tmp_path):
    file_path = tmp_path / "resume-engine-partial.pdf"
    file_path.write_bytes(b"pdf-content")
    run_id = "run-resume-external-partial"

    import brix.context as context_mod

    run_dir = tmp_path / "runs" / run_id
    artifact_dir = run_dir / "external_job_artifacts" / "extract"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "pipeline": "resume-partial",
                "input": {},
                "status": "failed",
                "completed_steps": [],
            }
        )
    )
    (artifact_dir / "request.json").write_text(json.dumps({"mode": "default"}))

    class FakeResponse:
        is_error = False

        def json(self):
            return {
                "meta": {
                    "document_type": "receipt",
                    "template_used": "receipt",
                    "quality_score": 0.79,
                    "request_id": "req-resume-restart",
                },
                "normalized": {"vendor_name": "Restart Vendor"},
            }

    calls = {"count": 0}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            calls["count"] += 1
            return FakeResponse()

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", tmp_path / "runs")
    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    pipeline = PipelineLoader().load_from_string(
        f"""
name: resume-partial
steps:
  - id: extract
    type: extract.document_with_daigestr
    params:
      file_bytes_path: "{file_path}"
"""
    )

    engine = PipelineEngine()
    result = await engine.run(pipeline, run_id=run_id)

    assert result.success is True
    assert calls["count"] == 1
    assert result.result["normalized"]["vendor_name"] == "Restart Vendor"
    assert result.result["replay"] is None


def test_extract_document_with_daigestr_ignores_noncanonical_top_level_mirrors(monkeypatch, tmp_path):
    file_path = tmp_path / "mirror-conflict.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "bank_statement",
                    "template_used": "bank_statement",
                    "quality_score": 0.81,
                },
                "document_type": "invoice",
                "template": "invoice",
                "quality_score": 0.12,
                "_quality_score": 0.11,
                "normalized": {
                    "document_type": "receipt",
                    "_quality_score": 0.25,
                    "iban": "DE62...",
                },
                "extracted": {
                    "document_type": "letter",
                    "quality_score": 0.33,
                },
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["document_type"] == "bank_statement"
    assert data["quality_score"] == 0.81
    assert data["_quality_score"] == 0.81
    assert data["_meta"]["template"] == "bank_statement"


def test_extract_document_with_daigestr_canonicalizes_partial_business_payloads(monkeypatch, tmp_path):
    file_path = tmp_path / "partial-business-payload.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "receipt",
                    "template_used": "receipt",
                    "quality_score": 0.77,
                },
                "extracted": {"vendor_name": "REWE"},
                "normalized": {"vendor_name": "REWE", "summary": "Receipt summary"},
                "raw": {
                    "meta": {
                        "document_type": "receipt",
                        "template_used": "receipt",
                        "quality_score": 0.77,
                    }
                },
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["normalized"] == {"vendor_name": "REWE", "summary": "Receipt summary"}
    assert data["raw"]["extracted"] == {"vendor_name": "REWE"}
    assert data["raw"]["normalized"] == {"vendor_name": "REWE", "summary": "Receipt summary"}


def test_v90_runs_via_normal_migration_loop(tmp_path, monkeypatch):
    db = BrixDB(db_path=tmp_path / "migration_loop.db")
    fake_v90 = {
        "version": len(MIGRATIONS) + 1,
        "name": "test_register_document_extract_bricks",
        "up": "",
        "up_fn": "_register_document_extract_bricks_v90",
        "down": "",
    }
    monkeypatch.setattr("brix.migrations.MIGRATIONS", [*MIGRATIONS, fake_v90])

    applied = run_pending_migrations(db)

    assert applied[-1]["name"] == "test_register_document_extract_bricks"
    assert db.brick_definitions_get("document.prepare_extractable_payload") is not None
    assert db.brick_definitions_get("extract.document_with_daigestr") is not None
