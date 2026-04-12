from __future__ import annotations

import asyncio
from pathlib import Path

from brix.models import Step
from brix.runners.document_extract import ExtractDocumentWithDaigestrRunner
from brix.external_service_capabilities import fetch_daigestr_capabilities


def test_fetch_daigestr_capabilities_detects_incomplete_async_contract():
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

        async def get(self, url):
            if url.endswith("/v1/health"):
                return FakeResponse({"status": "ok", "version": "13.6.2"})
            if url.endswith("/v1/tips"):
                return FakeResponse(
                    {
                        "response_contract": {
                            "job_progress_endpoints": {
                                "start": "POST /v1/convert/async returns {job_id, status}.",
                                "status": "GET /v1/jobs/{id} returns canonical progress under progress.",
                            },
                            "job_progress_fields": {
                                "progress.status": "Job status snapshot",
                                "progress.job_id": "Job id",
                            },
                        }
                    }
                )
            raise AssertionError(f"unexpected url: {url}")

    capabilities = asyncio.run(fetch_daigestr_capabilities(base_url="http://daigestr:8081", client=FakeClient()))

    assert capabilities.supports_async_jobs is False
    assert "async_contract_incomplete" in capabilities.drift_issues
    assert "job_progress_fields_incomplete" in capabilities.drift_issues


def test_extract_document_with_daigestr_fails_on_explicit_async_drift(monkeypatch, tmp_path: Path):
    file_path = tmp_path / "async-drift.pdf"
    file_path.write_bytes(b"pdf-content")

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
            if url.endswith("/v1/health"):
                return FakeResponse({"status": "ok", "version": "13.6.2"})
            if url.endswith("/v1/tips"):
                return FakeResponse(
                    {
                        "response_contract": {
                            "job_progress_endpoints": {
                                "start": "POST /v1/convert/async returns {job_id, status}.",
                                "status": "GET /v1/jobs/{id} returns canonical progress under progress.",
                            },
                            "job_progress_fields": {
                                "progress.status": "Job status snapshot",
                                "progress.job_id": "Job id",
                            },
                        }
                    }
                )
            raise AssertionError(f"unexpected get url: {url}")

        async def post(self, url, json, headers):
            raise AssertionError(f"unexpected post url: {url}")

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={"file_bytes_path": str(file_path), "use_async_jobs": True},
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is False
    assert result["error"]["error_type"] == "external_job_capability_error"
    assert "async_contract_incomplete" in result["error"]["external_job"]["service_capabilities"]["drift_issues"]
