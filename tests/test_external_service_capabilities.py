from __future__ import annotations

import asyncio

from brix.external_service_capabilities import fetch_daigestr_capabilities


def test_fetch_daigestr_capabilities_reads_version_and_async_support(monkeypatch):
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
                                "result": "GET /v1/jobs/{id}/result returns the final ConvertResponse after completion.",
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

    assert capabilities.service == "daigestr"
    assert capabilities.version == "13.6.2"
    assert capabilities.supports_async_jobs is True
    assert capabilities.supports_job_status is True
    assert capabilities.supports_job_result is True
    assert "progress.status" in capabilities.job_progress_fields


def test_fetch_daigestr_capabilities_reports_missing_async_contract(monkeypatch):
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
                return FakeResponse({"status": "ok", "version": "13.5.0"})
            if url.endswith("/v1/tips"):
                return FakeResponse({"response_contract": {}})
            raise AssertionError(f"unexpected url: {url}")

    capabilities = asyncio.run(fetch_daigestr_capabilities(base_url="http://daigestr:8081", client=FakeClient()))

    assert capabilities.version == "13.5.0"
    assert capabilities.supports_async_jobs is False
    assert capabilities.supports_job_status is False
    assert capabilities.supports_job_result is False
    assert capabilities.job_progress_fields == ()
