from __future__ import annotations

import json
import time

import pytest

from brix.history import RunHistory
from brix.mcp_handlers.runs import _handle_get_run_errors, _handle_get_run_status


@pytest.mark.asyncio
async def test_completed_run_keeps_persisted_external_progress_history(tmp_path, monkeypatch):
    import brix.context as context_mod
    from brix.config import config

    runs_base = tmp_path / "runs"
    run_id = "run-external-progress-history"
    run_dir = runs_base / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", runs_base)
    monkeypatch.setattr(config, "RUNS_BASE_DIR", str(runs_base), raising=False)

    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "pipeline": "ext-progress",
                "status": "completed",
                "completed_steps": ["extract"],
                "progress": {
                    "step_id": "extract",
                    "stage": "result",
                    "attempt_number": 2,
                    "attempt_count": 2,
                    "attempt_mode": "full",
                    "retry_reason": "low_quality",
                    "request_id": "req-progress",
                    "message": "response received",
                },
            }
        )
    )
    (run_dir / "step_progress_history.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"step_id": "extract", "stage": "request", "attempt_number": 1, "attempt_mode": "default"}),
                json.dumps(
                    {
                        "step_id": "extract",
                        "stage": "result",
                        "attempt_number": 2,
                        "attempt_count": 2,
                        "attempt_mode": "full",
                        "retry_reason": "low_quality",
                        "request_id": "req-progress",
                    }
                ),
            ]
        )
        + "\n"
    )

    history = RunHistory()
    history.record_start(run_id=run_id, pipeline="ext-progress", version="1.0.0", input_data={})
    history.record_finish(
        run_id=run_id,
        success=True,
        duration=1.2,
        steps={"extract": {"status": "ok", "duration": 1.2}},
        result_summary={"status": "ok"},
    )

    status = await _handle_get_run_status({"run_id": run_id})

    assert status["success"] is True
    assert status["source"] == "history"
    assert status["current_progress"]["stage"] == "result"
    assert status["current_progress"]["attempt"] == 2
    assert status["step_progress_history"][0]["stage"] == "request"
    assert status["step_progress_history"][1]["retry_reason"] == "low_quality"


@pytest.mark.asyncio
async def test_stale_live_run_json_does_not_mask_finished_history(tmp_path, monkeypatch):
    import brix.context as context_mod

    runs_base = tmp_path / "runs"
    run_id = "run-external-stale-live"
    run_dir = runs_base / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", runs_base)

    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "pipeline": "ext-stale",
                "input": {},
                "status": "running",
                "completed_steps": [],
                "last_heartbeat": time.time() - 900,
            }
        )
    )

    history = RunHistory()
    history.record_start(run_id=run_id, pipeline="ext-stale", version="1.0.0", input_data={})
    history.record_finish(run_id=run_id, success=True, duration=0.5, steps={}, result_summary={"done": True})

    status = await _handle_get_run_status({"run_id": run_id})

    assert status["success"] is True
    assert status["source"] == "history"
    assert status["result"] == {"done": True}


@pytest.mark.asyncio
async def test_get_run_errors_surfaces_structured_external_retry_failure():
    run_id = "run-external-structured-error"
    history = RunHistory()
    history.record_start(run_id=run_id, pipeline="ext-errors", version="1.0.0", input_data={})
    history.record_finish(
        run_id=run_id,
        success=False,
        duration=1.0,
        steps={
            "extract": {
                "status": "error",
                "duration": 1.0,
                "errors": 1,
                "error_message": "Daigestr request failed with HTTP 502",
                "error_detail": {
                    "error": "Daigestr request failed with HTTP 502",
                    "error_type": "external_job_http_error",
                    "external_job": {
                        "request_id": "req-gate",
                        "retry_reason": "low_quality",
                        "attempt_history": [
                            {"attempt": 1, "attempt_count": 2, "mode": "default", "status": "retry_triggered"},
                            {"attempt": 2, "attempt_count": 2, "mode": "full", "status": "completed"},
                        ],
                    },
                },
            }
        },
        result_summary=None,
    )

    result = await _handle_get_run_errors({"run_id": run_id})

    assert result["success"] is True
    assert result["count"] == 1
    assert result["errors"][0]["error_detail"]["external_job"]["request_id"] == "req-gate"
    assert result["errors"][0]["error_detail"]["external_job"]["attempt_history"][0]["status"] == "retry_triggered"


@pytest.mark.asyncio
async def test_get_run_status_polls_service_backed_external_progress(tmp_path, monkeypatch):
    import brix.context as context_mod
    from brix.config import config

    runs_base = tmp_path / "runs"
    run_id = "run-poll-external-service"
    run_dir = runs_base / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(context_mod, "WORKDIR_BASE", runs_base)
    monkeypatch.setattr(config, "DAIGESTR_URL", "http://daigestr:8081", raising=False)
    monkeypatch.setattr(config, "DAIGESTR_JOB_STATUS_ENDPOINT_TEMPLATE", "/v1/jobs/{job_id}", raising=False)

    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "pipeline": "ext-poll",
                "input": {},
                "status": "running",
                "completed_steps": [],
                "last_heartbeat": time.time(),
                "progress": {
                    "step_id": "extract",
                    "service": "daigestr",
                    "job_id": "job-123",
                    "status": "queued",
                    "stage": "request",
                    "message": "job queued",
                },
            }
        )
    )

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
            assert url == "http://daigestr:8081/v1/jobs/job-123"
            return FakeResponse(
                {
                    "status": "processing",
                    "progress": {
                        "job_id": "job-123",
                        "request_id": "req-123",
                        "current_stage": "extract",
                        "message": "extracting",
                        "percent": 63,
                        "attempt_number": 2,
                        "attempt_count": 2,
                        "attempt_mode": "full",
                        "page_current": 33,
                        "page_total": 52,
                    },
                }
            )

    monkeypatch.setattr("brix.mcp_handlers.runs.httpx.AsyncClient", FakeClient)

    status = await _handle_get_run_status({"run_id": run_id})

    assert status["success"] is True
    assert status["source"] == "live"
    assert status["current_progress"]["service"] == "daigestr"
    assert status["current_progress"]["job_id"] == "job-123"
    assert status["current_progress"]["status"] == "processing"
    assert status["current_progress"]["stage"] == "extract"
    assert status["current_progress"]["attempt"] == 2
    assert status["current_progress"]["mode"] == "full"
    assert status["current_progress"]["page_current"] == 33
    assert status["current_progress"]["page_total"] == 52
