"""Document extraction preparation and Daigestr execution runners."""
from __future__ import annotations

import base64
import asyncio
import json
import time
from pathlib import Path
from typing import Any

import httpx

from brix.config import BrixConfig, config
from brix.external_job_progress import canonicalize_external_job_progress
from brix.runners.base import BaseRunner, _coerce_bool
from brix.serialization import sanitize_for_json


def _daigestr_base_url() -> str:
    return BrixConfig.reload().DAIGESTR_URL


def _daigestr_default_endpoint() -> str:
    return BrixConfig.reload().DAIGESTR_CONVERT_ENDPOINT


def _daigestr_async_start_endpoint() -> str:
    return BrixConfig.reload().DAIGESTR_ASYNC_CONVERT_ENDPOINT


def _daigestr_use_async_jobs() -> bool:
    return BrixConfig.reload().DAIGESTR_USE_ASYNC_JOBS


def _daigestr_job_status_endpoint(job_id: str) -> str:
    template = BrixConfig.reload().DAIGESTR_JOB_STATUS_ENDPOINT_TEMPLATE
    return template.format(job_id=job_id)


def _daigestr_job_result_endpoint(job_id: str) -> str:
    template = BrixConfig.reload().DAIGESTR_JOB_RESULT_ENDPOINT_TEMPLATE
    return template.format(job_id=job_id)


def _daigestr_job_poll_interval_seconds() -> float:
    return BrixConfig.reload().DAIGESTR_JOB_POLL_INTERVAL_SECONDS


def _daigestr_default_mode() -> str:
    return BrixConfig.reload().DAIGESTR_MODE


def _join_url(base_url: str, endpoint: str) -> str:
    normalized_endpoint = str(endpoint or "").strip()
    if not normalized_endpoint.startswith("/"):
        normalized_endpoint = f"/{normalized_endpoint}"
    return f"{base_url.rstrip('/')}{normalized_endpoint}"


def _first_mapping(mapping: Any, *keys: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _normalize_payload(step: Any, context: Any) -> dict[str, Any]:
    params = getattr(step, "params", None)
    if isinstance(params, dict) and params:
        return params
    config = getattr(step, "config", None)
    if isinstance(config, dict) and config:
        return config
    last_output = getattr(context, "last_output", None) if context is not None else None
    return last_output if isinstance(last_output, dict) else {}


def _canonical_daigestr_meta(data: dict[str, Any]) -> dict[str, Any]:
    meta = data.get("meta")
    if isinstance(meta, dict):
        return meta
    raw = data.get("raw")
    raw_meta = raw.get("meta") if isinstance(raw, dict) else None
    return raw_meta if isinstance(raw_meta, dict) else {}


def _canonical_daigestr_business_payloads(data: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    raw = data.get("raw")
    raw_payload = dict(raw) if isinstance(raw, dict) else {}

    top_extracted = data.get("extracted") if isinstance(data.get("extracted"), dict) else {}
    top_normalized = data.get("normalized") if isinstance(data.get("normalized"), dict) else {}
    raw_extracted = raw_payload.get("extracted") if isinstance(raw_payload.get("extracted"), dict) else {}
    raw_normalized = raw_payload.get("normalized") if isinstance(raw_payload.get("normalized"), dict) else {}

    if not raw_extracted and top_extracted:
        raw_extracted = top_extracted
    if not raw_normalized and top_normalized:
        raw_normalized = top_normalized

    raw_payload["meta"] = _canonical_daigestr_meta(data)
    raw_payload["extracted"] = raw_extracted
    raw_payload["normalized"] = raw_normalized
    return raw_payload, raw_extracted, raw_normalized


def _attempt_history(meta: dict[str, Any], fallback_mode: str) -> list[dict[str, Any]]:
    request_id = _first_mapping(meta, "request_id")
    retry_applied = bool(_first_mapping(meta, "retry_applied"))
    retry_reason = _first_mapping(meta, "retry_reason")
    retry_threshold_used = _first_mapping(meta, "retry_threshold_used")
    initial_mode = _first_mapping(meta, "initial_mode") or fallback_mode
    final_mode = _first_mapping(meta, "final_mode") or _first_mapping(meta, "attempt_mode") or initial_mode
    final_attempt = _first_mapping(meta, "attempt_number") or _first_mapping(meta, "attempt_count") or 1
    attempt_count = _first_mapping(meta, "attempt_count") or final_attempt or 1
    history: list[dict[str, Any]] = []

    if initial_mode:
        first_attempt = {
            "attempt": 1,
            "mode": initial_mode,
            "quality_score": _first_mapping(meta, "initial_quality_score"),
            "request_id": request_id,
            "status": "retry_triggered" if retry_applied and final_mode and final_mode != initial_mode else "completed",
        }
        if retry_reason:
            first_attempt["retry_reason"] = retry_reason
        if retry_threshold_used not in (None, ""):
            first_attempt["retry_threshold_used"] = retry_threshold_used
        history.append(first_attempt)

    final_attempt_entry = {
        "attempt": int(final_attempt),
        "mode": final_mode,
        "quality_score": _first_mapping(meta, "final_quality_score") or _first_mapping(meta, "quality_score"),
        "request_id": request_id,
        "status": "completed",
    }
    if retry_reason:
        final_attempt_entry["retry_reason"] = retry_reason
    if retry_threshold_used not in (None, ""):
        final_attempt_entry["retry_threshold_used"] = retry_threshold_used

    if not history:
        history.append(final_attempt_entry)
    elif history[-1]["attempt"] != final_attempt_entry["attempt"] or history[-1]["mode"] != final_attempt_entry["mode"]:
        history.append(final_attempt_entry)
    else:
        history[-1] = final_attempt_entry

    for entry in history:
        entry["attempt_count"] = int(attempt_count)
    return sanitize_for_json(history)


def _persist_daigestr_artifacts(
    context: Any,
    step_id: str,
    request_payload: dict[str, Any],
    response_data: dict[str, Any],
    attempt_history: list[dict[str, Any]],
) -> dict[str, Any]:
    if context is None or not hasattr(context, "workdir"):
        return {}
    artifact_dir = Path(context.workdir) / "external_job_artifacts" / step_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    request_summary = dict(request_payload)
    base64_content = str(request_summary.pop("base64", "") or "")
    content = str(request_summary.pop("content", "") or "")
    request_summary["base64_bytes"] = len(base64_content)
    request_summary["content_bytes"] = len(content)
    request_summary = sanitize_for_json(request_summary)

    response_summary = dict(response_data)
    markdown = str(response_summary.pop("markdown", "") or "")
    response_summary = sanitize_for_json(response_summary)

    request_path = artifact_dir / "request.json"
    response_path = artifact_dir / "response.json"
    attempt_history_path = artifact_dir / "attempt_history.json"
    markdown_path = artifact_dir / "markdown.md"

    request_path.write_text(json.dumps(request_summary, indent=2, ensure_ascii=True))
    response_summary["markdown_path"] = str(markdown_path.relative_to(context.workdir)) if markdown else None
    response_summary["markdown_chars"] = len(markdown)
    response_path.write_text(json.dumps(response_summary, indent=2, ensure_ascii=True))
    attempt_history_path.write_text(json.dumps(attempt_history, indent=2, ensure_ascii=True))
    if markdown:
        markdown_path.write_text(markdown, encoding="utf-8")

    return sanitize_for_json(
        {
            "request_path": str(request_path.relative_to(context.workdir)),
            "response_path": str(response_path.relative_to(context.workdir)),
            "attempt_history_path": str(attempt_history_path.relative_to(context.workdir)),
            "markdown_path": str(markdown_path.relative_to(context.workdir)) if markdown else None,
        }
    )


def _resolve_replay_path(path_value: str, context: Any) -> Path:
    raw = Path(path_value)
    if raw.is_absolute():
        return raw
    if context is not None and hasattr(context, "workdir"):
        candidate = Path(context.workdir) / raw
        if candidate.exists():
            return candidate
    return Path.cwd() / raw


def _load_replay_response(
    payload: dict[str, Any],
    context: Any,
    step_id: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    fixture_path = str(payload.get("replay_fixture_path") or "").strip()
    artifact_path = str(payload.get("replay_response_path") or "").strip()
    source_type = "fixture" if fixture_path else "artifact"
    source_path: Path | None = None
    if fixture_path or artifact_path:
        source_path = _resolve_replay_path(fixture_path or artifact_path, context)
    elif getattr(context, "_resume_from", None) and context is not None and hasattr(context, "workdir"):
        candidate = Path(context.workdir) / "external_job_artifacts" / step_id / "response.json"
        if candidate.exists():
            source_type = "resume_artifact"
            source_path = candidate

    if source_path is None:
        return None, None

    raw_payload = json.loads(source_path.read_text())
    if not isinstance(raw_payload, dict):
        raise ValueError(f"Replay source must contain a JSON object: {source_path}")

    extraction_result = raw_payload.get("extraction_result")
    response_data = extraction_result if isinstance(extraction_result, dict) else raw_payload
    if not isinstance(response_data, dict):
        raise ValueError(f"Replay source does not contain a usable extraction payload: {source_path}")

    return (
        {
            "source_type": source_type,
            "source_path": str(source_path),
        },
        response_data,
    )


def _structured_daigestr_error(
    *,
    message: str,
    error_type: str,
    request_payload: dict[str, Any],
    url: str,
    response_data: dict[str, Any] | None = None,
    status_code: int | None = None,
    artifacts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response_payload = response_data if isinstance(response_data, dict) else {}
    meta = _canonical_daigestr_meta(response_payload)
    attempt_history = _attempt_history(meta, fallback_mode=str(request_payload.get("mode") or _daigestr_default_mode()))
    if not attempt_history:
        attempt_history = sanitize_for_json(
            [
                {
                    "attempt": 1,
                    "attempt_count": 1,
                    "mode": str(request_payload.get("mode") or _daigestr_default_mode()),
                    "status": "failed",
                }
            ]
        )
    external_job = sanitize_for_json(
        {
            "service": "daigestr",
            "url": url,
            "http_status": status_code,
            "request_id": _first_mapping(meta, "request_id"),
            "stage": _first_mapping(meta, "stage"),
            "attempt_number": _first_mapping(meta, "attempt_number"),
            "attempt_count": _first_mapping(meta, "attempt_count"),
            "attempt_mode": _first_mapping(meta, "attempt_mode"),
            "retry_applied": _first_mapping(meta, "retry_applied"),
            "retry_reason": _first_mapping(meta, "retry_reason"),
            "initial_mode": _first_mapping(meta, "initial_mode"),
            "final_mode": _first_mapping(meta, "final_mode"),
            "initial_quality_score": _first_mapping(meta, "initial_quality_score"),
            "final_quality_score": _first_mapping(meta, "final_quality_score"),
            "retry_threshold_used": _first_mapping(meta, "retry_threshold_used"),
            "attempt_history": attempt_history,
            "artifacts": artifacts or None,
        }
    )
    return {
        "error": message,
        "error_type": error_type,
        "external_job": external_job,
    }


def _update_external_job_progress(context: Any, step_id: str, payload: dict[str, Any]) -> None:
    if context is None or not hasattr(context, "update_step_progress"):
        return
    progress = dict(payload)
    progress["service"] = "daigestr"
    context.update_step_progress(step_id, progress)


def _service_backed_progress(progress_payload: dict[str, Any], *, request_payload: dict[str, Any], job_id: str) -> dict[str, Any]:
    progress = canonicalize_external_job_progress(progress_payload)
    progress["service"] = "daigestr"
    progress["job_id"] = job_id
    progress["attempt_mode"] = progress.get("mode") or request_payload.get("mode")
    if progress.get("request_id") in (None, ""):
        progress["request_id"] = _first_mapping(progress_payload, "request_id")
    return sanitize_for_json(progress)


class DocumentPrepareExtractablePayloadRunner(BaseRunner):
    """Normalize file/base64 inputs into the document_extract_input contract."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "file_bytes_path": {"type": "string", "description": "Absolute path to the local file"},
                "base64": {"type": "string", "description": "Optional base64 file content"},
                "mime_type": {"type": "string", "description": "Optional MIME type hint"},
                "filename": {"type": "string", "description": "Optional filename"},
                "language": {"type": "string", "description": "Optional language hint"},
                "metadata": {"type": "object", "description": "Optional extra metadata"},
                "include_base64": {"type": "boolean", "description": "Read the file and include base64 in the output"},
                "markdown": {"type": "string", "description": "Optional pre-rendered markdown"},
            },
            "required": ["file_bytes_path"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "document_extract_input"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        payload = _normalize_payload(step, context)
        file_bytes_path = str(payload.get("file_bytes_path") or "").strip()
        if not file_bytes_path:
            return {"success": False, "error": "'file_bytes_path' is required", "duration": time.monotonic() - start}
        path = Path(file_bytes_path)
        if not path.exists():
            return {"success": False, "error": f"File does not exist: {file_bytes_path}", "duration": time.monotonic() - start}

        include_base64 = _coerce_bool(payload.get("include_base64", False))
        base64_content = payload.get("base64")
        if include_base64 and not base64_content:
            base64_content = base64.b64encode(path.read_bytes()).decode("ascii")

        result = sanitize_for_json(
            {
                "file_bytes_path": str(path),
                "mime_type": payload.get("mime_type") or "",
                "base64": base64_content or "",
                "markdown": payload.get("markdown") or "",
                "language": payload.get("language") or "",
                "metadata": payload.get("metadata") or {},
                "filename": payload.get("filename") or path.name,
            }
        )
        return {"success": True, "data": result, "duration": time.monotonic() - start}


class ExtractDocumentWithDaigestrRunner(BaseRunner):
    """Execute a Daigestr extraction call against a normalized payload."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "file_bytes_path": {"type": "string", "description": "Absolute path to the local file"},
                "base64": {"type": "string", "description": "Optional base64 file content"},
                "filename": {"type": "string", "description": "Optional filename"},
                "language": {"type": "string", "description": "Optional language hint"},
                "metadata": {"type": "object", "description": "Optional extra metadata"},
                "endpoint": {"type": "string", "description": "Optional Daigestr endpoint override"},
                "template": {"type": "string", "description": "Optional extraction template"},
                "mode": {"type": "string", "description": "Optional Daigestr mode override"},
                "use_async_jobs": {
                    "type": "boolean",
                    "description": "Prefer the Daigestr async job contract when the service supports it",
                },
                "replay_fixture_path": {
                    "type": "string",
                    "description": "Optional path to a persisted Daigestr fixture for offline replay",
                },
                "replay_response_path": {
                    "type": "string",
                    "description": "Optional path to a persisted response.json artifact for offline replay",
                },
                "retry_on_low_quality": {
                    "type": "boolean",
                    "description": "Retry extraction with a stronger mode when quality is below threshold",
                },
                "quality_retry_threshold": {
                    "type": "number",
                    "description": "Quality threshold below which Daigestr retries the extraction",
                },
                "quality_retry_mode": {
                    "type": "string",
                    "description": "Target mode used for low-quality retry escalation",
                },
                "mime_type": {"type": "string", "description": "Optional MIME type hint"},
            },
            "required": [],
        }

    def input_type(self) -> str:
        return "document_extract_input"

    def output_type(self) -> str:
        return "document_extraction_result"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        step_id = str(getattr(step, "id", "extract") or "extract")
        payload = _normalize_payload(step, context)
        file_bytes_path = str(payload.get("file_bytes_path") or "").strip()
        if not file_bytes_path:
            return {"success": False, "error": "'file_bytes_path' is required", "duration": time.monotonic() - start}
        path = Path(file_bytes_path)
        if not path.exists():
            return {"success": False, "error": f"File does not exist: {file_bytes_path}", "duration": time.monotonic() - start}

        base64_content = payload.get("base64")
        if not base64_content:
            base64_content = base64.b64encode(path.read_bytes()).decode("ascii")

        retry_on_low_quality = payload.get("retry_on_low_quality")
        if retry_on_low_quality is None:
            retry_on_low_quality = BrixConfig.reload().DAIGESTR_RETRY_ON_LOW_QUALITY
        quality_retry_threshold = payload.get("quality_retry_threshold")
        if quality_retry_threshold in (None, ""):
            quality_retry_threshold = BrixConfig.reload().DAIGESTR_QUALITY_RETRY_THRESHOLD
        quality_retry_mode = payload.get("quality_retry_mode") or BrixConfig.reload().DAIGESTR_QUALITY_RETRY_MODE

        request_payload = {
            "content": base64_content,
            "base64": base64_content,
            "filename": payload.get("filename") or path.name,
            "language": payload.get("language") or "de",
            "mime_type": payload.get("mime_type") or "",
            "metadata": payload.get("metadata") or {},
            "mode": payload.get("mode") or _daigestr_default_mode(),
            "auto_extract": True,
            "retry_on_low_quality": _coerce_bool(retry_on_low_quality),
            "quality_retry_threshold": float(quality_retry_threshold),
            "quality_retry_mode": str(quality_retry_mode),
        }
        if payload.get("template"):
            request_payload["template"] = payload["template"]

        base_url = _daigestr_base_url().rstrip("/")
        endpoint = str(payload.get("endpoint") or _daigestr_default_endpoint())
        url = _join_url(base_url, endpoint)
        async_start_url = _join_url(base_url, _daigestr_async_start_endpoint())
        retry_enabled = _coerce_bool(request_payload["retry_on_low_quality"])
        use_async_jobs = _coerce_bool(payload.get("use_async_jobs"))
        if "use_async_jobs" not in payload:
            use_async_jobs = _daigestr_use_async_jobs()

        _update_external_job_progress(
            context,
            step_id,
            {
                "stage": "request",
                "status": "running",
                "attempt_number": 1,
                "attempt_count": 1,
                "attempt_mode": request_payload["mode"],
                "retry_on_low_quality": retry_enabled,
                "message": "calling daigestr",
            },
        )

        async_job_id: str | None = None
        replay, data = _load_replay_response(payload, context, step_id)
        if replay is not None:
            _update_external_job_progress(
                context,
                step_id,
                {
                    "stage": "replay",
                    "status": "running",
                    "attempt_number": 1,
                    "attempt_count": 1,
                    "attempt_mode": request_payload["mode"],
                    "message": f"replaying daigestr response from {replay['source_type']}",
                },
            )
        else:
            try:
                async with httpx.AsyncClient(timeout=config.BRIX_DEFAULT_TIMEOUT) as client:
                    if use_async_jobs:
                        async_response = await client.post(
                            async_start_url,
                            json=request_payload,
                            headers={"Content-Type": "application/json"},
                        )
                        async_payload: dict[str, Any]
                        try:
                            parsed_async = async_response.json()
                            async_payload = parsed_async if isinstance(parsed_async, dict) else {}
                        except ValueError:
                            async_payload = {}
                        if not getattr(async_response, "is_error", False):
                            async_job_id = str(async_payload.get("job_id") or "").strip() or None
                        if async_job_id:
                            _update_external_job_progress(
                                context,
                                step_id,
                                {
                                    **_service_backed_progress(
                                        async_payload.get("progress") if isinstance(async_payload.get("progress"), dict) else async_payload,
                                        request_payload=request_payload,
                                        job_id=async_job_id,
                                    ),
                                    "status": async_payload.get("status") or "queued",
                                    "stage": _first_mapping(
                                        async_payload.get("progress") if isinstance(async_payload.get("progress"), dict) else async_payload,
                                        "current_stage",
                                        "stage",
                                    )
                                    or "queued",
                                    "message": _first_mapping(
                                        async_payload.get("progress") if isinstance(async_payload.get("progress"), dict) else async_payload,
                                        "message",
                                        "msg",
                                    )
                                    or "daigestr async job started",
                                },
                            )
                            terminal_status = ""
                            while True:
                                status_response = await client.get(_join_url(base_url, _daigestr_job_status_endpoint(async_job_id)))
                                try:
                                    parsed_status = status_response.json()
                                    status_payload = parsed_status if isinstance(parsed_status, dict) else {}
                                except ValueError:
                                    status_payload = {}
                                if getattr(status_response, "is_error", False):
                                    artifacts = _persist_daigestr_artifacts(
                                        context,
                                        step_id,
                                        request_payload,
                                        status_payload if isinstance(status_payload, dict) else {"response_text": str(status_payload)},
                                        _attempt_history(
                                            _canonical_daigestr_meta(status_payload if isinstance(status_payload, dict) else {}),
                                            fallback_mode=request_payload["mode"],
                                        ),
                                    )
                                    error = _structured_daigestr_error(
                                        message=f"Daigestr status poll failed with HTTP {getattr(status_response, 'status_code', 'unknown')}",
                                        error_type="external_job_http_error",
                                        request_payload=request_payload,
                                        url=_join_url(base_url, _daigestr_job_status_endpoint(async_job_id)),
                                        response_data=status_payload if isinstance(status_payload, dict) else None,
                                        status_code=getattr(status_response, "status_code", None),
                                        artifacts=artifacts,
                                    )
                                    _update_external_job_progress(
                                        context,
                                        step_id,
                                        {
                                            "job_id": async_job_id,
                                            "stage": "error",
                                            "status": "failed",
                                            "attempt_number": _first_mapping(error["external_job"], "attempt_number") or 1,
                                            "attempt_count": _first_mapping(error["external_job"], "attempt_count") or 1,
                                            "attempt_mode": _first_mapping(error["external_job"], "attempt_mode") or request_payload["mode"],
                                            "retry_state": "failed",
                                            "retry_reason": _first_mapping(error["external_job"], "retry_reason"),
                                            "request_id": _first_mapping(error["external_job"], "request_id"),
                                            "message": error["error"],
                                        },
                                    )
                                    return {"success": False, "error": error, "duration": time.monotonic() - start}
                                polled_progress = status_payload.get("progress") if isinstance(status_payload.get("progress"), dict) else status_payload
                                terminal_status = str(status_payload.get("status") or _first_mapping(polled_progress if isinstance(polled_progress, dict) else {}, "status") or "").strip().lower()
                                _update_external_job_progress(
                                    context,
                                    step_id,
                                    {
                                        **_service_backed_progress(
                                            polled_progress if isinstance(polled_progress, dict) else {},
                                            request_payload=request_payload,
                                            job_id=async_job_id,
                                        ),
                                        "status": terminal_status or "processing",
                                    },
                                )
                                if terminal_status in {"completed", "done", "success", "succeeded"}:
                                    result_response = await client.get(_join_url(base_url, _daigestr_job_result_endpoint(async_job_id)))
                                    try:
                                        parsed_result = result_response.json()
                                        data = parsed_result if isinstance(parsed_result, dict) else {}
                                    except ValueError:
                                        data = {}
                                    if getattr(result_response, "is_error", False):
                                        artifacts = _persist_daigestr_artifacts(
                                            context,
                                            step_id,
                                            request_payload,
                                            data if isinstance(data, dict) else {"response_text": str(data)},
                                            _attempt_history(
                                                _canonical_daigestr_meta(data if isinstance(data, dict) else {}),
                                                fallback_mode=request_payload["mode"],
                                            ),
                                        )
                                        error = _structured_daigestr_error(
                                            message=f"Daigestr result fetch failed with HTTP {getattr(result_response, 'status_code', 'unknown')}",
                                            error_type="external_job_http_error",
                                            request_payload=request_payload,
                                            url=_join_url(base_url, _daigestr_job_result_endpoint(async_job_id)),
                                            response_data=data if isinstance(data, dict) else None,
                                            status_code=getattr(result_response, "status_code", None),
                                            artifacts=artifacts,
                                        )
                                        return {"success": False, "error": error, "duration": time.monotonic() - start}
                                    break
                                if terminal_status in {"failed", "error", "cancelled", "canceled"}:
                                    artifacts = _persist_daigestr_artifacts(
                                        context,
                                        step_id,
                                        request_payload,
                                        status_payload if isinstance(status_payload, dict) else {"response_text": str(status_payload)},
                                        _attempt_history(
                                            _canonical_daigestr_meta(status_payload if isinstance(status_payload, dict) else {}),
                                            fallback_mode=request_payload["mode"],
                                        ),
                                    )
                                    error = _structured_daigestr_error(
                                        message=f"Daigestr async job failed with status {terminal_status or 'unknown'}",
                                        error_type="external_job_runtime_error",
                                        request_payload=request_payload,
                                        url=_join_url(base_url, _daigestr_job_status_endpoint(async_job_id)),
                                        response_data=status_payload if isinstance(status_payload, dict) else None,
                                        artifacts=artifacts,
                                    )
                                    return {"success": False, "error": error, "duration": time.monotonic() - start}
                                await asyncio.sleep(_daigestr_job_poll_interval_seconds())
                    if async_job_id is None:
                        response = await client.post(url, json=request_payload, headers={"Content-Type": "application/json"})
                        try:
                            data = response.json()
                        except ValueError:
                            response_text = getattr(response, "text", "")
                            data = {"response_text": response_text[:4000]} if response_text else {}
                        if getattr(response, "is_error", False):
                            artifacts = _persist_daigestr_artifacts(
                                context,
                                step_id,
                                request_payload,
                                data if isinstance(data, dict) else {"response_text": str(data)},
                                _attempt_history(
                                    _canonical_daigestr_meta(data if isinstance(data, dict) else {}),
                                    fallback_mode=request_payload["mode"],
                                ),
                            )
                            error = _structured_daigestr_error(
                                message=f"Daigestr request failed with HTTP {getattr(response, 'status_code', 'unknown')}",
                                error_type="external_job_http_error",
                                request_payload=request_payload,
                                url=url,
                                response_data=data if isinstance(data, dict) else None,
                                status_code=getattr(response, "status_code", None),
                                artifacts=artifacts,
                            )
                            _update_external_job_progress(
                                context,
                                step_id,
                                {
                                    "stage": "error",
                                    "status": "failed",
                                    "attempt_number": _first_mapping(error["external_job"], "attempt_number") or 1,
                                    "attempt_count": _first_mapping(error["external_job"], "attempt_count") or 1,
                                    "attempt_mode": _first_mapping(error["external_job"], "attempt_mode")
                                    or request_payload["mode"],
                                    "retry_state": "failed",
                                    "retry_reason": _first_mapping(error["external_job"], "retry_reason"),
                                    "request_id": _first_mapping(error["external_job"], "request_id"),
                                    "message": error["error"],
                                },
                            )
                            return {"success": False, "error": error, "duration": time.monotonic() - start}
            except Exception as exc:
                artifacts = _persist_daigestr_artifacts(
                    context,
                    step_id,
                    request_payload,
                    {"transport_error": str(exc)},
                    [
                        {
                            "attempt": 1,
                            "attempt_count": 1,
                            "mode": request_payload["mode"],
                            "status": "failed",
                        }
                    ],
                )
                error = _structured_daigestr_error(
                    message=str(exc),
                    error_type="external_job_transport_error",
                    request_payload=request_payload,
                    url=url,
                    artifacts=artifacts,
                )
                _update_external_job_progress(
                    context,
                    step_id,
                    {
                        "stage": "error",
                        "status": "failed",
                        "attempt_number": 1,
                        "attempt_count": 1,
                        "attempt_mode": request_payload["mode"],
                        "retry_state": "failed",
                        "message": str(exc),
                    },
                )
                return {"success": False, "error": error, "duration": time.monotonic() - start}

        if isinstance(data, dict):
            meta = data.get("meta")
            if isinstance(meta, dict) and async_job_id and meta.get("job_id") in (None, ""):
                meta["job_id"] = async_job_id

        raw_payload, extracted, normalized = _canonical_daigestr_business_payloads(data)
        meta = raw_payload["meta"]
        attempt_history = _attempt_history(meta, fallback_mode=request_payload["mode"])
        raw_payload["meta"]["attempt_history"] = attempt_history
        artifacts = _persist_daigestr_artifacts(context, step_id, request_payload, data, attempt_history)
        if artifacts:
            raw_payload["meta"]["artifacts"] = artifacts
        if replay is not None:
            raw_payload["meta"]["replay"] = replay
        if not normalized:
            normalized = extracted

        document_type = _first_mapping(meta, "document_type") or ""
        quality_score = (
            _first_mapping(meta, "quality_score")
            or _first_mapping(meta, "final_quality_score")
            or _first_mapping(meta, "initial_quality_score")
        )
        template_name = _first_mapping(meta, "template_used")
        final_mode = _first_mapping(meta, "final_mode") or _first_mapping(meta, "attempt_mode") or request_payload["mode"]

        _update_external_job_progress(
            context,
            step_id,
            {
                "stage": "result",
                "status": "completed",
                "attempt_number": _first_mapping(meta, "attempt_number") or 1,
                "attempt_count": _first_mapping(meta, "attempt_count") or 1,
                "attempt_mode": final_mode,
                "retry_applied": _first_mapping(meta, "retry_applied"),
                "retry_reason": _first_mapping(meta, "retry_reason"),
                "request_id": _first_mapping(meta, "request_id"),
                "job_id": _first_mapping(meta, "job_id"),
                "message": "daigestr response received",
            },
        )

        result = sanitize_for_json(
            {
                "normalized": normalized,
                "document_type": document_type,
                "quality_score": quality_score,
                "_quality_score": quality_score,
                "markdown": data.get("markdown", ""),
                "_meta": {
                    "template": template_name,
                    "document_type_confidence": _first_mapping(meta, "document_type_confidence"),
                    "quality_grade": _first_mapping(meta, "quality_grade"),
                    "retry_applied": _first_mapping(meta, "retry_applied"),
                    "retry_reason": _first_mapping(meta, "retry_reason"),
                    "initial_mode": _first_mapping(meta, "initial_mode"),
                    "final_mode": _first_mapping(meta, "final_mode"),
                    "initial_quality_score": _first_mapping(meta, "initial_quality_score"),
                    "final_quality_score": _first_mapping(meta, "final_quality_score"),
                    "retry_threshold_used": _first_mapping(meta, "retry_threshold_used"),
                    "request_id": _first_mapping(meta, "request_id"),
                    "attempt_number": _first_mapping(meta, "attempt_number"),
                    "attempt_count": _first_mapping(meta, "attempt_count"),
                    "attempt_mode": _first_mapping(meta, "attempt_mode"),
                    "pipeline_steps": meta.get("pipeline_steps") if isinstance(meta.get("pipeline_steps"), list) else None,
                    "attempt_history": attempt_history,
                    "artifacts": artifacts or None,
                    "replay": replay,
                },
                "raw": raw_payload,
                "attempt_history": attempt_history,
                "artifacts": artifacts,
                "replay": replay,
                "warnings": data.get("warnings", []),
            }
        )
        return {"success": True, "data": result, "duration": time.monotonic() - start}
