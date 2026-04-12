"""Document extraction preparation and Daigestr execution runners."""
from __future__ import annotations

import base64
import time
from pathlib import Path
from typing import Any

import httpx

from brix.config import BrixConfig, config
from brix.runners.base import BaseRunner, _coerce_bool
from brix.serialization import sanitize_for_json


def _daigestr_base_url() -> str:
    return BrixConfig.reload().DAIGESTR_URL


def _daigestr_default_endpoint() -> str:
    return BrixConfig.reload().DAIGESTR_CONVERT_ENDPOINT


def _daigestr_default_mode() -> str:
    return BrixConfig.reload().DAIGESTR_MODE


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
    return meta if isinstance(meta, dict) else {}


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

        request_payload = {
            "content": base64_content,
            "base64": base64_content,
            "filename": payload.get("filename") or path.name,
            "language": payload.get("language") or "de",
            "mime_type": payload.get("mime_type") or "",
            "metadata": payload.get("metadata") or {},
            "mode": payload.get("mode") or _daigestr_default_mode(),
            "auto_extract": True,
            "retry_on_low_quality": _coerce_bool(
                payload.get("retry_on_low_quality", BrixConfig.reload().DAIGESTR_RETRY_ON_LOW_QUALITY)
            ),
            "quality_retry_threshold": float(
                payload.get("quality_retry_threshold", BrixConfig.reload().DAIGESTR_QUALITY_RETRY_THRESHOLD)
            ),
            "quality_retry_mode": str(
                payload.get("quality_retry_mode") or BrixConfig.reload().DAIGESTR_QUALITY_RETRY_MODE
            ),
        }
        if payload.get("template"):
            request_payload["template"] = payload["template"]

        base_url = _daigestr_base_url().rstrip("/")
        endpoint = str(payload.get("endpoint") or _daigestr_default_endpoint())
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        url = f"{base_url}{endpoint}"

        try:
            async with httpx.AsyncClient(timeout=config.BRIX_DEFAULT_TIMEOUT) as client:
                response = await client.post(url, json=request_payload, headers={"Content-Type": "application/json"})
                response.raise_for_status()
                data = response.json()
        except Exception as exc:
            return {"success": False, "error": str(exc), "duration": time.monotonic() - start}

        meta = _canonical_daigestr_meta(data)
        normalized = data.get("normalized") if isinstance(data.get("normalized"), dict) else {}
        extracted = data.get("extracted") if isinstance(data.get("extracted"), dict) else {}
        if not normalized:
            normalized = extracted

        document_type = _first_mapping(meta, "document_type") or ""
        quality_score = (
            _first_mapping(meta, "quality_score")
            or _first_mapping(meta, "final_quality_score")
            or _first_mapping(meta, "initial_quality_score")
        )
        template_name = _first_mapping(meta, "template_used")

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
                },
                "raw": data,
                "warnings": data.get("warnings", []),
            }
        )
        return {"success": True, "data": result, "duration": time.monotonic() - start}
