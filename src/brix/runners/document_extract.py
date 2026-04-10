"""Document extraction preparation and Daigestr execution runners."""
from __future__ import annotations

import base64
import os
import time
from pathlib import Path
from typing import Any

import httpx

from brix.config import config
from brix.runners.base import BaseRunner, _coerce_bool
from brix.serialization import sanitize_for_json


def _daigestr_base_url() -> str:
    return os.environ.get("BRIX_DAIGESTR_URL", "http://daigestr:8080")


def _normalize_payload(step: Any, context: Any) -> dict[str, Any]:
    params = getattr(step, "params", None)
    if isinstance(params, dict) and params:
        return params
    config = getattr(step, "config", None)
    if isinstance(config, dict) and config:
        return config
    last_output = getattr(context, "last_output", None) if context is not None else None
    return last_output if isinstance(last_output, dict) else {}


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
            "filename": payload.get("filename") or path.name,
            "language": payload.get("language") or "de",
            "mime_type": payload.get("mime_type") or "",
            "metadata": payload.get("metadata") or {},
        }
        if payload.get("template"):
            request_payload["template"] = payload["template"]

        base_url = _daigestr_base_url().rstrip("/")
        endpoint = str(payload.get("endpoint") or "/v1/extract")
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

        normalized = data.get("normalized")
        if not isinstance(normalized, dict):
            normalized = data.get("extracted") if isinstance(data.get("extracted"), dict) else {}

        result = sanitize_for_json(
            {
                "normalized": normalized,
                "document_type": data.get("document_type") or normalized.get("document_type") or "",
                "quality_score": data.get("quality_score", data.get("_quality_score")),
                "_quality_score": data.get("_quality_score", data.get("quality_score")),
                "markdown": data.get("markdown", ""),
                "raw": data,
                "warnings": data.get("warnings", []),
            }
        )
        return {"success": True, "data": result, "duration": time.monotonic() - start}
