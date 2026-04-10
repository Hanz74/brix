"""Source/download runners for reusable remote fetch-and-store patterns."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import mimetypes
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from brix.config import config
from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout
from brix.serialization import sanitize_for_json

_DEFAULT_DOWNLOAD_DIR = Path("/tmp/brix-downloads")
_TEXTUAL_MIME_PREFIXES = ("text/",)
_TEXTUAL_MIME_TYPES = {
    "application/json",
    "application/xml",
    "application/pdf",
    "application/javascript",
}


def _safe_filename(value: str, fallback: str) -> str:
    candidate = (value or "").strip()
    candidate = os.path.basename(candidate)
    candidate = "".join(ch for ch in candidate if ch.isalnum() or ch in {"-", "_", ".", " "}).strip()
    return candidate or fallback


def _infer_filename(url: str, filename: str | None = None) -> str:
    if filename:
        return _safe_filename(filename, "download.bin")
    parsed = urllib.parse.urlparse(url)
    basename = os.path.basename(parsed.path.rstrip("/"))
    return _safe_filename(basename, "download.bin")


def _is_extractable(mime_type: str) -> bool:
    if not mime_type:
        return False
    lowered = mime_type.lower()
    return lowered.startswith(_TEXTUAL_MIME_PREFIXES) or lowered in _TEXTUAL_MIME_TYPES


def _build_result(path: Path, source_url: str, mime_type: str, data: bytes) -> dict[str, Any]:
    digest = hashlib.sha256(data).hexdigest()
    return sanitize_for_json(
        {
            "file_bytes_path": str(path),
            "source_url": source_url,
            "file_size": len(data),
            "mime_type": mime_type,
            "content_hash": digest,
            "extractable": _is_extractable(mime_type),
            "file": {
                "name": path.name,
                "extension": path.suffix.lstrip("."),
            },
            "success": True,
        }
    )


def _resolve_target_path(output_dir: str | None, filename: str) -> Path:
    base = Path(output_dir).expanduser() if output_dir else _DEFAULT_DOWNLOAD_DIR
    base.mkdir(parents=True, exist_ok=True)
    return base / filename


class SourceDownloadToFileRunner(BaseRunner):
    """Download a remote URL to a managed local file and return a normalized contract."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "Remote file URL", "required": True},
                "filename": {"type": "string", "description": "Optional target filename override"},
                "output_dir": {"type": "string", "description": "Target directory for the downloaded file"},
                "headers": {"type": "object", "description": "Optional HTTP headers"},
                "timeout_seconds": {"type": "integer", "description": "Optional request timeout override"},
            },
            "required": ["url"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "remote_download_result"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else config.BRIX_DEFAULT_TIMEOUT
        try:
            return await asyncio.wait_for(self._execute_inner(step), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return {"success": False, "error": f"Timeout after {timeout_seconds}s", "duration": time.monotonic() - start}

    async def _execute_inner(self, step: Any) -> dict:
        start = time.monotonic()
        cfg = getattr(step, "config", None) if isinstance(getattr(step, "config", None), dict) else {}
        url = str(cfg.get("url") or "").strip()
        if not url:
            return {"success": False, "error": "'url' is required", "duration": time.monotonic() - start}
        filename = _infer_filename(url, cfg.get("filename"))
        headers = cfg.get("headers") if isinstance(cfg.get("headers"), dict) else {}
        output_path = _resolve_target_path(cfg.get("output_dir"), filename)
        request_timeout = int(cfg.get("timeout_seconds") or 30)

        self.report_progress(10.0, "downloading remote file")
        req = urllib.request.Request(url, headers={str(k): str(v) for k, v in headers.items()})
        try:
            with urllib.request.urlopen(req, timeout=request_timeout) as response:
                data = response.read()
                mime_type = response.headers.get_content_type() or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        except Exception as exc:
            return {"success": False, "error": str(exc), "duration": time.monotonic() - start}

        output_path.write_bytes(data)
        self.report_progress(100.0, f"saved {len(data)} bytes to {output_path.name}")
        return {"success": True, "data": _build_result(output_path, url, mime_type, data), "duration": time.monotonic() - start}


class SourcePersistDownloadPayloadRunner(BaseRunner):
    """Persist a fetched payload to a managed file and emit the standard download contract."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "filename": {"type": "string", "description": "Target filename", "required": True},
                "output_dir": {"type": "string", "description": "Target directory for the persisted file"},
                "base64": {"type": "string", "description": "Base64-encoded payload to persist"},
                "text": {"type": "string", "description": "Plain-text payload to persist"},
                "source_url": {"type": "string", "description": "Optional original source URL"},
                "mime_type": {"type": "string", "description": "Optional MIME type override"},
            },
            "required": ["filename"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "remote_download_result"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        cfg = getattr(step, "config", None) if isinstance(getattr(step, "config", None), dict) else {}
        raw_filename = str(cfg.get("filename") or "").strip()
        if not raw_filename:
            return {"success": False, "error": "'filename' is required", "duration": time.monotonic() - start}
        filename = _safe_filename(raw_filename, "download.bin")

        raw_base64 = cfg.get("base64")
        raw_text = cfg.get("text")
        if raw_base64 in (None, "") and raw_text in (None, ""):
            return {"success": False, "error": "Provide either 'base64' or 'text'", "duration": time.monotonic() - start}

        if raw_base64 not in (None, ""):
            try:
                data = base64.b64decode(str(raw_base64), validate=True)
            except Exception as exc:
                return {"success": False, "error": f"Invalid base64 payload: {exc}", "duration": time.monotonic() - start}
        else:
            data = str(raw_text).encode("utf-8")

        output_path = _resolve_target_path(cfg.get("output_dir"), filename)
        mime_type = str(cfg.get("mime_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream")
        source_url = str(cfg.get("source_url") or "")

        self.report_progress(50.0, "persisting download payload")
        output_path.write_bytes(data)
        self.report_progress(100.0, f"saved {len(data)} bytes to {output_path.name}")
        return {"success": True, "data": _build_result(output_path, source_url, mime_type, data), "duration": time.monotonic() - start}
