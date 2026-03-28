"""Markitdown runner — converts files to Markdown via markitdown-mcp service."""
import base64
import os
import time
from pathlib import Path
from typing import Any

import httpx

from brix.runners.base import BaseRunner


def _get_markitdown_base_url() -> str:
    """Return the markitdown service base URL from env or default."""
    return os.environ.get("BRIX_MARKITDOWN_URL", "http://markitdown-mcp:8081")


class MarkitdownRunner(BaseRunner):
    """Converts a file (or base64-encoded content) to Markdown using the
    markitdown-mcp HTTP service.

    Config parameters:
        input      — File path on disk OR base64-encoded file content (string).
                     If omitted the runner tries to use the previous step's output.
        filename   — Original filename (used for MIME detection, optional).
        auto_extract — When True, POST to /v1/extract instead of /v1/convert.
        language   — Language hint for OCR / extraction (default "de").
        template   — Optional extraction template string.

    Returns:
        {"markdown": "...", "metadata": {...}, "extracted": {...}}
    """

    # ------------------------------------------------------------------
    # BaseRunner interface
    # ------------------------------------------------------------------

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input": {
                    "type": "string",
                    "description": (
                        "File path on disk or base64-encoded content. "
                        "Omit to use previous step output."
                    ),
                },
                "filename": {
                    "type": "string",
                    "description": "Original filename for MIME-type detection.",
                },
                "auto_extract": {
                    "type": "boolean",
                    "description": "Use /v1/extract endpoint instead of /v1/convert.",
                },
                "language": {
                    "type": "string",
                    "description": "Language hint for OCR/extraction (default: 'de').",
                },
                "template": {
                    "type": "string",
                    "description": "Extraction template string (optional).",
                },
            },
            "required": [],
        }

    def input_type(self) -> str:
        return "dict"

    def output_type(self) -> str:
        return "dict"

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        self.report_progress(0.0, "starting markitdown conversion")

        cfg = getattr(step, "params", {}) or {}

        # Resolve config values (step attributes take precedence over params dict)
        def _get(key: str, default: Any = None) -> Any:
            val = getattr(step, key, None)
            if val is None:
                val = cfg.get(key, default)
            return val

        raw_input: str | None = _get("input")
        filename: str | None = _get("filename")
        auto_extract: bool = bool(_get("auto_extract", False))
        language: str = _get("language", "de") or "de"
        template: str | None = _get("template")

        # Resolve input: file path → base64, context dict → use directly
        base64_content: str | None = None

        if raw_input is None:
            # Try to get from previous step output
            last = getattr(context, "last_output", None) if context is not None else None
            if isinstance(last, dict):
                base64_content = last.get("base64") or last.get("content")
                if filename is None:
                    filename = last.get("filename")
            elif isinstance(last, str):
                base64_content = last
        elif _looks_like_file_path(raw_input):
            # Read file and base64-encode it
            try:
                file_bytes = Path(raw_input).read_bytes()
                base64_content = base64.b64encode(file_bytes).decode("ascii")
                if filename is None:
                    filename = Path(raw_input).name
            except OSError as exc:
                return {
                    "success": False,
                    "error": f"Cannot read file '{raw_input}': {exc}",
                    "duration": time.monotonic() - start,
                }
        else:
            # Treat as raw base64 string passed directly
            base64_content = raw_input

        if not base64_content:
            return {
                "success": False,
                "error": "No input provided: set 'input' config or pipe output from previous step.",
                "duration": time.monotonic() - start,
            }

        # Build request payload
        payload: dict = {"content": base64_content, "language": language}
        if filename:
            payload["filename"] = filename
        if template:
            payload["template"] = template

        # Choose endpoint
        base_url = _get_markitdown_base_url().rstrip("/")
        endpoint = "/v1/extract" if auto_extract else "/v1/convert"
        url = f"{base_url}{endpoint}"

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )

                if response.status_code >= 400:
                    return {
                        "success": False,
                        "error": f"Markitdown service error HTTP {response.status_code}: {response.text[:300]}",
                        "status_code": response.status_code,
                        "duration": time.monotonic() - start,
                    }

                try:
                    data = response.json()
                except Exception:
                    data = {"markdown": response.text, "metadata": {}, "extracted": {}}

        except httpx.ConnectError as exc:
            return {
                "success": False,
                "error": f"Markitdown service unreachable at {base_url}: {exc}",
                "duration": time.monotonic() - start,
            }
        except httpx.TimeoutException:
            return {
                "success": False,
                "error": f"Markitdown service timed out after 120s",
                "duration": time.monotonic() - start,
            }
        except httpx.RequestError as exc:
            return {
                "success": False,
                "error": f"Request error: {exc}",
                "duration": time.monotonic() - start,
            }

        duration = time.monotonic() - start
        self.report_progress(100.0, "conversion complete")

        # Normalise response to always have expected keys
        if isinstance(data, dict):
            result = {
                "markdown": data.get("markdown", ""),
                "metadata": data.get("metadata", {}),
                "extracted": data.get("extracted", {}),
            }
        else:
            result = {"markdown": str(data), "metadata": {}, "extracted": {}}

        return {"success": True, "data": result, "duration": duration}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _looks_like_file_path(value: str) -> bool:
    """Return True if *value* looks like a filesystem path rather than base64."""
    if not value:
        return False
    # Paths start with / or ./ or ../ or a Windows drive letter
    if value.startswith(("/", "./", "../")):
        return True
    # Has path separators and no base64-only chars that would be invalid in paths
    if os.sep in value:
        return True
    return False
