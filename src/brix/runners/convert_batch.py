"""Convert Batch runner — batch-convert files via the Daigestr HTTP API."""
import asyncio
import base64
import time
from pathlib import Path
from typing import Any

import httpx

from brix.config import BrixConfig, config
from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout


def _get_markitdown_base_url() -> str:
    """Return the Daigestr conversion service base URL from env or default."""
    return BrixConfig.reload().DAIGESTR_URL


def _get_markitdown_convert_endpoint() -> str:
    """Return the default Daigestr conversion endpoint."""
    return BrixConfig.reload().DAIGESTR_CONVERT_ENDPOINT


class ConvertBatchRunner(BaseRunner):
    """Batch-convert files in a directory via the Daigestr HTTP service.

    Pipeline YAML example::

        - id: convert_docs
          type: convert.batch
          params:
            input_dir: "/host/root/documents/input"
            output_dir: "/host/root/documents/output"
            format: "markdown"
            pattern: "*.pdf"

    Returns::

        {
          "success": true,
          "data": {
            "converted": 5,
            "errors": 1,
            "details": [
              {"file": "doc.pdf", "status": "ok", "output_path": "/host/root/.../doc.pdf.md"},
              {"file": "bad.pdf", "status": "error", "error": "..."}
            ]
          },
          "duration": 12.3
        }
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input_dir": {
                    "type": "string",
                    "description": "Directory containing files to convert",
                },
                "output_dir": {
                    "type": "string",
                    "description": "Directory to write converted files to",
                },
                "format": {
                    "type": "string",
                    "description": "Output format (default: 'markdown')",
                    "default": "markdown",
                },
                "pattern": {
                    "type": "string",
                    "description": "Glob pattern to match input files (default: '*')",
                    "default": "*",
                },
            },
            "required": ["input_dir", "output_dir"],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        fmt = config.get("format")
        if fmt is not None and not isinstance(fmt, str):
            errors.append("'format' must be a string")
        pattern = config.get("pattern")
        if pattern is not None and not isinstance(pattern, str):
            errors.append("'pattern' must be a string")
        return errors

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else config.BRIX_DEFAULT_TIMEOUT

        try:
            return await asyncio.wait_for(
                self._execute_inner(step, context, start),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            return {
                "success": False,
                "error": f"Timeout after {timeout_seconds}s",
                "duration": time.monotonic() - start,
            }

    async def _execute_inner(self, step: Any, context: Any, start: float) -> dict:
        params = getattr(step, "params", {}) or {}
        input_dir = params.get("input_dir")
        output_dir = params.get("output_dir")
        fmt = params.get("format", "markdown")
        pattern = params.get("pattern", "*")

        if not input_dir:
            self.report_progress(0.0, "error: missing input_dir")
            return {"success": False, "error": "convert.batch requires 'input_dir'", "duration": time.monotonic() - start}
        if not output_dir:
            self.report_progress(0.0, "error: missing output_dir")
            return {"success": False, "error": "convert.batch requires 'output_dir'", "duration": time.monotonic() - start}

        input_path = Path(input_dir)
        output_path = Path(output_dir)

        if not input_path.exists() or not input_path.is_dir():
            self.report_progress(0.0, "error: input_dir not found")
            return {"success": False, "error": f"Input directory not found: {input_dir}", "duration": time.monotonic() - start}

        # Create output directory if needed
        output_path.mkdir(parents=True, exist_ok=True)

        # Collect files matching pattern
        files = sorted(f for f in input_path.glob(pattern) if f.is_file())
        if not files:
            self.report_progress(100.0, "no files to convert")
            return {
                "success": True,
                "data": {"converted": 0, "errors": 0, "details": []},
                "duration": time.monotonic() - start,
            }

        self.report_progress(0.0, f"Converting {len(files)} files")

        base_url = _get_markitdown_base_url().rstrip("/")
        endpoint = _get_markitdown_convert_endpoint()
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        url = f"{base_url}{endpoint}"
        timeout_http = config.TIMEOUT_MARKITDOWN

        details: list[dict] = []
        converted = 0
        error_count = 0

        async with httpx.AsyncClient(timeout=timeout_http) as client:
            for idx, file in enumerate(files):
                try:
                    file_bytes = file.read_bytes()
                    b64_content = base64.b64encode(file_bytes).decode("ascii")

                    payload = {
                        "content": b64_content,
                        "filename": file.name,
                    }

                    response = await client.post(
                        url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                    )

                    if response.status_code >= 400:
                        error_count += 1
                        details.append({
                            "file": file.name,
                            "status": "error",
                            "error": f"HTTP {response.status_code}: {response.text[:200]}",
                        })
                        continue

                    try:
                        data = response.json()
                    except Exception:
                        data = {"markdown": response.text}

                    # Write output
                    ext = ".md" if fmt == "markdown" else f".{fmt}"
                    out_file = output_path / f"{file.name}{ext}"
                    markdown_content = data.get("markdown", "") if isinstance(data, dict) else str(data)
                    out_file.write_text(markdown_content, encoding="utf-8")

                    converted += 1
                    details.append({
                        "file": file.name,
                        "status": "ok",
                        "output_path": str(out_file),
                    })

                except Exception as exc:
                    error_count += 1
                    details.append({
                        "file": file.name,
                        "status": "error",
                        "error": str(exc),
                    })

                pct = ((idx + 1) / len(files)) * 100.0
                self.report_progress(pct, f"Converted {idx + 1}/{len(files)}", done=idx + 1, total=len(files))

        duration = time.monotonic() - start
        self.report_progress(100.0, f"Done — {converted} converted, {error_count} errors")
        return {
            "success": True,
            "data": {
                "converted": converted,
                "errors": error_count,
                "details": details,
            },
            "duration": duration,
        }
