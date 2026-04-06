"""File I/O runners — read, write, list, and load JSON files.

Provides four brick types:
- file.read_base64  (FileReadBase64Runner)  — read file → base64
- file.write        (FileWriteRunner)       — write content to file
- file.list         (FileListRunner)        — list directory contents
- file.load_json    (FileLoadJsonRunner)    — read & parse JSON file

All runners honour the /host/root/ path convention (Brix runs in Docker).
"""

import asyncio
import base64
import glob as globmod
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from brix.config import config
from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout


class FileReadRunner(BaseRunner):
    """Read a text file and return its content with metadata."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to the file to read",
                },
                "encoding": {
                    "type": "string",
                    "description": "Text encoding to use",
                    "default": "utf-8",
                },
                "max_size": {
                    "type": "integer",
                    "description": "Maximum allowed file size in bytes",
                    "default": 10485760,
                },
            },
            "required": ["path"],
        }

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
        path = params.get("path")
        encoding = params.get("encoding", "utf-8")
        max_size = params.get("max_size", 10485760)

        if not path:
            self.report_progress(0.0, "error: missing path")
            return {"success": False, "error": "file.read requires 'path'", "duration": time.monotonic() - start}

        self.report_progress(0.0, f"Reading {path}")

        try:
            file_path = Path(path)
            if not file_path.exists():
                return {"success": False, "error": f"File not found: {path}", "duration": time.monotonic() - start}

            size = file_path.stat().st_size
            if size > max_size:
                return {
                    "success": False,
                    "error": f"File too large: {size} bytes exceeds max_size={max_size}",
                    "duration": time.monotonic() - start,
                }

            text = file_path.read_text(encoding=encoding)
            result = {
                "text": text,
                "size": size,
                "name": file_path.name,
                "encoding": encoding,
            }
            self.report_progress(100.0, f"Read {size} bytes from {file_path.name}")
            return {"success": True, "data": result, "duration": time.monotonic() - start}
        except PermissionError:
            return {"success": False, "error": f"Permission denied: {path}", "duration": time.monotonic() - start}
        except Exception as exc:
            return {"success": False, "error": str(exc), "duration": time.monotonic() - start}


class FileReadBase64Runner(BaseRunner):
    """Read a file and return its content as a base64-encoded string.

    Pipeline YAML example::

        - id: read_pdf
          type: file.read_base64
          params:
            path: "/host/root/documents/invoice.pdf"
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to the file to read",
                },
            },
            "required": ["path"],
        }

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
        path = params.get("path")

        if not path:
            self.report_progress(0.0, "error: missing path")
            return {"success": False, "error": "file.read_base64 requires 'path'", "duration": time.monotonic() - start}

        self.report_progress(0.0, f"Reading {path}")

        try:
            file_path = Path(path)
            if not file_path.exists():
                return {"success": False, "error": f"File not found: {path}", "duration": time.monotonic() - start}

            data = file_path.read_bytes()
            encoded = base64.b64encode(data).decode("ascii")
            result = {
                "base64": encoded,
                "size": len(data),
                "name": file_path.name,
            }
            self.report_progress(100.0, f"Read {len(data)} bytes from {file_path.name}")
            return {"success": True, "data": result, "duration": time.monotonic() - start}
        except PermissionError:
            return {"success": False, "error": f"Permission denied: {path}", "duration": time.monotonic() - start}
        except Exception as exc:
            return {"success": False, "error": str(exc), "duration": time.monotonic() - start}


class FileWriteRunner(BaseRunner):
    """Write content to a file.

    Pipeline YAML example::

        - id: save_result
          type: file.write
          params:
            path: "/host/root/output/result.json"
            content: "{{ prev_step.output | tojson }}"
            mode: "text"
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to the file to write",
                },
                "content": {
                    "type": "string",
                    "description": "Content to write to the file",
                },
                "mode": {
                    "type": "string",
                    "description": "Write mode: 'text' (default) or 'binary' (content is base64)",
                    "default": "text",
                    "enum": ["text", "binary"],
                },
            },
            "required": ["path", "content"],
        }

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
        path = params.get("path")
        content = params.get("content")
        mode = params.get("mode", "text")

        if not path:
            self.report_progress(0.0, "error: missing path")
            return {"success": False, "error": "file.write requires 'path'", "duration": time.monotonic() - start}
        if content is None:
            self.report_progress(0.0, "error: missing content")
            return {"success": False, "error": "file.write requires 'content'", "duration": time.monotonic() - start}

        self.report_progress(0.0, f"Writing to {path}")

        try:
            file_path = Path(path)
            # Create parent directories if they don't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)

            if mode == "binary":
                data = base64.b64decode(content)
                file_path.write_bytes(data)
                size = len(data)
            else:
                # If content is not a string (e.g. dict/list), serialize to JSON
                if not isinstance(content, str):
                    content = json.dumps(content, ensure_ascii=False, indent=2)
                file_path.write_text(content, encoding="utf-8")
                size = len(content.encode("utf-8"))

            result = {
                "path": str(file_path),
                "size": size,
                "success": True,
            }
            self.report_progress(100.0, f"Wrote {size} bytes to {file_path.name}")
            return {"success": True, "data": result, "duration": time.monotonic() - start}
        except PermissionError:
            return {"success": False, "error": f"Permission denied: {path}", "duration": time.monotonic() - start}
        except Exception as exc:
            return {"success": False, "error": str(exc), "duration": time.monotonic() - start}


class FileListRunner(BaseRunner):
    """List files in a directory with metadata.

    Pipeline YAML example::

        - id: list_pdfs
          type: file.list
          params:
            path: "/host/root/documents/"
            pattern: "*.pdf"
            recursive: true
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Directory path to list",
                },
                "pattern": {
                    "type": "string",
                    "description": "Glob pattern to filter files",
                    "default": "*",
                },
                "recursive": {
                    "type": "boolean",
                    "description": "Search subdirectories recursively",
                    "default": False,
                },
            },
            "required": ["path"],
        }

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
        path = params.get("path")
        pattern = params.get("pattern", "*")
        recursive = params.get("recursive", False)

        if not path:
            self.report_progress(0.0, "error: missing path")
            return {"success": False, "error": "file.list requires 'path'", "duration": time.monotonic() - start}

        self.report_progress(0.0, f"Listing {path} with pattern '{pattern}'")

        try:
            dir_path = Path(path)
            if not dir_path.exists():
                return {"success": False, "error": f"Directory not found: {path}", "duration": time.monotonic() - start}
            if not dir_path.is_dir():
                return {"success": False, "error": f"Not a directory: {path}", "duration": time.monotonic() - start}

            if recursive:
                glob_pattern = f"**/{pattern}"
            else:
                glob_pattern = pattern

            files = []
            for entry in dir_path.glob(glob_pattern):
                if entry.is_file():
                    stat = entry.stat()
                    files.append({
                        "name": entry.name,
                        "path": str(entry),
                        "size": stat.st_size,
                        "modified": datetime.fromtimestamp(
                            stat.st_mtime, tz=timezone.utc
                        ).isoformat(),
                    })

            # Sort by name for deterministic output
            files.sort(key=lambda f: f["name"])

            result = {
                "files": files,
                "count": len(files),
            }
            self.report_progress(100.0, f"Found {len(files)} files", done=len(files), total=len(files))
            return {"success": True, "data": result, "duration": time.monotonic() - start}
        except PermissionError:
            return {"success": False, "error": f"Permission denied: {path}", "duration": time.monotonic() - start}
        except Exception as exc:
            return {"success": False, "error": str(exc), "duration": time.monotonic() - start}


class FileLoadJsonRunner(BaseRunner):
    """Read a JSON file and return parsed content.

    Pipeline YAML example::

        - id: load_config
          type: file.load_json
          params:
            path: "/host/root/config/settings.json"
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to the JSON file to read",
                },
            },
            "required": ["path"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "any"

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
        path = params.get("path")

        if not path:
            self.report_progress(0.0, "error: missing path")
            return {"success": False, "error": "file.load_json requires 'path'", "duration": time.monotonic() - start}

        self.report_progress(0.0, f"Loading JSON from {path}")

        try:
            file_path = Path(path)
            if not file_path.exists():
                return {"success": False, "error": f"File not found: {path}", "duration": time.monotonic() - start}

            text = file_path.read_text(encoding="utf-8")
            data = json.loads(text)

            self.report_progress(100.0, f"Loaded JSON from {file_path.name}")
            return {"success": True, "data": data, "duration": time.monotonic() - start}
        except json.JSONDecodeError as exc:
            return {"success": False, "error": f"Invalid JSON in {path}: {exc}", "duration": time.monotonic() - start}
        except PermissionError:
            return {"success": False, "error": f"Permission denied: {path}", "duration": time.monotonic() - start}
        except Exception as exc:
            return {"success": False, "error": str(exc), "duration": time.monotonic() - start}
