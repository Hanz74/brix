"""Util Load Dir runner — load all files of a type from a directory."""
import asyncio
import time
from pathlib import Path
from typing import Any

from brix.config import config
from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout


class UtilLoadDirRunner(BaseRunner):
    """Load all files matching a pattern from a directory.

    Pipeline YAML example::

        - id: load_docs
          type: util.load_dir
          params:
            path: "/host/root/documents/"
            pattern: "*.md"
            as_text: true

    Returns::

        {
          "success": true,
          "data": {
            "files": [
              {"name": "readme.md", "path": "/host/root/.../readme.md", "content": "# Title..."}
            ],
            "count": 1
          },
          "duration": 0.05
        }
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Directory path to load files from",
                },
                "pattern": {
                    "type": "string",
                    "description": "Glob pattern to match files (default: '*.md')",
                    "default": "*.md",
                },
                "as_text": {
                    "type": "boolean",
                    "description": "Load file content as text (default: true). If false, only returns paths.",
                    "default": True,
                },
            },
            "required": ["path"],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
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
        path = params.get("path")
        pattern = params.get("pattern", "*.md")
        as_text = params.get("as_text", True)

        if not path:
            self.report_progress(0.0, "error: missing path")
            return {"success": False, "error": "util.load_dir requires 'path'", "duration": time.monotonic() - start}

        dir_path = Path(path)
        if not dir_path.exists():
            self.report_progress(0.0, "error: directory not found")
            return {"success": False, "error": f"Directory not found: {path}", "duration": time.monotonic() - start}
        if not dir_path.is_dir():
            self.report_progress(0.0, "error: not a directory")
            return {"success": False, "error": f"Not a directory: {path}", "duration": time.monotonic() - start}

        # Collect matching files
        files_found = sorted(f for f in dir_path.glob(pattern) if f.is_file())
        self.report_progress(0.0, f"Loading {len(files_found)} files from {path}")

        result_files: list[dict] = []
        for idx, file in enumerate(files_found):
            entry: dict = {
                "name": file.name,
                "path": str(file),
            }
            if as_text:
                try:
                    entry["content"] = file.read_text(encoding="utf-8")
                except UnicodeDecodeError:
                    entry["content"] = None
                    entry["error"] = "binary file — cannot read as text"
                except Exception as exc:
                    entry["content"] = None
                    entry["error"] = str(exc)

            result_files.append(entry)

            if files_found:
                pct = ((idx + 1) / len(files_found)) * 100.0
                self.report_progress(pct, f"Loaded {idx + 1}/{len(files_found)}", done=idx + 1, total=len(files_found))

        duration = time.monotonic() - start
        self.report_progress(100.0, f"Done — loaded {len(result_files)} files")
        return {
            "success": True,
            "data": {
                "files": result_files,
                "count": len(result_files),
            },
            "duration": duration,
        }
