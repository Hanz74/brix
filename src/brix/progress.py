"""Structured progress streaming for pipeline execution."""
import json
import sys
import time
from typing import Any, Optional, TextIO


class ProgressReporter:
    """Reports pipeline execution progress to stderr and/or JSON log."""

    def __init__(self, stream: TextIO = None, log_file: str = None):
        self.stream = stream or sys.stderr
        self.log_file = log_file
        self._log_entries: list[dict] = []

    def pipeline_start(self, pipeline_name: str, step_count: int):
        msg = f"[brix] Pipeline: {pipeline_name} ({step_count} steps)"
        self._write(msg)
        self._log("pipeline_start", pipeline=pipeline_name, steps=step_count)

    def step_start(self, step_id: str, step_type: str, detail: str = ""):
        detail_str = f" {detail}" if detail else ""
        msg = f"[brix] ▶ {step_id:<20} {step_type}{detail_str} ..."
        self._write(msg, end="")
        self._log("step_start", step=step_id, type=step_type)

    def step_ok(self, step_id: str, duration: float, items: int = None):
        items_str = f", {items} items" if items else ""
        msg = f" ok ({duration:.1f}s{items_str})"
        self._write(msg)
        self._log("step_ok", step=step_id, duration=duration, items=items)

    def step_error(self, step_id: str, error: str, duration: float = 0):
        msg = f" FAILED ({error})"
        self._write(msg)
        self._log("step_error", step=step_id, error=error, duration=duration)

    def step_skipped(self, step_id: str, reason: str = "condition not met"):
        msg = f"[brix] ○ {step_id:<20} skipped ({reason})"
        self._write(msg)
        self._log("step_skipped", step=step_id, reason=reason)

    def step_resumed(self, step_id: str):
        msg = f"[brix] ↩ {step_id:<20} resumed (cached)"
        self._write(msg)
        self._log("step_resumed", step=step_id)

    def foreach_progress(self, step_id: str, current: int, total: int, failed: int = 0):
        pct = int(current / total * 100) if total > 0 else 0
        bar_len = 20
        filled = int(bar_len * current / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)
        fail_str = f", {failed} failed" if failed else ""
        msg = f"[brix]   [{current}/{total}] {bar} {pct}%{fail_str}"
        self._write(msg, end="\r")
        self._log("foreach_progress", step=step_id, current=current, total=total, failed=failed)

    def foreach_done(self, step_id: str, total: int, succeeded: int, failed: int, duration: float):
        msg = f"[brix]   {succeeded}/{total} items ({duration:.1f}s, {failed} failed)"
        self._write(msg)
        self._log("foreach_done", step=step_id, total=total, succeeded=succeeded, failed=failed, duration=duration)

    def pipeline_done(self, pipeline_name: str, success: bool, duration: float, step_count: int):
        status = "Done" if success else "FAILED"
        line = "─" * 50
        msg = f"[brix] {line}\n[brix] {status}. {step_count} steps. Duration: {duration:.1f}s"
        self._write(msg)
        self._log("pipeline_done", pipeline=pipeline_name, success=success, duration=duration)

    def retry(self, step_id: str, attempt: int, max_attempts: int, error: str):
        msg = f"[brix]   retry {attempt}/{max_attempts} ({error})"
        self._write(msg)
        self._log("retry", step=step_id, attempt=attempt, max_attempts=max_attempts)

    def _write(self, msg: str, end: str = "\n"):
        print(msg, file=self.stream, end=end, flush=True)

    def _log(self, event: str, **kwargs):
        entry = {"event": event, "timestamp": time.time(), **kwargs}
        self._log_entries.append(entry)
        if self.log_file:
            try:
                with open(self.log_file, "a") as f:
                    f.write(json.dumps(entry) + "\n")
            except Exception:
                pass

    @property
    def log_entries(self) -> list[dict]:
        return self._log_entries
