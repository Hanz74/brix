"""Structured progress streaming for pipeline execution."""
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional, TextIO


class ProgressReporter:
    """Reports pipeline execution progress to stderr and/or JSON log."""

    def __init__(self, stream: TextIO = None, log_file: str = None, workdir: str = None):
        self.stream = stream or sys.stderr
        self.log_file = log_file
        self._log_entries: list[dict] = []
        self._workdir = workdir

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
        if self._workdir:
            progress_file = Path(self._workdir) / "progress.jsonl"
            try:
                with open(progress_file, "a") as f:
                    f.write(json.dumps(entry) + "\n")
            except Exception:
                pass

    @property
    def log_entries(self) -> list[dict]:
        return self._log_entries


class McpProgressReporter(ProgressReporter):
    """ProgressReporter that also sends MCP progress notifications to the client.

    When a pipeline runs synchronously via the MCP ``brix__run_pipeline`` tool
    the caller gets no feedback until the tool returns.  This subclass hooks
    into every pipeline/step event and forwards them as MCP
    ``notifications/progress`` messages so the client can display live status.

    Usage::

        reporter = McpProgressReporter(session=session, progress_token=token)
        engine = PipelineEngine()
        engine.progress = reporter
        result = await engine.run(pipeline, user_input)

    ``session`` must be an ``mcp.server.session.ServerSession`` (or compatible)
    that exposes ``send_progress_notification(progress_token, progress, total,
    message)``.  If *progress_token* is ``None`` the MCP notifications are
    silently skipped and only the normal stderr/log output is produced.
    """

    def __init__(self, session: Any, progress_token: "str | int | None", **kwargs: Any):
        super().__init__(**kwargs)
        self._session = session
        self._progress_token = progress_token
        self._step_count: int = 0
        self._current_step: int = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _send(self, progress: float, total: float, message: str) -> None:
        """Schedule a fire-and-forget MCP progress notification."""
        if self._progress_token is None or self._session is None:
            return
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(
                    self._session.send_progress_notification(
                        progress_token=self._progress_token,
                        progress=progress,
                        total=total,
                        message=message,
                    )
                )
        except Exception:
            pass  # Never let notification errors break execution

    # ------------------------------------------------------------------
    # ProgressReporter overrides
    # ------------------------------------------------------------------

    def pipeline_start(self, pipeline_name: str, step_count: int) -> None:
        self._step_count = step_count
        self._current_step = 0
        super().pipeline_start(pipeline_name, step_count)
        self._send(0, step_count, f"Starting pipeline '{pipeline_name}' ({step_count} steps)")

    def step_start(self, step_id: str, step_type: str, detail: str = "") -> None:
        super().step_start(step_id, step_type, detail)
        detail_str = f" [{detail}]" if detail else ""
        msg = f"Step {self._current_step + 1}/{self._step_count}: {step_id} ({step_type}){detail_str}"
        self._send(self._current_step, self._step_count, msg)

    def step_ok(self, step_id: str, duration: float, items: int = None) -> None:
        self._current_step += 1
        super().step_ok(step_id, duration, items)
        items_str = f", {items} items" if items else ""
        msg = f"Step {self._current_step}/{self._step_count}: {step_id} done ({duration:.1f}s{items_str})"
        self._send(self._current_step, self._step_count, msg)

    def step_error(self, step_id: str, error: str, duration: float = 0) -> None:
        self._current_step += 1
        super().step_error(step_id, error, duration)
        msg = f"Step {self._current_step}/{self._step_count}: {step_id} FAILED — {error}"
        self._send(self._current_step, self._step_count, msg)

    def step_skipped(self, step_id: str, reason: str = "condition not met") -> None:
        self._current_step += 1
        super().step_skipped(step_id, reason)
        msg = f"Step {self._current_step}/{self._step_count}: {step_id} skipped ({reason})"
        self._send(self._current_step, self._step_count, msg)

    def step_resumed(self, step_id: str) -> None:
        self._current_step += 1
        super().step_resumed(step_id)
        msg = f"Step {self._current_step}/{self._step_count}: {step_id} resumed (cached)"
        self._send(self._current_step, self._step_count, msg)

    def foreach_progress(self, step_id: str, current: int, total: int, failed: int = 0) -> None:
        super().foreach_progress(step_id, current, total, failed)
        pct = int(current / total * 100) if total > 0 else 0
        fail_str = f", {failed} failed" if failed else ""
        msg = f"Step {self._current_step + 1}/{self._step_count}: {step_id} [{current}/{total}] {pct}%{fail_str}"
        self._send(self._current_step, self._step_count, msg)

    def foreach_done(self, step_id: str, total: int, succeeded: int, failed: int, duration: float) -> None:
        self._current_step += 1
        super().foreach_done(step_id, total, succeeded, failed, duration)
        msg = f"Step {self._current_step}/{self._step_count}: {step_id} done ({succeeded}/{total} items, {duration:.1f}s)"
        self._send(self._current_step, self._step_count, msg)

    def pipeline_done(self, pipeline_name: str, success: bool, duration: float, step_count: int) -> None:
        super().pipeline_done(pipeline_name, success, duration, step_count)
        status = "done" if success else "FAILED"
        msg = f"Pipeline '{pipeline_name}' {status} in {duration:.1f}s ({step_count} steps)"
        self._send(self._step_count, self._step_count, msg)
