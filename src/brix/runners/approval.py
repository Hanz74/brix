"""Approval runner — pauses a pipeline until a human approves or rejects (T-BRIX-V4-12)."""
import asyncio
import json
import time
from pathlib import Path
from typing import Any

from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout


class ApprovalRunner(BaseRunner):
    """Pauses pipeline execution until a human approves, rejects, or the timeout expires.

    The runner writes an ``approval_pending.json`` file in the run's workdir and
    polls it every 5 seconds. The REST endpoint ``POST /approve/{run_id}`` (or any
    external process) can update the file's ``status`` field to ``"approved"`` or
    ``"rejected"`` to unblock the pipeline.

    Pipeline YAML example::

        - id: wait_approval
          type: approval
          message: "Please review the generated report before proceeding."
          approval_timeout: "1h"
          on_timeout: stop      # or "continue"
          channel: whatsapp     # optional — notify via NotifyRunner
          to: "491701234567@c.us"
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "Message shown to the approver"},
                "approval_timeout": {"type": "string", "description": "Timeout e.g. '1h', '24h'"},
                "on_timeout": {"type": "string", "enum": ["stop", "continue"], "description": "Action when timeout expires"},
                "channel": {"type": "string", "description": "Optional notification channel"},
                "to": {"type": "string", "description": "Optional notification recipient"},
            },
            "required": ["message"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        message = (getattr(step, "message", None) or "Approval required")
        timeout_str = (getattr(step, "approval_timeout", None) or "24h")
        on_timeout = (getattr(step, "on_timeout", None) or "stop")
        timeout_seconds = parse_timeout(timeout_str)

        # ------------------------------------------------------------------ #
        # Write approval request file into the run's workdir
        # ------------------------------------------------------------------ #
        approval_file: Path | None = None
        workdir = getattr(context, "workdir", None)
        if workdir is not None:
            approval_file = Path(workdir) / "approval_pending.json"
            approval_file.write_text(json.dumps({
                "step_id": step.id,
                "message": message,
                "requested_at": time.time(),
                "status": "pending",
            }))

        # ------------------------------------------------------------------ #
        # Persist "waiting_approval" progress into run metadata
        # ------------------------------------------------------------------ #
        save_meta = getattr(context, "save_run_metadata", None)
        pipeline_name = getattr(context, "pipeline_name", "_approval")
        if callable(save_meta):
            save_meta(pipeline_name, "running", progress={
                "step": step.id,
                "message": message,
                "awaiting": "human_approval",
            })

        # ------------------------------------------------------------------ #
        # Optional notification (if channel + to are configured)
        # ------------------------------------------------------------------ #
        channel = getattr(step, "channel", None)
        to_addr = getattr(step, "to", None)
        if channel and to_addr:
            from brix.runners.notify import NotifyRunner

            class _FakeNotifyStep:
                def __init__(self):
                    self.id = f"{step.id}_notify"
                    self.type = "notify"
                    self.channel = channel
                    self.to = to_addr
                    self.message = f"Approval needed: {message}"
                    self.params = {"message": self.message}

            notify = NotifyRunner()
            try:
                await notify.execute(_FakeNotifyStep(), context)
            except Exception:
                pass  # Notification failure must not abort the approval wait

        # ------------------------------------------------------------------ #
        # Poll for approval decision
        # ------------------------------------------------------------------ #
        # Poll interval: 5 seconds normally, but never more than half the timeout
        # so short timeouts (e.g. in tests) still behave correctly.
        poll_interval = min(5.0, max(0.1, timeout_seconds / 2))

        poll_start = time.monotonic()
        while time.monotonic() - poll_start < timeout_seconds:
            if approval_file is not None and approval_file.exists():
                try:
                    data = json.loads(approval_file.read_text())
                except (OSError, json.JSONDecodeError):
                    data = {}

                status = data.get("status")
                if status == "approved":
                    self.report_progress(100.0, "approved")
                    return {
                        "success": True,
                        "data": {
                            "approved": True,
                            "approved_by": data.get("approved_by", "unknown"),
                        },
                        "duration": time.monotonic() - start,
                    }
                if status == "rejected":
                    return {
                        "success": False,
                        "error": "Approval rejected",
                        "data": {
                            "approved": False,
                            "reason": data.get("reason", ""),
                        },
                        "duration": time.monotonic() - start,
                    }

            await asyncio.sleep(poll_interval)

        # ------------------------------------------------------------------ #
        # Timeout reached
        # ------------------------------------------------------------------ #
        elapsed = time.monotonic() - start
        if on_timeout == "continue":
            return {
                "success": True,
                "data": {
                    "approved": False,
                    "reason": "timeout",
                    "auto_continued": True,
                },
                "duration": elapsed,
            }
        return {
            "success": False,
            "error": f"Approval timed out after {timeout_str}",
            "duration": elapsed,
        }
