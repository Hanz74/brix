"""Notify runner — sends notifications via WhatsApp, Slack, or logs them (T-BRIX-V4-11)."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class _FakeStep:
    """Minimal step-like object for delegating to sub-runners."""

    def __init__(self, **kw: Any):
        for k, v in kw.items():
            setattr(self, k, v)


class NotifyRunner(BaseRunner):
    """Sends notifications over various channels.

    Supported channels:
    - ``whatsapp``: delegates to the WhatsApp MCP server via McpRunner.
    - ``slack``:    sends an HTTP POST to a webhook URL (``to`` field).
    - anything else / omitted: logs the message and returns success.

    Pipeline YAML example::

        - id: alert
          type: notify
          channel: whatsapp
          to: "491701234567@c.us"
          message: "Pipeline done: {{ input.name }}"
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "channel": {"type": "string", "enum": ["whatsapp", "slack", "log"], "description": "Notification channel"},
                "to": {"type": "string", "description": "Recipient address / webhook URL"},
                "message": {"type": "string", "description": "Message text (Jinja2 template)"},
            },
            "required": ["message"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        channel = (getattr(step, "channel", None) or "").lower()
        to = getattr(step, "to", None) or ""
        # message may be a rendered string already (via _RenderedStep.params)
        message = (getattr(step, "params", None) or {}).get("message") or getattr(step, "message", None) or ""

        if channel == "whatsapp":
            from brix.runners.mcp import McpRunner

            mcp = McpRunner()
            mcp_step = _FakeStep(
                server="whatsapp",
                tool="send_message",
                params={"chatId": to, "message": message},
                timeout="30s",
            )
            result = await mcp.execute(mcp_step, context)
            result.setdefault("duration", time.monotonic() - start)
            return result

        elif channel == "slack":
            from brix.runners.http import HttpRunner

            http = HttpRunner()
            http_step = _FakeStep(
                url=to,
                method="POST",
                body={"text": message},
                headers={},
                timeout="10s",
                params={},
            )
            result = await http.execute(http_step, context)
            result.setdefault("duration", time.monotonic() - start)
            return result

        else:
            # Fallback: just log / record the notification
            duration = time.monotonic() - start
            self.report_progress(100.0, "done")
            return {
                "success": True,
                "data": {
                    "channel": channel or "log",
                    "to": to,
                    "message": message,
                    "status": "logged",
                },
                "duration": duration,
            }
