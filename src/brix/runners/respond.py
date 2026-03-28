"""Respond runner — constructs a webhook/API response payload."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class RespondRunner(BaseRunner):
    """Builds a structured response payload for webhook endpoints.

    Pipeline YAML example:
        - id: send_response
          type: respond
          params:
            status: 200
            headers:
              Content-Type: application/json
            body: '{"ok": true, "count": {{ process.items_count }}}'

    Output:
        {
            "status": 200,
            "headers": {"Content-Type": "application/json"},
            "body": '{"ok": true, "count": 42}',
            "responded": true,
        }
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "HTTP status code (default: 200)",
                },
                "headers": {
                    "type": "object",
                    "description": "Response headers as a dict",
                },
                "body": {
                    "type": "string",
                    "description": "Response body — may be a Jinja2 template rendered with the pipeline context",
                },
            },
            "required": [],
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        status = params.get("status", 200)
        headers = params.get("headers") or {}
        body_template = params.get("body", "")

        try:
            status = int(status)
        except (TypeError, ValueError):
            return {"success": False, "error": f"Respond 'status' must be an integer, got: {status!r}", "duration": 0.0}

        if not isinstance(headers, dict):
            return {"success": False, "error": f"Respond 'headers' must be a dict, got {type(headers).__name__}", "duration": 0.0}

        # Render body through Jinja2 with available context
        body = body_template
        if body_template:
            try:
                from jinja2.sandbox import SandboxedEnvironment
                env = SandboxedEnvironment()
                tmpl = env.from_string(body_template)

                # Build template variables from context if available
                ctx_vars: dict = {}
                if context is not None:
                    # If context is a dict-like, expose its keys directly
                    if isinstance(context, dict):
                        ctx_vars = context
                    elif hasattr(context, "__dict__"):
                        ctx_vars = vars(context)

                body = tmpl.render(**ctx_vars)
            except Exception as e:
                return {"success": False, "error": f"Respond body template error: {e}", "duration": time.monotonic() - start}

        result = {
            "status": status,
            "headers": headers,
            "body": body,
            "responded": True,
        }

        duration = time.monotonic() - start
        self.report_progress(100.0, "done")
        return {
            "success": True,
            "data": result,
            "duration": duration,
        }
