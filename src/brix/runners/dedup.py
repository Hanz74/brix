"""Dedup runner — removes duplicate items from a list using a Jinja2 key expression."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class DedupRunner(BaseRunner):
    """Deduplicates a list by evaluating a Jinja2 key expression per item.

    Pipeline YAML example:
        - id: unique_emails
          type: dedup
          params:
            input: "{{ fetch.output }}"
            key: "{{ item.email }}"
            keep: first
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input": {"description": "List to deduplicate (Jinja2 expression or literal list)"},
                "key": {"type": "string", "description": "Jinja2 expression evaluated per item to produce the dedup key"},
                "keep": {
                    "type": "string",
                    "enum": ["first", "last"],
                    "description": "Which duplicate to keep: 'first' (default) or 'last'",
                },
            },
            "required": ["key"],
        }

    def input_type(self) -> str:
        return "list[dict]"

    def output_type(self) -> str:
        return "list[dict]"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        input_data = params.get("input") if "input" in params else params.get("_input")
        key_expr = params.get("key")
        keep = params.get("keep", "first")

        if input_data is None:
            return {"success": False, "error": "Dedup brick needs 'input' (a list)", "duration": 0.0}
        if not key_expr:
            return {"success": False, "error": "Dedup brick needs 'key' (Jinja2 expression)", "duration": 0.0}
        if keep not in ("first", "last"):
            return {"success": False, "error": f"Dedup 'keep' must be 'first' or 'last', got: {keep!r}", "duration": 0.0}

        if not isinstance(input_data, list):
            return {"success": False, "error": f"Dedup input must be a list, got {type(input_data).__name__}", "duration": 0.0}

        from jinja2.sandbox import SandboxedEnvironment
        env = SandboxedEnvironment()

        seen: dict = {}  # key → index in result list

        if keep == "first":
            result = []
            for item in input_data:
                try:
                    tmpl = env.from_string(key_expr)
                    key_val = tmpl.render(item=item)
                except Exception:
                    key_val = repr(item)
                if key_val not in seen:
                    seen[key_val] = True
                    result.append(item)
        else:
            # keep=last: iterate all, track last occurrence
            keyed: dict = {}  # key → item (latest wins)
            order: list = []  # insertion order for keys
            for item in input_data:
                try:
                    tmpl = env.from_string(key_expr)
                    key_val = tmpl.render(item=item)
                except Exception:
                    key_val = repr(item)
                if key_val not in keyed:
                    order.append(key_val)
                keyed[key_val] = item
            result = [keyed[k] for k in order]

        duration = time.monotonic() - start
        self.report_progress(100.0, "done", done=len(result), total=len(input_data))
        return {
            "success": True,
            "data": result,
            "duration": duration,
            "items_count": len(result),
            "original_count": len(input_data),
        }
