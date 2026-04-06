"""Dedup runner — removes duplicate items from a list.

Supports two modes:
1. Jinja2 key expression (original) — ``key: "{{ item.email }}"``
2. Content-hash based (T-BRIX-BRICK-03) — ``field: "body"``, ``algorithm: "sha256"``

If ``field`` is given, the runner hashes the field value for deduplication.
If ``key`` is given, the Jinja2 expression result is used as the dedup key.
At least one of ``key`` or ``field`` is required.
"""

import hashlib
import json
import time
from typing import Any

from brix.runners.base import BaseRunner


class DedupRunner(BaseRunner):
    """Deduplicates a list by key expression or content-hash.

    Pipeline YAML examples::

        # Mode 1: Jinja2 key expression
        - id: unique_emails
          type: dedup
          params:
            input: "{{ fetch.output }}"
            key: "{{ item.email }}"
            keep: first

        # Mode 2: Content-hash based
        - id: unique_bodies
          type: flow.dedup
          params:
            input: "{{ fetch.output }}"
            field: body
            algorithm: sha256
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input": {"description": "List to deduplicate (Jinja2 expression or literal list)"},
                "key": {
                    "type": "string",
                    "description": "Jinja2 expression evaluated per item to produce the dedup key. Either 'key' or 'field' must be set.",
                },
                "field": {
                    "type": "string",
                    "description": "Field name to hash for content-based dedup. Either 'field' or 'key' must be set.",
                },
                "algorithm": {
                    "type": "string",
                    "enum": ["md5", "sha256"],
                    "description": "Hash algorithm for content-based dedup (default: sha256)",
                },
                "keep": {
                    "type": "string",
                    "enum": ["first", "last"],
                    "description": "Which duplicate to keep: 'first' (default) or 'last'",
                },
            },
            "required": [],
            "oneOf": [{"required": ["key"]}, {"required": ["field"]}],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        key = config.get("key")
        field = config.get("field")
        if not key and not field:
            errors.append("Dedup brick needs either 'key' (Jinja2 expression) or 'field' (field name to hash)")
        algorithm = config.get("algorithm")
        if algorithm is not None and algorithm not in ("md5", "sha256"):
            errors.append("'algorithm' must be 'md5' or 'sha256'")
        keep = config.get("keep")
        if keep is not None and keep not in ("first", "last"):
            errors.append("'keep' must be 'first' or 'last'")
        return errors

    def input_type(self) -> str:
        return "list[dict]"

    def output_type(self) -> str:
        return "list[dict]"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        input_data = params.get("input") if "input" in params else params.get("_input")
        key_expr = params.get("key")
        field = params.get("field")
        algorithm = params.get("algorithm", "sha256")
        keep = params.get("keep", "first")

        if input_data is None:
            return {"success": False, "error": "Dedup brick needs 'input' (a list)", "duration": 0.0}
        if not key_expr and not field:
            return {"success": False, "error": "Dedup brick needs either 'key' or 'field'", "duration": 0.0}
        if keep not in ("first", "last"):
            return {"success": False, "error": f"Dedup 'keep' must be 'first' or 'last', got: {keep!r}", "duration": 0.0}
        if algorithm not in ("md5", "sha256"):
            return {"success": False, "error": f"Dedup 'algorithm' must be 'md5' or 'sha256', got: {algorithm!r}", "duration": 0.0}

        if not isinstance(input_data, list):
            return {"success": False, "error": f"Dedup input must be a list, got {type(input_data).__name__}", "duration": 0.0}

        original_count = len(input_data)

        # Determine the key function
        if field:
            # Content-hash mode
            def get_key(item: Any) -> str:
                value = item.get(field, "") if isinstance(item, dict) else str(item)
                raw = json.dumps(value, sort_keys=True, ensure_ascii=False) if not isinstance(value, str) else value
                if algorithm == "md5":
                    return hashlib.md5(raw.encode("utf-8")).hexdigest()
                else:
                    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
        else:
            # Jinja2 key expression mode
            from jinja2.sandbox import SandboxedEnvironment
            env = SandboxedEnvironment()

            def get_key(item: Any) -> str:
                try:
                    tmpl = env.from_string(key_expr)
                    return tmpl.render(item=item)
                except Exception:
                    return repr(item)

        if keep == "first":
            seen: set = set()
            result = []
            for item in input_data:
                key_val = get_key(item)
                if key_val not in seen:
                    seen.add(key_val)
                    result.append(item)
        else:
            # keep=last: iterate all, track last occurrence
            keyed: dict = {}
            order: list = []
            for item in input_data:
                key_val = get_key(item)
                if key_val not in keyed:
                    order.append(key_val)
                keyed[key_val] = item
            result = [keyed[k] for k in order]

        removed = original_count - len(result)
        duration = time.monotonic() - start
        self.report_progress(100.0, "done", done=len(result), total=original_count)
        return {
            "success": True,
            "data": {
                "items": result,
                "removed": removed,
                "total": original_count,
            },
            "duration": duration,
            "items_count": len(result),
            "original_count": original_count,
        }
