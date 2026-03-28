"""Flatten runner — flattens nested lists by a configurable depth."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class FlattenRunner(BaseRunner):
    """Flattens nested lists to a configurable depth.

    Pipeline YAML example:
        - id: flat_list
          type: flatten
          params:
            input: "{{ fetch.output }}"
            depth: 1

        # Or flatten a specific field from each item:
        - id: flat_tags
          type: flatten
          params:
            input: "{{ fetch.output }}"
            field: tags
            depth: 1
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input": {"description": "Input list (possibly nested)"},
                "depth": {
                    "type": "integer",
                    "description": "How many levels to flatten (default: 1). Use -1 for unlimited.",
                },
                "field": {
                    "type": "string",
                    "description": "If set, extract this field from each item and flatten the resulting list of lists.",
                },
            },
            "required": [],
        }

    def input_type(self) -> str:
        return "list"

    def output_type(self) -> str:
        return "list"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        input_data = params.get("input") if "input" in params else params.get("_input")
        depth = params.get("depth", 1)
        field = params.get("field")

        if input_data is None:
            return {"success": False, "error": "Flatten brick needs 'input'", "duration": 0.0}

        if not isinstance(input_data, list):
            return {"success": False, "error": f"Flatten input must be a list, got {type(input_data).__name__}", "duration": 0.0}

        try:
            depth = int(depth)
        except (TypeError, ValueError):
            return {"success": False, "error": f"Flatten 'depth' must be an integer, got: {depth!r}", "duration": 0.0}

        # If field is specified, extract that field from each item first
        if field:
            extracted = []
            for item in input_data:
                if isinstance(item, dict) and field in item:
                    val = item[field]
                    if isinstance(val, list):
                        extracted.extend(val)
                    else:
                        extracted.append(val)
                # Items without the field are skipped
            result = _flatten(extracted, depth)
        else:
            result = _flatten(input_data, depth)

        duration = time.monotonic() - start
        self.report_progress(100.0, "done", done=len(result), total=len(input_data))
        return {
            "success": True,
            "data": result,
            "duration": duration,
            "items_count": len(result),
        }


def _flatten(lst: list, depth: int) -> list:
    """Recursively flatten a list up to *depth* levels.

    depth=-1 means unlimited.
    """
    if depth == 0:
        return list(lst)
    result = []
    for item in lst:
        if isinstance(item, list):
            if depth == 1:
                result.extend(item)
            else:
                result.extend(_flatten(item, depth - 1 if depth > 0 else depth))
        else:
            result.append(item)
    return result
