"""Diff runner — computes the difference between two lists of dicts using a key field."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class DiffRunner(BaseRunner):
    """Computes the symmetric diff between two lists keyed by a common field.

    Pipeline YAML example:
        - id: changes
          type: diff
          params:
            left: "{{ snapshot_old.output }}"
            right: "{{ snapshot_new.output }}"
            key: id

    Output:
        {
            "added":     [...],   # items in right but not in left
            "removed":   [...],   # items in left but not in right
            "changed":   [...],   # items present in both but with different values
            "unchanged": [...],   # items present in both with identical values
        }

    Each "changed" item has shape: {"key": <key_value>, "left": <old_item>, "right": <new_item>}.
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "left": {"description": "Left (old) list of dicts"},
                "right": {"description": "Right (new) list of dicts"},
                "key": {"type": "string", "description": "Field name used to match items between left and right"},
            },
            "required": ["key"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        left = params.get("left", [])
        right = params.get("right", [])
        key = params.get("key")

        if not key:
            return {"success": False, "error": "Diff brick needs 'key' (field name)", "duration": 0.0}

        if left is None:
            left = []
        if right is None:
            right = []

        if not isinstance(left, list):
            return {"success": False, "error": f"Diff 'left' must be a list, got {type(left).__name__}", "duration": 0.0}
        if not isinstance(right, list):
            return {"success": False, "error": f"Diff 'right' must be a list, got {type(right).__name__}", "duration": 0.0}

        # Build lookup dicts: key_value → item
        left_map: dict = {}
        for item in left:
            if isinstance(item, dict) and key in item:
                left_map[item[key]] = item

        right_map: dict = {}
        for item in right:
            if isinstance(item, dict) and key in item:
                right_map[item[key]] = item

        left_keys = set(left_map.keys())
        right_keys = set(right_map.keys())

        added = [right_map[k] for k in (right_keys - left_keys)]
        removed = [left_map[k] for k in (left_keys - right_keys)]

        changed = []
        unchanged = []
        for k in left_keys & right_keys:
            l_item = left_map[k]
            r_item = right_map[k]
            if l_item == r_item:
                unchanged.append(l_item)
            else:
                changed.append({"key": k, "left": l_item, "right": r_item})

        result = {
            "added": added,
            "removed": removed,
            "changed": changed,
            "unchanged": unchanged,
        }

        duration = time.monotonic() - start
        total = len(added) + len(removed) + len(changed) + len(unchanged)
        self.report_progress(100.0, "done", done=total, total=total)
        return {
            "success": True,
            "data": result,
            "duration": duration,
            "summary": {
                "added": len(added),
                "removed": len(removed),
                "changed": len(changed),
                "unchanged": len(unchanged),
            },
        }
