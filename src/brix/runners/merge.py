"""Merge runner — combine outputs from multiple steps (T-BRIX-DB-17)."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class MergeRunner(BaseRunner):
    """Merges outputs from multiple preceding steps into a single list.

    Modes:

    - **append** (default): Concatenate all step outputs into one flat list.
    - **zip**: Pair items positionally — output[i] = merge of inputs[*][i].
    - **lookup**: Left-join the first input against the remaining inputs on
      a shared key.

    Pipeline YAML example::

        - id: combined
          type: merge
          inputs: [step_a, step_b]
          mode: append

        - id: enriched
          type: merge
          inputs: [step_users, step_orders]
          mode: lookup
          key: user_id
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "inputs": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of step IDs whose outputs to merge",
                },
                "mode": {
                    "type": "string",
                    "enum": ["append", "zip", "lookup"],
                    "description": "How to merge: append | zip | lookup",
                },
                "key": {
                    "type": "string",
                    "description": "Join key for lookup mode (required when mode=lookup)",
                },
            },
            "required": ["inputs"],
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "list[dict]"

    def _collect_outputs(self, step_ids: list[str], context: Any) -> list[list]:
        """Retrieve output data for each step_id from context."""
        results = []
        for sid in step_ids:
            raw = None
            if context is not None:
                if hasattr(context, "get_output"):
                    raw = context.get_output(sid)
                elif hasattr(context, "outputs") and isinstance(context.outputs, dict):
                    raw = context.outputs.get(sid)
            if raw is None:
                raw = []
            elif not isinstance(raw, list):
                raw = [raw]
            results.append(raw)
        return results

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        step_ids = getattr(step, "inputs", None) or []
        mode = getattr(step, "mode", "append") or "append"
        key = getattr(step, "key", None)

        if not step_ids:
            self.report_progress(100.0, "done")
            return {
                "success": True,
                "data": [],
                "duration": time.monotonic() - start,
            }

        collections = self._collect_outputs(step_ids, context)
        self.report_progress(50.0, f"merging {len(collections)} inputs mode={mode}")

        try:
            if mode == "append":
                merged = _merge_append(collections)
            elif mode == "zip":
                merged = _merge_zip(collections)
            elif mode == "lookup":
                if not key:
                    self.report_progress(100.0, "error")
                    return {
                        "success": False,
                        "error": "MergeRunner: 'key' is required for lookup mode",
                        "duration": time.monotonic() - start,
                    }
                merged = _merge_lookup(collections, key)
            else:
                self.report_progress(100.0, "error")
                return {
                    "success": False,
                    "error": f"MergeRunner: unknown mode '{mode}', must be append|zip|lookup",
                    "duration": time.monotonic() - start,
                }
        except Exception as e:
            self.report_progress(100.0, "error")
            return {
                "success": False,
                "error": f"MergeRunner: merge failed: {e}",
                "duration": time.monotonic() - start,
            }

        self.report_progress(100.0, f"done — {len(merged)} items")
        return {
            "success": True,
            "data": merged,
            "duration": time.monotonic() - start,
        }


# ---------------------------------------------------------------------------
# Merge strategy helpers
# ---------------------------------------------------------------------------


def _merge_append(collections: list[list]) -> list:
    """Concatenate all lists in order."""
    result = []
    for col in collections:
        result.extend(col)
    return result


def _merge_zip(collections: list[list]) -> list:
    """Combine lists item-for-item; shorter lists are padded with empty dicts."""
    if not collections:
        return []
    max_len = max(len(c) for c in collections)
    result = []
    for i in range(max_len):
        merged_item: dict = {}
        for col in collections:
            if i < len(col):
                item = col[i]
                if isinstance(item, dict):
                    merged_item.update(item)
                else:
                    # Non-dict items: wrap under a positional key
                    merged_item[f"_item_{len(merged_item)}"] = item
        result.append(merged_item)
    return result


def _merge_lookup(collections: list[list], key: str) -> list:
    """Left-join: take first collection as base, enrich with data from others.

    For each item in collections[0], look up matching items from subsequent
    collections where item[key] == base[key].  Matching fields are merged in.
    """
    if not collections:
        return []
    base = collections[0]
    lookups = collections[1:]

    # Build lookup tables for each additional collection
    lookup_tables: list[dict] = []
    for col in lookups:
        tbl: dict = {}
        for item in col:
            if isinstance(item, dict) and key in item:
                tbl[item[key]] = item
        lookup_tables.append(tbl)

    result = []
    for base_item in base:
        merged = dict(base_item) if isinstance(base_item, dict) else {"_value": base_item}
        base_key_val = merged.get(key)
        for tbl in lookup_tables:
            extra = tbl.get(base_key_val, {})
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in merged:
                        merged[k] = v
        result.append(merged)
    return result
