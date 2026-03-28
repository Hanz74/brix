"""Aggregate runner — groups a list by a key and applies aggregation operations."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class AggregateRunner(BaseRunner):
    """Groups items by a Jinja2 key expression and computes aggregate values.

    Pipeline YAML example:
        - id: totals_by_category
          type: aggregate
          params:
            input: "{{ fetch.output }}"
            group_by: "{{ item.category }}"
            operations:
              total_amount:
                op: sum
                field: amount
              count:
                op: count
              names:
                op: collect
                field: name

    Output is a dict keyed by the group value:
        {
            "groceries": {"total_amount": 42.5, "count": 3, "names": ["a","b","c"]},
            ...
        }
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input": {"description": "List to aggregate"},
                "group_by": {"type": "string", "description": "Jinja2 expression for grouping key per item"},
                "operations": {
                    "type": "object",
                    "description": "Dict mapping output_name → {op, field?}. ops: sum, count, min, max, avg, collect",
                },
            },
            "required": ["group_by", "operations"],
        }

    def input_type(self) -> str:
        return "list[dict]"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        input_data = params.get("input") if "input" in params else params.get("_input")
        group_by_expr = params.get("group_by")
        operations = params.get("operations")

        if input_data is None:
            return {"success": False, "error": "Aggregate brick needs 'input' (a list)", "duration": 0.0}
        if not group_by_expr:
            return {"success": False, "error": "Aggregate brick needs 'group_by' (Jinja2 expression)", "duration": 0.0}
        if not operations or not isinstance(operations, dict):
            return {"success": False, "error": "Aggregate brick needs 'operations' (dict)", "duration": 0.0}

        if not isinstance(input_data, list):
            return {"success": False, "error": f"Aggregate input must be a list, got {type(input_data).__name__}", "duration": 0.0}

        from jinja2.sandbox import SandboxedEnvironment
        env = SandboxedEnvironment()

        # Group items
        groups: dict[str, list] = {}
        for item in input_data:
            try:
                tmpl = env.from_string(group_by_expr)
                group_key = tmpl.render(item=item)
            except Exception:
                group_key = "__error__"
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(item)

        # Apply operations per group
        result: dict = {}
        for group_key, items in groups.items():
            group_result: dict = {}
            for out_name, op_cfg in operations.items():
                if not isinstance(op_cfg, dict):
                    continue
                op = op_cfg.get("op")
                field = op_cfg.get("field")

                if op == "count":
                    group_result[out_name] = len(items)
                elif op == "collect":
                    if field:
                        group_result[out_name] = [item.get(field) for item in items if field in item]
                    else:
                        group_result[out_name] = list(items)
                elif op in ("sum", "min", "max", "avg"):
                    if not field:
                        group_result[out_name] = None
                        continue
                    values = []
                    for item in items:
                        val = item.get(field)
                        if val is not None:
                            try:
                                values.append(float(val))
                            except (TypeError, ValueError):
                                pass
                    if not values:
                        group_result[out_name] = None
                    elif op == "sum":
                        group_result[out_name] = sum(values)
                    elif op == "min":
                        group_result[out_name] = min(values)
                    elif op == "max":
                        group_result[out_name] = max(values)
                    elif op == "avg":
                        group_result[out_name] = sum(values) / len(values)
                else:
                    group_result[out_name] = None

            result[group_key] = group_result

        duration = time.monotonic() - start
        self.report_progress(100.0, "done", done=len(groups), total=len(input_data))
        return {
            "success": True,
            "data": result,
            "duration": duration,
            "group_count": len(groups),
        }
