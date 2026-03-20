"""Filter runner — declarative list filtering via Jinja2 expressions."""
import json
import time
from typing import Any

from brix.runners.base import BaseRunner


class FilterRunner(BaseRunner):
    """Filters a list using a Jinja2 boolean expression per item.

    Pipeline YAML example:
        - id: only_pdfs
          type: filter
          params:
            input: "{{ fetch.output }}"
            where: "{{ item.name | lower | endswith('.pdf') }}"
    """

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, 'params', {}) or {}
        input_data = params.get('input') or params.get('_input')
        where_expr = params.get('where')

        if input_data is None:
            return {"success": False, "error": "Filter brick needs 'input' (a list)", "duration": 0.0}
        if not where_expr:
            return {"success": False, "error": "Filter brick needs 'where' (Jinja2 expression)", "duration": 0.0}

        # Ensure input is a list
        if isinstance(input_data, str):
            try:
                input_data = json.loads(input_data)
            except (json.JSONDecodeError, ValueError):
                return {"success": False, "error": f"Filter input is not a list: {type(input_data)}", "duration": 0.0}

        if not isinstance(input_data, list):
            return {"success": False, "error": f"Filter input must be a list, got {type(input_data).__name__}", "duration": 0.0}

        # Use Jinja2 SandboxedEnvironment for safe expression evaluation
        from jinja2.sandbox import SandboxedEnvironment
        env = SandboxedEnvironment()

        # Add custom tests for string operations not available as Jinja2 tests by default
        env.tests['endswith'] = lambda value, suffix: str(value).endswith(suffix)
        env.tests['startswith'] = lambda value, suffix: str(value).startswith(suffix)
        env.tests['contains'] = lambda value, substr: substr in str(value)

        filtered = []
        for item in input_data:
            try:
                template = env.from_string(where_expr)
                result_str = template.render(item=item)
                # Evaluate truthiness: everything except explicit falsy values passes
                if result_str.strip().lower() not in ('false', '0', '', 'none'):
                    filtered.append(item)
            except Exception:
                # On expression error, skip item
                continue

        duration = time.monotonic() - start
        return {
            "success": True,
            "data": filtered,
            "duration": duration,
            "items_count": len(filtered),
        }
