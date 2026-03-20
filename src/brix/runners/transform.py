"""Transform runner — declarative data transformation via Jinja2."""
import json
import time
from typing import Any

from brix.runners.base import BaseRunner


class TransformRunner(BaseRunner):
    """Transforms data using a Jinja2 expression.

    Pipeline YAML example:
        - id: extract_names
          type: transform
          params:
            input: "{{ fetch.output }}"
            expression: "{{ item.firstName }} {{ item.lastName }}"

    For list input: applies expression to each item.
    For dict input: applies expression once with 'data' variable.
    For other input: applies expression with 'value' variable.
    """

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, 'params', {}) or {}
        input_data = params.get('input') or params.get('_input')
        expression = params.get('expression')

        if input_data is None:
            return {"success": False, "error": "Transform brick needs 'input'", "duration": 0.0}
        if not expression:
            return {"success": False, "error": "Transform brick needs 'expression'", "duration": 0.0}

        from jinja2.sandbox import SandboxedEnvironment
        env = SandboxedEnvironment()

        try:
            template = env.from_string(expression)

            if isinstance(input_data, list):
                # Apply expression to each item, expose as 'item'
                results = []
                for item in input_data:
                    rendered = template.render(item=item)
                    # Try JSON parse for structured output
                    try:
                        results.append(json.loads(rendered))
                    except (json.JSONDecodeError, ValueError):
                        results.append(rendered)
                data = results
            elif isinstance(input_data, dict):
                # Single dict exposed as 'data'
                rendered = template.render(data=input_data)
                try:
                    data = json.loads(rendered)
                except (json.JSONDecodeError, ValueError):
                    data = rendered
            else:
                # Scalar or other type exposed as 'value'
                rendered = template.render(value=input_data)
                data = rendered

        except Exception as e:
            return {"success": False, "error": f"Transform error: {e}", "duration": time.monotonic() - start}

        duration = time.monotonic() - start
        return {"success": True, "data": data, "duration": duration}
