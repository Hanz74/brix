"""Transform runner — declarative data transformation via Jinja2."""
import asyncio
import json
import time
from typing import Any

from brix.config import config
from brix.loader import register_brix_jinja_globals
from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout


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

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input": {"description": "Input data to transform"},
                "expression": {"type": "string", "description": "Jinja2 expression applied to each item"},
            },
            "required": ["expression"],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        expr = config.get("expression")
        if expr is not None and not isinstance(expr, str):
            errors.append("'expression' must be a string (Jinja2 template)")
        return errors

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "any"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        # Resolve timeout
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else config.BRIX_DEFAULT_TIMEOUT

        try:
            return await asyncio.wait_for(
                self._execute_inner(step, context, start),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            return {
                "success": False,
                "error": f"Timeout after {timeout_seconds}s",
                "duration": time.monotonic() - start,
            }

    async def _execute_inner(self, step: Any, context: Any, start: float) -> dict:
        params = getattr(step, 'params', {}) or {}
        input_data = params.get('input') or params.get('_input')
        expression = params.get('expression')

        if not expression:
            self.report_progress(0.0, "error: missing expression")
            return {"success": False, "error": "Transform brick needs 'expression'", "duration": time.monotonic() - start}

        # input is optional — default to empty dict so expressions like {{ now() }} work
        if input_data is None:
            input_data = {}

        n_items = len(input_data) if isinstance(input_data, (list, dict)) else 1
        self.report_progress(0.0, f"Transforming {n_items} items")

        from jinja2.sandbox import SandboxedEnvironment
        env = register_brix_jinja_globals(SandboxedEnvironment())

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
        _n_out = len(data) if isinstance(data, list) else 1
        self.report_progress(100.0, f"Processed {_n_out} items", done=_n_out, total=n_items)
        return {"success": True, "data": data, "duration": duration}
