"""Switch runner — multi-way conditional branching (T-BRIX-DB-17)."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class SwitchRunner(BaseRunner):
    """Evaluates a field expression and routes to the matching case's target step.

    Pipeline YAML example::

        - id: route
          type: switch
          field: "{{ item.status }}"
          cases:
            approved: step_approve
            rejected: step_reject
            pending: step_hold
          default: step_fallback

    Returns a dict with ``matched_case`` and ``target_step`` so the engine
    can use this information for branching decisions.
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "field": {
                    "type": "string",
                    "description": "Jinja2 expression to evaluate for matching",
                },
                "cases": {
                    "type": "object",
                    "description": "Mapping of case value → target_step_id",
                },
                "default": {
                    "type": "string",
                    "description": "Fallback step_id when no case matches",
                },
            },
            "required": ["field", "cases"],
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        field_expr = getattr(step, "field", None)
        cases = getattr(step, "cases", None) or {}
        default = getattr(step, "default", None)

        if not field_expr:
            self.report_progress(100.0, "error")
            return {
                "success": False,
                "error": "SwitchRunner: 'field' config is required",
                "duration": time.monotonic() - start,
            }

        # Evaluate the field expression via Jinja2
        from brix.loader import PipelineLoader
        loader = PipelineLoader()
        jinja_ctx = context.to_jinja_context() if (context and hasattr(context, "to_jinja_context")) else {}

        try:
            evaluated = loader.render_template(field_expr, jinja_ctx)
        except Exception as e:
            self.report_progress(100.0, "error")
            return {
                "success": False,
                "error": f"SwitchRunner: field evaluation error: {e}",
                "duration": time.monotonic() - start,
            }

        # Match against cases (string comparison)
        target = cases.get(evaluated)
        if target is not None:
            matched_case = evaluated
        elif default is not None:
            matched_case = None
            target = default
        else:
            self.report_progress(100.0, "no-match")
            return {
                "success": False,
                "error": f"SwitchRunner: no case matched '{evaluated}' and no default set",
                "duration": time.monotonic() - start,
            }

        self.report_progress(100.0, f"matched={matched_case or 'default'}")
        return {
            "success": True,
            "data": {
                "matched_case": matched_case,
                "target_step": target,
                "evaluated_value": evaluated,
            },
            "duration": time.monotonic() - start,
        }
