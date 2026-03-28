"""Set runner — assigns computed values to pipeline context (T-BRIX-V4-03)."""
import time
from typing import Any

from brix.runners.base import BaseRunner


class SetRunner(BaseRunner):
    """Evaluates Jinja2-templated key/value pairs and returns them as output.

    Pipeline YAML example:
        - id: computed
          type: set
          values:
            greeting: "Hello {{ input.name }}"
            count: "{{ items | length }}"

    The engine pre-renders 'values' via render_step_params and stores the
    result in _RenderedStep.values, so this runner receives already-rendered
    values and just needs to return them.

    When ``persist: true`` is set on the step, all rendered values are also
    written to the persistent_store DB table so they survive across runs
    (T-BRIX-DB-13).
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "values": {"type": "object", "description": "Key/value pairs with Jinja2 templates"},
                "persist": {"type": "boolean", "description": "When true, write values to persistent_store"},
            },
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        # _RenderedStep exposes pre-rendered values in self.values.
        # Fall back to params for backwards compatibility.
        values = getattr(step, "values", None) or getattr(step, "params", {}) or {}
        self.report_progress(0.0, f"Setting {len(values)} values")

        # Persist to DB when persist: true is set (T-BRIX-DB-13)
        persist = getattr(step, "persist", False)
        if persist and values:
            try:
                from brix.db import BrixDB
                db = BrixDB()
                # Determine pipeline name from context if available
                pipeline_name = ""
                for attr in ("pipeline_name", "_pipeline_name"):
                    if hasattr(context, attr):
                        pipeline_name = getattr(context, attr) or ""
                        break
                for key, val in values.items():
                    db.store_set(key, str(val), pipeline_name)
            except Exception:
                pass  # Non-fatal: persist failure should not break run

        duration = time.monotonic() - start
        self.report_progress(100.0, f"Processed {len(values)} items", done=len(values), total=len(values))
        return {
            "success": True,
            "data": values,
            "duration": duration,
        }
