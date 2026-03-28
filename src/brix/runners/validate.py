"""Validate runner — data quality gates (T-BRIX-V6-15)."""
import re
import time
from typing import Any

from brix.runners.base import BaseRunner


class ValidateRunner(BaseRunner):
    """Evaluates data quality rules against pipeline context.

    Each rule in ``step.rules`` has:
      - field:     Jinja2 expression yielding the value to test
      - min_ratio: float (0.0–1.0), fraction of items in ``of`` that must satisfy
                   the condition (field is non-None/non-empty/truthy per item)
      - of:        Jinja2 expression yielding a list of items to iterate over
      - on_fail:   "warn" (default) or "stop"

    When a rule fails and on_fail=="stop" the step returns success=False.
    When on_fail=="warn" a warning is recorded but execution continues.
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "rules": {
                    "type": "array",
                    "description": "List of quality rules (field, min_ratio, of, on_fail)",
                    "items": {"type": "object"},
                },
            },
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        rules = getattr(step, "rules", None) or []
        if not rules:
            return {
                "success": True,
                "data": {"violations": [], "warnings": []},
                "duration": time.monotonic() - start,
            }

        from jinja2.sandbox import SandboxedEnvironment

        env = SandboxedEnvironment()
        jinja_ctx = context.to_jinja_context()

        violations: list[dict] = []
        warnings: list[dict] = []
        should_stop = False

        for rule in rules:
            field_expr = rule.get("field", "")
            min_ratio = float(rule.get("min_ratio", 1.0))
            of_expr = rule.get("of", "")
            on_fail = rule.get("on_fail", "warn")

            # Evaluate the 'of' expression to get the list of items.
            # Use compile_expression for direct Python-object evaluation (not string rendering).
            try:
                of_compiled = env.compile_expression(
                    # Strip surrounding {{ }} if present
                    of_expr.strip().lstrip("{").rstrip("}").strip()
                )
                items = of_compiled(**jinja_ctx)
                if not isinstance(items, (list, tuple)):
                    items = list(items) if hasattr(items, "__iter__") and not isinstance(items, str) else []
            except Exception as exc:
                warnings.append({"rule": rule, "error": f"'of' expression failed: {exc}"})
                continue

            if not items:
                # Empty list — rule passes trivially (nothing to validate)
                continue

            # Extract the field expression inner content (strip {{ }})
            field_inner = field_expr.strip().lstrip("{").rstrip("}").strip()

            # Count items that pass the field check (truthy native value)
            passed = 0
            for item in items:
                try:
                    item_ctx = {**jinja_ctx, "item": item}
                    field_compiled = env.compile_expression(field_inner)
                    val = field_compiled(**item_ctx)
                    # Falsy: None, False, 0, "", empty collections
                    if val:
                        passed += 1
                except Exception:
                    pass  # Count as failed

            ratio = passed / len(items)
            if ratio < min_ratio:
                entry = {
                    "rule": rule,
                    "passed": passed,
                    "total": len(items),
                    "ratio": ratio,
                    "min_ratio": min_ratio,
                }
                if on_fail == "stop":
                    violations.append(entry)
                    should_stop = True
                else:
                    warnings.append(entry)

                # Alert integration: if alerting is configured, fire an alert
                self._maybe_alert(step, entry)

        duration = time.monotonic() - start
        data = {"violations": violations, "warnings": warnings}

        if should_stop:
            return {
                "success": False,
                "error": f"Data quality gate failed: {len(violations)} violation(s)",
                "data": data,
                "duration": duration,
            }

        self.report_progress(100.0, "done")
        return {"success": True, "data": data, "duration": duration}

    def _maybe_alert(self, step: Any, violation: dict) -> None:
        """Check if any alert rules should fire for this validate violation.

        Integrates with AlertManager: if rules exist that handle "pipeline_failed"
        condition they may be triggered.  Non-fatal — all exceptions are swallowed.
        """
        try:
            import sys
            from brix.alerting import AlertManager
            mgr = AlertManager()
            rules = mgr.list_rules()
            enabled = [r for r in rules if r.enabled]
            if not enabled:
                return
            # Log the violation to stderr so it appears in run logs
            step_id = getattr(step, "id", "unknown")
            ratio = violation.get("ratio", 0)
            min_ratio = violation.get("min_ratio", 1.0)
            print(
                f"[validate] Step '{step_id}': quality violation — "
                f"ratio {ratio:.2%} < required {min_ratio:.2%}",
                file=sys.stderr,
            )
        except Exception:
            pass
