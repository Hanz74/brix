"""Specialist runner — declarative data extraction (T-BRIX-V8-03).

Applies a list of ExtractionRules to an input value, optionally validates
the extracted fields, then returns the result in the requested format.

Supported extraction methods
-----------------------------
regex       re.search / re.findall with pattern.
            ``group`` selects a capture-group index (0 = full match).
            ``findall=True`` returns a list of all matches instead of first.
json_path   Dot-notation accessor (e.g. ``a.b.0.c``) on a nested dict/list.
split       String split using ``pattern`` as the separator.  Returns a list.
template    Jinja2 template rendered with ``{{ text }}`` and all extracted
            fields that were computed before this rule in scope.

Supported validation rules
---------------------------
required    Field must be truthy (non-None, non-empty).
min_length  len(value) >= rule.value (strings and lists).
max_length  len(value) <= rule.value.
regex       re.search(rule.value, str(value)) must match.
type        type(value).__name__ == rule.value  (e.g. "str", "int", "list").

on_fail behaviour
-----------------
warn   Add entry to ``warnings`` list; continue.
skip   Skip this *item* entirely (for foreach use; from step context: same as warn).
error  Return success=False with the validation error.

Output formats
--------------
dict    {field_name: value, ...}  (default)
list    [value, value, ...]       (preserves extraction order)
flat    {field_name: value, ...}  where list-values are flattened to strings
"""
from __future__ import annotations

import re
import time
from typing import Any

from jinja2.sandbox import SandboxedEnvironment

from brix.models import ExtractionRule, SpecialistConfig, ValidationRule
from brix.runners.base import BaseRunner

_jinja_env = SandboxedEnvironment()


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _extract_regex(text: str, rule: ExtractionRule) -> Any:
    """Apply regex extraction to *text*."""
    if not rule.pattern:
        return rule.default

    if rule.findall:
        matches = re.findall(rule.pattern, text)
        return matches if matches else rule.default

    match = re.search(rule.pattern, text)
    if not match:
        return rule.default

    # Group 0 = full match; groups 1..n = capture groups
    try:
        if rule.group == 0:
            return match.group(0)
        return match.group(rule.group)
    except IndexError:
        return rule.default


def _extract_json_path(data: Any, rule: ExtractionRule) -> Any:
    """Dot-notation accessor on a nested dict/list.

    Supports integer keys for list indexing (e.g. ``items.0.name``).
    """
    if not rule.pattern:
        return rule.default

    parts = rule.pattern.split(".")
    current = data
    for part in parts:
        if current is None:
            return rule.default
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return rule.default
        else:
            return rule.default

    return current if current is not None else rule.default


def _extract_split(text: str, rule: ExtractionRule) -> Any:
    """Split *text* by ``rule.pattern`` separator."""
    if not isinstance(text, str):
        text = str(text)
    sep = rule.pattern  # None → split on whitespace
    result = text.split(sep) if sep else text.split()
    return result if result else rule.default


def _extract_template(context_vars: dict, rule: ExtractionRule) -> Any:
    """Render a Jinja2 template with the current extraction context."""
    if not rule.template:
        return rule.default
    try:
        tmpl = _jinja_env.from_string(rule.template)
        return tmpl.render(**context_vars)
    except Exception:
        return rule.default


def _apply_extraction(text: Any, rule: ExtractionRule, ctx: dict) -> Any:
    """Dispatch extraction to the correct method."""
    method = rule.method.lower()
    if method == "regex":
        return _extract_regex(str(text) if text is not None else "", rule)
    elif method == "json_path":
        return _extract_json_path(text, rule)
    elif method == "split":
        return _extract_split(text, rule)
    elif method == "template":
        return _extract_template(ctx, rule)
    else:
        # Unknown method — return default
        return rule.default


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_field(value: Any, rule: ValidationRule) -> str | None:
    """Validate *value* against *rule*.

    Returns an error message string on failure, or None on success.
    """
    rule_type = rule.rule.lower()

    if rule_type == "required":
        if value is None or value == "" or value == [] or value == {}:
            return f"Field '{rule.field}' is required but missing or empty"

    elif rule_type == "min_length":
        threshold = int(rule.value) if rule.value is not None else 0
        try:
            length = len(value)
        except TypeError:
            return f"Field '{rule.field}' has no length (got {type(value).__name__})"
        if length < threshold:
            return f"Field '{rule.field}' length {length} < min_length {threshold}"

    elif rule_type == "max_length":
        threshold = int(rule.value) if rule.value is not None else 0
        try:
            length = len(value)
        except TypeError:
            return f"Field '{rule.field}' has no length (got {type(value).__name__})"
        if length > threshold:
            return f"Field '{rule.field}' length {length} > max_length {threshold}"

    elif rule_type == "regex":
        pattern = str(rule.value) if rule.value is not None else ""
        if not re.search(pattern, str(value) if value is not None else ""):
            return f"Field '{rule.field}' value {value!r} does not match pattern {pattern!r}"

    elif rule_type == "type":
        expected = str(rule.value) if rule.value is not None else ""
        actual = type(value).__name__
        if actual != expected:
            return f"Field '{rule.field}' expected type {expected!r}, got {actual!r}"

    return None  # Validation passed


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------

def _format_output(extracted: dict, fmt: str) -> Any:
    """Convert the extracted dict to the desired output format."""
    if fmt == "list":
        return list(extracted.values())
    elif fmt == "flat":
        result: dict = {}
        for k, v in extracted.items():
            if isinstance(v, list):
                result[k] = ", ".join(str(i) for i in v)
            else:
                result[k] = v
        return result
    else:
        # "dict" (default)
        return extracted


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class SpecialistRunner(BaseRunner):
    """Executes specialist steps: declarative extract → validate → output."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "extract": {"type": "array", "description": "List of ExtractionRule objects"},
                "validate": {"type": "array", "description": "List of ValidationRule objects"},
                "output_format": {"type": "string", "enum": ["dict", "list", "flat"], "description": "Output format"},
            },
        }

    def input_type(self) -> str:
        return "text"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        # --- Parse SpecialistConfig from step.config -------------------------
        raw_config = getattr(step, "config", None) or {}
        if not raw_config:
            self.report_progress(0.0, "error: missing config")
            return {
                "success": False,
                "error": "specialist step requires a 'config' block with 'extract' rules",
                "duration": time.monotonic() - start,
            }
        try:
            cfg = SpecialistConfig.model_validate(raw_config)
        except Exception as exc:
            return {
                "success": False,
                "error": f"Invalid specialist config: {exc}",
                "duration": time.monotonic() - start,
            }

        # --- Resolve input value from context --------------------------------
        jinja_ctx = context.to_jinja_context()
        input_field = cfg.input_field  # e.g. "text" or "steps.fetch.data.body"

        # Support dot-notation to reach nested context values
        parts = input_field.split(".")
        value: Any = jinja_ctx
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, (list, tuple)):
                try:
                    value = value[int(part)]
                except (ValueError, IndexError):
                    value = None
                    break
            else:
                value = None
                break

        # --- Run extraction rules -------------------------------------------
        extracted: dict = {}
        total_rules = len(cfg.extract)
        self.report_progress(0.0, f"Extracting {total_rules} rules", done=0, total=total_rules)
        for rule_idx, rule in enumerate(cfg.extract):
            # Pass current extraction context to template renderer
            template_ctx = {**jinja_ctx, **extracted}
            extracted[rule.name] = _apply_extraction(value, rule, template_ctx)
            _rule_pct = round((rule_idx + 1) / total_rules * 100, 1) if total_rules > 0 else 100.0
            self.report_progress(_rule_pct, f"Extracted {rule.name}", done=rule_idx + 1, total=total_rules)

        # --- Run validation rules -------------------------------------------
        warnings: list[str] = []
        validation_errors: list[str] = []
        skip_item = False

        if cfg.checks:
            for vrule in cfg.checks:
                field_value = extracted.get(vrule.field)
                error_msg = _validate_field(field_value, vrule)
                if error_msg:
                    on_fail = vrule.on_fail.lower()
                    if on_fail == "error":
                        validation_errors.append(error_msg)
                    elif on_fail == "skip":
                        skip_item = True
                        warnings.append(f"[skip] {error_msg}")
                    else:
                        # "warn" — record and continue
                        warnings.append(error_msg)

        if validation_errors:
            return {
                "success": False,
                "error": "; ".join(validation_errors),
                "data": {
                    "extracted": extracted,
                    "validation_errors": validation_errors,
                    "warnings": warnings,
                },
                "duration": time.monotonic() - start,
            }

        # --- Format output --------------------------------------------------
        output = _format_output(extracted, cfg.output_format)

        self.report_progress(100.0, "done")
        return {
            "success": True,
            "data": {
                "result": output,
                "warnings": warnings,
                "skipped": skip_item,
            },
            "duration": time.monotonic() - start,
        }
