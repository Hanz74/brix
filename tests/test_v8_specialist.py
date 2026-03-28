"""Tests for T-BRIX-V8-03: specialist step type — declarative data extraction.

Coverage:
1.  Regex extraction — simple full match
2.  Regex extraction — named capture group
3.  Regex extraction — findall returns list
4.  Regex extraction — no match returns default
5.  JSON-path extraction — nested dict
6.  JSON-path extraction — list index
7.  JSON-path extraction — missing path returns default
8.  Split extraction — basic split
9.  Split extraction — None separator splits on whitespace
10. Template extraction — uses previously extracted fields
11. Validation: required — pass
12. Validation: required — fail (on_fail=error)
13. Validation: required — fail (on_fail=warn)
14. Validation: required — fail (on_fail=skip)
15. Validation: min_length — pass + fail
16. Validation: max_length — pass + fail
17. Validation: regex rule — pass + fail
18. Validation: type rule — pass + fail
19. Output format: dict (default)
20. Output format: list
21. Output format: flat (list fields → comma-joined string)
22. Integration: specialist step in pipeline context
23. Edge case: missing config block
24. Edge case: empty text / no match — all defaults
25. Edge case: unknown extraction method falls back to default
26. Edge case: multiple validation errors accumulate warnings
"""
from __future__ import annotations

import asyncio
import pytest

from brix.models import ExtractionRule, ValidationRule, SpecialistConfig
from brix.runners.specialist import (
    SpecialistRunner,
    _extract_regex,
    _extract_json_path,
    _extract_split,
    _extract_template,
    _apply_extraction,
    _validate_field,
    _format_output,
)


# ---------------------------------------------------------------------------
# Minimal fake context for integration tests
# ---------------------------------------------------------------------------

class FakeContext:
    """Minimal pipeline context stub."""

    def __init__(self, data: dict):
        self._data = data

    def to_jinja_context(self) -> dict:
        return dict(self._data)


def make_step(config: dict) -> object:
    """Build a minimal fake step with config dict."""
    class FakeStep:
        pass

    s = FakeStep()
    s.config = config
    return s


# ---------------------------------------------------------------------------
# Low-level extraction tests
# ---------------------------------------------------------------------------

def make_regex_rule(**kwargs) -> ExtractionRule:
    return ExtractionRule(name="field", method="regex", **kwargs)


def make_json_rule(**kwargs) -> ExtractionRule:
    return ExtractionRule(name="field", method="json_path", **kwargs)


def make_split_rule(**kwargs) -> ExtractionRule:
    return ExtractionRule(name="field", method="split", **kwargs)


def make_template_rule(**kwargs) -> ExtractionRule:
    return ExtractionRule(name="field", method="template", **kwargs)


# 1. Regex — simple full match
def test_regex_simple_match():
    rule = make_regex_rule(pattern=r"\d+")
    assert _extract_regex("Betrag 42 EUR", rule) == "42"


# 2. Regex — capture group
def test_regex_capture_group():
    rule = make_regex_rule(pattern=r"IBAN\s*([A-Z]{2}\d+)", group=1)
    assert _extract_regex("IBAN DE12345678901234567890", rule) == "DE12345678901234567890"


# 3. Regex — findall returns list
def test_regex_findall():
    rule = make_regex_rule(pattern=r"\d+", findall=True)
    result = _extract_regex("Pos 1: 10 EUR, Pos 2: 20 EUR", rule)
    assert result == ["1", "10", "2", "20"]


# 4. Regex — no match returns default
def test_regex_no_match_default():
    rule = make_regex_rule(pattern=r"INV-\d{4}-\d+", default="N/A")
    assert _extract_regex("No invoice number here", rule) == "N/A"


# 5. JSON-path — nested dict
def test_json_path_nested_dict():
    rule = make_json_rule(pattern="invoice.amount")
    data = {"invoice": {"amount": "99.50", "currency": "EUR"}}
    assert _extract_json_path(data, rule) == "99.50"


# 6. JSON-path — list index
def test_json_path_list_index():
    rule = make_json_rule(pattern="items.0.name", default="none")
    data = {"items": [{"name": "Widget"}, {"name": "Gadget"}]}
    assert _extract_json_path(data, rule) == "Widget"


# 7. JSON-path — missing path returns default
def test_json_path_missing_returns_default():
    rule = make_json_rule(pattern="invoice.vat.id", default="MISSING")
    data = {"invoice": {"amount": "42"}}
    assert _extract_json_path(data, rule) == "MISSING"


# 8. Split — basic split by separator
def test_split_basic():
    rule = make_split_rule(pattern=",")
    result = _extract_split("a,b,c", rule)
    assert result == ["a", "b", "c"]


# 9. Split — None separator splits on whitespace
def test_split_whitespace():
    rule = make_split_rule(pattern=None)
    result = _extract_split("hello world  foo", rule)
    assert result == ["hello", "world", "foo"]


# 10. Template — uses previously extracted fields
def test_template_extraction():
    rule = make_template_rule(template="{{ first_name }} {{ last_name }}")
    ctx = {"first_name": "Max", "last_name": "Mustermann"}
    result = _extract_template(ctx, rule)
    assert result == "Max Mustermann"


# ---------------------------------------------------------------------------
# Low-level validation tests
# ---------------------------------------------------------------------------

def make_val_rule(**kwargs) -> ValidationRule:
    return ValidationRule(field="f", rule="required", **kwargs)


# 11. required — pass
def test_validate_required_pass():
    rule = ValidationRule(field="f", rule="required")
    assert _validate_field("something", rule) is None


# 12. required — fail
def test_validate_required_fail():
    rule = ValidationRule(field="f", rule="required")
    assert _validate_field("", rule) is not None
    assert _validate_field(None, rule) is not None
    assert _validate_field([], rule) is not None


# 13. min_length — pass and fail
def test_validate_min_length():
    rule = ValidationRule(field="f", rule="min_length", value=3)
    assert _validate_field("abc", rule) is None
    assert _validate_field("ab", rule) is not None


# 14. max_length — pass and fail
def test_validate_max_length():
    rule = ValidationRule(field="f", rule="max_length", value=5)
    assert _validate_field("abcde", rule) is None
    assert _validate_field("abcdef", rule) is not None


# 15. regex rule — pass and fail
def test_validate_regex_rule():
    rule = ValidationRule(field="f", rule="regex", value=r"^\d{4}$")
    assert _validate_field("1234", rule) is None
    assert _validate_field("12345", rule) is not None
    assert _validate_field("abcd", rule) is not None


# 16. type rule — pass and fail
def test_validate_type_rule():
    rule = ValidationRule(field="f", rule="type", value="str")
    assert _validate_field("hello", rule) is None
    assert _validate_field(42, rule) is not None

    rule_int = ValidationRule(field="f", rule="type", value="int")
    assert _validate_field(42, rule_int) is None
    assert _validate_field("42", rule_int) is not None

    rule_list = ValidationRule(field="f", rule="type", value="list")
    assert _validate_field([1, 2], rule_list) is None
    assert _validate_field((1, 2), rule_list) is not None


# ---------------------------------------------------------------------------
# Output format tests
# ---------------------------------------------------------------------------

# 17. Output format: dict
def test_format_dict():
    d = {"a": 1, "b": [2, 3]}
    result = _format_output(d, "dict")
    assert result == {"a": 1, "b": [2, 3]}


# 18. Output format: list
def test_format_list():
    d = {"a": 1, "b": 2, "c": 3}
    result = _format_output(d, "list")
    assert result == [1, 2, 3]


# 19. Output format: flat — list values joined
def test_format_flat():
    d = {"tags": ["a", "b", "c"], "name": "foo"}
    result = _format_output(d, "flat")
    assert result["tags"] == "a, b, c"
    assert result["name"] == "foo"


# ---------------------------------------------------------------------------
# SpecialistRunner integration tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_runner_basic_regex_extraction():
    """End-to-end: extract invoice number via regex from context text."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "Invoice: INV-2024-0042 from Acme Corp"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "invoice_number", "method": "regex", "pattern": r"INV-\d{4}-\d+", "default": None},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert result["data"]["result"]["invoice_number"] == "INV-2024-0042"
    assert result["data"]["warnings"] == []


@pytest.mark.asyncio
async def test_runner_multiple_rules():
    """Multiple extraction rules produce all fields in output."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "IBAN: DE12345678 Amount: 199.99 EUR"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "iban", "method": "regex", "pattern": r"IBAN:\s*(\S+)", "group": 1},
            {"name": "amount", "method": "regex", "pattern": r"Amount:\s*([\d.]+)", "group": 1},
            {"name": "currency", "method": "regex", "pattern": r"\d\s+([A-Z]{3})", "group": 1},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    r = result["data"]["result"]
    assert r["iban"] == "DE12345678"
    assert r["amount"] == "199.99"
    assert r["currency"] == "EUR"


@pytest.mark.asyncio
async def test_runner_json_path_extraction():
    """Extract nested fields from a dict via json_path."""
    runner = SpecialistRunner()
    ctx = FakeContext({
        "payload": {
            "order": {"id": "ORD-001", "total": 42.5, "items": [{"sku": "A1"}, {"sku": "B2"}]}
        }
    })
    step = make_step({
        "input_field": "payload",
        "extract": [
            {"name": "order_id", "method": "json_path", "pattern": "order.id"},
            {"name": "total", "method": "json_path", "pattern": "order.total"},
            {"name": "first_sku", "method": "json_path", "pattern": "order.items.0.sku"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    r = result["data"]["result"]
    assert r["order_id"] == "ORD-001"
    assert r["total"] == 42.5
    assert r["first_sku"] == "A1"


@pytest.mark.asyncio
async def test_runner_split_extraction():
    """Split extraction returns list of parts."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "alpha,beta,gamma"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "parts", "method": "split", "pattern": ","},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert result["data"]["result"]["parts"] == ["alpha", "beta", "gamma"]


@pytest.mark.asyncio
async def test_runner_template_uses_prior_fields():
    """Template rule can reference earlier extracted fields."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "Firstname: John Lastname: Doe"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "first", "method": "regex", "pattern": r"Firstname:\s+(\w+)", "group": 1},
            {"name": "last", "method": "regex", "pattern": r"Lastname:\s+(\w+)", "group": 1},
            {"name": "full_name", "method": "template", "template": "{{ first }} {{ last }}"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert result["data"]["result"]["full_name"] == "John Doe"


@pytest.mark.asyncio
async def test_runner_validation_on_fail_error():
    """required validation with on_fail=error causes success=False."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "No invoice number here"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "invoice_id", "method": "regex", "pattern": r"INV-\d+", "default": None},
        ],
        "checks": [
            {"field": "invoice_id", "rule": "required", "on_fail": "error"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is False
    assert "invoice_id" in result["error"].lower() or "required" in result["error"].lower()


@pytest.mark.asyncio
async def test_runner_validation_on_fail_warn():
    """required validation with on_fail=warn keeps success=True, adds to warnings."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "No invoice number here"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "invoice_id", "method": "regex", "pattern": r"INV-\d+", "default": None},
        ],
        "checks": [
            {"field": "invoice_id", "rule": "required", "on_fail": "warn"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert len(result["data"]["warnings"]) == 1


@pytest.mark.asyncio
async def test_runner_validation_on_fail_skip():
    """required validation with on_fail=skip sets skipped=True in data."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "nothing"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "amount", "method": "regex", "pattern": r"\d+\.\d+", "default": None},
        ],
        "checks": [
            {"field": "amount", "rule": "required", "on_fail": "skip"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert result["data"]["skipped"] is True


@pytest.mark.asyncio
async def test_runner_output_format_list():
    """output_format=list returns values in extraction order."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "A 1 B 2"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "letter1", "method": "regex", "pattern": r"A"},
            {"name": "digit1", "method": "regex", "pattern": r"1"},
        ],
        "output_format": "list",
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert result["data"]["result"] == ["A", "1"]


@pytest.mark.asyncio
async def test_runner_output_format_flat():
    """output_format=flat joins list-valued fields into a comma-separated string."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "cats dogs birds"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "animals", "method": "split", "pattern": " "},
        ],
        "output_format": "flat",
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert result["data"]["result"]["animals"] == "cats, dogs, birds"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_runner_missing_config_block():
    """Missing config block produces a clear error."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "hello"})
    step = make_step(None)  # config=None
    step.config = None
    result = await runner.execute(step, ctx)
    assert result["success"] is False
    assert "config" in result["error"].lower()


@pytest.mark.asyncio
async def test_runner_empty_text_all_defaults():
    """Empty text string — all rules return their defaults."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": ""})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "amount", "method": "regex", "pattern": r"\d+", "default": "0"},
            {"name": "iban", "method": "regex", "pattern": r"IBAN\s+\S+", "default": "N/A"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    r = result["data"]["result"]
    assert r["amount"] == "0"
    assert r["iban"] == "N/A"


@pytest.mark.asyncio
async def test_runner_unknown_extraction_method_returns_default():
    """Unknown method falls back to rule default without crashing."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "some text"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "x", "method": "magic_method", "default": "fallback"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert result["data"]["result"]["x"] == "fallback"


@pytest.mark.asyncio
async def test_runner_multiple_warnings_accumulate():
    """Multiple warn-level validation failures are all collected."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "minimal"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "a", "method": "regex", "pattern": r"NOMATCH_A", "default": None},
            {"name": "b", "method": "regex", "pattern": r"NOMATCH_B", "default": None},
        ],
        "checks": [
            {"field": "a", "rule": "required", "on_fail": "warn"},
            {"field": "b", "rule": "required", "on_fail": "warn"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert len(result["data"]["warnings"]) == 2


@pytest.mark.asyncio
async def test_runner_nested_input_field():
    """input_field supports dot-notation to reach nested context values."""
    runner = SpecialistRunner()
    ctx = FakeContext({"response": {"body": "Total: 88.00 EUR"}})
    step = make_step({
        "input_field": "response.body",
        "extract": [
            {"name": "total", "method": "regex", "pattern": r"Total:\s*([\d.]+)", "group": 1},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True
    assert result["data"]["result"]["total"] == "88.00"


@pytest.mark.asyncio
async def test_runner_min_length_validation_pass():
    """min_length validation passes for a sufficiently long string."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "IBAN DE12345678901234567890"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "iban", "method": "regex", "pattern": r"IBAN\s+(\S+)", "group": 1},
        ],
        "checks": [
            {"field": "iban", "rule": "min_length", "value": 5, "on_fail": "error"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True


@pytest.mark.asyncio
async def test_runner_min_length_validation_fail():
    """min_length validation fails and returns error."""
    runner = SpecialistRunner()
    ctx = FakeContext({"text": "AB"})
    step = make_step({
        "input_field": "text",
        "extract": [
            {"name": "code", "method": "regex", "pattern": r"[A-Z]+"},
        ],
        "checks": [
            {"field": "code", "rule": "min_length", "value": 5, "on_fail": "error"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is False


@pytest.mark.asyncio
async def test_runner_type_validation():
    """type validation works on json_path extracted values."""
    runner = SpecialistRunner()
    ctx = FakeContext({"data": {"count": 42}})
    step = make_step({
        "input_field": "data",
        "extract": [
            {"name": "count", "method": "json_path", "pattern": "count"},
        ],
        "checks": [
            {"field": "count", "rule": "type", "value": "int", "on_fail": "error"},
        ],
    })
    result = await runner.execute(step, ctx)
    assert result["success"] is True


# ---------------------------------------------------------------------------
# Brick registry integration
# ---------------------------------------------------------------------------

def test_specialist_brick_in_registry():
    """specialist brick is discoverable via BrickRegistry."""
    from brix.bricks.registry import BrickRegistry
    reg = BrickRegistry()
    brick = reg.get("specialist")
    assert brick is not None
    assert brick.type == "specialist"
    assert brick.category == "transform"
    schema = brick.to_json_schema()
    assert "extract" in schema["properties"]
    assert "extract" in schema.get("required", [])


def test_specialist_step_type_in_model():
    """Step model accepts type='specialist'."""
    from brix.models import Step
    step = Step.model_validate({
        "id": "extract_data",
        "type": "specialist",
        "config": {
            "input_field": "text",
            "extract": [
                {"name": "amount", "method": "regex", "pattern": r"\d+"},
            ],
        },
    })
    assert step.type == "specialist"
    assert step.config["extract"][0]["name"] == "amount"


def test_specialist_runner_registered_in_engine():
    """SpecialistRunner is registered in PipelineEngine._runners."""
    from brix.engine import PipelineEngine
    from brix.runners.specialist import SpecialistRunner
    engine = PipelineEngine()
    assert "specialist" in engine._runners
    assert isinstance(engine._runners["specialist"], SpecialistRunner)
