"""Tests for T-BRIX-LOADER-01: render_value() must not reverse | tojson.

When a Jinja2 template uses ``| tojson``, the output is an explicit JSON
string.  render_value() must NOT auto-parse it back to a Python object.
"""

import json

import pytest

from brix.loader import PipelineLoader


@pytest.fixture
def loader():
    return PipelineLoader()


class TestTojsonPreservation:
    """{{ data | tojson }} must render to a JSON *string*, not back to a dict/list."""

    def test_tojson_dict_stays_string(self, loader):
        ctx = {"data": {"key": "val", "num": 42}}
        result = loader.render_value("{{ data | tojson }}", ctx)
        # Must be a string (the JSON representation), NOT a dict
        assert isinstance(result, str), f"Expected str, got {type(result).__name__}: {result!r}"
        # And the string must be valid JSON that round-trips to the original
        assert json.loads(result) == {"key": "val", "num": 42}

    def test_tojson_list_stays_string(self, loader):
        ctx = {"data": [1, 2, 3]}
        result = loader.render_value("{{ data | tojson }}", ctx)
        assert isinstance(result, str), f"Expected str, got {type(result).__name__}: {result!r}"
        assert json.loads(result) == [1, 2, 3]

    def test_tojson_nested_stays_string(self, loader):
        ctx = {"data": {"items": [{"a": 1}, {"b": 2}]}}
        result = loader.render_value("{{ data | tojson }}", ctx)
        assert isinstance(result, str)
        assert json.loads(result) == {"items": [{"a": 1}, {"b": 2}]}


class TestTemplateOutputNotParsed:
    """Any Jinja2 template output must be returned as-is (no auto-parsing)."""

    def test_template_int_parsed(self, loader):
        """{{ 42 }} renders to '42' and is auto-parsed to int (no tojson)."""
        ctx = {}
        result = loader.render_value("{{ 42 }}", ctx)
        assert result == 42

    def test_template_float_parsed(self, loader):
        """{{ 3.14 }} auto-parsed to float (no tojson filter)."""
        ctx = {}
        result = loader.render_value("{{ 3.14 }}", ctx)
        assert result == 3.14

    def test_template_bool_parsed(self, loader):
        """{{ flag }} where flag=True renders to 'True' — ast.literal_eval parses to bool."""
        ctx = {"flag": True}
        result = loader.render_value("{{ flag }}", ctx)
        # Jinja2 renders True as "True", ast.literal_eval parses it back to bool
        assert result is True

    def test_template_string_concat(self, loader):
        ctx = {"name": "world"}
        result = loader.render_value("hello {{ name }}", ctx)
        assert isinstance(result, str)
        assert result == "hello world"

    def test_tojson_int_stays_string(self, loader):
        """{{ val | tojson }} with int value stays string '42', not int."""
        ctx = {"val": 42}
        result = loader.render_value("{{ val | tojson }}", ctx)
        assert isinstance(result, str)
        assert result == "42"


class TestPureExpressionFastPath:
    """Pure expressions like {{ x }} still resolve natively for dicts/lists
    via _resolve_pure_expression (T-BRIX-BUG-10). This must NOT change."""

    def test_pure_dict_expression(self, loader):
        ctx = {"data": {"key": "val"}}
        result = loader.render_value("{{ data }}", ctx)
        # Pure expression fast path returns dict directly
        assert isinstance(result, dict)
        assert result == {"key": "val"}

    def test_pure_list_expression(self, loader):
        ctx = {"items": [1, 2, 3]}
        result = loader.render_value("{{ items }}", ctx)
        assert isinstance(result, list)
        assert result == [1, 2, 3]

    def test_pure_nested_path(self, loader):
        ctx = {"step1": {"output": {"data": [1, 2]}}}
        result = loader.render_value("{{ step1.output.data }}", ctx)
        assert isinstance(result, list)
        assert result == [1, 2]


class TestStaticValuesUnchanged:
    """Static values (no {{ }}) must pass through render_value unchanged."""

    def test_static_string(self, loader):
        result = loader.render_value("hello world", {})
        assert result == "hello world"

    def test_static_int(self, loader):
        result = loader.render_value(42, {})
        assert result == 42

    def test_static_dict(self, loader):
        result = loader.render_value({"key": "val"}, {})
        assert result == {"key": "val"}

    def test_static_list(self, loader):
        result = loader.render_value([1, 2, 3], {})
        assert result == [1, 2, 3]

    def test_static_string_number(self, loader):
        """A static string '42' (no template) stays '42' — no type coercion."""
        result = loader.render_value("42", {})
        assert isinstance(result, str)
        assert result == "42"


class TestRenderStepParamsTojson:
    """render_step_params must also preserve | tojson through the dict recursion."""

    def test_tojson_in_step_params(self, loader):
        from brix.models import Step

        step = Step(
            id="test_step",
            type="http.request",
            params={"body": "{{ data | tojson }}"},
        )
        ctx = {"data": {"key": "val"}}
        rendered = loader.render_step_params(step, ctx)
        body = rendered["body"]
        assert isinstance(body, str), f"Expected str, got {type(body).__name__}: {body!r}"
        assert json.loads(body) == {"key": "val"}
