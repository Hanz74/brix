"""Tests for T-BRIX-LOADER-02: SandboxedNativeEnvironment preserves Python types.

render_value() uses SandboxedNativeEnvironment so that expressions like
{{ step.output | default({}) }} return a dict (not the string "{}").
render_template() and evaluate_condition() keep using the regular
SandboxedEnvironment (string output).
"""

import json

import pytest

from brix.loader import PipelineLoader


@pytest.fixture
def loader():
    return PipelineLoader()


class TestNativeTypePreservation:
    """render_value() must return native Python types for common patterns."""

    def test_default_dict_returns_dict(self, loader):
        """{{ step.output | default({}) }} returns dict, not string '{}'."""
        ctx = {}  # step.output is undefined
        result = loader.render_value("{{ step.output | default({}) }}", ctx)
        assert isinstance(result, dict), f"Expected dict, got {type(result).__name__}: {result!r}"
        assert result == {}

    def test_default_dict_with_data(self, loader):
        """{{ step.output | default({}) }} returns the actual dict when present."""
        ctx = {"step": {"output": {"key": "val", "num": 42}}}
        result = loader.render_value("{{ step.output | default({}) }}", ctx)
        assert isinstance(result, dict), f"Expected dict, got {type(result).__name__}: {result!r}"
        assert result == {"key": "val", "num": 42}

    def test_default_list_returns_list(self, loader):
        """{{ step.output | default([]) }} returns list, not string '[]'."""
        ctx = {}
        result = loader.render_value("{{ step.output | default([]) }}", ctx)
        assert isinstance(result, list), f"Expected list, got {type(result).__name__}: {result!r}"
        assert result == []

    def test_default_list_with_data(self, loader):
        """{{ step.output | default([]) }} returns the actual list when present."""
        ctx = {"step": {"output": [1, 2, 3]}}
        result = loader.render_value("{{ step.output | default([]) }}", ctx)
        assert isinstance(result, list), f"Expected list, got {type(result).__name__}: {result!r}"
        assert result == [1, 2, 3]

    def test_default_int_returns_int(self, loader):
        """{{ step.output.field | default(0) }} returns int 0, not string '0'."""
        ctx = {}
        result = loader.render_value("{{ step.output.field | default(0) }}", ctx)
        assert isinstance(result, int), f"Expected int, got {type(result).__name__}: {result!r}"
        assert result == 0

    def test_default_int_with_data(self, loader):
        """{{ step.output.count | default(0) }} returns the actual int when present."""
        ctx = {"step": {"output": {"count": 42}}}
        result = loader.render_value("{{ step.output.count | default(0) }}", ctx)
        assert isinstance(result, (int, float)), f"Expected int, got {type(result).__name__}: {result!r}"
        assert result == 42

    def test_default_bool_returns_bool(self, loader):
        """{{ step.output.flag | default(false) }} returns bool."""
        ctx = {}
        result = loader.render_value("{{ step.output.flag | default(false) }}", ctx)
        assert isinstance(result, bool), f"Expected bool, got {type(result).__name__}: {result!r}"
        assert result is False

    def test_default_float_returns_float(self, loader):
        """{{ step.output.rate | default(0.0) }} returns float."""
        ctx = {}
        result = loader.render_value("{{ step.output.rate | default(0.0) }}", ctx)
        assert isinstance(result, (int, float)), f"Expected float, got {type(result).__name__}: {result!r}"
        assert result == 0.0


class TestStringConcatenation:
    """String concatenation with ~ must still return a string."""

    def test_tilde_concat_returns_string(self, loader):
        """{{ 'prefix_' ~ value }} returns string."""
        ctx = {"value": "hello"}
        result = loader.render_value("{{ 'prefix_' ~ value }}", ctx)
        assert isinstance(result, str), f"Expected str, got {type(result).__name__}: {result!r}"
        assert result == "prefix_hello"

    def test_mixed_text_and_expression(self, loader):
        """Static text around {{ }} returns string."""
        ctx = {"name": "world"}
        result = loader.render_value("hello {{ name }}", ctx)
        assert isinstance(result, str)
        assert result == "hello world"

    def test_multiple_expressions(self, loader):
        """Multiple {{ }} blocks in one string return string."""
        ctx = {"a": "foo", "b": "bar"}
        result = loader.render_value("{{ a }}-{{ b }}", ctx)
        assert isinstance(result, str)
        assert result == "foo-bar"


class TestTojsonCompatibility:
    """LOADER-01 compat: {{ data | tojson }} must still return a JSON string."""

    def test_tojson_returns_string(self, loader):
        ctx = {"data": {"key": "val"}}
        result = loader.render_value("{{ data | tojson }}", ctx)
        assert isinstance(result, str), f"Expected str, got {type(result).__name__}: {result!r}"
        assert json.loads(result) == {"key": "val"}

    def test_tojson_list_returns_string(self, loader):
        ctx = {"data": [1, 2, 3]}
        result = loader.render_value("{{ data | tojson }}", ctx)
        assert isinstance(result, str)
        assert json.loads(result) == [1, 2, 3]


class TestFilterExpressions:
    """Jinja2 filter expressions should return native types."""

    def test_selectattr_list(self, loader):
        """{{ items | selectattr('active') | list }} returns list."""
        ctx = {"items": [
            {"name": "a", "active": True},
            {"name": "b", "active": False},
            {"name": "c", "active": True},
        ]}
        result = loader.render_value("{{ items | selectattr('active') | list }}", ctx)
        assert isinstance(result, list), f"Expected list, got {type(result).__name__}: {result!r}"
        assert len(result) == 2
        assert result[0]["name"] == "a"
        assert result[1]["name"] == "c"

    def test_reject_filter_returns_list(self, loader):
        """{{ items | reject('equalto', 2) | list }} returns list."""
        ctx = {"items": [1, 2, 3]}
        result = loader.render_value("{{ items | reject('equalto', 2) | list }}", ctx)
        assert isinstance(result, list), f"Expected list, got {type(result).__name__}: {result!r}"
        assert result == [1, 3]

    def test_length_filter_returns_int(self, loader):
        """{{ items | length }} returns int."""
        ctx = {"items": [1, 2, 3]}
        result = loader.render_value("{{ items | length }}", ctx)
        assert isinstance(result, int), f"Expected int, got {type(result).__name__}: {result!r}"
        assert result == 3

    def test_map_filter_returns_list(self, loader):
        """{{ items | map(attribute='name') | list }} returns list."""
        ctx = {"items": [{"name": "a"}, {"name": "b"}]}
        result = loader.render_value("{{ items | map(attribute='name') | list }}", ctx)
        assert isinstance(result, list), f"Expected list, got {type(result).__name__}: {result!r}"
        assert result == ["a", "b"]


class TestRenderTemplateStaysString:
    """render_template() must always return strings (used for when-conditions, SQL etc.)."""

    def test_render_template_dict_is_string(self, loader):
        ctx = {"data": {"key": "val"}}
        result = loader.render_template("{{ data }}", ctx)
        assert isinstance(result, str)

    def test_render_template_list_is_string(self, loader):
        ctx = {"items": [1, 2, 3]}
        result = loader.render_template("{{ items }}", ctx)
        assert isinstance(result, str)


class TestEvaluateConditionStaysString:
    """evaluate_condition() uses render_template internally -- string-based."""

    def test_condition_true(self, loader):
        ctx = {"flag": True}
        assert loader.evaluate_condition("{{ flag }}", ctx) is True

    def test_condition_false(self, loader):
        ctx = {"flag": False}
        assert loader.evaluate_condition("{{ flag }}", ctx) is False

    def test_condition_with_comparison(self, loader):
        ctx = {"count": 5}
        assert loader.evaluate_condition("{{ count > 3 }}", ctx) is True
        assert loader.evaluate_condition("{{ count > 10 }}", ctx) is False


class TestNativeEnvHasAllGlobalsAndFilters:
    """The native_env must have the same globals and filters as the regular env."""

    def test_now_global(self, loader):
        import datetime
        result = loader.render_value("{{ now() }}", {})
        # NativeEnvironment preserves the datetime object
        assert isinstance(result, (str, datetime.datetime))

    def test_uuid4_global(self, loader):
        result = loader.render_value("{{ uuid4() }}", {})
        assert isinstance(result, str)
        assert len(result) == 36  # UUID format

    def test_fromjson_filter(self, loader):
        ctx = {"json_str": '{"a": 1}'}
        result = loader.render_value("{{ json_str | fromjson }}", ctx)
        assert isinstance(result, dict)
        assert result == {"a": 1}

    def test_fromjson_global(self, loader):
        ctx = {"json_str": '[1, 2, 3]'}
        result = loader.render_value("{{ fromjson(json_str) }}", ctx)
        assert isinstance(result, list)
        assert result == [1, 2, 3]

    def test_iif_filter(self, loader):
        ctx = {"flag": True}
        result = loader.render_value("{{ flag | iif('yes', 'no') }}", ctx)
        assert result == "yes"

    def test_b64encode_filter(self, loader):
        result = loader.render_value("{{ 'hello' | b64encode }}", {})
        assert isinstance(result, str)
        assert result == "aGVsbG8="

    def test_b64decode_filter(self, loader):
        result = loader.render_value("{{ 'aGVsbG8=' | b64decode }}", {})
        assert isinstance(result, str)
        assert result == "hello"
