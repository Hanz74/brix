"""Tests for custom Jinja2 base64 filters."""

from brix.loader import PipelineLoader


def test_b64encode_string():
    loader = PipelineLoader()
    result = loader.render_value("{{ 'hello' | b64encode }}", {})
    assert result == "aGVsbG8="


def test_b64decode_string():
    loader = PipelineLoader()
    result = loader.render_value("{{ 'aGVsbG8=' | b64decode }}", {})
    assert result == "hello"


def test_b64_roundtrip():
    loader = PipelineLoader()
    result = loader.render_value("{{ ('Brix rocks' | b64encode) | b64decode }}", {})
    assert result == "Brix rocks"


def test_b64_filter_used_in_render_value_context():
    loader = PipelineLoader()
    result = loader.render_value("{{ payload | b64encode }}", {"payload": "umlaut-ä"})
    assert result == "dW1sYXV0LcOk"
