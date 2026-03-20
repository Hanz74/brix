"""Tests for FilterRunner and TransformRunner."""

import pytest

from brix.runners.filter import FilterRunner
from brix.runners.transform import TransformRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step stand-in for tests."""

    def __init__(self, params=None):
        self.params = params or {}


# ---------------------------------------------------------------------------
# FilterRunner tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_basic():
    """Filter a list of integers keeping only those > 5."""
    runner = FilterRunner()
    step = _Step(params={"input": [1, 10, 3, 7, 2, 8], "where": "{{ item > 5 }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == [10, 7, 8]


@pytest.mark.asyncio
async def test_filter_endswith():
    """Filter list of dicts, keeping only items whose name ends with .pdf."""
    runner = FilterRunner()
    items = [
        {"name": "report.pdf"},
        {"name": "image.png"},
        {"name": "notes.pdf"},
        {"name": "data.csv"},
    ]
    step = _Step(params={"input": items, "where": "{{ item.name is endswith('.pdf') }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == [{"name": "report.pdf"}, {"name": "notes.pdf"}]
    assert result["items_count"] == 2


@pytest.mark.asyncio
async def test_filter_numeric():
    """Filter dicts where value field > 10."""
    runner = FilterRunner()
    items = [{"value": 5}, {"value": 15}, {"value": 10}, {"value": 20}]
    step = _Step(params={"input": items, "where": "{{ item.value > 10 }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == [{"value": 15}, {"value": 20}]


@pytest.mark.asyncio
async def test_filter_empty_result():
    """No item matches — should return empty list (not an error)."""
    runner = FilterRunner()
    step = _Step(params={"input": [1, 2, 3], "where": "{{ item > 100 }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == []
    assert result["items_count"] == 0


@pytest.mark.asyncio
async def test_filter_all_match():
    """All items match — should return the full list."""
    runner = FilterRunner()
    step = _Step(params={"input": [1, 2, 3], "where": "{{ item > 0 }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == [1, 2, 3]
    assert result["items_count"] == 3


@pytest.mark.asyncio
async def test_filter_no_input():
    """Missing 'input' param returns an error."""
    runner = FilterRunner()
    step = _Step(params={"where": "{{ item > 0 }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "input" in result["error"].lower()


@pytest.mark.asyncio
async def test_filter_no_where():
    """Missing 'where' param returns an error."""
    runner = FilterRunner()
    step = _Step(params={"input": [1, 2, 3]})
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "where" in result["error"].lower()


@pytest.mark.asyncio
async def test_filter_invalid_input_type():
    """Input is a dict (not a list) — should return an error."""
    runner = FilterRunner()
    step = _Step(params={"input": {"key": "value"}, "where": "{{ item > 0 }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "list" in result["error"].lower()


@pytest.mark.asyncio
async def test_filter_expression_error():
    """Items that cause expression errors are silently skipped."""
    runner = FilterRunner()
    # Mix of dicts with and without 'value' key; missing key raises AttributeError in sandbox
    items = [{"value": 5}, {"other": 10}, {"value": 15}]
    step = _Step(params={"input": items, "where": "{{ item.value > 10 }}"})
    result = await runner.execute(step, context=None)
    # Only {"value": 15} clearly passes; {"other": 10} causes an error → skipped
    assert result["success"] is True
    assert {"value": 15} in result["data"]


# ---------------------------------------------------------------------------
# TransformRunner tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transform_list_of_dicts():
    """Extract a single field from each item in a list."""
    runner = TransformRunner()
    items = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    step = _Step(params={"input": items, "expression": "{{ item.name }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_transform_dict():
    """Transform a single dict — expression receives 'data' variable."""
    runner = TransformRunner()
    step = _Step(params={
        "input": {"first": "John", "last": "Doe"},
        "expression": "{{ data.first }} {{ data.last }}",
    })
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == "John Doe"


@pytest.mark.asyncio
async def test_transform_string_concat():
    """Concatenate two fields from each list item."""
    runner = TransformRunner()
    items = [{"first": "Alice", "last": "Smith"}, {"first": "Bob", "last": "Jones"}]
    step = _Step(params={"input": items, "expression": "{{ item.first }} {{ item.last }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == ["Alice Smith", "Bob Jones"]


@pytest.mark.asyncio
async def test_transform_no_input():
    """Missing 'input' returns an error."""
    runner = TransformRunner()
    step = _Step(params={"expression": "{{ item.name }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "input" in result["error"].lower()


@pytest.mark.asyncio
async def test_transform_no_expression():
    """Missing 'expression' returns an error."""
    runner = TransformRunner()
    step = _Step(params={"input": [1, 2, 3]})
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "expression" in result["error"].lower()


@pytest.mark.asyncio
async def test_transform_json_output():
    """Expression that produces valid JSON is parsed into a native type."""
    runner = TransformRunner()
    items = [{"id": 1, "tags": ["a", "b"]}, {"id": 2, "tags": ["c"]}]
    # Render a JSON array string — loader will parse it back to a list
    step = _Step(params={
        "input": items,
        "expression": '{"id": {{ item.id }}, "tag_count": {{ item.tags | length }}}',
    })
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == [{"id": 1, "tag_count": 2}, {"id": 2, "tag_count": 1}]
