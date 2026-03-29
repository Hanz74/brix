"""Tests for T-BRIX-IMP-03: contains/in operator in flow.filter.

Verifies that FilterRunner supports substring checks via:
  - Jinja2 'in' operator:       {{ 'keyword' in item.title }}
  - Custom 'contains' test:     {{ item.title is contains 'keyword' }}
"""

import pytest

from brix.runners.filter import FilterRunner


class _Step:
    """Minimal step stand-in for tests."""

    def __init__(self, params=None):
        self.params = params or {}


# ---------------------------------------------------------------------------
# contains/in operator tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_in_operator_matches():
    """'in' operator keeps items where substring is found in field."""
    runner = FilterRunner()
    items = [
        {"title": "Python tutorial for beginners"},
        {"title": "Java crash course"},
        {"title": "Advanced Python patterns"},
        {"title": "Go language basics"},
    ]
    step = _Step(params={"input": items, "where": "{{ 'Python' in item.title }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["items_count"] == 2
    assert all("Python" in it["title"] for it in result["data"])


@pytest.mark.asyncio
async def test_filter_in_operator_no_match():
    """'in' operator returns empty list when no items contain the substring."""
    runner = FilterRunner()
    items = [
        {"title": "Java crash course"},
        {"title": "Go language basics"},
    ]
    step = _Step(params={"input": items, "where": "{{ 'Python' in item.title }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == []
    assert result["items_count"] == 0


@pytest.mark.asyncio
async def test_filter_contains_test_matches():
    """Jinja2 'contains' custom test keeps items where substring is found."""
    runner = FilterRunner()
    items = [
        {"subject": "Invoice #2024-001"},
        {"subject": "Meeting notes"},
        {"subject": "Invoice #2024-002"},
        {"subject": "Weekly report"},
    ]
    step = _Step(params={"input": items, "where": "{{ item.subject is contains 'Invoice' }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["items_count"] == 2
    assert all("Invoice" in it["subject"] for it in result["data"])


@pytest.mark.asyncio
async def test_filter_contains_case_sensitive():
    """contains/in operator is case-sensitive — 'invoice' does not match 'Invoice'."""
    runner = FilterRunner()
    items = [
        {"subject": "Invoice #2024-001"},
        {"subject": "invoice reminder"},
    ]
    # 'Invoice' (capital I) should only match the first item
    step = _Step(params={"input": items, "where": "{{ 'Invoice' in item.subject }}"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["items_count"] == 1
    assert result["data"][0]["subject"] == "Invoice #2024-001"


@pytest.mark.asyncio
async def test_filter_in_operator_combined_with_other_conditions():
    """'in' operator can be combined with other Jinja2 conditions via 'and'."""
    runner = FilterRunner()
    items = [
        {"title": "Python basics", "level": "beginner"},
        {"title": "Python advanced", "level": "expert"},
        {"title": "Java basics", "level": "beginner"},
        {"title": "Python intermediate", "level": "intermediate"},
    ]
    step = _Step(
        params={
            "input": items,
            "where": "{{ 'Python' in item.title and item.level != 'beginner' }}",
        }
    )
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["items_count"] == 2
    titles = [it["title"] for it in result["data"]]
    assert "Python advanced" in titles
    assert "Python intermediate" in titles
