"""Tests for T-BRIX-TIPS-02: Registry content in get_tips output.

Covers:
- _load_registry_content returns formatted lines when entries exist
- Empty registry produces no extra output (no empty headers)
- Formatting is correct for all three registry types
"""
import asyncio
from unittest.mock import patch

import pytest

from brix.db import BrixDB


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Return a BrixDB backed by a temporary file."""
    return BrixDB(db_path=tmp_path / "brix.db")


# ---------------------------------------------------------------------------
# _load_registry_content
# ---------------------------------------------------------------------------

class TestLoadRegistryContent:
    def test_empty_registry_no_output(self, db):
        """When no registry entries exist, _load_registry_content returns []."""
        from brix.mcp_handlers.help import _load_registry_content
        with patch("brix.db.BrixDB", return_value=db):
            lines = _load_registry_content()
        assert lines == []

    def test_lessons_learned_appear(self, db):
        """Lessons learned entries are formatted under ## LESSONS LEARNED."""
        db.registry_add(
            "lessons_learned",
            "Always validate input",
            content={"detail": "Prevents crashes"},
            description="Input validation is critical",
        )
        from brix.mcp_handlers.help import _load_registry_content
        with patch("brix.db.BrixDB", return_value=db):
            lines = _load_registry_content()
        joined = "\n".join(lines)
        assert "## LESSONS LEARNED" in joined
        assert "### Always validate input" in joined
        assert "Input validation is critical" in joined
        assert "detail: Prevents crashes" in joined

    def test_error_patterns_appear(self, db):
        """Error pattern entries are formatted under ## ERROR PATTERNS with solution first."""
        db.registry_add(
            "error_patterns",
            "Connection timeout",
            content={"solution": "Increase timeout to 30s", "cause": "Slow network"},
            description="Timeout when connecting to DB",
        )
        from brix.mcp_handlers.help import _load_registry_content
        with patch("brix.db.BrixDB", return_value=db):
            lines = _load_registry_content()
        joined = "\n".join(lines)
        assert "## ERROR PATTERNS" in joined
        assert "### Connection timeout" in joined
        assert "Timeout when connecting to DB" in joined
        assert "Solution: Increase timeout to 30s" in joined
        assert "cause: Slow network" in joined

    def test_best_practices_appear(self, db):
        """Best practice entries are formatted under ## BEST PRACTICES."""
        db.registry_add(
            "best_practices",
            "Use Brick-First approach",
            content={"rule": "Configure bricks before writing helpers"},
            description="Always prefer built-in bricks",
        )
        from brix.mcp_handlers.help import _load_registry_content
        with patch("brix.db.BrixDB", return_value=db):
            lines = _load_registry_content()
        joined = "\n".join(lines)
        assert "## BEST PRACTICES" in joined
        assert "### Use Brick-First approach" in joined
        assert "Always prefer built-in bricks" in joined
        assert "rule: Configure bricks before writing helpers" in joined

    def test_all_three_types_together(self, db):
        """When all three registry types have entries, all sections appear."""
        db.registry_add("lessons_learned", "Lesson A", content="lesson text", description="desc A")
        db.registry_add("error_patterns", "Error B", content={"solution": "fix it"}, description="desc B")
        db.registry_add("best_practices", "Practice C", content="practice text", description="desc C")

        from brix.mcp_handlers.help import _load_registry_content
        with patch("brix.db.BrixDB", return_value=db):
            lines = _load_registry_content()
        joined = "\n".join(lines)
        assert "## LESSONS LEARNED" in joined
        assert "## ERROR PATTERNS" in joined
        assert "## BEST PRACTICES" in joined
        assert "### Lesson A" in joined
        assert "### Error B" in joined
        assert "### Practice C" in joined

    def test_partial_registry_only_populated_sections(self, db):
        """Only sections with entries get headers — no empty ## headers."""
        db.registry_add("error_patterns", "Only Error", content={"solution": "fix"}, description="")

        from brix.mcp_handlers.help import _load_registry_content
        with patch("brix.db.BrixDB", return_value=db):
            lines = _load_registry_content()
        joined = "\n".join(lines)
        assert "## ERROR PATTERNS" in joined
        assert "## LESSONS LEARNED" not in joined
        assert "## BEST PRACTICES" not in joined

    def test_string_content_rendering(self, db):
        """String content (not dict) is rendered line by line."""
        db.registry_add(
            "lessons_learned",
            "Multi-line lesson",
            content="Line one\nLine two\nLine three",
            description="",
        )
        from brix.mcp_handlers.help import _load_registry_content
        with patch("brix.db.BrixDB", return_value=db):
            lines = _load_registry_content()
        joined = "\n".join(lines)
        assert "Line one" in joined
        assert "Line two" in joined
        assert "Line three" in joined


# ---------------------------------------------------------------------------
# Integration: registry content in _handle_get_tips
# ---------------------------------------------------------------------------

class TestGetTipsIncludesRegistry:
    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_registry_in_get_tips_output(self, db):
        """Registry content appears in the full get_tips output."""
        db.registry_add("best_practices", "Test BP", content={"tip": "be good"}, description="A best practice")

        from brix.mcp_handlers.help import _handle_get_tips
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_get_tips({}))
        joined = "\n".join(result["tips"])
        assert "## BEST PRACTICES" in joined
        assert "### Test BP" in joined

    def test_empty_registry_no_extra_in_tips(self, db):
        """When registry is empty, get_tips output has no registry headers."""
        from brix.mcp_handlers.help import _handle_get_tips
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_get_tips({}))
        joined = "\n".join(result["tips"])
        assert "## LESSONS LEARNED" not in joined
        assert "## ERROR PATTERNS" not in joined
        assert "## BEST PRACTICES" not in joined
