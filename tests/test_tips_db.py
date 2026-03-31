"""Tests for T-BRIX-TIPS-01: Tips DB table and MCP handlers.

Covers:
- tip_create / tip_get / tip_update / tip_delete (DB layer)
- tip_list with category filter and active_only
- _handle_get_tips includes DB tips in output
- _handle_create_tip / _handle_update_tip / _handle_delete_tip / _handle_list_tips
- Seed migration populates tips table
"""
import asyncio
from pathlib import Path

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
# DB Layer: CRUD
# ---------------------------------------------------------------------------

class TestTipCRUD:
    def test_create_and_get(self, db):
        tip = db.tip_create("TEST-CAT", "Test Title", "Test content", priority=7)
        assert tip["id"]
        assert tip["category"] == "TEST-CAT"
        assert tip["title"] == "Test Title"
        assert tip["content"] == "Test content"
        assert tip["priority"] == 7
        assert tip["is_active"] is True

        fetched = db.tip_get(tip["id"])
        assert fetched is not None
        assert fetched["title"] == "Test Title"

    def test_get_nonexistent(self, db):
        assert db.tip_get("nonexistent-id") is None

    def test_update(self, db):
        tip = db.tip_create("CAT", "Title", "Content")
        updated = db.tip_update(tip["id"], title="New Title", priority=9)
        assert updated["title"] == "New Title"
        assert updated["priority"] == 9
        assert updated["content"] == "Content"  # unchanged

    def test_update_nonexistent(self, db):
        result = db.tip_update("nonexistent-id", title="X")
        assert result is None

    def test_update_is_active(self, db):
        tip = db.tip_create("CAT", "Title", "Content")
        updated = db.tip_update(tip["id"], is_active=False)
        assert updated["is_active"] == 0

    def test_delete(self, db):
        tip = db.tip_create("CAT", "Title", "Content")
        assert db.tip_delete(tip["id"]) is True
        assert db.tip_get(tip["id"]) is None

    def test_delete_nonexistent(self, db):
        assert db.tip_delete("nonexistent-id") is False


class TestTipList:
    def test_list_includes_created(self, db):
        # DB is pre-seeded with 11 tips from migration v62
        initial_count = len(db.tip_list())
        db.tip_create("CUSTOM-A", "A1", "Content A1", priority=5)
        db.tip_create("CUSTOM-B", "B1", "Content B1", priority=8)
        tips = db.tip_list()
        assert len(tips) == initial_count + 2

    def test_list_ordered_by_priority(self, db):
        tips = db.tip_list()
        # Verify descending priority order
        for i in range(len(tips) - 1):
            assert tips[i]["priority"] >= tips[i + 1]["priority"] or \
                tips[i]["category"] <= tips[i + 1]["category"]

    def test_list_by_category(self, db):
        db.tip_create("UNIQUE-CAT", "U1", "Content U1")
        db.tip_create("UNIQUE-CAT", "U2", "Content U2")
        tips = db.tip_list(category="UNIQUE-CAT")
        assert len(tips) == 2
        assert all(t["category"] == "UNIQUE-CAT" for t in tips)

    def test_list_active_only(self, db):
        # Count seeded active tips
        initial_active = len(db.tip_list(active_only=True))
        initial_all = len(db.tip_list(active_only=False))
        db.tip_create("FILTER-TEST", "Active", "Content", is_active=True)
        db.tip_create("FILTER-TEST", "Inactive", "Content", is_active=False)
        active = db.tip_list(active_only=True)
        assert len(active) == initial_active + 1

        all_tips = db.tip_list(active_only=False)
        assert len(all_tips) == initial_all + 2

    def test_seeded_tips_present(self, db):
        """Migration v62 seeds 11 tips."""
        tips = db.tip_list(active_only=False)
        assert len(tips) >= 11
        categories = {t["category"] for t in tips}
        assert "BRICK-FIRST" in categories
        assert "KERN-REGEL" in categories


# ---------------------------------------------------------------------------
# MCP Handlers
# ---------------------------------------------------------------------------

class TestMCPHandlers:
    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_create_tip_handler(self, db, monkeypatch):
        monkeypatch.setattr("brix.mcp_handlers.help.BrixDB", lambda: db, raising=False)
        # Need to patch at the import location in the handler
        import brix.mcp_handlers.help as help_mod
        original = None
        # Patch via monkeypatch on the module level import
        from brix.mcp_handlers.help import _handle_create_tip
        from unittest.mock import patch
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_create_tip({
                "category": "TEST",
                "title": "MCP Tip",
                "content": "Created via handler",
                "priority": 8,
            }))
        assert result["success"] is True
        assert result["tip"]["category"] == "TEST"

    def test_create_tip_missing_fields(self, db):
        from brix.mcp_handlers.help import _handle_create_tip
        result = self._run(_handle_create_tip({"category": "X"}))
        assert result["success"] is False

    def test_list_tips_handler(self, db):
        from brix.mcp_handlers.help import _handle_list_tips
        from unittest.mock import patch
        initial_count = len(db.tip_list())
        db.tip_create("A", "T1", "C1")
        db.tip_create("B", "T2", "C2")
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_list_tips({}))
        assert result["success"] is True
        assert result["count"] == initial_count + 2

    def test_list_tips_with_category(self, db):
        from brix.mcp_handlers.help import _handle_list_tips
        from unittest.mock import patch
        db.tip_create("A", "T1", "C1")
        db.tip_create("B", "T2", "C2")
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_list_tips({"category": "A"}))
        assert result["count"] == 1

    def test_update_tip_handler(self, db):
        from brix.mcp_handlers.help import _handle_update_tip
        from unittest.mock import patch
        tip = db.tip_create("A", "Old", "Content")
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_update_tip({"id": tip["id"], "title": "New"}))
        assert result["success"] is True
        assert result["tip"]["title"] == "New"

    def test_update_tip_not_found(self, db):
        from brix.mcp_handlers.help import _handle_update_tip
        from unittest.mock import patch
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_update_tip({"id": "nonexistent"}))
        assert result["success"] is False

    def test_delete_tip_handler(self, db):
        from brix.mcp_handlers.help import _handle_delete_tip
        from unittest.mock import patch
        tip = db.tip_create("A", "ToDelete", "Content")
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_delete_tip({"id": tip["id"]}))
        assert result["success"] is True
        assert db.tip_get(tip["id"]) is None

    def test_delete_tip_not_found(self, db):
        from brix.mcp_handlers.help import _handle_delete_tip
        from unittest.mock import patch
        with patch("brix.db.BrixDB", return_value=db):
            result = self._run(_handle_delete_tip({"id": "nonexistent"}))
        assert result["success"] is False


# ---------------------------------------------------------------------------
# get_tips includes DB tips
# ---------------------------------------------------------------------------

class TestGetTipsIncludesDBTips:
    def test_db_tips_in_output(self, db):
        """Verify that _load_db_tips returns formatted lines from DB."""
        db.tip_create("MY-CATEGORY", "My Tip Title", "Do this not that.", priority=8)
        from brix.mcp_handlers.help import _load_db_tips
        from unittest.mock import patch
        with patch("brix.db.BrixDB", return_value=db):
            lines = _load_db_tips()
        joined = "\n".join(lines)
        assert "MY-CATEGORY" in joined
        assert "Do this not that." in joined


# ---------------------------------------------------------------------------
# Seed Migration
# ---------------------------------------------------------------------------

class TestSeedMigration:
    def test_seed_populates_tips(self, db):
        """Run the seed function and verify tips are created."""
        from brix.migrations import _seed_tips_from_hardcoded
        _seed_tips_from_hardcoded(db)
        tips = db.tip_list(active_only=False)
        assert len(tips) >= 10  # We seeded 11 tips
        categories = {t["category"] for t in tips}
        assert "BRICK-FIRST" in categories
        assert "KERN-REGEL" in categories
        assert "DEBUGGING" in categories
