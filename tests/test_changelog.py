"""Tests for changelog DB table + MCP tool — T-BRIX-CHANGELOG-01."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from brix.db import BrixDB


@pytest.fixture
def db(tmp_path: Path) -> BrixDB:
    """Return a BrixDB backed by a temporary database file."""
    return BrixDB(db_path=tmp_path / "test_changelog.db")


class TestChangelogCRUD:
    """Test add + list roundtrip for changelog entries."""

    def test_add_and_list_roundtrip(self, db: BrixDB) -> None:
        """Adding entries and listing them returns the correct data."""
        db.add_changelog_entry(
            version="10.1.0",
            type="feature",
            title="Add changelog support",
            description="New table and MCP tool",
            task_id="T-BRIX-CHANGELOG-01",
            commit_sha="abc123",
        )
        db.add_changelog_entry(
            version="10.1.0",
            type="fix",
            title="Fix migration order",
            commit_sha="def456",
        )
        db.add_changelog_entry(
            version="10.0.0",
            type="breaking",
            title="Remove legacy YAML loader",
        )

        entries = db.list_changelog()
        assert len(entries) == 3
        # Ordered by version DESC, timestamp DESC
        assert entries[0]["version"] == "10.1.0"
        assert entries[0]["type"] in ("feature", "fix")
        assert entries[2]["version"] == "10.0.0"

    def test_list_with_since_filter(self, db: BrixDB) -> None:
        """The since filter excludes older versions."""
        db.add_changelog_entry(version="9.0.0", type="feature", title="Old feature")
        db.add_changelog_entry(version="10.0.0", type="feature", title="New feature")
        db.add_changelog_entry(version="10.1.0", type="fix", title="Latest fix")

        entries = db.list_changelog(since="10.0.0")
        versions = {e["version"] for e in entries}
        assert "9.0.0" not in versions
        assert "10.0.0" in versions
        assert "10.1.0" in versions

    def test_list_with_type_filter(self, db: BrixDB) -> None:
        """The type filter only returns matching entries."""
        db.add_changelog_entry(version="10.0.0", type="feature", title="A feature")
        db.add_changelog_entry(version="10.0.0", type="fix", title="A fix")
        db.add_changelog_entry(version="10.0.0", type="breaking", title="A break")

        entries = db.list_changelog(type="fix")
        assert len(entries) == 1
        assert entries[0]["type"] == "fix"
        assert entries[0]["title"] == "A fix"

    def test_list_with_limit(self, db: BrixDB) -> None:
        """The limit parameter caps the result count."""
        for i in range(10):
            db.add_changelog_entry(version="10.0.0", type="fix", title=f"Fix #{i}")

        entries = db.list_changelog(limit=3)
        assert len(entries) == 3

    def test_invalid_type_raises(self, db: BrixDB) -> None:
        """An invalid type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid changelog type"):
            db.add_changelog_entry(version="1.0.0", type="invalid", title="Bad")

    def test_entry_has_all_fields(self, db: BrixDB) -> None:
        """The returned dict contains all expected fields."""
        result = db.add_changelog_entry(
            version="10.2.0",
            type="docs",
            title="Update README",
            description="Added examples",
            task_id="T-BRIX-DOCS-01",
            commit_sha="aaa111",
        )
        assert result["id"]
        assert result["version"] == "10.2.0"
        assert result["type"] == "docs"
        assert result["title"] == "Update README"
        assert result["description"] == "Added examples"
        assert result["task_id"] == "T-BRIX-DOCS-01"
        assert result["commit_sha"] == "aaa111"
        assert result["created_at"]


class TestChangelogMCPHandler:
    """Test the MCP handler for brix__changelog."""

    def test_handler_groups_by_version(self, db: BrixDB, monkeypatch) -> None:
        """The handler groups entries by version, sorted descending."""
        db.add_changelog_entry(version="10.0.0", type="feature", title="Feat A")
        db.add_changelog_entry(version="10.0.0", type="fix", title="Fix B")
        db.add_changelog_entry(version="10.1.0", type="breaking", title="Break C")

        # Monkeypatch BrixDB() to return our test db
        import brix.mcp_handlers.changelog as changelog_mod
        monkeypatch.setattr(changelog_mod, "BrixDB", lambda: db)

        from brix.mcp_handlers.changelog import _handle_changelog
        result = asyncio.get_event_loop().run_until_complete(
            _handle_changelog({})
        )

        assert result["total_entries"] == 3
        versions = result["versions"]
        assert len(versions) == 2
        # First version should be 10.1.0 (descending)
        assert versions[0]["version"] == "10.1.0"
        assert len(versions[0]["entries"]) == 1
        assert versions[1]["version"] == "10.0.0"
        assert len(versions[1]["entries"]) == 2

    def test_handler_with_filters(self, db: BrixDB, monkeypatch) -> None:
        """The handler passes filters through correctly."""
        db.add_changelog_entry(version="9.0.0", type="feature", title="Old")
        db.add_changelog_entry(version="10.0.0", type="fix", title="New fix")

        import brix.mcp_handlers.changelog as changelog_mod
        monkeypatch.setattr(changelog_mod, "BrixDB", lambda: db)

        from brix.mcp_handlers.changelog import _handle_changelog
        result = asyncio.get_event_loop().run_until_complete(
            _handle_changelog({"since": "10.0.0", "type": "fix"})
        )

        assert result["total_entries"] == 1
        assert result["versions"][0]["version"] == "10.0.0"
