"""Tests for T-BRIX-DBF-01: seed.py only seeds empty tables, never overwrites.

Covers:
1. Seed into empty DB -> data appears
2. Seed into non-empty DB -> existing data NOT overwritten
3. Manual edits via direct DB writes survive a second seed call
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from brix.db import BrixDB
from brix.seed import (
    _seed_bricks_from_data,
    _seed_connectors_from_data,
    _seed_tools_from_data,
    _seed_help_from_data,
    _seed_keywords_from_data,
    _seed_types_from_data,
    _SEED_FILE,
    seed_if_empty,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db(tmp_path) -> BrixDB:
    """Return a BrixDB instance backed by a fresh temp file."""
    return BrixDB(db_path=tmp_path / "test_seed_skip.db")


@pytest.fixture
def seed_data() -> dict:
    """Load the actual seed-data.json from the project root."""
    with open(_SEED_FILE, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 1. Seed into empty DB -> data appears
# ---------------------------------------------------------------------------

class TestSeedEmptyDB:
    def test_bricks_seeded_into_empty(self, fresh_db, seed_data):
        records = seed_data.get("brick_definitions", [])
        result = _seed_bricks_from_data(fresh_db, records)
        assert result == len(records)
        assert fresh_db.brick_definitions_count() == len(records)

    def test_connectors_seeded_into_empty(self, fresh_db, seed_data):
        records = seed_data.get("connector_definitions", [])
        result = _seed_connectors_from_data(fresh_db, records)
        assert result == len(records)
        assert fresh_db.connector_definitions_count() == len(records)

    def test_tools_seeded_into_empty(self, fresh_db, seed_data):
        records = seed_data.get("mcp_tool_schemas", [])
        # Note: migrations may pre-populate mcp_tool_schema with system entries
        # (e.g. brix__get_app_log), so if the table is already non-empty the
        # seed function correctly skips. We test both scenarios.
        pre_count = fresh_db.mcp_tool_schemas_count()
        result = _seed_tools_from_data(fresh_db, records)
        if pre_count > 0:
            # Table was pre-populated by migrations -> seed correctly skips
            assert result == 0
        else:
            assert result == len(records)
            assert fresh_db.mcp_tool_schemas_count() == len(records)

    def test_help_seeded_into_empty(self, fresh_db, seed_data):
        records = seed_data.get("help_topics", [])
        result = _seed_help_from_data(fresh_db, records)
        assert result == len(records)
        assert fresh_db.help_topics_count() == len(records)

    def test_keywords_seeded_into_empty(self, fresh_db, seed_data):
        records = seed_data.get("keyword_taxonomies", [])
        result = _seed_keywords_from_data(fresh_db, records)
        assert result == len(records)
        assert fresh_db.keyword_taxonomies_count() == len(records)

    def test_types_seeded_into_empty(self, fresh_db, seed_data):
        records = seed_data.get("type_compatibility", [])
        result = _seed_types_from_data(fresh_db, records)
        assert result == len(records)
        assert fresh_db.type_compatibility_count() == len(records)


# ---------------------------------------------------------------------------
# 2. Seed into non-empty DB -> existing data NOT overwritten
# ---------------------------------------------------------------------------

class TestSeedSkipsNonEmpty:
    def test_bricks_skipped_when_populated(self, fresh_db, seed_data):
        records = seed_data.get("brick_definitions", [])
        _seed_bricks_from_data(fresh_db, records)
        count_before = fresh_db.brick_definitions_count()

        result = _seed_bricks_from_data(fresh_db, records)
        assert result == 0
        assert fresh_db.brick_definitions_count() == count_before

    def test_connectors_skipped_when_populated(self, fresh_db, seed_data):
        records = seed_data.get("connector_definitions", [])
        _seed_connectors_from_data(fresh_db, records)
        count_before = fresh_db.connector_definitions_count()

        result = _seed_connectors_from_data(fresh_db, records)
        assert result == 0
        assert fresh_db.connector_definitions_count() == count_before

    def test_tools_skipped_when_populated(self, fresh_db, seed_data):
        records = seed_data.get("mcp_tool_schemas", [])
        _seed_tools_from_data(fresh_db, records)
        count_before = fresh_db.mcp_tool_schemas_count()

        result = _seed_tools_from_data(fresh_db, records)
        assert result == 0
        assert fresh_db.mcp_tool_schemas_count() == count_before

    def test_help_skipped_when_populated(self, fresh_db, seed_data):
        records = seed_data.get("help_topics", [])
        _seed_help_from_data(fresh_db, records)
        count_before = fresh_db.help_topics_count()

        result = _seed_help_from_data(fresh_db, records)
        assert result == 0
        assert fresh_db.help_topics_count() == count_before

    def test_keywords_skipped_when_populated(self, fresh_db, seed_data):
        records = seed_data.get("keyword_taxonomies", [])
        _seed_keywords_from_data(fresh_db, records)
        count_before = fresh_db.keyword_taxonomies_count()

        result = _seed_keywords_from_data(fresh_db, records)
        assert result == 0
        assert fresh_db.keyword_taxonomies_count() == count_before

    def test_types_skipped_when_populated(self, fresh_db, seed_data):
        records = seed_data.get("type_compatibility", [])
        _seed_types_from_data(fresh_db, records)
        count_before = fresh_db.type_compatibility_count()

        result = _seed_types_from_data(fresh_db, records)
        assert result == 0
        assert fresh_db.type_compatibility_count() == count_before

    def test_skip_logs_message(self, fresh_db, seed_data, caplog):
        """Verify that skipping a non-empty table produces a log message."""
        records = seed_data.get("brick_definitions", [])
        _seed_bricks_from_data(fresh_db, records)

        with caplog.at_level(logging.INFO, logger="brix.seed"):
            _seed_bricks_from_data(fresh_db, records)

        assert any("brick_definition already has" in msg for msg in caplog.messages)

    def test_full_seed_if_empty_idempotent(self, fresh_db):
        """Full seed_if_empty called twice returns zeros on second call."""
        seed_if_empty(fresh_db)
        counts2 = seed_if_empty(fresh_db)
        for table in [
            "brick_definitions",
            "connector_definitions",
            "mcp_tool_schemas",
            "help_topics",
            "keyword_taxonomies",
            "type_compatibility",
        ]:
            assert counts2[table] == 0, (
                f"Second seed should skip '{table}', got {counts2[table]}"
            )


# ---------------------------------------------------------------------------
# 3. Manual edits survive a rebuild (simulated by second seed call)
# ---------------------------------------------------------------------------

class TestManualEditsPreserved:
    def test_modified_brick_description_survives_seed(self, fresh_db, seed_data):
        """Simulate: user edits a brick description via MCP, then container restarts."""
        records = seed_data.get("brick_definitions", [])
        _seed_bricks_from_data(fresh_db, records)

        # Simulate manual edit via direct DB update
        with fresh_db._connect() as conn:
            conn.execute(
                "UPDATE brick_definition SET description='CUSTOM DESCRIPTION' WHERE rowid=1"
            )

        # Verify custom description is in place
        with fresh_db._connect() as conn:
            row = conn.execute(
                "SELECT description FROM brick_definition WHERE rowid=1"
            ).fetchone()
            assert row[0] == "CUSTOM DESCRIPTION"

        # Second seed call (simulates container restart) should skip entirely
        result = _seed_bricks_from_data(fresh_db, records)
        assert result == 0

        # Verify custom description is still intact
        with fresh_db._connect() as conn:
            row = conn.execute(
                "SELECT description FROM brick_definition WHERE rowid=1"
            ).fetchone()
            assert row[0] == "CUSTOM DESCRIPTION"

    def test_modified_help_topic_survives_seed(self, fresh_db, seed_data):
        """Simulate: user edits a help topic, then container restarts."""
        records = seed_data.get("help_topics", [])
        _seed_help_from_data(fresh_db, records)

        # Simulate manual edit
        with fresh_db._connect() as conn:
            conn.execute(
                "UPDATE help_topic SET content='CUSTOM CONTENT' WHERE rowid=1"
            )

        # Second seed should skip
        result = _seed_help_from_data(fresh_db, records)
        assert result == 0

        # Custom content preserved
        with fresh_db._connect() as conn:
            row = conn.execute(
                "SELECT content FROM help_topic WHERE rowid=1"
            ).fetchone()
            assert row[0] == "CUSTOM CONTENT"

    def test_modified_tool_schema_survives_seed(self, fresh_db, seed_data):
        """Simulate: user edits a tool schema description, then container restarts."""
        records = seed_data.get("mcp_tool_schemas", [])
        _seed_tools_from_data(fresh_db, records)

        # Simulate manual edit
        with fresh_db._connect() as conn:
            conn.execute(
                "UPDATE mcp_tool_schema SET description='CUSTOM TOOL DESC' WHERE rowid=1"
            )

        # Second seed should skip
        result = _seed_tools_from_data(fresh_db, records)
        assert result == 0

        # Custom description preserved
        with fresh_db._connect() as conn:
            row = conn.execute(
                "SELECT description FROM mcp_tool_schema WHERE rowid=1"
            ).fetchone()
            assert row[0] == "CUSTOM TOOL DESC"
