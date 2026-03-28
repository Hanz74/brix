"""Tests for T-BRIX-DB-08: seed-data.json export and JSON-file-based seeding.

Covers:
1.  seed-data.json exists in project root
2.  seed-data.json is valid JSON
3.  seed-data.json contains all required tables
4.  Each table in seed-data.json has at least 1 entry
5.  seed_if_empty reads from seed-data.json (primary path)
6.  All tables are filled after seeding from JSON
7.  Row counts match seed-data.json contents
8.  Seeder skips tables that already have data (idempotent)
9.  Fallback to code-import path when seed-data.json is absent
10. export_seed_data writes a valid JSON file with correct shape
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from brix.db import BrixDB
from brix.seed import seed_if_empty, _SEED_FILE
from brix.export_seed_data import export_seed_data

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

REQUIRED_TABLES = [
    "brick_definitions",
    "connector_definitions",
    "mcp_tool_schemas",
    "help_topics",
    "keyword_taxonomies",
    "type_compatibility",
]


@pytest.fixture
def fresh_db(tmp_path) -> BrixDB:
    """Return a BrixDB instance backed by a fresh temp file."""
    return BrixDB(db_path=tmp_path / "test_migration.db")


@pytest.fixture
def seed_data() -> dict:
    """Load the actual seed-data.json from the project root."""
    with open(_SEED_FILE, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 1. seed-data.json file existence
# ---------------------------------------------------------------------------

class TestSeedFileExists:
    def test_seed_file_exists(self):
        assert _SEED_FILE.exists(), f"seed-data.json not found at {_SEED_FILE}"

    def test_seed_file_is_file(self):
        assert _SEED_FILE.is_file()

    def test_seed_file_not_empty(self):
        assert _SEED_FILE.stat().st_size > 0


# ---------------------------------------------------------------------------
# 2. seed-data.json is valid JSON
# ---------------------------------------------------------------------------

class TestSeedFileJSON:
    def test_valid_json(self, seed_data):
        # If fixture loaded without exception, JSON is valid
        assert isinstance(seed_data, dict)

    def test_top_level_is_dict(self, seed_data):
        assert isinstance(seed_data, dict)


# ---------------------------------------------------------------------------
# 3. All required tables present
# ---------------------------------------------------------------------------

class TestSeedFileContents:
    def test_all_tables_present(self, seed_data):
        for table in REQUIRED_TABLES:
            assert table in seed_data, f"Missing table '{table}' in seed-data.json"

    def test_all_tables_are_lists(self, seed_data):
        for table in REQUIRED_TABLES:
            assert isinstance(seed_data[table], list), f"'{table}' is not a list"


# ---------------------------------------------------------------------------
# 4. Each table has entries
# ---------------------------------------------------------------------------

class TestSeedFileEntries:
    @pytest.mark.parametrize("table", REQUIRED_TABLES)
    def test_table_has_entries(self, seed_data, table):
        entries = seed_data[table]
        assert len(entries) > 0, f"Table '{table}' is empty in seed-data.json"

    def test_brick_definitions_minimum_count(self, seed_data):
        assert len(seed_data["brick_definitions"]) >= 10

    def test_mcp_tool_schemas_minimum_count(self, seed_data):
        assert len(seed_data["mcp_tool_schemas"]) >= 10

    def test_keyword_taxonomies_minimum_count(self, seed_data):
        assert len(seed_data["keyword_taxonomies"]) >= 50


# ---------------------------------------------------------------------------
# 5-7. seed_if_empty reads from seed-data.json and fills tables
# ---------------------------------------------------------------------------

class TestSeedFromFile:
    def test_seed_if_empty_returns_counts(self, fresh_db):
        counts = seed_if_empty(fresh_db)
        assert isinstance(counts, dict)
        for table in REQUIRED_TABLES:
            assert table in counts

    def test_seed_fills_brick_definitions(self, fresh_db, seed_data):
        seed_if_empty(fresh_db)
        count = fresh_db.brick_definitions_count()
        assert count == len(seed_data["brick_definitions"])

    def test_seed_fills_connector_definitions(self, fresh_db, seed_data):
        seed_if_empty(fresh_db)
        count = fresh_db.connector_definitions_count()
        assert count == len(seed_data["connector_definitions"])

    def test_seed_fills_mcp_tool_schemas(self, fresh_db, seed_data):
        seed_if_empty(fresh_db)
        count = fresh_db.mcp_tool_schemas_count()
        assert count == len(seed_data["mcp_tool_schemas"])

    def test_seed_fills_help_topics(self, fresh_db, seed_data):
        seed_if_empty(fresh_db)
        count = fresh_db.help_topics_count()
        assert count == len(seed_data["help_topics"])

    def test_seed_fills_keyword_taxonomies(self, fresh_db, seed_data):
        seed_if_empty(fresh_db)
        count = fresh_db.keyword_taxonomies_count()
        assert count == len(seed_data["keyword_taxonomies"])

    def test_seed_fills_type_compatibility(self, fresh_db, seed_data):
        seed_if_empty(fresh_db)
        count = fresh_db.type_compatibility_count()
        assert count == len(seed_data["type_compatibility"])

    def test_seed_counts_match_seed_data(self, fresh_db, seed_data):
        counts = seed_if_empty(fresh_db)
        for table in REQUIRED_TABLES:
            assert counts[table] == len(seed_data[table]), (
                f"{table}: seeded {counts[table]} but seed-data.json has {len(seed_data[table])}"
            )


# ---------------------------------------------------------------------------
# 8. Idempotency — seeder skips non-empty tables
# ---------------------------------------------------------------------------

class TestSeedIdempotency:
    def test_second_seed_returns_zeros(self, fresh_db):
        seed_if_empty(fresh_db)
        counts2 = seed_if_empty(fresh_db)
        for table in REQUIRED_TABLES:
            assert counts2[table] == 0, (
                f"Second seed should skip '{table}' (already populated), got {counts2[table]}"
            )

    def test_counts_unchanged_after_second_seed(self, fresh_db, seed_data):
        seed_if_empty(fresh_db)
        count_after_first = fresh_db.brick_definitions_count()
        seed_if_empty(fresh_db)
        count_after_second = fresh_db.brick_definitions_count()
        assert count_after_first == count_after_second


# ---------------------------------------------------------------------------
# 9. Fallback to code imports when seed-data.json absent
# ---------------------------------------------------------------------------

class TestSeedFallback:
    def test_fallback_when_no_seed_file(self, fresh_db, tmp_path):
        """When seed-data.json is absent, _seed_from_code should be called."""
        missing_path = tmp_path / "nonexistent-seed.json"
        with patch("brix.seed._SEED_FILE", missing_path):
            counts = seed_if_empty(fresh_db)
        # Code fallback should still populate tables
        assert isinstance(counts, dict)
        assert sum(counts.values()) > 0


# ---------------------------------------------------------------------------
# 10. export_seed_data writes valid JSON
# ---------------------------------------------------------------------------

class TestExportSeedData:
    def test_export_creates_file(self, tmp_path):
        out = tmp_path / "export_test.json"
        export_seed_data(str(out))
        assert out.exists()

    def test_export_valid_json(self, tmp_path):
        out = tmp_path / "export_test.json"
        export_seed_data(str(out))
        with open(out) as f:
            data = json.load(f)
        assert isinstance(data, dict)

    def test_export_all_tables_present(self, tmp_path):
        out = tmp_path / "export_test.json"
        export_seed_data(str(out))
        with open(out) as f:
            data = json.load(f)
        for table in REQUIRED_TABLES:
            assert table in data, f"Exported JSON missing table '{table}'"

    def test_export_no_timestamps(self, tmp_path):
        out = tmp_path / "export_test.json"
        export_seed_data(str(out))
        with open(out) as f:
            data = json.load(f)
        for table, records in data.items():
            for rec in records:
                assert "created_at" not in rec, f"{table} record has 'created_at' in export"
                assert "updated_at" not in rec, f"{table} record has 'updated_at' in export"
