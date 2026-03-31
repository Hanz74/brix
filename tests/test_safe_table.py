"""Tests for _safe_table defence-in-depth guard (T-BRIX-SEC-01).

Covers:
- Known tables pass validation
- Unknown / malicious table names are rejected
- REGISTRY_TYPES values all pass validation
- _KNOWN_TABLES is derived from _DDL (not hardcoded separately)
- Registry queries still work end-to-end through _safe_table
"""

import re

import pytest

from brix.db import (
    BrixDB,
    REGISTRY_TYPES,
    _KNOWN_TABLES,
    _safe_table,
    _DDL,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Return a BrixDB backed by a temporary file."""
    return BrixDB(db_path=tmp_path / "brix.db")


# ---------------------------------------------------------------------------
# _safe_table unit tests
# ---------------------------------------------------------------------------

class TestSafeTable:
    """Unit tests for _safe_table()."""

    def test_known_tables_pass(self):
        """Every table defined in _DDL passes validation."""
        for table in _KNOWN_TABLES:
            assert _safe_table(table) == table

    def test_unknown_table_raises(self):
        """An unknown table name raises ValueError."""
        with pytest.raises(ValueError, match="Unknown table"):
            _safe_table("nonexistent_table")

    def test_sql_injection_rejected(self):
        """SQL injection attempts are rejected."""
        malicious_names = [
            "runs; DROP TABLE runs --",
            "runs UNION SELECT * FROM helpers",
            "' OR 1=1 --",
            "runs\nDROP TABLE helpers",
            "",
        ]
        for name in malicious_names:
            with pytest.raises(ValueError, match="Unknown table"):
                _safe_table(name)

    def test_registry_type_values_all_valid(self):
        """All REGISTRY_TYPES values are in _KNOWN_TABLES."""
        for rtype, table in REGISTRY_TYPES.items():
            assert table in _KNOWN_TABLES, (
                f"REGISTRY_TYPES['{rtype}'] = '{table}' not in _KNOWN_TABLES"
            )

    def test_allowlist_derived_from_ddl(self):
        """_KNOWN_TABLES is derived from _DDL, not separately hardcoded."""
        expected = frozenset(
            re.findall(
                r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)",
                " ".join(_DDL),
            )
        )
        assert _KNOWN_TABLES == expected

    def test_known_tables_not_empty(self):
        """Sanity: _KNOWN_TABLES has a reasonable number of tables."""
        assert len(_KNOWN_TABLES) >= 20


# ---------------------------------------------------------------------------
# Integration: registry queries through _safe_table
# ---------------------------------------------------------------------------

class TestRegistryThroughSafeTable:
    """Registry operations still work with _safe_table in the path."""

    def test_registry_add_and_get(self, db):
        """registry_add + registry_get round-trip works."""
        entry_id = db.registry_add(
            "templates", "test-tpl", {"foo": "bar"}, tags=["test"]
        )
        entry = db.registry_get("templates", "test-tpl")
        assert entry is not None
        assert entry["id"] == entry_id
        assert entry["name"] == "test-tpl"

    def test_registry_list(self, db):
        """registry_list works."""
        db.registry_add("patterns", "p1", "content1")
        db.registry_add("patterns", "p2", "content2")
        entries = db.registry_list("patterns")
        assert len(entries) == 2

    def test_registry_update(self, db):
        """registry_update works."""
        db.registry_add("schemas", "s1", {"v": 1})
        updated = db.registry_update("schemas", "s1", content={"v": 2})
        assert updated is not None
        assert updated["content"] == {"v": 2}

    def test_registry_delete(self, db):
        """registry_delete works."""
        db.registry_add("error_patterns", "ep1", "err")
        assert db.registry_delete("error_patterns", "ep1") is True
        assert db.registry_get("error_patterns", "ep1") is None

    def test_registry_search(self, db):
        """registry_search works across types."""
        db.registry_add("best_practices", "bp1", "always test", tags=["testing"])
        results = db.registry_search("test")
        assert len(results) >= 1

    def test_invalid_registry_type_rejected(self, db):
        """Unknown registry type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown registry_type"):
            db.registry_add("fake_type", "x", "y")
