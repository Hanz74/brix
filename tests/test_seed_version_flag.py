"""Tests for T-BRIX-DBF-03: seed-data.json one-time import via version flag.

Covers:
1. First seed sets the version flag in persistent_store
2. Second call skips entirely (returns early, no JSON parsing)
3. Clearing the flag allows re-seeding
"""
from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.seed import seed_if_empty, _SEED_VERSION_KEY, _get_brix_version


@pytest.fixture
def fresh_db(tmp_path) -> BrixDB:
    """Return a BrixDB instance backed by a fresh temp file."""
    return BrixDB(db_path=tmp_path / "test_seed_version.db")


class TestSeedVersionFlag:
    def test_first_seed_sets_version_flag(self, fresh_db):
        """After the first seed, the version flag is stored in persistent_store."""
        seed_if_empty(fresh_db)
        stored = fresh_db.store_get(_SEED_VERSION_KEY)
        assert stored is not None
        assert stored == _get_brix_version()

    def test_second_call_skips_entirely(self, fresh_db):
        """Second call returns early with skipped=True and seed_version."""
        seed_if_empty(fresh_db)
        result = seed_if_empty(fresh_db)
        assert result["skipped"] is True
        assert result["seed_version"] == _get_brix_version()

    def test_second_call_does_not_touch_tables(self, fresh_db):
        """Second call does not re-count tables or touch any seed data."""
        seed_if_empty(fresh_db)
        brick_count = fresh_db.brick_definitions_count()
        assert brick_count > 0  # first seed populated bricks

        result = seed_if_empty(fresh_db)
        assert result["skipped"] is True
        # Bricks are still there, untouched
        assert fresh_db.brick_definitions_count() == brick_count

    def test_clearing_flag_allows_reseed(self, fresh_db):
        """Deleting the version flag allows seed_if_empty to run again."""
        seed_if_empty(fresh_db)
        assert fresh_db.store_get(_SEED_VERSION_KEY) is not None

        # Clear the flag
        fresh_db.store_delete(_SEED_VERSION_KEY)
        assert fresh_db.store_get(_SEED_VERSION_KEY) is None

        # Seed should run again (tables already populated, so individual
        # table counts will be 0, but the function runs through and sets
        # the flag again)
        result = seed_if_empty(fresh_db)
        assert result.get("skipped") is not True
        assert fresh_db.store_get(_SEED_VERSION_KEY) == _get_brix_version()

    def test_version_matches_brix_version(self, fresh_db):
        """The stored version matches the current Brix __version__."""
        from brix import __version__
        seed_if_empty(fresh_db)
        assert fresh_db.store_get(_SEED_VERSION_KEY) == __version__
