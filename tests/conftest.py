"""Shared test fixtures for brix tests."""

import os
import tempfile

import pytest

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore


# ---------------------------------------------------------------------------
# Session-wide DB isolation: ALL tests use a temporary database by default.
# This prevents test-created pipelines/helpers/runs from polluting the
# production brix.db.
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def _isolate_db_session():
    """Redirect BRIX_DB_PATH to a temp file for the entire test session.

    Sets the ``BRIX_DB_PATH`` env var **and** patches the module-level
    constant in ``brix.db`` so that every ``BrixDB()`` call (with no
    explicit ``db_path``) lands in the temporary database.  Schema
    migrations are applied automatically by ``BrixDB.__init__``.
    """
    with tempfile.TemporaryDirectory(prefix="brix_test_") as td:
        test_db = os.path.join(td, "brix_test.db")
        old_env = os.environ.get("BRIX_DB_PATH")
        os.environ["BRIX_DB_PATH"] = test_db

        import brix.db as db_mod
        original_path = db_mod.BRIX_DB_PATH
        from pathlib import Path
        db_mod.BRIX_DB_PATH = Path(test_db)

        # Also patch history.py which caches a copy of the path
        try:
            import brix.history as hist_mod
            hist_mod.HISTORY_DB_PATH = Path(test_db)
        except (ImportError, AttributeError):
            pass

        # Ensure schema is initialised in the temp DB
        BrixDB(db_path=Path(test_db))

        yield test_db

        # Restore
        db_mod.BRIX_DB_PATH = original_path
        if old_env is None:
            os.environ.pop("BRIX_DB_PATH", None)
        else:
            os.environ["BRIX_DB_PATH"] = old_env


@pytest.fixture
def isolated_db(tmp_path):
    """Return a BrixDB backed by a temporary database file."""
    return BrixDB(db_path=tmp_path / "test_isolated.db")


@pytest.fixture
def isolated_store(tmp_path, isolated_db):
    """Return a PipelineStore with isolated DB and filesystem."""
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
