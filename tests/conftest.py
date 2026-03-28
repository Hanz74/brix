"""Shared test fixtures for brix tests."""

import pytest

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore


@pytest.fixture
def isolated_db(tmp_path):
    """Return a BrixDB backed by a temporary database file."""
    return BrixDB(db_path=tmp_path / "test_isolated.db")


@pytest.fixture
def isolated_store(tmp_path, isolated_db):
    """Return a PipelineStore with isolated DB and filesystem."""
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
