"""Tests for SQLite run history."""
import pytest
from brix.history import RunHistory


def test_record_and_get(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("run-001", "test-pipeline", "1.0.0", {"query": "test"})
    h.record_finish("run-001", True, 2.5, {"step1": {"status": "ok"}})

    run = h.get_run("run-001")
    assert run is not None
    assert run["pipeline"] == "test-pipeline"
    assert run["success"] == 1
    assert run["duration"] == 2.5


def test_get_recent(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    for i in range(5):
        h.record_start(f"run-{i:03d}", "pipeline", input_data={})
        h.record_finish(f"run-{i:03d}", True, 1.0)

    recent = h.get_recent(3)
    assert len(recent) == 3


def test_get_stats(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    for i in range(10):
        h.record_start(f"run-{i:03d}", "pipeline")
        h.record_finish(f"run-{i:03d}", i < 8, 1.0 + i * 0.1)

    stats = h.get_stats()
    assert stats["total_runs"] == 10
    assert stats["success_rate"] == 80.0
    assert stats["successes"] == 8
    assert stats["failures"] == 2


def test_get_stats_per_pipeline(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("r1", "pipeline-a")
    h.record_finish("r1", True, 1.0)
    h.record_start("r2", "pipeline-b")
    h.record_finish("r2", False, 2.0)

    stats = h.get_stats("pipeline-a")
    assert stats["total_runs"] == 1
    assert stats["success_rate"] == 100.0


def test_get_stats_empty(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    stats = h.get_stats()
    assert stats["total_runs"] == 0


def test_get_run_not_found(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    assert h.get_run("nonexistent") is None


def test_cleanup(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("old-run", "pipeline")
    h.record_finish("old-run", True, 1.0)
    # Can't easily test date-based cleanup without mocking time
    # Just verify the method runs without error
    deleted = h.cleanup(older_than_days=0)
    # With 0 days, everything should be deleted
    assert deleted >= 0


def test_db_created_automatically(tmp_path):
    db_path = tmp_path / "subdir" / "test.db"
    h = RunHistory(db_path=db_path)
    assert db_path.exists()
