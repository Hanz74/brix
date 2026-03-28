"""Tests for Pipeline-Idempotency (T-BRIX-V6-22).

Covers:
- Pipeline model accepts idempotency_key field
- RunHistory.find_by_idempotency_key returns correct rows
- history.record_start stores idempotency_key in DB
- Engine short-circuits when matching key found in last 24h
- Engine runs normally when no matching key found
- Engine skips idempotency check when key is absent
- Expired runs (> 24h) are not matched
- Failed runs are not matched (only success=1)
- Jinja2 expressions in idempotency_key are evaluated against input
"""

import json
import pytest
import asyncio
from datetime import datetime, timezone, timedelta

from brix.history import RunHistory
from brix.loader import PipelineLoader
from brix.models import Pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_pipeline(yaml_str: str) -> Pipeline:
    return PipelineLoader().load_from_string(yaml_str)


def _store_finished_run(
    history: RunHistory,
    run_id: str,
    pipeline: str = "test-pipeline",
    success: bool = True,
    idempotency_key: str = None,
    result: dict = None,
    hours_ago: float = 0,
):
    """Insert a finished run into history, optionally backdating started_at."""
    history.record_start(
        run_id=run_id,
        pipeline=pipeline,
        version="1.0.0",
        input_data={},
        idempotency_key=idempotency_key,
    )
    history.record_finish(
        run_id=run_id,
        success=success,
        duration=1.0,
        result_summary=result or {"status": "done"},
    )
    if hours_ago:
        # Backdate started_at and finished_at so expiry logic can be tested
        past = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()
        import sqlite3
        with sqlite3.connect(str(history.db_path)) as conn:
            conn.execute(
                "UPDATE runs SET started_at=?, finished_at=? WHERE run_id=?",
                (past, past, run_id),
            )


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_pipeline_model_accepts_idempotency_key():
    pipeline = load_pipeline("""
name: test-idempotency
idempotency_key: "{{ input.date }}"
steps:
  - id: step1
    type: cli
    args: ["echo", "hi"]
""")
    assert pipeline.idempotency_key == "{{ input.date }}"


def test_pipeline_model_idempotency_key_defaults_to_none():
    pipeline = load_pipeline("""
name: no-idempotency
steps:
  - id: step1
    type: cli
    args: ["echo", "hi"]
""")
    assert pipeline.idempotency_key is None


# ---------------------------------------------------------------------------
# RunHistory tests
# ---------------------------------------------------------------------------


def test_find_by_idempotency_key_returns_existing(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    _store_finished_run(h, "run-001", idempotency_key="sync-2025-01-01")

    result = h.find_by_idempotency_key("sync-2025-01-01")
    assert result is not None
    assert result["run_id"] == "run-001"


def test_find_by_idempotency_key_returns_none_when_missing(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    result = h.find_by_idempotency_key("nonexistent-key")
    assert result is None


def test_find_by_idempotency_key_ignores_failed_runs(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    _store_finished_run(h, "run-fail", idempotency_key="key-fail", success=False)

    result = h.find_by_idempotency_key("key-fail")
    assert result is None


def test_find_by_idempotency_key_ignores_expired_runs(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    # Store a run started 25 hours ago — outside the 24h window
    _store_finished_run(h, "run-old", idempotency_key="key-old", hours_ago=25)

    result = h.find_by_idempotency_key("key-old", within_hours=24)
    assert result is None


def test_find_by_idempotency_key_accepts_recent_runs(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    # Store a run started 23 hours ago — inside the 24h window
    _store_finished_run(h, "run-recent", idempotency_key="key-recent", hours_ago=23)

    result = h.find_by_idempotency_key("key-recent", within_hours=24)
    assert result is not None
    assert result["run_id"] == "run-recent"


def test_find_by_idempotency_key_returns_most_recent(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    _store_finished_run(h, "run-a", idempotency_key="shared-key", hours_ago=10)
    _store_finished_run(h, "run-b", idempotency_key="shared-key", hours_ago=5)
    _store_finished_run(h, "run-c", idempotency_key="shared-key", hours_ago=1)

    result = h.find_by_idempotency_key("shared-key")
    assert result is not None
    assert result["run_id"] == "run-c"


def test_record_start_stores_idempotency_key(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("run-x", "pipeline", idempotency_key="stored-key")
    h.record_finish("run-x", True, 1.0)

    run = h.get_run("run-x")
    assert run is not None
    assert run.get("idempotency_key") == "stored-key"


def test_record_start_without_idempotency_key_stores_none(tmp_path):
    h = RunHistory(db_path=tmp_path / "test.db")
    h.record_start("run-y", "pipeline")
    h.record_finish("run-y", True, 1.0)

    run = h.get_run("run-y")
    assert run is not None
    assert run.get("idempotency_key") is None


# ---------------------------------------------------------------------------
# Engine integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_short_circuits_on_matching_key(tmp_path):
    """Engine returns existing run_id when idempotency key matches recent run."""
    from brix.engine import PipelineEngine
    from brix.history import RunHistory
    import brix.history as _hist_mod

    # Redirect history to tmp DB
    original = _hist_mod.HISTORY_DB_PATH
    _hist_mod.HISTORY_DB_PATH = tmp_path / "brix.db"
    try:
        h = RunHistory(db_path=tmp_path / "brix.db")
        _store_finished_run(
            h, "prev-run-001", pipeline="idempotent-pipe",
            idempotency_key="daily-sync-2025-01-01",
            result={"imported": 42},
        )

        pipeline = load_pipeline("""
name: idempotent-pipe
idempotency_key: "daily-sync-{{ input.date }}"
input:
  date:
    type: string
steps:
  - id: step1
    type: cli
    args: ["echo", "should-not-run"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline, user_input={"date": "2025-01-01"})

        assert result.success is True
        assert result.run_id == "prev-run-001"
        # step1 was not executed — steps dict is empty
        assert "step1" not in result.steps
    finally:
        _hist_mod.HISTORY_DB_PATH = original


@pytest.mark.asyncio
async def test_engine_runs_normally_when_no_matching_key(tmp_path):
    """Engine executes pipeline normally when no idempotency hit."""
    from brix.engine import PipelineEngine
    import brix.history as _hist_mod

    original = _hist_mod.HISTORY_DB_PATH
    _hist_mod.HISTORY_DB_PATH = tmp_path / "brix.db"
    try:
        pipeline = load_pipeline("""
name: idempotent-pipe
idempotency_key: "daily-sync-{{ input.date }}"
input:
  date:
    type: string
steps:
  - id: greet
    type: cli
    args: ["echo", "hello"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline, user_input={"date": "2025-01-02"})

        assert result.success is True
        assert result.steps["greet"].status == "ok"
    finally:
        _hist_mod.HISTORY_DB_PATH = original


@pytest.mark.asyncio
async def test_engine_skips_check_when_no_idempotency_key():
    """Engine runs normally when pipeline has no idempotency_key."""
    from brix.engine import PipelineEngine

    pipeline = load_pipeline("""
name: normal-pipe
steps:
  - id: echo
    type: cli
    args: ["echo", "hi"]
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["echo"].status == "ok"


@pytest.mark.asyncio
async def test_engine_does_not_match_failed_runs(tmp_path):
    """Failed runs are not returned for idempotency short-circuit."""
    from brix.engine import PipelineEngine
    from brix.history import RunHistory
    import brix.history as _hist_mod

    original = _hist_mod.HISTORY_DB_PATH
    _hist_mod.HISTORY_DB_PATH = tmp_path / "brix.db"
    try:
        h = RunHistory(db_path=tmp_path / "brix.db")
        _store_finished_run(
            h, "failed-run", pipeline="test-pipe",
            idempotency_key="key-with-failure",
            success=False,
        )

        pipeline = load_pipeline("""
name: test-pipe
idempotency_key: "key-with-failure"
steps:
  - id: greet
    type: cli
    args: ["echo", "should-run"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.success is True
        assert result.run_id != "failed-run"
        assert result.steps["greet"].status == "ok"
    finally:
        _hist_mod.HISTORY_DB_PATH = original


@pytest.mark.asyncio
async def test_engine_stores_idempotency_key_on_new_run(tmp_path):
    """After a successful run, the resolved key is stored in history."""
    from brix.engine import PipelineEngine
    from brix.history import RunHistory
    import brix.history as _hist_mod

    original = _hist_mod.HISTORY_DB_PATH
    _hist_mod.HISTORY_DB_PATH = tmp_path / "brix.db"
    try:
        pipeline = load_pipeline("""
name: store-key-pipe
idempotency_key: "fixed-key-123"
steps:
  - id: greet
    type: cli
    args: ["echo", "hello"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        assert result.success is True

        h = RunHistory(db_path=tmp_path / "brix.db")
        run = h.get_run(result.run_id)
        assert run is not None
        assert run.get("idempotency_key") == "fixed-key-123"
    finally:
        _hist_mod.HISTORY_DB_PATH = original
