"""Tests for Pipeline Testing: step pins, mock data, and test_mode (T-BRIX-DB-24)."""
from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

import pytest
import yaml

from brix.db import BrixDB
from brix.models import Pipeline


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_db(tmp_path) -> BrixDB:
    """Return a BrixDB backed by a temp file."""
    return BrixDB(db_path=tmp_path / "test.db")


@pytest.fixture
def pipeline_yaml_path(tmp_path) -> Path:
    """Write a simple two-step pipeline to a temp dir and return its path."""
    pl = {
        "name": "test-pipeline",
        "steps": [
            {
                "id": "fetch",
                "type": "python",
                "script": "/dev/null",
            },
            {
                "id": "process",
                "type": "python",
                "script": "/dev/null",
            },
        ],
    }
    p = tmp_path / "test-pipeline.yaml"
    p.write_text(yaml.dump(pl))
    return p


# ---------------------------------------------------------------------------
# BrixDB — step_pins table
# ---------------------------------------------------------------------------


def test_pin_step_creates_record(tmp_db):
    """pin_step stores data and returns a dict with expected keys."""
    record = tmp_db.pin_step(
        pipeline_name="my-pipeline",
        step_id="fetch",
        data={"items": [1, 2, 3]},
        from_run="run-abc",
    )
    assert record["pipeline_name"] == "my-pipeline"
    assert record["step_id"] == "fetch"
    assert record["pinned_data"] == {"items": [1, 2, 3]}
    assert record["pinned_from_run"] == "run-abc"
    assert record["created_at"]


def test_pin_step_upsert_overwrites(tmp_db):
    """Pinning the same step twice overwrites the previous data."""
    tmp_db.pin_step("pipe", "s1", {"v": 1})
    tmp_db.pin_step("pipe", "s1", {"v": 2}, from_run="run-2")

    pin = tmp_db.get_pin("pipe", "s1")
    assert pin is not None
    assert pin["pinned_data"] == {"v": 2}
    assert pin["pinned_from_run"] == "run-2"


def test_get_pin_returns_none_for_missing(tmp_db):
    """get_pin returns None when no pin exists."""
    assert tmp_db.get_pin("nonexistent", "step") is None


def test_get_pin_deserializes_json(tmp_db):
    """get_pin returns Python objects (not raw JSON strings)."""
    tmp_db.pin_step("pipe", "step1", [1, 2, 3])
    pin = tmp_db.get_pin("pipe", "step1")
    assert pin is not None
    assert isinstance(pin["pinned_data"], list)
    assert pin["pinned_data"] == [1, 2, 3]


def test_unpin_step_returns_true_on_success(tmp_db):
    """unpin_step returns True when the pin existed."""
    tmp_db.pin_step("pipe", "s2", "data")
    result = tmp_db.unpin_step("pipe", "s2")
    assert result is True


def test_unpin_step_returns_false_when_missing(tmp_db):
    """unpin_step returns False when no pin exists."""
    assert tmp_db.unpin_step("pipe", "nonexistent") is False


def test_unpin_removes_pin(tmp_db):
    """After unpin, get_pin returns None."""
    tmp_db.pin_step("pipe", "s3", {"k": "v"})
    tmp_db.unpin_step("pipe", "s3")
    assert tmp_db.get_pin("pipe", "s3") is None


def test_get_pins_returns_empty_list(tmp_db):
    """get_pins returns [] when no pins exist for the pipeline."""
    assert tmp_db.get_pins("empty-pipe") == []


def test_get_pins_returns_all_pins(tmp_db):
    """get_pins returns all pins for a pipeline sorted by step_id."""
    tmp_db.pin_step("pipe", "step-b", {"b": True})
    tmp_db.pin_step("pipe", "step-a", {"a": True})
    tmp_db.pin_step("other", "step-x", {"x": True})  # different pipeline

    pins = tmp_db.get_pins("pipe")
    assert len(pins) == 2
    assert pins[0]["step_id"] == "step-a"
    assert pins[1]["step_id"] == "step-b"


def test_get_pins_isolation_by_pipeline(tmp_db):
    """get_pins only returns pins for the requested pipeline."""
    tmp_db.pin_step("pipe-a", "s1", {})
    tmp_db.pin_step("pipe-b", "s1", {})

    assert len(tmp_db.get_pins("pipe-a")) == 1
    assert len(tmp_db.get_pins("pipe-b")) == 1


def test_pin_complex_data_types(tmp_db):
    """Pin can store complex nested structures."""
    data = {
        "items": [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}],
        "summary": {"total": 2, "ok": 2},
        "nested": {"deep": {"deeper": [True, False, None]}},
    }
    tmp_db.pin_step("pipe", "complex", data)
    pin = tmp_db.get_pin("pipe", "complex")
    assert pin["pinned_data"] == data


def test_pin_none_data(tmp_db):
    """Pinning None data is allowed."""
    tmp_db.pin_step("pipe", "null-step", None)
    pin = tmp_db.get_pin("pipe", "null-step")
    assert pin is not None
    assert pin["pinned_data"] is None


# ---------------------------------------------------------------------------
# Engine integration — pin check intercepts step execution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_uses_pinned_data(tmp_path):
    """When a step is pinned, the engine returns the mock data without executing."""
    from brix.engine import PipelineEngine
    from brix.loader import PipelineLoader

    # Write a helper that would fail if actually executed
    helper = tmp_path / "fail.py"
    helper.write_text("raise RuntimeError('should not be called')\n")

    # Use a unique pipeline name to avoid conflicts with other tests
    pipeline_name = f"pin-test-pipeline-{id(tmp_path)}"
    pl_yaml = f"""
name: {pipeline_name}
steps:
  - id: step1
    type: python
    script: {helper}
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pl_yaml)

    # Pin step1 with mock data in the real DB (same DB the engine uses)
    db = BrixDB()
    db.pin_step(pipeline_name, "step1", {"mocked": True, "value": 42})

    try:
        engine = PipelineEngine()
        result = await engine.run(pipeline)
    finally:
        db.unpin_step(pipeline_name, "step1")

    assert result.success is True
    # step1 output is the mocked data
    step_status = result.steps.get("step1")
    assert step_status is not None
    assert step_status.status == "ok"
    assert step_status.reason == "pin_mock"


# ---------------------------------------------------------------------------
# models.py — test_mode field
# ---------------------------------------------------------------------------


def test_pipeline_model_test_mode_default():
    """Pipeline has test_mode=False by default."""
    pl = Pipeline(name="t", steps=[{"id": "s1", "type": "set", "values": {}}])
    assert pl.test_mode is False


def test_pipeline_model_test_mode_can_be_set():
    """test_mode can be set to True."""
    pl = Pipeline(
        name="t",
        test_mode=True,
        steps=[{"id": "s1", "type": "set", "values": {}}],
    )
    assert pl.test_mode is True


# ---------------------------------------------------------------------------
# Engine integration — test_mode intercepts db.upsert and action.notify
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_test_mode_dry_run_db_upsert():
    """In test_mode, db.upsert steps are skipped with dry output."""
    from brix.engine import PipelineEngine
    from brix.loader import PipelineLoader

    pl_yaml = """
name: test-mode-upsert
test_mode: true
steps:
  - id: upsert1
    type: set
    values:
      x: 1
  - id: upsert2
    type: db_upsert
    params:
      table: test_table
      data: some_data
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pl_yaml)
    assert pipeline.test_mode is True

    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    status = result.steps.get("upsert2")
    assert status is not None
    assert status.status == "ok"
    assert status.reason == "test_mode_dry"


@pytest.mark.asyncio
async def test_engine_test_mode_log_only_notify():
    """In test_mode, action.notify steps are log-only (no real send)."""
    from brix.engine import PipelineEngine
    from brix.loader import PipelineLoader

    pl_yaml = """
name: test-mode-notify
test_mode: true
steps:
  - id: notify1
    type: notify
    channel: email
    to: test@example.com
    message: Hello
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pl_yaml)

    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    status = result.steps.get("notify1")
    assert status is not None
    assert status.status == "ok"
    assert status.reason == "test_mode_log_only"


# ---------------------------------------------------------------------------
# MCP handlers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_handler_pin_step_data_inline():
    """_handle_pin_step_data stores inline data."""
    from brix.mcp_handlers.testing import _handle_pin_step_data

    # Use a unique pipeline name to avoid conflicts
    pipe_name = f"mcp-handler-test-pipe-{id(test_mcp_handler_pin_step_data_inline)}"
    db = BrixDB()
    try:
        result = await _handle_pin_step_data({
            "pipeline_name": pipe_name,
            "step_id": "step-a",
            "data": {"mocked": 99},
        })
        assert result["success"] is True
        assert result["pin"]["pipeline_name"] == pipe_name
        assert result["pin"]["step_id"] == "step-a"
        assert result["pin"]["pinned_data"] == {"mocked": 99}
    finally:
        db.unpin_step(pipe_name, "step-a")


@pytest.mark.asyncio
async def test_mcp_handler_unpin_step_data():
    """_handle_unpin_step_data removes a pin."""
    from brix.mcp_handlers.testing import _handle_unpin_step_data

    pipe_name = f"mcp-unpin-test-{id(test_mcp_handler_unpin_step_data)}"
    db = BrixDB()
    db.pin_step(pipe_name, "step-b", {"v": 1})

    result = await _handle_unpin_step_data({
        "pipeline_name": pipe_name,
        "step_id": "step-b",
    })
    assert result["success"] is True
    # Verify pin is gone
    assert db.get_pin(pipe_name, "step-b") is None


@pytest.mark.asyncio
async def test_mcp_handler_unpin_missing_returns_error():
    """_handle_unpin_step_data returns error when pin doesn't exist."""
    from brix.mcp_handlers.testing import _handle_unpin_step_data

    result = await _handle_unpin_step_data({
        "pipeline_name": "definitely-no-pipe-x999",
        "step_id": "definitely-no-step-x999",
    })
    assert result["success"] is False


@pytest.mark.asyncio
async def test_mcp_handler_list_pins():
    """_handle_list_pins returns all pins for a pipeline."""
    from brix.mcp_handlers.testing import _handle_list_pins

    pipe_name = f"mcp-list-test-{id(test_mcp_handler_list_pins)}"
    db = BrixDB()
    try:
        db.pin_step(pipe_name, "s1", {"a": 1})
        db.pin_step(pipe_name, "s2", {"b": 2})

        result = await _handle_list_pins({"pipeline_name": pipe_name})
        assert result["success"] is True
        assert result["count"] == 2
        ids = {p["step_id"] for p in result["pins"]}
        assert ids == {"s1", "s2"}
    finally:
        db.unpin_step(pipe_name, "s1")
        db.unpin_step(pipe_name, "s2")


@pytest.mark.asyncio
async def test_mcp_handler_pin_missing_args():
    """_handle_pin_step_data returns error when required args are missing."""
    from brix.mcp_handlers.testing import _handle_pin_step_data
    result = await _handle_pin_step_data({})
    assert result["success"] is False
    assert "pipeline_name" in result["error"]


@pytest.mark.asyncio
async def test_mcp_handler_list_pins_missing_arg():
    """_handle_list_pins returns error when pipeline_name is missing."""
    from brix.mcp_handlers.testing import _handle_list_pins
    result = await _handle_list_pins({})
    assert result["success"] is False
