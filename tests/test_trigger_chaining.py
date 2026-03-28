"""Tests for T-BRIX-V6-20 — Intelligent Pipeline Chaining.

Covers all 6 sub-features:
1. Input-Filter on pipeline_done trigger
2. Output-Filter / when condition (Jinja2 expression)
3. Input-Forwarding (forward_input)
4. Trigger context (trigger.source_run.input / .result)
5. Multi-Pipeline Trigger (pipelines list)
6. Trigger Groups (TriggerGroupStore + MCP handlers)
"""
import asyncio
import json
import unittest.mock as mock
from pathlib import Path

import pytest

from brix.triggers.models import TriggerConfig
from brix.triggers.state import TriggerState
from brix.triggers.store import TriggerGroupStore, TriggerStore
from brix.triggers.runners import PipelineDoneTriggerRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_state(tmp_path: Path) -> TriggerState:
    return TriggerState(db_path=tmp_path / "triggers.db")


def _make_trigger(**kwargs) -> TriggerConfig:
    defaults = {
        "id": "t-chain",
        "type": "pipeline_done",
        "pipeline": "downstream-pipeline",
        "filter": {"pipeline": "source-pipeline"},
        "status": "any",
    }
    defaults.update(kwargs)
    return TriggerConfig(**defaults)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Sub-Feature 4: Trigger context — source_run in event
# ---------------------------------------------------------------------------

def test_source_run_context_in_event(tmp_path):
    """PipelineDoneTriggerRunner includes source_run context in events."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        result_json='{"inserted_new": 5}',
        input_json='{"folder": "Inbox"}',
    )

    trigger = _make_trigger()
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    assert len(events) == 1
    ev = events[0]
    assert "source_run" in ev
    sr = ev["source_run"]
    assert sr["run_id"] == "run-001"
    assert sr["pipeline_name"] == "source-pipeline"
    assert sr["status"] == "success"
    assert sr["result"] == {"inserted_new": 5}
    assert sr["input"] == {"folder": "Inbox"}


def test_source_run_null_result(tmp_path):
    """source_run.result is empty dict when result_json is NULL."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")

    trigger = _make_trigger()
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    assert len(events) == 1
    assert events[0]["source_run"]["result"] == {}
    assert events[0]["source_run"]["input"] == {}


# ---------------------------------------------------------------------------
# Sub-Feature 1: Input-Filter
# ---------------------------------------------------------------------------

def test_input_filter_match(tmp_path):
    """input_filter allows events where source run had matching input params."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        input_json='{"mode": "live", "folder": "Inbox"}',
    )

    trigger = _make_trigger(input_filter={"mode": "live"})
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    assert len(events) == 1
    assert events[0]["run_id"] == "run-001"


def test_input_filter_no_match(tmp_path):
    """input_filter rejects events where source run did NOT have matching input params."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        input_json='{"mode": "dry_run"}',
    )

    trigger = _make_trigger(input_filter={"mode": "live"})
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    assert events == []


def test_input_filter_no_input_json(tmp_path):
    """input_filter rejects event when source run had no input recorded."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")

    trigger = _make_trigger(input_filter={"folder": "Inbox"})
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    assert events == []


def test_input_filter_partial_match_all_keys(tmp_path):
    """input_filter requires ALL keys to match."""
    state = _make_state(tmp_path)
    # Only one key matches
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        input_json='{"mode": "live", "scope": "partial"}',
    )

    trigger = _make_trigger(input_filter={"mode": "live", "scope": "full"})
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    assert events == []


def test_input_filter_empty_is_noop(tmp_path):
    """Empty input_filter (default) does not filter anything."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")

    trigger = _make_trigger(input_filter={})
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    assert len(events) == 1


# ---------------------------------------------------------------------------
# Sub-Feature 2: Output-Filter / when condition
# ---------------------------------------------------------------------------

def test_when_condition_true(tmp_path):
    """when expression evaluates to True — fire proceeds."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        result_json='{"inserted_new": 5}',
    )

    trigger = _make_trigger(
        when="{{ trigger.source_run.result.inserted_new > 0 }}"
    )
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())
    assert len(events) == 1

    # _passes_when should return True
    assert runner._passes_when(events[0]) is True


def test_when_condition_false(tmp_path):
    """when expression evaluates to False — fire() returns None."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        result_json='{"inserted_new": 0}',
    )

    trigger = _make_trigger(
        when="{{ trigger.source_run.result.inserted_new > 0 }}"
    )
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())
    assert len(events) == 1

    assert runner._passes_when(events[0]) is False

    # fire() should return None when when-guard fails
    result = _run(runner.fire(events[0]))
    assert result is None


def test_when_condition_empty_is_always_true(tmp_path):
    """when='' means no condition — always fires."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")

    trigger = _make_trigger(when="")
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())
    assert len(events) == 1
    assert runner._passes_when(events[0]) is True


def test_when_condition_status_check(tmp_path):
    """when can check trigger.source_run.status."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")
    state.record_pipeline_event("run-002", "source-pipeline", "failure")

    trigger_success_only = _make_trigger(
        when="{{ trigger.source_run.status == 'success' }}"
    )
    runner = PipelineDoneTriggerRunner(trigger_success_only, state)
    events = _run(runner.poll())

    assert len(events) == 2
    passing = [e for e in events if runner._passes_when(e)]
    assert len(passing) == 1
    assert passing[0]["status"] == "success"


# ---------------------------------------------------------------------------
# Sub-Feature 3: Input-Forwarding
# ---------------------------------------------------------------------------

def test_forward_input_literal(tmp_path):
    """forward_input passes literal values to downstream pipeline."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")

    trigger = _make_trigger(forward_input={"mode": "auto"})
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    forwarded = runner._build_forward_input(events[0])
    assert forwarded == {"mode": "auto"}


def test_forward_input_jinja2(tmp_path):
    """forward_input renders Jinja2 expressions against source_run context."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        result_json='{"batch_id": "batch-42"}',
        input_json='{"folder": "Inbox"}',
    )

    trigger = _make_trigger(
        forward_input={
            "batch": "{{ trigger.source_run.result.batch_id }}",
            "src_folder": "{{ trigger.source_run.input.folder }}",
        }
    )
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    forwarded = runner._build_forward_input(events[0])
    assert forwarded["batch"] == "batch-42"
    assert forwarded["src_folder"] == "Inbox"


def test_forward_input_merged_into_fire(tmp_path):
    """forward_input is merged into params when fire() calls the pipeline."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        result_json='{"count": 3}',
    )

    trigger = _make_trigger(
        forward_input={"upstream_count": "{{ trigger.source_run.result.count }}"}
    )
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())
    assert len(events) == 1

    captured_params = {}

    async def fake_run(self_engine, pipeline, params=None, **kw):
        captured_params.update(params or {})
        result = mock.MagicMock()
        result.run_id = "run-downstream"
        result.success = True
        return result

    from brix.engine import PipelineEngine
    from brix.pipeline_store import PipelineStore

    fake_pipeline = mock.MagicMock()
    with mock.patch.object(PipelineStore, "load", return_value=fake_pipeline), \
         mock.patch.object(PipelineEngine, "run", new=fake_run):
        _run(runner.fire(events[0]))

    assert captured_params.get("upstream_count") == "3"


def test_forward_input_empty_is_noop(tmp_path):
    """Empty forward_input returns empty dict."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")

    trigger = _make_trigger(forward_input={})
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())
    forwarded = runner._build_forward_input(events[0])
    assert forwarded == {}


# ---------------------------------------------------------------------------
# Sub-Feature 5: Multi-Pipeline Trigger
# ---------------------------------------------------------------------------

def test_multi_pipeline_fires_all(tmp_path):
    """pipelines list fires each pipeline and returns last result."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")

    trigger = _make_trigger(
        pipelines=[
            {"pipeline": "pipeline-a", "params": {"key": "a"}},
            {"pipeline": "pipeline-b", "params": {"key": "b"}},
        ]
    )
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())
    assert len(events) == 1

    fired_pipelines = []
    captured_params = []

    async def fake_run(self_engine, pipeline, params=None, **kw):
        fired_pipelines.append(pipeline)
        captured_params.append(dict(params or {}))
        result = mock.MagicMock()
        result.run_id = f"run-{len(fired_pipelines)}"
        result.success = True
        return result

    from brix.engine import PipelineEngine
    from brix.pipeline_store import PipelineStore

    fake_pipeline = mock.MagicMock()
    with mock.patch.object(PipelineStore, "load", return_value=fake_pipeline), \
         mock.patch.object(PipelineEngine, "run", new=fake_run):
        _run(runner.fire(events[0]))

    assert len(fired_pipelines) == 2
    assert captured_params[0]["key"] == "a"
    assert captured_params[1]["key"] == "b"


def test_multi_pipeline_skips_not_found(tmp_path):
    """Multi-pipeline fire skips pipelines that are not found."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "source-pipeline", "success")

    trigger = _make_trigger(
        pipelines=[
            {"pipeline": "missing-pipeline"},
        ]
    )
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    from brix.pipeline_store import PipelineStore
    with mock.patch.object(PipelineStore, "load", side_effect=FileNotFoundError("not found")):
        result = _run(runner.fire(events[0]))

    assert result is None


def test_multi_pipeline_with_forward_input(tmp_path):
    """Multi-pipeline fire merges forward_input into each pipeline's params."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        result_json='{"batch": "X"}',
    )

    trigger = _make_trigger(
        forward_input={"forwarded_batch": "{{ trigger.source_run.result.batch }}"},
        pipelines=[
            {"pipeline": "pipeline-a", "params": {}},
        ]
    )
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    captured = {}

    async def fake_run(self_engine, pipeline, params=None, **kw):
        captured.update(params or {})
        result = mock.MagicMock()
        result.run_id = "r1"
        result.success = True
        return result

    from brix.engine import PipelineEngine
    from brix.pipeline_store import PipelineStore
    fake_pipeline = mock.MagicMock()
    with mock.patch.object(PipelineStore, "load", return_value=fake_pipeline), \
         mock.patch.object(PipelineEngine, "run", new=fake_run):
        _run(runner.fire(events[0]))

    assert captured.get("forwarded_batch") == "X"


# ---------------------------------------------------------------------------
# Sub-Feature 6: Trigger Groups — TriggerGroupStore
# ---------------------------------------------------------------------------

def test_trigger_group_add_and_get(tmp_path):
    """TriggerGroupStore.add creates a group and get retrieves it."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    group = store.add(
        name="import-group",
        triggers=["watch-inbox", "watch-files"],
        description="All import triggers",
    )

    assert group["name"] == "import-group"
    assert group["triggers"] == ["watch-inbox", "watch-files"]
    assert group["description"] == "All import triggers"
    assert group["enabled"] is True
    assert "id" in group
    assert "created_at" in group


def test_trigger_group_get_by_id(tmp_path):
    """TriggerGroupStore.get retrieves by UUID."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    created = store.add(name="g1", triggers=["t1"])
    fetched = store.get(created["id"])
    assert fetched is not None
    assert fetched["name"] == "g1"


def test_trigger_group_get_missing(tmp_path):
    """TriggerGroupStore.get returns None for unknown group."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    assert store.get("nonexistent") is None


def test_trigger_group_list(tmp_path):
    """TriggerGroupStore.list_all returns all groups sorted by name."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    store.add(name="z-group", triggers=["t1"])
    store.add(name="a-group", triggers=["t2"])

    groups = store.list_all()
    assert len(groups) == 2
    assert groups[0]["name"] == "a-group"
    assert groups[1]["name"] == "z-group"


def test_trigger_group_duplicate_name(tmp_path):
    """TriggerGroupStore.add raises ValueError for duplicate name."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    store.add(name="dup", triggers=[])
    with pytest.raises(ValueError, match="already exists"):
        store.add(name="dup", triggers=[])


def test_trigger_group_update(tmp_path):
    """TriggerGroupStore.update modifies triggers and enabled state."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    store.add(name="g1", triggers=["t1", "t2"])

    updated = store.update("g1", triggers=["t1", "t2", "t3"], enabled=False)
    assert updated is not None
    assert updated["triggers"] == ["t1", "t2", "t3"]
    assert updated["enabled"] is False


def test_trigger_group_update_missing(tmp_path):
    """TriggerGroupStore.update returns None for unknown group."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    assert store.update("ghost", enabled=False) is None


def test_trigger_group_delete(tmp_path):
    """TriggerGroupStore.delete removes the group."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    store.add(name="del-me", triggers=[])
    assert store.delete("del-me") is True
    assert store.get("del-me") is None


def test_trigger_group_delete_missing(tmp_path):
    """TriggerGroupStore.delete returns False for unknown group."""
    store = TriggerGroupStore(db_path=tmp_path / "brix.db")
    assert store.delete("ghost") is False


# ---------------------------------------------------------------------------
# Sub-Feature 6: Trigger Groups — MCP handlers
# ---------------------------------------------------------------------------

import brix.mcp_server as _mcp


def _run_mcp(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _patch_group_store(monkeypatch, tmp_path):
    """Monkeypatch TriggerGroupStore to use a temp DB."""
    original_init = TriggerGroupStore.__init__

    def patched_init(self, db_path=None):
        original_init(self, db_path=tmp_path / "brix.db")

    monkeypatch.setattr(TriggerGroupStore, "__init__", patched_init)


def _patch_trigger_store(monkeypatch, tmp_path):
    """Monkeypatch TriggerStore to use a temp DB."""
    original_init = TriggerStore.__init__

    def patched_init(self, db_path=None):
        original_init(self, db_path=tmp_path / "brix.db")

    monkeypatch.setattr(TriggerStore, "__init__", patched_init)


def test_mcp_trigger_group_add(monkeypatch, tmp_path):
    """_handle_trigger_group_add creates a group."""
    _patch_group_store(monkeypatch, tmp_path)
    result = _run_mcp(_mcp._handle_trigger_group_add({
        "name": "import-group",
        "triggers": ["watch-inbox"],
        "description": "Import triggers",
    }))
    assert result["success"] is True
    assert result["group"]["name"] == "import-group"
    assert result["group"]["triggers"] == ["watch-inbox"]


def test_mcp_trigger_group_add_missing_name(monkeypatch, tmp_path):
    """_handle_trigger_group_add returns error when name is missing."""
    _patch_group_store(monkeypatch, tmp_path)
    result = _run_mcp(_mcp._handle_trigger_group_add({"triggers": []}))
    assert result["success"] is False
    assert "name" in result["error"]


def test_mcp_trigger_group_list_empty(monkeypatch, tmp_path):
    """_handle_trigger_group_list returns empty list when no groups exist."""
    _patch_group_store(monkeypatch, tmp_path)
    result = _run_mcp(_mcp._handle_trigger_group_list({}))
    assert result["groups"] == []
    assert result["total"] == 0


def test_mcp_trigger_group_list_populated(monkeypatch, tmp_path):
    """_handle_trigger_group_list returns all groups."""
    _patch_group_store(monkeypatch, tmp_path)
    _run_mcp(_mcp._handle_trigger_group_add({"name": "g1", "triggers": ["t1"]}))
    _run_mcp(_mcp._handle_trigger_group_add({"name": "g2", "triggers": ["t2", "t3"]}))

    result = _run_mcp(_mcp._handle_trigger_group_list({}))
    assert result["total"] == 2
    names = {g["name"] for g in result["groups"]}
    assert names == {"g1", "g2"}


def test_mcp_trigger_group_delete(monkeypatch, tmp_path):
    """_handle_trigger_group_delete removes a group."""
    _patch_group_store(monkeypatch, tmp_path)
    _run_mcp(_mcp._handle_trigger_group_add({"name": "del-me", "triggers": []}))
    result = _run_mcp(_mcp._handle_trigger_group_delete({"name": "del-me"}))
    assert result["success"] is True

    list_result = _run_mcp(_mcp._handle_trigger_group_list({}))
    assert list_result["total"] == 0


def test_mcp_trigger_group_delete_missing(monkeypatch, tmp_path):
    """_handle_trigger_group_delete returns error for unknown group."""
    _patch_group_store(monkeypatch, tmp_path)
    result = _run_mcp(_mcp._handle_trigger_group_delete({"name": "ghost"}))
    assert result["success"] is False
    assert "not found" in result["error"]


def test_mcp_trigger_group_start(monkeypatch, tmp_path):
    """_handle_trigger_group_start enables all triggers in the group."""
    _patch_group_store(monkeypatch, tmp_path)
    _patch_trigger_store(monkeypatch, tmp_path)

    # Add triggers first
    _run_mcp(_mcp._handle_trigger_add({
        "name": "t1", "type": "file", "pipeline": "p1", "enabled": False,
    }))
    _run_mcp(_mcp._handle_trigger_add({
        "name": "t2", "type": "mail", "pipeline": "p2", "enabled": False,
    }))
    _run_mcp(_mcp._handle_trigger_group_add({
        "name": "my-group", "triggers": ["t1", "t2"],
    }))

    result = _run_mcp(_mcp._handle_trigger_group_start({"name": "my-group"}))
    assert result["success"] is True
    assert set(result["enabled"]) == {"t1", "t2"}
    assert result["not_found"] == []

    # Verify triggers are now enabled
    t_store = TriggerStore()
    t1 = t_store.get("t1")
    t2 = t_store.get("t2")
    assert t1["enabled"] is True
    assert t2["enabled"] is True


def test_mcp_trigger_group_stop(monkeypatch, tmp_path):
    """_handle_trigger_group_stop disables all triggers in the group."""
    _patch_group_store(monkeypatch, tmp_path)
    _patch_trigger_store(monkeypatch, tmp_path)

    _run_mcp(_mcp._handle_trigger_add({
        "name": "t1", "type": "file", "pipeline": "p1", "enabled": True,
    }))
    _run_mcp(_mcp._handle_trigger_group_add({
        "name": "my-group", "triggers": ["t1"],
    }))

    result = _run_mcp(_mcp._handle_trigger_group_stop({"name": "my-group"}))
    assert result["success"] is True
    assert "t1" in result["disabled"]
    assert result["not_found"] == []

    t_store = TriggerStore()
    t1 = t_store.get("t1")
    assert t1["enabled"] is False


def test_mcp_trigger_group_stop_not_found_triggers(monkeypatch, tmp_path):
    """_handle_trigger_group_stop reports triggers that don't exist."""
    _patch_group_store(monkeypatch, tmp_path)
    _patch_trigger_store(monkeypatch, tmp_path)

    _run_mcp(_mcp._handle_trigger_group_add({
        "name": "my-group", "triggers": ["missing-trigger"],
    }))

    result = _run_mcp(_mcp._handle_trigger_group_stop({"name": "my-group"}))
    assert result["success"] is True
    assert "missing-trigger" in result["not_found"]
    assert result["disabled"] == []


def test_mcp_trigger_group_start_missing_group(monkeypatch, tmp_path):
    """_handle_trigger_group_start returns error for unknown group."""
    _patch_group_store(monkeypatch, tmp_path)
    result = _run_mcp(_mcp._handle_trigger_group_start({"name": "ghost"}))
    assert result["success"] is False
    assert "not found" in result["error"]


def test_mcp_trigger_group_stop_missing_group(monkeypatch, tmp_path):
    """_handle_trigger_group_stop returns error for unknown group."""
    _patch_group_store(monkeypatch, tmp_path)
    result = _run_mcp(_mcp._handle_trigger_group_stop({"name": "ghost"}))
    assert result["success"] is False
    assert "not found" in result["error"]


# ---------------------------------------------------------------------------
# State — input_json persistence
# ---------------------------------------------------------------------------

def test_state_stores_input_json(tmp_path):
    """record_pipeline_event stores input_json and get_unprocessed_events returns it."""
    state = _make_state(tmp_path)
    state.record_pipeline_event(
        "run-001", "p", "success",
        input_json='{"key": "val"}',
    )
    events = state.get_unprocessed_events()
    assert len(events) == 1
    assert events[0]["input_json"] == '{"key": "val"}'


def test_state_record_pipeline_completion_with_input(tmp_path):
    """record_pipeline_completion stores the input dict as JSON."""
    state = _make_state(tmp_path)
    state.record_pipeline_completion(
        "my-pipe", "run-001", "success",
        result={"out": 1},
        input={"folder": "Inbox", "limit": 10},
    )
    events = state.get_unprocessed_events()
    assert len(events) == 1
    ev = events[0]
    assert json.loads(ev["input_json"]) == {"folder": "Inbox", "limit": 10}


def test_state_record_pipeline_completion_no_input(tmp_path):
    """record_pipeline_completion with input=None stores NULL input_json."""
    state = _make_state(tmp_path)
    state.record_pipeline_completion("p", "r1", "success", input=None)
    events = state.get_unprocessed_events()
    assert events[0]["input_json"] is None


# ---------------------------------------------------------------------------
# Integration: all features together
# ---------------------------------------------------------------------------

def test_full_chaining_pipeline(tmp_path):
    """Integration: input_filter + when + forward_input work together."""
    state = _make_state(tmp_path)

    # Matching event
    state.record_pipeline_event(
        "run-001", "source-pipeline", "success",
        result_json='{"new_records": 7}',
        input_json='{"env": "production"}',
    )
    # Non-matching event (env is staging)
    state.record_pipeline_event(
        "run-002", "source-pipeline", "success",
        result_json='{"new_records": 3}',
        input_json='{"env": "staging"}',
    )

    trigger = _make_trigger(
        input_filter={"env": "production"},
        when="{{ trigger.source_run.result.new_records > 5 }}",
        forward_input={"count": "{{ trigger.source_run.result.new_records }}"},
    )
    runner = PipelineDoneTriggerRunner(trigger, state)
    events = _run(runner.poll())

    # Only run-001 passes input_filter
    assert len(events) == 1
    assert events[0]["run_id"] == "run-001"

    # when condition is also satisfied
    assert runner._passes_when(events[0]) is True

    # forward_input produces correct value
    forwarded = runner._build_forward_input(events[0])
    assert forwarded["count"] == "7"
