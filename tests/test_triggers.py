"""Tests for the Brix trigger system (T-BRIX-V4-08)."""
import tempfile
from pathlib import Path

import pytest
import yaml

from brix.triggers.models import TriggerConfig
from brix.triggers.state import TriggerState
from brix.triggers.service import TriggerService


# ---------------------------------------------------------------------------
# TriggerConfig
# ---------------------------------------------------------------------------

def test_trigger_config_parse():
    """TriggerConfig can be constructed from a plain dict."""
    data = {
        "id": "my-trigger",
        "type": "mail",
        "interval": "10m",
        "pipeline": "import-mails",
        "params": {"folder": "Inbox"},
        "enabled": True,
    }
    config = TriggerConfig(**data)
    assert config.id == "my-trigger"
    assert config.type == "mail"
    assert config.interval == "10m"
    assert config.pipeline == "import-mails"
    assert config.params == {"folder": "Inbox"}
    assert config.enabled is True


def test_trigger_config_defaults():
    """TriggerConfig fills in sensible defaults for optional fields."""
    config = TriggerConfig(id="t1", type="file", pipeline="process-file")
    assert config.interval == "5m"
    assert config.params == {}
    assert config.dedupe_key == ""
    assert config.enabled is True
    assert config.filter == {}
    assert config.headers == {}
    assert config.path is None
    assert config.url is None


# ---------------------------------------------------------------------------
# TriggerState — dedup
# ---------------------------------------------------------------------------

def test_trigger_state_dedup(tmp_path):
    """is_deduped returns False before record_fired, True after."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    assert state.is_deduped("t1", "key-abc") is False
    state.record_fired("t1", "key-abc", run_id="run-001")
    assert state.is_deduped("t1", "key-abc") is True


def test_trigger_state_dedup_different_keys(tmp_path):
    """Different dedupe keys for the same trigger are independent."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_fired("t1", "key-1")
    assert state.is_deduped("t1", "key-1") is True
    assert state.is_deduped("t1", "key-2") is False


def test_trigger_state_dedup_different_triggers(tmp_path):
    """Same dedupe key for different triggers are independent."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_fired("t1", "shared-key")
    assert state.is_deduped("t1", "shared-key") is True
    assert state.is_deduped("t2", "shared-key") is False


# ---------------------------------------------------------------------------
# TriggerState — pipeline events
# ---------------------------------------------------------------------------

def test_trigger_state_pipeline_event(tmp_path):
    """record_pipeline_event creates an event visible in get_unprocessed_events."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_pipeline_event("run-001", "my-pipeline", "success", '{"result_type": "dict"}')

    events = state.get_unprocessed_events()
    assert len(events) == 1
    ev = events[0]
    assert ev["run_id"] == "run-001"
    assert ev["pipeline_name"] == "my-pipeline"
    assert ev["status"] == "success"
    assert ev["processed"] == 0


def test_trigger_state_pipeline_event_filter_by_name(tmp_path):
    """get_unprocessed_events can filter by pipeline_name."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_pipeline_event("run-001", "pipeline-a", "success")
    state.record_pipeline_event("run-002", "pipeline-b", "failure")

    events_a = state.get_unprocessed_events(pipeline_name="pipeline-a")
    assert len(events_a) == 1
    assert events_a[0]["pipeline_name"] == "pipeline-a"

    events_b = state.get_unprocessed_events(pipeline_name="pipeline-b")
    assert len(events_b) == 1
    assert events_b[0]["pipeline_name"] == "pipeline-b"


def test_trigger_state_pipeline_event_filter_by_status(tmp_path):
    """get_unprocessed_events can filter by status."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_pipeline_event("run-001", "pipeline-a", "success")
    state.record_pipeline_event("run-002", "pipeline-a", "failure")

    success_events = state.get_unprocessed_events(status="success")
    assert len(success_events) == 1
    assert success_events[0]["status"] == "success"

    failure_events = state.get_unprocessed_events(status="failure")
    assert len(failure_events) == 1
    assert failure_events[0]["status"] == "failure"


def test_trigger_state_pipeline_event_filter_status_any(tmp_path):
    """status='any' returns all events regardless of status."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_pipeline_event("run-001", "pipeline-a", "success")
    state.record_pipeline_event("run-002", "pipeline-a", "failure")

    events = state.get_unprocessed_events(status="any")
    assert len(events) == 2


# ---------------------------------------------------------------------------
# TriggerState — mark processed
# ---------------------------------------------------------------------------

def test_trigger_state_pipeline_completion(tmp_path):
    """record_pipeline_completion stores event with correct pipeline_name/run_id/status."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_pipeline_completion("my-pipeline", "run-001", "success", {"count": 42})

    events = state.get_unprocessed_events()
    assert len(events) == 1
    ev = events[0]
    assert ev["pipeline_name"] == "my-pipeline"
    assert ev["run_id"] == "run-001"
    assert ev["status"] == "success"
    assert ev["processed"] == 0
    # result should be JSON-serialised
    import json
    assert json.loads(ev["result_json"])["count"] == 42


def test_trigger_state_pipeline_completion_none_result(tmp_path):
    """record_pipeline_completion handles result=None (result_json stays NULL)."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_pipeline_completion("pipe-x", "run-002", "failure", None)

    events = state.get_unprocessed_events()
    assert len(events) == 1
    assert events[0]["result_json"] is None
    assert events[0]["status"] == "failure"


def test_trigger_state_pipeline_completion_non_json_result(tmp_path):
    """record_pipeline_completion handles non-serialisable objects via default=str."""
    from datetime import datetime
    state = TriggerState(db_path=tmp_path / "triggers.db")
    dt = datetime(2026, 1, 15, 10, 30)
    state.record_pipeline_completion("pipe-y", "run-003", "success", dt)

    events = state.get_unprocessed_events()
    assert len(events) == 1
    # datetime serialised via default=str
    assert "2026" in events[0]["result_json"]


# ---------------------------------------------------------------------------
# Engine writes pipeline_done event via record_pipeline_completion
# ---------------------------------------------------------------------------

def test_engine_writes_pipeline_done_event(tmp_path):
    """Engine.run() writes a pipeline_done event into TriggerState after completion."""
    import asyncio
    import unittest.mock as mock
    from brix.engine import PipelineEngine
    from brix.loader import PipelineLoader

    pipeline = PipelineLoader().load_from_string("""
name: test-done-event
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")

    db_path = tmp_path / "triggers.db"
    state = TriggerState(db_path=db_path)

    engine = PipelineEngine()

    with mock.patch(
        "brix.triggers.state.TriggerState",
        return_value=state,
    ):
        result = asyncio.get_event_loop().run_until_complete(engine.run(pipeline))

    assert result.success is True
    events = state.get_unprocessed_events(pipeline_name="test-done-event")
    assert len(events) == 1
    ev = events[0]
    assert ev["status"] == "success"
    assert ev["run_id"] == result.run_id


def test_engine_writes_failure_event_on_error(tmp_path):
    """Engine.run() writes a 'failure' event when pipeline fails."""
    import asyncio
    import unittest.mock as mock
    from brix.engine import PipelineEngine
    from brix.loader import PipelineLoader

    pipeline = PipelineLoader().load_from_string("""
name: test-done-failure
steps:
  - id: s1
    type: cli
    args: ["false"]
""")

    db_path = tmp_path / "triggers.db"
    state = TriggerState(db_path=db_path)

    engine = PipelineEngine()

    with mock.patch(
        "brix.triggers.state.TriggerState",
        return_value=state,
    ):
        result = asyncio.get_event_loop().run_until_complete(engine.run(pipeline))

    assert result.success is False
    events = state.get_unprocessed_events(pipeline_name="test-done-failure")
    assert len(events) == 1
    assert events[0]["status"] == "failure"


def test_trigger_state_mark_processed(tmp_path):
    """After mark_event_processed the event no longer appears in get_unprocessed_events."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.record_pipeline_event("run-001", "my-pipeline", "success")

    events = state.get_unprocessed_events()
    assert len(events) == 1
    event_id = events[0]["id"]

    state.mark_event_processed(event_id)

    events_after = state.get_unprocessed_events()
    assert len(events_after) == 0


# ---------------------------------------------------------------------------
# TriggerService — loading
# ---------------------------------------------------------------------------

def test_trigger_service_load(tmp_path):
    """TriggerService.load_triggers reads triggers from a YAML config file."""
    config = {
        "triggers": [
            {"id": "t1", "type": "mail", "pipeline": "import-mails"},
            {"id": "t2", "type": "file", "pipeline": "process-file", "interval": "1m"},
        ]
    }
    config_path = tmp_path / "triggers.yaml"
    config_path.write_text(yaml.dump(config))

    state = TriggerState(db_path=tmp_path / "triggers.db")
    svc = TriggerService(config_path=config_path, state=state)
    svc.load_triggers()

    assert svc.trigger_count == 2
    assert svc.enabled_count == 2
    assert svc._triggers[0].id == "t1"
    assert svc._triggers[1].id == "t2"


def test_trigger_service_empty(tmp_path):
    """TriggerService with no config file has 0 triggers."""
    config_path = tmp_path / "triggers.yaml"  # does not exist
    state = TriggerState(db_path=tmp_path / "triggers.db")
    svc = TriggerService(config_path=config_path, state=state)
    svc.load_triggers()

    assert svc.trigger_count == 0
    assert svc.enabled_count == 0


def test_trigger_service_enabled_count(tmp_path):
    """enabled_count only counts triggers where enabled=True."""
    config = {
        "triggers": [
            {"id": "t1", "type": "mail", "pipeline": "pipeline-a", "enabled": True},
            {"id": "t2", "type": "mail", "pipeline": "pipeline-b", "enabled": True},
            {"id": "t3", "type": "file", "pipeline": "pipeline-c", "enabled": False},
        ]
    }
    config_path = tmp_path / "triggers.yaml"
    config_path.write_text(yaml.dump(config))

    state = TriggerState(db_path=tmp_path / "triggers.db")
    svc = TriggerService(config_path=config_path, state=state)
    svc.load_triggers()

    assert svc.trigger_count == 3
    assert svc.enabled_count == 2


# ---------------------------------------------------------------------------
# Runner tests (T-BRIX-V4-xx)
# ---------------------------------------------------------------------------

import asyncio
import unittest.mock as mock

from brix.triggers.runners import (
    TRIGGER_RUNNERS,
    FileTriggerRunner,
    HttpPollTriggerRunner,
    PipelineDoneTriggerRunner,
    MailTriggerRunner,
)


def _make_trigger(**kwargs) -> TriggerConfig:
    defaults = {"id": "t-test", "type": "file", "pipeline": "test-pipeline"}
    defaults.update(kwargs)
    return TriggerConfig(**defaults)


def _make_state(tmp_path) -> TriggerState:
    return TriggerState(db_path=tmp_path / "triggers.db")


# --- FileTriggerRunner ---

def test_file_trigger_finds_files(tmp_path):
    """FileTriggerRunner.poll() returns one event per file in the directory."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (watch_dir / "a.txt").write_text("hello")
    (watch_dir / "b.txt").write_text("world")

    trigger = _make_trigger(type="file", path=str(watch_dir))
    state = _make_state(tmp_path)
    runner = FileTriggerRunner(trigger, state)

    events = asyncio.get_event_loop().run_until_complete(runner.poll())
    filenames = {e["filename"] for e in events}
    assert filenames == {"a.txt", "b.txt"}
    assert all("path" in e and "size" in e and "mtime" in e for e in events)


def test_file_trigger_empty_dir(tmp_path):
    """FileTriggerRunner.poll() returns 0 events for an empty directory."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()

    trigger = _make_trigger(type="file", path=str(watch_dir))
    state = _make_state(tmp_path)
    runner = FileTriggerRunner(trigger, state)

    events = asyncio.get_event_loop().run_until_complete(runner.poll())
    assert events == []


def test_file_trigger_pattern(tmp_path):
    """FileTriggerRunner respects the pattern filter (e.g. '*.pdf')."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (watch_dir / "report.pdf").write_bytes(b"%PDF")
    (watch_dir / "notes.txt").write_text("notes")

    trigger = _make_trigger(type="file", path=str(watch_dir), pattern="*.pdf")
    state = _make_state(tmp_path)
    runner = FileTriggerRunner(trigger, state)

    events = asyncio.get_event_loop().run_until_complete(runner.poll())
    assert len(events) == 1
    assert events[0]["filename"] == "report.pdf"


def test_file_trigger_missing_path(tmp_path):
    """FileTriggerRunner returns [] when the path does not exist."""
    trigger = _make_trigger(type="file", path=str(tmp_path / "nonexistent"))
    state = _make_state(tmp_path)
    runner = FileTriggerRunner(trigger, state)

    events = asyncio.get_event_loop().run_until_complete(runner.poll())
    assert events == []


# --- HttpPollTriggerRunner ---

def test_http_poll_trigger(tmp_path):
    """HttpPollTriggerRunner.poll() returns hash + payload on success."""
    trigger = _make_trigger(type="http_poll", url="http://example.com/api/status")
    state = _make_state(tmp_path)
    runner = HttpPollTriggerRunner(trigger, state)

    fake_response = mock.MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {"version": "1.2.3", "healthy": True}

    with mock.patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = mock.AsyncMock()
        mock_client.get = mock.AsyncMock(return_value=fake_response)
        mock_client_cls.return_value.__aenter__ = mock.AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = mock.AsyncMock(return_value=False)

        events = asyncio.get_event_loop().run_until_complete(runner.poll())

    assert len(events) == 1
    ev = events[0]
    assert ev["url"] == "http://example.com/api/status"
    assert ev["status_code"] == 200
    assert "hash" in ev and len(ev["hash"]) == 16
    assert ev["payload"] == {"version": "1.2.3", "healthy": True}


def test_http_poll_trigger_error(tmp_path):
    """HttpPollTriggerRunner.poll() returns [] on network error."""
    import httpx

    trigger = _make_trigger(type="http_poll", url="http://unreachable.example/api")
    state = _make_state(tmp_path)
    runner = HttpPollTriggerRunner(trigger, state)

    with mock.patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = mock.AsyncMock()
        mock_client.get = mock.AsyncMock(side_effect=httpx.ConnectError("refused"))
        mock_client_cls.return_value.__aenter__ = mock.AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = mock.AsyncMock(return_value=False)

        events = asyncio.get_event_loop().run_until_complete(runner.poll())

    assert events == []


# --- PipelineDoneTriggerRunner ---

def test_pipeline_done_trigger(tmp_path):
    """PipelineDoneTriggerRunner.poll() returns unprocessed events and marks them processed."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "my-pipeline", "success")
    state.record_pipeline_event("run-002", "my-pipeline", "failure")

    trigger = _make_trigger(
        type="pipeline_done",
        filter={"pipeline": "my-pipeline"},
        status="any",
    )
    runner = PipelineDoneTriggerRunner(trigger, state)

    events = asyncio.get_event_loop().run_until_complete(runner.poll())
    assert len(events) == 2
    run_ids = {e["run_id"] for e in events}
    assert run_ids == {"run-001", "run-002"}

    # Second poll: already marked processed
    events2 = asyncio.get_event_loop().run_until_complete(runner.poll())
    assert events2 == []


def test_pipeline_done_trigger_status_filter(tmp_path):
    """PipelineDoneTriggerRunner respects the status filter."""
    state = _make_state(tmp_path)
    state.record_pipeline_event("run-001", "pipe-a", "success")
    state.record_pipeline_event("run-002", "pipe-a", "failure")

    trigger = _make_trigger(
        type="pipeline_done",
        filter={"pipeline": "pipe-a"},
        status="success",
    )
    runner = PipelineDoneTriggerRunner(trigger, state)

    events = asyncio.get_event_loop().run_until_complete(runner.poll())
    assert len(events) == 1
    assert events[0]["status"] == "success"


# --- MailTriggerRunner — graceful when no MCP server ---

def test_mail_trigger_no_server(tmp_path):
    """MailTriggerRunner returns [] gracefully when MCP server is unavailable."""
    trigger = _make_trigger(
        type="mail",
        filter={"unread": True},
    )
    state = _make_state(tmp_path)
    runner = MailTriggerRunner(trigger, state)

    # McpRunner.execute raises FileNotFoundError when servers.yaml doesn't exist
    with mock.patch(
        "brix.triggers.runners.McpRunner.execute",
        new_callable=mock.AsyncMock,
        side_effect=FileNotFoundError("No servers.yaml"),
    ):
        events = asyncio.get_event_loop().run_until_complete(runner.poll())

    assert events == []


def test_mail_trigger_mcp_returns_failure(tmp_path):
    """MailTriggerRunner returns [] when MCP call reports success=False."""
    trigger = _make_trigger(type="mail", filter={})
    state = _make_state(tmp_path)
    runner = MailTriggerRunner(trigger, state)

    with mock.patch(
        "brix.triggers.runners.McpRunner.execute",
        new_callable=mock.AsyncMock,
        return_value={"success": False, "error": "auth error"},
    ):
        events = asyncio.get_event_loop().run_until_complete(runner.poll())


    assert events == []


# --- Dedup ---

def test_dedup_filters_already_seen(tmp_path):
    """dedupe() returns [] for events whose dedupe_key was already recorded."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (watch_dir / "file.txt").write_text("x")

    trigger = _make_trigger(
        type="file",
        path=str(watch_dir),
        dedupe_key="{{ trigger.filename }}",
    )
    state = _make_state(tmp_path)
    runner = FileTriggerRunner(trigger, state)

    events = asyncio.get_event_loop().run_until_complete(runner.poll())
    assert len(events) == 1

    # First dedupe: not yet seen
    new_events = runner.dedupe(events)
    assert len(new_events) == 1

    # Record as fired
    state.record_fired(trigger.id, "file.txt")

    # Second dedupe: already seen
    new_events2 = runner.dedupe(events)
    assert new_events2 == []


def test_dedup_no_key_passes_all(tmp_path):
    """When dedupe_key is empty, dedupe() returns all events unchanged."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (watch_dir / "a.txt").write_text("a")
    (watch_dir / "b.txt").write_text("b")

    # Record one as if already fired
    state = _make_state(tmp_path)
    state.record_fired("t-test", "a.txt")

    trigger = _make_trigger(type="file", path=str(watch_dir), dedupe_key="")
    runner = FileTriggerRunner(trigger, state)

    events = asyncio.get_event_loop().run_until_complete(runner.poll())
    new_events = runner.dedupe(events)
    # No dedupe_key → all events pass through
    assert len(new_events) == len(events)


# --- Registry ---

def test_trigger_runner_registry():
    """TRIGGER_RUNNERS contains all 4 expected runner types."""
    assert set(TRIGGER_RUNNERS.keys()) == {"mail", "file", "http_poll", "pipeline_done"}
    assert TRIGGER_RUNNERS["mail"] is MailTriggerRunner
    assert TRIGGER_RUNNERS["file"] is FileTriggerRunner
    assert TRIGGER_RUNNERS["http_poll"] is HttpPollTriggerRunner
    assert TRIGGER_RUNNERS["pipeline_done"] is PipelineDoneTriggerRunner
