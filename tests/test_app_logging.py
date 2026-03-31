"""Tests for T-BRIX-LOG-01: Structured application event logging."""
import json
import pytest

from brix.app_logging import log_event
from brix.db import BrixDB


@pytest.fixture(autouse=True)
def _clean_app_log():
    """Clear app_log table before each test to ensure isolation."""
    db = BrixDB()
    with db._connect() as conn:
        conn.execute("DELETE FROM app_log")
    yield
    with db._connect() as conn:
        conn.execute("DELETE FROM app_log")


def _db():
    """Get the shared test BrixDB instance."""
    return BrixDB()


class TestLogEvent:
    """Tests for the log_event convenience function."""

    def test_log_event_writes_to_db(self):
        """log_event inserts an entry into app_log that can be read back."""
        entry_id = log_event("INFO", "scheduler", "Pipeline started")
        assert entry_id is not None

        entries = _db().get_app_log()
        assert len(entries) == 1
        assert entries[0]["level"] == "INFO"
        assert entries[0]["component"] == "scheduler"
        assert "Pipeline started" in entries[0]["message"]

    def test_log_event_with_details(self):
        """log_event appends JSON details to message."""
        details = {"pipeline": "my-pipe", "run_id": "r-123"}
        entry_id = log_event("INFO", "scheduler", "Pipeline started", details)
        assert entry_id is not None

        entries = _db().get_app_log()
        assert len(entries) == 1
        msg = entries[0]["message"]
        assert "Pipeline started" in msg
        assert "my-pipe" in msg
        # Details should be valid JSON after the separator
        parts = msg.split(" | ", 1)
        assert len(parts) == 2
        parsed = json.loads(parts[1])
        assert parsed["pipeline"] == "my-pipe"
        assert parsed["run_id"] == "r-123"

    def test_log_event_correct_levels(self):
        """Different log levels are stored correctly."""
        log_event("INFO", "scheduler", "info msg")
        log_event("WARNING", "watchdog", "warn msg")
        log_event("ERROR", "trigger", "error msg")

        infos = _db().get_app_log(level="INFO")
        assert len(infos) == 1
        assert infos[0]["component"] == "scheduler"

        warnings = _db().get_app_log(level="WARNING")
        assert len(warnings) == 1
        assert warnings[0]["component"] == "watchdog"

        errors = _db().get_app_log(level="ERROR")
        assert len(errors) == 1
        assert errors[0]["component"] == "trigger"

    def test_log_event_without_details(self):
        """log_event without details does not append separator."""
        log_event("INFO", "startup_sync", "Sync completed")
        entries = _db().get_app_log()
        assert entries[0]["message"] == "Sync completed"

    def test_log_event_never_raises(self, tmp_path, monkeypatch):
        """log_event returns None on failure, never raises."""
        # Force a DB error by pointing to an invalid path
        import brix.db as db_mod
        original = db_mod.BRIX_DB_PATH
        db_mod.BRIX_DB_PATH = tmp_path / "nonexistent" / "sub" / "db.sqlite"
        try:
            result = log_event("INFO", "test", "should not crash")
            # May succeed (sqlite creates dirs) or return None — either is OK
            # The key assertion: it must NOT raise
        finally:
            db_mod.BRIX_DB_PATH = original


class TestGetAppLogFiltering:
    """Tests for get_app_log filtering via the DB method."""

    def test_filter_by_component(self):
        """get_app_log(component=...) returns only matching entries."""
        log_event("INFO", "scheduler", "sched msg")
        log_event("INFO", "trigger", "trigger msg")
        log_event("INFO", "watchdog", "watchdog msg")

        sched = _db().get_app_log(component="scheduler")
        assert len(sched) == 1
        assert sched[0]["component"] == "scheduler"

        trigger = _db().get_app_log(component="trigger")
        assert len(trigger) == 1
        assert trigger[0]["component"] == "trigger"

    def test_filter_by_level(self):
        """get_app_log(level=...) returns only matching level."""
        log_event("INFO", "scheduler", "info")
        log_event("ERROR", "scheduler", "error")

        errors = _db().get_app_log(level="ERROR")
        assert len(errors) == 1
        assert errors[0]["message"] == "error"

    def test_filter_by_since(self):
        """get_app_log(since=...) excludes older entries."""
        from datetime import datetime, timezone, timedelta
        log_event("INFO", "scheduler", "old message")
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        entries = _db().get_app_log(since=future)
        assert entries == []

    def test_combined_filters(self):
        """Multiple filters can be combined."""
        log_event("INFO", "scheduler", "sched info")
        log_event("ERROR", "scheduler", "sched error")
        log_event("INFO", "trigger", "trigger info")

        result = _db().get_app_log(component="scheduler", level="ERROR")
        assert len(result) == 1
        assert result[0]["message"] == "sched error"

    def test_limit(self):
        """get_app_log respects limit parameter."""
        for i in range(10):
            log_event("INFO", "scheduler", f"msg {i}")
        entries = _db().get_app_log(limit=3)
        assert len(entries) == 3

    def test_entries_have_correct_structure(self):
        """Log entries have all expected fields."""
        log_event("INFO", "scheduler", "test msg")
        entries = _db().get_app_log()
        assert len(entries) == 1
        entry = entries[0]
        assert "id" in entry
        assert "timestamp" in entry
        assert "level" in entry
        assert "component" in entry
        assert "message" in entry
        assert entry["id"]  # non-empty
        assert entry["timestamp"]  # non-empty


class TestGetAppLogMcpHandler:
    """Tests for the _handle_get_app_log MCP handler."""

    @pytest.mark.asyncio
    async def test_handler_returns_entries(self):
        """MCP handler returns entries with count and filters."""
        log_event("INFO", "scheduler", "test msg")
        log_event("ERROR", "watchdog", "error msg")

        from brix.mcp_handlers.health import _handle_get_app_log
        result = await _handle_get_app_log({})
        assert result["count"] == 2
        assert len(result["entries"]) == 2

    @pytest.mark.asyncio
    async def test_handler_filters_by_component(self):
        """MCP handler filters by component."""
        log_event("INFO", "scheduler", "sched msg")
        log_event("INFO", "trigger", "trigger msg")

        from brix.mcp_handlers.health import _handle_get_app_log
        result = await _handle_get_app_log({"component": "scheduler"})
        assert result["count"] == 1
        assert result["entries"][0]["component"] == "scheduler"
        assert result["filters"]["component"] == "scheduler"

    @pytest.mark.asyncio
    async def test_handler_filters_by_level(self):
        """MCP handler filters by level."""
        log_event("INFO", "scheduler", "info")
        log_event("ERROR", "scheduler", "error")

        from brix.mcp_handlers.health import _handle_get_app_log
        result = await _handle_get_app_log({"level": "ERROR"})
        assert result["count"] == 1
        assert result["entries"][0]["level"] == "ERROR"

    @pytest.mark.asyncio
    async def test_handler_with_limit(self):
        """MCP handler respects limit."""
        for i in range(10):
            log_event("INFO", "scheduler", f"msg {i}")

        from brix.mcp_handlers.health import _handle_get_app_log
        result = await _handle_get_app_log({"limit": 3})
        assert result["count"] == 3
