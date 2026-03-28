"""Tests for T-BRIX-V6-01: source parameter on all MCP tools + audit_log.

Covers:
- audit_log table created in BrixDB
- BrixDB.write_audit_entry stores correct fields
- BrixDB.get_audit_log returns entries newest-first
- _extract_source helper
- _source_summary helper
- Every BRIX_TOOL has 'source' in its inputSchema.properties
- 'source' is NOT in required list (optional/backward-compat)
- source parameter description is non-empty
- _handle_create_pipeline writes to audit_log
- _handle_delete_pipeline writes to audit_log
- _handle_update_pipeline writes to audit_log
- _handle_create_helper writes to audit_log
- _handle_register_helper writes to audit_log
- _handle_update_helper writes to audit_log
- _handle_delete_helper writes to audit_log
- _handle_add_step writes to audit_log
- _handle_remove_step writes to audit_log
- _handle_update_step writes to audit_log
- source=None is handled gracefully (no crash, no audit entry for non-mutating tools)
- pipeline tools (brix__pipeline__*) also get source in their schema
"""
import json
import sqlite3
from pathlib import Path

import pytest

from brix.db import BrixDB
from brix.mcp_server import (
    BRIX_TOOLS,
    _HANDLERS,
    _build_pipeline_tools,
    _extract_source,
    _source_summary,
    _handle_create_pipeline,
    _handle_delete_pipeline,
    _handle_update_pipeline,
    _handle_create_helper,
    _handle_register_helper,
    _handle_update_helper,
    _handle_delete_helper,
    _handle_add_step,
    _handle_remove_step,
    _handle_update_step,
    PIPELINE_DIR,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Return a BrixDB backed by a temp file."""
    return BrixDB(db_path=tmp_path / "brix.db")


SAMPLE_SOURCE = {"session": "test-session", "model": "sonnet", "agent": "agent-alpha"}


# ---------------------------------------------------------------------------
# BrixDB: audit_log table + CRUD
# ---------------------------------------------------------------------------

class TestAuditLogTable:
    def test_audit_log_table_created(self, db):
        """audit_log table must exist after BrixDB init."""
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "audit_log" in tables

    def test_audit_log_columns(self, db):
        """audit_log must have all required columns."""
        with db._connect() as conn:
            cols = {
                row[1]
                for row in conn.execute("PRAGMA table_info(audit_log)").fetchall()
            }
        expected = {"id", "timestamp", "tool", "source_session", "source_model", "source_agent", "arguments_summary"}
        assert expected == cols

    def test_write_audit_entry_returns_id(self, db):
        entry_id = db.write_audit_entry(
            tool="brix__test_tool",
            source=SAMPLE_SOURCE,
            arguments_summary="pipeline='test'",
        )
        assert isinstance(entry_id, str)
        assert len(entry_id) > 0

    def test_write_audit_entry_stores_fields(self, db):
        db.write_audit_entry(
            tool="brix__create_pipeline",
            source=SAMPLE_SOURCE,
            arguments_summary="pipeline='my-pipe'",
        )
        entries = db.get_audit_log()
        assert len(entries) == 1
        e = entries[0]
        assert e["tool"] == "brix__create_pipeline"
        assert e["source_session"] == "test-session"
        assert e["source_model"] == "sonnet"
        assert e["source_agent"] == "agent-alpha"
        assert e["arguments_summary"] == "pipeline='my-pipe'"
        assert e["timestamp"]  # non-empty ISO timestamp

    def test_write_audit_entry_null_source(self, db):
        """source=None results in all source_* columns being NULL."""
        db.write_audit_entry(tool="brix__list_pipelines", source=None)
        entries = db.get_audit_log()
        e = entries[0]
        assert e["source_session"] is None
        assert e["source_model"] is None
        assert e["source_agent"] is None

    def test_write_audit_entry_partial_source(self, db):
        """Partial source dict (only 'session') leaves others NULL."""
        db.write_audit_entry(
            tool="brix__run_pipeline",
            source={"session": "buddy"},
        )
        entries = db.get_audit_log()
        e = entries[0]
        assert e["source_session"] == "buddy"
        assert e["source_model"] is None
        assert e["source_agent"] is None

    def test_get_audit_log_newest_first(self, db):
        """get_audit_log returns entries newest-first."""
        db.write_audit_entry(tool="tool_a")
        db.write_audit_entry(tool="tool_b")
        db.write_audit_entry(tool="tool_c")
        entries = db.get_audit_log()
        assert entries[0]["tool"] == "tool_c"
        assert entries[-1]["tool"] == "tool_a"

    def test_get_audit_log_limit(self, db):
        """get_audit_log respects the limit parameter."""
        for i in range(10):
            db.write_audit_entry(tool=f"tool_{i}")
        entries = db.get_audit_log(limit=3)
        assert len(entries) == 3

    def test_arguments_summary_truncated_to_500(self, db):
        """arguments_summary longer than 500 chars is truncated."""
        long_summary = "x" * 1000
        db.write_audit_entry(tool="brix__test", arguments_summary=long_summary)
        entries = db.get_audit_log()
        assert len(entries[0]["arguments_summary"]) == 500

    def test_write_audit_entry_empty_source(self, db):
        """Empty dict source works fine, all source_* fields are NULL."""
        db.write_audit_entry(tool="brix__get_tips", source={})
        entries = db.get_audit_log()
        e = entries[0]
        assert e["source_session"] is None
        assert e["source_model"] is None
        assert e["source_agent"] is None


# ---------------------------------------------------------------------------
# _extract_source helper
# ---------------------------------------------------------------------------

class TestExtractSource:
    def test_returns_dict_when_present(self):
        result = _extract_source({"source": {"session": "s1", "model": "m1"}})
        assert result == {"session": "s1", "model": "m1"}

    def test_returns_none_when_absent(self):
        assert _extract_source({"pipeline_id": "test"}) is None

    def test_returns_none_when_source_is_string(self):
        """Non-dict source is ignored."""
        assert _extract_source({"source": "not-a-dict"}) is None

    def test_returns_none_when_source_is_none(self):
        assert _extract_source({"source": None}) is None

    def test_returns_empty_dict_when_source_is_empty_dict(self):
        assert _extract_source({"source": {}}) == {}


# ---------------------------------------------------------------------------
# _source_summary helper
# ---------------------------------------------------------------------------

class TestSourceSummary:
    def test_with_source_and_kwargs(self):
        result = _source_summary(SAMPLE_SOURCE, pipeline="my-pipe")
        assert "pipeline='my-pipe'" in result
        assert "session='test-session'" in result

    def test_with_none_source(self):
        result = _source_summary(None, pipeline="my-pipe")
        assert "pipeline='my-pipe'" in result
        assert "session" not in result

    def test_with_empty_kwargs(self):
        result = _source_summary(SAMPLE_SOURCE)
        assert "session='test-session'" in result

    def test_empty_values_skipped(self):
        result = _source_summary(None, pipeline="", step="my-step")
        assert "pipeline" not in result
        assert "step='my-step'" in result


# ---------------------------------------------------------------------------
# Tool schemas: every BRIX_TOOL has 'source' in properties, not in required
# ---------------------------------------------------------------------------

class TestSourceParameterInToolSchemas:
    def test_all_brix_tools_have_source_property(self):
        """Every BRIX_TOOL must expose 'source' in its inputSchema.properties."""
        for tool in BRIX_TOOLS:
            props = tool.inputSchema.get("properties", {})
            assert "source" in props, (
                f"Tool '{tool.name}' missing 'source' in properties"
            )

    def test_source_not_required_on_any_tool(self):
        """'source' must be optional (not in required) for backward-compat."""
        for tool in BRIX_TOOLS:
            required = tool.inputSchema.get("required", [])
            assert "source" not in required, (
                f"Tool '{tool.name}' has 'source' in required — must be optional"
            )

    def test_source_property_has_type_object(self):
        """'source' property type should be 'object'."""
        for tool in BRIX_TOOLS:
            props = tool.inputSchema.get("properties", {})
            source_prop = props.get("source", {})
            assert source_prop.get("type") == "object", (
                f"Tool '{tool.name}' source property type != 'object'"
            )

    def test_source_property_has_description(self):
        """'source' property should have a non-empty description."""
        for tool in BRIX_TOOLS:
            props = tool.inputSchema.get("properties", {})
            source_prop = props.get("source", {})
            assert source_prop.get("description"), (
                f"Tool '{tool.name}' source property missing description"
            )

    def test_pipeline_tools_also_have_source(self, tmp_path):
        """Dynamically built pipeline tools also get the source property."""
        from brix.db import BrixDB
        from brix.pipeline_store import PipelineStore
        isolated_db = BrixDB(db_path=tmp_path / "test.db")
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        store.save({
            "name": "test-pipe",
            "version": "1.0.0",
            "steps": [{"id": "s", "type": "cli", "args": ["echo", "hi"]}],
        })
        pipeline_tools = _build_pipeline_tools(store)
        assert len(pipeline_tools) >= 1
        for tool in pipeline_tools:
            props = tool.inputSchema.get("properties", {})
            assert "source" in props, (
                f"Pipeline tool '{tool.name}' missing 'source' in properties"
            )
            assert "source" not in tool.inputSchema.get("required", [])


# ---------------------------------------------------------------------------
# Handler audit log writes (integration tests using tmp dirs)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_audit_db(tmp_path, monkeypatch):
    """Redirect _audit_db to a temp DB so tests don't pollute ~/.brix/brix.db."""
    import brix.mcp_handlers._shared as _shared
    temp_db = BrixDB(db_path=tmp_path / "audit_test.db")
    # Patch _audit_db in the _shared module (canonical location) AND in all handler modules that import it
    monkeypatch.setattr(_shared, "_audit_db", temp_db)
    for mod_name in [
        "brix.mcp_handlers.pipelines",
        "brix.mcp_handlers.helpers",
        "brix.mcp_handlers.steps",
        "brix.mcp_handlers.runs",
        "brix.mcp_handlers.templates",
        "brix.mcp_handlers.composer",
    ]:
        try:
            import importlib
            mod = importlib.import_module(mod_name)
            monkeypatch.setattr(mod, "_audit_db", temp_db)
        except (ImportError, AttributeError):
            pass
    # Also redirect _pipeline_dir in all handler modules that use it
    for mod_path in [
        "brix.mcp_handlers._shared",
        "brix.mcp_handlers.pipelines",
        "brix.mcp_handlers.steps",
        "brix.mcp_handlers.templates",
    ]:
        try:
            monkeypatch.setattr(f"{mod_path}._pipeline_dir", lambda: tmp_path)
        except AttributeError:
            pass
    yield temp_db


class TestHandlerAuditLog:
    """Verify that mutating handlers write to the audit_log."""

    @pytest.mark.asyncio
    async def test_create_pipeline_writes_audit(self, patch_audit_db):
        result = await _handle_create_pipeline({
            "name": "audit-test-pipeline",
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__create_pipeline" for e in entries)
        entry = next(e for e in entries if e["tool"] == "brix__create_pipeline")
        assert entry["source_session"] == SAMPLE_SOURCE["session"]
        assert entry["source_model"] == SAMPLE_SOURCE["model"]
        assert entry["source_agent"] == SAMPLE_SOURCE["agent"]

    @pytest.mark.asyncio
    async def test_create_pipeline_without_source_no_crash(self, patch_audit_db):
        """source is optional — handler must not crash when missing."""
        result = await _handle_create_pipeline({"name": "no-source-pipeline"})
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        entry = next(e for e in entries if e["tool"] == "brix__create_pipeline")
        assert entry["source_session"] is None

    @pytest.mark.asyncio
    async def test_delete_pipeline_writes_audit(self, tmp_path, patch_audit_db):
        """delete_pipeline writes to audit_log after successful deletion."""
        # First create a pipeline to delete
        await _handle_create_pipeline({"name": "to-delete", "source": SAMPLE_SOURCE})
        patch_audit_db.get_audit_log()  # consume create entry

        result = await _handle_delete_pipeline({
            "name": "to-delete",
            "force": True,
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__delete_pipeline" for e in entries)

    @pytest.mark.asyncio
    async def test_update_pipeline_writes_audit(self, patch_audit_db):
        """update_pipeline writes to audit_log when fields change."""
        # Create first
        await _handle_create_pipeline({"name": "upd-pipe"})
        result = await _handle_update_pipeline({
            "name": "upd-pipe",
            "description": "New description",
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__update_pipeline" for e in entries)
        entry = next(e for e in entries if e["tool"] == "brix__update_pipeline")
        assert entry["source_session"] == SAMPLE_SOURCE["session"]

    @pytest.mark.asyncio
    async def test_add_step_writes_audit(self, patch_audit_db):
        """add_step writes to audit_log."""
        await _handle_create_pipeline({"name": "step-pipe"})
        result = await _handle_add_step({
            "pipeline_id": "step-pipe",
            "step_id": "my-step",
            "type": "set",
            "values": {"x": "1"},
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__add_step" for e in entries)

    @pytest.mark.asyncio
    async def test_remove_step_writes_audit(self, patch_audit_db):
        """remove_step writes to audit_log."""
        await _handle_create_pipeline({
            "name": "rmstep-pipe",
            "steps": [{"id": "kill-me", "type": "set", "values": {"x": "1"}}],
        })
        result = await _handle_remove_step({
            "pipeline_id": "rmstep-pipe",
            "step_id": "kill-me",
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__remove_step" for e in entries)

    @pytest.mark.asyncio
    async def test_update_step_writes_audit(self, tmp_path, patch_audit_db):
        """update_step writes to audit_log."""
        import brix.mcp_server as mcp_mod
        from brix.pipeline_store import PipelineStore
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save({
            "name": "updstep-pipe",
            "version": "1.0.0",
            "steps": [{"id": "my-step", "type": "set", "values": {"x": "1"}}],
        })
        result = await _handle_update_step({
            "pipeline_name": "updstep-pipe",
            "step_id": "my-step",
            "updates": {"values": {"x": "2"}},
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__update_step" for e in entries)

    @pytest.mark.asyncio
    async def test_create_helper_writes_audit(self, patch_audit_db, tmp_path, monkeypatch):
        """create_helper writes to audit_log."""
        import brix.mcp_server as mcp_mod

        def _fake_managed_dir():
            d = tmp_path / "helpers"
            d.mkdir(parents=True, exist_ok=True)
            return d

        monkeypatch.setattr("brix.mcp_handlers._shared._managed_helper_dir", _fake_managed_dir)
        result = await _handle_create_helper({
            "name": "my-helper",
            "code": "import json, sys\nprint(json.dumps({}))",
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__create_helper" for e in entries)
        entry = next(e for e in entries if e["tool"] == "brix__create_helper")
        assert entry["source_agent"] == SAMPLE_SOURCE["agent"]

    @pytest.mark.asyncio
    async def test_register_helper_writes_audit(self, tmp_path, patch_audit_db, monkeypatch):
        """register_helper writes to audit_log."""
        import brix.mcp_server as mcp_mod
        from brix.helper_registry import HelperRegistry

        script = tmp_path / "my_reg_helper.py"
        script.write_text("import json, sys\nprint(json.dumps({}))")

        # Use isolated registry
        reg = HelperRegistry(registry_path=tmp_path / "registry.yaml")

        def _fake_registry():
            return reg

        monkeypatch.setattr("brix.mcp_handlers.helpers.HelperRegistry", lambda: reg)

        result = await _handle_register_helper({
            "name": "reg-helper",
            "script": str(script),
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__register_helper" for e in entries)

    @pytest.mark.asyncio
    async def test_update_helper_remove_writes_audit(self, tmp_path, patch_audit_db, monkeypatch):
        """update_helper with action=remove writes to audit_log."""
        import brix.mcp_server as mcp_mod
        from brix.helper_registry import HelperRegistry

        script = tmp_path / "upd_helper.py"
        script.write_text("import json, sys\nprint(json.dumps({}))")
        reg = HelperRegistry(registry_path=tmp_path / "registry.yaml")
        reg.register(name="upd-helper", script=str(script))
        monkeypatch.setattr("brix.mcp_handlers.helpers.HelperRegistry", lambda: reg)

        result = await _handle_update_helper({
            "name": "upd-helper",
            "action": "remove",
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__update_helper" for e in entries)

    @pytest.mark.asyncio
    async def test_delete_helper_writes_audit(self, tmp_path, patch_audit_db, monkeypatch):
        """delete_helper writes to audit_log after successful deletion."""
        import brix.mcp_server as mcp_mod
        from brix.helper_registry import HelperRegistry

        script = tmp_path / "del_helper.py"
        script.write_text("import json, sys\nprint(json.dumps({}))")
        reg = HelperRegistry(registry_path=tmp_path / "registry.yaml")
        reg.register(name="del-helper", script=str(script))
        monkeypatch.setattr("brix.mcp_handlers.helpers.HelperRegistry", lambda: reg)

        result = await _handle_delete_helper({
            "name": "del-helper",
            "force": True,
            "source": SAMPLE_SOURCE,
        })
        assert result["success"] is True
        entries = patch_audit_db.get_audit_log()
        assert any(e["tool"] == "brix__delete_helper" for e in entries)
