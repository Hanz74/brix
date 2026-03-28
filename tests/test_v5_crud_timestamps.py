"""Tests for T-BRIX-V5-01: Full CRUD + Timestamps + delete_pipeline + UUIDs.

Covers:
1. delete_pipeline — history check, force flag, file deletion
2. get_step — basic lookup, recursive lookup, not-found
3. delete_helper — pipeline scan, force flag, delete_script option
4. delete_run — success, not-found
5. Pipeline timestamps — created_at / updated_at on create + update ops
6. Helper timestamps — created_at / updated_at on register + update
7. Pipeline UUIDs — id assigned on create, preserved on update, lookup by id
8. Helper UUIDs — id assigned on register, preserved on update, lookup by id
"""
import pytest
import asyncio
from pathlib import Path
from unittest.mock import patch

import brix.mcp_server as mcp_module
from brix.mcp_server import (
    _handle_create_pipeline,
    _handle_get_pipeline,
    _handle_add_step,
    _handle_remove_step,
    _handle_update_step,
    _handle_update_pipeline,
    _handle_delete_pipeline,
    _handle_get_step,
    _handle_delete_helper,
    _handle_delete_run,
    _handle_register_helper,
    _handle_get_helper,
    _handle_update_helper,
    _handle_list_helpers,
)
from brix.helper_registry import HelperRegistry
from brix.history import RunHistory
from brix.pipeline_store import PipelineStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_pipeline_dir(tmp_path, monkeypatch):
    """Redirect PIPELINE_DIR to a temp directory for each test."""
    monkeypatch.setattr(mcp_module, "PIPELINE_DIR", tmp_path)
    return tmp_path


@pytest.fixture
def tmp_registry(tmp_path):
    """Return a HelperRegistry backed by a temp file."""
    return HelperRegistry(registry_path=tmp_path / "registry.yaml")


@pytest.fixture
def tmp_history(tmp_path):
    """Return a RunHistory backed by a temp SQLite file."""
    return RunHistory(db_path=tmp_path / "history.db")


# ---------------------------------------------------------------------------
# 1. delete_pipeline
# ---------------------------------------------------------------------------

class TestDeletePipeline:
    @pytest.mark.asyncio
    async def test_delete_existing_pipeline_no_history(self, tmp_pipeline_dir):
        """delete_pipeline succeeds when no run history exists."""
        await _handle_create_pipeline({"name": "my-pipe", "version": "1.0.0"})

        result = await _handle_delete_pipeline({"name": "my-pipe"})

        assert result["success"] is True
        assert result["deleted_pipeline"] == "my-pipe"
        assert not (tmp_pipeline_dir / "my-pipe.yaml").exists()

    @pytest.mark.asyncio
    async def test_delete_not_found(self, tmp_pipeline_dir):
        """delete_pipeline returns error for unknown pipeline."""
        result = await _handle_delete_pipeline({"name": "ghost"})
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_delete_missing_name(self, tmp_pipeline_dir):
        """delete_pipeline requires name parameter."""
        result = await _handle_delete_pipeline({})
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_delete_warns_if_run_history_exists(self, tmp_pipeline_dir, tmp_path):
        """delete_pipeline returns warning when run history exists (no force)."""
        await _handle_create_pipeline({"name": "tracked-pipe"})

        # Inject a fake run
        history = RunHistory(db_path=tmp_path / "h.db")
        history.record_start("run-abc", "tracked-pipe", version="1.0.0")

        with patch("brix.mcp_handlers.pipelines.RunHistory", return_value=history):
            result = await _handle_delete_pipeline({"name": "tracked-pipe"})

        assert result["success"] is False
        assert "warning" in result
        assert result["run_count"] >= 1
        # File should still exist
        assert (tmp_pipeline_dir / "tracked-pipe.yaml").exists()

    @pytest.mark.asyncio
    async def test_delete_force_ignores_history(self, tmp_pipeline_dir, tmp_path):
        """delete_pipeline with force=true deletes even with run history."""
        await _handle_create_pipeline({"name": "forced-pipe"})

        history = RunHistory(db_path=tmp_path / "h.db")
        history.record_start("run-xyz", "forced-pipe", version="1.0.0")

        with patch("brix.mcp_handlers.pipelines.RunHistory", return_value=history):
            result = await _handle_delete_pipeline({"name": "forced-pipe", "force": True})

        assert result["success"] is True
        assert not (tmp_pipeline_dir / "forced-pipe.yaml").exists()


# ---------------------------------------------------------------------------
# 2. get_step
# ---------------------------------------------------------------------------

class TestGetStep:
    @pytest.mark.asyncio
    async def test_get_top_level_step(self, tmp_pipeline_dir):
        """get_step returns a top-level step by ID."""
        await _handle_create_pipeline({
            "name": "step-pipe",
            "steps": [{"id": "fetch", "type": "http", "url": "https://example.com"}],
        })

        result = await _handle_get_step({
            "pipeline_name": "step-pipe",
            "step_id": "fetch",
        })

        assert result["success"] is True
        assert result["step"]["id"] == "fetch"
        assert result["step"]["type"] == "http"

    @pytest.mark.asyncio
    async def test_get_step_not_found(self, tmp_pipeline_dir):
        """get_step returns error for unknown step ID."""
        await _handle_create_pipeline({"name": "empty-pipe", "steps": []})

        result = await _handle_get_step({
            "pipeline_name": "empty-pipe",
            "step_id": "nonexistent",
        })

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_get_step_pipeline_not_found(self, tmp_pipeline_dir):
        result = await _handle_get_step({
            "pipeline_name": "ghost-pipe",
            "step_id": "any",
        })
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_get_step_recursive_in_repeat(self, tmp_pipeline_dir):
        """get_step finds steps nested inside a repeat block."""
        steps = [
            {
                "id": "loop",
                "type": "repeat",
                "max_iterations": 5,
                "until": "{{ True }}",
                "sequence": [
                    {"id": "inner_check", "type": "http", "url": "https://api.test/check"},
                ],
            }
        ]
        await _handle_create_pipeline({"name": "nested-pipe", "steps": steps})

        result = await _handle_get_step({
            "pipeline_name": "nested-pipe",
            "step_id": "inner_check",
        })

        assert result["success"] is True
        assert result["step"]["id"] == "inner_check"

    @pytest.mark.asyncio
    async def test_get_step_missing_params(self, tmp_pipeline_dir):
        """get_step returns error when required params are missing."""
        result = await _handle_get_step({})
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_get_step_returns_all_fields(self, tmp_pipeline_dir):
        """get_step returns full step dict including all custom fields."""
        steps = [
            {
                "id": "classify",
                "type": "python",
                "script": "/app/helpers/classify.py",
                "params": {"model": "gpt-4"},
                "on_error": "skip",
                "timeout": "5m",
            }
        ]
        await _handle_create_pipeline({"name": "full-pipe", "steps": steps})

        result = await _handle_get_step({
            "pipeline_name": "full-pipe",
            "step_id": "classify",
        })

        assert result["success"] is True
        step = result["step"]
        assert step["params"] == {"model": "gpt-4"}
        assert step["on_error"] == "skip"
        assert step["timeout"] == "5m"


# ---------------------------------------------------------------------------
# 3. delete_helper
# ---------------------------------------------------------------------------

class TestDeleteHelper:
    @pytest.mark.asyncio
    async def test_delete_helper_no_references(self, tmp_pipeline_dir, tmp_path):
        """delete_helper succeeds when no pipelines reference the helper."""
        registry = HelperRegistry(registry_path=tmp_path / "registry.yaml")
        registry.register("parse", "/app/helpers/parse.py")

        with (
            patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry),
            patch("brix.mcp_handlers.helpers._scan_pipelines_for_helper", return_value=[]),
        ):
            result = await _handle_delete_helper({"name": "parse"})

        assert result["success"] is True
        assert result["deleted_helper"] == "parse"
        assert result["affected_pipelines"] == []
        assert registry.get("parse") is None

    @pytest.mark.asyncio
    async def test_delete_helper_warns_if_referenced(self, tmp_path):
        """delete_helper warns if pipelines reference the helper (no force)."""
        registry = HelperRegistry(registry_path=tmp_path / "registry.yaml")
        registry.register("classify", "/app/helpers/classify.py")

        with (
            patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry),
            patch("brix.mcp_handlers.helpers._scan_pipelines_for_helper", return_value=["pipe-a", "pipe-b"]),
        ):
            result = await _handle_delete_helper({"name": "classify"})

        assert result["success"] is False
        assert "warning" in result
        assert result["affected_pipelines"] == ["pipe-a", "pipe-b"]
        # Helper should still exist
        assert registry.get("classify") is not None

    @pytest.mark.asyncio
    async def test_delete_helper_force(self, tmp_path):
        """delete_helper with force=true removes even with pipeline references."""
        registry = HelperRegistry(registry_path=tmp_path / "registry.yaml")
        registry.register("classify", "/app/helpers/classify.py")

        with (
            patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry),
            patch("brix.mcp_handlers.helpers._scan_pipelines_for_helper", return_value=["pipe-a"]),
        ):
            result = await _handle_delete_helper({"name": "classify", "force": True})

        assert result["success"] is True
        assert registry.get("classify") is None

    @pytest.mark.asyncio
    async def test_delete_helper_not_found(self, tmp_path):
        """delete_helper returns error if helper does not exist."""
        registry = HelperRegistry(registry_path=tmp_path / "registry.yaml")

        with (
            patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry),
            patch("brix.mcp_handlers.helpers._scan_pipelines_for_helper", return_value=[]),
        ):
            result = await _handle_delete_helper({"name": "ghost"})

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_delete_helper_missing_name(self, tmp_path):
        result = await _handle_delete_helper({})
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_delete_helper_delete_script(self, tmp_path):
        """delete_helper with delete_script=true removes the script file."""
        script = tmp_path / "my_helper.py"
        script.write_text("# helper")

        registry = HelperRegistry(registry_path=tmp_path / "registry.yaml")
        registry.register("my_helper", str(script))

        with (
            patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry),
            patch("brix.mcp_handlers.helpers._scan_pipelines_for_helper", return_value=[]),
        ):
            result = await _handle_delete_helper({
                "name": "my_helper",
                "delete_script": True,
            })

        assert result["success"] is True
        assert "deleted_script" in result
        assert not script.exists()


# ---------------------------------------------------------------------------
# 4. delete_run
# ---------------------------------------------------------------------------

class TestDeleteRun:
    @pytest.mark.asyncio
    async def test_delete_existing_run(self, tmp_path):
        """delete_run removes an existing run from history."""
        history = RunHistory(db_path=tmp_path / "h.db")
        history.record_start("run-del-01", "my-pipe")
        history.record_finish("run-del-01", success=True, duration=1.0)

        with patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = await _handle_delete_run({"run_id": "run-del-01"})

        assert result["success"] is True
        assert result["deleted_run_id"] == "run-del-01"
        assert history.get_run("run-del-01") is None

    @pytest.mark.asyncio
    async def test_delete_run_not_found(self, tmp_path):
        """delete_run returns error if run_id does not exist."""
        history = RunHistory(db_path=tmp_path / "h.db")

        with patch("brix.mcp_handlers.runs.RunHistory", return_value=history):
            result = await _handle_delete_run({"run_id": "run-ghost"})

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_delete_run_missing_run_id(self):
        result = await _handle_delete_run({})
        assert result["success"] is False


# ---------------------------------------------------------------------------
# 5. Pipeline timestamps
# ---------------------------------------------------------------------------

class TestPipelineTimestamps:
    @pytest.mark.asyncio
    async def test_create_pipeline_sets_created_at(self, tmp_pipeline_dir):
        """create_pipeline sets created_at and updated_at timestamps."""
        result = await _handle_create_pipeline({"name": "ts-pipe"})
        assert result["success"] is True

        pipeline = await _handle_get_pipeline({"pipeline_id": "ts-pipe"})
        assert "created_at" in pipeline
        assert "updated_at" in pipeline
        assert pipeline["created_at"] is not None
        assert pipeline["updated_at"] is not None

    @pytest.mark.asyncio
    async def test_update_pipeline_refreshes_updated_at(self, tmp_pipeline_dir):
        """update_pipeline refreshes updated_at but preserves created_at."""
        import time
        await _handle_create_pipeline({"name": "ts-upd"})
        pipeline_before = await _handle_get_pipeline({"pipeline_id": "ts-upd"})
        created_at_before = pipeline_before["created_at"]

        time.sleep(0.01)

        await _handle_update_pipeline({"name": "ts-upd", "description": "updated"})
        pipeline_after = await _handle_get_pipeline({"pipeline_id": "ts-upd"})

        assert pipeline_after["created_at"] == created_at_before
        assert pipeline_after["updated_at"] >= pipeline_before["updated_at"]

    @pytest.mark.asyncio
    async def test_add_step_updates_updated_at(self, tmp_pipeline_dir):
        """add_step refreshes the pipeline's updated_at timestamp."""
        import time
        await _handle_create_pipeline({"name": "ts-add"})
        pipeline_before = await _handle_get_pipeline({"pipeline_id": "ts-add"})

        time.sleep(0.01)

        await _handle_add_step({
            "pipeline_id": "ts-add",
            "step_id": "my_step",
            "type": "set",
            "values": {},
        })
        pipeline_after = await _handle_get_pipeline({"pipeline_id": "ts-add"})
        assert pipeline_after["updated_at"] >= pipeline_before["updated_at"]

    @pytest.mark.asyncio
    async def test_remove_step_updates_updated_at(self, tmp_pipeline_dir):
        """remove_step refreshes the pipeline's updated_at timestamp."""
        import time
        await _handle_create_pipeline({
            "name": "ts-rem",
            "steps": [{"id": "s1", "type": "set", "values": {}}],
        })
        pipeline_before = await _handle_get_pipeline({"pipeline_id": "ts-rem"})

        time.sleep(0.01)

        await _handle_remove_step({"pipeline_id": "ts-rem", "step_id": "s1"})
        pipeline_after = await _handle_get_pipeline({"pipeline_id": "ts-rem"})
        assert pipeline_after["updated_at"] >= pipeline_before["updated_at"]

    @pytest.mark.asyncio
    async def test_update_step_updates_updated_at(self, tmp_pipeline_dir):
        """update_step refreshes the pipeline's updated_at timestamp."""
        import time
        await _handle_create_pipeline({
            "name": "ts-upd-step",
            "steps": [{"id": "s1", "type": "set", "values": {}}],
        })
        pipeline_before = await _handle_get_pipeline({"pipeline_id": "ts-upd-step"})

        time.sleep(0.01)

        await _handle_update_step({
            "pipeline_name": "ts-upd-step",
            "step_id": "s1",
            "updates": {"values": {"key": "val"}},
        })
        pipeline_after = await _handle_get_pipeline({"pipeline_id": "ts-upd-step"})
        assert pipeline_after["updated_at"] >= pipeline_before["updated_at"]

    @pytest.mark.asyncio
    async def test_created_at_preserved_on_resave(self, tmp_pipeline_dir):
        """created_at stays the same even after multiple saves."""
        import time
        await _handle_create_pipeline({"name": "ts-stable"})
        p1 = await _handle_get_pipeline({"pipeline_id": "ts-stable"})

        time.sleep(0.01)

        await _handle_update_pipeline({"name": "ts-stable", "description": "v2"})
        p2 = await _handle_get_pipeline({"pipeline_id": "ts-stable"})

        time.sleep(0.01)

        await _handle_update_pipeline({"name": "ts-stable", "description": "v3"})
        p3 = await _handle_get_pipeline({"pipeline_id": "ts-stable"})

        assert p1["created_at"] == p2["created_at"] == p3["created_at"]


# ---------------------------------------------------------------------------
# 6. Helper timestamps
# ---------------------------------------------------------------------------

class TestHelperTimestamps:
    def test_register_helper_sets_timestamps(self, tmp_path):
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        entry = registry.register("ts_helper", "/tmp/ts.py")
        assert entry.created_at is not None
        assert entry.updated_at is not None

    def test_update_helper_refreshes_updated_at(self, tmp_path):
        import time
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        entry1 = registry.register("ts_upd", "/tmp/ts.py")
        time.sleep(0.01)
        entry2 = registry.update("ts_upd", description="new desc")
        assert entry2.created_at == entry1.created_at
        assert entry2.updated_at >= entry1.updated_at  # type: ignore[operator]

    def test_register_preserves_created_at_on_overwrite(self, tmp_path):
        import time
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        entry1 = registry.register("overwrite_me", "/tmp/a.py")
        time.sleep(0.01)
        entry2 = registry.register("overwrite_me", "/tmp/b.py", description="updated")
        assert entry2.created_at == entry1.created_at

    @pytest.mark.asyncio
    async def test_register_helper_mcp_returns_timestamps(self, tmp_path):
        """register_helper MCP handler returns timestamps in response."""
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        with patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry):
            result = await _handle_register_helper({
                "name": "ts_mcp",
                "script": "/tmp/ts_mcp.py",
            })
        assert result["success"] is True
        helper = result["helper"]
        assert "created_at" in helper
        assert "updated_at" in helper

    @pytest.mark.asyncio
    async def test_get_helper_returns_timestamps(self, tmp_path):
        """get_helper MCP handler includes timestamps."""
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        registry.register("ts_get", "/tmp/ts_get.py")
        with patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry):
            result = await _handle_get_helper({"name": "ts_get"})
        assert result["success"] is True
        assert "created_at" in result["helper"]
        assert "updated_at" in result["helper"]

    @pytest.mark.asyncio
    async def test_list_helpers_returns_timestamps(self, tmp_path):
        """list_helpers includes timestamps for each helper."""
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        registry.register("ts_list1", "/tmp/a.py")
        registry.register("ts_list2", "/tmp/b.py")
        with patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry):
            result = await _handle_list_helpers({})
        assert result["success"] is True
        for h in result["helpers"]:
            assert "created_at" in h
            assert "updated_at" in h


# ---------------------------------------------------------------------------
# 7. Pipeline UUIDs
# ---------------------------------------------------------------------------

class TestPipelineUUIDs:
    @pytest.mark.asyncio
    async def test_create_pipeline_assigns_uuid(self, tmp_pipeline_dir):
        """create_pipeline assigns a stable UUID id."""
        result = await _handle_create_pipeline({"name": "uuid-pipe"})
        assert result["success"] is True
        assert "id" in result
        # Should be a valid UUID4 format
        import uuid
        parsed = uuid.UUID(result["id"])
        assert parsed.version == 4

    @pytest.mark.asyncio
    async def test_get_pipeline_returns_uuid(self, tmp_pipeline_dir):
        """get_pipeline returns the uuid id field."""
        create_result = await _handle_create_pipeline({"name": "uuid-get"})
        get_result = await _handle_get_pipeline({"pipeline_id": "uuid-get"})
        assert "id" in get_result
        assert get_result["id"] == create_result["id"]

    @pytest.mark.asyncio
    async def test_pipeline_uuid_preserved_across_updates(self, tmp_pipeline_dir):
        """Pipeline UUID stays the same after update_pipeline."""
        create_result = await _handle_create_pipeline({"name": "uuid-stable"})
        original_id = create_result["id"]

        await _handle_update_pipeline({"name": "uuid-stable", "description": "changed"})
        get_result = await _handle_get_pipeline({"pipeline_id": "uuid-stable"})
        assert get_result["id"] == original_id

    @pytest.mark.asyncio
    async def test_lookup_pipeline_by_uuid(self, tmp_pipeline_dir):
        """load_pipeline_yaml resolves pipeline by UUID as well as name."""
        from brix.mcp_server import _load_pipeline_yaml
        create_result = await _handle_create_pipeline({"name": "uuid-lookup"})
        pipeline_id = create_result["id"]

        # Should be loadable by UUID
        data = _load_pipeline_yaml(pipeline_id)
        assert data["name"] == "uuid-lookup"

    @pytest.mark.asyncio
    async def test_get_pipeline_by_uuid(self, tmp_pipeline_dir):
        """get_pipeline works with a UUID as pipeline_id argument."""
        create_result = await _handle_create_pipeline({"name": "uuid-by-id"})
        pipeline_uuid = create_result["id"]

        result = await _handle_get_pipeline({"pipeline_id": pipeline_uuid})
        assert result.get("name") == "uuid-by-id"

    @pytest.mark.asyncio
    async def test_pipeline_uuid_unique_per_pipeline(self, tmp_pipeline_dir):
        """Each pipeline gets a different UUID."""
        r1 = await _handle_create_pipeline({"name": "uuid-a"})
        r2 = await _handle_create_pipeline({"name": "uuid-b"})
        assert r1["id"] != r2["id"]


# ---------------------------------------------------------------------------
# 8. Helper UUIDs
# ---------------------------------------------------------------------------

class TestHelperUUIDs:
    def test_register_assigns_uuid(self, tmp_path):
        """register() assigns a stable UUID id."""
        import uuid
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        entry = registry.register("uuid_helper", "/tmp/uuid.py")
        assert entry.id is not None
        parsed = uuid.UUID(entry.id)
        assert parsed.version == 4

    def test_register_preserves_uuid_on_overwrite(self, tmp_path):
        """Re-registering a helper preserves its UUID."""
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        entry1 = registry.register("uuid_stable", "/tmp/a.py")
        original_id = entry1.id

        entry2 = registry.register("uuid_stable", "/tmp/b.py", description="v2")
        assert entry2.id == original_id

    def test_get_helper_by_uuid(self, tmp_path):
        """get() finds a helper by its UUID as well as by name."""
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        entry = registry.register("uuid_lookup", "/tmp/lk.py")
        helper_id = entry.id

        # Lookup by UUID
        found = registry.get(helper_id)
        assert found is not None
        assert found.name == "uuid_lookup"

    def test_get_helper_uuid_unique(self, tmp_path):
        """Each helper gets a unique UUID."""
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        e1 = registry.register("h1", "/tmp/a.py")
        e2 = registry.register("h2", "/tmp/b.py")
        assert e1.id != e2.id

    @pytest.mark.asyncio
    async def test_get_helper_mcp_returns_uuid(self, tmp_path):
        """get_helper MCP handler includes id (UUID) in response."""
        registry = HelperRegistry(registry_path=tmp_path / "r.yaml")
        registry.register("uuid_mcp", "/tmp/uuid_mcp.py")
        with patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry):
            result = await _handle_get_helper({"name": "uuid_mcp"})
        assert result["success"] is True
        assert "id" in result["helper"]


# ---------------------------------------------------------------------------
# 9. RunHistory.delete_run (unit level)
# ---------------------------------------------------------------------------

class TestRunHistoryDeleteRun:
    def test_delete_run_returns_true(self, tmp_path):
        history = RunHistory(db_path=tmp_path / "h.db")
        history.record_start("run-001", "pipe-a")
        history.record_finish("run-001", success=True, duration=0.5)

        deleted = history.delete_run("run-001")
        assert deleted is True
        assert history.get_run("run-001") is None

    def test_delete_run_not_found_returns_false(self, tmp_path):
        history = RunHistory(db_path=tmp_path / "h.db")
        deleted = history.delete_run("run-ghost")
        assert deleted is False

    def test_delete_run_does_not_affect_other_runs(self, tmp_path):
        history = RunHistory(db_path=tmp_path / "h.db")
        history.record_start("run-keep", "pipe-a")
        history.record_start("run-del", "pipe-a")

        history.delete_run("run-del")

        assert history.get_run("run-keep") is not None
        assert history.get_run("run-del") is None


# ---------------------------------------------------------------------------
# 10. PipelineStore.resolve and find_by_id
# ---------------------------------------------------------------------------

class TestPipelineStoreResolveAndFindById:
    def test_resolve_by_name(self, tmp_path):
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        (tmp_path / "mypipe.yaml").write_text("name: mypipe\nsteps: []\n")
        resolved = store.resolve("mypipe")
        assert resolved == "mypipe"

    def test_resolve_by_id(self, tmp_path):
        import uuid
        pipe_id = str(uuid.uuid4())
        (tmp_path / "idpipe.yaml").write_text(
            f"name: idpipe\nid: {pipe_id}\nsteps: []\n"
        )
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        resolved = store.resolve(pipe_id)
        assert resolved == "idpipe"

    def test_resolve_not_found_raises(self, tmp_path):
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        with pytest.raises(FileNotFoundError):
            store.resolve("definitely-not-here")

    def test_find_by_id_returns_name(self, tmp_path):
        import uuid
        pipe_id = str(uuid.uuid4())
        (tmp_path / "findme.yaml").write_text(
            f"name: findme\nid: {pipe_id}\nsteps: []\n"
        )
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        name = store.find_by_id(pipe_id)
        assert name == "findme"

    def test_find_by_id_none_when_missing(self, tmp_path):
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        assert store.find_by_id("00000000-0000-0000-0000-000000000000") is None
