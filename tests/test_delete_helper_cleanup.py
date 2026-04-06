from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from brix.helper_registry import HelperRegistry
from brix.mcp_handlers.helpers import _handle_delete_helper
from brix.mcp_handlers.pipelines import _handle_create_pipeline, _handle_delete_pipeline
from brix.pipeline_store import PipelineStore
from brix.startup_sync import _sync_helpers, run_startup_sync


@pytest.mark.asyncio
async def test_delete_helper_removes_managed_script_and_prevents_restart_reregistration(
    tmp_path, isolated_db
):
    helper_dir = tmp_path / "helpers"
    helper_dir.mkdir()
    script = helper_dir / "restart_bug.py"
    script.write_text("# helper\n", encoding="utf-8")

    registry = HelperRegistry(registry_path=tmp_path / "registry.yaml", db=isolated_db)
    registry.register("restart_bug", str(script))

    with (
        patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry),
        patch("brix.mcp_handlers.helpers._scan_pipelines_for_helper", return_value=[]),
        patch("brix.mcp_handlers.helpers._helper_script_delete_roots", return_value=(helper_dir,)),
        patch("brix.startup_sync._HELPER_SEARCH_PATHS", [helper_dir]),
    ):
        result = await _handle_delete_helper({"name": "restart_bug"})

        assert result["success"] is True
        assert result["deleted_helper"] == "restart_bug"
        assert result["deleted_script"] == str(script)
        assert not script.exists()
        assert isolated_db.get_helper("restart_bug") is None

        # Simulate container restart: deleted helper must not come back from disk.
        assert _sync_helpers(isolated_db) == 0
        assert isolated_db.get_helper("restart_bug") is None


@pytest.mark.asyncio
async def test_delete_helper_keeps_external_script_unless_explicit_delete(tmp_path, isolated_db):
    external_dir = tmp_path / "external_helpers"
    external_dir.mkdir()
    script = external_dir / "keep_me.py"
    script.write_text("# helper\n", encoding="utf-8")

    registry = HelperRegistry(registry_path=tmp_path / "registry.yaml", db=isolated_db)
    registry.register("keep_me", str(script))

    with (
        patch("brix.mcp_handlers.helpers.HelperRegistry", return_value=registry),
        patch("brix.mcp_handlers.helpers._scan_pipelines_for_helper", return_value=[]),
        patch("brix.mcp_handlers.helpers._helper_script_delete_roots", return_value=(tmp_path / "managed",)),
    ):
        result = await _handle_delete_helper({"name": "keep_me"})

    assert result["success"] is True
    assert "deleted_script" not in result
    assert script.exists()


@pytest.mark.asyncio
async def test_delete_pipeline_removes_files_from_all_search_paths(tmp_path, monkeypatch, isolated_db):
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    mounted_dir = tmp_path / "mounted_pipelines"
    mounted_dir.mkdir()

    import brix.mcp_server as mcp_module

    monkeypatch.setattr(mcp_module, "PIPELINE_DIR", pipelines_dir)
    store = PipelineStore(pipelines_dir=pipelines_dir, search_paths=[pipelines_dir, mounted_dir], db=isolated_db)

    await _handle_create_pipeline({"name": "stale-pipe"})

    mounted_copy = mounted_dir / "stale-pipe.yaml"
    mounted_copy.write_text("name: stale-pipe\nsteps: []\n", encoding="utf-8")

    with patch("brix.mcp_handlers.pipelines.PipelineStore", return_value=store):
        result = await _handle_delete_pipeline({"name": "stale-pipe"})

    assert result["success"] is True
    assert not (pipelines_dir / "stale-pipe.yaml").exists()
    assert not mounted_copy.exists()
    assert isolated_db.get_pipeline("stale-pipe") is None

    # Pipelines are not re-imported from disk on startup sync.
    summary = run_startup_sync(isolated_db)
    assert summary["pipelines_imported"] == 0
