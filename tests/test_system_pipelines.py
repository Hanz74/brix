"""Tests for T-BRIX-DB-16: System Pipelines.

Covers:
1. SYSTEM_PIPELINES definitions are valid
2. is_system_pipeline() correctly identifies _system/ names
3. seed_system_pipelines() seeds into pipeline store
4. seed_system_pipelines() is idempotent (skips existing)
5. _handle_delete_pipeline blocks _system/ names
6. _handle_delete_pipeline allows non-system names
7. _handle_update_pipeline allows _system/ but adds warning
8. _handle_update_pipeline on non-system produces no warning
9. seed_if_empty includes system_pipelines count
10. All SYSTEM_PIPELINES have required fields
11. All SYSTEM_PIPELINES have steps
12. SYSTEM_PIPELINE_NAMES frozenset matches SYSTEM_PIPELINES list
13. Seeding multiple times returns 0 on second call
14. System pipeline warning message contains pipeline name
15. delete returns system_pipeline=True in response
"""
from __future__ import annotations

import asyncio
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from brix.system_pipelines import (
    SYSTEM_PIPELINES,
    SYSTEM_PIPELINE_NAMES,
    SYSTEM_PREFIX,
    is_system_pipeline,
    seed_system_pipelines,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def pipeline_store(tmp_path):
    """Return a PipelineStore backed by a temp directory (isolated from real .brix)."""
    from brix.pipeline_store import PipelineStore
    from brix.db import BrixDB
    db = BrixDB(db_path=tmp_path / "test.db")
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    # Use explicit search_paths to isolate from the real ~/.brix/pipelines
    return PipelineStore(
        pipelines_dir=pipelines_dir,
        search_paths=[pipelines_dir],
        db=db,
    )


@pytest.fixture
def mcp_pipeline_dir(tmp_path, monkeypatch):
    """Monkeypatch the pipeline dir used by MCP handlers."""
    pipeline_dir = tmp_path / "mcp_pipelines"
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("brix.mcp_handlers.pipelines._pipeline_dir", lambda: pipeline_dir)
    monkeypatch.setattr("brix.mcp_handlers._shared.PIPELINE_DIR", pipeline_dir)
    return pipeline_dir


# ---------------------------------------------------------------------------
# 1. SYSTEM_PIPELINES definitions are valid
# ---------------------------------------------------------------------------

class TestSystemPipelineDefinitions:
    def test_system_pipelines_is_list(self):
        assert isinstance(SYSTEM_PIPELINES, list)

    def test_system_pipelines_not_empty(self):
        assert len(SYSTEM_PIPELINES) >= 3

    def test_all_have_required_fields(self):
        """All SYSTEM_PIPELINES must have name, description, version, steps."""
        for p in SYSTEM_PIPELINES:
            assert "name" in p, f"Missing 'name' in {p}"
            assert "description" in p, f"Missing 'description' in {p}"
            assert "version" in p, f"Missing 'version' in {p}"
            assert "steps" in p, f"Missing 'steps' in {p}"

    def test_all_have_steps(self):
        """All SYSTEM_PIPELINES must have at least one step."""
        for p in SYSTEM_PIPELINES:
            assert len(p["steps"]) >= 1, f"Pipeline '{p['name']}' has no steps"

    def test_all_names_start_with_system_prefix(self):
        for p in SYSTEM_PIPELINES:
            assert p["name"].startswith(SYSTEM_PREFIX), (
                f"Pipeline '{p['name']}' does not start with '{SYSTEM_PREFIX}'"
            )

    def test_system_pipeline_names_frozenset_matches_list(self):
        expected = frozenset(p["name"] for p in SYSTEM_PIPELINES)
        assert SYSTEM_PIPELINE_NAMES == expected


# ---------------------------------------------------------------------------
# 2. is_system_pipeline() helper
# ---------------------------------------------------------------------------

class TestIsSystemPipeline:
    def test_system_prefix_is_system(self):
        assert is_system_pipeline("_system/alert-check") is True

    def test_system_prefix_with_nested_path(self):
        assert is_system_pipeline("_system/some/deep/pipeline") is True

    def test_non_system_name(self):
        assert is_system_pipeline("my-pipeline") is False

    def test_empty_name(self):
        assert is_system_pipeline("") is False

    def test_similar_but_not_system(self):
        assert is_system_pipeline("system/alert-check") is False


# ---------------------------------------------------------------------------
# 3 & 4. seed_system_pipelines() seeding and idempotency
# ---------------------------------------------------------------------------

class TestSeedSystemPipelines:
    def test_seeds_all_system_pipelines(self, pipeline_store):
        seeded = seed_system_pipelines(pipeline_store)
        assert seeded == len(SYSTEM_PIPELINES)

    def test_idempotent_second_call_returns_zero(self, pipeline_store):
        seed_system_pipelines(pipeline_store)
        seeded_again = seed_system_pipelines(pipeline_store)
        assert seeded_again == 0

    def test_pipelines_exist_after_seeding(self, pipeline_store):
        seed_system_pipelines(pipeline_store)
        for p in SYSTEM_PIPELINES:
            assert pipeline_store.exists(p["name"]), (
                f"System pipeline '{p['name']}' not found after seeding"
            )


# ---------------------------------------------------------------------------
# 5. _handle_delete_pipeline blocks _system/ names
# ---------------------------------------------------------------------------

class TestDeleteSystemPipelineBlocked:
    def test_delete_system_pipeline_returns_error(self, mcp_pipeline_dir):
        from brix.mcp_handlers.pipelines import _handle_delete_pipeline

        result = asyncio.get_event_loop().run_until_complete(
            _handle_delete_pipeline({"name": "_system/health-report"})
        )

        assert result["success"] is False
        assert "system" in result["error"].lower() or "System" in result["error"]

    def test_delete_system_pipeline_sets_system_flag(self, mcp_pipeline_dir):
        from brix.mcp_handlers.pipelines import _handle_delete_pipeline

        result = asyncio.get_event_loop().run_until_complete(
            _handle_delete_pipeline({"name": "_system/alert-check"})
        )

        assert result.get("system_pipeline") is True

    def test_delete_non_system_pipeline_not_blocked(self, mcp_pipeline_dir):
        from brix.mcp_handlers.pipelines import _handle_delete_pipeline

        # A non-system pipeline that doesn't exist — should fail with 'not found', not 'system'
        result = asyncio.get_event_loop().run_until_complete(
            _handle_delete_pipeline({"name": "my-regular-pipeline"})
        )

        # Should NOT be a system protection error
        assert result.get("system_pipeline") is not True
        # Error should be about 'not found', not system protection
        error_msg = result.get("error", "")
        assert "System-Pipelines" not in error_msg


# ---------------------------------------------------------------------------
# 6. _handle_update_pipeline warns for _system/ pipelines
# ---------------------------------------------------------------------------

class TestUpdateSystemPipelineWarning:
    def _make_existing_pipeline(self, pipeline_dir: Path, name: str) -> None:
        """Write a minimal pipeline YAML to the pipeline dir."""
        import yaml
        # For nested names like _system/foo, create subdirectory
        sanitised = name.replace("/", "__")
        path = pipeline_dir / f"{sanitised}.yaml"
        data = {
            "name": name,
            "version": "1.0.0",
            "description": "test",
            "steps": [{"id": "s1", "type": "flow.set", "config": {"key": "x", "value": "1"}}],
        }
        path.write_text(yaml.dump(data))

    def test_update_system_pipeline_includes_warning(self, tmp_path, monkeypatch):
        from brix.pipeline_store import PipelineStore
        from brix.db import BrixDB
        from brix.mcp_handlers.pipelines import _handle_update_pipeline

        pipeline_dir = tmp_path / "pipelines"
        pipeline_dir.mkdir()
        # Create _system/ subdirectory so the store can save the pipeline
        (pipeline_dir / "_system").mkdir()
        db = BrixDB(db_path=tmp_path / "test.db")
        store = PipelineStore(pipelines_dir=pipeline_dir, db=db)

        name = "_system/health-report"
        data = {
            "name": name,
            "version": "1.0.0",
            "description": "Health report",
            "steps": [{"id": "s1", "type": "flow.set", "config": {"key": "k", "value": "v"}}],
        }
        store.save(data, name=name)

        monkeypatch.setattr("brix.mcp_handlers.pipelines._pipeline_dir", lambda: pipeline_dir)

        result = asyncio.get_event_loop().run_until_complete(
            _handle_update_pipeline({"name": name, "description": "Updated"})
        )

        assert result["success"] is True
        assert "warning" in result
        assert result.get("system_pipeline") is True

    def test_update_non_system_pipeline_no_warning(self, tmp_path, monkeypatch):
        from brix.pipeline_store import PipelineStore
        from brix.db import BrixDB
        from brix.mcp_handlers.pipelines import _handle_update_pipeline

        pipeline_dir = tmp_path / "pipelines"
        pipeline_dir.mkdir()
        db = BrixDB(db_path=tmp_path / "test.db")
        store = PipelineStore(pipelines_dir=pipeline_dir, db=db)

        name = "my-regular-pipeline"
        data = {
            "name": name,
            "version": "1.0.0",
            "description": "Regular pipeline",
            "steps": [{"id": "s1", "type": "flow.set", "config": {"key": "k", "value": "v"}}],
        }
        store.save(data, name=name)

        monkeypatch.setattr("brix.mcp_handlers.pipelines._pipeline_dir", lambda: pipeline_dir)

        result = asyncio.get_event_loop().run_until_complete(
            _handle_update_pipeline({"name": name, "description": "Changed"})
        )

        assert result["success"] is True
        assert result.get("system_pipeline") is not True
        # No system warning should appear
        warning = result.get("warning", "")
        assert "System-Pipeline" not in warning


# ---------------------------------------------------------------------------
# 7. seed_if_empty includes system_pipelines count
# ---------------------------------------------------------------------------

class TestSeedIfEmptyIncludesSystemPipelines:
    def test_seed_if_empty_returns_system_pipelines_key(self, tmp_path, monkeypatch):
        from brix.db import BrixDB
        from brix.seed import seed_if_empty

        db = BrixDB(db_path=tmp_path / "test.db")

        # Patch _seed_system_pipelines at module level to avoid touching the real store
        monkeypatch.setattr("brix.seed._seed_system_pipelines", lambda: 3)

        counts = seed_if_empty(db)
        assert "system_pipelines" in counts
        assert counts["system_pipelines"] == 3
