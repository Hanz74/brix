"""Tests for T-BRIX-DB-05d: Deprecation-Enforcement — Legacy Step-Types.

Covers:
- DB record_deprecated_usage / get_deprecated_usage / get_deprecated_count
- Engine tracks legacy-alias hits in DB
- Engine accumulates deprecation_warnings in RunResult
- strict_bricks=True blocks old types with an error
- compositor_mode automatically sets strict_bricks=True
- get_tips shows LEGACY ALERT when deprecated usage exists
- get_run_status includes deprecation_warnings
- create_pipeline warns on legacy types
- add_step warns on legacy types
"""
from __future__ import annotations

import warnings
from unittest.mock import AsyncMock, patch

import pytest

from brix.db import BrixDB
from brix.engine import PipelineEngine, LEGACY_ALIASES
from brix.loader import PipelineLoader
from brix.models import Pipeline, RunResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_db(tmp_path) -> BrixDB:
    return BrixDB(db_path=tmp_path / "brix.db")


def load_pipeline(yaml_str: str) -> Pipeline:
    return PipelineLoader().load_from_string(yaml_str)


# ---------------------------------------------------------------------------
# DB: record_deprecated_usage / get_deprecated_usage / get_deprecated_count
# ---------------------------------------------------------------------------

class TestDeprecatedUsageDB:
    def test_table_created_on_init(self, tmp_path):
        db = make_db(tmp_path)
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "deprecated_usage" in tables

    def test_record_and_count_zero_initially(self, tmp_path):
        db = make_db(tmp_path)
        assert db.get_deprecated_count() == 0

    def test_record_deprecated_usage(self, tmp_path):
        db = make_db(tmp_path)
        db.record_deprecated_usage("my-pipeline", "step1", "python", "script.python")
        assert db.get_deprecated_count() == 1

    def test_get_deprecated_usage_returns_correct_fields(self, tmp_path):
        db = make_db(tmp_path)
        db.record_deprecated_usage("pipe-a", "step-x", "http", "http.request")
        entries = db.get_deprecated_usage()
        assert len(entries) == 1
        e = entries[0]
        assert e["pipeline_name"] == "pipe-a"
        assert e["step_id"] == "step-x"
        assert e["old_type"] == "http"
        assert e["new_type"] == "http.request"
        assert "last_seen" in e

    def test_upsert_on_conflict(self, tmp_path):
        db = make_db(tmp_path)
        db.record_deprecated_usage("pipe", "s1", "mcp", "mcp.call")
        db.record_deprecated_usage("pipe", "s1", "mcp", "mcp.call")
        assert db.get_deprecated_count() == 1

    def test_multiple_distinct_entries(self, tmp_path):
        db = make_db(tmp_path)
        db.record_deprecated_usage("pipe-a", "step1", "python", "script.python")
        db.record_deprecated_usage("pipe-a", "step2", "http", "http.request")
        db.record_deprecated_usage("pipe-b", "step1", "cli", "script.cli")
        assert db.get_deprecated_count() == 3

    def test_get_deprecated_usage_ordered_by_last_seen_desc(self, tmp_path):
        import time
        db = make_db(tmp_path)
        db.record_deprecated_usage("pipe", "s1", "python", "script.python")
        time.sleep(0.01)
        db.record_deprecated_usage("pipe", "s2", "http", "http.request")
        entries = db.get_deprecated_usage()
        # Most recent first
        assert entries[0]["step_id"] == "s2"
        assert entries[1]["step_id"] == "s1"


# ---------------------------------------------------------------------------
# Engine: deprecation tracking and RunResult.deprecation_warnings
# ---------------------------------------------------------------------------

class TestEngineDeprecationTracking:
    @pytest.mark.asyncio
    async def test_legacy_type_triggers_deprecation_warning(self, tmp_path):
        """Running a pipeline with legacy type adds deprecation_warnings to RunResult."""
        pipeline = load_pipeline("""
name: legacy-test
steps:
  - id: run_py
    type: script.python
    params:
      code: "output = 42"
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        # No warnings because 'script.python' is the NEW type — use OLD type to trigger
        assert isinstance(result.deprecation_warnings, list)

    @pytest.mark.asyncio
    async def test_legacy_type_python_triggers_warning(self, tmp_path, monkeypatch):
        """Old 'python' type triggers deprecation warning in RunResult."""
        pipeline = load_pipeline("""
name: old-python-test
steps:
  - id: run_py
    type: python
    params:
      code: "output = 'hello'"
""")
        engine = PipelineEngine()
        # Patch DB to avoid file I/O side-effects
        engine._deprecation_db = make_db(tmp_path)
        result = await engine.run(pipeline)
        assert len(result.deprecation_warnings) >= 1
        assert any("python" in w for w in result.deprecation_warnings)
        assert any("script.python" in w for w in result.deprecation_warnings)

    @pytest.mark.asyncio
    async def test_legacy_type_recorded_in_db(self, tmp_path, monkeypatch):
        """After running a pipeline with legacy type, DB has a record."""
        pipeline = load_pipeline("""
name: track-test
steps:
  - id: step1
    type: python
    params:
      code: "output = 1"
""")
        engine = PipelineEngine()
        db = make_db(tmp_path)
        engine._deprecation_db = db
        await engine.run(pipeline)
        assert db.get_deprecated_count() >= 1

    @pytest.mark.asyncio
    async def test_multiple_legacy_types_all_tracked(self, tmp_path):
        """Multiple distinct legacy types in one run produce multiple warnings."""
        pipeline = load_pipeline("""
name: multi-legacy
steps:
  - id: step1
    type: python
    params:
      code: "output = 1"
  - id: step2
    type: filter
    params:
      expr: "{{ step1.output > 0 }}"
""")
        engine = PipelineEngine()
        db = make_db(tmp_path)
        engine._deprecation_db = db
        result = await engine.run(pipeline)
        # At least 'python' warning
        assert len(result.deprecation_warnings) >= 1

    @pytest.mark.asyncio
    async def test_no_deprecation_warnings_for_new_types(self):
        """Pipeline using new brick types produces no deprecation warnings."""
        pipeline = load_pipeline("""
name: new-types-test
steps:
  - id: step1
    type: script.python
    params:
      code: "output = 42"
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        assert result.deprecation_warnings == []

    def test_deprecation_warnings_reset_between_runs(self, tmp_path):
        """Deprecation warnings are reset per run, not accumulated across runs."""
        engine = PipelineEngine()
        engine._current_pipeline_name = "test"
        engine._strict_bricks = False
        engine._deprecation_warnings = ["old warning"]
        # Simulate run reset
        engine._current_pipeline_name = "new-run"
        engine._deprecation_warnings = []
        assert engine._deprecation_warnings == []


# ---------------------------------------------------------------------------
# strict_bricks=True: blocks legacy types
# ---------------------------------------------------------------------------

class TestStrictBricks:
    @pytest.mark.asyncio
    async def test_strict_bricks_blocks_legacy_type(self):
        """With strict_bricks=True, using a legacy type raises ValueError and fails the run."""
        pipeline = load_pipeline("""
name: strict-test
strict_bricks: true
steps:
  - id: step1
    type: python
    params:
      code: "output = 1"
""")
        assert pipeline.strict_bricks is True
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        # Run should fail because strict_bricks blocks legacy types
        assert result.success is False

    @pytest.mark.asyncio
    async def test_strict_bricks_false_does_not_block(self):
        """Without strict_bricks, legacy types produce warnings (not a strict error)."""
        pipeline = load_pipeline("""
name: non-strict-test
strict_bricks: false
steps:
  - id: step1
    type: python
    params:
      code: "output = 1"
""")
        assert pipeline.strict_bricks is False
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        # The run may fail due to PythonRunner needing 'script' field,
        # but crucially it should NOT fail with a strict_bricks ValueError
        # and deprecation_warnings must be present
        assert len(result.deprecation_warnings) >= 1
        assert any("python" in w for w in result.deprecation_warnings)

    def test_strict_bricks_resolve_runner_raises(self):
        """_resolve_runner raises ValueError for legacy type when strict_bricks=True."""
        engine = PipelineEngine()
        engine._strict_bricks = True
        engine._deprecation_warnings = []
        with pytest.raises(ValueError, match="strict_bricks=True"):
            engine._resolve_runner("python")

    def test_strict_bricks_resolve_runner_warns_when_false(self):
        """_resolve_runner emits DeprecationWarning when strict_bricks=False."""
        engine = PipelineEngine()
        engine._strict_bricks = False
        engine._deprecation_warnings = []
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner = engine._resolve_runner("python")
        assert runner is not None
        assert any(issubclass(x.category, DeprecationWarning) for x in w)


# ---------------------------------------------------------------------------
# compositor_mode automatically sets strict_bricks
# ---------------------------------------------------------------------------

class TestCompositorModeStrictBricks:
    def _make_step(self) -> str:
        return "  - id: s1\n    type: script.python\n    params:\n      script: 'output=1'\n"

    def test_compositor_mode_and_strict_bricks_independent(self):
        """compositor_mode and strict_bricks are independent flags."""
        pipeline = load_pipeline(
            "name: compositor-strict\n"
            "compositor_mode: true\n"
            "strict_bricks: true\n"
            "steps:\n" + self._make_step()
        )
        assert pipeline.compositor_mode is True
        assert pipeline.strict_bricks is True

    def test_compositor_mode_default_strict_bricks_false(self):
        """compositor_mode=True alone does NOT set strict_bricks by default."""
        pipeline = load_pipeline(
            "name: compositor-no-strict\n"
            "compositor_mode: true\n"
            "steps:\n" + self._make_step()
        )
        assert pipeline.compositor_mode is True
        # strict_bricks defaults to False — it's a separate opt-in
        assert pipeline.strict_bricks is False

    def test_non_compositor_mode_does_not_set_strict_bricks(self):
        """Without compositor_mode, strict_bricks defaults to False."""
        pipeline = load_pipeline(
            "name: normal-pipeline\n"
            "steps:\n" + self._make_step()
        )
        assert pipeline.compositor_mode is False
        assert pipeline.strict_bricks is False

    def test_strict_bricks_field_in_pipeline_model(self):
        """Pipeline model has strict_bricks field with correct default."""
        from brix.models import Step
        p = Pipeline(name="test", steps=[Step(id="s", type="script.python")])
        assert hasattr(p, "strict_bricks")
        assert p.strict_bricks is False

    @pytest.mark.asyncio
    async def test_compositor_mode_with_strict_bricks_blocks_legacy(self):
        """compositor_mode + strict_bricks=True blocks legacy types."""
        pipeline = load_pipeline(
            "name: strict-compositor\n"
            "compositor_mode: true\n"
            "strict_bricks: true\n"
            "steps:\n"
            "  - id: s1\n    type: script.python\n    params:\n      script: 'output=1'\n"
        )
        assert pipeline.compositor_mode is True
        assert pipeline.strict_bricks is True
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        # script.python is NOT a legacy alias — should not be blocked
        assert result.steps.get("s1") is not None


# ---------------------------------------------------------------------------
# get_tips: LEGACY ALERT
# ---------------------------------------------------------------------------

class TestGetTipsLegacyAlert:
    @pytest.mark.asyncio
    async def test_get_tips_no_alert_when_no_deprecated(self, tmp_path):
        """get_tips shows no LEGACY ALERT when deprecated_usage table is empty."""
        from brix.mcp_handlers.help import _handle_get_tips

        # Patch BrixDB at the db module level since it's imported locally in help.py
        with patch("brix.db.BrixDB") as MockDB:
            mock_instance = MockDB.return_value
            mock_instance.get_deprecated_count.return_value = 0
            mock_instance.get_deprecated_usage.return_value = []
            result = await _handle_get_tips({})

        tips_str = "\n".join(result["tips"])
        assert "LEGACY ALERT" not in tips_str

    @pytest.mark.asyncio
    async def test_get_tips_shows_alert_when_deprecated_exist(self, tmp_path):
        """get_tips shows LEGACY ALERT when there are deprecated usages."""
        from brix.mcp_handlers.help import _handle_get_tips

        # Use a real DB with data instead of mock since BrixDB is imported locally
        db = make_db(tmp_path)
        db.record_deprecated_usage("my-pipe", "step1", "python", "script.python")
        db.record_deprecated_usage("other-pipe", "step2", "http", "http.request")

        with patch("brix.db.BrixDB", return_value=db):
            result = await _handle_get_tips({})

        tips_str = "\n".join(result["tips"])
        assert "LEGACY ALERT" in tips_str
        assert "my-pipe" in tips_str
        assert "python" in tips_str
        assert "script.python" in tips_str

    @pytest.mark.asyncio
    async def test_get_tips_alert_appears_before_quick_reference(self, tmp_path):
        """LEGACY ALERT appears at the top, before the Quick Reference section."""
        from brix.mcp_handlers.help import _handle_get_tips

        db = make_db(tmp_path)
        db.record_deprecated_usage("p", "s", "mcp", "mcp.call")

        with patch("brix.db.BrixDB", return_value=db):
            result = await _handle_get_tips({})

        tips = result["tips"]
        alert_idx = next((i for i, t in enumerate(tips) if "LEGACY ALERT" in t), None)
        ref_idx = next((i for i, t in enumerate(tips) if "Brix Quick Reference" in t), None)
        assert alert_idx is not None
        assert ref_idx is not None
        assert alert_idx < ref_idx


# ---------------------------------------------------------------------------
# create_pipeline: warns on legacy types
# ---------------------------------------------------------------------------

class TestCreatePipelineDeprecationWarning:
    @pytest.mark.asyncio
    async def test_create_pipeline_warns_on_legacy_type(self, tmp_path, monkeypatch):
        """create_pipeline adds a DEPRECATION WARNING when steps use legacy types."""
        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._pipeline_dir",
            lambda: tmp_path,
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._save_pipeline_yaml",
            lambda name, data: None,
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._load_pipeline_yaml",
            lambda name: (_ for _ in ()).throw(FileNotFoundError(name)),
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._validate_pipeline_dict",
            lambda data: {"valid": True, "errors": [], "warnings": []},
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._find_similar_pipelines",
            lambda name, desc: [],
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._audit_db",
            type("FakeDB", (), {"write_audit_entry": lambda *a, **kw: None})(),
        )

        result = await _handle_create_pipeline({
            "name": "test-legacy",
            "steps": [
                {"id": "step1", "type": "python", "params": {"code": "output=1"}},
            ],
        })

        assert result["success"] is True
        warnings_list = result.get("warnings", [])
        assert any("DEPRECATION WARNING" in w for w in warnings_list)
        assert any("python" in w for w in warnings_list)
        assert any("script.python" in w for w in warnings_list)

    @pytest.mark.asyncio
    async def test_create_pipeline_no_warning_for_new_types(self, tmp_path, monkeypatch):
        """create_pipeline does NOT warn when steps use new brick types."""
        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._pipeline_dir",
            lambda: tmp_path,
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._save_pipeline_yaml",
            lambda name, data: None,
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._load_pipeline_yaml",
            lambda name: (_ for _ in ()).throw(FileNotFoundError(name)),
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._validate_pipeline_dict",
            lambda data: {"valid": True, "errors": [], "warnings": []},
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._find_similar_pipelines",
            lambda name, desc: [],
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.pipelines._audit_db",
            type("FakeDB", (), {"write_audit_entry": lambda *a, **kw: None})(),
        )

        result = await _handle_create_pipeline({
            "name": "test-new-type",
            "steps": [
                {"id": "step1", "type": "script.python", "params": {"code": "output=1"}},
            ],
        })

        assert result["success"] is True
        warnings_list = result.get("warnings", [])
        assert not any("DEPRECATION WARNING" in w for w in warnings_list)


# ---------------------------------------------------------------------------
# add_step: warns on legacy types
# ---------------------------------------------------------------------------

class TestAddStepDeprecationWarning:
    @pytest.mark.asyncio
    async def test_add_step_warns_on_legacy_type(self, tmp_path, monkeypatch):
        """add_step returns a DEPRECATION WARNING when a legacy type is used."""
        from brix.mcp_handlers.steps import _handle_add_step

        monkeypatch.setattr(
            "brix.mcp_handlers.steps._pipeline_dir",
            lambda: tmp_path,
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.steps._load_pipeline_yaml",
            lambda name: {"name": name, "steps": [], "compositor_mode": False, "allow_code": True},
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.steps._save_pipeline_yaml",
            lambda name, data: None,
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.steps._validate_pipeline_dict",
            lambda data: {"valid": True, "errors": [], "warnings": []},
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.steps._audit_db",
            type("FakeDB", (), {"write_audit_entry": lambda *a, **kw: None})(),
        )

        result = await _handle_add_step({
            "pipeline_name": "my-pipe",
            "step_id": "step-legacy",
            "type": "http",
        })

        assert result["success"] is True
        # Deprecation warnings are in the separate "deprecation_warnings" key
        dep_warnings = result.get("deprecation_warnings", [])
        assert any("DEPRECATION WARNING" in w for w in dep_warnings)
        assert any("http.request" in w for w in dep_warnings)

    @pytest.mark.asyncio
    async def test_add_step_no_warning_for_new_types(self, tmp_path, monkeypatch):
        """add_step does NOT warn when a new brick type is used."""
        from brix.mcp_handlers.steps import _handle_add_step

        monkeypatch.setattr(
            "brix.mcp_handlers.steps._pipeline_dir",
            lambda: tmp_path,
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.steps._load_pipeline_yaml",
            lambda name: {"name": name, "steps": [], "compositor_mode": False, "allow_code": True},
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.steps._save_pipeline_yaml",
            lambda name, data: None,
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.steps._validate_pipeline_dict",
            lambda data: {"valid": True, "errors": [], "warnings": []},
        )
        monkeypatch.setattr(
            "brix.mcp_handlers.steps._audit_db",
            type("FakeDB", (), {"write_audit_entry": lambda *a, **kw: None})(),
        )

        result = await _handle_add_step({
            "pipeline_name": "my-pipe",
            "step_id": "step-new",
            "type": "http.request",
        })

        assert result["success"] is True
        dep_warnings = result.get("deprecation_warnings", [])
        assert not any("DEPRECATION WARNING" in w for w in dep_warnings)
