"""Tests for T-BRIX-IMP-02 and T-BRIX-IMP-04.

T-BRIX-IMP-02: Custom-brick config_defaults are automatically merged into
step.params when a step's type matches a registered custom brick.

T-BRIX-IMP-04: After a run starts, the pipeline's project field is
automatically written as a JSON annotation on the run record.
"""

import json
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine():
    """Return a PipelineEngine with mocked runners and an empty brick registry."""
    from brix.engine import PipelineEngine
    from brix.bricks.registry import BrickRegistry

    engine = PipelineEngine()
    # Replace the brick registry with a fresh instance backed by no DB so
    # built-in bricks load from code, not an external DB.
    engine._brick_registry = BrickRegistry()
    return engine


def _make_step(step_type: str, params: dict | None = None):
    """Return a Step model instance for the given type."""
    from brix.models import Step

    # For custom dot-notation types we need to bypass the Literal validation
    # because test brick names are not in the Literal enum.  We use
    # model_construct() to skip validation entirely.
    s = Step.model_construct(
        id="test_step",
        type=step_type,
        params=params,
        enabled=True,
        profile=None,
        helper=None,
        script=None,
        server=None,
        tool=None,
        foreach=None,
        parallel=False,
        concurrency=10,
        batch_size=0,
        on_error=None,
        when=None,
        else_of=None,
        requirements=None,
        method="GET",
        headers=None,
        body=None,
        command=None,
        args=None,
        shell=False,
        pipeline=None,
        pipelines=None,
        shared_params={},
        values=None,
        persist=False,
        message=None,
        success_on_stop=True,
        channel=None,
        to=None,
        approval_timeout="24h",
        on_timeout="stop",
        choices=None,
        default_steps=None,
        sub_steps=None,
        until=None,
        while_condition=None,
        max_iterations=100,
        sequence=None,
        flat_output=False,
    )
    return s


# ===========================================================================
# T-BRIX-IMP-02: config_defaults merging
# ===========================================================================


class TestBrickDefaultsMerge:
    """_apply_brick_defaults merges config_schema defaults into step.params."""

    def _make_db_row(self, config_schema: dict) -> dict:
        """Return a minimal DB row for a brick with the given config_schema."""
        return {
            "name": "cody.call",
            "runner": "mcp",
            "config_schema": json.dumps(config_schema),
            "namespace": "cody",
            "category": "custom",
            "description": "test",
            "when_to_use": "",
            "when_NOT_to_use": "",
            "aliases": "[]",
            "input_type": "*",
            "output_type": "*",
            "examples": "[]",
            "related_connector": "",
            "system": False,
        }

    def test_defaults_applied_when_step_params_empty(self):
        """Brick defaults fill step.params when step has no params."""
        engine = _make_engine()
        step = _make_step("cody.call", params=None)

        db_row = self._make_db_row({"server": "cody", "tool": "default_tool"})
        with patch("brix.db.BrixDB.brick_definitions_get", return_value=db_row):
            result = engine._apply_brick_defaults(step)

        assert result.params == {"server": "cody", "tool": "default_tool"}

    def test_step_params_override_brick_defaults(self):
        """Step's own params always win over brick defaults."""
        engine = _make_engine()
        step = _make_step("cody.call", params={"tool": "my_tool", "extra": "x"})

        db_row = self._make_db_row({"server": "cody", "tool": "default_tool"})
        with patch("brix.db.BrixDB.brick_definitions_get", return_value=db_row):
            result = engine._apply_brick_defaults(step)

        assert result.params["tool"] == "my_tool"   # step wins
        assert result.params["server"] == "cody"    # default fills in
        assert result.params["extra"] == "x"        # step extra preserved

    def test_brick_defaults_add_missing_keys_only(self):
        """Brick defaults only add keys absent from step.params."""
        engine = _make_engine()
        step = _make_step("cody.call", params={"server": "custom_server"})

        db_row = self._make_db_row({"server": "cody", "circuit_breaker": "default_cb"})
        with patch("brix.db.BrixDB.brick_definitions_get", return_value=db_row):
            result = engine._apply_brick_defaults(step)

        assert result.params["server"] == "custom_server"  # step wins
        assert result.params["circuit_breaker"] == "default_cb"  # default added

    def test_no_change_when_brick_not_in_db(self):
        """Step is returned unchanged if type is not in the brick registry."""
        engine = _make_engine()
        step = _make_step("cody.call", params={"server": "mine"})

        with patch("brix.db.BrixDB.brick_definitions_get", return_value=None):
            result = engine._apply_brick_defaults(step)

        assert result is step  # identical object — no copy made

    def test_no_change_for_non_dot_notation_types(self):
        """Legacy flat types (no dot) are skipped entirely — no DB lookup."""
        engine = _make_engine()
        step = _make_step("mcp", params={"server": "x"})

        with patch("brix.db.BrixDB.brick_definitions_get") as mock_get:
            result = engine._apply_brick_defaults(step)
            mock_get.assert_not_called()

        assert result is step

    def test_empty_config_schema_returns_step_unchanged(self):
        """Brick with empty config_schema does not modify step.params."""
        engine = _make_engine()
        step = _make_step("my.brick", params={"key": "val"})

        db_row = self._make_db_row({})
        with patch("brix.db.BrixDB.brick_definitions_get", return_value=db_row):
            result = engine._apply_brick_defaults(step)

        assert result is step

    def test_db_exception_returns_original_step(self):
        """DB errors are swallowed — original step is returned."""
        engine = _make_engine()
        step = _make_step("cody.call", params={})

        with patch("brix.db.BrixDB.brick_definitions_get", side_effect=RuntimeError("db down")):
            result = engine._apply_brick_defaults(step)

        assert result is step

    def test_config_schema_as_dict_object(self):
        """config_schema stored as a dict (not JSON string) is also handled."""
        engine = _make_engine()
        step = _make_step("cody.call", params=None)

        db_row = self._make_db_row({"server": "cody"})
        # Override the string with a native dict
        db_row["config_schema"] = {"server": "cody"}
        with patch("brix.db.BrixDB.brick_definitions_get", return_value=db_row):
            result = engine._apply_brick_defaults(step)

        assert result.params == {"server": "cody"}


# ===========================================================================
# T-BRIX-IMP-04: Auto-annotation with pipeline project
# ===========================================================================


class TestAutoAnnotation:
    """Auto-annotation writes pipeline project to run notes after record_start."""

    def _minimal_pipeline(self, name: str = "test-pipeline"):
        """Return a minimal Pipeline object (no steps — runs instantly)."""
        from brix.models import Pipeline, ErrorConfig

        return Pipeline.model_construct(
            name=name,
            version="1.0",
            steps=[],
            error_handling=ErrorConfig(),
            strict_bricks=False,
            compositor_mode=False,
            allow_code=False,
            idempotency_key=None,
            input={},
            project=None,
        )

    def _run_pipeline(self, engine, pipeline):
        """Run pipeline synchronously, return RunResult."""
        import asyncio

        async def _inner():
            return await engine.run(pipeline, user_input={})

        return asyncio.run(_inner())

    def test_project_annotation_written_when_project_set(self, tmp_path):
        """When pipeline has a project, run notes are annotated with JSON project."""
        from brix.db import BrixDB
        import brix.history as _history_mod

        db_path = tmp_path / "brix.db"
        db = BrixDB(db_path=db_path)
        db.upsert_pipeline(
            name="test-pipeline",
            path="/tmp/test-pipeline.yaml",
            yaml_content="name: test-pipeline\nsteps: []\n",
            project="forge",
        )

        engine = _make_engine()
        pipeline = self._minimal_pipeline("test-pipeline")

        # Redirect RunHistory to our isolated DB
        with patch.object(_history_mod, "HISTORY_DB_PATH", db_path):
            result = self._run_pipeline(engine, pipeline)

        # Verify annotation was written
        run_record = db.get_run(result.run_id)
        assert run_record is not None
        notes = run_record.get("notes") or ""
        assert notes != ""
        parsed = json.loads(notes)
        assert parsed.get("project") == "forge"

    def test_no_annotation_when_project_empty(self, tmp_path):
        """When pipeline has no project, run notes remain empty/null."""
        from brix.db import BrixDB
        import brix.history as _history_mod

        db_path = tmp_path / "brix.db"
        db = BrixDB(db_path=db_path)
        db.upsert_pipeline(
            name="test-pipeline",
            path="/tmp/test-pipeline.yaml",
            yaml_content="name: test-pipeline\nsteps: []\n",
            # no project
        )

        engine = _make_engine()
        pipeline = self._minimal_pipeline("test-pipeline")

        with patch.object(_history_mod, "HISTORY_DB_PATH", db_path):
            result = self._run_pipeline(engine, pipeline)

        run_record = db.get_run(result.run_id)
        notes = run_record.get("notes") if run_record else None
        # notes should be absent or not contain a project annotation
        if notes:
            parsed = _safe_json(notes, {})
            assert not parsed.get("project")

    def test_annotation_exception_does_not_crash_pipeline(self, tmp_path):
        """An exception in annotate_run must not abort pipeline execution."""
        from brix.db import BrixDB
        import brix.history as _history_mod

        db_path = tmp_path / "brix.db"
        db = BrixDB(db_path=db_path)
        db.upsert_pipeline(
            name="test-pipeline",
            path="/tmp/test-pipeline.yaml",
            yaml_content="name: test-pipeline\nsteps: []\n",
            project="forge",
        )

        engine = _make_engine()
        pipeline = self._minimal_pipeline("test-pipeline")

        with patch.object(_history_mod, "HISTORY_DB_PATH", db_path):
            # Also patch annotate_run to raise AFTER the history is set up
            with patch.object(BrixDB, "annotate_run", side_effect=RuntimeError("db error")):
                result = self._run_pipeline(engine, pipeline)

        # Pipeline itself must succeed despite annotation failure
        assert result.success is True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _safe_json(s: str, default):
    try:
        return json.loads(s)
    except Exception:
        return default
