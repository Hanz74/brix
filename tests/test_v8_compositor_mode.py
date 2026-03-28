"""Tests for T-BRIX-V8-07: Compositor-Mode — Restricted Brick Set without python_script.

Covers:
1. Pipeline with compositor_mode=True and python step → error at execution
2. Pipeline with compositor_mode=True and mcp_call step → OK
3. Pipeline with compositor_mode=True and allow_code=True → python/cli allowed (override)
4. add_step warning when compositor_mode pipeline gets python/cli step
5. compose_pipeline with compositor_mode parameter
6. Default: compositor_mode=False, everything allowed (backward-compat)
7. Model field defaults and auto-apply logic
8. create_pipeline warning when compositor_mode steps include python/cli
"""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import patch, MagicMock, AsyncMock


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestPipelineModelCompositorMode:
    """Test Pipeline model fields and model_validator logic."""

    def test_default_compositor_mode_false(self):
        """Default pipeline has compositor_mode=False and allow_code=True."""
        from brix.models import Pipeline, Step
        pipeline = Pipeline(
            name="test",
            steps=[Step(id="s1", type="python", script="/app/helpers/foo.py")],
        )
        assert pipeline.compositor_mode is False
        assert pipeline.allow_code is True

    def test_compositor_mode_auto_disables_allow_code(self):
        """Setting compositor_mode=True auto-sets allow_code=False when not explicitly provided."""
        from brix.models import Pipeline, Step
        pipeline = Pipeline(
            name="test",
            compositor_mode=True,
            steps=[Step(id="s1", type="mcp", server="m365", tool="get-mail-message")],
        )
        assert pipeline.compositor_mode is True
        assert pipeline.allow_code is False

    def test_compositor_mode_allow_code_explicit_override(self):
        """compositor_mode=True with explicit allow_code=True keeps allow_code True."""
        from brix.models import Pipeline, Step
        pipeline = Pipeline(
            name="test",
            compositor_mode=True,
            allow_code=True,
            steps=[Step(id="s1", type="python", script="/app/helpers/foo.py")],
        )
        assert pipeline.compositor_mode is True
        assert pipeline.allow_code is True

    def test_allow_code_true_without_compositor_mode(self):
        """allow_code=True stays True when compositor_mode is False (no-op)."""
        from brix.models import Pipeline, Step
        pipeline = Pipeline(
            name="test",
            compositor_mode=False,
            allow_code=True,
            steps=[Step(id="s1", type="python", script="/app/helpers/foo.py")],
        )
        assert pipeline.allow_code is True


# ---------------------------------------------------------------------------
# Engine execution tests
# ---------------------------------------------------------------------------

class TestEngineCompositorMode:
    """Test that the PipelineEngine enforces compositor_mode at execution time."""

    def _make_pipeline(self, step_type: str, compositor_mode: bool = True, allow_code: bool = None) -> "Pipeline":
        from brix.models import Pipeline, Step

        kwargs: dict = {"name": "test", "compositor_mode": compositor_mode}
        if allow_code is not None:
            kwargs["allow_code"] = allow_code

        step_kwargs: dict = {"id": "s1", "type": step_type}
        if step_type == "python":
            step_kwargs["script"] = "/app/helpers/foo.py"
        elif step_type == "cli":
            step_kwargs["command"] = "echo hello"
            step_kwargs["shell"] = True
        elif step_type == "mcp":
            step_kwargs["server"] = "m365"
            step_kwargs["tool"] = "get-mail-message"
            step_kwargs["params"] = {}
        elif step_type == "set":
            step_kwargs["values"] = {"x": 1}

        return Pipeline(steps=[Step(**step_kwargs)], **kwargs)

    def test_python_step_blocked_in_compositor_mode(self):
        """python step raises error when compositor_mode=True, allow_code=False."""
        from brix.engine import PipelineEngine

        pipeline = self._make_pipeline("python", compositor_mode=True)
        assert pipeline.allow_code is False

        engine = PipelineEngine()
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(pipeline, user_input={})
        )
        assert result.success is False
        s1 = result.steps.get("s1")
        assert s1 is not None
        assert s1.status == "error"
        assert "Compositor-Mode" in (s1.error_message or "")
        assert "python" in (s1.error_message or "")

    def test_cli_step_blocked_in_compositor_mode(self):
        """cli step raises error when compositor_mode=True, allow_code=False."""
        from brix.engine import PipelineEngine

        pipeline = self._make_pipeline("cli", compositor_mode=True)
        assert pipeline.allow_code is False

        engine = PipelineEngine()
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(pipeline, user_input={})
        )
        assert result.success is False
        s1 = result.steps.get("s1")
        assert s1 is not None
        assert s1.status == "error"
        assert "Compositor-Mode" in (s1.error_message or "")

    def test_mcp_step_not_blocked_by_compositor_mode(self):
        """mcp step is NOT blocked by the compositor-mode guard (even if it fails for other reasons)."""
        from brix.engine import PipelineEngine

        pipeline = self._make_pipeline("mcp", compositor_mode=True)
        assert pipeline.allow_code is False

        engine = PipelineEngine()
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(pipeline, user_input={})
        )
        # The step may fail (e.g. no MCP server), but must NOT be blocked by compositor-mode
        s1 = result.steps.get("s1")
        if s1 is not None and s1.status == "error":
            assert "Compositor-Mode" not in (s1.error_message or ""), (
                f"mcp step should not be blocked by compositor-mode, got: {s1.error_message}"
            )

    def test_python_step_allowed_with_allow_code_override(self):
        """python step is not blocked by compositor guard when allow_code=True."""
        from brix.engine import PipelineEngine

        pipeline = self._make_pipeline("python", compositor_mode=True, allow_code=True)
        assert pipeline.compositor_mode is True
        assert pipeline.allow_code is True

        engine = PipelineEngine()
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(pipeline, user_input={})
        )
        s1 = result.steps.get("s1")
        assert s1 is not None
        # Must NOT be blocked by compositor-mode guard (may fail for other reasons like missing script)
        assert s1.error_message is None or "Compositor-Mode" not in s1.error_message

    def test_backward_compat_no_compositor_mode(self):
        """Default pipeline (compositor_mode=False) does not block python step."""
        from brix.engine import PipelineEngine

        pipeline = self._make_pipeline("python", compositor_mode=False)
        assert pipeline.compositor_mode is False

        engine = PipelineEngine()
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(pipeline, user_input={})
        )
        s1 = result.steps.get("s1")
        assert s1 is not None
        # No compositor-mode block (may fail for other reasons like missing script)
        assert s1.error_message is None or "Compositor-Mode" not in (s1.error_message or "")

    def test_set_step_allowed_in_compositor_mode(self):
        """set step (built-in brick) is not blocked by compositor-mode guard."""
        from brix.engine import PipelineEngine

        pipeline = self._make_pipeline("set", compositor_mode=True)
        assert pipeline.allow_code is False

        engine = PipelineEngine()
        result = asyncio.get_event_loop().run_until_complete(
            engine.run(pipeline, user_input={})
        )
        s1 = result.steps.get("s1")
        assert s1 is not None
        # set step is not python/cli — must not be blocked by compositor-mode guard
        assert "Compositor-Mode" not in (s1.error_message or "")


# ---------------------------------------------------------------------------
# MCP Handler: add_step warning
# ---------------------------------------------------------------------------

class TestAddStepCompositorWarning:
    """Test that add_step emits a warning when adding python/cli to a compositor pipeline."""

    @pytest.fixture
    def tmp_pipelines_dir(self, tmp_path, monkeypatch):
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        import brix.mcp_handlers._shared as shared_mod
        monkeypatch.setattr(shared_mod, "_pipeline_dir", lambda: pipelines_dir)
        return pipelines_dir

    def _write_pipeline(self, pipelines_dir, compositor_mode: bool = True, allow_code: bool = False):
        """Write a minimal pipeline YAML with optional compositor_mode."""
        import yaml
        data = {
            "name": "compositor-mode-test-pipe",
            "compositor_mode": compositor_mode,
            "allow_code": allow_code,
            "steps": [
                {"id": "s1", "type": "set", "values": {"x": 1}},
            ],
        }
        (pipelines_dir / "compositor-mode-test-pipe.yaml").write_text(yaml.dump(data))

    def test_add_python_step_warns_in_compositor_mode(self, tmp_pipelines_dir):
        """Adding a python step to a compositor_mode pipeline yields a warning."""
        self._write_pipeline(tmp_pipelines_dir, compositor_mode=True, allow_code=False)

        from brix.mcp_handlers.steps import _handle_add_step
        result = asyncio.get_event_loop().run_until_complete(
            _handle_add_step({
                "pipeline_name": "compositor-mode-test-pipe",
                "step_id": "s2",
                "type": "python",
                "script": "/app/helpers/foo.py",
            })
        )
        assert result["success"] is True
        assert "warning" in result
        assert "COMPOSITOR-MODE" in result["warning"]
        assert "python" in result["warning"]

    def test_add_cli_step_warns_in_compositor_mode(self, tmp_pipelines_dir):
        """Adding a cli step to a compositor_mode pipeline yields a warning."""
        self._write_pipeline(tmp_pipelines_dir, compositor_mode=True, allow_code=False)

        from brix.mcp_handlers.steps import _handle_add_step
        result = asyncio.get_event_loop().run_until_complete(
            _handle_add_step({
                "pipeline_name": "compositor-mode-test-pipe",
                "step_id": "s2",
                "type": "cli",
                "command": "echo hello",
                "shell": True,
            })
        )
        assert result["success"] is True
        assert "warning" in result
        assert "COMPOSITOR-MODE" in result["warning"]

    def test_add_mcp_step_no_warning_in_compositor_mode(self, tmp_pipelines_dir):
        """Adding an mcp step to a compositor_mode pipeline does NOT produce a warning."""
        self._write_pipeline(tmp_pipelines_dir, compositor_mode=True, allow_code=False)

        from brix.mcp_handlers.steps import _handle_add_step
        result = asyncio.get_event_loop().run_until_complete(
            _handle_add_step({
                "pipeline_name": "compositor-mode-test-pipe",
                "step_id": "s2",
                "type": "mcp",
                "server": "m365",
                "tool": "get-mail-message",
            })
        )
        assert result["success"] is True
        assert "warning" not in result

    def test_add_python_step_no_warning_when_allow_code(self, tmp_pipelines_dir):
        """Adding a python step when allow_code=True produces no compositor warning."""
        self._write_pipeline(tmp_pipelines_dir, compositor_mode=True, allow_code=True)

        from brix.mcp_handlers.steps import _handle_add_step
        result = asyncio.get_event_loop().run_until_complete(
            _handle_add_step({
                "pipeline_name": "compositor-mode-test-pipe",
                "step_id": "s2",
                "type": "python",
                "script": "/app/helpers/foo.py",
            })
        )
        assert result["success"] is True
        # No compositor warning when allow_code=True
        if "warning" in result:
            assert "COMPOSITOR-MODE" not in result["warning"]

    def test_add_step_no_warning_without_compositor_mode(self, tmp_pipelines_dir):
        """Adding a python step to a non-compositor pipeline produces no warning."""
        self._write_pipeline(tmp_pipelines_dir, compositor_mode=False)

        from brix.mcp_handlers.steps import _handle_add_step
        result = asyncio.get_event_loop().run_until_complete(
            _handle_add_step({
                "pipeline_name": "compositor-mode-test-pipe",
                "step_id": "s2",
                "type": "python",
                "script": "/app/helpers/foo.py",
            })
        )
        assert result["success"] is True
        if "warning" in result:
            assert "COMPOSITOR-MODE" not in result["warning"]


# ---------------------------------------------------------------------------
# MCP Handler: compose_pipeline with compositor_mode parameter
# ---------------------------------------------------------------------------

class TestComposePipelineCompositorMode:
    """Test compose_pipeline with compositor_mode=True."""

    @pytest.fixture
    def tmp_pipelines_dir(self, tmp_path, monkeypatch):
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        import brix.mcp_handlers._shared as shared_mod
        import brix.mcp_handlers.composer as composer_mod
        monkeypatch.setattr(shared_mod, "_pipeline_dir", lambda: pipelines_dir)
        monkeypatch.setattr(composer_mod, "_pipeline_dir", lambda: pipelines_dir)
        return pipelines_dir

    def test_compose_pipeline_with_compositor_mode(self, tmp_pipelines_dir):
        """compose_pipeline with compositor_mode=True sets flag in proposed pipeline."""
        from brix.mcp_handlers.composer import _handle_compose_pipeline

        result = asyncio.get_event_loop().run_until_complete(
            _handle_compose_pipeline({"goal": "fetch emails from outlook", "compositor_mode": True})
        )
        assert result["success"] is True
        assert result.get("compositor_mode") is True
        proposed = result["proposed_pipeline"]
        assert proposed.get("compositor_mode") is True
        assert proposed.get("allow_code") is False
        # No proposed step should have type python or cli
        for step in proposed.get("steps", []):
            assert step.get("type") not in ("python", "cli"), (
                f"Compositor mode should not propose python/cli step: {step}"
            )

    def test_compose_pipeline_without_compositor_mode(self, tmp_pipelines_dir):
        """compose_pipeline without explicit compositor_mode defaults to True (T-BRIX-DB-11)."""
        from brix.mcp_handlers.composer import _handle_compose_pipeline

        result = asyncio.get_event_loop().run_until_complete(
            _handle_compose_pipeline({"goal": "fetch emails from outlook"})
        )
        assert result["success"] is True
        # Default is now True per T-BRIX-DB-11
        assert result.get("compositor_mode") is True

    def test_compose_pipeline_compositor_mode_false_explicit(self, tmp_pipelines_dir):
        """compose_pipeline with compositor_mode=False behaves normally."""
        from brix.mcp_handlers.composer import _handle_compose_pipeline

        result = asyncio.get_event_loop().run_until_complete(
            _handle_compose_pipeline({"goal": "fetch files from onedrive", "compositor_mode": False})
        )
        assert result["success"] is True
        # No compositor flag in result
        assert not result.get("compositor_mode")


# ---------------------------------------------------------------------------
# MCP Handler: create_pipeline warning
# ---------------------------------------------------------------------------

class TestCreatePipelineCompositorWarning:
    """Test that create_pipeline emits a warning for python/cli in compositor_mode."""

    @pytest.fixture
    def tmp_pipelines_dir(self, tmp_path, monkeypatch):
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        import brix.mcp_handlers._shared as shared_mod
        import brix.mcp_handlers.pipelines as pipelines_mod
        monkeypatch.setattr(shared_mod, "_pipeline_dir", lambda: pipelines_dir)
        monkeypatch.setattr(pipelines_mod, "_pipeline_dir", lambda: pipelines_dir)
        return pipelines_dir

    def test_create_pipeline_warns_for_python_in_compositor_mode(self, tmp_pipelines_dir):
        """create_pipeline with compositor_mode=True and python step warns."""
        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        result = asyncio.get_event_loop().run_until_complete(
            _handle_create_pipeline({
                "name": "cm-test",
                "compositor_mode": True,
                "allow_code": False,
                "steps": [
                    {"id": "s1", "type": "python", "script": "/app/helpers/foo.py"},
                ],
            })
        )
        assert result["success"] is True
        warnings = result.get("warnings", [])
        compositor_warnings = [w for w in warnings if "COMPOSITOR-MODE" in w]
        assert len(compositor_warnings) >= 1

    def test_create_pipeline_no_warning_for_mcp_in_compositor_mode(self, tmp_pipelines_dir):
        """create_pipeline with compositor_mode=True and mcp step does NOT warn."""
        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        result = asyncio.get_event_loop().run_until_complete(
            _handle_create_pipeline({
                "name": "cm-test2",
                "compositor_mode": True,
                "allow_code": False,
                "steps": [
                    {"id": "s1", "type": "mcp", "server": "m365", "tool": "get-mail-message"},
                ],
            })
        )
        assert result["success"] is True
        warnings = result.get("warnings", [])
        compositor_warnings = [w for w in warnings if "COMPOSITOR-MODE" in w]
        assert len(compositor_warnings) == 0


# ---------------------------------------------------------------------------
# get_tips includes compositor-mode hint
# ---------------------------------------------------------------------------

class TestGetTipsCompositorMode:
    """Test that get_tips includes information about compositor-mode."""

    def test_get_tips_includes_compositor_mode_hint(self):
        """get_tips output must mention COMPOSITOR-MODE."""
        from brix.mcp_handlers.help import _handle_get_tips

        result = asyncio.get_event_loop().run_until_complete(_handle_get_tips({}))
        tips_text = "\n".join(result.get("tips", []))
        assert "COMPOSITOR-MODE" in tips_text
        assert "compositor_mode" in tips_text
