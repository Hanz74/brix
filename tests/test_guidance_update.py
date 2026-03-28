"""Tests for T-BRIX-DB-11: Guidance Update — get_tips, get_help, compose_pipeline.

Covers:
1. get_tips contains BRICK-FIRST rule at top (before COMPOSITOR-REGEL)
2. get_tips contains Brick alternatives (db.query, llm.batch, etc.)
3. get_tips contains PROFILES & VARIABLES section
4. get_tips lists new help topics (brick-first, db-bricks, etc.)
5. get_help has all 8 new topics
6. New help topics have correct content
7. compose_pipeline defaults to compositor_mode=True
8. compose_pipeline uses Brick names (not 'python') in proposed steps
9. compose_pipeline fallback steps use brick alternatives
"""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# get_tips tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_tips_brick_first_rule_present():
    """get_tips must contain the BRICK-FIRST rule."""
    from brix.mcp_handlers.help import _handle_get_tips

    result = await _handle_get_tips({})
    tips_text = "\n".join(result["tips"])
    assert "BRICK-FIRST" in tips_text


@pytest.mark.asyncio
async def test_get_tips_brick_first_before_compositor():
    """BRICK-FIRST must appear before COMPOSITOR-REGEL in tips."""
    from brix.mcp_handlers.help import _handle_get_tips

    result = await _handle_get_tips({})
    tips_text = "\n".join(result["tips"])
    brick_first_pos = tips_text.index("BRICK-FIRST")
    compositor_pos = tips_text.index("COMPOSITOR-REGEL")
    assert brick_first_pos < compositor_pos, "BRICK-FIRST must appear before COMPOSITOR-REGEL"


@pytest.mark.asyncio
async def test_get_tips_brick_examples():
    """get_tips must list concrete brick names (db.query, llm.batch, etc.)."""
    from brix.mcp_handlers.help import _handle_get_tips

    result = await _handle_get_tips({})
    tips_text = "\n".join(result["tips"])
    for brick in ["db.query", "llm.batch", "markitdown.convert", "extract.specialist", "source.fetch", "flow.filter"]:
        assert brick in tips_text, f"Brick '{brick}' should be listed in tips"


@pytest.mark.asyncio
async def test_get_tips_no_create_helper_for_standard_tasks():
    """get_tips must warn against create_helper for standard tasks."""
    from brix.mcp_handlers.help import _handle_get_tips

    result = await _handle_get_tips({})
    tips_text = "\n".join(result["tips"])
    assert "create_helper" in tips_text
    # Should reference that standard tasks have brick alternatives
    assert "db.query" in tips_text and "llm.batch" in tips_text


@pytest.mark.asyncio
async def test_get_tips_profiles_and_variables():
    """get_tips must contain PROFILES & VARIABLES section."""
    from brix.mcp_handlers.help import _handle_get_tips

    result = await _handle_get_tips({})
    tips_text = "\n".join(result["tips"])
    assert "PROFILES" in tips_text or "Profiles" in tips_text
    assert "Variables" in tips_text or "var.name" in tips_text or "set_variable" in tips_text


@pytest.mark.asyncio
async def test_get_tips_new_help_topics_listed():
    """get_tips must list new help topics."""
    from brix.mcp_handlers.help import _handle_get_tips

    result = await _handle_get_tips({})
    tips_text = "\n".join(result["tips"])
    for topic in ["brick-first", "db-bricks", "llm-bricks", "source-bricks", "resilience", "variables", "profiles", "testing"]:
        assert topic in tips_text, f"Topic '{topic}' should be mentioned in tips"


# ---------------------------------------------------------------------------
# get_help new topics tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_help_quick_start_topic_exists():
    """get_help must have the 'quick-start' topic (DB-seeded)."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({"topic": "quick-start"})
    assert "error" not in result
    assert "content" in result


@pytest.mark.asyncio
async def test_get_help_debugging_topic_exists():
    """get_help must have the 'debugging' topic (DB-seeded)."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({"topic": "debugging"})
    assert "error" not in result
    assert "content" in result


@pytest.mark.asyncio
async def test_get_help_foreach_topic_exists():
    """get_help must have the 'foreach' topic (DB-seeded)."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({"topic": "foreach"})
    assert "error" not in result
    assert "content" in result


@pytest.mark.asyncio
async def test_get_help_credentials_topic_exists():
    """get_help must have the 'credentials' topic (DB-seeded)."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({"topic": "credentials"})
    assert "error" not in result
    assert "content" in result


@pytest.mark.asyncio
async def test_get_help_triggers_topic_exists():
    """get_help must have the 'triggers' topic (DB-seeded)."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({"topic": "triggers"})
    assert "error" not in result
    assert "content" in result


@pytest.mark.asyncio
async def test_get_help_helpers_topic_exists():
    """get_help must have the 'helpers' topic (DB-seeded)."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({"topic": "helpers"})
    assert "error" not in result
    assert "content" in result


@pytest.mark.asyncio
async def test_get_help_templates_topic_exists():
    """get_help must have the 'templates' topic (DB-seeded)."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({"topic": "templates"})
    assert "error" not in result
    assert "content" in result


@pytest.mark.asyncio
async def test_get_help_step_referenzen_topic_exists():
    """get_help must have the 'step-referenzen' topic (DB-seeded)."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({"topic": "step-referenzen"})
    assert "error" not in result
    assert "content" in result


@pytest.mark.asyncio
async def test_get_help_all_db_topics_listed():
    """get_help without topic must list all DB-seeded topics."""
    from brix.mcp_handlers.help import _handle_get_help

    result = await _handle_get_help({})
    all_topics = result.get("topics", [])
    # These topics are known to exist in seed-data.json
    for topic in ["quick-start", "debugging", "foreach", "credentials", "triggers"]:
        assert topic in all_topics, f"Topic '{topic}' should be in topic list"


# ---------------------------------------------------------------------------
# compose_pipeline tests
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_pipelines_dir(tmp_path, monkeypatch):
    """Redirect pipeline storage to a temp directory."""
    from brix.pipeline_store import PipelineStore

    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    import brix.mcp_server as mcp_mod
    import brix.mcp_handlers._shared as shared_mod
    import brix.mcp_handlers.composer as composer_mod

    monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", pipelines_dir)

    def patched_pipeline_dir():
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        return pipelines_dir

    monkeypatch.setattr(shared_mod, "_pipeline_dir", patched_pipeline_dir)
    monkeypatch.setattr(composer_mod, "_pipeline_dir", patched_pipeline_dir)

    OriginalPipelineStore = PipelineStore

    class IsolatedPipelineStore(OriginalPipelineStore):
        def __init__(self, pipelines_dir=None, search_paths=None, db=None):
            super().__init__(
                pipelines_dir=pipelines_dir or patched_pipeline_dir(),
                search_paths=[patched_pipeline_dir()],
                db=db,
            )

    import brix.mcp_handlers.composer as cm
    monkeypatch.setattr(cm, "PipelineStore", IsolatedPipelineStore)
    return pipelines_dir


@pytest.mark.asyncio
async def test_compose_pipeline_defaults_compositor_mode(tmp_pipelines_dir):
    """compose_pipeline must default to compositor_mode=True."""
    from brix.mcp_handlers.composer import _handle_compose_pipeline

    result = await _handle_compose_pipeline({"goal": "fetch emails and extract invoice data"})
    assert result.get("success") is True
    assert result.get("compositor_mode") is True


@pytest.mark.asyncio
async def test_compose_pipeline_no_python_steps_by_default(tmp_pipelines_dir):
    """compose_pipeline must not produce python/cli step types by default."""
    from brix.mcp_handlers.composer import _handle_compose_pipeline

    result = await _handle_compose_pipeline({"goal": "extract data from PDF documents and store in database"})
    assert result.get("success") is True
    steps = result.get("proposed_pipeline", {}).get("steps", [])
    for step in steps:
        step_type = step.get("type", "")
        assert step_type not in ("python", "cli"), (
            f"Step '{step.get('id')}' has type '{step_type}' — should be a brick name"
        )


@pytest.mark.asyncio
async def test_compose_pipeline_uses_brick_names(tmp_pipelines_dir):
    """compose_pipeline fallback steps should use brick names."""
    from brix.mcp_handlers.composer import _handle_compose_pipeline

    result = await _handle_compose_pipeline({
        "goal": "filter and classify documents",
        "compositor_mode": True,
    })
    assert result.get("success") is True
    steps = result.get("proposed_pipeline", {}).get("steps", [])
    step_types = [s.get("type", "") for s in steps]
    # Should not contain raw 'python' or 'cli'
    assert "python" not in step_types
    assert "cli" not in step_types


@pytest.mark.asyncio
async def test_compose_pipeline_explicit_compositor_false_can_use_python(tmp_pipelines_dir):
    """When compositor_mode=False, compose_pipeline may use python steps."""
    from brix.mcp_handlers.composer import _handle_compose_pipeline

    result = await _handle_compose_pipeline({
        "goal": "custom processing with complex business logic",
        "compositor_mode": False,
    })
    assert result.get("success") is True
    # compositor_mode key should not be set (or False) in result
    assert not result.get("compositor_mode")
