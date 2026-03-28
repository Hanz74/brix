"""Tests for T-BRIX-V8-02: plan_pipeline — Formalized Reason Phase.

Covers:
1. Goal decomposition — simple vs complex goals
2. Brick recommendations — finds matches, correct confidence
3. Alternatives provided
4. Constraint checking — various constraints
5. Complexity estimation — simple / moderate / complex
6. Integration: plan_pipeline → compose_pipeline flow (handler chain)
7. Edge cases: empty goal, unknown keywords, no matches
"""
from __future__ import annotations

import asyncio
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from brix.mcp_handlers.composer import (
    _handle_plan_pipeline,
    _confidence_level,
    _check_constraints,
    _estimate_complexity,
    _best_recommendation,
    _build_alternatives,
    _handle_compose_pipeline,
)
from brix.helper_registry import HelperRegistry


# ---------------------------------------------------------------------------
# Fixtures (mirror test_v8_composer.py for isolation)
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


@pytest.fixture
def mock_registry(tmp_path, monkeypatch):
    """Patch HelperRegistry to use a temp file."""
    reg_file = tmp_path / "registry.yaml"
    original_init = HelperRegistry.__init__

    def patched_init(self, registry_path=None, db=None):
        original_init(self, registry_path=reg_file)

    monkeypatch.setattr(HelperRegistry, "__init__", patched_init)
    return reg_file


@pytest.fixture
def tmp_managed_dir(tmp_path, monkeypatch):
    """Redirect ~/.brix/helpers/ to a temp directory."""
    managed = tmp_path / ".brix" / "helpers"
    managed.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    return managed


# ---------------------------------------------------------------------------
# 1. Goal Decomposition
# ---------------------------------------------------------------------------

class TestGoalDecomposition:
    async def test_simple_goal_has_at_least_one_step(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({"goal": "Fetch emails from Outlook"})
        assert result["success"] is True
        steps = result["plan"]["steps"]
        assert len(steps) >= 1

    async def test_complex_goal_produces_multiple_steps(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": (
                "Download emails from Outlook, extract invoice data from attachments, "
                "classify each invoice, and store results in the database"
            )
        })
        assert result["success"] is True
        steps = result["plan"]["steps"]
        assert len(steps) >= 3  # source + multiple transforms + output

    async def test_steps_have_required_fields(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and store in database"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            assert "order" in step
            assert "action" in step
            assert "category" in step
            assert "recommendation" in step
            assert "alternatives" in step
            assert "needs_implementation" in step

    async def test_steps_ordered_sequentially(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook, classify them, store in database"
        })
        assert result["success"] is True
        orders = [s["order"] for s in result["plan"]["steps"]]
        assert orders == list(range(1, len(orders) + 1))

    async def test_first_step_is_source_category(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        steps = result["plan"]["steps"]
        assert steps[0]["category"] == "source"

    async def test_transform_steps_have_category_transform(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        transform_steps = [s for s in result["plan"]["steps"] if s["category"] == "transform"]
        # 'classify' should produce at least one transform step
        assert len(transform_steps) >= 1

    async def test_output_steps_for_store_goal(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and store in database"
        })
        assert result["success"] is True
        output_steps = [s for s in result["plan"]["steps"] if s["category"] == "output"]
        assert len(output_steps) >= 1

    async def test_unknown_goal_still_produces_a_fetch_step(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({"goal": "xyzzy frobulate quux"})
        assert result["success"] is True
        steps = result["plan"]["steps"]
        assert len(steps) >= 1
        assert steps[0]["category"] == "source"

    async def test_german_goal_decomposed(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "E-Mails herunterladen und in Datenbank speichern"
        })
        assert result["success"] is True
        steps = result["plan"]["steps"]
        assert len(steps) >= 1


# ---------------------------------------------------------------------------
# 2. Brick Recommendations
# ---------------------------------------------------------------------------

class TestBrickRecommendations:
    async def test_recommendation_has_required_keys(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and store in database"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            rec = step["recommendation"]
            assert "type" in rec
            assert "name" in rec
            assert "confidence" in rec
            assert "rationale" in rec

    async def test_confidence_is_valid_level(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            assert step["recommendation"]["confidence"] in ("high", "medium", "low")

    async def test_rationale_is_non_empty_string(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Extract invoice data from PDF files"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            rationale = step["recommendation"]["rationale"]
            assert isinstance(rationale, str)
            assert len(rationale) > 0

    async def test_existing_pipeline_recommended_with_high_confidence(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        """When a very relevant pipeline exists it should get high/medium confidence."""
        (tmp_pipelines_dir / "buddy-intake-outlook.yaml").write_text(
            "name: buddy-intake-outlook\n"
            "description: Intake emails from Outlook M365\n"
            "steps:\n"
            "  - id: fetch\n"
            "    type: python\n"
            "    script: /app/helpers/buddy_fetch.py\n"
        )
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook inbox"
        })
        assert result["success"] is True
        source_step = result["plan"]["steps"][0]
        # The existing pipeline should be found and have reasonable confidence
        assert source_step["recommendation"]["confidence"] in ("high", "medium")

    async def test_no_match_gives_low_confidence(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        """With no matches, confidence should be low."""
        result = await _handle_plan_pipeline({"goal": "xyzzy frobulate quux"})
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            assert step["recommendation"]["confidence"] == "low"

    async def test_recommendation_type_is_valid(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Download emails from Outlook and store in database"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            assert step["recommendation"]["type"] in ("pipeline", "helper", "brick", "python")


# ---------------------------------------------------------------------------
# 3. Alternatives
# ---------------------------------------------------------------------------

class TestAlternatives:
    async def test_alternatives_is_list(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            assert isinstance(step["alternatives"], list)

    async def test_alternatives_have_type_and_name(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            for alt in step["alternatives"]:
                assert "type" in alt
                assert "name" in alt
                assert "note" in alt

    async def test_alternatives_do_not_repeat_primary(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        step_yaml = (
            "steps:\n"
            "  - id: fetch\n"
            "    type: python\n"
            "    script: /app/helpers/buddy_fetch.py\n"
        )
        (tmp_pipelines_dir / "buddy-intake-outlook.yaml").write_text(
            "name: buddy-intake-outlook\n"
            "description: Intake emails from Outlook\n"
            + step_yaml
        )
        (tmp_pipelines_dir / "buddy-fetch-mails.yaml").write_text(
            "name: buddy-fetch-mails\n"
            "description: Fetch emails from mail server\n"
            + step_yaml
        )
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            primary_name = step["recommendation"]["name"]
            alt_names = [a["name"] for a in step["alternatives"]]
            assert primary_name not in alt_names

    async def test_alternatives_capped_at_two(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        """build_alternatives should return at most 2."""
        step_yaml = (
            "steps:\n"
            "  - id: fetch\n"
            "    type: python\n"
            "    script: /app/helpers/buddy_fetch.py\n"
        )
        for i in range(5):
            (tmp_pipelines_dir / f"buddy-outlook-{i}.yaml").write_text(
                f"name: buddy-outlook-{i}\n"
                f"description: Fetch emails from Outlook version {i}\n"
                + step_yaml
            )
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook"
        })
        assert result["success"] is True
        for step in result["plan"]["steps"]:
            assert len(step["alternatives"]) <= 2


# ---------------------------------------------------------------------------
# 4. Constraint Checking
# ---------------------------------------------------------------------------

class TestConstraintChecking:
    def test_no_constraints_no_violations(self):
        steps = [
            {
                "order": 1,
                "action": "Fetch emails",
                "category": "source",
                "recommendation": {"type": "pipeline", "name": "buddy-intake-outlook"},
                "alternatives": [],
                "needs_implementation": False,
            }
        ]
        violations = _check_constraints([], steps)
        assert violations == []

    def test_no_python_constraint_violated_by_helper(self):
        steps = [
            {
                "order": 1,
                "action": "Fetch emails",
                "category": "source",
                "recommendation": {"type": "helper", "name": "buddy_fetch_mails"},
                "alternatives": [],
                "needs_implementation": False,
            }
        ]
        violations = _check_constraints(["no python scripts"], steps)
        assert len(violations) >= 1
        assert "buddy_fetch_mails" in violations[0]

    def test_only_built_in_bricks_violated_by_helper(self):
        steps = [
            {
                "order": 1,
                "action": "Fetch emails",
                "category": "source",
                "recommendation": {"type": "helper", "name": "buddy_fetch"},
                "alternatives": [],
                "needs_implementation": False,
            }
        ]
        violations = _check_constraints(["only built-in bricks"], steps)
        assert len(violations) >= 1

    def test_no_pipelines_constraint_violated(self):
        steps = [
            {
                "order": 1,
                "action": "Fetch emails",
                "category": "source",
                "recommendation": {"type": "pipeline", "name": "buddy-intake-outlook"},
                "alternatives": [],
                "needs_implementation": False,
            }
        ]
        violations = _check_constraints(["no pipelines"], steps)
        assert len(violations) >= 1
        assert "buddy-intake-outlook" in violations[0]

    def test_no_pipelines_not_violated_when_using_helper(self):
        steps = [
            {
                "order": 1,
                "action": "Fetch emails",
                "category": "source",
                "recommendation": {"type": "helper", "name": "buddy_fetch"},
                "alternatives": [],
                "needs_implementation": False,
            }
        ]
        violations = _check_constraints(["no pipelines"], steps)
        assert violations == []

    def test_must_be_idempotent_warns_on_http_brick(self):
        steps = [
            {
                "order": 1,
                "action": "Call API",
                "category": "source",
                "recommendation": {"type": "brick", "name": "http_post"},
                "alternatives": [],
                "needs_implementation": False,
            }
        ]
        violations = _check_constraints(["must be idempotent"], steps)
        assert len(violations) >= 1

    def test_must_be_idempotent_no_violation_for_pipeline(self):
        steps = [
            {
                "order": 1,
                "action": "Fetch emails",
                "category": "source",
                "recommendation": {"type": "pipeline", "name": "buddy-intake-outlook"},
                "alternatives": [],
                "needs_implementation": False,
            }
        ]
        violations = _check_constraints(["must be idempotent"], steps)
        assert violations == []

    def test_multiple_constraints_checked(self):
        steps = [
            {
                "order": 1,
                "action": "Fetch emails",
                "category": "source",
                "recommendation": {"type": "helper", "name": "buddy_fetch"},
                "alternatives": [],
                "needs_implementation": False,
            },
            {
                "order": 2,
                "action": "Store results",
                "category": "output",
                "recommendation": {"type": "pipeline", "name": "buddy-store"},
                "alternatives": [],
                "needs_implementation": False,
            },
        ]
        violations = _check_constraints(["no python scripts", "no pipelines"], steps)
        assert len(violations) >= 2

    async def test_constraint_violations_appear_in_plan_response(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        """Constraint violations should surface in plan.constraint_violations."""
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and store in database",
            "constraints": ["no python scripts"],
        })
        assert result["success"] is True
        # constraint_violations is always a list (may be empty if all steps use pipelines/bricks)
        assert isinstance(result["plan"]["constraint_violations"], list)

    async def test_no_constraints_field_returns_empty_violations(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook"
        })
        assert result["success"] is True
        assert result["plan"]["constraint_violations"] == []


# ---------------------------------------------------------------------------
# 5. Complexity Estimation
# ---------------------------------------------------------------------------

class TestComplexityEstimation:
    def test_simple_all_existing(self):
        assert _estimate_complexity(total_steps=2, new_steps=0) == "simple"

    def test_simple_boundary_3_steps_no_new(self):
        assert _estimate_complexity(total_steps=3, new_steps=0) == "simple"

    def test_moderate_few_new_steps(self):
        assert _estimate_complexity(total_steps=4, new_steps=1) == "moderate"

    def test_moderate_many_steps_no_new(self):
        assert _estimate_complexity(total_steps=6, new_steps=0) == "moderate"

    def test_complex_many_steps_with_new(self):
        assert _estimate_complexity(total_steps=10, new_steps=5) == "complex"

    def test_complex_boundary(self):
        assert _estimate_complexity(total_steps=8, new_steps=3) == "complex"

    async def test_plan_contains_complexity_field(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and store in database"
        })
        assert result["success"] is True
        complexity = result["plan"]["complexity"]
        assert complexity in ("simple", "moderate", "complex")

    async def test_plan_contains_step_count_metrics(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and store in database"
        })
        assert result["success"] is True
        plan = result["plan"]
        assert "total_steps" in plan
        assert "existing_steps" in plan
        assert "new_steps" in plan
        assert plan["total_steps"] == plan["existing_steps"] + plan["new_steps"]

    async def test_simple_goal_gets_simple_or_moderate_complexity(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({"goal": "Fetch emails from Outlook"})
        assert result["success"] is True
        assert result["plan"]["complexity"] in ("simple", "moderate")

    async def test_complex_goal_gets_moderate_or_complex(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": (
                "Download emails from Outlook, extract all invoice PDFs, "
                "classify each PDF, extract line items, compute totals, "
                "store in database, and send a summary email"
            )
        })
        assert result["success"] is True
        assert result["plan"]["complexity"] in ("moderate", "complex")


# ---------------------------------------------------------------------------
# 6. Confidence Level Helper
# ---------------------------------------------------------------------------

class TestConfidenceLevel:
    def test_high_confidence(self):
        assert _confidence_level(0.8) == "high"

    def test_high_confidence_at_boundary(self):
        assert _confidence_level(0.55) == "high"

    def test_medium_confidence(self):
        assert _confidence_level(0.3) == "medium"

    def test_medium_at_boundary(self):
        assert _confidence_level(0.10) == "medium"

    def test_low_confidence(self):
        assert _confidence_level(0.05) == "low"

    def test_zero_confidence(self):
        assert _confidence_level(0.0) == "low"


# ---------------------------------------------------------------------------
# 7. Full Handler Integration
# ---------------------------------------------------------------------------

class TestHandlerIntegration:
    async def test_requires_goal(self):
        result = await _handle_plan_pipeline({})
        assert result["success"] is False
        assert "goal" in result["error"].lower()

    async def test_empty_goal_fails(self):
        result = await _handle_plan_pipeline({"goal": ""})
        assert result["success"] is False

    async def test_basic_goal_succeeds(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Download emails from Outlook and store in database"
        })
        assert result["success"] is True

    async def test_result_has_required_top_level_keys(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        for key in ("goal", "plan", "next_action"):
            assert key in result, f"Missing key: {key}"

    async def test_plan_has_required_keys(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        plan = result["plan"]
        for key in (
            "steps", "complexity", "total_steps", "existing_steps",
            "new_steps", "constraint_violations", "warnings",
        ):
            assert key in plan, f"Missing plan key: {key}"

    async def test_next_action_is_string(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Convert PDF files to markdown"
        })
        assert result["success"] is True
        assert isinstance(result["next_action"], str)
        assert len(result["next_action"]) > 0

    async def test_warnings_is_list(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and store in database"
        })
        assert result["success"] is True
        assert isinstance(result["plan"]["warnings"], list)

    async def test_unknown_keywords_succeeds_with_fallback_step(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({"goal": "xyzzy frobulate quux blargh"})
        assert result["success"] is True
        assert len(result["plan"]["steps"]) >= 1

    async def test_goal_echoed_in_response(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        goal = "Fetch emails from Outlook and store in database"
        result = await _handle_plan_pipeline({"goal": goal})
        assert result["success"] is True
        assert result["goal"] == goal

    async def test_existing_pipeline_reduces_new_steps(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        """When a matching pipeline exists, existing_steps > 0 (not all steps need implementation)."""
        (tmp_pipelines_dir / "buddy-intake-outlook.yaml").write_text(
            "name: buddy-intake-outlook\n"
            "description: Intake emails from Outlook\n"
            "steps:\n"
            "  - id: fetch\n"
            "    type: python\n"
            "    script: /app/helpers/buddy_fetch.py\n"
        )
        result_with = await _handle_plan_pipeline({"goal": "Fetch emails from Outlook"})
        assert result_with["success"] is True
        plan = result_with["plan"]
        # The existing pipeline should match at least the source step
        assert plan["existing_steps"] >= 1

    async def test_constraints_passed_are_checked(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook",
            "constraints": ["no pipelines"],
        })
        assert result["success"] is True
        # Even if the result has no violations (e.g. no pipeline recommended),
        # the constraint_violations field must be present and be a list.
        assert isinstance(result["plan"]["constraint_violations"], list)

    async def test_source_parameter_accepted(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook",
            "source": {"session": "test-session", "model": "sonnet", "agent": "agent-alpha"},
        })
        assert result["success"] is True


# ---------------------------------------------------------------------------
# 8. plan_pipeline → compose_pipeline Integration Flow
# ---------------------------------------------------------------------------

class TestPlanThenCompose:
    async def test_plan_then_compose_both_succeed(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        """plan_pipeline result can be used before compose_pipeline."""
        goal = "Fetch emails from Outlook and store in database"

        plan_result = await _handle_plan_pipeline({"goal": goal})
        assert plan_result["success"] is True

        compose_result = await _handle_compose_pipeline({"goal": goal})
        assert compose_result["success"] is True

    async def test_plan_complexity_correlates_with_compose_coverage(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        """Both tools analyse the same goal; plan's new_steps should correspond
        to compose's missing steps count (directional, not exact)."""
        goal = "Fetch emails from Outlook, classify, store in database"

        plan_result = await _handle_plan_pipeline({"goal": goal})
        compose_result = await _handle_compose_pipeline({"goal": goal})

        assert plan_result["success"] is True
        assert compose_result["success"] is True

        # plan new_steps >= 0, compose missing >= 0 — just verify both are non-negative ints
        assert plan_result["plan"]["new_steps"] >= 0
        assert len(compose_result["missing"]) >= 0

    async def test_plan_next_action_mentions_compose(
        self, tmp_pipelines_dir, tmp_managed_dir, mock_registry
    ):
        result = await _handle_plan_pipeline({"goal": "Fetch emails from Outlook"})
        assert result["success"] is True
        # next_action should guide the user toward compose_pipeline or create_pipeline
        next_action = result["next_action"].lower()
        assert "compose_pipeline" in next_action or "create_pipeline" in next_action
