"""Tests for T-BRIX-V8-01: compose_pipeline — Intent-to-Pipeline Assembly.

Covers:
1. Intent-Parsing: various goals → correct keyword categories
2. Brick-Discovery: finds relevant bricks from the built-in registry
3. Pipeline-Discovery: finds existing pipelines by keyword
4. Helper-Discovery: finds existing helpers by keyword
5. Pipeline-Assembly: steps in correct order (source → transform → target)
6. Coverage calculation
7. Missing-step detection
8. Edge Cases: empty goal, unknown-keyword-only goal
"""
from __future__ import annotations

import asyncio
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from brix.mcp_handlers.composer import (
    _parse_intent,
    _discover_bricks,
    _discover_helpers,
    _discover_pipelines,
    _assemble_pipeline,
    _calculate_coverage,
    _collect_missing,
    _handle_compose_pipeline,
    _word_overlap,
    _keyword_hit_score,
)
from brix.helper_registry import HelperRegistry
from brix.mcp_server import _handle_compose_pipeline as _server_compose


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_pipelines_dir(tmp_path, monkeypatch):
    """Redirect pipeline storage to a temp directory.

    Patches _pipeline_dir() everywhere AND restricts PipelineStore search_paths
    to only the temp dir (so real /app/pipelines are not found during tests).
    """
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

    # Additionally patch PipelineStore to restrict search_paths in discover_pipelines
    # PipelineStore still includes /app/pipelines unless we force search_paths.
    OriginalPipelineStore = PipelineStore

    from brix.db import BrixDB
    isolated_db = BrixDB(db_path=tmp_path / "test.db")

    class IsolatedPipelineStore(OriginalPipelineStore):
        def __init__(self, pipelines_dir=None, search_paths=None, db=None):
            # Always restrict to our temp dir and isolated DB only
            super().__init__(
                pipelines_dir=pipelines_dir or patched_pipeline_dir(),
                search_paths=[patched_pipeline_dir()],
                db=isolated_db,
            )

    import brix.mcp_handlers.composer as cm
    monkeypatch.setattr(cm, "PipelineStore", IsolatedPipelineStore)
    return pipelines_dir


@pytest.fixture
def mock_registry(tmp_path, monkeypatch):
    """Patch HelperRegistry to use a temp file and isolated DB."""
    from brix.db import BrixDB
    reg_file = tmp_path / "registry.yaml"
    test_db = BrixDB(db_path=tmp_path / "helper_test.db")
    original_init = HelperRegistry.__init__

    def patched_init(self, registry_path=None, db=None):
        original_init(self, registry_path=reg_file, db=test_db)

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
# 1. Intent Parsing
# ---------------------------------------------------------------------------

class TestIntentParsing:
    def test_outlook_source_detected(self):
        intent = _parse_intent("Download emails from Outlook and store in database")
        assert "outlook" in intent["sources"]

    def test_gmail_source_detected(self):
        intent = _parse_intent("Fetch mails from Gmail inbox")
        assert "gmail" in intent["sources"]

    def test_onedrive_source_detected(self):
        intent = _parse_intent("Scan OneDrive folder for new files")
        assert "onedrive" in intent["sources"]

    def test_sparkasse_source_detected(self):
        intent = _parse_intent("Import Sparkasse Kontoauszug transactions")
        assert "sparkasse" in intent["sources"]

    def test_paypal_source_detected(self):
        intent = _parse_intent("Fetch PayPal payment transactions and classify them")
        assert "paypal" in intent["sources"]

    def test_file_source_detected(self):
        intent = _parse_intent("Read local files from a folder and convert them")
        assert "file" in intent["sources"]

    def test_download_action_detected(self):
        intent = _parse_intent("Download attachments from Outlook emails")
        assert "download" in intent["actions"]

    def test_extract_action_detected(self):
        intent = _parse_intent("Extract invoice data from PDF files")
        assert "extract" in intent["actions"]

    def test_classify_action_detected(self):
        intent = _parse_intent("Classify emails into categories")
        assert "classify" in intent["actions"]

    def test_ingest_action_detected(self):
        intent = _parse_intent("Ingest transactions into the database")
        assert "ingest" in intent["actions"]

    def test_send_action_detected(self):
        intent = _parse_intent("Send a notification email after processing")
        assert "send" in intent["actions"]

    def test_notify_action_detected(self):
        intent = _parse_intent("Notify the team when done")
        assert "notify" in intent["actions"]

    def test_database_target_detected(self):
        intent = _parse_intent("Store parsed data in the database")
        assert "database" in intent["targets"]

    def test_file_target_detected(self):
        intent = _parse_intent("Save results to a local JSON file")
        assert "file" in intent["targets"]

    def test_markdown_target_detected(self):
        intent = _parse_intent("Generate a markdown report")
        assert "markdown" in intent["targets"]

    def test_multiple_sources_detected(self):
        intent = _parse_intent("Download emails from Outlook and files from OneDrive")
        assert "outlook" in intent["sources"]
        assert "onedrive" in intent["sources"]

    def test_multiple_actions_detected(self):
        intent = _parse_intent("Fetch, classify and store emails")
        assert len(intent["actions"]) >= 2

    def test_german_keywords(self):
        intent = _parse_intent("E-Mails herunterladen und in Datenbank speichern")
        assert len(intent["sources"]) > 0
        assert len(intent["actions"]) > 0

    def test_empty_goal_returns_empty_intent(self):
        intent = _parse_intent("")
        assert intent["sources"] == []
        assert intent["actions"] == []
        assert intent["targets"] == []

    def test_unknown_keywords_return_empty(self):
        intent = _parse_intent("xyzzy frobulate quux")
        assert intent["sources"] == []
        assert intent["actions"] == []
        assert intent["targets"] == []

    def test_no_duplicates_in_sources(self):
        intent = _parse_intent("mail email e-mail outlook emails mails")
        # All map to 'outlook' — should only appear once
        assert intent["sources"].count("outlook") == 1


# ---------------------------------------------------------------------------
# 2. Brick Discovery
# ---------------------------------------------------------------------------

class TestBrickDiscovery:
    def test_discovers_bricks_for_http_goal(self):
        intent = _parse_intent("Fetch data from an HTTP API endpoint")
        matches = _discover_bricks(intent, "Fetch data from an HTTP API endpoint")
        names = [m["name"] for m in matches]
        # Should find some bricks (http_get, http_post, or similar)
        assert len(matches) > 0

    def test_discovery_returns_type_brick(self):
        intent = _parse_intent("Download files via HTTP")
        matches = _discover_bricks(intent, "Download files via HTTP")
        for m in matches:
            assert m["type"] == "brick"

    def test_discovery_has_relevance_field(self):
        intent = _parse_intent("Process email attachments")
        matches = _discover_bricks(intent, "Process email attachments")
        for m in matches:
            assert "relevance" in m
            assert 0.0 <= m["relevance"] <= 1.0

    def test_discovery_has_reason_field(self):
        intent = _parse_intent("Convert PDF documents")
        matches = _discover_bricks(intent, "Convert PDF documents")
        for m in matches:
            assert "reason" in m
            assert isinstance(m["reason"], str)

    def test_discovery_sorted_by_relevance_desc(self):
        intent = _parse_intent("Fetch and store data")
        matches = _discover_bricks(intent, "Fetch and store data")
        if len(matches) >= 2:
            relevances = [m["relevance"] for m in matches]
            assert relevances == sorted(relevances, reverse=True)

    def test_empty_intent_may_return_empty_or_low_score(self):
        intent = _parse_intent("xyzzy totally unknown frobulate")
        matches = _discover_bricks(intent, "xyzzy totally unknown frobulate")
        # With no keywords matched, we expect 0 or very few results
        assert isinstance(matches, list)


# ---------------------------------------------------------------------------
# 3. Pipeline Discovery
# ---------------------------------------------------------------------------

class TestPipelineDiscovery:
    async def test_discovers_existing_pipeline_by_name(self, tmp_pipelines_dir):
        """A pipeline named buddy-intake-outlook should match an Outlook goal."""
        # Create a dummy pipeline YAML
        (tmp_pipelines_dir / "buddy-intake-outlook.yaml").write_text(
            "name: buddy-intake-outlook\ndescription: Intake emails from Outlook\nsteps: []\n"
        )
        intent = _parse_intent("Fetch emails from Outlook")
        matches = _discover_pipelines(intent, "Fetch emails from Outlook")
        names = [m["name"] for m in matches]
        assert "buddy-intake-outlook" in names

    async def test_pipeline_match_has_correct_type(self, tmp_pipelines_dir):
        (tmp_pipelines_dir / "my-pipeline.yaml").write_text(
            "name: my-pipeline\ndescription: Download and process emails\nsteps: []\n"
        )
        intent = _parse_intent("Download emails")
        matches = _discover_pipelines(intent, "Download emails")
        for m in matches:
            assert m["type"] == "pipeline"

    async def test_empty_store_returns_no_matches(self, tmp_pipelines_dir):
        intent = _parse_intent("Process Outlook emails")
        matches = _discover_pipelines(intent, "Process Outlook emails")
        assert matches == []

    async def test_pipelines_sorted_by_relevance(self, tmp_pipelines_dir):
        (tmp_pipelines_dir / "buddy-intake-outlook.yaml").write_text(
            "name: buddy-intake-outlook\ndescription: Outlook email intake pipeline\nsteps: []\n"
        )
        (tmp_pipelines_dir / "convert-folder.yaml").write_text(
            "name: convert-folder\ndescription: Convert all files in a folder\nsteps: []\n"
        )
        intent = _parse_intent("Fetch emails from Outlook inbox")
        matches = _discover_pipelines(intent, "Fetch emails from Outlook inbox")
        if len(matches) >= 2:
            relevances = [m["relevance"] for m in matches]
            assert relevances == sorted(relevances, reverse=True)


# ---------------------------------------------------------------------------
# 4. Helper Discovery
# ---------------------------------------------------------------------------

class TestHelperDiscovery:
    async def test_discovers_existing_helper_by_name(self, tmp_managed_dir, mock_registry):
        """A helper named buddy_classify should match a classify goal."""
        reg = HelperRegistry()
        reg.register(
            name="buddy_classify",
            script="/app/helpers/buddy_classify.py",
            description="Classify emails into categories",
        )
        intent = _parse_intent("Classify incoming emails")
        matches = _discover_helpers(intent, "Classify incoming emails")
        names = [m["name"] for m in matches]
        assert "buddy_classify" in names

    async def test_helper_match_type(self, tmp_managed_dir, mock_registry):
        reg = HelperRegistry()
        reg.register(
            name="buddy_extract_contacts",
            script="/app/helpers/buddy_extract_contacts.py",
            description="Extract contact information from emails",
        )
        intent = _parse_intent("Extract contacts from emails")
        matches = _discover_helpers(intent, "Extract contacts from emails")
        for m in matches:
            assert m["type"] == "helper"

    async def test_empty_registry_returns_no_matches(self, tmp_managed_dir, mock_registry):
        intent = _parse_intent("Fetch emails from Outlook")
        matches = _discover_helpers(intent, "Fetch emails from Outlook")
        assert matches == []

    async def test_helpers_sorted_by_relevance(self, tmp_managed_dir, mock_registry):
        reg = HelperRegistry()
        reg.register(
            name="buddy_fetch_mails",
            script="/app/helpers/buddy_fetch_mails.py",
            description="Fetch emails from IMAP/Outlook",
        )
        reg.register(
            name="buddy_convert_folder",
            script="/app/helpers/buddy_convert_folder.py",
            description="Convert files in a folder",
        )
        intent = _parse_intent("Fetch Outlook emails")
        matches = _discover_helpers(intent, "Fetch Outlook emails")
        if len(matches) >= 2:
            relevances = [m["relevance"] for m in matches]
            assert relevances == sorted(relevances, reverse=True)


# ---------------------------------------------------------------------------
# 5. Pipeline Assembly
# ---------------------------------------------------------------------------

class TestPipelineAssembly:
    def test_assembly_returns_named_pipeline(self):
        intent = _parse_intent("Download emails from Outlook")
        proposed = _assemble_pipeline(intent, [], "my-pipeline", "Download emails from Outlook")
        assert proposed["name"] == "my-pipeline"

    def test_assembly_has_steps_list(self):
        intent = _parse_intent("Fetch emails and store in database")
        proposed = _assemble_pipeline(intent, [], "test", "Fetch emails and store in database")
        assert "steps" in proposed
        assert isinstance(proposed["steps"], list)
        assert len(proposed["steps"]) > 0

    def test_assembly_source_step_first(self):
        intent = _parse_intent("Fetch emails from Outlook and store in database")
        proposed = _assemble_pipeline(intent, [], "test", "Fetch emails from Outlook and store in database")
        steps = proposed["steps"]
        assert steps[0]["id"] == "fetch"

    def test_assembly_uses_pipeline_match_for_source(self):
        intent = _parse_intent("Fetch emails from Outlook")
        matches = [
            {"type": "pipeline", "name": "buddy-intake-outlook", "description": "Outlook intake", "relevance": 0.9, "reason": "keyword-match"},
        ]
        proposed = _assemble_pipeline(intent, matches, "test", "Fetch emails from Outlook")
        fetch_step = proposed["steps"][0]
        assert fetch_step["status"] == "AVAILABLE"
        assert "buddy-intake-outlook" in fetch_step.get("from", "")

    def test_assembly_marks_missing_steps(self):
        intent = _parse_intent("Extract invoice data from PDF and store in database")
        # No matches → all steps NEEDS_IMPLEMENTATION
        proposed = _assemble_pipeline(intent, [], "test", "Extract invoice data from PDF and store in database")
        steps = proposed["steps"]
        statuses = {s["id"]: s["status"] for s in steps}
        # At least one step should be NEEDS_IMPLEMENTATION when no matches
        needs_impl = [s for s in steps if s["status"] == "NEEDS_IMPLEMENTATION"]
        assert len(needs_impl) > 0

    def test_assembly_with_full_matches(self):
        intent = _parse_intent("Fetch Outlook emails, classify them, store in database")
        matches = [
            {"type": "pipeline", "name": "buddy-intake-outlook", "description": "Outlook intake", "relevance": 0.9, "reason": "keyword-match"},
            {"type": "helper", "name": "buddy_classify", "description": "Classify emails", "relevance": 0.8, "reason": "keyword-match"},
            {"type": "helper", "name": "buddy_ingest_db", "description": "Store in database", "relevance": 0.7, "reason": "keyword-match"},
        ]
        proposed = _assemble_pipeline(intent, matches, "test", "Fetch Outlook emails, classify them, store in database")
        steps = proposed["steps"]
        assert len(steps) >= 2  # At least fetch + one more

    def test_assembly_no_source_falls_back_to_best_match(self):
        intent = {"sources": [], "actions": ["extract"], "targets": ["database"]}
        matches = [
            {"type": "pipeline", "name": "some-pipeline", "description": "Some pipeline", "relevance": 0.5, "reason": "partial match"},
        ]
        proposed = _assemble_pipeline(intent, matches, "test", "extract and store data")
        fetch_step = proposed["steps"][0]
        assert fetch_step["id"] == "fetch"

    def test_assembly_no_matches_no_source_still_has_fetch(self):
        intent = {"sources": [], "actions": [], "targets": []}
        proposed = _assemble_pipeline(intent, [], "test", "xyzzy")
        assert proposed["steps"][0]["id"] == "fetch"
        assert proposed["steps"][0]["status"] == "NEEDS_IMPLEMENTATION"


# ---------------------------------------------------------------------------
# 6. Coverage Calculation
# ---------------------------------------------------------------------------

class TestCoverageCalculation:
    def test_all_available_is_100_percent(self):
        steps = [
            {"id": "fetch", "status": "AVAILABLE"},
            {"id": "extract", "status": "AVAILABLE"},
            {"id": "store", "status": "AVAILABLE"},
        ]
        assert _calculate_coverage(steps) == "100%"

    def test_all_missing_is_0_percent(self):
        steps = [
            {"id": "fetch", "status": "NEEDS_IMPLEMENTATION"},
            {"id": "store", "status": "NEEDS_IMPLEMENTATION"},
        ]
        assert _calculate_coverage(steps) == "0%"

    def test_partial_coverage(self):
        steps = [
            {"id": "fetch", "status": "AVAILABLE"},
            {"id": "extract", "status": "NEEDS_IMPLEMENTATION"},
            {"id": "store", "status": "NEEDS_IMPLEMENTATION"},
        ]
        # 1 of 3 = 33%
        assert _calculate_coverage(steps) == "33%"

    def test_two_thirds_coverage(self):
        steps = [
            {"id": "fetch", "status": "AVAILABLE"},
            {"id": "extract", "status": "AVAILABLE"},
            {"id": "store", "status": "NEEDS_IMPLEMENTATION"},
        ]
        assert _calculate_coverage(steps) == "66%"

    def test_empty_steps_is_zero(self):
        assert _calculate_coverage([]) == "0%"


# ---------------------------------------------------------------------------
# 7. Missing Steps Detection
# ---------------------------------------------------------------------------

class TestMissingSteps:
    def test_no_missing_when_all_available(self):
        steps = [
            {"id": "fetch", "status": "AVAILABLE", "description": "Fetch emails"},
            {"id": "store", "status": "AVAILABLE", "description": "Store in DB"},
        ]
        assert _collect_missing(steps) == []

    def test_collects_missing_steps(self):
        steps = [
            {"id": "fetch", "status": "AVAILABLE", "description": "Fetch emails"},
            {"id": "store", "status": "NEEDS_IMPLEMENTATION", "description": "Store in database"},
        ]
        missing = _collect_missing(steps)
        assert len(missing) == 1
        assert "store" in missing[0].lower() or "Store" in missing[0]

    def test_collects_multiple_missing_steps(self):
        steps = [
            {"id": "fetch", "status": "NEEDS_IMPLEMENTATION", "description": "Fetch data"},
            {"id": "extract", "status": "NEEDS_IMPLEMENTATION", "description": "Extract fields"},
        ]
        missing = _collect_missing(steps)
        assert len(missing) == 2

    def test_missing_descriptions_included(self):
        steps = [
            {"id": "store", "status": "NEEDS_IMPLEMENTATION", "description": "Store in database"},
        ]
        missing = _collect_missing(steps)
        assert "database" in missing[0].lower() or "Store" in missing[0]


# ---------------------------------------------------------------------------
# 8. Full Handler Integration
# ---------------------------------------------------------------------------

class TestHandlerIntegration:
    async def test_requires_goal(self):
        result = await _handle_compose_pipeline({})
        assert result["success"] is False
        assert "goal" in result["error"].lower()

    async def test_empty_goal_fails(self):
        result = await _handle_compose_pipeline({"goal": ""})
        assert result["success"] is False

    async def test_basic_goal_succeeds(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Download emails from Outlook and store in database"
        })
        assert result["success"] is True

    async def test_result_has_required_keys(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        required_keys = ["goal", "parsed_intent", "matches", "proposed_pipeline", "coverage", "missing", "next_steps"]
        for key in required_keys:
            assert key in result, f"Missing key: {key}"

    async def test_parsed_intent_has_sources_actions_targets(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Extract invoice data from PDF and store in database"
        })
        intent = result["parsed_intent"]
        assert "sources" in intent
        assert "actions" in intent
        assert "targets" in intent

    async def test_coverage_is_percentage_string(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Fetch emails from Outlook"
        })
        assert result["success"] is True
        coverage = result["coverage"]
        assert coverage.endswith("%")
        pct = int(coverage[:-1])
        assert 0 <= pct <= 100

    async def test_custom_name_used(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Fetch emails and process them",
            "name": "my-custom-pipeline",
        })
        assert result["success"] is True
        assert result["proposed_pipeline"]["name"] == "my-custom-pipeline"

    async def test_auto_name_derived_from_goal(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Download emails from Outlook"
        })
        assert result["success"] is True
        name = result["proposed_pipeline"]["name"]
        assert isinstance(name, str)
        assert len(name) > 0

    async def test_next_steps_is_list_of_strings(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Convert PDF files to markdown"
        })
        assert result["success"] is True
        next_steps = result["next_steps"]
        assert isinstance(next_steps, list)
        assert len(next_steps) > 0
        for step in next_steps:
            assert isinstance(step, str)

    async def test_matches_contains_type_field(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Fetch emails from Outlook"
        })
        assert result["success"] is True
        for match in result["matches"]:
            assert match["type"] in ("pipeline", "helper", "brick")

    async def test_unknown_keywords_still_succeeds(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        """Even with completely unknown keywords, handler should return success with empty matches."""
        result = await _handle_compose_pipeline({
            "goal": "xyzzy frobulate quux blargh"
        })
        assert result["success"] is True
        assert result["parsed_intent"]["sources"] == []
        assert result["parsed_intent"]["actions"] == []

    async def test_pipeline_with_existing_pipeline_match(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        """If a matching pipeline exists, it should appear in matches."""
        (tmp_pipelines_dir / "buddy-intake-outlook.yaml").write_text(
            "name: buddy-intake-outlook\ndescription: Intake emails from Outlook\nsteps: []\n"
        )
        result = await _handle_compose_pipeline({
            "goal": "Fetch Outlook emails"
        })
        assert result["success"] is True
        pipeline_matches = [m for m in result["matches"] if m["type"] == "pipeline"]
        names = [m["name"] for m in pipeline_matches]
        assert "buddy-intake-outlook" in names

    async def test_missing_list_describes_steps(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        """Missing steps should have descriptive text."""
        result = await _handle_compose_pipeline({
            "goal": "xyzzy frobulate quux"
        })
        missing = result["missing"]
        for m in missing:
            assert isinstance(m, str)
            assert len(m) > 0

    async def test_server_export_accessible(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        """The handler is accessible via mcp_server re-export."""
        result = await _server_compose({
            "goal": "Fetch emails from Outlook"
        })
        assert result["success"] is True


# ---------------------------------------------------------------------------
# 9. Word Overlap + Keyword Hit Score Utilities
# ---------------------------------------------------------------------------

class TestUtilities:
    def test_word_overlap_identical(self):
        assert _word_overlap("outlook mail", "outlook mail") == 1.0

    def test_word_overlap_partial(self):
        score = _word_overlap("outlook email fetch", "fetch emails")
        assert 0.0 < score < 1.0

    def test_word_overlap_no_common_words(self):
        assert _word_overlap("fetch emails", "convert pdf") == 0.0

    def test_word_overlap_handles_underscores(self):
        score = _word_overlap("buddy_fetch_mails", "fetch mails from outlook")
        assert score > 0.0

    def test_keyword_hit_score_empty_intent(self):
        score = _keyword_hit_score("outlook fetch emails", {"sources": [], "actions": [], "targets": []})
        assert score == 0.0

    def test_keyword_hit_score_positive(self):
        intent = _parse_intent("Fetch emails from Outlook")
        score = _keyword_hit_score("outlook mail fetch emails imap", intent)
        assert score > 0.0

    def test_keyword_hit_score_max_one(self):
        intent = _parse_intent("Fetch emails from Outlook")
        score = _keyword_hit_score("outlook mail fetch emails imap download", intent)
        assert score <= 1.0
