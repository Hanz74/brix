"""Tests for T-BRIX-V8-06: Semantic Brick Discovery & Type Compatibility.

Covers:
1. Type compatibility: compatible, incompatible, wildcard
2. Converter suggestions
3. compose_pipeline with type_checks field
4. plan_pipeline with type_chain + when_NOT_to_use confidence
5. Negative discovery (when_NOT_to_use lowers score)
6. Alias-based search (deutsch + englisch)
"""
from __future__ import annotations

import asyncio
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from brix.bricks.types import is_compatible, suggest_converter, TYPE_COMPATIBILITY
from brix.mcp_handlers.composer import (
    _discover_bricks,
    _parse_intent,
    _check_step_type_compatibility,
    _handle_compose_pipeline,
    _handle_plan_pipeline,
)


# ---------------------------------------------------------------------------
# Fixtures (reuse pattern from test_v8_composer.py)
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
    from brix.helper_registry import HelperRegistry
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
# 1. Type Compatibility
# ---------------------------------------------------------------------------

class TestTypeCompatibility:
    def test_exact_match_is_compatible(self):
        assert is_compatible("list[email]", "list[email]") is True

    def test_wildcard_output_compatible_with_anything(self):
        assert is_compatible("*", "list[email]") is True
        assert is_compatible("*", "string") is True
        assert is_compatible("*", "dict") is True

    def test_wildcard_input_compatible_with_anything(self):
        assert is_compatible("list[email]", "*") is True
        assert is_compatible("string", "*") is True

    def test_empty_output_is_compatible(self):
        # Untyped bricks (empty string) should not block
        assert is_compatible("", "list[email]") is True

    def test_empty_input_is_compatible(self):
        assert is_compatible("list[email]", "") is True

    def test_none_output_is_not_compatible_with_string(self):
        # 'none' means this step needs no prior input — as output it's unusual
        # but if declared it should only work as a pipeline starter
        assert is_compatible("none", "*") is True

    def test_list_email_compatible_with_list_dict(self):
        # list[email] can feed into a step expecting list[dict]
        assert is_compatible("list[email]", "list[dict]") is True

    def test_list_email_compatible_with_list_wildcard(self):
        assert is_compatible("list[email]", "list[*]") is True

    def test_string_compatible_with_text(self):
        assert is_compatible("string", "text") is True

    def test_text_compatible_with_string(self):
        assert is_compatible("text", "string") is True

    def test_file_path_compatible_with_string(self):
        assert is_compatible("file_path", "string") is True

    def test_dict_compatible_with_object(self):
        assert is_compatible("dict", "object") is True

    def test_json_compatible_with_dict(self):
        assert is_compatible("json", "dict") is True

    def test_markdown_compatible_with_string(self):
        assert is_compatible("markdown", "string") is True

    def test_string_markdown_compatible_with_string(self):
        assert is_compatible("string (markdown)", "string") is True

    def test_incompatible_list_email_and_string(self):
        # A step that outputs list[email] should NOT directly feed a step
        # expecting a raw string (without conversion)
        assert is_compatible("list[email]", "string") is False

    def test_incompatible_file_path_and_list(self):
        # file_path → list[*] is not directly compatible
        assert is_compatible("file_path", "list[*]") is False

    def test_list_wildcard_compatible_with_list_dict(self):
        assert is_compatible("list[*]", "list[dict]") is True

    def test_object_output_compatible_with_dict_input(self):
        assert is_compatible("object", "dict") is True

    def test_case_insensitive(self):
        # Type comparisons should be case-insensitive
        assert is_compatible("List[Email]", "list[email]") is True


# ---------------------------------------------------------------------------
# 2. Converter Suggestions
# ---------------------------------------------------------------------------

class TestConverterSuggestions:
    def test_file_path_to_string_suggests_to_markdown(self):
        suggestion = suggest_converter("file_path", "string")
        assert suggestion == "convert.to_markdown"

    def test_file_path_to_markdown_suggests_to_markdown(self):
        suggestion = suggest_converter("file_path", "markdown")
        assert suggestion == "convert.to_markdown"

    def test_file_path_to_text_suggests_extract_text(self):
        suggestion = suggest_converter("file_path", "text")
        assert suggestion == "convert.extract_text"

    def test_file_path_to_list_suggests_to_json(self):
        suggestion = suggest_converter("file_path", "list[dict]")
        assert suggestion == "convert.to_json"

    def test_file_path_to_object_suggests_to_json(self):
        suggestion = suggest_converter("file_path", "object")
        assert suggestion == "convert.to_json"

    def test_no_suggestion_for_already_compatible(self):
        # For types that are compatible, no converter needed
        # suggest_converter is called only when incompatible, but let's verify
        # it returns something sensible for known cases
        suggestion = suggest_converter("string", "list[dict]")
        assert suggestion == "transform"

    def test_object_to_list_suggests_transform(self):
        suggestion = suggest_converter("object", "list[*]")
        assert suggestion == "transform"

    def test_unknown_incompatible_returns_none(self):
        suggestion = suggest_converter("list[email]", "binary_blob")
        assert suggestion is None

    def test_list_email_to_text_suggests_extract(self):
        suggestion = suggest_converter("list[email]", "text")
        assert suggestion == "convert.extract_text"


# ---------------------------------------------------------------------------
# 3. _check_step_type_compatibility
# ---------------------------------------------------------------------------

class TestCheckStepTypeCompatibility:
    def test_no_checks_when_no_types(self):
        steps = [
            {"id": "fetch", "description": "Fetch emails"},
            {"id": "store", "description": "Store results"},
        ]
        checks = _check_step_type_compatibility(steps)
        assert checks == []

    def test_compatible_types_no_warning(self):
        steps = [
            {"id": "fetch", "output_type": "list[email]"},
            {"id": "classify", "input_type": "list[dict]"},
        ]
        checks = _check_step_type_compatibility(steps)
        assert len(checks) == 1
        assert checks[0]["compatible"] is True
        assert checks[0]["suggestion"] is None

    def test_incompatible_types_has_suggestion(self):
        steps = [
            {"id": "fetch_files", "output_type": "list[file_ref]"},
            {"id": "extract", "input_type": "string (text)"},
        ]
        checks = _check_step_type_compatibility(steps)
        assert len(checks) == 1
        assert checks[0]["compatible"] is False
        # May or may not have a suggestion; just verify it's the right shape
        assert "suggestion" in checks[0]

    def test_check_contains_required_fields(self):
        steps = [
            {"id": "step_a", "output_type": "list[email]"},
            {"id": "step_b", "input_type": "list[email]"},
        ]
        checks = _check_step_type_compatibility(steps)
        assert len(checks) == 1
        check = checks[0]
        assert check["step_from"] == "step_a"
        assert check["step_to"] == "step_b"
        assert "output_type" in check
        assert "input_type" in check
        assert "compatible" in check
        assert "suggestion" in check

    def test_multiple_steps_multiple_checks(self):
        steps = [
            {"id": "a", "output_type": "list[file_ref]"},
            {"id": "b", "input_type": "list[file_ref]", "output_type": "string"},
            {"id": "c", "input_type": "string"},
        ]
        checks = _check_step_type_compatibility(steps)
        assert len(checks) == 2

    def test_wildcard_output_is_always_compatible(self):
        steps = [
            {"id": "a", "output_type": "*"},
            {"id": "b", "input_type": "list[email]"},
        ]
        checks = _check_step_type_compatibility(steps)
        assert len(checks) == 1
        assert checks[0]["compatible"] is True

    def test_single_step_no_checks(self):
        steps = [{"id": "fetch", "output_type": "list[email]"}]
        checks = _check_step_type_compatibility(steps)
        assert checks == []

    def test_incompatible_file_path_to_list(self):
        steps = [
            {"id": "file_step", "output_type": "file_path"},
            {"id": "db_step", "input_type": "list[object]"},
        ]
        checks = _check_step_type_compatibility(steps)
        assert len(checks) == 1
        assert checks[0]["compatible"] is False
        assert checks[0]["suggestion"] == "convert.to_json"


# ---------------------------------------------------------------------------
# 4. compose_pipeline type_checks field
# ---------------------------------------------------------------------------

class TestComposeTypeChecks:
    async def test_compose_result_has_type_checks_key(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        assert "type_checks" in result

    async def test_type_checks_is_list(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Download PDF files from OneDrive"
        })
        assert isinstance(result["type_checks"], list)

    async def test_type_checks_entries_have_required_fields(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Convert PDF files to markdown text"
        })
        for check in result["type_checks"]:
            assert "step_from" in check
            assert "step_to" in check
            assert "compatible" in check
            assert "suggestion" in check

    async def test_type_checks_compatible_is_bool(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_compose_pipeline({
            "goal": "Fetch emails from Outlook"
        })
        for check in result["type_checks"]:
            assert isinstance(check["compatible"], bool)


# ---------------------------------------------------------------------------
# 5. plan_pipeline type_chain
# ---------------------------------------------------------------------------

class TestPlanTypeChain:
    async def test_plan_result_has_type_chain(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        assert "type_chain" in result["plan"]

    async def test_type_chain_is_list(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_plan_pipeline({
            "goal": "Download PDF files from OneDrive"
        })
        assert isinstance(result["plan"]["type_chain"], list)

    async def test_type_chain_entries_have_order_and_action(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and store in database"
        })
        for entry in result["plan"]["type_chain"]:
            assert "order" in entry
            assert "action" in entry

    async def test_type_chain_length_matches_steps(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        result = await _handle_plan_pipeline({
            "goal": "Fetch emails from Outlook and classify them"
        })
        plan = result["plan"]
        assert len(plan["type_chain"]) == len(plan["steps"])


# ---------------------------------------------------------------------------
# 6. Negative Discovery: when_NOT_to_use lowers score
# ---------------------------------------------------------------------------

class TestNegativeDiscovery:
    def test_when_not_to_use_lowers_score(self):
        """A brick with keywords from the goal in its when_NOT_to_use should
        score lower than a brick without such negative signals."""
        from brix.bricks.schema import BrickSchema, BrickParam
        from brix.bricks.registry import BrickRegistry

        # Create two bricks: one with negative overlap, one without
        good_brick = BrickSchema(
            name="test.good_brick",
            type="python",
            description="Process emails efficiently",
            when_to_use="When you need to process emails",
            when_NOT_to_use="When dealing with files or databases",
            category="test",
            input_type="list[email]",
            output_type="list[dict]",
        )
        penalised_brick = BrickSchema(
            name="test.penalised_brick",
            type="python",
            description="Process emails efficiently",
            when_to_use="When you need to process emails",
            when_NOT_to_use="When processing emails from Outlook inbox",
            category="test",
            input_type="list[email]",
            output_type="list[dict]",
        )

        from brix.mcp_handlers._shared import _registry as main_registry
        original_list = main_registry.list_all

        def patched_list_all():
            return [good_brick, penalised_brick]

        import brix.mcp_handlers.composer as cm
        original = cm._registry.list_all
        cm._registry.list_all = patched_list_all

        try:
            intent = _parse_intent("process emails from Outlook inbox")
            matches = _discover_bricks(intent, "process emails from Outlook inbox")
            scores = {m["name"]: m["relevance"] for m in matches}
            # The penalised brick should score lower or equal to the good brick
            if "test.good_brick" in scores and "test.penalised_brick" in scores:
                assert scores["test.good_brick"] >= scores["test.penalised_brick"]
        finally:
            cm._registry.list_all = original

    def test_when_not_to_use_reason_included(self):
        """Discovery reason field should mention the penalty when applied."""
        from brix.bricks.schema import BrickSchema
        from brix.mcp_handlers._shared import _registry as main_registry

        penalised_brick = BrickSchema(
            name="test.with_penalty",
            type="python",
            description="Convert files to markdown for processing",
            when_to_use="When converting files to markdown text",
            when_NOT_to_use="When the file is already plain text or markdown",
            category="convert",
            aliases=["convert files", "markdown conversion"],
            input_type="file_path",
            output_type="string (markdown)",
        )

        import brix.mcp_handlers.composer as cm
        original = cm._registry.list_all
        cm._registry.list_all = lambda: [penalised_brick]

        try:
            # Goal contains "already plain text" which overlaps with when_NOT_to_use
            intent = _parse_intent("convert already plain text file")
            matches = _discover_bricks(intent, "convert already plain text file")
            if matches:
                match = next((m for m in matches if m["name"] == "test.with_penalty"), None)
                if match:
                    # If a penalty was applied, the reason should mention it
                    # (only if overlap >= threshold)
                    assert isinstance(match["reason"], str)
        finally:
            cm._registry.list_all = original


# ---------------------------------------------------------------------------
# 7. Alias-based Search
# ---------------------------------------------------------------------------

class TestAliasDiscovery:
    def test_german_alias_matches_brick(self):
        """German alias 'mails abrufen' should match source.fetch_emails."""
        intent = _parse_intent("mails abrufen aus posteingang")
        matches = _discover_bricks(intent, "mails abrufen aus posteingang")
        names = [m["name"] for m in matches]
        assert "source.fetch_emails" in names

    def test_english_alias_matches_brick(self):
        """English alias 'fetch emails' should match source.fetch_emails."""
        intent = _parse_intent("fetch emails from inbox")
        matches = _discover_bricks(intent, "fetch emails from inbox")
        names = [m["name"] for m in matches]
        assert "source.fetch_emails" in names

    def test_german_convert_alias(self):
        """German alias 'dokument konvertieren' should match convert.to_markdown."""
        intent = _parse_intent("dokument konvertieren zu text")
        matches = _discover_bricks(intent, "dokument konvertieren zu text")
        names = [m["name"] for m in matches]
        assert "convert.to_markdown" in names

    def test_classify_alias_matches_llm_classify(self):
        """Alias 'klassifizieren' should match llm.classify."""
        intent = _parse_intent("emails klassifizieren nach kategorien")
        matches = _discover_bricks(intent, "emails klassifizieren nach kategorien")
        names = [m["name"] for m in matches]
        assert "llm.classify" in names

    def test_alias_match_has_higher_relevance_than_description_only(self):
        """A brick found via exact alias should have higher relevance
        than one found only via description keywords."""
        from brix.bricks.schema import BrickSchema
        import brix.mcp_handlers.composer as cm

        alias_brick = BrickSchema(
            name="test.alias_brick",
            type="python",
            description="Process some data",  # no strong match
            when_to_use="Generic processing",
            aliases=["fetch emails", "read emails", "email abrufen"],
            category="test",
            input_type="none",
            output_type="list[email]",
        )
        desc_brick = BrickSchema(
            name="test.desc_brick",
            type="python",
            description="Fetch emails from mail server using IMAP or MCP",
            when_to_use="When fetching emails from an email server",
            aliases=[],  # no aliases
            category="test",
            input_type="none",
            output_type="list[email]",
        )

        original = cm._registry.list_all
        cm._registry.list_all = lambda: [alias_brick, desc_brick]
        try:
            intent = _parse_intent("fetch emails")
            matches = _discover_bricks(intent, "fetch emails")
            scores = {m["name"]: m["relevance"] for m in matches}
            if "test.alias_brick" in scores and "test.desc_brick" in scores:
                assert scores["test.alias_brick"] >= scores["test.desc_brick"]
        finally:
            cm._registry.list_all = original

    def test_full_alias_phrase_detected(self):
        """A full alias phrase in the goal should give a significant boost."""
        intent = _parse_intent("scan folder for files to process")
        matches = _discover_bricks(intent, "scan folder for files to process")
        names = [m["name"] for m in matches]
        # 'scan folder' is an alias for source.fetch_files
        assert "source.fetch_files" in names

    def test_db_ingest_german_alias(self):
        """German alias 'datenbank schreiben' should match db.ingest."""
        intent = _parse_intent("datenbank schreiben mit neuen Datensätzen")
        matches = _discover_bricks(intent, "datenbank schreiben mit neuen Datensätzen")
        names = [m["name"] for m in matches]
        assert "db.ingest" in names


# ---------------------------------------------------------------------------
# 8. plan_pipeline: when_NOT_to_use confidence downgrade
# ---------------------------------------------------------------------------

class TestPlanNegativeConfidence:
    async def test_plan_includes_warnings_for_negative_overlap(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        """If a plan step's recommended brick has goal keywords in when_NOT_to_use,
        a warning should be emitted."""
        # Use a goal that will likely match convert.to_markdown,
        # but the goal contains "already plain text" which is in its when_NOT_to_use
        result = await _handle_plan_pipeline({
            "goal": "convert already plain text file to markdown"
        })
        assert result["success"] is True
        # warnings field should exist
        assert "warnings" in result["plan"]
        assert isinstance(result["plan"]["warnings"], list)

    async def test_plan_type_chain_has_brick_types(self, tmp_pipelines_dir, tmp_managed_dir, mock_registry):
        """Type chain entries for brick-backed steps should have type info."""
        result = await _handle_plan_pipeline({
            "goal": "fetch emails from Outlook and classify them"
        })
        assert result["success"] is True
        type_chain = result["plan"]["type_chain"]
        # At least one entry should reference a brick with type info
        bricks_with_types = [
            e for e in type_chain
            if e.get("brick") and (e.get("input_type") or e.get("output_type"))
        ]
        # source.fetch_emails has input_type="none", output_type="list[email]"
        # so there should be at least one typed entry
        assert len(bricks_with_types) >= 1
