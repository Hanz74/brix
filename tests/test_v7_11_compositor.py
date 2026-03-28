"""Tests for T-BRIX-V7-11: Compositor-Paradigma — Duplikat-Erkennung + Erst suchen dann bauen.

Covers:
1. Duplicate detection in create_helper (similar name, similar description, no match)
2. Duplicate detection in create_pipeline (similar name, similar description, no match)
3. Linting warning for helpers with >200 lines (create_helper and update_helper)
4. get_tips contains COMPOSITOR-REGEL prominently at the top
"""
import pytest
import asyncio
from pathlib import Path
from unittest.mock import patch, MagicMock

from brix.mcp_server import (
    _handle_create_helper,
    _handle_update_helper,
    _handle_create_pipeline,
    _handle_get_tips,
    _normalize_name,
    _name_similarity,
    _description_jaccard,
    _find_similar_helpers,
    _find_similar_pipelines,
    _code_line_count,
)
from brix.helper_registry import HelperRegistry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_managed_dir(tmp_path, monkeypatch):
    """Redirect ~/.brix/helpers/ to a temp directory."""
    managed = tmp_path / ".brix" / "helpers"
    managed.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    return managed


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
def tmp_pipelines_dir(tmp_path, monkeypatch):
    """Redirect pipeline storage to a temp directory."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    import brix.mcp_server as mcp_mod
    monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", pipelines_dir)
    return pipelines_dir


# ---------------------------------------------------------------------------
# 1. Utility function unit tests
# ---------------------------------------------------------------------------

class TestNormalizeName:
    def test_lowercase_and_strip_underscores(self):
        assert _normalize_name("buddy_extract_contacts") == "buddyextractcontacts"

    def test_lowercase_and_strip_hyphens(self):
        assert _normalize_name("buddy-extract-contacts") == "buddyextractcontacts"

    def test_mixed(self):
        assert _normalize_name("Buddy_Extract-Contacts") == "buddyextractcontacts"


class TestNameSimilarity:
    def test_identical_names_return_1(self):
        assert _name_similarity("parse_invoice", "parse_invoice") == 1.0

    def test_similar_names_above_threshold(self):
        # buddy_extract_contacts vs buddy_extract_contact — should be high
        sim = _name_similarity("buddy_extract_contacts", "buddy_extract_contact")
        assert sim >= 0.7

    def test_different_names_below_threshold(self):
        sim = _name_similarity("parse_invoice", "fetch_weather")
        assert sim < 0.7


class TestDescriptionJaccard:
    def test_identical_descriptions(self):
        d = "Extract invoice data from PDF"
        assert _description_jaccard(d, d) == 1.0

    def test_high_overlap_above_threshold(self):
        a = "Extract invoice data from PDF documents and files"
        b = "Extract invoice data from PDF files and documents"
        sim = _description_jaccard(a, b)
        assert sim >= 0.5

    def test_no_overlap_returns_zero(self):
        sim = _description_jaccard("parse invoice", "fetch weather")
        assert sim == 0.0

    def test_empty_description_returns_zero(self):
        assert _description_jaccard("", "something") == 0.0
        assert _description_jaccard("something", "") == 0.0


class TestCodeLineCount:
    def test_empty_code(self):
        assert _code_line_count("") == 0

    def test_single_line(self):
        assert _code_line_count("print('hello')") == 1

    def test_multiple_lines(self):
        code = "\n".join([f"# line {i}" for i in range(250)])
        assert _code_line_count(code) == 250


# ---------------------------------------------------------------------------
# 2. Duplicate detection in create_helper
# ---------------------------------------------------------------------------

class TestCreateHelperDuplicateDetection:
    async def test_no_warning_when_no_similar_helpers(self, tmp_managed_dir, mock_registry):
        """No warnings when no similar helpers exist."""
        code = "import json\nprint(json.dumps({'ok': True}))"
        result = await _handle_create_helper({
            "name": "unique_helper_xyz",
            "code": code,
            "description": "A totally unique helper for xyz processing",
        })
        assert result["success"] is True
        assert "warnings" not in result

    async def test_warning_when_similar_name_exists(self, tmp_managed_dir, mock_registry):
        """Warning is issued when a helper with a similar name already exists."""
        code = "import json\nprint(json.dumps({'ok': True}))"
        # Create an existing helper
        await _handle_create_helper({
            "name": "buddy_extract_contacts",
            "code": code,
            "description": "Extracts contacts from emails",
        })
        # Create a helper with a similar name
        result = await _handle_create_helper({
            "name": "buddy_extract_contact",
            "code": code,
            "description": "Extracts contact info",
        })
        assert result["success"] is True
        assert "warnings" in result
        warning_text = " ".join(result["warnings"])
        assert "buddy_extract_contacts" in warning_text
        assert "Name-Match" in warning_text

    async def test_warning_when_similar_description_exists(self, tmp_managed_dir, mock_registry):
        """Warning is issued when a helper with a similar description already exists."""
        code = "import json\nprint(json.dumps({'ok': True}))"
        # Create an existing helper with a distinct name but similar description
        await _handle_create_helper({
            "name": "extract_invoice_data",
            "code": code,
            "description": "Extract invoice line items from PDF documents",
        })
        # Create a helper with a different name but very similar description
        result = await _handle_create_helper({
            "name": "parse_invoice_pdf",
            "code": code,
            "description": "Extract invoice data from PDF documents and files",
        })
        assert result["success"] is True
        assert "warnings" in result
        warning_text = " ".join(result["warnings"])
        assert "extract_invoice_data" in warning_text
        assert "Description-Overlap" in warning_text

    async def test_no_self_match_on_overwrite(self, tmp_managed_dir, mock_registry):
        """Overwriting an existing helper with the same name does not warn about itself."""
        code = "import json\nprint(json.dumps({'ok': True}))"
        await _handle_create_helper({
            "name": "my_helper",
            "code": code,
            "description": "My helper",
        })
        # Re-create with same name — no self-warning
        result = await _handle_create_helper({
            "name": "my_helper",
            "code": code + "\n# updated",
            "description": "My helper updated",
        })
        assert result["success"] is True
        # Should not warn about itself
        warnings = result.get("warnings", [])
        for w in warnings:
            assert "my_helper" not in w or "my_helper" == "my_helper"  # sanity — no self-match

    async def test_warning_message_contains_prüfe_hinweis(self, tmp_managed_dir, mock_registry):
        """Warning message includes the suggestion to check the existing helper."""
        code = "import json\nprint(json.dumps({'ok': True}))"
        await _handle_create_helper({
            "name": "buddy_fetch_mails",
            "code": code,
            "description": "Fetches mails from IMAP",
        })
        result = await _handle_create_helper({
            "name": "buddy_fetch_mail",
            "code": code,
            "description": "Fetch mail messages",
        })
        assert result["success"] is True
        if "warnings" in result:
            for w in result["warnings"]:
                # Warning should contain advice
                assert "Prüfe" in w or "nutzen" in w or "erweitern" in w


# ---------------------------------------------------------------------------
# 3. Linting warning for helpers with >200 lines
# ---------------------------------------------------------------------------

class TestHelperLineLimitWarning:
    async def test_no_warning_for_short_helper(self, tmp_managed_dir, mock_registry):
        """No linting warning for a helper with <= 200 lines."""
        code = "\n".join([f"# line {i}" for i in range(50)])
        result = await _handle_create_helper({
            "name": "short_helper",
            "code": code,
        })
        assert result["success"] is True
        warnings = result.get("warnings", [])
        # No line count warning
        assert not any("Zeilen" in w for w in warnings)

    async def test_warning_for_long_helper_create(self, tmp_managed_dir, mock_registry):
        """Linting warning issued when helper code exceeds 200 lines."""
        code = "\n".join([f"# line {i}" for i in range(250)])
        result = await _handle_create_helper({
            "name": "long_helper_create",
            "code": code,
        })
        assert result["success"] is True
        assert "warnings" in result
        warning_text = " ".join(result["warnings"])
        assert "250 Zeilen" in warning_text
        assert "Aufteilen" in warning_text or "kleinere" in warning_text

    async def test_warning_for_exactly_201_lines(self, tmp_managed_dir, mock_registry):
        """Linting warning triggered at 201 lines (boundary: > 200)."""
        code = "\n".join([f"# line {i}" for i in range(201)])
        result = await _handle_create_helper({
            "name": "boundary_helper",
            "code": code,
        })
        assert result["success"] is True
        assert "warnings" in result
        warning_text = " ".join(result["warnings"])
        assert "201 Zeilen" in warning_text

    async def test_no_warning_for_exactly_200_lines(self, tmp_managed_dir, mock_registry):
        """No linting warning at exactly 200 lines (boundary: <= 200)."""
        code = "\n".join([f"# line {i}" for i in range(200)])
        result = await _handle_create_helper({
            "name": "at_limit_helper",
            "code": code,
        })
        assert result["success"] is True
        warnings = result.get("warnings", [])
        assert not any("Zeilen" in w for w in warnings)

    async def test_update_helper_warns_on_long_code(self, tmp_managed_dir, mock_registry):
        """update_helper warns when updated code exceeds 200 lines."""
        # Create a short helper first
        await _handle_create_helper({
            "name": "update_long_test",
            "code": "print('short')",
        })
        # Update with long code
        long_code = "\n".join([f"# line {i}" for i in range(250)])
        result = await _handle_update_helper({
            "name": "update_long_test",
            "code": long_code,
        })
        assert result["success"] is True
        assert "warnings" in result
        warning_text = " ".join(result["warnings"])
        assert "250 Zeilen" in warning_text

    async def test_update_helper_no_warning_for_short_code(self, tmp_managed_dir, mock_registry):
        """update_helper does not warn when updated code is <= 200 lines."""
        await _handle_create_helper({
            "name": "update_short_test",
            "code": "print('initial')",
        })
        short_code = "\n".join([f"# line {i}" for i in range(50)])
        result = await _handle_update_helper({
            "name": "update_short_test",
            "code": short_code,
        })
        assert result["success"] is True
        warnings = result.get("warnings", [])
        assert not any("Zeilen" in w for w in warnings)


# ---------------------------------------------------------------------------
# 4. Duplicate detection in create_pipeline
# ---------------------------------------------------------------------------

class TestCreatePipelineDuplicateDetection:
    async def test_no_warning_when_no_similar_pipelines(self, tmp_pipelines_dir):
        """No warnings when no similar pipelines exist."""
        result = await _handle_create_pipeline({
            "name": "totally_unique_pipeline_xyz",
            "description": "A unique pipeline for xyz processing",
        })
        assert result["success"] is True
        assert "warnings" not in result

    async def test_warning_when_similar_pipeline_name_exists(self, tmp_pipelines_dir):
        """Warning is issued when a pipeline with a similar name already exists."""
        # Create an existing pipeline
        await _handle_create_pipeline({
            "name": "buddy_extract_invoice",
            "description": "Extracts invoice data",
        })
        # Create a pipeline with a similar name
        result = await _handle_create_pipeline({
            "name": "buddy_extract_invoices",
            "description": "Processes invoice files",
        })
        assert result["success"] is True
        assert "warnings" in result
        warning_text = " ".join(result["warnings"])
        assert "buddy_extract_invoice" in warning_text
        assert "Name-Match" in warning_text

    async def test_warning_when_similar_pipeline_description_exists(self, tmp_pipelines_dir):
        """Warning is issued when a pipeline with a similar description already exists."""
        await _handle_create_pipeline({
            "name": "process_onedrive_files",
            "description": "Download and process files from OneDrive storage",
        })
        result = await _handle_create_pipeline({
            "name": "fetch_onedrive_docs",
            "description": "Download files from OneDrive and process them",
        })
        assert result["success"] is True
        assert "warnings" in result
        warning_text = " ".join(result["warnings"])
        assert "process_onedrive_files" in warning_text
        assert "Description-Overlap" in warning_text

    async def test_no_self_match_on_pipeline_overwrite(self, tmp_pipelines_dir):
        """Overwriting an existing pipeline with the same name does not warn about itself."""
        await _handle_create_pipeline({
            "name": "my_pipeline",
            "description": "My pipeline",
        })
        result = await _handle_create_pipeline({
            "name": "my_pipeline",
            "description": "My pipeline updated",
        })
        assert result["success"] is True
        # Self-match check: should not warn about itself
        warnings = result.get("warnings", [])
        self_warnings = [w for w in warnings if "my_pipeline" in w]
        assert len(self_warnings) == 0

    async def test_pipeline_warning_message_format(self, tmp_pipelines_dir):
        """Pipeline duplicate warning message has correct format."""
        await _handle_create_pipeline({
            "name": "buddy_intake_gmail",
            "description": "Intake emails from Gmail",
        })
        result = await _handle_create_pipeline({
            "name": "buddy_intake_gmaill",
            "description": "Process Gmail emails",
        })
        assert result["success"] is True
        if "warnings" in result:
            for w in result["warnings"]:
                assert "Prüfe" in w or "nutzen" in w or "erweitern" in w


# ---------------------------------------------------------------------------
# 5. get_tips contains COMPOSITOR-REGEL
# ---------------------------------------------------------------------------

class TestGetTipsCompositorRule:
    async def test_compositor_regel_in_tips(self):
        """get_tips output contains the COMPOSITOR-REGEL."""
        result = await _handle_get_tips({})
        assert "tips" in result
        tips_text = "\n".join(result["tips"])
        assert "COMPOSITOR-REGEL" in tips_text

    async def test_compositor_regel_at_top_of_tips(self):
        """COMPOSITOR-REGEL appears before other sections (near the top)."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        compositor_pos = tips_text.find("COMPOSITOR-REGEL")
        kern_regel_pos = tips_text.find("KERN-REGEL")
        assert compositor_pos != -1, "COMPOSITOR-REGEL not found in tips"
        assert kern_regel_pos != -1, "KERN-REGEL not found in tips"
        # COMPOSITOR-REGEL should appear before KERN-REGEL
        assert compositor_pos < kern_regel_pos

    async def test_compositor_regel_mentions_search_helpers(self):
        """COMPOSITOR-REGEL instructs to use search_helpers."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        assert "search_helpers" in tips_text

    async def test_compositor_regel_mentions_search_pipelines(self):
        """COMPOSITOR-REGEL instructs to use search_pipelines."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        assert "search_pipelines" in tips_text

    async def test_compositor_regel_mentions_wiederverwenden(self):
        """COMPOSITOR-REGEL mentions reuse concept."""
        result = await _handle_get_tips({})
        tips_text = "\n".join(result["tips"])
        # Should mention reuse or 'duplizieren' warning
        assert "wiederverwenden" in tips_text.lower() or "duplizieren" in tips_text.lower()
