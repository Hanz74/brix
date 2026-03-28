"""Tests for T-BRIX-DB-10: Helper-to-Brick migration mapping.

Verifies that:
- HELPER_TO_BRICK_MAPPING contains all expected replaceable helpers
- Not-convertible helpers have a reason
- analyze_migration returns correct summary structure
- Each type (single_brick, pipeline, not_convertible) has required fields
"""
import pytest

from brix.migration_templates import HELPER_TO_BRICK_MAPPING, analyze_migration


# ---------------------------------------------------------------------------
# Expected helper lists
# ---------------------------------------------------------------------------

EXPECTED_CONVERTIBLE = {
    # Query helpers (8)
    "buddy_onedrive_filter",
    "dedup_filter",
    "filter_mails_by_keywords",
    "inline_extract",
    "extract_attachment_urls",
    "list_files",
    "debug_id",
    "debug_raw",
    # Extract helpers (17)
    "buddy_extract_contacts",
    "buddy_extract_persons",
    "buddy_extract_beihilfe",
    "buddy_extract_debeka",
    "buddy_extract_deadlines",
    "buddy_extract_insurance",
    "buddy_extract_invoice_review",
    "buddy_extract_kfz",
    "buddy_extract_line_items",
    "buddy_extract_payment_terms",
    "buddy_extract_references",
    "buddy_extract_salary",
    "buddy_extract_language",
    "buddy_extract_tax_ids",
    "buddy_extract_promocodes",
    "buddy_extract_tax_hints",
    # MarkItDown helpers (4)
    "buddy_onedrive_download",
    "convert_files",
    "buddy_onedrive_backfill_familie",
    "analyze_unknown_sonstiges",
    # Upsert/write helpers (6)
    "save_attachment",
    "save_markdown",
    "structured_save",
    "buddy_ingest_transactions",
    "insert_birthdays",
    "parse_ics_birthdays",
}

EXPECTED_NOT_CONVERTIBLE = {
    "buddy_extract_iban",
    "buddy_onedrive_scan",
    "buddy_intake_onedrive",
    "buddy_intake_process",
    "buddy_extract_cases",
    "buddy_classify",
    "buddy_parse_bank",
    "buddy_parse_kontoauszug",
    "buddy_llm_batch",
}


# ---------------------------------------------------------------------------
# Mapping structure tests
# ---------------------------------------------------------------------------

class TestMappingCompleteness:
    def test_all_convertible_helpers_present(self):
        """All 37 expected convertible helpers must be in the mapping."""
        missing = EXPECTED_CONVERTIBLE - set(HELPER_TO_BRICK_MAPPING.keys())
        assert not missing, f"Missing convertible helpers: {sorted(missing)}"

    def test_all_not_convertible_helpers_present(self):
        """All 9 not-convertible helpers must be in the mapping."""
        missing = EXPECTED_NOT_CONVERTIBLE - set(HELPER_TO_BRICK_MAPPING.keys())
        assert not missing, f"Missing not-convertible helpers: {sorted(missing)}"

    def test_total_count(self):
        """Mapping must contain all convertible + not-convertible helpers."""
        expected = len(EXPECTED_CONVERTIBLE) + len(EXPECTED_NOT_CONVERTIBLE)
        assert len(HELPER_TO_BRICK_MAPPING) >= expected

    def test_no_duplicate_keys(self):
        """Dict keys are unique by definition, but spot-check for expected helpers."""
        all_keys = list(HELPER_TO_BRICK_MAPPING.keys())
        assert len(all_keys) == len(set(all_keys))


class TestMappingTypes:
    def test_all_entries_have_type(self):
        """Every entry must have a 'type' field."""
        for name, entry in HELPER_TO_BRICK_MAPPING.items():
            assert "type" in entry, f"Helper '{name}' missing 'type' field"

    def test_valid_types(self):
        """All types must be one of the three valid values."""
        valid_types = {"single_brick", "pipeline", "not_convertible"}
        for name, entry in HELPER_TO_BRICK_MAPPING.items():
            assert entry["type"] in valid_types, (
                f"Helper '{name}' has invalid type '{entry['type']}'"
            )

    def test_not_convertible_helpers_have_reason(self):
        """All not-convertible entries must have a non-empty reason."""
        for name in EXPECTED_NOT_CONVERTIBLE:
            entry = HELPER_TO_BRICK_MAPPING[name]
            assert entry["type"] == "not_convertible", f"'{name}' should be not_convertible"
            assert "reason" in entry, f"'{name}' missing 'reason' field"
            assert entry["reason"].strip(), f"'{name}' has empty reason"

    def test_single_brick_entries_have_brick(self):
        """All single_brick entries must name a brick."""
        for name, entry in HELPER_TO_BRICK_MAPPING.items():
            if entry["type"] == "single_brick":
                assert "brick" in entry, f"'{name}' single_brick missing 'brick' field"
                assert entry["brick"], f"'{name}' single_brick has empty brick name"

    def test_pipeline_entries_have_steps(self):
        """All pipeline entries must have at least 2 steps."""
        for name, entry in HELPER_TO_BRICK_MAPPING.items():
            if entry["type"] == "pipeline":
                steps = entry.get("steps", [])
                assert len(steps) >= 2, (
                    f"Pipeline '{name}' must have >= 2 steps, got {len(steps)}"
                )

    def test_pipeline_steps_have_ids(self):
        """Each step in pipeline entries must have an 'id' field."""
        for name, entry in HELPER_TO_BRICK_MAPPING.items():
            if entry["type"] == "pipeline":
                for i, step in enumerate(entry.get("steps", [])):
                    assert "id" in step, (
                        f"Pipeline '{name}' step[{i}] missing 'id'"
                    )

    def test_extract_pipelines_have_three_steps(self):
        """Standard extract pipelines (fetch → llm/regex → upsert) have exactly 3 steps
        unless method is regex_and_llm (which adds a filter step = 4 steps)."""
        extract_helpers = [
            "buddy_extract_contacts", "buddy_extract_persons", "buddy_extract_beihilfe",
            "buddy_extract_debeka", "buddy_extract_deadlines", "buddy_extract_insurance",
            "buddy_extract_invoice_review", "buddy_extract_kfz", "buddy_extract_line_items",
            "buddy_extract_payment_terms", "buddy_extract_references", "buddy_extract_salary",
            "buddy_extract_language", "buddy_extract_tax_ids", "buddy_extract_promocodes",
        ]
        for name in extract_helpers:
            entry = HELPER_TO_BRICK_MAPPING[name]
            steps = entry.get("steps", [])
            assert len(steps) == 3, (
                f"Extract pipeline '{name}' should have 3 steps, got {len(steps)}"
            )

    def test_regex_and_llm_has_four_steps(self):
        """buddy_extract_tax_hints has regex prefilter + llm, so 4 steps."""
        entry = HELPER_TO_BRICK_MAPPING["buddy_extract_tax_hints"]
        assert entry["type"] == "pipeline"
        assert len(entry["steps"]) == 4


# ---------------------------------------------------------------------------
# analyze_migration function tests
# ---------------------------------------------------------------------------

class TestAnalyzeMigration:
    def test_returns_dict(self):
        result = analyze_migration()
        assert isinstance(result, dict)

    def test_success_true(self):
        result = analyze_migration()
        assert result["success"] is True

    def test_total_count(self):
        result = analyze_migration()
        assert result["total"] == len(HELPER_TO_BRICK_MAPPING)

    def test_convertible_count(self):
        result = analyze_migration()
        assert result["convertible_count"] == len(EXPECTED_CONVERTIBLE)

    def test_not_convertible_count(self):
        result = analyze_migration()
        assert result["not_convertible_count"] == len(EXPECTED_NOT_CONVERTIBLE)

    def test_summary_has_by_type(self):
        result = analyze_migration()
        assert "by_type" in result["summary"]
        by_type = result["summary"]["by_type"]
        assert "single_brick" in by_type
        assert "pipeline" in by_type
        assert "not_convertible" in by_type

    def test_summary_not_convertible_count_matches(self):
        result = analyze_migration()
        assert result["summary"]["by_type"]["not_convertible"] == 9

    def test_helpers_list_present(self):
        result = analyze_migration()
        assert "helpers" in result
        assert isinstance(result["helpers"], list)
        assert len(result["helpers"]) == len(HELPER_TO_BRICK_MAPPING)

    def test_helper_detail_by_name(self):
        result = analyze_migration(helper_name="buddy_extract_contacts")
        assert result["success"] is True
        assert result["helper"] == "buddy_extract_contacts"
        assert "migration" in result
        assert result["migration"]["type"] == "pipeline"

    def test_not_convertible_detail(self):
        result = analyze_migration(helper_name="buddy_extract_iban")
        assert result["success"] is True
        assert result["migration"]["type"] == "not_convertible"
        assert "reason" in result["migration"]

    def test_unknown_helper_returns_error(self):
        result = analyze_migration(helper_name="nonexistent_helper")
        assert result["success"] is False
        assert "error" in result
        assert "available" in result

    def test_each_helper_detail_has_name(self):
        result = analyze_migration()
        for detail in result["helpers"]:
            assert "name" in detail
            assert "type" in detail

    def test_pipeline_details_include_step_count(self):
        result = analyze_migration()
        pipeline_details = [h for h in result["helpers"] if h["type"] == "pipeline"]
        for detail in pipeline_details:
            assert "step_count" in detail
            assert detail["step_count"] >= 2

    def test_not_convertible_details_include_reason(self):
        result = analyze_migration()
        nc_details = [h for h in result["helpers"] if h["type"] == "not_convertible"]
        for detail in nc_details:
            assert "reason" in detail
            assert detail["reason"].strip()
