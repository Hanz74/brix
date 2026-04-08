from __future__ import annotations

from brix.scripts.backfill_helper_metadata import (
    build_updates,
    extract_first_docstring_summary,
    infer_project_from_name,
)


def test_infer_project_from_name_uses_prefix_map() -> None:
    assert infer_project_from_name("buddy_fetch_mail") == "buddy"
    assert infer_project_from_name("cody_extract_pdf") == "cody"
    assert infer_project_from_name("att_parse_invoice") == "buddy"
    assert infer_project_from_name("dedup_records") == "utility"
    assert infer_project_from_name("structured_output_normalize") == "utility"
    assert infer_project_from_name("misc_helper") is None


def test_extract_first_docstring_summary_prefers_module_docstring() -> None:
    code = '''"""Backfill helper metadata for existing DB rows.

    Keep the first sentence concise.
    """

def main() -> None:
    """Ignored function docstring."""
    return None
'''
    assert extract_first_docstring_summary(code) == "Backfill helper metadata for existing DB rows. Keep the first sentence concise."


def test_extract_first_docstring_summary_falls_back_to_first_nested_docstring() -> None:
    code = '''
def main() -> None:
    """Generate one-line descriptions for helpers."""
    return None
'''
    assert extract_first_docstring_summary(code) == "Generate one-line descriptions for helpers."


def test_build_updates_backfills_only_missing_fields() -> None:
    row = {
        "name": "buddy_missing_meta",
        "project": "",
        "description": "",
        "code": '"""Import buddy records from OneDrive."""\n',
    }

    assert build_updates(row) == {
        "project": "buddy",
        "description": "Import buddy records from OneDrive.",
    }


def test_build_updates_skips_when_values_already_present() -> None:
    row = {
        "name": "buddy_existing_meta",
        "project": "buddy",
        "description": "Existing description",
        "code": '"""Should not overwrite."""\n',
    }

    assert build_updates(row) == {}
