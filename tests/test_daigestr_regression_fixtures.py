from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from brix.db import BrixDB
from brix.runners.document_persistence import (
    DocumentPersistExtractionResultRunner,
    validate_canonical_extraction_result,
)


_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "daigestr"
_MANIFEST_PATH = _FIXTURE_DIR / "manifest.json"


def _load_manifest() -> list[dict]:
    return json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))


def _load_fixture(doc_id: int) -> dict:
    manifest = {entry["doc_id"]: entry for entry in _load_manifest()}
    fixture_path = Path(manifest[doc_id]["fixture_path"])
    if not fixture_path.is_absolute():
        fixture_path = Path(__file__).resolve().parents[1] / fixture_path
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _sqlite_documents_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "documents.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE documents (
                id INTEGER PRIMARY KEY,
                raw_structured TEXT,
                doc_type TEXT,
                content_hash TEXT,
                file_path TEXT,
                extraction_specialists TEXT
            )
            """
        )
        for doc_id in (12421, 12513, 12550):
            conn.execute(
                """
                INSERT INTO documents (id, raw_structured, doc_type, content_hash, file_path, extraction_specialists)
                VALUES (?, NULL, NULL, NULL, NULL, '[]')
                """,
                (doc_id,),
            )
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_daigestr_fixture_manifest_tracks_real_hmk_cases():
    manifest = _load_manifest()

    assert {entry["doc_id"] for entry in manifest} == {12421, 12513, 12550}


def test_daigestr_regression_fixtures_satisfy_canonical_contract():
    expected_issue_fields = {
        12421: set(),
        12513: {"raw.normalized"},
        12550: {"raw.normalized"},
    }

    for entry in _load_manifest():
        fixture = _load_fixture(entry["doc_id"])
        issues = validate_canonical_extraction_result(fixture["extraction_result"])
        assert {issue["field"] for issue in issues} == expected_issue_fields[entry["doc_id"]], entry["fixture_path"]


def test_daigestr_regression_fixtures_persist_successfully(tmp_path):
    db_path = _sqlite_documents_db(tmp_path)
    runner = DocumentPersistExtractionResultRunner()
    expected_success = {
        12421: True,
        12513: False,
        12550: False,
    }

    for entry in _load_manifest():
        fixture = _load_fixture(entry["doc_id"])
        step = SimpleNamespace(
            config={
                "connection": str(db_path),
                "document_id": fixture["doc_id"],
                "extraction_result": fixture["extraction_result"],
                "specialist_name": "hmk_extracted",
            },
            timeout=None,
        )

        result = asyncio.run(runner.execute(step, context=None))

        assert result["success"] is expected_success[entry["doc_id"]]
        if not expected_success[entry["doc_id"]]:
            assert "raw.normalized" in result["error"]

    db = BrixDB(db_path=db_path)
    with db._connect() as conn:
        rows = conn.execute("SELECT id, raw_structured, doc_type FROM documents ORDER BY id").fetchall()

    assert len(rows) == 3
    for row in rows:
        if row[0] == 12421:
            payload = json.loads(row[1])
            assert payload["raw"]["meta"]["document_type"] == row[2]
        else:
            assert row[1] is None
            assert row[2] is None


def test_bundled_bank_statement_fixture_contains_multiple_statements():
    fixture = _load_fixture(12421)
    payload = fixture["extraction_result"]
    markdown = payload.get("markdown") or ""
    kontoauszug_hits = re.findall(r"Kontoauszug\s+\d+", markdown)

    assert len(kontoauszug_hits) >= 3
    normalized = payload["raw"]["normalized"]
    assert not normalized.get("kontoauszuege")
    assert not normalized.get("zeitraum")


def test_bundled_bank_statement_fixture_still_exposes_single_shape_without_bundle_fields(tmp_path):
    db_path = _sqlite_documents_db(tmp_path)
    runner = DocumentPersistExtractionResultRunner()
    fixture = _load_fixture(12421)
    step = SimpleNamespace(
        config={
            "connection": str(db_path),
            "document_id": 12421,
            "extraction_result": fixture["extraction_result"],
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    shape = result["data"]["document_shape"]
    assert shape["is_bundle"] is False
    assert shape["document_type"] == "bank_statement"
    assert shape["statement_count"] == 1
    assert shape["statement_numbers"] == []
    assert shape["period_from"] is None
    assert shape["period_to"] is None
    assert shape["bundle_gate_pass"] is False
    assert {finding["code"] for finding in shape["bundle_findings"]} == {
        "BUNDLED_STATEMENT_MISSING_ARRAY",
        "BUNDLED_STATEMENT_MISSING_PERIOD",
        "BUNDLED_STATEMENT_MISSING_BOOKING_COVERAGE",
    }
    quality_retry = result["data"]["quality_retry"]
    assert quality_retry["document_type"] == "bank_statement"
    assert quality_retry["template_used"] == "bank_statement"
    assert quality_retry["retry_applied"] is True
    assert quality_retry["initial_mode"] == "default"
    assert quality_retry["final_mode"] == "full"
    assert quality_retry["quality_score"] == fixture["extraction_result"]["raw"]["meta"]["quality_score"]
