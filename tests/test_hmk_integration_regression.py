from __future__ import annotations

import asyncio
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

from brix.db import BrixDB
from brix.runners.document_persistence import DocumentPersistExtractionResultRunner


_ROOT = Path(__file__).resolve().parents[1]
_FIXTURE_DIR = _ROOT / "tests" / "fixtures" / "daigestr"


def _load_hmk_fixture_module():
    module_path = _ROOT / "scripts" / "hmk_last_test_fixture.py"
    spec = importlib.util.spec_from_file_location("hmk_last_test_fixture", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_manifest() -> list[dict]:
    return json.loads((_FIXTURE_DIR / "manifest.json").read_text(encoding="utf-8"))


def _load_fixture(doc_id: int) -> dict:
    manifest = {entry["doc_id"]: entry for entry in _load_manifest()}
    path = _ROOT / manifest[doc_id]["fixture_path"]
    return json.loads(path.read_text(encoding="utf-8"))


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


def test_hmk_fixture_script_matches_regression_manifest():
    module = _load_hmk_fixture_module()
    manifest = _load_manifest()

    expected = {(doc.doc_id, doc.file_name) for doc in module.HMK_LAST_TEST_DOCS}
    actual = {(entry["doc_id"], entry["file_name"]) for entry in manifest}

    assert actual == expected


def test_hmk_regression_fixtures_capture_retry_enabled_live_extract_results():
    expected_document_types = {
        12421: "bank_statement",
        12513: "receipt",
        12550: "invoice",
    }

    for doc_id, document_type in expected_document_types.items():
        fixture = _load_fixture(doc_id)
        meta = fixture["extraction_result"]["raw"]["meta"]
        assert meta["document_type"] == document_type
        assert meta["retry_applied"] is True
        assert meta["initial_mode"] == "default"
        assert meta["final_mode"] == "full"
        assert meta["quality_score"] == meta["final_quality_score"]


def test_hmk_regression_fixtures_produce_consistent_persistence_summaries(tmp_path):
    db_path = _sqlite_documents_db(tmp_path)
    runner = DocumentPersistExtractionResultRunner()

    for doc_id in (12421, 12513, 12550):
        fixture = _load_fixture(doc_id)
        step = SimpleNamespace(
            config={
                "connection": str(db_path),
                "document_id": doc_id,
                "extraction_result": fixture["extraction_result"],
            },
            timeout=None,
        )
        result = asyncio.run(runner.execute(step, context=None))

        if doc_id == 12421:
            assert result["success"] is True
            assert result["data"]["quality_retry"]["retry_applied"] is True
            assert result["data"]["document_shape"]["document_type"] == "bank_statement"
        else:
            assert result["success"] is False
            assert "raw.normalized" in result["error"]

    db = BrixDB(db_path=db_path)
    with db._connect() as conn:
        rows = conn.execute("SELECT id, doc_type FROM documents ORDER BY id").fetchall()
    assert rows == [(12421, "bank_statement"), (12513, None), (12550, None)]
