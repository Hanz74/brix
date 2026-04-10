from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from brix.bricks.builtins import ALL_BUILTINS
from brix.db import BrixDB
from brix.migrations import MIGRATIONS, _register_document_persistence_bricks_v88, run_pending_migrations
from brix.runners.document_persistence import (
    DocumentMarkSpecialistProcessedRunner,
    DocumentPersistExtractionResultRunner,
)


def _sqlite_documents_db(tmp_path):
    db_path = tmp_path / "documents.sqlite"
    import sqlite3

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
        conn.execute(
            """
            INSERT INTO documents (id, raw_structured, doc_type, content_hash, file_path, extraction_specialists)
            VALUES (1, NULL, NULL, NULL, NULL, '[]')
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_document_persist_extraction_result_updates_document_row(tmp_path):
    db_path = _sqlite_documents_db(tmp_path)
    runner = DocumentPersistExtractionResultRunner()
    step = SimpleNamespace(
        config={
            "connection": str(db_path),
            "document_id": 1,
            "extraction_result": {
                "normalized": {"vendor_name": "REWE"},
                "document_type": "receipt",
                "quality_score": 0.85,
            },
            "content_hash": "hash-123",
            "file_path": "/tmp/test.pdf",
            "specialist_name": "hmk_extracted",
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert result["data"]["affected_rows"] == 1
    assert "raw_structured" in result["data"]["applied_fields"]
    db = BrixDB(db_path=db_path)
    with db._connect() as conn:
        row = conn.execute("SELECT * FROM documents WHERE id = 1").fetchone()
    assert row is not None
    assert json.loads(row[1])["document_type"] == "receipt"
    assert row[2] == "receipt"
    assert row[3] == "hash-123"
    assert row[4] == "/tmp/test.pdf"
    assert json.loads(row[5]) == ["hmk_extracted"]


def test_document_mark_specialist_processed_is_idempotent(tmp_path):
    db_path = _sqlite_documents_db(tmp_path)
    runner = DocumentMarkSpecialistProcessedRunner()
    step = SimpleNamespace(
        config={
            "connection": str(db_path),
            "document_id": 1,
            "specialist_name": "hmk_extracted",
        },
        timeout=None,
    )

    first = asyncio.run(runner.execute(step, context=None))
    second = asyncio.run(runner.execute(step, context=None))

    assert first["success"] is True
    assert first["data"]["specialist_added"] is True
    assert second["success"] is True
    assert second["data"]["affected_rows"] == 0
    assert second["data"]["specialist_added"] is False
    db = BrixDB(db_path=db_path)
    with db._connect() as conn:
        row = conn.execute("SELECT extraction_specialists FROM documents WHERE id = 1").fetchone()
    assert row is not None
    assert json.loads(row[0]) == ["hmk_extracted"]


def test_document_persistence_bricks_are_registered_in_builtins():
    builtin_names = {brick.name for brick in ALL_BUILTINS}
    assert "document.persist_extraction_result" in builtin_names
    assert "document.mark_specialist_processed" in builtin_names


def test_migration_registers_document_persistence_bricks(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")

    _register_document_persistence_bricks_v88(db)

    persist = db.brick_definitions_get("document.persist_extraction_result")
    mark = db.brick_definitions_get("document.mark_specialist_processed")
    assert persist is not None
    assert mark is not None
    assert persist["runner"] == "document_persist_extraction_result"
    assert mark["runner"] == "document_mark_specialist_processed"


def test_postgresql_uses_jsonb_cast_for_custom_raw_field(monkeypatch):
    executed: list[str] = []

    class FakeCursor:
        rowcount = 1

        def execute(self, query, params):
            executed.append(query)

        def fetchone(self):
            return ([],)

        def close(self):
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(
        "brix.runners.document_persistence.DbExecRunner._resolve_connection",
        staticmethod(lambda connection_ref, context=None: ("postgresql", "postgresql://example")),
    )
    monkeypatch.setattr("brix.runners.document_persistence._connect", lambda driver, dsn: FakeConnection())

    runner = DocumentPersistExtractionResultRunner()
    step = SimpleNamespace(
        config={
            "connection": "buddy-db",
            "document_id": 7,
            "raw_field": "payload_json",
            "extraction_result": {"document_type": "invoice"},
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    update_query = next(query for query in executed if query.startswith("UPDATE"))
    assert "payload_json = CAST(%s AS JSONB)" in update_query


def test_v88_runs_via_normal_migration_loop(tmp_path, monkeypatch):
    db = BrixDB(db_path=tmp_path / "migration_loop.db")
    fake_v88 = {
        "version": len(MIGRATIONS) + 1,
        "name": "test_register_document_persistence_bricks",
        "up": "",
        "up_fn": "_register_document_persistence_bricks_v88",
        "down": "",
    }
    monkeypatch.setattr("brix.migrations.MIGRATIONS", [*MIGRATIONS, fake_v88])

    applied = run_pending_migrations(db)

    assert applied[-1]["name"] == "test_register_document_persistence_bricks"
    assert db.brick_definitions_get("document.persist_extraction_result") is not None
    assert db.brick_definitions_get("document.mark_specialist_processed") is not None
