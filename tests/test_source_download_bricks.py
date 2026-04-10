from __future__ import annotations

import asyncio
import base64
from types import SimpleNamespace

from brix.bricks.builtins import ALL_BUILTINS
from brix.db import BrixDB
from brix.migrations import MIGRATIONS, _register_source_download_bricks_v89, run_pending_migrations
from brix.runners.source_download import (
    SourceDownloadToFileRunner,
    SourcePersistDownloadPayloadRunner,
)


def test_source_download_to_file_stages_remote_result(monkeypatch, tmp_path):
    class FakeHeaders:
        def get_content_type(self):
            return "application/pdf"

    class FakeResponse:
        headers = FakeHeaders()

        def read(self):
            return b"%PDF-1.4 test"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=30: FakeResponse())

    runner = SourceDownloadToFileRunner()
    step = SimpleNamespace(
        config={
            "url": "https://example.com/invoice.pdf",
            "output_dir": str(tmp_path),
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["file_bytes_path"].endswith("invoice.pdf")
    assert data["mime_type"] == "application/pdf"
    assert data["extractable"] is True


def test_source_persist_download_payload_normalizes_base64(tmp_path):
    runner = SourcePersistDownloadPayloadRunner()
    step = SimpleNamespace(
        config={
            "filename": "result.json",
            "base64": base64.b64encode(b'{"ok": true}').decode("ascii"),
            "output_dir": str(tmp_path),
            "mime_type": "application/json",
            "source_url": "mcp://m365/file/123",
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["mime_type"] == "application/json"
    assert data["source_url"] == "mcp://m365/file/123"
    assert data["extractable"] is True


def test_source_persist_download_payload_requires_filename(tmp_path):
    runner = SourcePersistDownloadPayloadRunner()
    step = SimpleNamespace(
        config={
            "base64": base64.b64encode(b"payload").decode("ascii"),
            "output_dir": str(tmp_path),
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is False
    assert "filename" in result["error"]


def test_source_download_bricks_are_registered_in_builtins():
    names = {brick.name for brick in ALL_BUILTINS}
    assert "source.download_to_file" in names
    assert "source.persist_download_payload" in names


def test_migration_registers_source_download_bricks(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")

    _register_source_download_bricks_v89(db)

    assert db.brick_definitions_get("source.download_to_file") is not None
    assert db.brick_definitions_get("source.persist_download_payload") is not None


def test_v89_runs_via_normal_migration_loop(tmp_path, monkeypatch):
    db = BrixDB(db_path=tmp_path / "migration_loop.db")
    fake_v89 = {
        "version": len(MIGRATIONS) + 1,
        "name": "test_register_source_download_bricks",
        "up": "",
        "up_fn": "_register_source_download_bricks_v89",
        "down": "",
    }
    monkeypatch.setattr("brix.migrations.MIGRATIONS", [*MIGRATIONS, fake_v89])

    applied = run_pending_migrations(db)

    assert applied[-1]["name"] == "test_register_source_download_bricks"
    assert db.brick_definitions_get("source.download_to_file") is not None
    assert db.brick_definitions_get("source.persist_download_payload") is not None
