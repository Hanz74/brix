from __future__ import annotations

import asyncio
from types import SimpleNamespace

from brix.bricks.builtins import ALL_BUILTINS
from brix.db import BrixDB
from brix.migrations import MIGRATIONS, _register_document_extract_bricks_v90, run_pending_migrations
from brix.models import Step
from brix.runners.document_extract import (
    DocumentPrepareExtractablePayloadRunner,
    ExtractDocumentWithDaigestrRunner,
)


def test_prepare_extractable_payload_reads_file_and_base64_encodes(tmp_path):
    file_path = tmp_path / "invoice.pdf"
    file_path.write_bytes(b"pdf-content")
    runner = DocumentPrepareExtractablePayloadRunner()
    step = SimpleNamespace(
        config={
            "file_bytes_path": str(file_path),
            "mime_type": "application/pdf",
            "language": "de",
            "include_base64": True,
            "metadata": {"source": "hmk"},
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["file_bytes_path"] == str(file_path)
    assert data["base64"]
    assert data["mime_type"] == "application/pdf"
    assert data["metadata"]["source"] == "hmk"


def test_extract_document_with_daigestr_normalizes_response(monkeypatch, tmp_path):
    file_path = tmp_path / "invoice.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "receipt",
                    "quality_score": 0.91,
                    "template_used": "receipt",
                },
                "normalized": {"vendor_name": "REWE"},
                "markdown": "# test",
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            assert url.endswith("/v1/convert")
            assert json["auto_extract"] is True
            assert json["mode"] == "default"
            assert json["retry_on_low_quality"] is True
            assert json["quality_retry_threshold"] == 0.75
            assert json["quality_retry_mode"] == "full"
            assert json["base64"]
            assert json["content"]
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = SimpleNamespace(
        config={
            "file_bytes_path": str(file_path),
            "language": "de",
            "metadata": {"source": "hmk"},
        },
        timeout=None,
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["normalized"]["vendor_name"] == "REWE"
    assert data["document_type"] == "receipt"
    assert data["quality_score"] == 0.91
    assert data["_meta"]["template"] == "receipt"
    assert data["attempt_history"] == [
        {
            "attempt": 1,
            "attempt_count": 1,
            "mode": "default",
            "quality_score": 0.91,
            "request_id": None,
            "status": "completed",
        }
    ]


def test_prepare_extractable_payload_prefers_step_params(tmp_path):
    file_path = tmp_path / "params.pdf"
    file_path.write_bytes(b"pdf-content")
    runner = DocumentPrepareExtractablePayloadRunner()
    step = Step(
        id="prepare",
        type="document.prepare_extractable_payload",
        params={
            "file_bytes_path": str(file_path),
            "mime_type": "application/pdf",
            "include_base64": True,
            "metadata": {"source": "params"},
        },
    )

    result = asyncio.run(runner.execute(step, context=SimpleNamespace(last_output={"file_bytes_path": "ignored"})))

    assert result["success"] is True
    data = result["data"]
    assert data["file_bytes_path"] == str(file_path)
    assert data["mime_type"] == "application/pdf"
    assert data["metadata"]["source"] == "params"
    assert data["base64"]


def test_extract_document_with_daigestr_prefers_step_params(monkeypatch, tmp_path):
    file_path = tmp_path / "params.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "invoice",
                    "quality_score": 0.77,
                },
                "normalized": {"vendor_name": "Params Vendor"},
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            assert url.endswith("/v1/convert")
            assert json["auto_extract"] is True
            assert json["filename"] == "params.pdf"
            assert json["mode"] == "default"
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={
            "file_bytes_path": str(file_path),
            "filename": "params.pdf",
            "language": "de",
            "metadata": {"source": "params"},
        },
    )

    result = asyncio.run(runner.execute(step, context=SimpleNamespace(last_output={"file_bytes_path": "ignored"})))

    assert result["success"] is True
    data = result["data"]
    assert data["normalized"]["vendor_name"] == "Params Vendor"
    assert data["document_type"] == "invoice"
    assert data["quality_score"] == 0.77


def test_extract_document_with_daigestr_prefers_explicit_base_url_and_endpoint(monkeypatch, tmp_path):
    file_path = tmp_path / "override.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"normalized": {"vendor_name": "Override"}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["url"] = url
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_URL", "http://daigestr:9999")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={
            "file_bytes_path": str(file_path),
            "endpoint": "/v1/extract",
        },
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert captured["url"] == "http://daigestr:9999/v1/extract"
    assert captured["json"]["auto_extract"] is True


def test_extract_document_with_daigestr_uses_env_default_endpoint(monkeypatch, tmp_path):
    file_path = tmp_path / "env-default.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"normalized": {"vendor_name": "Env Default"}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["url"] = url
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_URL", "http://daigestr:9999")
    monkeypatch.setenv("BRIX_DAIGESTR_CONVERT_ENDPOINT", "/custom/convert")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={"file_bytes_path": str(file_path)},
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert captured["url"] == "http://daigestr:9999/custom/convert"


def test_extract_document_with_daigestr_uses_retry_env_defaults(monkeypatch, tmp_path):
    file_path = tmp_path / "retry-defaults.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"normalized": {"vendor_name": "Retry Defaults"}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)
    monkeypatch.setenv("BRIX_DAIGESTR_MODE", "default")
    monkeypatch.setenv("BRIX_DAIGESTR_RETRY_ON_LOW_QUALITY", "true")
    monkeypatch.setenv("BRIX_DAIGESTR_QUALITY_RETRY_THRESHOLD", "0.75")
    monkeypatch.setenv("BRIX_DAIGESTR_QUALITY_RETRY_MODE", "full")

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={"file_bytes_path": str(file_path)},
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert captured["json"]["mode"] == "default"
    assert captured["json"]["retry_on_low_quality"] is True
    assert captured["json"]["quality_retry_threshold"] == 0.75
    assert captured["json"]["quality_retry_mode"] == "full"


def test_extract_document_with_daigestr_prefers_explicit_retry_settings(monkeypatch, tmp_path):
    file_path = tmp_path / "retry-explicit.pdf"
    file_path.write_bytes(b"pdf-content")
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"normalized": {"vendor_name": "Retry Explicit"}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(
        id="extract",
        type="extract.document_with_daigestr",
        params={
            "file_bytes_path": str(file_path),
            "mode": "default",
            "retry_on_low_quality": True,
            "quality_retry_threshold": 0.8,
            "quality_retry_mode": "full",
        },
    )

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    assert captured["json"]["mode"] == "default"
    assert captured["json"]["retry_on_low_quality"] is True
    assert captured["json"]["quality_retry_threshold"] == 0.8
    assert captured["json"]["quality_retry_mode"] == "full"


def test_document_extract_bricks_are_registered_in_builtins():
    names = {brick.name for brick in ALL_BUILTINS}
    assert "document.prepare_extractable_payload" in names
    assert "extract.document_with_daigestr" in names
    prepare = next(brick for brick in ALL_BUILTINS if brick.name == "document.prepare_extractable_payload")
    assert prepare.input_type == "remote_download_result"


def test_migration_registers_document_extract_bricks(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")

    _register_document_extract_bricks_v90(db)

    assert db.brick_definitions_get("document.prepare_extractable_payload") is not None
    assert db.brick_definitions_get("extract.document_with_daigestr") is not None


def test_extract_document_with_daigestr_uses_meta_as_canonical_contract(monkeypatch, tmp_path):
    file_path = tmp_path / "meta-contract.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "bank_statement",
                    "document_type_confidence": 0.99,
                    "template_used": "bank_statement",
                    "quality_score": 0.52,
                    "quality_grade": "medium",
                    "retry_applied": True,
                    "retry_reason": "low_quality",
                    "initial_mode": "default",
                    "final_mode": "full",
                    "initial_quality_score": 0.52,
                    "final_quality_score": 0.8045,
                    "retry_threshold_used": 0.75,
                    "request_id": "req-123",
                    "attempt_number": 2,
                    "attempt_count": 2,
                    "attempt_mode": "full",
                    "pipeline_steps": ["ocr", "dual_pass_validation"],
                },
                "extracted": {"iban": "DE62..."},
                "normalized": {"iban_normalized": "DE62..."},
                "markdown": "# statement",
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["document_type"] == "bank_statement"
    assert data["quality_score"] == 0.52
    assert data["_quality_score"] == 0.52
    assert data["_meta"]["template"] == "bank_statement"
    assert data["_meta"]["final_quality_score"] == 0.8045
    assert data["_meta"]["pipeline_steps"] == ["ocr", "dual_pass_validation"]
    assert data["attempt_history"] == [
        {
            "attempt": 1,
            "attempt_count": 2,
            "mode": "default",
            "quality_score": 0.52,
            "request_id": "req-123",
            "retry_reason": "low_quality",
            "retry_threshold_used": 0.75,
            "status": "retry_triggered",
        },
        {
            "attempt": 2,
            "attempt_count": 2,
            "mode": "full",
            "quality_score": 0.8045,
            "request_id": "req-123",
            "retry_reason": "low_quality",
            "retry_threshold_used": 0.75,
            "status": "completed",
        },
    ]


def test_extract_document_with_daigestr_updates_progress_with_attempt_metadata(monkeypatch, tmp_path):
    file_path = tmp_path / "attempt-progress.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "invoice",
                    "template_used": "invoice",
                    "quality_score": 0.81,
                    "request_id": "req-456",
                    "attempt_number": 2,
                    "attempt_count": 2,
                    "attempt_mode": "full",
                    "retry_applied": True,
                    "retry_reason": "low_quality",
                },
                "normalized": {"invoice_number": "R-1"},
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    updates: list[dict] = []

    class FakeContext:
        def update_step_progress(self, step_id, payload):
            updates.append({"step_id": step_id, **payload})

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=FakeContext()))

    assert result["success"] is True
    assert updates[0]["stage"] == "request"
    assert updates[0]["attempt_mode"] == "default"
    assert updates[1]["stage"] == "result"
    assert updates[1]["attempt_number"] == 2
    assert updates[1]["attempt_count"] == 2
    assert updates[1]["attempt_mode"] == "full"
    assert updates[1]["retry_applied"] is True
    assert updates[1]["retry_reason"] == "low_quality"
    assert updates[1]["request_id"] == "req-456"


def test_extract_document_with_daigestr_ignores_noncanonical_top_level_mirrors(monkeypatch, tmp_path):
    file_path = tmp_path / "mirror-conflict.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "bank_statement",
                    "template_used": "bank_statement",
                    "quality_score": 0.81,
                },
                "document_type": "invoice",
                "template": "invoice",
                "quality_score": 0.12,
                "_quality_score": 0.11,
                "normalized": {
                    "document_type": "receipt",
                    "_quality_score": 0.25,
                    "iban": "DE62...",
                },
                "extracted": {
                    "document_type": "letter",
                    "quality_score": 0.33,
                },
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["document_type"] == "bank_statement"
    assert data["quality_score"] == 0.81
    assert data["_quality_score"] == 0.81
    assert data["_meta"]["template"] == "bank_statement"


def test_extract_document_with_daigestr_canonicalizes_partial_business_payloads(monkeypatch, tmp_path):
    file_path = tmp_path / "partial-business-payload.pdf"
    file_path.write_bytes(b"pdf-content")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "meta": {
                    "document_type": "receipt",
                    "template_used": "receipt",
                    "quality_score": 0.77,
                },
                "extracted": {"vendor_name": "REWE"},
                "normalized": {"vendor_name": "REWE", "summary": "Receipt summary"},
                "raw": {
                    "meta": {
                        "document_type": "receipt",
                        "template_used": "receipt",
                        "quality_score": 0.77,
                    }
                },
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

    monkeypatch.setattr("brix.runners.document_extract.httpx.AsyncClient", FakeClient)

    runner = ExtractDocumentWithDaigestrRunner()
    step = Step(id="extract", type="extract.document_with_daigestr", params={"file_bytes_path": str(file_path)})

    result = asyncio.run(runner.execute(step, context=None))

    assert result["success"] is True
    data = result["data"]
    assert data["normalized"] == {"vendor_name": "REWE", "summary": "Receipt summary"}
    assert data["raw"]["extracted"] == {"vendor_name": "REWE"}
    assert data["raw"]["normalized"] == {"vendor_name": "REWE", "summary": "Receipt summary"}


def test_v90_runs_via_normal_migration_loop(tmp_path, monkeypatch):
    db = BrixDB(db_path=tmp_path / "migration_loop.db")
    fake_v90 = {
        "version": len(MIGRATIONS) + 1,
        "name": "test_register_document_extract_bricks",
        "up": "",
        "up_fn": "_register_document_extract_bricks_v90",
        "down": "",
    }
    monkeypatch.setattr("brix.migrations.MIGRATIONS", [*MIGRATIONS, fake_v90])

    applied = run_pending_migrations(db)

    assert applied[-1]["name"] == "test_register_document_extract_bricks"
    assert db.brick_definitions_get("document.prepare_extractable_payload") is not None
    assert db.brick_definitions_get("extract.document_with_daigestr") is not None
