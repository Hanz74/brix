"""Tests for brix.runners.source.SourceRunner (T-BRIX-DB-05)."""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from brix.runners.source import SourceRunner, _normalize_outlook_message, _build_local_file_item


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step object that mirrors how the engine passes config."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _make_outlook_message(
    msg_id: str = "msg-001",
    subject: str = "Test Subject",
    received: str = "2026-01-15T10:00:00Z",
    has_attachments: bool = False,
    from_addr: str = "sender@example.com",
    body: str = "Hello World",
) -> dict:
    return {
        "id": msg_id,
        "subject": subject,
        "receivedDateTime": received,
        "hasAttachments": has_attachments,
        "isRead": False,
        "importance": "normal",
        "categories": [],
        "from": {"emailAddress": {"address": from_addr, "name": "Sender"}},
        "toRecipients": [{"emailAddress": {"address": "me@example.com"}}],
        "body": {"contentType": "html", "content": body},
    }


# ---------------------------------------------------------------------------
# config_schema / input_type / output_type
# ---------------------------------------------------------------------------


def test_config_schema_structure():
    runner = SourceRunner()
    schema = runner.config_schema()
    assert schema["type"] == "object"
    assert "connector" in schema["properties"]
    assert "connector" in schema["required"]


def test_config_schema_has_local_files_params():
    runner = SourceRunner()
    props = runner.config_schema()["properties"]
    assert "path" in props
    assert "pattern" in props
    assert "recursive" in props


def test_config_schema_has_outlook_params():
    runner = SourceRunner()
    props = runner.config_schema()["properties"]
    assert "folder" in props
    assert "filter" in props
    assert "limit" in props


def test_input_type():
    assert SourceRunner().input_type() == "none"


def test_output_type():
    assert SourceRunner().output_type() == "list[dict]"


# ---------------------------------------------------------------------------
# local_files — basic scan
# ---------------------------------------------------------------------------


async def test_local_files_basic(tmp_path):
    """Scans a directory and returns NormalizedItem dicts for each file."""
    (tmp_path / "a.txt").write_text("hello")
    (tmp_path / "b.txt").write_text("world")
    (tmp_path / "c.pdf").write_text("%PDF")

    runner = SourceRunner()
    step = _Step(connector="local_files", path=str(tmp_path))
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    items = result["data"]
    assert len(items) == 3
    titles = {i["title"] for i in items}
    assert "a.txt" in titles
    assert "b.txt" in titles
    assert "c.pdf" in titles


async def test_local_files_pattern_filter(tmp_path):
    """Pattern filter limits results to matching files only."""
    (tmp_path / "invoice.pdf").write_text("%PDF")
    (tmp_path / "notes.txt").write_text("text")
    (tmp_path / "report.pdf").write_text("%PDF")

    runner = SourceRunner()
    step = _Step(connector="local_files", path=str(tmp_path), pattern="*.pdf")
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    items = result["data"]
    assert len(items) == 2
    for item in items:
        assert item["title"].endswith(".pdf")


async def test_local_files_recursive(tmp_path):
    """Recursive mode finds files in subdirectories."""
    (tmp_path / "root.txt").write_text("root")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "nested.txt").write_text("nested")
    deep = sub / "deep"
    deep.mkdir()
    (deep / "very_deep.txt").write_text("deep")

    runner = SourceRunner()
    step = _Step(connector="local_files", path=str(tmp_path), recursive=True)
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    titles = {i["title"] for i in result["data"]}
    assert "root.txt" in titles
    assert "nested.txt" in titles
    assert "very_deep.txt" in titles
    assert len(result["data"]) == 3


async def test_local_files_recursive_with_pattern(tmp_path):
    """Recursive + pattern filter works together."""
    (tmp_path / "top.pdf").write_text("%PDF")
    (tmp_path / "top.txt").write_text("text")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "nested.pdf").write_text("%PDF")
    (sub / "nested.md").write_text("# doc")

    runner = SourceRunner()
    step = _Step(connector="local_files", path=str(tmp_path), pattern="*.pdf", recursive=True)
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert len(result["data"]) == 2
    for item in result["data"]:
        assert item["title"].endswith(".pdf")


async def test_local_files_normalized_item_fields(tmp_path):
    """Each item has all required NormalizedItem fields."""
    (tmp_path / "test.txt").write_text("content")

    runner = SourceRunner()
    step = _Step(connector="local_files", path=str(tmp_path))
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    item = result["data"][0]
    assert item["source"] == "local_files"
    assert item["source_type"] == "file_storage"
    assert item["item_id"] == str(tmp_path / "test.txt")
    assert item["title"] == "test.txt"
    assert item["timestamp"] is not None
    assert "size" in item["metadata"]
    assert "path" in item["metadata"]


async def test_local_files_nonexistent_path():
    """Non-existent path returns error."""
    runner = SourceRunner()
    step = _Step(connector="local_files", path="/nonexistent/path/does/not/exist")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "does not exist" in result["error"]


async def test_local_files_missing_path():
    """Missing 'path' config returns descriptive error."""
    runner = SourceRunner()
    step = _Step(connector="local_files")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "path" in result["error"]


# ---------------------------------------------------------------------------
# outlook — mock MCP call
# ---------------------------------------------------------------------------


async def test_outlook_basic_mock():
    """outlook connector with mocked MCP returns NormalizedItem dicts."""
    messages = [
        _make_outlook_message("id-1", "Invoice Q1", from_addr="billing@corp.com"),
        _make_outlook_message("id-2", "Meeting tomorrow"),
    ]

    async def mock_caller(args):
        return messages

    runner = SourceRunner()
    runner._mcp_caller = mock_caller
    step = _Step(connector="outlook", folder="INBOX", limit=10)
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    items = result["data"]
    assert len(items) == 2
    assert items[0]["source"] == "outlook"
    assert items[0]["source_type"] == "email"
    assert items[0]["item_id"] == "id-1"
    assert items[0]["title"] == "Invoice Q1"


async def test_outlook_normalized_item_fields():
    """Outlook item has correct metadata fields."""
    messages = [_make_outlook_message(
        msg_id="msg-42",
        subject="Hello",
        from_addr="alice@example.com",
        received="2026-02-01T09:30:00Z",
        has_attachments=True,
    )]

    async def mock_caller(args):
        return messages

    runner = SourceRunner()
    runner._mcp_caller = mock_caller
    step = _Step(connector="outlook")
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    item = result["data"][0]
    assert item["timestamp"] == "2026-02-01T09:30:00Z"
    assert item["metadata"]["from"] == "alice@example.com"
    assert item["metadata"]["hasAttachments"] is True
    assert len(item["attachments"]) == 1  # placeholder attachment hint


async def test_outlook_odata_filter_passed_to_mcp():
    """OData filter from config is passed through to MCP caller args."""
    captured_args = {}

    async def mock_caller(args):
        captured_args.update(args)
        return []

    runner = SourceRunner()
    runner._mcp_caller = mock_caller
    step = _Step(connector="outlook", filter="isRead eq false", limit=25)
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert captured_args.get("filter") == "isRead eq false"
    assert captured_args.get("top") == 25


async def test_outlook_mcp_returns_value_dict():
    """If MCP returns {'value': [...]} dict, messages are extracted correctly."""
    messages = [_make_outlook_message("id-x", "From OData")]

    async def mock_caller(args):
        return {"value": messages, "@odata.count": 1}

    runner = SourceRunner()
    runner._mcp_caller = mock_caller
    step = _Step(connector="outlook")
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert len(result["data"]) == 1
    assert result["data"][0]["title"] == "From OData"


async def test_outlook_empty_result():
    """Empty inbox returns success with empty list."""
    async def mock_caller(args):
        return []

    runner = SourceRunner()
    runner._mcp_caller = mock_caller
    step = _Step(connector="outlook")
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"] == []


# ---------------------------------------------------------------------------
# Unknown / not-yet-implemented connectors
# ---------------------------------------------------------------------------


async def test_unknown_connector_returns_error():
    """Completely unknown connector name returns a descriptive error."""
    runner = SourceRunner()
    step = _Step(connector="nonexistent_connector_xyz")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "nonexistent_connector_xyz" in result["error"]
    assert "Unknown connector" in result["error"]


async def test_gmail_not_implemented():
    """gmail connector raises NotImplementedError with clear message."""
    runner = SourceRunner()
    step = _Step(connector="gmail")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "gmail" in result["error"]
    assert "not yet implemented" in result["error"]


async def test_paypal_not_implemented():
    """paypal connector raises NotImplementedError with clear message."""
    runner = SourceRunner()
    step = _Step(connector="paypal")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "paypal" in result["error"]
    assert "not yet implemented" in result["error"]


async def test_sparkasse_not_implemented():
    """sparkasse connector raises NotImplementedError with clear message."""
    runner = SourceRunner()
    step = _Step(connector="sparkasse")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "sparkasse" in result["error"]
    assert "not yet implemented" in result["error"]


async def test_onedrive_not_implemented():
    """onedrive connector raises NotImplementedError with clear message."""
    runner = SourceRunner()
    step = _Step(connector="onedrive")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "onedrive" in result["error"]
    assert "not yet implemented" in result["error"]


# ---------------------------------------------------------------------------
# Missing connector field
# ---------------------------------------------------------------------------


async def test_missing_connector_field():
    """Step without 'connector' field returns a descriptive error."""
    runner = SourceRunner()
    step = _Step(path="/tmp")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert "connector" in result["error"]


# ---------------------------------------------------------------------------
# report_progress is called
# ---------------------------------------------------------------------------


async def test_report_progress_called_local_files(tmp_path):
    """execute() calls report_progress; final state has pct=100."""
    (tmp_path / "file.txt").write_text("data")

    runner = SourceRunner()
    step = _Step(connector="local_files", path=str(tmp_path))
    await runner.execute(step, context=None)

    assert runner._progress is not None
    assert runner._progress["pct"] == 100.0


async def test_report_progress_called_outlook():
    """execute() calls report_progress after outlook fetch; pct=100."""
    async def mock_caller(args):
        return [_make_outlook_message()]

    runner = SourceRunner()
    runner._mcp_caller = mock_caller
    step = _Step(connector="outlook")
    await runner.execute(step, context=None)

    assert runner._progress is not None
    assert runner._progress["pct"] == 100.0
