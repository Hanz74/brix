"""Tests for pipeline helper scripts."""
import json
import subprocess
import pytest

HELPERS_DIR = "helpers"


def _run_helper(script, params=None):
    cmd = ["python3", f"{HELPERS_DIR}/{script}"]
    if params:
        cmd.append(json.dumps(params))
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result


def test_extract_attachment_urls_basic():
    messages = [
        {
            "subject": "Test Mail",
            "receivedDateTime": "2026-03-15T10:00:00Z",
            "attachments": [
                {"name": "file.pdf", "contentUrl": "https://example.com/file.pdf", "size": 100}
            ]
        }
    ]
    result = _run_helper("extract_attachment_urls.py", {"messages": messages})
    assert result.returncode == 0
    output = json.loads(result.stdout)
    assert len(output) == 1
    assert output[0]["filename"] == "file.pdf"
    assert output[0]["mail_subject"] == "Test Mail"


def test_extract_attachment_urls_empty():
    result = _run_helper("extract_attachment_urls.py", {"messages": []})
    assert result.returncode == 0
    output = json.loads(result.stdout)
    assert len(output) == 0


def test_extract_attachment_urls_multiple():
    messages = [
        {"subject": "A", "receivedDateTime": "2026-03-15", "attachments": [
            {"name": "a1.pdf", "contentUrl": "url1", "size": 10},
            {"name": "a2.doc", "contentUrl": "url2", "size": 20},
        ]},
        {"subject": "B", "receivedDateTime": "2026-03-16", "attachments": [
            {"name": "b1.xlsx", "contentUrl": "url3", "size": 30},
        ]},
    ]
    result = _run_helper("extract_attachment_urls.py", {"messages": messages})
    output = json.loads(result.stdout)
    assert len(output) == 3


def test_structured_save(tmp_path):
    params = {
        "content": "test content",
        "metadata": {"mail_date": "2026-03-15", "mail_subject": "Invoice", "filename": "test.txt"},
        "output_dir": str(tmp_path),
    }
    result = _run_helper("structured_save.py", params)
    assert result.returncode == 0
    output = json.loads(result.stdout)
    assert "path" in output
    assert "2026-03-15" in output["filename"]
    assert "Invoice" in output["filename"]


def test_summary_report():
    params = {
        "files": {
            "items": [
                {"success": True, "data": {"path": "/tmp/a.pdf", "filename": "a.pdf", "size": 100}},
                {"success": True, "data": {"path": "/tmp/b.doc", "filename": "b.doc", "size": 200}},
                {"success": False, "error": "404"},
            ],
            "summary": {"total": 3, "succeeded": 2, "failed": 1}
        },
        "converted": {},
        "input_query": "test query",
    }
    result = _run_helper("summary_report.py", params)
    assert result.returncode == 0
    output = json.loads(result.stdout)
    assert output["total_files"] == 2
    assert output["failed_downloads"] == 1
    assert output["query"] == "test query"
    assert len(output["files"]) == 2


def test_summary_report_empty():
    result = _run_helper("summary_report.py", {})
    assert result.returncode == 0
    output = json.loads(result.stdout)
    assert output["total_files"] == 0


def test_filter_mails_by_keywords():
    mails = [
        {"subject": "Rechnung März 2026", "bodyPreview": "Anbei Ihre Rechnung", "id": "1", "hasAttachments": True},
        {"subject": "Meeting Einladung", "bodyPreview": "Bitte kommen Sie", "id": "2", "hasAttachments": True},
        {"subject": "Invoice #123", "bodyPreview": "Please find attached", "id": "3", "hasAttachments": True},
    ]
    result = _run_helper("filter_mails_by_keywords.py", {"mails": mails, "keywords": "Rechnung,Invoice"})
    assert result.returncode == 0
    output = json.loads(result.stdout)
    assert len(output) == 2  # Rechnung + Invoice, not Meeting


def test_filter_mails_no_keywords():
    mails = [{"subject": "A", "id": "1"}, {"subject": "B", "id": "2"}]
    result = _run_helper("filter_mails_by_keywords.py", {"mails": mails, "keywords": ""})
    assert result.returncode == 0
    output = json.loads(result.stdout)
    assert len(output) == 2  # All pass through


def test_filter_mails_case_insensitive():
    mails = [{"subject": "RECHNUNG", "bodyPreview": "", "id": "1"}]
    result = _run_helper("filter_mails_by_keywords.py", {"mails": mails, "keywords": "rechnung"})
    assert result.returncode == 0
    output = json.loads(result.stdout)
    assert len(output) == 1


def test_save_attachment_duplicate(tmp_path):
    """Second save with same name gets _1 suffix."""
    import base64
    content_b64 = base64.b64encode(b"hello").decode()
    params = {
        "attachment": {
            "mail_date": "2026-01-01",
            "mail_subject": "Test",
            "name": "dup.txt",
            "content_bytes": content_b64,
        },
        "output_dir": str(tmp_path),
    }
    # First save
    result1 = _run_helper("save_attachment.py", params)
    assert result1.returncode == 0
    out1 = json.loads(result1.stdout)
    assert out1["filename"] == "2026-01-01_Test_dup.txt"

    # Second save — same params, file already exists
    result2 = _run_helper("save_attachment.py", params)
    assert result2.returncode == 0
    out2 = json.loads(result2.stdout)
    assert out2["filename"] == "2026-01-01_Test_dup_1.txt"
    assert out2["filename"] != out1["filename"]

    # Third save — both previous files exist
    result3 = _run_helper("save_attachment.py", params)
    assert result3.returncode == 0
    out3 = json.loads(result3.stdout)
    assert out3["filename"] == "2026-01-01_Test_dup_2.txt"
