"""Tests for Flow/Filter/Extract bricks (T-BRIX-BRICK-03).

Tests:
- flow.dedup (DedupRunner) — content-hash and key-based dedup
- flow.flatten (FlattenRunner) — nested list flattening
- filter.keyword (KeywordFilterRunner) — keyword matching
- extract.url (ExtractUrlRunner) — URL extraction
- extract.ics (ExtractIcsRunner) — ICS/iCal parsing
"""

import asyncio
import hashlib
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from brix.runners.dedup import DedupRunner
from brix.runners.flatten import FlattenRunner
from brix.runners.keyword_filter import KeywordFilterRunner
from brix.runners.extract_url import ExtractUrlRunner
from brix.runners.extract_ics import ExtractIcsRunner, parse_ics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_step(**kwargs):
    """Create a minimal step-like object for runner tests."""
    defaults = {"timeout": None, "params": {}}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ===========================================================================
# DedupRunner
# ===========================================================================

class TestDedupRunner:

    def test_config_schema(self):
        runner = DedupRunner()
        schema = runner.config_schema()
        assert "key" in schema["properties"]
        assert "field" in schema["properties"]
        assert "algorithm" in schema["properties"]

    def test_content_hash_dedup_sha256(self):
        runner = DedupRunner()
        items = [
            {"id": 1, "body": "hello world"},
            {"id": 2, "body": "foo bar"},
            {"id": 3, "body": "hello world"},  # duplicate
            {"id": 4, "body": "baz"},
        ]
        step = _make_step(params={"input": items, "field": "body", "algorithm": "sha256"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert len(data["items"]) == 3
        assert data["removed"] == 1
        assert data["total"] == 4
        # Items 1, 2, 4 should remain (first occurrence kept)
        assert [i["id"] for i in data["items"]] == [1, 2, 4]

    def test_content_hash_dedup_md5(self):
        runner = DedupRunner()
        items = [
            {"id": 1, "body": "same"},
            {"id": 2, "body": "same"},
            {"id": 3, "body": "different"},
        ]
        step = _make_step(params={"input": items, "field": "body", "algorithm": "md5"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert len(data["items"]) == 2
        assert data["removed"] == 1

    def test_key_expr_dedup(self):
        runner = DedupRunner()
        items = [
            {"email": "a@b.com", "name": "Alice"},
            {"email": "c@d.com", "name": "Bob"},
            {"email": "a@b.com", "name": "Alice2"},
        ]
        step = _make_step(params={"input": items, "key": "{{ item.email }}"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert len(data["items"]) == 2
        assert data["items"][0]["name"] == "Alice"  # first kept

    def test_keep_last(self):
        runner = DedupRunner()
        items = [
            {"id": 1, "body": "same"},
            {"id": 2, "body": "other"},
            {"id": 3, "body": "same"},
        ]
        step = _make_step(params={"input": items, "field": "body", "keep": "last"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert len(data["items"]) == 2
        # keep=last: the last "same" occurrence (id=3) should be kept
        ids = [i["id"] for i in data["items"]]
        assert 3 in ids
        assert 1 not in ids

    def test_missing_input(self):
        runner = DedupRunner()
        step = _make_step(params={"field": "body"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_missing_key_and_field(self):
        runner = DedupRunner()
        step = _make_step(params={"input": [{"a": 1}]})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_invalid_algorithm(self):
        runner = DedupRunner()
        step = _make_step(params={"input": [{"a": 1}], "field": "a", "algorithm": "crc32"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_empty_list(self):
        runner = DedupRunner()
        step = _make_step(params={"input": [], "field": "body"})
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["items"] == []
        assert result["data"]["removed"] == 0

    def test_validate_config(self):
        runner = DedupRunner()
        errors = runner.validate_config({"input": []})
        assert any("key" in e or "field" in e for e in errors)

        errors = runner.validate_config({"input": [], "field": "x", "algorithm": "crc32"})
        assert any("algorithm" in e for e in errors)


# ===========================================================================
# FlattenRunner
# ===========================================================================

class TestFlattenRunner:

    def test_config_schema(self):
        runner = FlattenRunner()
        schema = runner.config_schema()
        assert "depth" in schema["properties"]
        assert "field" in schema["properties"]

    def test_flatten_nested(self):
        runner = FlattenRunner()
        step = _make_step(params={"input": [[1, 2], [3, 4], [5]]})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["items"] == [1, 2, 3, 4, 5]
        assert data["count"] == 5

    def test_flatten_deeply_nested(self):
        runner = FlattenRunner()
        step = _make_step(params={"input": [[[1, 2]], [[3]]], "depth": -1})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["items"] == [1, 2, 3]

    def test_flatten_field(self):
        runner = FlattenRunner()
        items = [
            {"name": "a", "tags": ["x", "y"]},
            {"name": "b", "tags": ["z"]},
            {"name": "c"},  # no tags field
        ]
        step = _make_step(params={"input": items, "field": "tags"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["items"] == ["x", "y", "z"]
        assert result["data"]["count"] == 3

    def test_flatten_depth_zero(self):
        runner = FlattenRunner()
        nested = [[1, 2], [3]]
        step = _make_step(params={"input": nested, "depth": 0})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        # depth=0 means no flattening
        assert result["data"]["items"] == [[1, 2], [3]]

    def test_missing_input(self):
        runner = FlattenRunner()
        step = _make_step(params={})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_empty_list(self):
        runner = FlattenRunner()
        step = _make_step(params={"input": []})
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["items"] == []
        assert result["data"]["count"] == 0


# ===========================================================================
# KeywordFilterRunner
# ===========================================================================

class TestKeywordFilterRunner:

    def test_config_schema(self):
        runner = KeywordFilterRunner()
        schema = runner.config_schema()
        assert "fields" in schema["properties"]
        assert "keywords" in schema["properties"]
        assert "mode" in schema["properties"]

    def test_any_mode(self):
        runner = KeywordFilterRunner()
        items = [
            {"subject": "Invoice for January", "body": "Please pay"},
            {"subject": "Meeting notes", "body": "Team standup"},
            {"subject": "Payment received", "body": "Thank you"},
        ]
        step = _make_step(params={
            "input": items,
            "fields": ["subject", "body"],
            "keywords": ["invoice", "payment"],
            "mode": "any",
        })
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["count"] == 2
        assert data["filtered_out"] == 1
        subjects = [i["subject"] for i in data["items"]]
        assert "Meeting notes" not in subjects

    def test_all_mode(self):
        runner = KeywordFilterRunner()
        items = [
            {"subject": "Invoice payment due", "body": "Details"},
            {"subject": "Invoice for January", "body": "Other"},
            {"subject": "Payment received", "body": "Done"},
        ]
        step = _make_step(params={
            "input": items,
            "fields": ["subject"],
            "keywords": ["invoice", "payment"],
            "mode": "all",
        })
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["count"] == 1
        assert data["items"][0]["subject"] == "Invoice payment due"

    def test_case_insensitive(self):
        runner = KeywordFilterRunner()
        items = [{"text": "Hello WORLD"}]
        step = _make_step(params={
            "input": items,
            "fields": ["text"],
            "keywords": ["hello"],
            "case_sensitive": False,
        })
        result = _run(runner.execute(step, None))
        assert result["data"]["count"] == 1

    def test_case_sensitive(self):
        runner = KeywordFilterRunner()
        items = [{"text": "Hello WORLD"}]
        step = _make_step(params={
            "input": items,
            "fields": ["text"],
            "keywords": ["hello"],
            "case_sensitive": True,
        })
        result = _run(runner.execute(step, None))
        assert result["data"]["count"] == 0

    def test_missing_fields(self):
        runner = KeywordFilterRunner()
        step = _make_step(params={"input": [{}], "keywords": ["x"]})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_missing_keywords(self):
        runner = KeywordFilterRunner()
        step = _make_step(params={"input": [{}], "fields": ["x"]})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_empty_list(self):
        runner = KeywordFilterRunner()
        step = _make_step(params={
            "input": [],
            "fields": ["text"],
            "keywords": ["foo"],
        })
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["count"] == 0
        assert result["data"]["filtered_out"] == 0

    def test_validate_config(self):
        runner = KeywordFilterRunner()
        errors = runner.validate_config({"fields": "not-a-list", "keywords": ["x"]})
        assert any("fields" in e for e in errors)

        errors = runner.validate_config({"fields": ["x"], "keywords": "not-a-list"})
        assert any("keywords" in e for e in errors)

        errors = runner.validate_config({"fields": ["x"], "keywords": ["y"], "mode": "bad"})
        assert any("mode" in e for e in errors)


# ===========================================================================
# ExtractUrlRunner
# ===========================================================================

class TestExtractUrlRunner:

    def test_config_schema(self):
        runner = ExtractUrlRunner()
        schema = runner.config_schema()
        assert "field" in schema["properties"]
        assert "pattern" in schema["properties"]

    def test_extract_from_list(self):
        runner = ExtractUrlRunner()
        items = [
            {"body": "Check https://example.com and http://foo.bar/path?q=1"},
            {"body": "No urls here"},
            {"body": "Visit https://test.org/page"},
        ]
        step = _make_step(params={"input": items, "field": "body"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["count"] == 3
        assert "https://example.com" in data["urls"]
        assert "https://test.org/page" in data["urls"]

    def test_extract_from_single_dict(self):
        runner = ExtractUrlRunner()
        item = {"content": "Link: https://example.com/test"}
        step = _make_step(params={"input": item, "field": "content"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 1
        assert "https://example.com/test" in result["data"]["urls"]

    def test_deduplicates_urls(self):
        runner = ExtractUrlRunner()
        items = [
            {"body": "https://example.com"},
            {"body": "https://example.com again"},
        ]
        step = _make_step(params={"input": items, "field": "body"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 1

    def test_custom_pattern(self):
        runner = ExtractUrlRunner()
        items = [{"text": "Link: https://example.com/path and https://other.com/x"}]
        step = _make_step(params={
            "input": items,
            "field": "text",
            "pattern": r"https://example\.com/\w+",
        })
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 1
        assert result["data"]["urls"][0] == "https://example.com/path"

    def test_missing_field(self):
        runner = ExtractUrlRunner()
        step = _make_step(params={"input": [{}]})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_no_urls(self):
        runner = ExtractUrlRunner()
        items = [{"body": "no links here at all"}]
        step = _make_step(params={"input": items, "field": "body"})
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["count"] == 0
        assert result["data"]["urls"] == []

    def test_invalid_pattern(self):
        runner = ExtractUrlRunner()
        step = _make_step(params={"input": [{}], "field": "x", "pattern": "[invalid"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_validate_config(self):
        runner = ExtractUrlRunner()
        errors = runner.validate_config({"field": 123})
        assert any("field" in e for e in errors)

        errors = runner.validate_config({"field": "x", "pattern": "[invalid"})
        assert any("pattern" in e for e in errors)


# ===========================================================================
# ExtractIcsRunner
# ===========================================================================

_SAMPLE_ICS = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Test//Test//EN
BEGIN:VEVENT
DTSTART:20240115T100000Z
DTEND:20240115T110000Z
SUMMARY:Team Standup
LOCATION:Room A
DESCRIPTION:Daily standup meeting
UID:uid-001@example.com
STATUS:CONFIRMED
ORGANIZER:mailto:boss@example.com
ATTENDEE:mailto:alice@example.com
ATTENDEE:mailto:bob@example.com
END:VEVENT
BEGIN:VEVENT
DTSTART;TZID=Europe/Berlin:20240116T140000
DTEND;TZID=Europe/Berlin:20240116T150000
SUMMARY:Project Review
UID:uid-002@example.com
STATUS:TENTATIVE
END:VEVENT
BEGIN:VEVENT
DTSTART:20240117T090000Z
DTEND:20240117T093000Z
SUMMARY:Cancelled Meeting
UID:uid-003@example.com
STATUS:CANCELLED
END:VEVENT
END:VCALENDAR
"""

_ICS_WITH_FOLDING = """\
BEGIN:VCALENDAR
BEGIN:VEVENT
DTSTART:20240115T100000Z
DTEND:20240115T110000Z
SUMMARY:This is a very long summary that gets
 folded across multiple lines in the ICS file
DESCRIPTION:Short
UID:uid-fold@example.com
END:VEVENT
END:VCALENDAR
"""


class TestExtractIcsRunner:

    def test_config_schema(self):
        runner = ExtractIcsRunner()
        schema = runner.config_schema()
        assert "path" in schema["properties"]
        assert "event_types" in schema["properties"]

    def test_parse_from_content(self):
        runner = ExtractIcsRunner()
        step = _make_step(params={"content": _SAMPLE_ICS})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["count"] == 3
        events = data["events"]

        # First event
        ev1 = events[0]
        assert ev1["summary"] == "Team Standup"
        assert ev1["start"] == "20240115T100000Z"
        assert ev1["end"] == "20240115T110000Z"
        assert ev1["location"] == "Room A"
        assert ev1["status"] == "CONFIRMED"
        assert len(ev1["attendees"]) == 2

    def test_parse_from_file(self, tmp_path):
        runner = ExtractIcsRunner()
        ics_file = tmp_path / "test.ics"
        ics_file.write_text(_SAMPLE_ICS, encoding="utf-8")

        step = _make_step(params={"path": str(ics_file)})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 3

    def test_event_type_filter(self):
        runner = ExtractIcsRunner()
        step = _make_step(params={
            "content": _SAMPLE_ICS,
            "event_types": ["CONFIRMED"],
        })
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 1
        assert result["data"]["events"][0]["summary"] == "Team Standup"

    def test_line_folding(self):
        runner = ExtractIcsRunner()
        step = _make_step(params={"content": _ICS_WITH_FOLDING})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 1
        summary = result["data"]["events"][0]["summary"]
        assert "folded across" in summary
        assert "\n" not in summary  # Should be unfolded

    def test_file_not_found(self):
        runner = ExtractIcsRunner()
        step = _make_step(params={"path": "/nonexistent/file.ics"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_missing_path_and_content(self):
        runner = ExtractIcsRunner()
        step = _make_step(params={})
        result = _run(runner.execute(step, None))
        assert result["success"] is False

    def test_empty_ics(self):
        runner = ExtractIcsRunner()
        step = _make_step(params={"content": "BEGIN:VCALENDAR\nEND:VCALENDAR"})
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["count"] == 0
        assert result["data"]["events"] == []

    def test_validate_config(self):
        runner = ExtractIcsRunner()
        errors = runner.validate_config({})
        assert any("path" in e or "content" in e for e in errors)

        errors = runner.validate_config({"path": "/foo.ics", "event_types": "not-a-list"})
        assert any("event_types" in e for e in errors)

    def test_tzid_params_in_dtstart(self):
        """DTSTART with TZID parameter should still be parsed."""
        runner = ExtractIcsRunner()
        step = _make_step(params={"content": _SAMPLE_ICS})
        result = _run(runner.execute(step, None))

        # Second event has TZID param
        ev2 = result["data"]["events"][1]
        assert ev2["summary"] == "Project Review"
        assert ev2["start"] == "20240116T140000"


# ===========================================================================
# parse_ics standalone function
# ===========================================================================

class TestParseIcsFunction:

    def test_basic_parsing(self):
        events = parse_ics(_SAMPLE_ICS)
        assert len(events) == 3

    def test_with_filter(self):
        events = parse_ics(_SAMPLE_ICS, event_types=["TENTATIVE"])
        assert len(events) == 1
        assert events[0]["summary"] == "Project Review"

    def test_escaped_characters(self):
        ics = """\
BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Meeting\\, with comma
DESCRIPTION:Line 1\\nLine 2
UID:esc@example.com
END:VEVENT
END:VCALENDAR
"""
        events = parse_ics(ics)
        assert len(events) == 1
        assert events[0]["summary"] == "Meeting, with comma"
        assert "Line 1\nLine 2" in events[0]["description"]
