"""Tests for T-BRIX-DBF-04: MCP CRUD tools for Tool-Schemas, Help-Topics, Keywords, Type-Compatibility."""
from __future__ import annotations

import asyncio
import pytest

from brix.mcp_handlers.dbfirst_crud import (
    _handle_tool_schema,
    _handle_help_topic,
    _handle_keyword,
    _handle_type_compat,
)


def _run(coro):
    """Helper to run async handlers in sync tests."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ------------------------------------------------------------------
# brix__tool_schema
# ------------------------------------------------------------------

class TestToolSchema:
    def test_add_and_get(self):
        result = _run(_handle_tool_schema({"action": "add", "name": "test_tool", "description": "A test tool", "input_schema": {"type": "object"}}))
        assert result["success"] is True
        assert result["tool_schema"]["name"] == "test_tool"

        result = _run(_handle_tool_schema({"action": "get", "name": "test_tool"}))
        assert result["success"] is True
        assert result["tool_schema"]["description"] == "A test tool"

    def test_list(self):
        _run(_handle_tool_schema({"action": "add", "name": "ts_list_1", "description": "one"}))
        _run(_handle_tool_schema({"action": "add", "name": "ts_list_2", "description": "two"}))
        result = _run(_handle_tool_schema({"action": "list"}))
        assert result["success"] is True
        assert result["count"] >= 2
        names = [s["name"] for s in result["tool_schemas"]]
        assert "ts_list_1" in names
        assert "ts_list_2" in names

    def test_update(self):
        _run(_handle_tool_schema({"action": "add", "name": "ts_upd", "description": "old"}))
        result = _run(_handle_tool_schema({"action": "update", "name": "ts_upd", "description": "new"}))
        assert result["success"] is True
        assert result["tool_schema"]["description"] == "new"

    def test_delete(self):
        _run(_handle_tool_schema({"action": "add", "name": "ts_del", "description": "to delete"}))
        result = _run(_handle_tool_schema({"action": "delete", "name": "ts_del"}))
        assert result["success"] is True
        result = _run(_handle_tool_schema({"action": "get", "name": "ts_del"}))
        assert result["success"] is False

    def test_get_not_found(self):
        result = _run(_handle_tool_schema({"action": "get", "name": "nonexistent_ts"}))
        assert result["success"] is False

    def test_delete_not_found(self):
        result = _run(_handle_tool_schema({"action": "delete", "name": "nonexistent_ts"}))
        assert result["success"] is False

    def test_add_missing_name(self):
        result = _run(_handle_tool_schema({"action": "add"}))
        assert result["success"] is False

    def test_unknown_action(self):
        result = _run(_handle_tool_schema({"action": "bogus"}))
        assert result["success"] is False
        assert "Unknown action" in result["error"]

    def test_input_schema_as_string(self):
        result = _run(_handle_tool_schema({"action": "add", "name": "ts_str_schema", "input_schema": '{"type": "string"}'}))
        assert result["success"] is True

    def test_update_not_found(self):
        result = _run(_handle_tool_schema({"action": "update", "name": "nonexistent_ts_upd", "description": "x"}))
        assert result["success"] is False


# ------------------------------------------------------------------
# brix__help_topic
# ------------------------------------------------------------------

class TestHelpTopic:
    def test_add_and_get(self):
        result = _run(_handle_help_topic({"action": "add", "name": "ht_test", "title": "Test Topic", "content": "Some content", "category": "general"}))
        assert result["success"] is True
        assert result["help_topic"]["name"] == "ht_test"

        result = _run(_handle_help_topic({"action": "get", "name": "ht_test"}))
        assert result["success"] is True
        assert result["help_topic"]["title"] == "Test Topic"

    def test_list(self):
        _run(_handle_help_topic({"action": "add", "name": "ht_list_a", "title": "A", "category": "cat1"}))
        _run(_handle_help_topic({"action": "add", "name": "ht_list_b", "title": "B", "category": "cat2"}))
        result = _run(_handle_help_topic({"action": "list"}))
        assert result["success"] is True
        assert result["count"] >= 2

    def test_list_with_category_filter(self):
        _run(_handle_help_topic({"action": "add", "name": "ht_cat_x", "title": "X", "category": "special_cat"}))
        _run(_handle_help_topic({"action": "add", "name": "ht_cat_y", "title": "Y", "category": "other_cat"}))
        result = _run(_handle_help_topic({"action": "list", "category": "special_cat"}))
        assert result["success"] is True
        names = [t["name"] for t in result["help_topics"]]
        assert "ht_cat_x" in names
        assert "ht_cat_y" not in names

    def test_update(self):
        _run(_handle_help_topic({"action": "add", "name": "ht_upd", "title": "Old", "content": "old content"}))
        result = _run(_handle_help_topic({"action": "update", "name": "ht_upd", "title": "New"}))
        assert result["success"] is True
        assert result["help_topic"]["title"] == "New"
        # content should be preserved
        assert result["help_topic"]["content"] == "old content"

    def test_delete(self):
        _run(_handle_help_topic({"action": "add", "name": "ht_del", "title": "Delete me"}))
        result = _run(_handle_help_topic({"action": "delete", "name": "ht_del"}))
        assert result["success"] is True
        result = _run(_handle_help_topic({"action": "get", "name": "ht_del"}))
        assert result["success"] is False

    def test_get_not_found(self):
        result = _run(_handle_help_topic({"action": "get", "name": "nonexistent_ht"}))
        assert result["success"] is False

    def test_add_missing_name(self):
        result = _run(_handle_help_topic({"action": "add"}))
        assert result["success"] is False

    def test_unknown_action(self):
        result = _run(_handle_help_topic({"action": "bogus"}))
        assert result["success"] is False


# ------------------------------------------------------------------
# brix__keyword
# ------------------------------------------------------------------

class TestKeyword:
    def test_add_and_list(self):
        result = _run(_handle_keyword({"action": "add", "keyword": "rechnung", "category": "document_type"}))
        assert result["success"] is True
        assert result["keyword"] == "rechnung"

        result = _run(_handle_keyword({"action": "list", "category": "document_type"}))
        assert result["success"] is True
        keywords = [k["keyword"] for k in result["keywords"]]
        assert "rechnung" in keywords

    def test_list_all(self):
        _run(_handle_keyword({"action": "add", "keyword": "kw_all_a", "category": "cat_a"}))
        _run(_handle_keyword({"action": "add", "keyword": "kw_all_b", "category": "cat_b"}))
        result = _run(_handle_keyword({"action": "list"}))
        assert result["success"] is True
        assert result["count"] >= 2

    def test_delete(self):
        _run(_handle_keyword({"action": "add", "keyword": "kw_del", "category": "del_cat"}))
        result = _run(_handle_keyword({"action": "delete", "keyword": "kw_del", "category": "del_cat"}))
        assert result["success"] is True

        result = _run(_handle_keyword({"action": "delete", "keyword": "kw_del", "category": "del_cat"}))
        assert result["success"] is False

    def test_add_missing_keyword(self):
        result = _run(_handle_keyword({"action": "add", "category": "x"}))
        assert result["success"] is False

    def test_add_missing_category(self):
        result = _run(_handle_keyword({"action": "add", "keyword": "x"}))
        assert result["success"] is False

    def test_delete_missing_params(self):
        result = _run(_handle_keyword({"action": "delete", "keyword": "x"}))
        assert result["success"] is False

    def test_unknown_action(self):
        result = _run(_handle_keyword({"action": "bogus"}))
        assert result["success"] is False

    def test_add_with_mapped_to(self):
        result = _run(_handle_keyword({"action": "add", "keyword": "invoice", "category": "doc", "mapped_to": "rechnung", "language": "en"}))
        assert result["success"] is True
        assert result["mapped_to"] == "rechnung"
        assert result["language"] == "en"


# ------------------------------------------------------------------
# brix__type_compat
# ------------------------------------------------------------------

class TestTypeCompat:
    def test_add_and_list(self):
        result = _run(_handle_type_compat({"action": "add", "source_type": "json", "target_type": "dict"}))
        assert result["success"] is True

        result = _run(_handle_type_compat({"action": "list", "source_type": "json"}))
        assert result["success"] is True
        targets = [e["compatible_input"] for e in result["type_compatibilities"]]
        assert "dict" in targets

    def test_list_all(self):
        _run(_handle_type_compat({"action": "add", "source_type": "tc_a", "target_type": "tc_b"}))
        result = _run(_handle_type_compat({"action": "list"}))
        assert result["success"] is True
        assert result["count"] >= 1

    def test_delete(self):
        _run(_handle_type_compat({"action": "add", "source_type": "tc_del_s", "target_type": "tc_del_t"}))
        result = _run(_handle_type_compat({"action": "delete", "source_type": "tc_del_s", "target_type": "tc_del_t"}))
        assert result["success"] is True

        result = _run(_handle_type_compat({"action": "delete", "source_type": "tc_del_s", "target_type": "tc_del_t"}))
        assert result["success"] is False

    def test_add_missing_source(self):
        result = _run(_handle_type_compat({"action": "add", "target_type": "x"}))
        assert result["success"] is False

    def test_add_missing_target(self):
        result = _run(_handle_type_compat({"action": "add", "source_type": "x"}))
        assert result["success"] is False

    def test_unknown_action(self):
        result = _run(_handle_type_compat({"action": "bogus"}))
        assert result["success"] is False
