"""Tests for Registry System (T-BRIX-V7-10).

Covers:
- 6 registry tables created in brix.db
- BrixDB.registry_add / get / list / update / delete / search
- Invalid registry_type raises ValueError
- Duplicate name raises IntegrityError
- Tag filtering in registry_list
- Cross-registry search via registry_search
- MCP handler functions: registry_add, get, list, update, delete, search
- get_help topics: registries, error-patterns
- REGISTRY_TYPES constant exported from db
"""

import json
import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Return a BrixDB backed by a temporary file."""
    from brix.db import BrixDB
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ---------------------------------------------------------------------------
# Schema Tests
# ---------------------------------------------------------------------------

class TestRegistrySchema:
    def test_all_registry_tables_exist(self, db):
        """All 6 registry tables are created on init."""
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "registry_templates" in tables
        assert "registry_patterns" in tables
        assert "registry_schemas" in tables
        assert "registry_error_patterns" in tables
        assert "registry_best_practices" in tables
        assert "registry_lessons_learned" in tables

    def test_registry_table_columns(self, db):
        """Each registry table has the expected columns."""
        with db._connect() as conn:
            cols = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(registry_templates)"
                ).fetchall()
            }
        assert {"id", "name", "description", "content", "tags", "created_at", "updated_at"} <= cols

    def test_registry_types_constant(self):
        """REGISTRY_TYPES maps 6 type names to table names."""
        from brix.db import REGISTRY_TYPES
        assert len(REGISTRY_TYPES) == 6
        assert "templates" in REGISTRY_TYPES
        assert "patterns" in REGISTRY_TYPES
        assert "schemas" in REGISTRY_TYPES
        assert "error_patterns" in REGISTRY_TYPES
        assert "best_practices" in REGISTRY_TYPES
        assert "lessons_learned" in REGISTRY_TYPES

    def test_registry_types_values_are_table_names(self):
        """REGISTRY_TYPES values are valid table names."""
        from brix.db import REGISTRY_TYPES
        for rtype, table in REGISTRY_TYPES.items():
            assert table.startswith("registry_"), f"{rtype} → {table} should start with registry_"


# ---------------------------------------------------------------------------
# registry_add
# ---------------------------------------------------------------------------

class TestRegistryAdd:
    def test_add_returns_uuid(self, db):
        """registry_add returns a UUID string."""
        entry_id = db.registry_add(
            "templates",
            "my-template",
            content={"steps": ["fetch", "process"]},
        )
        assert isinstance(entry_id, str)
        assert len(entry_id) > 8

    def test_add_all_registry_types(self, db):
        """Can add entries to all 6 registry types."""
        from brix.db import REGISTRY_TYPES
        for rtype in REGISTRY_TYPES:
            entry_id = db.registry_add(
                rtype,
                f"test-{rtype}",
                content={"key": "value"},
                tags=["test"],
                description=f"Test {rtype}",
            )
            assert entry_id

    def test_add_invalid_type_raises(self, db):
        """registry_add raises ValueError for unknown registry_type."""
        with pytest.raises(ValueError, match="Unknown registry_type"):
            db.registry_add("nonexistent", "name", content={})

    def test_add_duplicate_name_raises(self, db):
        """registry_add raises IntegrityError on duplicate name."""
        db.registry_add("templates", "dup-name", content={"steps": []})
        with pytest.raises(sqlite3.IntegrityError):
            db.registry_add("templates", "dup-name", content={"steps": ["other"]})

    def test_add_stores_dict_content(self, db):
        """Content dict is serialized and can be retrieved."""
        content = {"steps": ["fetch", "process"], "tags": ["m365"]}
        entry_id = db.registry_add("patterns", "my-pattern", content=content)
        entry = db.registry_get("patterns", entry_id)
        assert entry["content"] == content

    def test_add_stores_tags(self, db):
        """Tags list is stored correctly."""
        db.registry_add("schemas", "my-schema", content={}, tags=["json", "v2"])
        entry = db.registry_get("schemas", "my-schema")
        assert entry["tags"] == ["json", "v2"]

    def test_add_stores_description(self, db):
        """Description is stored correctly."""
        db.registry_add("best_practices", "my-bp", content={}, description="My rule")
        entry = db.registry_get("best_practices", "my-bp")
        assert entry["description"] == "My rule"

    def test_add_default_tags_empty(self, db):
        """Default tags is an empty list."""
        db.registry_add("lessons_learned", "my-lesson", content={"problem": "x"})
        entry = db.registry_get("lessons_learned", "my-lesson")
        assert entry["tags"] == []

    def test_add_timestamps_set(self, db):
        """created_at and updated_at are set on add."""
        entry_id = db.registry_add("error_patterns", "my-error", content={})
        entry = db.registry_get("error_patterns", entry_id)
        assert entry["created_at"]
        assert entry["updated_at"]

    def test_add_string_content(self, db):
        """String content is stored as-is."""
        db.registry_add("templates", "str-template", content="raw string content")
        entry = db.registry_get("templates", "str-template")
        assert entry["content"] == "raw string content"


# ---------------------------------------------------------------------------
# registry_get
# ---------------------------------------------------------------------------

class TestRegistryGet:
    def test_get_by_name(self, db):
        """registry_get retrieves entry by name."""
        db.registry_add("templates", "fetch-pattern", content={"steps": ["fetch"]})
        entry = db.registry_get("templates", "fetch-pattern")
        assert entry is not None
        assert entry["name"] == "fetch-pattern"

    def test_get_by_id(self, db):
        """registry_get retrieves entry by UUID."""
        entry_id = db.registry_add("patterns", "my-pat", content={})
        entry = db.registry_get("patterns", entry_id)
        assert entry is not None
        assert entry["id"] == entry_id

    def test_get_not_found_returns_none(self, db):
        """registry_get returns None for non-existent name."""
        result = db.registry_get("templates", "nonexistent")
        assert result is None

    def test_get_invalid_type_raises(self, db):
        """registry_get raises ValueError for unknown type."""
        with pytest.raises(ValueError):
            db.registry_get("invalid_type", "anything")

    def test_get_returns_all_fields(self, db):
        """registry_get returns all fields including timestamps."""
        db.registry_add("schemas", "my-schema", content={"version": 1}, description="test")
        entry = db.registry_get("schemas", "my-schema")
        assert "id" in entry
        assert "name" in entry
        assert "description" in entry
        assert "content" in entry
        assert "tags" in entry
        assert "created_at" in entry
        assert "updated_at" in entry


# ---------------------------------------------------------------------------
# registry_list
# ---------------------------------------------------------------------------

class TestRegistryList:
    def test_list_empty(self, db):
        """registry_list returns empty list when no entries."""
        result = db.registry_list("templates")
        assert result == []

    def test_list_returns_all(self, db):
        """registry_list returns all entries sorted by name."""
        db.registry_add("templates", "b-template", content={})
        db.registry_add("templates", "a-template", content={})
        result = db.registry_list("templates")
        assert len(result) == 2
        assert result[0]["name"] == "a-template"
        assert result[1]["name"] == "b-template"

    def test_list_with_tag_filter(self, db):
        """registry_list filters by tag correctly."""
        db.registry_add("best_practices", "bp1", content={}, tags=["performance", "foreach"])
        db.registry_add("best_practices", "bp2", content={}, tags=["security"])
        db.registry_add("best_practices", "bp3", content={}, tags=["performance"])
        result = db.registry_list("best_practices", tag_filter="performance")
        names = [e["name"] for e in result]
        assert "bp1" in names
        assert "bp3" in names
        assert "bp2" not in names

    def test_list_tag_filter_no_match(self, db):
        """registry_list returns empty list when tag_filter matches nothing."""
        db.registry_add("patterns", "p1", content={}, tags=["other"])
        result = db.registry_list("patterns", tag_filter="nonexistent-tag")
        assert result == []

    def test_list_invalid_type_raises(self, db):
        """registry_list raises ValueError for unknown type."""
        with pytest.raises(ValueError):
            db.registry_list("bad_type")

    def test_list_different_types_isolated(self, db):
        """Entries in different registries don't leak into each other."""
        db.registry_add("templates", "t1", content={})
        db.registry_add("patterns", "p1", content={})
        assert len(db.registry_list("templates")) == 1
        assert len(db.registry_list("patterns")) == 1


# ---------------------------------------------------------------------------
# registry_update
# ---------------------------------------------------------------------------

class TestRegistryUpdate:
    def test_update_content(self, db):
        """registry_update replaces content."""
        db.registry_add("templates", "upd-template", content={"old": True})
        updated = db.registry_update("templates", "upd-template", content={"new": True})
        assert updated["content"] == {"new": True}

    def test_update_tags(self, db):
        """registry_update replaces tags."""
        db.registry_add("patterns", "upd-pattern", content={}, tags=["old"])
        updated = db.registry_update("patterns", "upd-pattern", tags=["new", "tags"])
        assert updated["tags"] == ["new", "tags"]

    def test_update_description(self, db):
        """registry_update replaces description."""
        db.registry_add("schemas", "upd-schema", content={}, description="old")
        updated = db.registry_update("schemas", "upd-schema", description="new description")
        assert updated["description"] == "new description"

    def test_update_partial_preserves_other_fields(self, db):
        """registry_update only changes provided fields."""
        db.registry_add(
            "best_practices", "upd-bp",
            content={"rule": "original"},
            tags=["original-tag"],
            description="original desc",
        )
        # Only update description
        updated = db.registry_update("best_practices", "upd-bp", description="new desc")
        assert updated["content"] == {"rule": "original"}
        assert updated["tags"] == ["original-tag"]
        assert updated["description"] == "new desc"

    def test_update_by_id(self, db):
        """registry_update works with UUID lookup."""
        entry_id = db.registry_add("error_patterns", "ep1", content={"old": True})
        updated = db.registry_update("error_patterns", entry_id, content={"new": True})
        assert updated is not None
        assert updated["content"] == {"new": True}

    def test_update_not_found_returns_none(self, db):
        """registry_update returns None for non-existent entry."""
        result = db.registry_update("templates", "nonexistent", content={})
        assert result is None

    def test_update_sets_updated_at(self, db):
        """registry_update changes updated_at timestamp."""
        import time
        entry_id = db.registry_add("lessons_learned", "ll1", content={})
        entry_before = db.registry_get("lessons_learned", entry_id)
        time.sleep(0.01)
        db.registry_update("lessons_learned", entry_id, description="changed")
        entry_after = db.registry_get("lessons_learned", entry_id)
        assert entry_after["updated_at"] >= entry_before["updated_at"]


# ---------------------------------------------------------------------------
# registry_delete
# ---------------------------------------------------------------------------

class TestRegistryDelete:
    def test_delete_by_name(self, db):
        """registry_delete removes entry by name."""
        db.registry_add("templates", "del-template", content={})
        result = db.registry_delete("templates", "del-template")
        assert result is True
        assert db.registry_get("templates", "del-template") is None

    def test_delete_by_id(self, db):
        """registry_delete removes entry by UUID."""
        entry_id = db.registry_add("patterns", "del-pattern", content={})
        result = db.registry_delete("patterns", entry_id)
        assert result is True
        assert db.registry_get("patterns", entry_id) is None

    def test_delete_not_found_returns_false(self, db):
        """registry_delete returns False when entry not found."""
        result = db.registry_delete("templates", "nonexistent")
        assert result is False

    def test_delete_invalid_type_raises(self, db):
        """registry_delete raises ValueError for unknown type."""
        with pytest.raises(ValueError):
            db.registry_delete("bad_type", "name")


# ---------------------------------------------------------------------------
# registry_search
# ---------------------------------------------------------------------------

class TestRegistrySearch:
    def test_search_by_name(self, db):
        """registry_search finds entries matching name."""
        db.registry_add("templates", "m365-mail-processor", content={})
        db.registry_add("patterns", "http-fetch", content={})
        results = db.registry_search("m365")
        names = [r["name"] for r in results]
        assert "m365-mail-processor" in names
        assert "http-fetch" not in names

    def test_search_by_description(self, db):
        """registry_search finds entries matching description."""
        db.registry_add("best_practices", "bp1", content={}, description="foreach concurrency optimization")
        db.registry_add("best_practices", "bp2", content={}, description="error handling")
        results = db.registry_search("concurrency")
        names = [r["name"] for r in results]
        assert "bp1" in names
        assert "bp2" not in names

    def test_search_by_tag(self, db):
        """registry_search finds entries matching tag."""
        db.registry_add("error_patterns", "ep1", content={}, tags=["m365", "odata"])
        db.registry_add("error_patterns", "ep2", content={}, tags=["python"])
        results = db.registry_search("odata")
        names = [r["name"] for r in results]
        assert "ep1" in names
        assert "ep2" not in names

    def test_search_by_content(self, db):
        """registry_search finds entries matching content."""
        db.registry_add("schemas", "invoice-schema", content={"fields": ["amount", "iban"]})
        db.registry_add("schemas", "contact-schema", content={"fields": ["name", "email"]})
        results = db.registry_search("iban")
        names = [r["name"] for r in results]
        assert "invoice-schema" in names
        assert "contact-schema" not in names

    def test_search_across_all_types(self, db):
        """registry_search spans all 6 registries by default."""
        from brix.db import REGISTRY_TYPES
        for rtype in REGISTRY_TYPES:
            db.registry_add(rtype, f"target-{rtype}", content={}, description="find-me")
        results = db.registry_search("find-me")
        assert len(results) == 6

    def test_search_with_registry_type_filter(self, db):
        """registry_search respects registry_types filter."""
        db.registry_add("templates", "target-template", content={}, description="hello")
        db.registry_add("patterns", "target-pattern", content={}, description="hello")
        results = db.registry_search("hello", registry_types=["templates"])
        assert len(results) == 1
        assert results[0]["registry_type"] == "templates"

    def test_search_results_have_registry_type_field(self, db):
        """Each search result has a 'registry_type' field."""
        db.registry_add("templates", "my-template", content={"x": 1})
        results = db.registry_search("my-template")
        assert results[0]["registry_type"] == "templates"

    def test_search_no_results(self, db):
        """registry_search returns empty list when nothing matches."""
        results = db.registry_search("zzz-no-match-xyz")
        assert results == []

    def test_search_invalid_type_in_filter_skipped(self, db):
        """registry_search skips invalid types in registry_types filter gracefully."""
        db.registry_add("templates", "t1", content={}, description="hello")
        results = db.registry_search("hello", registry_types=["templates", "bad_type"])
        names = [r["name"] for r in results]
        assert "t1" in names

    def test_search_case_insensitive(self, db):
        """registry_search is case-insensitive."""
        db.registry_add("templates", "CaseTest", content={}, description="UPPERCASE content")
        results = db.registry_search("uppercase")
        assert len(results) == 1


# ---------------------------------------------------------------------------
# MCP Handler Tests
# ---------------------------------------------------------------------------

class TestMCPHandlers:
    """Tests for the async MCP handler functions."""

    @pytest.fixture
    def mock_db(self, tmp_path, monkeypatch):
        """Monkeypatch BrixDB in mcp_server to use a temp database."""
        from brix.db import BrixDB
        tmp_db = BrixDB(db_path=tmp_path / "test.db")

        import brix.mcp_server as srv
        monkeypatch.setattr(srv, "BrixDB", lambda: tmp_db)
        return tmp_db

    @pytest.mark.anyio
    async def test_registry_add_success(self, mock_db):
        """registry_add handler returns success with id."""
        from brix.mcp_server import _handle_registry_add
        result = await _handle_registry_add({
            "registry_type": "templates",
            "name": "test-template",
            "content": {"steps": ["fetch"]},
            "tags": ["test"],
        })
        assert result["success"] is True
        assert "id" in result
        assert result["name"] == "test-template"
        assert result["registry_type"] == "templates"

    @pytest.mark.anyio
    async def test_registry_add_missing_registry_type(self):
        """registry_add handler returns error when registry_type missing."""
        from brix.mcp_server import _handle_registry_add
        result = await _handle_registry_add({
            "name": "test",
            "content": {},
        })
        assert result["success"] is False
        assert "registry_type" in result["error"]

    @pytest.mark.anyio
    async def test_registry_add_missing_name(self):
        """registry_add handler returns error when name missing."""
        from brix.mcp_server import _handle_registry_add
        result = await _handle_registry_add({
            "registry_type": "templates",
            "content": {},
        })
        assert result["success"] is False
        assert "name" in result["error"]

    @pytest.mark.anyio
    async def test_registry_add_missing_content(self):
        """registry_add handler returns error when content missing."""
        from brix.mcp_server import _handle_registry_add
        result = await _handle_registry_add({
            "registry_type": "templates",
            "name": "test",
        })
        assert result["success"] is False
        assert "content" in result["error"]

    @pytest.mark.anyio
    async def test_registry_add_invalid_type(self, mock_db):
        """registry_add handler returns error for invalid registry_type."""
        from brix.mcp_server import _handle_registry_add
        result = await _handle_registry_add({
            "registry_type": "invalid_type",
            "name": "test",
            "content": {},
        })
        assert result["success"] is False

    @pytest.mark.anyio
    async def test_registry_add_duplicate_name(self, mock_db):
        """registry_add handler returns error on duplicate name."""
        from brix.mcp_server import _handle_registry_add
        await _handle_registry_add({
            "registry_type": "templates",
            "name": "dup-name",
            "content": {},
        })
        result = await _handle_registry_add({
            "registry_type": "templates",
            "name": "dup-name",
            "content": {},
        })
        assert result["success"] is False
        assert "already exists" in result["error"]

    @pytest.mark.anyio
    async def test_registry_get_success(self, mock_db):
        """registry_get handler returns entry."""
        from brix.mcp_server import _handle_registry_add, _handle_registry_get
        await _handle_registry_add({
            "registry_type": "patterns",
            "name": "my-pattern",
            "content": {"key": "val"},
        })
        result = await _handle_registry_get({
            "registry_type": "patterns",
            "name_or_id": "my-pattern",
        })
        assert result["success"] is True
        assert result["entry"]["name"] == "my-pattern"
        assert result["entry"]["content"] == {"key": "val"}

    @pytest.mark.anyio
    async def test_registry_get_not_found(self, mock_db):
        """registry_get handler returns error when not found."""
        from brix.mcp_server import _handle_registry_get
        result = await _handle_registry_get({
            "registry_type": "templates",
            "name_or_id": "nonexistent",
        })
        assert result["success"] is False
        assert "not found" in result["error"]

    @pytest.mark.anyio
    async def test_registry_list_success(self, mock_db):
        """registry_list handler returns entries list."""
        from brix.mcp_server import _handle_registry_add, _handle_registry_list
        await _handle_registry_add({"registry_type": "schemas", "name": "s1", "content": {}})
        await _handle_registry_add({"registry_type": "schemas", "name": "s2", "content": {}})
        result = await _handle_registry_list({"registry_type": "schemas"})
        assert result["success"] is True
        assert result["count"] == 2
        assert len(result["entries"]) == 2

    @pytest.mark.anyio
    async def test_registry_list_with_tag_filter(self, mock_db):
        """registry_list handler filters by tag."""
        from brix.mcp_server import _handle_registry_add, _handle_registry_list
        await _handle_registry_add({"registry_type": "best_practices", "name": "bp1", "content": {}, "tags": ["perf"]})
        await _handle_registry_add({"registry_type": "best_practices", "name": "bp2", "content": {}, "tags": ["security"]})
        result = await _handle_registry_list({"registry_type": "best_practices", "tag_filter": "perf"})
        assert result["success"] is True
        assert result["count"] == 1
        assert result["entries"][0]["name"] == "bp1"

    @pytest.mark.anyio
    async def test_registry_update_success(self, mock_db):
        """registry_update handler updates entry."""
        from brix.mcp_server import _handle_registry_add, _handle_registry_update
        await _handle_registry_add({"registry_type": "templates", "name": "upd-t", "content": {"old": True}})
        result = await _handle_registry_update({
            "registry_type": "templates",
            "name_or_id": "upd-t",
            "content": {"new": True},
        })
        assert result["success"] is True
        assert result["entry"]["content"] == {"new": True}

    @pytest.mark.anyio
    async def test_registry_update_not_found(self, mock_db):
        """registry_update handler returns error when entry not found."""
        from brix.mcp_server import _handle_registry_update
        result = await _handle_registry_update({
            "registry_type": "templates",
            "name_or_id": "nonexistent",
            "content": {},
        })
        assert result["success"] is False

    @pytest.mark.anyio
    async def test_registry_update_no_fields_provided(self, mock_db):
        """registry_update handler returns error when no fields provided."""
        from brix.mcp_server import _handle_registry_update
        result = await _handle_registry_update({
            "registry_type": "templates",
            "name_or_id": "anything",
        })
        assert result["success"] is False
        assert "At least one" in result["error"]

    @pytest.mark.anyio
    async def test_registry_delete_success(self, mock_db):
        """registry_delete handler deletes entry."""
        from brix.mcp_server import _handle_registry_add, _handle_registry_delete
        await _handle_registry_add({"registry_type": "templates", "name": "del-t", "content": {}})
        result = await _handle_registry_delete({"registry_type": "templates", "name_or_id": "del-t"})
        assert result["success"] is True
        assert result["deleted"] == "del-t"

    @pytest.mark.anyio
    async def test_registry_delete_not_found(self, mock_db):
        """registry_delete handler returns error when not found."""
        from brix.mcp_server import _handle_registry_delete
        result = await _handle_registry_delete({"registry_type": "templates", "name_or_id": "nonexistent"})
        assert result["success"] is False

    @pytest.mark.anyio
    async def test_registry_search_success(self, mock_db):
        """registry_search handler returns matching results."""
        from brix.mcp_server import _handle_registry_add, _handle_registry_search
        await _handle_registry_add({"registry_type": "templates", "name": "find-me-template", "content": {}})
        await _handle_registry_add({"registry_type": "patterns", "name": "find-me-pattern", "content": {}})
        result = await _handle_registry_search({"query": "find-me"})
        assert result["success"] is True
        assert result["count"] == 2
        assert result["query"] == "find-me"

    @pytest.mark.anyio
    async def test_registry_search_with_type_filter(self, mock_db):
        """registry_search handler respects registry_types filter."""
        from brix.mcp_server import _handle_registry_add, _handle_registry_search
        await _handle_registry_add({"registry_type": "templates", "name": "find-me-template", "content": {}})
        await _handle_registry_add({"registry_type": "patterns", "name": "find-me-pattern", "content": {}})
        result = await _handle_registry_search({
            "query": "find-me",
            "registry_types": ["templates"],
        })
        assert result["success"] is True
        assert result["count"] == 1

    @pytest.mark.anyio
    async def test_registry_search_missing_query(self):
        """registry_search handler returns error when query missing."""
        from brix.mcp_server import _handle_registry_search
        result = await _handle_registry_search({})
        assert result["success"] is False
        assert "query" in result["error"]


# ---------------------------------------------------------------------------
# get_help Topics
# ---------------------------------------------------------------------------

class TestGetHelpTopics:
    @pytest.mark.anyio
    async def test_registries_topic_exists(self):
        """get_help returns content for 'registries' topic."""
        from brix.mcp_server import _handle_get_help
        result = await _handle_get_help({"topic": "registries"})
        assert "error" not in result
        assert result["topic"] == "registries"
        assert "templates" in result["content"]
        assert "patterns" in result["content"]
        assert "schemas" in result["content"]
        assert "error_patterns" in result["content"]
        assert "best_practices" in result["content"]
        assert "lessons_learned" in result["content"]

    @pytest.mark.anyio
    async def test_error_patterns_topic_exists(self):
        """get_help returns content for 'error-patterns' topic."""
        from brix.mcp_server import _handle_get_help
        result = await _handle_get_help({"topic": "error-patterns"})
        assert "error" not in result
        assert result["topic"] == "error-patterns"
        assert "error_regex" in result["content"]
        assert "root_cause" in result["content"]
        assert "fix" in result["content"]

    @pytest.mark.anyio
    async def test_registries_in_topic_list(self):
        """Both new topics appear in the topic directory."""
        from brix.mcp_server import _handle_get_help
        result = await _handle_get_help({})
        assert "registries" in result["topics"]
        assert "error-patterns" in result["topics"]

    @pytest.mark.anyio
    async def test_registries_description_present(self):
        """New topics have descriptions in the directory."""
        from brix.mcp_server import _handle_get_help
        result = await _handle_get_help({})
        assert "registries" in result["descriptions"]
        assert "error-patterns" in result["descriptions"]
        assert result["descriptions"]["registries"]
        assert result["descriptions"]["error-patterns"]


# ---------------------------------------------------------------------------
# Tool Definitions (BRIX_TOOLS)
# ---------------------------------------------------------------------------

class TestToolDefinitions:
    def test_registry_tools_in_brix_tools(self):
        """All 6 registry MCP tools are in BRIX_TOOLS."""
        from brix.mcp_server import BRIX_TOOLS
        tool_names = {t.name for t in BRIX_TOOLS}
        assert "brix__registry_add" in tool_names
        assert "brix__registry_get" in tool_names
        assert "brix__registry_list" in tool_names
        assert "brix__registry_update" in tool_names
        assert "brix__registry_delete" in tool_names
        assert "brix__registry_search" in tool_names

    def test_registry_tools_have_source_param(self):
        """All registry tools have the 'source' parameter injected."""
        from brix.mcp_server import BRIX_TOOLS
        for tool in BRIX_TOOLS:
            if tool.name.startswith("brix__registry_"):
                assert "source" in tool.inputSchema.get("properties", {}), \
                    f"{tool.name} missing 'source' param"

    def test_registry_tools_in_handlers(self):
        """Consolidated registry tool is registered in the dispatch table."""
        from brix.mcp_server import _HANDLERS
        assert "brix__registry" in _HANDLERS
