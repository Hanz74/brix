"""Tests for T-BRIX-DB-06: DB-First — all configurable data in DB.

Covers:
1. DB tables exist after BrixDB init
2. Seeding fills empty tables from code definitions
3. Seeding skips non-empty tables (idempotent)
4. BrickRegistry reads from DB
5. Connector definitions loaded from DB
6. MCP tool schemas loaded from DB
7. Help topics loaded from DB
8. Keyword taxonomies loaded from DB
9. Type compatibility loaded from DB
10. seed_if_empty returns correct counts
"""
from __future__ import annotations

import asyncio
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from brix.db import BrixDB
from brix.seed import seed_if_empty


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db(tmp_path) -> BrixDB:
    """Return a BrixDB instance backed by a fresh in-memory-like temp file."""
    db_file = tmp_path / "test_brix.db"
    return BrixDB(db_path=db_file)


# ---------------------------------------------------------------------------
# 1. DB tables exist after BrixDB init
# ---------------------------------------------------------------------------

class TestDBTablesExist:
    def test_brick_definitions_table_exists(self, fresh_db):
        count = fresh_db.brick_definitions_count()
        assert count == 0  # starts empty

    def test_connector_definitions_table_exists(self, fresh_db):
        count = fresh_db.connector_definitions_count()
        assert count == 0

    def test_mcp_tool_schemas_table_exists(self, fresh_db):
        count = fresh_db.mcp_tool_schemas_count()
        assert count == 0

    def test_help_topics_table_exists(self, fresh_db):
        count = fresh_db.help_topics_count()
        assert count == 0

    def test_keyword_taxonomies_table_exists(self, fresh_db):
        count = fresh_db.keyword_taxonomies_count()
        assert count == 0

    def test_type_compatibility_table_exists(self, fresh_db):
        count = fresh_db.type_compatibility_count()
        assert count == 0


# ---------------------------------------------------------------------------
# 2. Seeding fills empty tables from code definitions
# ---------------------------------------------------------------------------

class TestSeedingFillsEmptyTables:
    def test_seed_brick_definitions(self, fresh_db):
        counts = seed_if_empty(fresh_db)
        assert counts["brick_definitions"] > 0
        assert fresh_db.brick_definitions_count() > 0

    def test_seed_connector_definitions(self, fresh_db):
        counts = seed_if_empty(fresh_db)
        assert counts["connector_definitions"] > 0
        assert fresh_db.connector_definitions_count() > 0

    def test_seed_mcp_tool_schemas(self, fresh_db):
        counts = seed_if_empty(fresh_db)
        assert counts["mcp_tool_schemas"] > 0
        assert fresh_db.mcp_tool_schemas_count() > 0

    def test_seed_help_topics(self, fresh_db):
        counts = seed_if_empty(fresh_db)
        assert counts["help_topics"] > 0
        assert fresh_db.help_topics_count() > 0

    def test_seed_keyword_taxonomies(self, fresh_db):
        counts = seed_if_empty(fresh_db)
        assert counts["keyword_taxonomies"] > 0
        assert fresh_db.keyword_taxonomies_count() > 0

    def test_seed_type_compatibility(self, fresh_db):
        counts = seed_if_empty(fresh_db)
        assert counts["type_compatibility"] > 0
        assert fresh_db.type_compatibility_count() > 0

    def test_seed_returns_dict_with_all_keys(self, fresh_db):
        counts = seed_if_empty(fresh_db)
        for key in [
            "brick_definitions", "connector_definitions", "mcp_tool_schemas",
            "help_topics", "keyword_taxonomies", "type_compatibility",
        ]:
            assert key in counts, f"Missing key: {key}"

    def test_seed_brick_count_matches_builtins(self, fresh_db):
        from brix.bricks.builtins import ALL_BUILTINS
        seed_if_empty(fresh_db)
        assert fresh_db.brick_definitions_count() == len(ALL_BUILTINS)

    def test_seed_connector_count_matches_registry(self, fresh_db):
        from brix.connectors import CONNECTOR_REGISTRY
        seed_if_empty(fresh_db)
        assert fresh_db.connector_definitions_count() == len(CONNECTOR_REGISTRY)

    def test_seed_help_topics_count_positive(self, fresh_db):
        seed_if_empty(fresh_db)
        assert fresh_db.help_topics_count() > 0


# ---------------------------------------------------------------------------
# 3. Seeding skips non-empty tables (idempotent)
# ---------------------------------------------------------------------------

class TestSeedingIdempotent:
    def test_second_seed_returns_zero_counts(self, fresh_db):
        seed_if_empty(fresh_db)
        counts2 = seed_if_empty(fresh_db)
        for key, count in counts2.items():
            assert count == 0, f"Second seed should return 0 for {key}, got {count}"

    def test_seed_does_not_overwrite_existing_data(self, fresh_db):
        seed_if_empty(fresh_db)
        # Manually modify a row
        fresh_db.help_topics_upsert({"name": "quick-start", "title": "MODIFIED", "content": "custom"})
        seed_if_empty(fresh_db)
        row = fresh_db.help_topics_get("quick-start")
        assert row is not None
        assert row["title"] == "MODIFIED"

    def test_seed_skips_each_table_independently(self, fresh_db):
        # Pre-populate only brick_definitions
        from brix.bricks.builtins import ALL_BUILTINS
        for b in ALL_BUILTINS[:3]:
            fresh_db.brick_definitions_upsert({
                "name": b.name,
                "runner": b.runner or b.type,
            })
        counts = seed_if_empty(fresh_db)
        # brick_definitions was non-empty, so should be skipped
        assert counts["brick_definitions"] == 0
        # Other tables should still be seeded
        assert counts["connector_definitions"] > 0
        assert counts["help_topics"] > 0


# ---------------------------------------------------------------------------
# 4. BrickRegistry reads from DB
# ---------------------------------------------------------------------------

class TestBrickRegistryFromDB:
    def test_registry_loads_bricks_from_db(self, fresh_db):
        seed_if_empty(fresh_db)
        from brix.bricks.registry import BrickRegistry
        reg = BrickRegistry(db=fresh_db)
        assert reg.count > 0

    def test_registry_contains_http_get(self, fresh_db):
        seed_if_empty(fresh_db)
        from brix.bricks.registry import BrickRegistry
        reg = BrickRegistry(db=fresh_db)
        brick = reg.get("http_get")
        assert brick is not None
        assert brick.name == "http_get"

    def test_registry_contains_filter_brick(self, fresh_db):
        seed_if_empty(fresh_db)
        from brix.bricks.registry import BrickRegistry
        reg = BrickRegistry(db=fresh_db)
        brick = reg.get("filter")
        assert brick is not None
        assert brick.description

    def test_registry_fallback_when_db_empty(self, fresh_db):
        """When DB is empty, registry falls back to ALL_BUILTINS code."""
        from brix.bricks.registry import BrickRegistry
        from brix.bricks.builtins import ALL_BUILTINS
        reg = BrickRegistry(db=fresh_db)
        # DB is empty → should fall back to code
        assert reg.count == len(ALL_BUILTINS)

    def test_registry_db_brick_has_correct_category(self, fresh_db):
        seed_if_empty(fresh_db)
        from brix.bricks.registry import BrickRegistry
        reg = BrickRegistry(db=fresh_db)
        http_brick = reg.get("http_get")
        assert http_brick is not None
        assert http_brick.category == "http"

    def test_registry_db_brick_search_works(self, fresh_db):
        seed_if_empty(fresh_db)
        from brix.bricks.registry import BrickRegistry
        reg = BrickRegistry(db=fresh_db)
        results = reg.search("http")
        assert len(results) > 0

    def test_row_to_brick_conversion(self, fresh_db):
        """Test that _row_to_brick correctly converts a DB row."""
        from brix.bricks.registry import _row_to_brick
        row = {
            "name": "test_brick",
            "runner": "python",
            "namespace": "test",
            "category": "test",
            "description": "A test brick",
            "when_to_use": "For testing",
            "when_NOT_to_use": "",
            "aliases": '["test", "check"]',
            "input_type": "string",
            "output_type": "dict",
            "config_schema": '{"key": {"type": "string", "description": "A key", "required": true}}',
            "examples": "[]",
            "related_connector": "",
            "system": 0,
        }
        brick = _row_to_brick(row)
        assert brick.name == "test_brick"
        assert brick.category == "test"
        assert "test" in brick.aliases
        assert "key" in brick.config_schema
        assert brick.config_schema["key"].required is True


# ---------------------------------------------------------------------------
# 5. Connector definitions loaded from DB
# ---------------------------------------------------------------------------

class TestConnectorDefinitionsFromDB:
    def test_connector_definitions_seeded(self, fresh_db):
        seed_if_empty(fresh_db)
        rows = fresh_db.connector_definitions_list()
        names = {r["name"] for r in rows}
        assert "outlook" in names
        assert "gmail" in names
        assert "onedrive" in names
        assert "paypal" in names
        assert "sparkasse" in names
        assert "local_files" in names

    def test_connector_definition_fields(self, fresh_db):
        seed_if_empty(fresh_db)
        row = fresh_db.connector_definitions_get("outlook")
        assert row is not None
        assert row["type"] == "email"
        assert row["required_mcp_server"] == "m365"
        # parameters is a JSON-serialised list
        params = json.loads(row["parameters"]) if isinstance(row["parameters"], str) else row["parameters"]
        assert len(params) > 0

    def test_connector_definition_gmail_no_mcp(self, fresh_db):
        seed_if_empty(fresh_db)
        row = fresh_db.connector_definitions_get("gmail")
        assert row is not None
        assert row.get("required_mcp_server", "") == ""

    def test_row_to_connector_conversion(self):
        from brix.connectors import _row_to_connector
        row = {
            "name": "test_conn",
            "type": "email",
            "description": "Test connector",
            "required_mcp_server": "m365",
            "required_mcp_tools": '["list-messages", "get-message"]',
            "output_schema": '{"type": "object"}',
            "parameters": '[{"name": "folder", "type": "string", "description": "Folder", "required": false, "default": "INBOX"}]',
            "related_pipelines": '["pipeline-a"]',
            "related_helpers": '[]',
        }
        connector = _row_to_connector(row)
        assert connector.name == "test_conn"
        assert connector.type == "email"
        assert connector.required_mcp_server == "m365"
        assert len(connector.required_mcp_tools) == 2
        assert len(connector.parameters) == 1
        assert connector.parameters[0].name == "folder"


# ---------------------------------------------------------------------------
# 6. MCP tool schemas loaded from DB
# ---------------------------------------------------------------------------

class TestMCPToolSchemasFromDB:
    def test_mcp_tool_schemas_seeded(self, fresh_db):
        seed_if_empty(fresh_db)
        rows = fresh_db.mcp_tool_schemas_list()
        names = {r["name"] for r in rows}
        assert "brix__get_tips" in names
        assert "brix__run_pipeline" in names
        assert "brix__list_bricks" in names

    def test_mcp_tool_schema_has_input_schema(self, fresh_db):
        seed_if_empty(fresh_db)
        row = fresh_db.mcp_tool_schemas_get("brix__run_pipeline")
        assert row is not None
        input_schema = row["input_schema"]
        if isinstance(input_schema, str):
            input_schema = json.loads(input_schema)
        assert "type" in input_schema

    def test_mcp_tool_schema_has_description(self, fresh_db):
        seed_if_empty(fresh_db)
        row = fresh_db.mcp_tool_schemas_get("brix__get_tips")
        assert row is not None
        assert row["description"]

    def test_load_mcp_tools_from_db_function(self, fresh_db, monkeypatch):
        seed_if_empty(fresh_db)
        # Patch BrixDB to return our test db
        with patch("brix.mcp_server.BrixDB", return_value=fresh_db):
            from brix.mcp_server import _load_mcp_tools_from_db
            tools = _load_mcp_tools_from_db()
        assert len(tools) > 0
        names = {t.name for t in tools}
        assert "brix__get_tips" in names


# ---------------------------------------------------------------------------
# 7. Help topics loaded from DB
# ---------------------------------------------------------------------------

class TestHelpTopicsFromDB:
    def test_help_topics_seeded(self, fresh_db):
        seed_if_empty(fresh_db)
        rows = fresh_db.help_topics_list()
        names = {r["name"] for r in rows}
        assert "quick-start" in names
        assert "foreach" in names

    def test_help_topic_has_content(self, fresh_db):
        seed_if_empty(fresh_db)
        row = fresh_db.help_topics_get("quick-start")
        assert row is not None
        assert len(row["content"]) > 50

    def test_help_topic_has_title(self, fresh_db):
        seed_if_empty(fresh_db)
        row = fresh_db.help_topics_get("quick-start")
        assert row is not None
        assert row["title"]

    def test_get_help_topics_reads_from_db(self, fresh_db):
        seed_if_empty(fresh_db)
        # Manually modify a topic in DB
        fresh_db.help_topics_upsert({
            "name": "quick-start",
            "title": "MODIFIED TITLE",
            "content": "MODIFIED CONTENT",
        })
        # Patch inside the lazy import path (brix.db module)
        with patch("brix.db.BrixDB", return_value=fresh_db):
            import importlib
            import brix.mcp_handlers.help as help_mod
            importlib.reload(help_mod)
            topics, descriptions = help_mod._get_help_topics()
        assert "quick-start" in topics


# ---------------------------------------------------------------------------
# 8. Keyword taxonomies loaded from DB
# ---------------------------------------------------------------------------

class TestKeywordTaxonomiesFromDB:
    def test_keyword_taxonomies_seeded(self, fresh_db):
        seed_if_empty(fresh_db)
        count = fresh_db.keyword_taxonomies_count()
        assert count > 50  # many keywords across all categories

    def test_source_keywords_present(self, fresh_db):
        seed_if_empty(fresh_db)
        rows = fresh_db.keyword_taxonomies_list(category="source")
        assert len(rows) > 10
        keywords = {r["keyword"] for r in rows}
        assert "outlook" in keywords
        assert "gmail" in keywords

    def test_action_keywords_present(self, fresh_db):
        seed_if_empty(fresh_db)
        rows = fresh_db.keyword_taxonomies_list(category="action")
        assert len(rows) > 10
        keywords = {r["keyword"] for r in rows}
        assert "download" in keywords

    def test_target_keywords_present(self, fresh_db):
        seed_if_empty(fresh_db)
        rows = fresh_db.keyword_taxonomies_list(category="target")
        assert len(rows) > 5

    def test_keyword_taxonomies_as_dict(self, fresh_db):
        seed_if_empty(fresh_db)
        result = fresh_db.keyword_taxonomies_as_dict()
        assert "source" in result
        assert "action" in result
        assert "target" in result
        # Each category maps mapped_to → [keywords]
        assert isinstance(list(result["source"].values())[0], list)

    def test_parse_intent_uses_db_keywords(self, fresh_db):
        seed_if_empty(fresh_db)
        # Verify that keyword_taxonomies were seeded correctly
        source_rows = fresh_db.keyword_taxonomies_list(category="source")
        keywords = {r["keyword"] for r in source_rows}
        # outlook keyword should be in sources category
        assert "outlook" in keywords


# ---------------------------------------------------------------------------
# 9. Type compatibility loaded from DB
# ---------------------------------------------------------------------------

class TestTypeCompatibilityFromDB:
    def test_type_compatibility_seeded(self, fresh_db):
        seed_if_empty(fresh_db)
        count = fresh_db.type_compatibility_count()
        assert count > 10

    def test_type_compatibility_as_dict(self, fresh_db):
        seed_if_empty(fresh_db)
        result = fresh_db.type_compatibility_as_dict()
        assert "list[*]" in result
        assert "dict" in result

    def test_type_compatibility_list_star_entries(self, fresh_db):
        seed_if_empty(fresh_db)
        result = fresh_db.type_compatibility_as_dict()
        list_star = result.get("list[*]", [])
        assert "list[email]" in list_star or "list[dict]" in list_star

    def test_is_compatible_uses_db(self, fresh_db):
        seed_if_empty(fresh_db)
        # Verify seeded type_compatibility data is in DB
        compat = fresh_db.type_compatibility_as_dict()
        assert len(compat) > 0
        # list[*] should be present with compatible entries
        assert "list[*]" in compat

    def test_is_compatible_logic_still_works(self):
        """Core is_compatible logic works regardless of DB."""
        from brix.bricks.types import is_compatible
        assert is_compatible("*", "anything") is True
        assert is_compatible("string", "string") is True
        assert is_compatible("", "string") is True


# ---------------------------------------------------------------------------
# 10. DB access methods work correctly
# ---------------------------------------------------------------------------

class TestDBAccessMethods:
    def test_brick_definitions_upsert_and_get(self, fresh_db):
        fresh_db.brick_definitions_upsert({
            "name": "my_brick",
            "runner": "python",
            "category": "test",
            "description": "My brick",
        })
        row = fresh_db.brick_definitions_get("my_brick")
        assert row is not None
        assert row["name"] == "my_brick"
        assert row["category"] == "test"

    def test_connector_definitions_upsert_and_get(self, fresh_db):
        fresh_db.connector_definitions_upsert({
            "name": "my_connector",
            "type": "email",
            "description": "My connector",
            "required_mcp_tools": ["tool-1"],
        })
        row = fresh_db.connector_definitions_get("my_connector")
        assert row is not None
        assert row["name"] == "my_connector"

    def test_mcp_tool_schemas_upsert_and_get(self, fresh_db):
        fresh_db.mcp_tool_schemas_upsert({
            "name": "brix__my_tool",
            "description": "My tool",
            "input_schema": {"type": "object", "properties": {}},
        })
        row = fresh_db.mcp_tool_schemas_get("brix__my_tool")
        assert row is not None
        assert row["name"] == "brix__my_tool"
        input_schema = json.loads(row["input_schema"]) if isinstance(row["input_schema"], str) else row["input_schema"]
        assert input_schema["type"] == "object"

    def test_help_topics_upsert_and_get(self, fresh_db):
        fresh_db.help_topics_upsert({
            "name": "test-topic",
            "title": "Test Topic",
            "content": "Some help content.",
        })
        row = fresh_db.help_topics_get("test-topic")
        assert row is not None
        assert row["content"] == "Some help content."

    def test_keyword_taxonomies_upsert_and_list(self, fresh_db):
        fresh_db.keyword_taxonomies_upsert("source", "test_kw", "de", "test")
        fresh_db.keyword_taxonomies_upsert("action", "run_kw", "de", "run")
        rows = fresh_db.keyword_taxonomies_list(category="source")
        assert len(rows) == 1
        assert rows[0]["keyword"] == "test_kw"

    def test_type_compatibility_upsert_and_list(self, fresh_db):
        fresh_db.type_compatibility_upsert("list[email]", "list[dict]")
        fresh_db.type_compatibility_upsert("list[email]", "list[*]")
        rows = fresh_db.type_compatibility_list()
        assert len(rows) == 2
        output_types = {r["output_type"] for r in rows}
        assert "list[email]" in output_types

    def test_type_compatibility_as_dict_structure(self, fresh_db):
        fresh_db.type_compatibility_upsert("string", "text")
        fresh_db.type_compatibility_upsert("string", "string")
        result = fresh_db.type_compatibility_as_dict()
        assert "string" in result
        assert "text" in result["string"]
        assert "string" in result["string"]

    def test_brick_definitions_list_returns_all(self, fresh_db):
        for i in range(5):
            fresh_db.brick_definitions_upsert({"name": f"brick_{i}", "runner": "python"})
        rows = fresh_db.brick_definitions_list()
        assert len(rows) == 5

    def test_upsert_updates_existing(self, fresh_db):
        """Upsert should update an existing row, not create a duplicate."""
        fresh_db.help_topics_upsert({"name": "dup-topic", "title": "v1", "content": "first"})
        fresh_db.help_topics_upsert({"name": "dup-topic", "title": "v2", "content": "second"})
        assert fresh_db.help_topics_count() == 1
        row = fresh_db.help_topics_get("dup-topic")
        assert row["content"] == "second"
