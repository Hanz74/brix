"""Tests for BrickRegistry (T-BRIX-V2-02)."""
import pytest
from pathlib import Path

from brix.bricks.registry import BrickRegistry
from brix.bricks.schema import BrickParam, BrickSchema
from brix.cache import SchemaCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_custom_brick(name: str = "custom_brick", category: str = "custom") -> BrickSchema:
    return BrickSchema(
        name=name,
        type="python",
        description="A custom test brick for REST API calls",
        when_to_use="Use when you need to send email or test something custom.",
        category=category,
    )


def _make_schema_cache_with_tools(tmp_path: Path, server_name: str, tools: list[dict]) -> SchemaCache:
    """Create a SchemaCache populated with mock tools."""
    cache = SchemaCache(cache_dir=tmp_path / "cache")
    cache.save_tools(server_name, tools)
    return cache


# ---------------------------------------------------------------------------
# Built-in loading
# ---------------------------------------------------------------------------

def test_registry_has_builtins():
    """Registry has exactly 10 built-in bricks after creation."""
    reg = BrickRegistry()
    assert reg.count == 10
    assert reg.builtin_count == 10


def test_registry_get_builtin():
    """get('http_get') returns the correct BrickSchema."""
    reg = BrickRegistry()
    brick = reg.get("http_get")
    assert brick is not None
    assert isinstance(brick, BrickSchema)
    assert brick.name == "http_get"
    assert brick.type == "http"


def test_registry_get_nonexistent():
    """get('nonexistent') returns None."""
    reg = BrickRegistry()
    assert reg.get("nonexistent") is None


# ---------------------------------------------------------------------------
# Register / Unregister
# ---------------------------------------------------------------------------

def test_registry_register_custom():
    """A custom brick can be registered and retrieved."""
    reg = BrickRegistry()
    custom = _make_custom_brick("my_custom")
    reg.register(custom)
    assert reg.count == 11
    retrieved = reg.get("my_custom")
    assert retrieved is not None
    assert retrieved.name == "my_custom"


def test_registry_unregister():
    """A brick can be removed from the registry."""
    reg = BrickRegistry()
    custom = _make_custom_brick("to_remove")
    reg.register(custom)
    assert reg.get("to_remove") is not None

    reg.unregister("to_remove")
    assert reg.get("to_remove") is None
    assert reg.count == 10


def test_registry_unregister_nonexistent_noop():
    """Unregistering a non-existent brick does not raise."""
    reg = BrickRegistry()
    reg.unregister("does_not_exist")  # must not raise
    assert reg.count == 10


# ---------------------------------------------------------------------------
# list_all / list_by_category
# ---------------------------------------------------------------------------

def test_registry_list_all():
    """list_all() returns all registered bricks."""
    reg = BrickRegistry()
    all_bricks = reg.list_all()
    assert len(all_bricks) == 10
    assert all(isinstance(b, BrickSchema) for b in all_bricks)


def test_registry_list_all_includes_custom():
    """list_all() includes custom bricks."""
    reg = BrickRegistry()
    reg.register(_make_custom_brick("extra"))
    assert len(reg.list_all()) == 11


def test_registry_list_by_category():
    """list_by_category('http') returns only HTTP bricks."""
    reg = BrickRegistry()
    http_bricks = reg.list_by_category("http")
    assert len(http_bricks) == 2
    assert all(b.category == "http" for b in http_bricks)
    names = {b.name for b in http_bricks}
    assert names == {"http_get", "http_post"}


def test_registry_list_by_category_empty():
    """list_by_category for non-existent category returns empty list."""
    reg = BrickRegistry()
    result = reg.list_by_category("nonexistent_category")
    assert result == []


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

def test_registry_search_by_name():
    """Search 'http' matches bricks with http in their name."""
    reg = BrickRegistry()
    results = reg.search("http")
    names = {b.name for b in results}
    assert "http_get" in names
    assert "http_post" in names


def test_registry_search_by_description():
    """Search 'REST API' matches bricks mentioning REST API in description."""
    reg = BrickRegistry()
    results = reg.search("REST API")
    assert len(results) >= 1
    assert any("http" in b.type for b in results)


def test_registry_search_by_when_to_use():
    """Search 'email' matches bricks with 'email' in when_to_use."""
    reg = BrickRegistry()
    # Register a brick with 'email' in when_to_use
    custom = _make_custom_brick("email_brick")
    reg.register(custom)
    results = reg.search("email")
    names = {b.name for b in results}
    assert "email_brick" in names


def test_registry_search_case_insensitive():
    """Search is case-insensitive."""
    reg = BrickRegistry()
    results_lower = reg.search("http")
    results_upper = reg.search("HTTP")
    assert {b.name for b in results_lower} == {b.name for b in results_upper}


def test_registry_search_with_category():
    """Search with category filter returns only matching category."""
    reg = BrickRegistry()
    # 'run' appears in when_to_use of various bricks; filter to 'cli' only
    results = reg.search("command", category="cli")
    assert all(b.category == "cli" for b in results)
    assert any(b.name == "run_cli" for b in results)


def test_registry_search_no_results():
    """Search with a term that matches nothing returns empty list."""
    reg = BrickRegistry()
    results = reg.search("xyzzy_no_match_ever_42")
    assert results == []


# ---------------------------------------------------------------------------
# get_categories
# ---------------------------------------------------------------------------

def test_registry_get_categories():
    """get_categories() returns sorted unique category names."""
    reg = BrickRegistry()
    cats = reg.get_categories()
    assert isinstance(cats, list)
    assert len(cats) == len(set(cats)), "Duplicate categories returned"
    assert cats == sorted(cats), "Categories not sorted"
    # Built-in categories that must be present
    assert "http" in cats
    assert "cli" in cats
    assert "file" in cats
    assert "mcp" in cats
    assert "python" in cats
    assert "transform" in cats
    assert "pipeline" in cats


# ---------------------------------------------------------------------------
# MCP auto-discovery
# ---------------------------------------------------------------------------

def test_registry_discover_mcp_bricks(tmp_path):
    """discover_mcp_bricks with a populated cache registers correct bricks."""
    tools = [
        {"name": "list_messages", "description": "List email messages"},
        {"name": "send_message", "description": "Send an email"},
    ]
    cache = _make_schema_cache_with_tools(tmp_path, "m365", tools)

    reg = BrickRegistry()
    count = reg.discover_mcp_bricks("m365", cache)

    assert count == 2
    assert reg.count == 12  # 10 builtins + 2 discovered

    brick = reg.get("m365:list_messages")
    assert brick is not None
    assert brick.type == "mcp"
    assert brick.category == "mcp:m365"
    assert brick.name == "m365:list_messages"
    assert brick.description == "List email messages"


def test_registry_discover_mcp_empty(tmp_path):
    """discover_mcp_bricks with no cached tools returns 0."""
    cache = SchemaCache(cache_dir=tmp_path / "cache")  # empty cache

    reg = BrickRegistry()
    count = reg.discover_mcp_bricks("nonexistent_server", cache)

    assert count == 0
    assert reg.count == 10  # unchanged


def test_registry_discover_mcp_with_schema(tmp_path):
    """MCP tool with inputSchema maps to correct BrickParams."""
    tools = [
        {
            "name": "get_document",
            "description": "Fetch a document by ID",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "document_id": {
                        "type": "string",
                        "description": "The document identifier",
                    },
                    "include_content": {
                        "type": "boolean",
                        "description": "Whether to include full content",
                        "default": False,
                    },
                    "format": {
                        "type": "string",
                        "description": "Output format",
                        "enum": ["json", "text", "html"],
                    },
                },
                "required": ["document_id"],
            },
        }
    ]
    cache = _make_schema_cache_with_tools(tmp_path, "docs", tools)

    reg = BrickRegistry()
    count = reg.discover_mcp_bricks("docs", cache)

    assert count == 1

    brick = reg.get("docs:get_document")
    assert brick is not None

    # Implicit server/tool params
    assert "server" in brick.config_schema
    assert brick.config_schema["server"].default == "docs"
    assert "tool" in brick.config_schema
    assert brick.config_schema["tool"].default == "get_document"

    # document_id: required
    assert "document_id" in brick.config_schema
    doc_param = brick.config_schema["document_id"]
    assert doc_param.type == "string"
    assert doc_param.required is True
    assert doc_param.description == "The document identifier"

    # include_content: optional with default
    assert "include_content" in brick.config_schema
    content_param = brick.config_schema["include_content"]
    assert content_param.type == "boolean"
    assert content_param.required is False
    assert content_param.default is False

    # format: enum
    assert "format" in brick.config_schema
    fmt_param = brick.config_schema["format"]
    assert fmt_param.enum == ["json", "text", "html"]


def test_registry_discover_mcp_skips_unnamed_tools(tmp_path):
    """Tools without a name are silently skipped."""
    tools = [
        {"name": "valid_tool", "description": "OK"},
        {"description": "No name here"},  # no 'name' key
        {"name": "", "description": "Empty name"},  # empty name
    ]
    cache = _make_schema_cache_with_tools(tmp_path, "myserver", tools)

    reg = BrickRegistry()
    count = reg.discover_mcp_bricks("myserver", cache)

    assert count == 1
    assert reg.get("myserver:valid_tool") is not None


def test_registry_discover_all_mcp_servers(tmp_path):
    """discover_all_mcp_servers discovers from every cached server."""
    cache = SchemaCache(cache_dir=tmp_path / "cache")
    cache.save_tools("server_a", [{"name": "tool1"}, {"name": "tool2"}])
    cache.save_tools("server_b", [{"name": "toolX"}])

    reg = BrickRegistry()
    total = reg.discover_all_mcp_servers(cache)

    assert total == 3
    assert reg.count == 13  # 10 builtins + 3
    assert reg.get("server_a:tool1") is not None
    assert reg.get("server_a:tool2") is not None
    assert reg.get("server_b:toolX") is not None


def test_registry_discover_mcp_when_to_use(tmp_path):
    """Discovered MCP bricks have sensible when_to_use text."""
    tools = [{"name": "fetch_data", "description": "Fetches data"}]
    cache = _make_schema_cache_with_tools(tmp_path, "myapi", tools)

    reg = BrickRegistry()
    reg.discover_mcp_bricks("myapi", cache)

    brick = reg.get("myapi:fetch_data")
    assert "myapi" in brick.when_to_use
    assert "fetch_data" in brick.when_to_use


def test_registry_discover_mcp_default_description(tmp_path):
    """Tool without description gets a default description."""
    tools = [{"name": "mystery_tool"}]
    cache = _make_schema_cache_with_tools(tmp_path, "srv", tools)

    reg = BrickRegistry()
    reg.discover_mcp_bricks("srv", cache)

    brick = reg.get("srv:mystery_tool")
    assert brick is not None
    assert "mystery_tool" in brick.description


# ---------------------------------------------------------------------------
# count / builtin_count properties
# ---------------------------------------------------------------------------

def test_registry_count_property():
    """count property reflects total registered bricks."""
    reg = BrickRegistry()
    assert reg.count == 10
    reg.register(_make_custom_brick("extra1"))
    assert reg.count == 11
    reg.unregister("extra1")
    assert reg.count == 10


def test_registry_builtin_count_is_constant():
    """builtin_count always returns 10 regardless of custom registrations."""
    reg = BrickRegistry()
    reg.register(_make_custom_brick("extra"))
    assert reg.builtin_count == 10
