"""Tests for MCP schema caching."""
import json
import pytest
from datetime import datetime, timedelta
from brix.cache import SchemaCache

SAMPLE_TOOLS = [
    {"name": "list-mail", "description": "List mail messages", "inputSchema": {"type": "object"}},
    {"name": "get-mail", "description": "Get a specific mail", "inputSchema": {"type": "object"}},
]

def test_save_and_load_tools(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)

    loaded = cache.load_tools("m365")
    assert loaded == SAMPLE_TOOLS

def test_load_tools_not_cached(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    assert cache.load_tools("nonexistent") is None

def test_save_creates_meta(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)

    meta = cache.load_meta("m365")
    assert meta is not None
    assert meta["tool_count"] == 2
    assert "schema_hash" in meta
    assert "cached_at" in meta

def test_is_valid_fresh_cache(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)
    assert cache.is_valid("m365") is True

def test_is_valid_expired_cache(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)

    # Manipulate cached_at to be 8 days ago
    meta_path = tmp_path / "m365" / "meta.json"
    meta = json.loads(meta_path.read_text())
    meta["cached_at"] = (datetime.utcnow() - timedelta(days=8)).isoformat()
    meta_path.write_text(json.dumps(meta))

    assert cache.is_valid("m365") is False

def test_is_valid_no_cache(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    assert cache.is_valid("m365") is False

def test_is_stale_same_hash(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)
    current_hash = cache.compute_hash(SAMPLE_TOOLS)
    assert cache.is_stale("m365", current_hash) is False

def test_is_stale_different_hash(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)
    assert cache.is_stale("m365", "different_hash") is True

def test_invalidate(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)
    assert cache.load_tools("m365") is not None

    cache.invalidate("m365")
    assert cache.load_tools("m365") is None

def test_get_tool_names(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)
    names = cache.get_tool_names("m365")
    assert names == ["list-mail", "get-mail"]

def test_get_tool_names_no_cache(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    assert cache.get_tool_names("m365") == []

def test_get_tool_schema(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)
    schema = cache.get_tool_schema("m365", "list-mail")
    assert schema is not None
    assert schema["name"] == "list-mail"

def test_get_tool_schema_not_found(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)
    assert cache.get_tool_schema("m365", "nonexistent") is None

def test_list_cached_servers(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    cache.save_tools("m365", SAMPLE_TOOLS)
    cache.save_tools("docker", [{"name": "list-containers"}])

    servers = cache.list_cached_servers()
    assert set(servers) == {"m365", "docker"}

def test_list_cached_servers_empty(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    assert cache.list_cached_servers() == []

def test_compute_hash_deterministic(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    h1 = cache.compute_hash(SAMPLE_TOOLS)
    h2 = cache.compute_hash(SAMPLE_TOOLS)
    assert h1 == h2

def test_compute_hash_different_for_different_tools(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path)
    h1 = cache.compute_hash(SAMPLE_TOOLS)
    h2 = cache.compute_hash([{"name": "other-tool"}])
    assert h1 != h2
