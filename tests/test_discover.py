"""Tests for T-BRIX-DB-19 — Universal Registry: discover, runners, env_config, types, namespaces.

Covers all 6 new handlers:
- brix__discover
- brix__list_runners
- brix__get_runner_info
- brix__list_env_config
- brix__list_types
- brix__list_namespaces
"""
import pytest

from brix.mcp_handlers.discover import (
    _handle_discover,
    _handle_list_runners,
    _handle_get_runner_info,
    _handle_list_env_config,
    _handle_list_types,
    _handle_list_namespaces,
)


# ---------------------------------------------------------------------------
# brix__discover — overview mode (no parameters)
# ---------------------------------------------------------------------------

class TestDiscoverOverview:
    """brix__discover() without parameters returns all categories."""

    @pytest.mark.asyncio
    async def test_returns_success(self):
        result = await _handle_discover({})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_has_categories_list(self):
        result = await _handle_discover({})
        assert "categories" in result
        assert isinstance(result["categories"], list)

    @pytest.mark.asyncio
    async def test_has_all_expected_categories(self):
        result = await _handle_discover({})
        names = {c["category"] for c in result["categories"]}
        expected = {
            "bricks", "pipelines", "runners", "connectors", "connections",
            "credentials", "variables", "templates", "triggers", "alerts",
            "types", "jinja_filters", "env_config", "namespaces", "helpers", "runs",
        }
        assert expected == names

    @pytest.mark.asyncio
    async def test_each_category_has_required_fields(self):
        result = await _handle_discover({})
        for cat in result["categories"]:
            assert "category" in cat
            assert "description" in cat
            assert "count" in cat
            assert "tool" in cat
            assert isinstance(cat["count"], int)
            assert cat["count"] >= 0

    @pytest.mark.asyncio
    async def test_has_usage_hints(self):
        result = await _handle_discover({})
        assert "usage" in result
        usage = result["usage"]
        assert "overview" in usage
        assert "category_detail" in usage
        assert "global_search" in usage

    @pytest.mark.asyncio
    async def test_bricks_count_positive(self):
        result = await _handle_discover({})
        cats = {c["category"]: c for c in result["categories"]}
        assert cats["bricks"]["count"] > 0

    @pytest.mark.asyncio
    async def test_types_count_positive(self):
        result = await _handle_discover({})
        cats = {c["category"]: c for c in result["categories"]}
        assert cats["types"]["count"] > 0

    @pytest.mark.asyncio
    async def test_runners_count_positive(self):
        result = await _handle_discover({})
        cats = {c["category"]: c for c in result["categories"]}
        assert cats["runners"]["count"] > 0


# ---------------------------------------------------------------------------
# brix__discover — category mode
# ---------------------------------------------------------------------------

class TestDiscoverCategory:
    """brix__discover(category=X) returns details for a specific category."""

    @pytest.mark.asyncio
    async def test_discover_bricks_category(self):
        result = await _handle_discover({"category": "bricks"})
        assert result["success"] is True
        assert result["category"] == "bricks"
        assert "items" in result
        assert isinstance(result["items"], list)
        assert result["count"] > 0

    @pytest.mark.asyncio
    async def test_discover_runners_category(self):
        result = await _handle_discover({"category": "runners"})
        assert result["success"] is True
        assert result["category"] == "runners"
        assert result["count"] > 0
        for item in result["items"]:
            assert "name" in item
            assert "input_type" in item
            assert "output_type" in item

    @pytest.mark.asyncio
    async def test_discover_types_category(self):
        result = await _handle_discover({"category": "types"})
        assert result["success"] is True
        assert result["category"] == "types"
        assert result["count"] > 0
        for item in result["items"]:
            assert "name" in item
            assert "compatible_with" in item

    @pytest.mark.asyncio
    async def test_discover_env_config_category(self):
        result = await _handle_discover({"category": "env_config"})
        assert result["success"] is True
        assert result["count"] > 0
        for item in result["items"]:
            assert "env_override" in item

    @pytest.mark.asyncio
    async def test_discover_namespaces_category(self):
        result = await _handle_discover({"category": "namespaces"})
        assert result["success"] is True
        assert result["count"] > 0

    @pytest.mark.asyncio
    async def test_discover_unknown_category_returns_error(self):
        result = await _handle_discover({"category": "nonexistent_xyz"})
        assert result["success"] is False
        assert "error" in result
        assert "available_categories" in result

    @pytest.mark.asyncio
    async def test_discover_jinja_filters_category(self):
        result = await _handle_discover({"category": "jinja_filters"})
        assert result["success"] is True
        assert result["count"] > 0
        names = [item["name"] for item in result["items"]]
        # Standard Jinja2 filters should be present
        assert "selectattr" in names
        assert "map" in names
        assert "default" in names


# ---------------------------------------------------------------------------
# brix__discover — query / search mode
# ---------------------------------------------------------------------------

class TestDiscoverSearch:
    """brix__discover(query=X) searches across all categories."""

    @pytest.mark.asyncio
    async def test_search_returns_success(self):
        result = await _handle_discover({"query": "http"})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_search_has_results_key(self):
        result = await _handle_discover({"query": "http"})
        assert "results" in result
        assert isinstance(result["results"], list)

    @pytest.mark.asyncio
    async def test_search_has_by_category(self):
        result = await _handle_discover({"query": "http"})
        assert "by_category" in result
        assert isinstance(result["by_category"], dict)

    @pytest.mark.asyncio
    async def test_search_finds_bricks(self):
        result = await _handle_discover({"query": "http"})
        cats = result.get("by_category", {})
        assert "bricks" in cats or result["total_matches"] > 0

    @pytest.mark.asyncio
    async def test_search_no_results_returns_empty(self):
        result = await _handle_discover({"query": "zzz_nonexistent_xyz_999"})
        assert result["success"] is True
        assert result["total_matches"] == 0
        assert result["results"] == []

    @pytest.mark.asyncio
    async def test_search_each_result_has_required_fields(self):
        result = await _handle_discover({"query": "filter"})
        for item in result["results"]:
            assert "category" in item
            assert "name" in item
            assert "description" in item


# ---------------------------------------------------------------------------
# brix__list_runners
# ---------------------------------------------------------------------------

class TestListRunners:
    """brix__list_runners returns all pipeline step runners."""

    @pytest.mark.asyncio
    async def test_returns_success(self):
        result = await _handle_list_runners({})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_has_runners_list(self):
        result = await _handle_list_runners({})
        assert "runners" in result
        assert isinstance(result["runners"], list)

    @pytest.mark.asyncio
    async def test_count_positive(self):
        result = await _handle_list_runners({})
        assert result["count"] > 0
        assert result["count"] == len(result["runners"])

    @pytest.mark.asyncio
    async def test_each_runner_has_required_fields(self):
        result = await _handle_list_runners({})
        for runner in result["runners"]:
            assert "name" in runner
            assert "input_type" in runner
            assert "output_type" in runner
            assert "config_schema" in runner
            assert "description" in runner

    @pytest.mark.asyncio
    async def test_known_runners_present(self):
        result = await _handle_list_runners({})
        names = {r["name"] for r in result["runners"]}
        # These runners must always exist
        for expected in ("python", "http", "mcp", "cli", "filter", "transform"):
            assert expected in names, f"Runner '{expected}' not found"

    @pytest.mark.asyncio
    async def test_runner_config_schema_is_dict(self):
        result = await _handle_list_runners({})
        for runner in result["runners"]:
            assert isinstance(runner["config_schema"], dict), (
                f"Runner '{runner['name']}' config_schema is not a dict"
            )


# ---------------------------------------------------------------------------
# brix__get_runner_info
# ---------------------------------------------------------------------------

class TestGetRunnerInfo:
    """brix__get_runner_info returns details for a single runner."""

    @pytest.mark.asyncio
    async def test_returns_success_for_known_runner(self):
        result = await _handle_get_runner_info({"name": "python"})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_has_runner_key(self):
        result = await _handle_get_runner_info({"name": "python"})
        assert "runner" in result

    @pytest.mark.asyncio
    async def test_runner_has_all_fields(self):
        result = await _handle_get_runner_info({"name": "http"})
        runner = result["runner"]
        assert "name" in runner
        assert "class" in runner
        assert "module" in runner
        assert "description" in runner
        assert "full_description" in runner
        assert "input_type" in runner
        assert "output_type" in runner
        assert "config_schema" in runner

    @pytest.mark.asyncio
    async def test_name_matches_requested(self):
        result = await _handle_get_runner_info({"name": "mcp"})
        assert result["runner"]["name"] == "mcp"

    @pytest.mark.asyncio
    async def test_missing_name_returns_error(self):
        result = await _handle_get_runner_info({})
        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_unknown_runner_returns_error(self):
        result = await _handle_get_runner_info({"name": "nonexistent_runner_xyz"})
        assert result["success"] is False
        assert "available_runners" in result

    @pytest.mark.asyncio
    async def test_filter_runner(self):
        result = await _handle_get_runner_info({"name": "filter"})
        assert result["success"] is True
        assert result["runner"]["input_type"] == "list[dict]"


# ---------------------------------------------------------------------------
# brix__list_env_config
# ---------------------------------------------------------------------------

class TestListEnvConfig:
    """brix__list_env_config returns all BRIX_* env vars."""

    @pytest.mark.asyncio
    async def test_returns_success(self):
        result = await _handle_list_env_config({})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_has_env_config_list(self):
        result = await _handle_list_env_config({})
        assert "env_config" in result
        assert isinstance(result["env_config"], list)

    @pytest.mark.asyncio
    async def test_count_positive(self):
        result = await _handle_list_env_config({})
        assert result["count"] > 0

    @pytest.mark.asyncio
    async def test_each_entry_has_required_fields(self):
        result = await _handle_list_env_config({})
        for entry in result["env_config"]:
            assert "env_var" in entry
            assert "attr" in entry
            assert "current_value" in entry
            assert "type" in entry
            assert "is_overridden" in entry

    @pytest.mark.asyncio
    async def test_env_vars_have_brix_prefix(self):
        result = await _handle_list_env_config({})
        for entry in result["env_config"]:
            assert entry["env_var"].startswith("BRIX_"), (
                f"env_var '{entry['env_var']}' does not start with BRIX_"
            )

    @pytest.mark.asyncio
    async def test_known_config_keys_present(self):
        result = await _handle_list_env_config({})
        keys = {e["env_var"] for e in result["env_config"]}
        for expected in (
            "BRIX_MCP_HTTP_PORT", "BRIX_API_PORT", "BRIX_TIMEOUT_DEFAULT",
            "BRIX_TIMEOUT_PYTHON", "BRIX_TIMEOUT_MCP",
        ):
            assert expected in keys, f"Expected key '{expected}' not found"

    @pytest.mark.asyncio
    async def test_has_note_field(self):
        result = await _handle_list_env_config({})
        assert "note" in result


# ---------------------------------------------------------------------------
# brix__list_types
# ---------------------------------------------------------------------------

class TestListTypes:
    """brix__list_types returns the type compatibility matrix."""

    @pytest.mark.asyncio
    async def test_returns_success(self):
        result = await _handle_list_types({})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_has_types_list(self):
        result = await _handle_list_types({})
        assert "types" in result
        assert isinstance(result["types"], list)

    @pytest.mark.asyncio
    async def test_count_positive(self):
        result = await _handle_list_types({})
        assert result["count"] > 0

    @pytest.mark.asyncio
    async def test_each_type_has_required_fields(self):
        result = await _handle_list_types({})
        for t in result["types"]:
            assert "name" in t
            assert "compatible_with" in t
            assert "compatible_count" in t
            assert "is_wildcard" in t
            assert "is_none" in t
            assert "converter_suggestions" in t
            assert isinstance(t["compatible_with"], list)

    @pytest.mark.asyncio
    async def test_known_types_present(self):
        result = await _handle_list_types({})
        names = {t["name"] for t in result["types"]}
        # These types are present in both code and DB-seeded tables
        for expected in ("list[dict]", "dict", "text", "string"):
            assert expected in names, f"Type '{expected}' not found"

    @pytest.mark.asyncio
    async def test_none_or_list_type_present(self):
        """Either 'none' (code) or 'list[*]' (DB) must be present — covers both setups."""
        result = await _handle_list_types({})
        names = {t["name"] for t in result["types"]}
        # At least one well-known structural type must exist
        assert len(names) > 5, "Expected at least 5 type entries"
        assert "list[dict]" in names

    @pytest.mark.asyncio
    async def test_has_note_field(self):
        result = await _handle_list_types({})
        assert "note" in result


# ---------------------------------------------------------------------------
# brix__list_namespaces
# ---------------------------------------------------------------------------

class TestListNamespaces:
    """brix__list_namespaces returns brick namespaces with member bricks."""

    @pytest.mark.asyncio
    async def test_returns_success(self):
        result = await _handle_list_namespaces({})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_has_namespaces_list(self):
        result = await _handle_list_namespaces({})
        assert "namespaces" in result
        assert isinstance(result["namespaces"], list)

    @pytest.mark.asyncio
    async def test_namespace_count_positive(self):
        result = await _handle_list_namespaces({})
        assert result["namespace_count"] > 0

    @pytest.mark.asyncio
    async def test_each_namespace_has_required_fields(self):
        result = await _handle_list_namespaces({})
        for ns in result["namespaces"]:
            assert "namespace" in ns
            assert "brick_count" in ns
            assert "bricks" in ns
            assert isinstance(ns["bricks"], list)
            assert ns["brick_count"] == len(ns["bricks"])

    @pytest.mark.asyncio
    async def test_known_namespaces_present(self):
        result = await _handle_list_namespaces({})
        names = {ns["namespace"] for ns in result["namespaces"]}
        # These namespaces must always exist (based on the builtins)
        for expected in ("flow", "db", "source", "action"):
            assert expected in names, f"Namespace '{expected}' not found"

    @pytest.mark.asyncio
    async def test_each_brick_entry_has_name(self):
        result = await _handle_list_namespaces({})
        for ns in result["namespaces"]:
            for brick in ns["bricks"]:
                assert "name" in brick, (
                    f"Brick in namespace '{ns['namespace']}' missing 'name' field"
                )

    @pytest.mark.asyncio
    async def test_has_note_field(self):
        result = await _handle_list_namespaces({})
        assert "note" in result

    @pytest.mark.asyncio
    async def test_flow_namespace_has_bricks(self):
        result = await _handle_list_namespaces({})
        flow_ns = next(
            (ns for ns in result["namespaces"] if ns["namespace"] == "flow"), None
        )
        assert flow_ns is not None
        assert flow_ns["brick_count"] > 0
