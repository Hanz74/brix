from brix.db import merge_step_config_into_params


def test_helper_in_config_is_promoted_to_top_level():
    step = {
        "id": "run_helper",
        "type": "script.python",
        "config": {"helper": "parse_invoice"},
        "params": None,
        "helper": None,
    }

    result = merge_step_config_into_params(step)

    assert result["helper"] == "parse_invoice"


def test_script_in_config_is_promoted_to_top_level():
    step = {
        "id": "run_script",
        "type": "script.python",
        "config": {"script": "/app/helpers/parse_invoice.py"},
        "params": None,
        "script": None,
    }

    result = merge_step_config_into_params(step)

    assert result["script"] == "/app/helpers/parse_invoice.py"


def test_connection_in_config_is_promoted_to_top_level():
    step = {
        "id": "query_db",
        "type": "db.query",
        "config": {"connection": "analytics-db", "query": "SELECT 1"},
        "params": None,
        "connection": None,
    }

    result = merge_step_config_into_params(step)

    assert result["connection"] == "analytics-db"


def test_server_and_tool_in_config_are_promoted_to_top_level():
    step = {
        "id": "call_mcp",
        "type": "mcp.call",
        "config": {"server": "github", "tool": "fetch_pr"},
        "params": None,
        "server": None,
        "tool": None,
    }

    result = merge_step_config_into_params(step)

    assert result["server"] == "github"
    assert result["tool"] == "fetch_pr"


def test_config_values_override_existing_top_level_values():
    step = {
        "id": "mixed_sources",
        "type": "script.python",
        "config": {
            "helper": "config_helper",
            "script": "/tmp/config_script.py",
        },
        "params": None,
        "helper": "top_level_helper",
        "script": "/tmp/top_level_script.py",
    }

    result = merge_step_config_into_params(step)

    assert result["helper"] == "config_helper"
    assert result["script"] == "/tmp/config_script.py"
