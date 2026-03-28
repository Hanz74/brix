"""Tests for brix.sdk.McpClient (T-BRIX-V4-14)."""
import json
from pathlib import Path

import pytest

from brix.sdk import McpClient


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_sdk_no_config(tmp_path):
    """McpClient with a non-existent config returns an error on call."""
    client = McpClient(config_path=tmp_path / "nonexistent.yaml")
    result = client.call("m365", "list-mail-messages")

    assert result["success"] is False
    assert "not found" in result["error"].lower()


def test_sdk_unknown_server(tmp_path):
    """Calling an unknown server name returns an error."""
    # Write a minimal servers.yaml with one server
    config_file = tmp_path / "servers.yaml"
    config_file.write_text("servers:\n  known_server:\n    command: echo\n    args: []\n")

    client = McpClient(config_path=config_file)
    result = client.call("unknown_server", "some_tool")

    assert result["success"] is False
    assert "unknown_server" in result["error"]


def test_sdk_client_init():
    """McpClient() can be instantiated without raising an exception."""
    # Even if config does not exist, construction must not raise
    client = McpClient(config_path=Path("/nonexistent/path/servers.yaml"))
    assert isinstance(client, McpClient)
    # _servers defaults to empty dict when config does not exist
    assert isinstance(client._servers, dict)


def test_sdk_module_level_mcp_instance():
    """The module-level 'mcp' convenience instance is a McpClient."""
    from brix.sdk import mcp as module_mcp

    assert isinstance(module_mcp, McpClient)


def test_sdk_call_returns_error_for_empty_servers(tmp_path):
    """call() on a client with empty servers dict returns a helpful error."""
    config_file = tmp_path / "servers.yaml"
    config_file.write_text("servers: {}\n")

    client = McpClient(config_path=config_file)
    result = client.call("anything", "tool")

    assert result["success"] is False
    assert "No servers configured" in result["error"] or "anything" in result["error"]
