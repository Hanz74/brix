"""Tests for T-BRIX-V8-04: Source-Connector-Abstraktion.

Covers:
1. Connector-Registry: all 6 connectors present with correct fields
2. list_connectors: all connectors returned, type_filter works
3. get_connector: full details including output schema and parameters
4. connector_status: detects missing MCP server, missing credentials
5. NormalizedItem: Pydantic validation + required fields
6. MCP handlers: list_connectors, get_connector, connector_status
7. Composer integration: compose_pipeline includes connector matches
"""
from __future__ import annotations

import asyncio
import os
import pytest
from unittest.mock import patch, MagicMock

from brix.connectors import (
    CONNECTOR_REGISTRY,
    ConnectorParam,
    NormalizedItem,
    SourceConnector,
    connector_status,
    get_connector,
    list_connectors,
)
from brix.mcp_handlers.connectors import (
    _handle_connector_status,
    _handle_get_connector,
    _handle_list_connectors,
)


# ---------------------------------------------------------------------------
# 1. Connector-Registry: all 6 connectors present
# ---------------------------------------------------------------------------

class TestConnectorRegistry:
    EXPECTED_CONNECTORS = {"outlook", "gmail", "onedrive", "paypal", "sparkasse", "local_files"}

    def test_all_six_connectors_present(self):
        assert set(CONNECTOR_REGISTRY.keys()) == self.EXPECTED_CONNECTORS

    def test_outlook_connector_fields(self):
        c = CONNECTOR_REGISTRY["outlook"]
        assert c.name == "outlook"
        assert c.type == "email"
        assert c.required_mcp_server == "m365"
        assert "list-mail-messages" in c.required_mcp_tools
        assert "get-mail-message" in c.required_mcp_tools
        assert "list-mail-attachments" in c.required_mcp_tools
        assert len(c.parameters) >= 1
        assert len(c.output_schema) > 0

    def test_gmail_connector_fields(self):
        c = CONNECTOR_REGISTRY["gmail"]
        assert c.name == "gmail"
        assert c.type == "email"
        assert c.required_mcp_server is None
        assert c.required_mcp_tools == []
        # Must have username and app_password params
        param_names = {p.name for p in c.parameters}
        assert "username" in param_names
        assert "app_password" in param_names

    def test_onedrive_connector_fields(self):
        c = CONNECTOR_REGISTRY["onedrive"]
        assert c.name == "onedrive"
        assert c.type == "file_storage"
        assert c.required_mcp_server == "m365"
        assert "list-folder-files" in c.required_mcp_tools
        assert "download-onedrive-file-content" in c.required_mcp_tools

    def test_paypal_connector_fields(self):
        c = CONNECTOR_REGISTRY["paypal"]
        assert c.name == "paypal"
        assert c.type == "payment"
        assert c.required_mcp_server is None
        param_names = {p.name for p in c.parameters}
        assert "client_id" in param_names
        assert "client_secret" in param_names

    def test_sparkasse_connector_fields(self):
        c = CONNECTOR_REGISTRY["sparkasse"]
        assert c.name == "sparkasse"
        assert c.type == "bank"
        assert c.required_mcp_server is None
        param_names = {p.name for p in c.parameters}
        assert "blz" in param_names
        assert "username" in param_names
        assert "pin" in param_names

    def test_local_files_connector_fields(self):
        c = CONNECTOR_REGISTRY["local_files"]
        assert c.name == "local_files"
        assert c.type == "file_storage"
        assert c.required_mcp_server is None
        param_names = {p.name for p in c.parameters}
        assert "path" in param_names
        assert "pattern" in param_names

    def test_all_connectors_have_output_schema(self):
        for name, c in CONNECTOR_REGISTRY.items():
            assert c.output_schema, f"Connector '{name}' has empty output_schema"
            assert "type" in c.output_schema, f"Connector '{name}' output_schema missing 'type'"

    def test_all_connectors_have_description(self):
        for name, c in CONNECTOR_REGISTRY.items():
            assert c.description.strip(), f"Connector '{name}' has empty description"

    def test_connector_types_are_valid(self):
        valid_types = {"email", "file_storage", "payment", "bank"}
        for name, c in CONNECTOR_REGISTRY.items():
            assert c.type in valid_types, f"Connector '{name}' has unknown type '{c.type}'"


# ---------------------------------------------------------------------------
# 2. list_connectors helper
# ---------------------------------------------------------------------------

class TestListConnectors:
    def test_returns_all_six(self):
        result = list_connectors()
        assert len(result) == 6

    def test_type_filter_email(self):
        result = list_connectors(type_filter="email")
        assert len(result) == 2
        names = {c.name for c in result}
        assert names == {"outlook", "gmail"}

    def test_type_filter_file_storage(self):
        result = list_connectors(type_filter="file_storage")
        names = {c.name for c in result}
        assert names == {"onedrive", "local_files"}

    def test_type_filter_payment(self):
        result = list_connectors(type_filter="payment")
        assert len(result) == 1
        assert result[0].name == "paypal"

    def test_type_filter_bank(self):
        result = list_connectors(type_filter="bank")
        assert len(result) == 1
        assert result[0].name == "sparkasse"

    def test_type_filter_unknown_returns_empty(self):
        result = list_connectors(type_filter="unknown_type")
        assert result == []

    def test_no_filter_returns_source_connector_instances(self):
        result = list_connectors()
        for c in result:
            assert isinstance(c, SourceConnector)


# ---------------------------------------------------------------------------
# 3. get_connector helper
# ---------------------------------------------------------------------------

class TestGetConnector:
    def test_returns_connector_by_name(self):
        c = get_connector("outlook")
        assert c is not None
        assert c.name == "outlook"

    def test_returns_none_for_unknown(self):
        assert get_connector("nonexistent") is None

    def test_all_connectors_retrievable(self):
        for name in CONNECTOR_REGISTRY:
            c = get_connector(name)
            assert c is not None
            assert c.name == name


# ---------------------------------------------------------------------------
# 4. connector_status: detects missing MCP server / credentials
# ---------------------------------------------------------------------------

class TestConnectorStatus:
    def test_not_found_for_unknown_connector(self):
        result = connector_status("nonexistent")
        assert result["found"] is False
        assert result["status"] == "not_found"
        assert "nonexistent" in result["message"]

    def test_local_files_ready_without_mcp(self):
        # local_files has no required_mcp_server, so mcp_server_available = None
        result = connector_status("local_files")
        assert result["found"] is True
        assert result["mcp_server_available"] is None

    def test_outlook_missing_mcp_when_db_unavailable(self):
        # When DB raises, mcp_server_available should be None
        with patch("brix.connectors.BrixDB", side_effect=Exception("no db")):
            result = connector_status("outlook")
        assert result["found"] is True
        assert result["mcp_server_available"] is None

    def test_outlook_missing_mcp_when_server_not_registered(self):
        mock_db = MagicMock()
        mock_db.server_list.return_value = [{"name": "other_server"}]
        with patch("brix.connectors.BrixDB", return_value=mock_db):
            result = connector_status("outlook")
        assert result["found"] is True
        assert result["mcp_server_available"] is False
        assert result["status"] == "missing_mcp"

    def test_outlook_ready_when_m365_registered(self):
        mock_db = MagicMock()
        mock_db.server_list.return_value = [{"name": "m365"}]
        with patch("brix.connectors.BrixDB", return_value=mock_db):
            result = connector_status("outlook")
        assert result["mcp_server_available"] is True

    def test_result_keys_always_present(self):
        result = connector_status("sparkasse")
        for key in ("found", "mcp_server_available", "missing_env_vars", "status", "message"):
            assert key in result, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# 5. NormalizedItem: Pydantic validation
# ---------------------------------------------------------------------------

class TestNormalizedItem:
    def test_minimal_valid_item(self):
        item = NormalizedItem(
            source="outlook",
            source_type="email",
            item_id="msg-001",
            title="Test Subject",
        )
        assert item.source == "outlook"
        assert item.source_type == "email"
        assert item.item_id == "msg-001"
        assert item.title == "Test Subject"
        assert item.content is None
        assert item.metadata == {}
        assert item.attachments == []
        assert item.timestamp is None
        assert item.raw == {}

    def test_full_item(self):
        item = NormalizedItem(
            source="gmail",
            source_type="email",
            item_id="msg-123",
            title="Invoice #42",
            content="Dear customer...",
            metadata={"from": "sender@example.com", "folder": "INBOX"},
            attachments=[{"name": "invoice.pdf", "size": 12345}],
            timestamp="2026-01-15T10:30:00Z",
            raw={"original_id": "msg-123", "headers": {}},
        )
        assert item.source == "gmail"
        assert len(item.attachments) == 1
        assert item.metadata["from"] == "sender@example.com"
        assert item.timestamp == "2026-01-15T10:30:00Z"

    def test_missing_required_field_raises(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            NormalizedItem(
                source="outlook",
                source_type="email",
                # missing item_id and title
            )

    def test_serialization(self):
        item = NormalizedItem(
            source="paypal",
            source_type="payment",
            item_id="txn-456",
            title="Payment to Example Shop",
        )
        d = item.model_dump()
        assert d["source"] == "paypal"
        assert d["source_type"] == "payment"
        assert d["item_id"] == "txn-456"


# ---------------------------------------------------------------------------
# 6. MCP Handlers
# ---------------------------------------------------------------------------

class TestMCPHandlers:
    def test_list_connectors_returns_all(self):
        result = asyncio.run(_handle_list_connectors({}))
        assert result["success"] is True
        assert result["count"] == 6
        names = {c["name"] for c in result["connectors"]}
        assert "outlook" in names
        assert "gmail" in names
        assert "onedrive" in names
        assert "paypal" in names
        assert "sparkasse" in names
        assert "local_files" in names

    def test_list_connectors_type_filter(self):
        result = asyncio.run(_handle_list_connectors({"type_filter": "email"}))
        assert result["success"] is True
        assert result["count"] == 2
        assert result["type_filter"] == "email"

    def test_list_connectors_connector_fields(self):
        result = asyncio.run(_handle_list_connectors({}))
        connector = next(c for c in result["connectors"] if c["name"] == "outlook")
        assert "type" in connector
        assert "description" in connector
        assert "required_mcp_server" in connector
        assert "required_mcp_tools" in connector
        assert "parameter_count" in connector
        assert "related_pipelines" in connector

    def test_get_connector_known(self):
        result = asyncio.run(_handle_get_connector({"name": "outlook"}))
        assert result["success"] is True
        c = result["connector"]
        assert c["name"] == "outlook"
        assert c["type"] == "email"
        assert "output_schema" in c
        assert "parameters" in c
        assert len(c["parameters"]) >= 1
        assert "required_mcp_tools" in c

    def test_get_connector_parameter_fields(self):
        result = asyncio.run(_handle_get_connector({"name": "gmail"}))
        assert result["success"] is True
        params = result["connector"]["parameters"]
        param_names = {p["name"] for p in params}
        assert "username" in param_names
        assert "app_password" in param_names
        # Each param has required fields
        for p in params:
            for field in ("name", "type", "description", "required"):
                assert field in p, f"Missing field '{field}' in param {p}"

    def test_get_connector_unknown(self):
        result = asyncio.run(_handle_get_connector({"name": "unknown"}))
        assert result["success"] is False
        assert "error" in result
        assert "available_connectors" in result

    def test_get_connector_missing_name(self):
        result = asyncio.run(_handle_get_connector({}))
        assert result["success"] is False
        assert "name" in result["error"]

    def test_connector_status_not_found(self):
        result = asyncio.run(_handle_connector_status({"name": "unknown"}))
        assert result["success"] is True
        assert result["found"] is False
        assert result["status"] == "not_found"

    def test_connector_status_known(self):
        result = asyncio.run(_handle_connector_status({"name": "local_files"}))
        assert result["success"] is True
        assert result["found"] is True
        assert "status" in result
        assert "message" in result

    def test_connector_status_missing_name(self):
        result = asyncio.run(_handle_connector_status({}))
        assert result["success"] is False
        assert "name" in result["error"]

    def test_connector_status_outlook_detects_missing_mcp(self):
        mock_db = MagicMock()
        mock_db.server_list.return_value = []
        with patch("brix.connectors.BrixDB", return_value=mock_db):
            result = asyncio.run(_handle_connector_status({"name": "outlook"}))
        assert result["success"] is True
        assert result["found"] is True
        assert result["mcp_server_available"] is False
        assert result["status"] == "missing_mcp"


# ---------------------------------------------------------------------------
# 7. Composer integration: compose_pipeline includes connector matches
# ---------------------------------------------------------------------------

class TestComposerIntegration:
    @pytest.fixture
    def tmp_pipelines_dir(self, tmp_path, monkeypatch):
        """Redirect pipeline storage to a temp directory."""
        from brix.pipeline_store import PipelineStore
        import brix.mcp_handlers._shared as shared_mod
        import brix.mcp_handlers.composer as composer_mod

        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(
            composer_mod,
            "_pipeline_dir",
            lambda: pipelines_dir,
        )
        monkeypatch.setattr(
            shared_mod,
            "_pipeline_dir",
            lambda: pipelines_dir,
        )
        return pipelines_dir

    def test_compose_pipeline_includes_connectors_key(self, tmp_pipelines_dir):
        from brix.mcp_handlers.composer import _handle_compose_pipeline

        result = asyncio.run(
            _handle_compose_pipeline({"goal": "Download emails from Outlook"})
        )
        assert result["success"] is True
        assert "connectors" in result

    def test_compose_pipeline_outlook_matches_connector(self, tmp_pipelines_dir):
        from brix.mcp_handlers.composer import _handle_compose_pipeline

        result = asyncio.run(
            _handle_compose_pipeline({"goal": "Fetch emails from Outlook and classify them"})
        )
        assert result["success"] is True
        connector_names = [c["name"] for c in result["connectors"]]
        assert "outlook" in connector_names

    def test_compose_pipeline_gmail_matches_connector(self, tmp_pipelines_dir):
        from brix.mcp_handlers.composer import _handle_compose_pipeline

        result = asyncio.run(
            _handle_compose_pipeline({"goal": "Fetch gmail messages and store as json"})
        )
        assert result["success"] is True
        connector_names = [c["name"] for c in result["connectors"]]
        assert "gmail" in connector_names

    def test_compose_pipeline_connector_fields(self, tmp_pipelines_dir):
        from brix.mcp_handlers.composer import _handle_compose_pipeline

        result = asyncio.run(
            _handle_compose_pipeline({"goal": "Download files from OneDrive"})
        )
        assert result["success"] is True
        if result["connectors"]:
            c = result["connectors"][0]
            for field in ("type", "name", "connector_type", "description", "relevance", "reason"):
                assert field in c, f"Connector match missing field '{field}'"

    def test_compose_pipeline_next_steps_mentions_connector(self, tmp_pipelines_dir):
        from brix.mcp_handlers.composer import _handle_compose_pipeline

        result = asyncio.run(
            _handle_compose_pipeline({"goal": "Fetch emails from Outlook"})
        )
        assert result["success"] is True
        # When a connector matches, next_steps should mention brix__get_connector
        if result["connectors"]:
            next_steps_text = " ".join(result["next_steps"])
            assert "brix__get_connector" in next_steps_text

    def test_discover_connectors_function(self):
        """Direct test of _discover_connectors helper."""
        from brix.mcp_handlers.composer import _discover_connectors

        intent = {"sources": ["outlook"], "actions": ["download"], "targets": []}
        matches = _discover_connectors(intent, "fetch emails from outlook")
        assert len(matches) >= 1
        outlook_match = next((m for m in matches if m["name"] == "outlook"), None)
        assert outlook_match is not None
        assert outlook_match["relevance"] > 0
        assert outlook_match["type"] == "connector"

    def test_discover_connectors_no_match(self):
        """Unknown goal should return empty or very low relevance connectors."""
        from brix.mcp_handlers.composer import _discover_connectors

        intent = {"sources": [], "actions": [], "targets": []}
        matches = _discover_connectors(intent, "completely unrelated xyzzy goal")
        # Should have no high-relevance matches
        high_relevance = [m for m in matches if m["relevance"] > 0.5]
        assert len(high_relevance) == 0
