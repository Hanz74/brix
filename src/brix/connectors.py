"""Source-Connector-Abstraktion (T-BRIX-V8-04).

Unified interface for all data sources. Connectors are pure metadata/schema
definitions — they describe WHAT a source provides and HOW to connect to it,
but do NOT execute anything. Actual execution happens via existing pipelines
and helpers.

T-BRIX-DB-06: Connector data is stored in connector_definitions DB table.
CONNECTOR_REGISTRY is still available (populated from DB or code-fallback).

Usage:
    from brix.connectors import CONNECTOR_REGISTRY, NormalizedItem, get_connector
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from pydantic import BaseModel, Field
from brix.db import BrixDB

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parameter definition
# ---------------------------------------------------------------------------

class ConnectorParam(BaseModel):
    """A configurable parameter for a connector."""

    name: str
    type: str  # "string", "integer", "boolean", "date"
    description: str
    required: bool = False
    default: Any = None


# ---------------------------------------------------------------------------
# Connector definition
# ---------------------------------------------------------------------------

class SourceConnector(BaseModel):
    """Einheitliches Interface für alle Datenquellen.

    Connectors are discovery and compatibility metadata — they define what
    a source provides, what tools/credentials are needed, and what the
    normalized output looks like. They do NOT execute pipelines.
    """

    name: str  # e.g. "outlook", "gmail", "onedrive"
    type: str  # "email", "file_storage", "payment", "bank"
    description: str

    # Which MCP server / tools are required (None = no MCP dependency)
    required_mcp_server: Optional[str] = None
    required_mcp_tools: list[str] = Field(default_factory=list)

    # Standard output schema — JSON Schema of the normalized output
    output_schema: dict = Field(default_factory=dict)

    # Configurable connection parameters
    parameters: list[ConnectorParam] = Field(default_factory=list)

    # Related pipelines / helpers that implement this connector
    related_pipelines: list[str] = Field(default_factory=list)
    related_helpers: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Normalized output model
# ---------------------------------------------------------------------------

class NormalizedItem(BaseModel):
    """Normalisiertes Output-Format aller Connectors.

    Regardless of source (email, file, payment, bank), all connectors
    produce NormalizedItems with this common structure.
    """

    source: str          # Connector name, e.g. "outlook"
    source_type: str     # Connector type, e.g. "email"
    item_id: str         # Unique item ID from the source
    title: str           # Subject / filename / description
    content: Optional[str] = None       # Text content if available
    metadata: dict = Field(default_factory=dict)       # Source-specific metadata
    attachments: list[dict] = Field(default_factory=list)  # Attachments / files
    timestamp: Optional[str] = None     # ISO 8601 datetime
    raw: dict = Field(default_factory=dict)             # Original source data


# ---------------------------------------------------------------------------
# Shared output schema template (reused by all connectors)
# ---------------------------------------------------------------------------

_NORMALIZED_OUTPUT_SCHEMA: dict = {
    "type": "object",
    "description": "Normalized item in the standard NormalizedItem format.",
    "properties": {
        "source":       {"type": "string", "description": "Connector name"},
        "source_type":  {"type": "string", "description": "Connector type"},
        "item_id":      {"type": "string", "description": "Unique item ID"},
        "title":        {"type": "string", "description": "Subject / filename / description"},
        "content":      {"type": ["string", "null"], "description": "Text content"},
        "metadata":     {"type": "object", "description": "Source-specific metadata"},
        "attachments":  {"type": "array",  "description": "Attachments / files"},
        "timestamp":    {"type": ["string", "null"], "description": "ISO 8601 datetime"},
        "raw":          {"type": "object", "description": "Original source data"},
    },
    "required": ["source", "source_type", "item_id", "title"],
}


# ---------------------------------------------------------------------------
# Connector registry
# ---------------------------------------------------------------------------

CONNECTOR_REGISTRY: dict[str, SourceConnector] = {
    "outlook": SourceConnector(
        name="outlook",
        type="email",
        description=(
            "Microsoft Outlook / Exchange email via M365 MCP. "
            "Fetches, filters, and reads emails and their attachments from Outlook inboxes."
        ),
        required_mcp_server="m365",
        required_mcp_tools=[
            "list-mail-messages",
            "get-mail-message",
            "list-mail-attachments",
            "get-mail-attachment",
        ],
        output_schema=_NORMALIZED_OUTPUT_SCHEMA,
        parameters=[
            ConnectorParam(
                name="folder",
                type="string",
                description="Mail folder to read from (e.g. 'INBOX', 'Sent Items').",
                required=False,
                default="INBOX",
            ),
            ConnectorParam(
                name="filter",
                type="string",
                description="OData filter expression for message selection.",
                required=False,
            ),
            ConnectorParam(
                name="top",
                type="integer",
                description="Maximum number of messages to fetch.",
                required=False,
                default=50,
            ),
        ],
        related_pipelines=[
            "buddy-intake-outlook",
            "download-attachments",
            "download-attachments-broad",
        ],
        related_helpers=[
            "buddy_prepare_mails.py",
            "buddy_classify.py",
        ],
    ),

    "gmail": SourceConnector(
        name="gmail",
        type="email",
        description=(
            "Gmail / Google Mail via IMAP. "
            "Connects directly via IMAP using app-specific passwords. "
            "No MCP server dependency."
        ),
        required_mcp_server=None,
        required_mcp_tools=[],
        output_schema=_NORMALIZED_OUTPUT_SCHEMA,
        parameters=[
            ConnectorParam(
                name="imap_host",
                type="string",
                description="IMAP host (default: imap.gmail.com).",
                required=False,
                default="imap.gmail.com",
            ),
            ConnectorParam(
                name="username",
                type="string",
                description="Gmail address (e.g. user@gmail.com).",
                required=True,
            ),
            ConnectorParam(
                name="app_password",
                type="string",
                description="Google app-specific password (not the main account password).",
                required=True,
            ),
            ConnectorParam(
                name="folder",
                type="string",
                description="IMAP folder to read from.",
                required=False,
                default="INBOX",
            ),
            ConnectorParam(
                name="top",
                type="integer",
                description="Maximum number of messages to fetch.",
                required=False,
                default=50,
            ),
        ],
        related_pipelines=[
            "buddy-intake-gmail",
        ],
        related_helpers=[
            "buddy_fetch_gmail.py",
            "buddy_fetch_mails.py",
        ],
    ),

    "onedrive": SourceConnector(
        name="onedrive",
        type="file_storage",
        description=(
            "Microsoft OneDrive / SharePoint file storage via M365 MCP. "
            "Lists, downloads, and processes files and folders."
        ),
        required_mcp_server="m365",
        required_mcp_tools=[
            "list-folder-files",
            "download-onedrive-file-content",
            "get-drive-root-item",
            "list-drives",
        ],
        output_schema=_NORMALIZED_OUTPUT_SCHEMA,
        parameters=[
            ConnectorParam(
                name="folder_path",
                type="string",
                description="OneDrive folder path to list (e.g. '/Documents/Invoices').",
                required=False,
                default="/",
            ),
            ConnectorParam(
                name="pattern",
                type="string",
                description="Filename glob pattern to filter files (e.g. '*.pdf').",
                required=False,
            ),
            ConnectorParam(
                name="recursive",
                type="boolean",
                description="Recurse into subfolders.",
                required=False,
                default=False,
            ),
        ],
        related_pipelines=[
            "buddy-onedrive-scan",
            "buddy-onedrive-download",
            "buddy-onedrive-filter",
            "buddy-intake-onedrive",
        ],
        related_helpers=[
            "buddy_intake_onedrive.py",
            "buddy_prepare_onedrive.py",
        ],
    ),

    "paypal": SourceConnector(
        name="paypal",
        type="payment",
        description=(
            "PayPal transaction history via REST API. "
            "Fetches payment transactions, refunds, and order data. "
            "No MCP server dependency — uses HTTP with OAuth2 credentials."
        ),
        required_mcp_server=None,
        required_mcp_tools=[],
        output_schema=_NORMALIZED_OUTPUT_SCHEMA,
        parameters=[
            ConnectorParam(
                name="client_id",
                type="string",
                description="PayPal REST API client ID.",
                required=True,
            ),
            ConnectorParam(
                name="client_secret",
                type="string",
                description="PayPal REST API client secret.",
                required=True,
            ),
            ConnectorParam(
                name="environment",
                type="string",
                description="API environment: 'live' or 'sandbox'.",
                required=False,
                default="live",
            ),
            ConnectorParam(
                name="start_date",
                type="date",
                description="Fetch transactions from this date (ISO 8601).",
                required=False,
            ),
            ConnectorParam(
                name="end_date",
                type="date",
                description="Fetch transactions up to this date (ISO 8601).",
                required=False,
            ),
        ],
        related_pipelines=[
            "buddy-intake-paypal",
        ],
        related_helpers=[
            "buddy_ingest_transactions.py",
        ],
    ),

    "sparkasse": SourceConnector(
        name="sparkasse",
        type="bank",
        description=(
            "Sparkasse / German bank account via FinTS/HBCI protocol. "
            "Fetches account transactions and balances. "
            "No MCP server dependency — uses FinTS library directly."
        ),
        required_mcp_server=None,
        required_mcp_tools=[],
        output_schema=_NORMALIZED_OUTPUT_SCHEMA,
        parameters=[
            ConnectorParam(
                name="blz",
                type="string",
                description="Bank identification number (Bankleitzahl).",
                required=True,
            ),
            ConnectorParam(
                name="username",
                type="string",
                description="Online banking username / Kontonummer.",
                required=True,
            ),
            ConnectorParam(
                name="pin",
                type="string",
                description="Online banking PIN.",
                required=True,
            ),
            ConnectorParam(
                name="start_date",
                type="date",
                description="Fetch transactions from this date (ISO 8601).",
                required=False,
            ),
            ConnectorParam(
                name="end_date",
                type="date",
                description="Fetch transactions up to this date (ISO 8601).",
                required=False,
            ),
        ],
        related_pipelines=[
            "buddy-import-sparkasse-live",
        ],
        related_helpers=[
            "buddy_fetch_sparkasse.py",
            "buddy_ingest_transactions.py",
        ],
    ),

    "local_files": SourceConnector(
        name="local_files",
        type="file_storage",
        description=(
            "Local filesystem files. "
            "Reads files from a local directory, optionally filtered by glob pattern. "
            "No MCP server dependency."
        ),
        required_mcp_server=None,
        required_mcp_tools=[],
        output_schema=_NORMALIZED_OUTPUT_SCHEMA,
        parameters=[
            ConnectorParam(
                name="path",
                type="string",
                description="Absolute path to the directory to scan.",
                required=True,
            ),
            ConnectorParam(
                name="pattern",
                type="string",
                description="Glob pattern to filter files (e.g. '*.pdf', '**/*.csv').",
                required=False,
                default="*",
            ),
            ConnectorParam(
                name="recursive",
                type="boolean",
                description="Recurse into subdirectories.",
                required=False,
                default=False,
            ),
        ],
        related_pipelines=[
            "convert-folder",
            "analyze-batch-from-disk",
        ],
        related_helpers=[],
    ),
}


# ---------------------------------------------------------------------------
# DB-First loading helpers (T-BRIX-DB-06)
# ---------------------------------------------------------------------------

def _row_to_connector(row: dict) -> SourceConnector:
    """Convert a connector_definitions DB row to a SourceConnector instance."""

    def _load_json(value, default):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return default
        return value if value is not None else default

    raw_params = _load_json(row.get("parameters", "[]"), [])
    parameters = [
        ConnectorParam(
            name=p["name"],
            type=p.get("type", "string"),
            description=p.get("description", ""),
            required=bool(p.get("required", False)),
            default=p.get("default"),
        )
        for p in raw_params
    ]

    required_mcp = row.get("required_mcp_server") or None
    if required_mcp == "":
        required_mcp = None

    return SourceConnector(
        name=row["name"],
        type=row.get("type", ""),
        description=row.get("description", ""),
        required_mcp_server=required_mcp,
        required_mcp_tools=_load_json(row.get("required_mcp_tools", "[]"), []),
        output_schema=_load_json(row.get("output_schema", "{}"), {}),
        parameters=parameters,
        related_pipelines=_load_json(row.get("related_pipelines", "[]"), []),
        related_helpers=_load_json(row.get("related_helpers", "[]"), []),
    )


def _load_connector_registry_from_db() -> Optional[dict[str, SourceConnector]]:
    """Try to load all connectors from the DB. Returns None if DB empty or unavailable."""
    try:
        db = BrixDB()
        rows = db.connector_definitions_list()
        if not rows:
            return None
        result: dict[str, SourceConnector] = {}
        for row in rows:
            try:
                connector = _row_to_connector(row)
                result[connector.name] = connector
            except Exception as e:
                logger.warning("Could not load connector '%s' from DB: %s", row.get("name"), e)
        return result if result else None
    except Exception as e:
        logger.debug("Could not load connectors from DB: %s", e)
        return None


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _get_registry() -> dict[str, SourceConnector]:
    """Return the connector registry — from DB if available, else from code."""
    db_registry = _load_connector_registry_from_db()
    if db_registry is not None:
        return db_registry
    return CONNECTOR_REGISTRY


def get_connector(name: str) -> Optional[SourceConnector]:
    """Return a connector by name, or None if not found."""
    return _get_registry().get(name)


def list_connectors(type_filter: Optional[str] = None) -> list[SourceConnector]:
    """Return all connectors, optionally filtered by type."""
    connectors = list(_get_registry().values())
    if type_filter:
        connectors = [c for c in connectors if c.type == type_filter]
    return connectors


def connector_status(name: str) -> dict:
    """Check whether a connector's dependencies are available.

    Returns a dict with:
        - found: bool — connector exists in registry
        - mcp_server_available: bool | None — None if no MCP required
        - missing_env_vars: list[str] — required credential env vars not set
        - status: "ready" | "missing_mcp" | "missing_credentials" | "not_found"
        - message: str — human-readable status summary
    """
    connector = _get_registry().get(name)
    if connector is None:
        return {
            "found": False,
            "mcp_server_available": None,
            "missing_env_vars": [],
            "status": "not_found",
            "message": f"Connector '{name}' not found in registry.",
        }

    # Check MCP server availability
    mcp_available: Optional[bool] = None
    if connector.required_mcp_server is not None:
        # Try to detect via registered brix MCP servers
        try:
            db = BrixDB()
            servers = db.server_list()
            server_names = [s.get("name", "") for s in servers]
            mcp_available = connector.required_mcp_server in server_names
        except Exception:
            # If DB is not available, mark as unknown (None)
            mcp_available = None

    # Check for missing required credential env vars
    missing_env: list[str] = []
    for param in connector.parameters:
        if param.required:
            # Map common credential params to conventional env var names
            env_var = f"{name.upper()}_{param.name.upper()}"
            if not os.environ.get(env_var):
                missing_env.append(env_var)

    # Determine overall status
    if connector.required_mcp_server is not None and mcp_available is False:
        status = "missing_mcp"
        message = (
            f"Connector '{name}' requires MCP server '{connector.required_mcp_server}' "
            f"which is not registered. Register it via brix__server_add."
        )
    elif missing_env:
        status = "missing_credentials"
        message = (
            f"Connector '{name}' requires credentials but the following "
            f"environment variables are not set: {', '.join(missing_env)}."
        )
    else:
        status = "ready"
        message = f"Connector '{name}' appears ready."
        if mcp_available is None and connector.required_mcp_server:
            status = "ready"
            message = (
                f"Connector '{name}' requires MCP server '{connector.required_mcp_server}'. "
                f"MCP availability could not be verified (DB unavailable)."
            )

    return {
        "found": True,
        "mcp_server_available": mcp_available,
        "missing_env_vars": missing_env,
        "status": status,
        "message": message,
    }
