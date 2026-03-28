"""Source runner — fetches data from configured connectors (T-BRIX-DB-05).

Supported connectors:
- local_files  — scan local filesystem via glob
- outlook      — Microsoft Outlook / Exchange via M365 MCP

Not yet implemented (raise NotImplementedError):
- gmail, onedrive, paypal, sparkasse
"""
from __future__ import annotations

import fnmatch
import os
import time
from pathlib import Path
from typing import Any

from brix.connectors import CONNECTOR_REGISTRY, NormalizedItem
from brix.runners.base import BaseRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_to_dict(item: NormalizedItem) -> dict:
    """Convert a NormalizedItem to a plain dict."""
    return item.model_dump()


def _build_local_file_item(file_path: Path, base_path: Path) -> dict:
    """Build a NormalizedItem dict from a local file path."""
    stat = file_path.stat()
    import datetime

    ts = datetime.datetime.fromtimestamp(stat.st_mtime, tz=datetime.timezone.utc).isoformat()
    relative = str(file_path.relative_to(base_path))
    item = NormalizedItem(
        source="local_files",
        source_type="file_storage",
        item_id=str(file_path),
        title=file_path.name,
        content=None,
        metadata={
            "path": str(file_path),
            "relative_path": relative,
            "size": stat.st_size,
            "extension": file_path.suffix.lstrip("."),
        },
        attachments=[],
        timestamp=ts,
        raw={"path": str(file_path), "stat": {"size": stat.st_size, "mtime": stat.st_mtime}},
    )
    return _normalize_to_dict(item)


def _normalize_outlook_message(msg: dict) -> dict:
    """Normalize a raw Outlook message dict into NormalizedItem dict format."""
    item_id = msg.get("id", "")
    subject = msg.get("subject") or "(no subject)"
    received = msg.get("receivedDateTime") or msg.get("sentDateTime")
    sender = msg.get("from", {})
    sender_address = ""
    if isinstance(sender, dict):
        email_addr = sender.get("emailAddress", {})
        if isinstance(email_addr, dict):
            sender_address = email_addr.get("address", "")

    body_content = None
    body = msg.get("body", {})
    if isinstance(body, dict):
        body_content = body.get("content")

    metadata = {
        "from": sender_address,
        "to": msg.get("toRecipients", []),
        "isRead": msg.get("isRead", False),
        "hasAttachments": msg.get("hasAttachments", False),
        "importance": msg.get("importance", "normal"),
        "categories": msg.get("categories", []),
    }

    attachments = []
    if msg.get("hasAttachments"):
        attachments = [{"note": "fetch via list-mail-attachments", "messageId": item_id}]

    item = NormalizedItem(
        source="outlook",
        source_type="email",
        item_id=item_id,
        title=subject,
        content=body_content,
        metadata=metadata,
        attachments=attachments,
        timestamp=received,
        raw=msg,
    )
    return _normalize_to_dict(item)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class SourceRunner(BaseRunner):
    """Fetches data from a named connector and returns a list of NormalizedItem dicts.

    Config keys:
    - connector (str, required): Connector name from CONNECTOR_REGISTRY
    - Connector-specific params: see CONNECTOR_REGISTRY for each connector's parameters
    """

    # Injected in tests to mock the MCP call
    _mcp_caller: Any = None

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "connector": {
                    "type": "string",
                    "description": "Connector name (e.g. 'outlook', 'local_files')",
                },
                # local_files params
                "path": {
                    "type": "string",
                    "description": "[local_files] Absolute path to the directory to scan.",
                },
                "pattern": {
                    "type": "string",
                    "description": "[local_files] Glob pattern to filter files (e.g. '*.pdf').",
                },
                "recursive": {
                    "type": "boolean",
                    "description": "[local_files/onedrive] Recurse into subdirectories.",
                },
                # outlook params
                "folder": {
                    "type": "string",
                    "description": "[outlook] Mail folder to read from (default 'INBOX').",
                },
                "filter": {
                    "type": "string",
                    "description": "[outlook] OData filter expression.",
                },
                "limit": {
                    "type": "integer",
                    "description": "[outlook] Max messages to return (default 50).",
                },
            },
            "required": ["connector"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "list[dict]"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        config = self._extract_config(step)
        connector_name = config.get("connector")

        if not connector_name:
            self.report_progress(0.0, "missing connector")
            return {
                "success": False,
                "error": "Missing required config field: 'connector'",
                "duration": time.monotonic() - start,
            }

        # Check connector exists in registry
        connector = CONNECTOR_REGISTRY.get(connector_name)
        if connector is None:
            self.report_progress(0.0, f"unknown connector: {connector_name}")
            return {
                "success": False,
                "error": (
                    f"Unknown connector '{connector_name}'. "
                    f"Available: {', '.join(sorted(CONNECTOR_REGISTRY.keys()))}"
                ),
                "duration": time.monotonic() - start,
            }

        # Dispatch to connector implementation
        try:
            if connector_name == "local_files":
                items = await self._execute_local_files(config, start)
            elif connector_name == "outlook":
                items = await self._execute_outlook(config, start)
            else:
                raise NotImplementedError(
                    f"Connector '{connector_name}' (type={connector.type}) is not yet implemented "
                    f"in SourceRunner. Supported connectors: local_files, outlook. "
                    f"To use {connector_name}, implement _execute_{connector_name}() "
                    f"in brix/runners/source.py."
                )
        except NotImplementedError as e:
            self.report_progress(0.0, f"not implemented: {connector_name}")
            return {
                "success": False,
                "error": str(e),
                "duration": time.monotonic() - start,
            }
        except Exception as e:
            self.report_progress(0.0, f"error: {e}")
            return {
                "success": False,
                "error": str(e),
                "duration": time.monotonic() - start,
            }

        duration = time.monotonic() - start
        self.report_progress(100.0, "done", done=len(items), total=len(items))
        return {"success": True, "data": items, "duration": duration}

    # ------------------------------------------------------------------
    # local_files
    # ------------------------------------------------------------------

    async def _execute_local_files(self, config: dict, start: float) -> list[dict]:
        """Scan local filesystem and return NormalizedItem dicts."""
        path_str = config.get("path")
        if not path_str:
            raise ValueError("Missing required config field: 'path' for local_files connector")

        base_path = Path(path_str)
        if not base_path.exists():
            raise ValueError(f"Path does not exist: {path_str}")
        if not base_path.is_dir():
            raise ValueError(f"Path is not a directory: {path_str}")

        pattern = config.get("pattern", "*")
        recursive = config.get("recursive", False)

        self.report_progress(10.0, f"scanning {path_str}")

        files: list[Path] = []
        if recursive:
            for root, _dirs, filenames in os.walk(base_path):
                for fname in filenames:
                    fpath = Path(root) / fname
                    if fnmatch.fnmatch(fname, pattern):
                        files.append(fpath)
        else:
            for entry in base_path.iterdir():
                if entry.is_file() and fnmatch.fnmatch(entry.name, pattern):
                    files.append(entry)

        self.report_progress(50.0, f"found {len(files)} files", done=0, total=len(files))

        items = []
        for i, fpath in enumerate(sorted(files), 1):
            items.append(_build_local_file_item(fpath, base_path))
            if i % 10 == 0:
                pct = 50.0 + 50.0 * i / max(len(files), 1)
                self.report_progress(pct, f"processed {i}/{len(files)}", done=i, total=len(files))

        return items

    # ------------------------------------------------------------------
    # outlook
    # ------------------------------------------------------------------

    async def _execute_outlook(self, config: dict, start: float) -> list[dict]:
        """Fetch messages from Outlook via M365 MCP and return NormalizedItem dicts."""
        folder = config.get("folder", "INBOX")
        odata_filter = config.get("filter")
        limit = int(config.get("limit", 50))

        self.report_progress(10.0, f"fetching from outlook folder={folder}")

        # Build MCP call arguments for m365 list-mail-messages
        mcp_args: dict = {
            "folderId": folder,
            "top": limit,
        }
        if odata_filter:
            mcp_args["filter"] = odata_filter

        # Execute MCP call — either via injected mock or real MCP runner
        raw_messages = await self._call_mcp_list_mail_messages(mcp_args)

        self.report_progress(60.0, f"received {len(raw_messages)} messages", done=0, total=len(raw_messages))

        items = []
        for i, msg in enumerate(raw_messages, 1):
            items.append(_normalize_outlook_message(msg))
            if i % 10 == 0:
                pct = 60.0 + 40.0 * i / max(len(raw_messages), 1)
                self.report_progress(pct, f"normalized {i}/{len(raw_messages)}", done=i, total=len(raw_messages))

        return items

    async def _call_mcp_list_mail_messages(self, args: dict) -> list[dict]:
        """Call the M365 list-mail-messages MCP tool.

        In tests this method is replaced by a mock. In production it would
        be called via the MCP runner or brix server infrastructure.
        """
        if self._mcp_caller is not None:
            result = await self._mcp_caller(args)
            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                return result.get("value", [])
            return []

        # Fallback: no MCP caller configured — return empty list with a warning
        import warnings
        warnings.warn(
            "SourceRunner: no MCP caller configured for outlook connector. "
            "Inject _mcp_caller for testing or configure MCP server for production.",
            stacklevel=3,
        )
        return []

    # ------------------------------------------------------------------
    # Config extraction helper
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_config(step: Any) -> dict:
        """Extract config dict from step (supports object with attributes or plain dict)."""
        if isinstance(step, dict):
            return step
        # Step object — collect all non-private, non-callable attributes
        config = {}
        for attr in ("connector", "path", "pattern", "recursive", "folder", "filter", "limit"):
            val = getattr(step, attr, None)
            if val is not None:
                config[attr] = val
        # Also try step.config or step.params if present
        extra = getattr(step, "config", None) or getattr(step, "params", None)
        if isinstance(extra, dict):
            for k, v in extra.items():
                if k not in config:
                    config[k] = v
        return config
