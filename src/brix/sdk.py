"""Brix SDK for helper scripts — synchronous MCP calls (T-BRIX-V4-14)."""
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import yaml  # PyYAML
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

SERVERS_CONFIG = Path.home() / ".brix" / "servers.yaml"


class McpClient:
    """Synchronous MCP client for use in helper scripts.

    Usage::

        from brix.sdk import mcp

        result = mcp.call("m365", "list-mail-messages", {"folder": "Inbox"})
        if result["success"]:
            messages = result["data"]

    The client reads server configuration from ``~/.brix/servers.yaml`` (same
    file that the Brix MCP server uses) and spawns the server process directly
    via ``subprocess``.  Each :meth:`call` is a blocking operation — suitable
    for use inside synchronous helper scripts.
    """

    def __init__(self, config_path: Path = None):
        self._config_path = config_path or SERVERS_CONFIG
        self._servers: dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load server configuration from YAML file."""
        if not self._config_path.exists():
            return
        if not _HAS_YAML:
            # Fallback: try json-style parsing (won't work for real YAML, but
            # avoids a hard crash when PyYAML is not installed)
            return
        try:
            with open(self._config_path) as f:
                data = yaml.safe_load(f) or {}
            self._servers = data.get("servers", {})
        except Exception:
            self._servers = {}

    def call(self, server_name: str, tool_name: str, params: dict = None) -> dict:
        """Call an MCP tool synchronously.

        Returns a dict with keys:
        - ``success`` (bool)
        - ``data`` (Any) — parsed tool result on success
        - ``error`` (str) — error message on failure
        """
        if not self._servers:
            if not self._config_path.exists():
                return {
                    "success": False,
                    "error": f"Servers config not found: {self._config_path}",
                }
            return {
                "success": False,
                "error": "No servers configured (check ~/.brix/servers.yaml)",
            }

        if server_name not in self._servers:
            return {
                "success": False,
                "error": f"Server '{server_name}' not found in config. "
                         f"Available: {', '.join(self._servers.keys())}",
            }

        config = self._servers[server_name]
        command = config.get("command", "")
        args = config.get("args", [])

        if not command:
            return {"success": False, "error": f"Server '{server_name}' has no command configured"}

        # JSON-RPC 2.0 — initialize then call
        init_msg = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "brix-sdk", "version": "1.0"},
            },
        })
        call_msg = json.dumps({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": params or {}},
        })

        stdin_data = init_msg + "\n" + call_msg + "\n"

        try:
            proc = subprocess.run(
                [command] + list(args),
                input=stdin_data,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "MCP call timed out (60s)"}
        except FileNotFoundError:
            return {"success": False, "error": f"Command not found: {command}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

        # Parse responses line by line — find the tool-call response (id=2)
        for line in proc.stdout.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                resp = json.loads(line)
            except json.JSONDecodeError:
                continue

            if resp.get("id") != 2:
                continue

            # Check for JSON-RPC error
            if "error" in resp:
                return {"success": False, "error": str(resp["error"])}

            result = resp.get("result", {})
            content = result.get("content", [])
            texts = [c.get("text", "") for c in content if c.get("type") == "text"]
            combined = "\n".join(texts)

            try:
                data = json.loads(combined)
            except (json.JSONDecodeError, ValueError):
                data = combined

            return {"success": True, "data": data}

        # No tool-call response found
        stderr_hint = proc.stderr.strip()[:200] if proc.stderr else ""
        return {
            "success": False,
            "error": f"No valid response from MCP server '{server_name}'"
                     + (f": {stderr_hint}" if stderr_hint else ""),
        }


# Module-level convenience instance — works out-of-the-box when
# ~/.brix/servers.yaml exists.
mcp = McpClient()


def is_cancelled() -> bool:
    """Return True if the current run has been cancelled.

    Checks for the ``cancel_requested.json`` sentinel file in the run workdir.
    The workdir is determined via the ``BRIX_RUN_WORKDIR`` environment variable
    which the Brix engine injects into every Python helper subprocess.

    Helper scripts can call this inside long-running loops to stop gracefully::

        from brix.sdk import is_cancelled

        for item in items:
            if is_cancelled():
                break
            process(item)
    """
    workdir = os.environ.get("BRIX_RUN_WORKDIR", "")
    if not workdir:
        return False
    sentinel = Path(workdir) / "cancel_requested.json"
    return sentinel.exists()


def check_cancellation() -> None:
    """Raise ``asyncio.CancelledError`` (or ``SystemExit(130)``) if cancelled.

    Designed for use inside helper scripts — call at checkpoints to abort
    cleanly when ``cancel_run`` has been invoked::

        from brix.sdk import check_cancellation

        for item in items:
            check_cancellation()
            process(item)

    Raises:
        SystemExit(130): when the cancel sentinel is present.
    """
    if is_cancelled():
        print("BRIX: run cancelled — stopping helper", file=sys.stderr, flush=True)
        sys.exit(130)


def progress(processed: int, total: int, message: str = None) -> None:
    """Emit intra-step progress to stderr for the Brix runner to capture.

    Helper scripts call this to report live progress while running inside a
    ``progress: true`` pipeline step.  Brix parses the ``BRIX_PROGRESS:``
    lines from stderr and stores the latest value in the run state so it
    can be read via ``brix__get_run_status``.

    Usage::

        from brix.sdk import progress

        for i, item in enumerate(items):
            process(item)
            progress(i + 1, len(items), f"Processed {item['name']}")

    Args:
        processed: Number of items processed so far.
        total: Total number of items to process.
        message: Optional human-readable status message.
    """
    payload: dict = {"processed": processed, "total": total}
    if message is not None:
        payload["message"] = message
    print(f"BRIX_PROGRESS: {json.dumps(payload)}", file=sys.stderr, flush=True)
