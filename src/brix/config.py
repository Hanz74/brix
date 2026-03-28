"""Brix central configuration — all tuneable values read from environment.

Usage::

    from brix.config import config

    port = config.MCP_HTTP_PORT
    timeout = config.MCP_POOL_CALL_TIMEOUT

Every value has a safe default so Brix works out-of-the-box without any
environment variables set.  Set the corresponding ``BRIX_*`` env var to
override a default.
"""

from __future__ import annotations

import os


class BrixConfig:
    """All configurable Brix values.  Reads from environment on each instantiation.

    Instance attributes are set in ``__init__`` by reading ``os.environ`` at
    construction time.  This means that patching ``os.environ`` in tests
    and then constructing a fresh ``BrixConfig()`` (or calling
    ``BrixConfig.reload()``) will pick up the overrides correctly.

    The module-level ``config`` singleton is created once at import time.
    """

    def __init__(self) -> None:
        # -----------------------------------------------------------------------
        # Ports & Hosts
        # -----------------------------------------------------------------------

        #: Port for the MCP HTTP server (``brix mcp --transport http``)
        self.MCP_HTTP_PORT: int = int(os.environ.get("BRIX_MCP_HTTP_PORT", "8091"))

        #: Port for the REST API server (``brix api``)
        self.API_PORT: int = int(os.environ.get("BRIX_API_PORT", "8090"))

        #: Bind host for the MCP HTTP server
        self.MCP_HOST: str = os.environ.get("BRIX_MCP_HOST", "0.0.0.0")

        #: Bind host for the REST API server
        self.API_HOST: str = os.environ.get("BRIX_API_HOST", "0.0.0.0")

        # -----------------------------------------------------------------------
        # Paths
        # -----------------------------------------------------------------------

        #: Container path that holds pipeline YAMLs shipped with the image
        self.CONTAINER_PIPELINES_DIR: str = os.environ.get(
            "BRIX_CONTAINER_PIPELINES_DIR", "/app/pipelines"
        )

        #: Legacy helper-scripts directory (container path, deprecated)
        self.LEGACY_HELPERS_DIR: str = os.environ.get(
            "BRIX_LEGACY_HELPERS_DIR", "/app/helpers"
        )

        # -----------------------------------------------------------------------
        # Timeouts (seconds)
        # -----------------------------------------------------------------------

        #: Default timeout for individual MCP tool calls via the connection pool
        self.MCP_POOL_CALL_TIMEOUT: float = float(
            os.environ.get("BRIX_MCP_POOL_CALL_TIMEOUT", "60")
        )

        #: Fallback default timeout for step types not listed in the per-type table
        self.TIMEOUT_DEFAULT: float = float(
            os.environ.get("BRIX_TIMEOUT_DEFAULT", "600")
        )

        #: Default timeout for ``python`` steps
        self.TIMEOUT_PYTHON: float = float(
            os.environ.get("BRIX_TIMEOUT_PYTHON", "3600")
        )

        #: Default timeout for ``cli`` steps
        self.TIMEOUT_CLI: float = float(os.environ.get("BRIX_TIMEOUT_CLI", "300"))

        #: Default timeout for ``mcp`` steps
        self.TIMEOUT_MCP: float = float(os.environ.get("BRIX_TIMEOUT_MCP", "120"))

        #: Default timeout for ``http`` steps
        self.TIMEOUT_HTTP: float = float(os.environ.get("BRIX_TIMEOUT_HTTP", "60"))

        #: Default timeout for ``repeat`` steps
        self.TIMEOUT_REPEAT: float = float(
            os.environ.get("BRIX_TIMEOUT_REPEAT", "7200")
        )

        #: Default timeout for ``approval`` steps (waits for human input)
        self.TIMEOUT_APPROVAL: float = float(
            os.environ.get("BRIX_TIMEOUT_APPROVAL", "86400")
        )

        #: Timeout for Mattermost webhook POSTs (alerting & run notifications)
        self.MATTERMOST_WEBHOOK_TIMEOUT: float = float(
            os.environ.get("BRIX_MATTERMOST_WEBHOOK_TIMEOUT", "10")
        )

        #: Timeout for HTTP poll trigger requests (triggers/runners.py)
        self.HTTP_POLL_TIMEOUT: float = float(
            os.environ.get("BRIX_HTTP_POLL_TIMEOUT", "30")
        )

        #: Auto-kill background runs whose heartbeat is older than this many seconds
        self.BACKGROUND_RUN_TIMEOUT_SECONDS: int = int(
            os.environ.get("BRIX_BACKGROUND_RUN_TIMEOUT_SECONDS", "1800")  # 30 min
        )

        #: Watchdog task check interval in seconds
        self.WATCHDOG_INTERVAL_SECONDS: int = int(
            os.environ.get("BRIX_WATCHDOG_INTERVAL_SECONDS", "60")
        )

        #: Retention loop interval in seconds (runs once per day inside scheduler)
        self.RETENTION_LOOP_INTERVAL_SECONDS: int = int(
            os.environ.get("BRIX_RETENTION_LOOP_INTERVAL_SECONDS", "86400")
        )

        #: Schema consultation TTL in seconds
        self.SCHEMA_CONSULTATION_TTL_SECONDS: int = int(
            os.environ.get("BRIX_SCHEMA_CONSULTATION_TTL_SECONDS", "1800")  # 30 min
        )

        # -----------------------------------------------------------------------
        # Idempotency
        # -----------------------------------------------------------------------

        #: TTL in seconds for idempotency keys stored in the API (24 hours)
        self.IDEMPOTENCY_TTL: int = int(
            os.environ.get("BRIX_IDEMPOTENCY_TTL", "86400")
        )

        # -----------------------------------------------------------------------
        # Output / Memory limits
        # -----------------------------------------------------------------------

        #: Number of items above which a step output is spilled to JSONL on disk
        self.LARGE_OUTPUT_THRESHOLD: int = int(
            os.environ.get("BRIX_LARGE_OUTPUT_THRESHOLD", "100")
        )

        #: Byte size (in MB) above which a step output is spilled to JSONL on disk
        self.LARGE_OUTPUT_SIZE_MB: int = int(
            os.environ.get("BRIX_LARGE_OUTPUT_SIZE_MB", "10")
        )

        # -----------------------------------------------------------------------
        # History / DB
        # -----------------------------------------------------------------------

        #: Maximum number of history rows returned when checking for pipeline run history
        self.HISTORY_LIST_LIMIT: int = int(
            os.environ.get("BRIX_HISTORY_LIST_LIMIT", "1000")
        )

        # -----------------------------------------------------------------------
        # Triggers
        # -----------------------------------------------------------------------

        #: Default IMAP server for mail triggers
        self.IMAP_DEFAULT_SERVER: str = os.environ.get(
            "BRIX_IMAP_DEFAULT_SERVER", "imap.gmail.com"
        )

        #: Default polling interval for triggers
        self.TRIGGER_DEFAULT_INTERVAL: str = os.environ.get(
            "BRIX_TRIGGER_DEFAULT_INTERVAL", "5m"
        )

        # -----------------------------------------------------------------------
        # SSE streaming (api.py)
        # -----------------------------------------------------------------------

        #: How often the SSE stream polls for status updates (seconds)
        self.SSE_POLL_INTERVAL: float = float(
            os.environ.get("BRIX_SSE_POLL_INTERVAL", "1.0")
        )

        #: Maximum duration for an SSE stream before sending a timeout event (seconds)
        self.SSE_TIMEOUT: float = float(os.environ.get("BRIX_SSE_TIMEOUT", "3600.0"))

    # -----------------------------------------------------------------------
    # Derived helpers
    # -----------------------------------------------------------------------

    @property
    def large_output_size_bytes(self) -> int:
        """``LARGE_OUTPUT_SIZE_MB`` expressed in bytes."""
        return self.LARGE_OUTPUT_SIZE_MB * 1024 * 1024

    # -----------------------------------------------------------------------
    # Reload helper (useful in tests)
    # -----------------------------------------------------------------------

    @classmethod
    def reload(cls) -> "BrixConfig":
        """Return a fresh BrixConfig instance that re-reads the environment.

        Replaces the module-level ``config`` singleton in-place::

            import brix.config as _cfg
            os.environ["BRIX_API_PORT"] = "9999"
            _cfg.config = BrixConfig.reload()
        """
        return cls()


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

config = BrixConfig()
