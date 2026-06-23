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

        #: Base URL of the Daigestr conversion / extraction service
        self.DAIGESTR_URL: str = os.environ.get(
            "BRIX_DAIGESTR_URL", "http://daigestr:8081"
        )

        #: Default Daigestr endpoint for conversion-style requests
        self.DAIGESTR_CONVERT_ENDPOINT: str = os.environ.get(
            "BRIX_DAIGESTR_CONVERT_ENDPOINT", "/v1/convert"
        )

        #: Async Daigestr start endpoint for pollable conversion jobs
        self.DAIGESTR_ASYNC_CONVERT_ENDPOINT: str = os.environ.get(
            "BRIX_DAIGESTR_ASYNC_CONVERT_ENDPOINT", "/v1/convert/async"
        )

        #: Whether Brix should prefer Daigestr async jobs when available
        self.DAIGESTR_USE_ASYNC_JOBS: bool = os.environ.get(
            "BRIX_DAIGESTR_USE_ASYNC_JOBS", "true"
        ).strip().lower() in {"1", "true", "yes", "on"}

        #: Daigestr job status endpoint template
        self.DAIGESTR_JOB_STATUS_ENDPOINT_TEMPLATE: str = os.environ.get(
            "BRIX_DAIGESTR_JOB_STATUS_ENDPOINT_TEMPLATE", "/v1/jobs/{job_id}"
        )

        #: Daigestr job result endpoint template
        self.DAIGESTR_JOB_RESULT_ENDPOINT_TEMPLATE: str = os.environ.get(
            "BRIX_DAIGESTR_JOB_RESULT_ENDPOINT_TEMPLATE", "/v1/jobs/{job_id}/result"
        )

        #: Polling interval for Daigestr async job progress checks
        self.DAIGESTR_JOB_POLL_INTERVAL_SECONDS: float = float(
            os.environ.get("BRIX_DAIGESTR_JOB_POLL_INTERVAL_SECONDS", "2")
        )

        #: Default Daigestr endpoint for extraction-style requests
        self.DAIGESTR_EXTRACT_ENDPOINT: str = os.environ.get(
            "BRIX_DAIGESTR_EXTRACT_ENDPOINT", "/v1/extract"
        )

        #: Daigestr health endpoint used for capability/version handshakes
        self.DAIGESTR_HEALTH_ENDPOINT: str = os.environ.get(
            "BRIX_DAIGESTR_HEALTH_ENDPOINT", "/v1/health"
        )

        #: Daigestr tips/contract endpoint used for capability handshakes
        self.DAIGESTR_TIPS_ENDPOINT: str = os.environ.get(
            "BRIX_DAIGESTR_TIPS_ENDPOINT", "/v1/tips"
        )

        #: Default named DB connection for document persistence flows
        self.DEFAULT_DOCUMENT_CONNECTION: str = os.environ.get(
            "BRIX_DEFAULT_DOCUMENT_CONNECTION", "buddy-db"
        )

        #: Default Daigestr mode for document extraction requests
        self.DAIGESTR_MODE: str = os.environ.get(
            "BRIX_DAIGESTR_MODE", "default"
        )

        #: Whether low-quality Daigestr results should be retried automatically
        self.DAIGESTR_RETRY_ON_LOW_QUALITY: bool = os.environ.get(
            "BRIX_DAIGESTR_RETRY_ON_LOW_QUALITY", "true"
        ).strip().lower() in {"1", "true", "yes", "on"}

        #: Quality threshold below which Daigestr should retry with a stronger mode
        self.DAIGESTR_QUALITY_RETRY_THRESHOLD: float = float(
            os.environ.get("BRIX_DAIGESTR_QUALITY_RETRY_THRESHOLD", "0.75")
        )

        #: Retry mode used when low-quality fallback is enabled
        self.DAIGESTR_QUALITY_RETRY_MODE: str = os.environ.get(
            "BRIX_DAIGESTR_QUALITY_RETRY_MODE", "full"
        )

        # -----------------------------------------------------------------------
        # Timeouts (seconds)
        # -----------------------------------------------------------------------

        #: Universal default timeout (12 hours).  All per-type defaults
        #: fall back to this value when their own env var is unset or 0.
        self.BRIX_DEFAULT_TIMEOUT: float = float(
            os.environ.get("BRIX_DEFAULT_TIMEOUT", "43200")
        )

        #: Default timeout for individual MCP tool calls via the connection pool
        self.MCP_POOL_CALL_TIMEOUT: float = float(
            os.environ.get("BRIX_MCP_POOL_CALL_TIMEOUT", "60")
        )

        #: Fallback default timeout for step types not listed in the per-type table
        self.TIMEOUT_DEFAULT: float = (
            float(os.environ.get("BRIX_TIMEOUT_DEFAULT", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
        )

        #: Default timeout for ``python`` / ``cli`` script steps
        self.TIMEOUT_SCRIPT: float = (
            float(os.environ.get("BRIX_SCRIPT_TIMEOUT", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
        )

        #: Default timeout for ``python`` steps
        self.TIMEOUT_PYTHON: float = (
            float(os.environ.get("BRIX_TIMEOUT_PYTHON", "0"))
            or self.TIMEOUT_SCRIPT
        )

        #: Default timeout for ``cli`` steps
        self.TIMEOUT_CLI: float = (
            float(os.environ.get("BRIX_TIMEOUT_CLI", "0"))
            or self.TIMEOUT_SCRIPT
        )

        #: Default timeout for ``mcp`` steps
        self.TIMEOUT_MCP: float = (
            float(os.environ.get("BRIX_MCP_TIMEOUT", "0"))
            or float(os.environ.get("BRIX_TIMEOUT_MCP", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
        )

        #: Default timeout for ``http`` steps
        self.TIMEOUT_HTTP: float = (
            float(os.environ.get("BRIX_HTTP_TIMEOUT", "0"))
            or float(os.environ.get("BRIX_TIMEOUT_HTTP", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
        )

        #: Default timeout for ``db_query`` / ``db_upsert`` steps
        self.TIMEOUT_DB: float = (
            float(os.environ.get("BRIX_DB_TIMEOUT", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
        )

        #: Default timeout for ``llm_batch`` steps
        self.TIMEOUT_LLM: float = (
            float(os.environ.get("BRIX_LLM_TIMEOUT", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
        )

        #: Default timeout for ``repeat`` steps
        self.TIMEOUT_REPEAT: float = (
            float(os.environ.get("BRIX_TIMEOUT_REPEAT", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
        )

        #: Default timeout for ``approval`` steps (waits for human input)
        self.TIMEOUT_APPROVAL: float = (
            float(os.environ.get("BRIX_TIMEOUT_APPROVAL", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
        )

        #: Default timeout for ``markitdown`` steps
        self.TIMEOUT_MARKITDOWN: float = (
            float(os.environ.get("BRIX_TIMEOUT_MARKITDOWN", "0"))
            or self.BRIX_DEFAULT_TIMEOUT
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

        #: Maximum number of rows to keep in app_log (oldest trimmed when exceeded)
        self.BRIX_MAX_LOG_ROWS: int = int(
            os.environ.get("BRIX_MAX_LOG_ROWS", "50000")
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
