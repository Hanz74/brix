"""Tests for BrixConfig — defaults, ENV overrides, derived properties."""
import os
import importlib
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_config(env_overrides: dict | None = None):
    """Return a fresh BrixConfig instance with optional env overrides applied.

    Temporarily patches os.environ, re-evaluates the class attributes, and
    restores the environment afterwards.
    """
    from brix.config import BrixConfig

    original = {}
    try:
        for k, v in (env_overrides or {}).items():
            original[k] = os.environ.get(k)
            os.environ[k] = str(v)
        return BrixConfig()
    finally:
        for k, orig in original.items():
            if orig is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = orig


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------

class TestBrixConfigDefaults:
    def test_mcp_http_port_default(self):
        cfg = _fresh_config()
        assert cfg.MCP_HTTP_PORT == 8091

    def test_api_port_default(self):
        cfg = _fresh_config()
        assert cfg.API_PORT == 8090

    def test_mcp_host_default(self):
        cfg = _fresh_config()
        assert cfg.MCP_HOST == "0.0.0.0"

    def test_api_host_default(self):
        cfg = _fresh_config()
        assert cfg.API_HOST == "0.0.0.0"

    def test_container_pipelines_dir_default(self):
        cfg = _fresh_config()
        assert cfg.CONTAINER_PIPELINES_DIR == "/app/pipelines"

    def test_legacy_helpers_dir_default(self):
        cfg = _fresh_config()
        assert cfg.LEGACY_HELPERS_DIR == "/app/helpers"

    def test_mcp_pool_call_timeout_default(self):
        cfg = _fresh_config()
        assert cfg.MCP_POOL_CALL_TIMEOUT == 60.0

    def test_timeout_default(self):
        cfg = _fresh_config()
        assert cfg.TIMEOUT_DEFAULT == 600.0

    def test_timeout_python_default(self):
        cfg = _fresh_config()
        assert cfg.TIMEOUT_PYTHON == 3600.0

    def test_timeout_cli_default(self):
        cfg = _fresh_config()
        assert cfg.TIMEOUT_CLI == 300.0

    def test_timeout_mcp_default(self):
        cfg = _fresh_config()
        assert cfg.TIMEOUT_MCP == 120.0

    def test_timeout_http_default(self):
        cfg = _fresh_config()
        assert cfg.TIMEOUT_HTTP == 60.0

    def test_timeout_repeat_default(self):
        cfg = _fresh_config()
        assert cfg.TIMEOUT_REPEAT == 7200.0

    def test_timeout_approval_default(self):
        cfg = _fresh_config()
        assert cfg.TIMEOUT_APPROVAL == 86400.0

    def test_mattermost_webhook_timeout_default(self):
        cfg = _fresh_config()
        assert cfg.MATTERMOST_WEBHOOK_TIMEOUT == 10.0

    def test_http_poll_timeout_default(self):
        cfg = _fresh_config()
        assert cfg.HTTP_POLL_TIMEOUT == 30.0

    def test_background_run_timeout_default(self):
        cfg = _fresh_config()
        assert cfg.BACKGROUND_RUN_TIMEOUT_SECONDS == 1800

    def test_watchdog_interval_default(self):
        cfg = _fresh_config()
        assert cfg.WATCHDOG_INTERVAL_SECONDS == 60

    def test_retention_loop_interval_default(self):
        cfg = _fresh_config()
        assert cfg.RETENTION_LOOP_INTERVAL_SECONDS == 86400

    def test_schema_consultation_ttl_default(self):
        cfg = _fresh_config()
        assert cfg.SCHEMA_CONSULTATION_TTL_SECONDS == 1800

    def test_idempotency_ttl_default(self):
        cfg = _fresh_config()
        assert cfg.IDEMPOTENCY_TTL == 86400

    def test_large_output_threshold_default(self):
        cfg = _fresh_config()
        assert cfg.LARGE_OUTPUT_THRESHOLD == 100

    def test_large_output_size_mb_default(self):
        cfg = _fresh_config()
        assert cfg.LARGE_OUTPUT_SIZE_MB == 10

    def test_history_list_limit_default(self):
        cfg = _fresh_config()
        assert cfg.HISTORY_LIST_LIMIT == 1000

    def test_imap_default_server_default(self):
        cfg = _fresh_config()
        assert cfg.IMAP_DEFAULT_SERVER == "imap.gmail.com"

    def test_trigger_default_interval_default(self):
        cfg = _fresh_config()
        assert cfg.TRIGGER_DEFAULT_INTERVAL == "5m"

    def test_sse_poll_interval_default(self):
        cfg = _fresh_config()
        assert cfg.SSE_POLL_INTERVAL == 1.0

    def test_sse_timeout_default(self):
        cfg = _fresh_config()
        assert cfg.SSE_TIMEOUT == 3600.0


# ---------------------------------------------------------------------------
# ENV overrides
# ---------------------------------------------------------------------------

class TestBrixConfigEnvOverride:
    def test_mcp_http_port_override(self):
        cfg = _fresh_config({"BRIX_MCP_HTTP_PORT": "9001"})
        assert cfg.MCP_HTTP_PORT == 9001

    def test_api_port_override(self):
        cfg = _fresh_config({"BRIX_API_PORT": "9090"})
        assert cfg.API_PORT == 9090

    def test_mcp_host_override(self):
        cfg = _fresh_config({"BRIX_MCP_HOST": "127.0.0.1"})
        assert cfg.MCP_HOST == "127.0.0.1"

    def test_container_pipelines_dir_override(self):
        cfg = _fresh_config({"BRIX_CONTAINER_PIPELINES_DIR": "/custom/pipelines"})
        assert cfg.CONTAINER_PIPELINES_DIR == "/custom/pipelines"

    def test_legacy_helpers_dir_override(self):
        cfg = _fresh_config({"BRIX_LEGACY_HELPERS_DIR": "/custom/helpers"})
        assert cfg.LEGACY_HELPERS_DIR == "/custom/helpers"

    def test_mcp_pool_call_timeout_override(self):
        cfg = _fresh_config({"BRIX_MCP_POOL_CALL_TIMEOUT": "120"})
        assert cfg.MCP_POOL_CALL_TIMEOUT == 120.0

    def test_timeout_python_override(self):
        cfg = _fresh_config({"BRIX_TIMEOUT_PYTHON": "7200"})
        assert cfg.TIMEOUT_PYTHON == 7200.0

    def test_timeout_cli_override(self):
        cfg = _fresh_config({"BRIX_TIMEOUT_CLI": "600"})
        assert cfg.TIMEOUT_CLI == 600.0

    def test_mattermost_webhook_timeout_override(self):
        cfg = _fresh_config({"BRIX_MATTERMOST_WEBHOOK_TIMEOUT": "30"})
        assert cfg.MATTERMOST_WEBHOOK_TIMEOUT == 30.0

    def test_http_poll_timeout_override(self):
        cfg = _fresh_config({"BRIX_HTTP_POLL_TIMEOUT": "60"})
        assert cfg.HTTP_POLL_TIMEOUT == 60.0

    def test_large_output_threshold_override(self):
        cfg = _fresh_config({"BRIX_LARGE_OUTPUT_THRESHOLD": "500"})
        assert cfg.LARGE_OUTPUT_THRESHOLD == 500

    def test_large_output_size_mb_override(self):
        cfg = _fresh_config({"BRIX_LARGE_OUTPUT_SIZE_MB": "50"})
        assert cfg.LARGE_OUTPUT_SIZE_MB == 50

    def test_history_list_limit_override(self):
        cfg = _fresh_config({"BRIX_HISTORY_LIST_LIMIT": "500"})
        assert cfg.HISTORY_LIST_LIMIT == 500

    def test_imap_default_server_override(self):
        cfg = _fresh_config({"BRIX_IMAP_DEFAULT_SERVER": "imap.outlook.com"})
        assert cfg.IMAP_DEFAULT_SERVER == "imap.outlook.com"

    def test_trigger_default_interval_override(self):
        cfg = _fresh_config({"BRIX_TRIGGER_DEFAULT_INTERVAL": "15m"})
        assert cfg.TRIGGER_DEFAULT_INTERVAL == "15m"

    def test_sse_poll_interval_override(self):
        cfg = _fresh_config({"BRIX_SSE_POLL_INTERVAL": "2.5"})
        assert cfg.SSE_POLL_INTERVAL == 2.5

    def test_sse_timeout_override(self):
        cfg = _fresh_config({"BRIX_SSE_TIMEOUT": "7200.0"})
        assert cfg.SSE_TIMEOUT == 7200.0

    def test_background_run_timeout_override(self):
        cfg = _fresh_config({"BRIX_BACKGROUND_RUN_TIMEOUT_SECONDS": "3600"})
        assert cfg.BACKGROUND_RUN_TIMEOUT_SECONDS == 3600

    def test_idempotency_ttl_override(self):
        cfg = _fresh_config({"BRIX_IDEMPOTENCY_TTL": "43200"})
        assert cfg.IDEMPOTENCY_TTL == 43200

    def test_schema_consultation_ttl_override(self):
        cfg = _fresh_config({"BRIX_SCHEMA_CONSULTATION_TTL_SECONDS": "900"})
        assert cfg.SCHEMA_CONSULTATION_TTL_SECONDS == 900


# ---------------------------------------------------------------------------
# Derived properties
# ---------------------------------------------------------------------------

class TestBrixConfigDerived:
    def test_large_output_size_bytes_default(self):
        cfg = _fresh_config()
        assert cfg.large_output_size_bytes == 10 * 1024 * 1024

    def test_large_output_size_bytes_custom(self):
        cfg = _fresh_config({"BRIX_LARGE_OUTPUT_SIZE_MB": "5"})
        assert cfg.large_output_size_bytes == 5 * 1024 * 1024


# ---------------------------------------------------------------------------
# Reload helper
# ---------------------------------------------------------------------------

class TestBrixConfigReload:
    def test_reload_returns_new_instance(self):
        from brix.config import BrixConfig
        cfg1 = BrixConfig()
        cfg2 = BrixConfig.reload()
        # Both instances should have the same defaults
        assert cfg1.API_PORT == cfg2.API_PORT
        # reload() returns a new object, not the same reference
        assert cfg1 is not cfg2
