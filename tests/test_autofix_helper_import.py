"""Tests for T-BRIX-AUTOFIX-01 — auto_fix_step should not pip-install helper modules.

Coverage:
- ModuleNotFoundError for a module that matches a registered Brix helper
  → returns fixed=False with a hint to use the imports field
- ModuleNotFoundError for an unknown module (not a helper)
  → normal pip install suggestion (mocked install succeeds)
"""
import asyncio
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from brix.history import RunHistory
from brix.db import BrixDB
from brix.helper_registry import HelperRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):
    """Run a coroutine synchronously for testing."""
    return asyncio.get_event_loop().run_until_complete(coro)


def make_run(history: RunHistory, run_id: str, pipeline: str, steps: dict, success: bool = False):
    """Record a finished run with the given steps_data."""
    history.record_start(run_id, pipeline)
    history.record_finish(run_id, success, 1.0, steps)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAutoFixHelperImport:

    def test_module_is_known_helper_returns_hint(self, tmp_path):
        """ModuleNotFoundError for a registered helper → hint about imports field, no pip install."""
        from brix.mcp_server import _handle_auto_fix_step

        # Register a helper named 'my_utils' in an isolated DB
        isolated_db = BrixDB(db_path=tmp_path / "brix.db")
        registry = HelperRegistry(db=isolated_db)
        registry.register(
            name="my_utils",
            code="def hello(): return 'hi'",
            description="A test helper",
        )

        # Record a run whose step failed with a ModuleNotFoundError for 'my_utils'
        h = RunHistory(db_path=tmp_path / "history.db")
        steps = {
            "process": {
                "status": "error",
                "error_message": "ModuleNotFoundError: No module named 'my_utils'",
            }
        }
        make_run(h, "run-helper-01", "pipe-a", steps, success=False)

        # Patch RunHistory (imported locally inside _handle_auto_fix_step via brix.history)
        # and BrixDB (imported locally inside the fix block via brix.db)
        with patch("brix.history.RunHistory", return_value=h), \
             patch("brix.db.BrixDB", return_value=isolated_db):
            result = _run(
                _handle_auto_fix_step({"run_id": "run-helper-01", "step_id": "process"})
            )

        assert result["fixed"] is False
        assert "my_utils" in result["rerun_hint"]
        assert "imports" in result["rerun_hint"]
        # Must NOT attempt pip install — action should mention helper, not pip
        assert "pip install" not in result.get("action", "")

    def test_unknown_module_proceeds_to_pip_install(self, tmp_path):
        """ModuleNotFoundError for a module that is not a helper → normal pip install."""
        from brix.mcp_server import _handle_auto_fix_step

        # Use a fresh isolated DB with NO helpers registered
        isolated_db = BrixDB(db_path=tmp_path / "brix.db")

        h = RunHistory(db_path=tmp_path / "history.db")
        steps = {
            "fetch": {
                "status": "error",
                "error_message": "ModuleNotFoundError: No module named 'httpx'",
            }
        }
        make_run(h, "run-pip-01", "pipe-b", steps, success=False)

        with patch("brix.history.RunHistory", return_value=h), \
             patch("brix.db.BrixDB", return_value=isolated_db), \
             patch("brix.deps.install_requirements", return_value=True) as mock_install:
            result = _run(
                _handle_auto_fix_step({"run_id": "run-pip-01", "step_id": "fetch"})
            )

        assert result["fixed"] is True
        assert "httpx" in result["action"]
        mock_install.assert_called_once_with(["httpx"])
