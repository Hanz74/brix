"""Tests for T-BRIX-IMP-01: per-step unwrap_json override.

Step-level unwrap_json (True/False/None) must:
- Override the server-level unwrap_json when explicitly set.
- Fall back to server_config.unwrap_json when None.
"""

import json
import pytest
import yaml
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from brix.runners.mcp import McpRunner
from brix.models import Step


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal step stand-in with optional unwrap_json attribute."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        # Default to None (absent) if not supplied — mirrors Step model default.
        if not hasattr(self, "unwrap_json"):
            self.unwrap_json = None


def _write_servers_yaml(tmp_path: Path, server_unwrap: bool) -> Path:
    config = {
        "servers": {
            "srv": {
                "command": "echo",
                "args": [],
                "unwrap_json": server_unwrap,
            }
        }
    }
    path = tmp_path / "servers.yaml"
    path.write_text(yaml.dump(config))
    return path


def _mock_mcp_response(raw_text: str):
    """Build a minimal MCP call_tool mock that returns raw_text as content."""
    content_block = MagicMock()
    content_block.text = raw_text
    mock_result = MagicMock()
    mock_result.isError = False
    mock_result.structuredContent = None
    mock_result.content = [content_block]
    return mock_result


async def _run_step(runner: McpRunner, step) -> dict:
    """Execute step through McpRunner with patched stdio transport."""
    mock_session = AsyncMock()
    mock_session.initialize = AsyncMock()
    mock_session.call_tool = AsyncMock(return_value=_mock_mcp_response(step._raw))

    with patch("brix.runners.mcp.stdio_client") as mock_ctx:
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with patch("brix.runners.mcp.ClientSession") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            return await runner.execute(step, context=None)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_step_unwrap_true_overrides_server_false(tmp_path: Path):
    """Step unwrap_json=True activates unwrapping even when server has False."""
    config_path = _write_servers_yaml(tmp_path, server_unwrap=False)
    inner = {"answer": 99}
    raw = json.dumps({"result": json.dumps(inner)})

    step = _Step(server="srv", tool="t", params={}, unwrap_json=True)
    step._raw = raw

    runner = McpRunner(servers_config_path=config_path)
    result = await _run_step(runner, step)

    assert result["success"] is True
    # Step-level True must trigger unwrapping despite server_config.unwrap_json=False
    assert result["data"]["result"] == inner


async def test_step_unwrap_false_overrides_server_true(tmp_path: Path):
    """Step unwrap_json=False suppresses unwrapping even when server has True."""
    config_path = _write_servers_yaml(tmp_path, server_unwrap=True)
    inner = {"answer": 99}
    raw = json.dumps({"result": json.dumps(inner)})

    step = _Step(server="srv", tool="t", params={}, unwrap_json=False)
    step._raw = raw

    runner = McpRunner(servers_config_path=config_path)
    result = await _run_step(runner, step)

    assert result["success"] is True
    # Step-level False must suppress unwrapping despite server_config.unwrap_json=True
    assert isinstance(result["data"]["result"], str)


async def test_step_unwrap_none_falls_back_to_server_true(tmp_path: Path):
    """Step unwrap_json=None falls back to server_config.unwrap_json=True."""
    config_path = _write_servers_yaml(tmp_path, server_unwrap=True)
    inner = {"answer": 7}
    raw = json.dumps({"result": json.dumps(inner)})

    step = _Step(server="srv", tool="t", params={}, unwrap_json=None)
    step._raw = raw

    runner = McpRunner(servers_config_path=config_path)
    result = await _run_step(runner, step)

    assert result["success"] is True
    assert result["data"]["result"] == inner


async def test_step_unwrap_none_falls_back_to_server_false(tmp_path: Path):
    """Step unwrap_json=None falls back to server_config.unwrap_json=False."""
    config_path = _write_servers_yaml(tmp_path, server_unwrap=False)
    inner = {"answer": 7}
    raw = json.dumps({"result": json.dumps(inner)})

    step = _Step(server="srv", tool="t", params={}, unwrap_json=None)
    step._raw = raw

    runner = McpRunner(servers_config_path=config_path)
    result = await _run_step(runner, step)

    assert result["success"] is True
    # No unwrapping — inner dict remains as a JSON string
    assert isinstance(result["data"]["result"], str)


def test_step_model_has_unwrap_json_field():
    """Step model accepts unwrap_json field and defaults to None."""
    step = Step(
        id="s1",
        type="mcp",
        server="my-server",
        tool="my-tool",
    )
    assert step.unwrap_json is None


def test_step_model_unwrap_json_explicit_values():
    """Step model accepts True and False for unwrap_json."""
    step_true = Step(id="s2", type="mcp", server="srv", tool="t", unwrap_json=True)
    step_false = Step(id="s3", type="mcp", server="srv", tool="t", unwrap_json=False)
    assert step_true.unwrap_json is True
    assert step_false.unwrap_json is False
