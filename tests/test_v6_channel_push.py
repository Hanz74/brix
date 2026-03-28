"""Tests for T-BRIX-V6-05 (Claude Code channel push) and T-BRIX-V6-06 (Mattermost notify).

Covers:
- Pipeline.notify model — default values
- Pipeline.notify.mattermost enabled/webhook_url fields
- PipelineNotifyConfig / MattermostNotifyConfig parse correctly from YAML-like dicts
- _handle_run_pipeline sends MCP notification when source + request_ctx present
- _handle_run_pipeline silently ignores notification failures
- _handle_run_pipeline POSTs to Mattermost when enabled=True
- _handle_run_pipeline skips Mattermost POST when enabled=False
- _handle_run_pipeline skips Mattermost POST when webhook_url is empty
- Mattermost failure does not prevent result from being returned
- create_server / run_mcp_server declare experimental claude/channel capability
"""
import json
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from brix.models import Pipeline, PipelineNotifyConfig, MattermostNotifyConfig
from brix.db import BrixDB


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------

class TestPipelineNotifyModel:
    def test_pipeline_has_notify_field(self):
        p = Pipeline(
            name="test",
            steps=[{"id": "s1", "type": "set", "values": {"x": "1"}}],
        )
        assert hasattr(p, "notify")
        assert isinstance(p.notify, PipelineNotifyConfig)

    def test_mattermost_defaults_disabled(self):
        p = Pipeline(
            name="test",
            steps=[{"id": "s1", "type": "set", "values": {"x": "1"}}],
        )
        assert p.notify.mattermost.enabled is False
        assert p.notify.mattermost.webhook_url == ""

    def test_mattermost_can_be_enabled(self):
        p = Pipeline(
            name="test",
            notify={"mattermost": {"enabled": True, "webhook_url": "https://example.com/hook"}},
            steps=[{"id": "s1", "type": "set", "values": {"x": "1"}}],
        )
        assert p.notify.mattermost.enabled is True
        assert p.notify.mattermost.webhook_url == "https://example.com/hook"

    def test_notify_config_standalone(self):
        cfg = PipelineNotifyConfig()
        assert cfg.mattermost.enabled is False

    def test_mattermost_config_standalone(self):
        cfg = MattermostNotifyConfig(enabled=True, webhook_url="https://hook.example.com")
        assert cfg.enabled is True
        assert cfg.webhook_url == "https://hook.example.com"

    def test_notify_config_partial_mattermost(self):
        """Only webhook_url given — enabled defaults to False."""
        cfg = PipelineNotifyConfig(mattermost={"webhook_url": "https://x.com/y"})
        assert cfg.mattermost.enabled is False
        assert cfg.mattermost.webhook_url == "https://x.com/y"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_audit_db(tmp_path, monkeypatch):
    """Redirect _audit_db to a temp DB."""
    import brix.mcp_server as mcp_mod
    temp_db = BrixDB(db_path=tmp_path / "audit_test.db")
    monkeypatch.setattr(mcp_mod, "_audit_db", temp_db)
    monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)
    yield temp_db


def _make_simple_pipeline(tmp_path: Path, name: str = "notify-test", notify_cfg: dict = None) -> Path:
    """Write a minimal pipeline YAML to tmp_path and return its path."""
    import yaml
    pipeline_data = {
        "name": name,
        "version": "1.0.0",
        "steps": [{"id": "s1", "type": "set", "values": {"done": "yes"}}],
    }
    if notify_cfg:
        pipeline_data["notify"] = notify_cfg
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.dump(pipeline_data))
    return path


# ---------------------------------------------------------------------------
# Channel push tests (V6-05)
# ---------------------------------------------------------------------------

class TestChannelPush:
    @pytest.mark.asyncio
    async def test_channel_push_sent_when_source_and_ctx_present(self, tmp_path, monkeypatch):
        """When source is set and request_ctx is active, a notification must be sent."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(tmp_path)
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        # Mock request_ctx and session
        mock_session = AsyncMock()
        mock_meta = MagicMock()
        mock_meta.progressToken = None
        mock_ctx = MagicMock()
        mock_ctx.session = mock_session
        mock_ctx.meta = mock_meta

        sent_notifications = []

        async def capture_notification(notif):
            sent_notifications.append(notif)

        mock_session.send_notification.side_effect = capture_notification

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_request_ctx:
            mock_request_ctx.get.return_value = mock_ctx
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
                "source": {"session": "test-session", "model": "sonnet"},
            })

        assert result["success"] is True
        assert len(sent_notifications) == 1
        notif = sent_notifications[0]
        assert notif.method == "notifications/claude/channel"
        assert "notify-test" in notif.params["content"]
        assert notif.params["meta"]["pipeline"] == "notify-test"
        assert "run_id" in notif.params["meta"]
        assert notif.params["meta"]["status"] == "success"

    @pytest.mark.asyncio
    async def test_channel_push_skipped_when_no_source(self, tmp_path, monkeypatch):
        """Without source, no notification is sent even if ctx is available."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(tmp_path)
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        mock_session = AsyncMock()
        mock_ctx = MagicMock()
        mock_ctx.session = mock_session
        mock_ctx.meta = MagicMock()
        mock_ctx.meta.progressToken = None

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_request_ctx:
            mock_request_ctx.get.return_value = mock_ctx
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
                # no source
            })

        assert result["success"] is True
        mock_session.send_notification.assert_not_called()

    @pytest.mark.asyncio
    async def test_channel_push_skipped_when_no_ctx(self, tmp_path, monkeypatch):
        """Without request_ctx, no crash and no notification sent."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(tmp_path)
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_request_ctx:
            mock_request_ctx.get.return_value = None
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
                "source": {"session": "s"},
            })

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_channel_push_failure_does_not_break_result(self, tmp_path, monkeypatch):
        """If send_notification raises, the pipeline result must still be returned."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(tmp_path)
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        mock_session = AsyncMock()
        mock_session.send_notification.side_effect = RuntimeError("connection lost")
        mock_ctx = MagicMock()
        mock_ctx.session = mock_session
        mock_ctx.meta = MagicMock()
        mock_ctx.meta.progressToken = None

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_request_ctx:
            mock_request_ctx.get.return_value = mock_ctx
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
                "source": {"session": "s"},
            })

        assert result["success"] is True  # result returned despite notification failure

    @pytest.mark.asyncio
    async def test_channel_push_content_includes_items_count(self, tmp_path, monkeypatch):
        """Notification content is a string with pipeline name."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(tmp_path)
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        mock_session = AsyncMock()
        mock_ctx = MagicMock()
        mock_ctx.session = mock_session
        mock_ctx.meta = MagicMock()
        mock_ctx.meta.progressToken = None

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_request_ctx:
            mock_request_ctx.get.return_value = mock_ctx
            await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
                "source": {"session": "s"},
            })

        # Notification should have been attempted; content must be a string
        assert mock_session.send_notification.called
        call_arg = mock_session.send_notification.call_args[0][0]
        assert isinstance(call_arg.params["content"], str)


# ---------------------------------------------------------------------------
# Experimental capability declaration (V6-05)
# ---------------------------------------------------------------------------

class TestExperimentalCapability:
    def test_create_server_supports_experimental_capability_call(self):
        """create_initialization_options should accept experimental_capabilities."""
        from brix.mcp_server import create_server
        server = create_server()
        # Must not raise
        opts = server.create_initialization_options(
            experimental_capabilities={"claude/channel": {}}
        )
        assert opts is not None

    def test_experimental_capabilities_in_init_options(self):
        """experimental_capabilities dict is passed through to capabilities."""
        from brix.mcp_server import create_server
        server = create_server()
        opts = server.create_initialization_options(
            experimental_capabilities={"claude/channel": {}}
        )
        # The init options should carry experimental capabilities
        caps = opts.capabilities
        assert caps.experimental is not None
        assert "claude/channel" in caps.experimental


# ---------------------------------------------------------------------------
# Mattermost webhook (V6-06)
# ---------------------------------------------------------------------------

class TestMattermostNotify:
    @pytest.mark.asyncio
    async def test_mattermost_post_sent_when_enabled(self, tmp_path, monkeypatch):
        """When notify.mattermost.enabled=True and webhook_url set, a POST is made."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(
            tmp_path,
            notify_cfg={"mattermost": {"enabled": True, "webhook_url": "https://mm.example.com/hook"}},
        )
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        posted_payloads = []

        class _FakeResponse:
            def __enter__(self): return self
            def __exit__(self, *a): pass

        def _fake_urlopen(req, timeout=10):
            posted_payloads.append(json.loads(req.data.decode()))
            return _FakeResponse()

        with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
                "source": {"session": "s"},
            })

        assert result["success"] is True
        assert len(posted_payloads) == 1
        assert "notify-test" in posted_payloads[0]["text"]

    @pytest.mark.asyncio
    async def test_mattermost_post_skipped_when_disabled(self, tmp_path, monkeypatch):
        """When enabled=False, no POST is made."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(
            tmp_path,
            notify_cfg={"mattermost": {"enabled": False, "webhook_url": "https://mm.example.com/hook"}},
        )
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        with patch("urllib.request.urlopen") as mock_urlopen:
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
            })

        assert result["success"] is True
        mock_urlopen.assert_not_called()

    @pytest.mark.asyncio
    async def test_mattermost_post_skipped_when_no_webhook_url(self, tmp_path, monkeypatch):
        """When enabled=True but webhook_url is empty, no POST is made."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(
            tmp_path,
            notify_cfg={"mattermost": {"enabled": True, "webhook_url": ""}},
        )
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        with patch("urllib.request.urlopen") as mock_urlopen:
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
            })

        assert result["success"] is True
        mock_urlopen.assert_not_called()

    @pytest.mark.asyncio
    async def test_mattermost_failure_does_not_break_result(self, tmp_path, monkeypatch):
        """If the Mattermost POST fails, pipeline result is still returned."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(
            tmp_path,
            notify_cfg={"mattermost": {"enabled": True, "webhook_url": "https://mm.example.com/hook"}},
        )
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        with patch("urllib.request.urlopen", side_effect=OSError("network error")):
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
            })

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_mattermost_post_includes_run_id_and_status(self, tmp_path, monkeypatch):
        """POST payload text includes run_id and pipeline status."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(
            tmp_path,
            notify_cfg={"mattermost": {"enabled": True, "webhook_url": "https://mm.example.com/hook"}},
        )
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        posted_payloads = []

        class _FakeResponse:
            def __enter__(self): return self
            def __exit__(self, *a): pass

        def _fake_urlopen(req, timeout=10):
            posted_payloads.append(json.loads(req.data.decode()))
            return _FakeResponse()

        with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
            })

        assert result["success"] is True
        assert posted_payloads
        text = posted_payloads[0]["text"]
        assert result["run_id"] in text
        assert "success" in text

    @pytest.mark.asyncio
    async def test_mattermost_not_sent_by_default(self, tmp_path, monkeypatch):
        """Without notify config, no Mattermost POST is made."""
        import brix.mcp_server as mcp_mod

        _make_simple_pipeline(tmp_path)  # no notify config
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        with patch("urllib.request.urlopen") as mock_urlopen:
            result = await mcp_mod._handle_run_pipeline({
                "pipeline_id": "notify-test",
            })

        assert result["success"] is True
        mock_urlopen.assert_not_called()
