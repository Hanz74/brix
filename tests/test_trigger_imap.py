"""Tests for MailTriggerRunner IMAP provider support (T-BRIX-V6-BUG-02)."""
import asyncio
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from brix.triggers.models import TriggerConfig
from brix.triggers.state import TriggerState
from brix.triggers.runners import MailTriggerRunner
from brix.credential_store import CredentialStore, CredentialNotFoundError


# ---------------------------------------------------------------------------
# TriggerConfig — IMAP fields
# ---------------------------------------------------------------------------

def test_trigger_config_imap_defaults():
    """TriggerConfig has sensible defaults for IMAP fields."""
    config = TriggerConfig(id="t1", type="mail", pipeline="process-mail")
    assert config.provider == "m365"
    assert config.email is None
    assert config.app_password_credential is None
    assert config.folder == "INBOX"
    assert config.server == "imap.gmail.com"


def test_trigger_config_imap_provider_fields():
    """TriggerConfig accepts IMAP provider configuration."""
    config = TriggerConfig(
        id="gmail-trigger",
        type="mail",
        pipeline="process-gmail",
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
        folder="[Gmail]/All Mail",
        server="imap.gmail.com",
    )
    assert config.provider == "imap"
    assert config.email == "user@gmail.com"
    assert config.app_password_credential == "cred-uuid-1234"
    assert config.folder == "[Gmail]/All Mail"
    assert config.server == "imap.gmail.com"


# ---------------------------------------------------------------------------
# TriggerState — last_check persistence
# ---------------------------------------------------------------------------

def test_trigger_state_get_last_check_none(tmp_path):
    """get_last_check returns None when no check has been recorded."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    assert state.get_last_check("my-trigger") is None


def test_trigger_state_set_and_get_last_check(tmp_path):
    """set_last_check persists a timestamp; get_last_check retrieves it."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    ts = time.time()
    state.set_last_check("my-trigger", ts)
    result = state.get_last_check("my-trigger")
    assert result == pytest.approx(ts, rel=1e-6)


def test_trigger_state_set_last_check_upsert(tmp_path):
    """set_last_check can overwrite an existing value."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.set_last_check("my-trigger", 1000.0)
    state.set_last_check("my-trigger", 2000.0)
    assert state.get_last_check("my-trigger") == pytest.approx(2000.0, rel=1e-6)


def test_trigger_state_last_check_per_trigger(tmp_path):
    """last_check timestamps are independent per trigger_id."""
    state = TriggerState(db_path=tmp_path / "triggers.db")
    state.set_last_check("trigger-a", 1111.0)
    state.set_last_check("trigger-b", 2222.0)
    assert state.get_last_check("trigger-a") == pytest.approx(1111.0)
    assert state.get_last_check("trigger-b") == pytest.approx(2222.0)


# ---------------------------------------------------------------------------
# MailTriggerRunner — provider routing
# ---------------------------------------------------------------------------

def _make_trigger(provider="m365", **kwargs):
    return TriggerConfig(
        id="test-mail-trigger",
        type="mail",
        pipeline="process-mail",
        provider=provider,
        **kwargs,
    )


def _make_state(tmp_path):
    return TriggerState(db_path=tmp_path / "triggers.db")


def _make_runner(trigger, state):
    return MailTriggerRunner(trigger, state)


@pytest.mark.asyncio
async def test_mail_runner_routes_to_m365_by_default(tmp_path):
    """MailTriggerRunner calls _poll_m365 when provider is 'm365'."""
    trigger = _make_trigger(provider="m365")
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    with patch.object(runner, "_poll_m365", return_value=[]) as mock_m365, \
         patch.object(runner, "_poll_imap", return_value=[]) as mock_imap:
        await runner.poll()
        mock_m365.assert_called_once()
        mock_imap.assert_not_called()


@pytest.mark.asyncio
async def test_mail_runner_routes_to_imap_when_provider_set(tmp_path):
    """MailTriggerRunner calls _poll_imap when provider is 'imap'."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    with patch.object(runner, "_poll_m365", return_value=[]) as mock_m365, \
         patch.object(runner, "_poll_imap", return_value=[]) as mock_imap:
        await runner.poll()
        mock_imap.assert_called_once()
        mock_m365.assert_not_called()


# ---------------------------------------------------------------------------
# MailTriggerRunner — _poll_imap validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_poll_imap_missing_email_returns_empty(tmp_path):
    """_poll_imap returns [] when email is not configured."""
    trigger = _make_trigger(
        provider="imap",
        email=None,
        app_password_credential="cred-uuid-1234",
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)
    events = await runner._poll_imap()
    assert events == []


@pytest.mark.asyncio
async def test_poll_imap_missing_credential_returns_empty(tmp_path):
    """_poll_imap returns [] when app_password_credential is not configured."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential=None,
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)
    events = await runner._poll_imap()
    assert events == []


@pytest.mark.asyncio
async def test_poll_imap_credential_not_found_returns_empty(tmp_path):
    """_poll_imap returns [] when credential UUID is not in CredentialStore."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="nonexistent-uuid",
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    with patch("brix.triggers.runners.CredentialStore") as MockCredStore:
        mock_instance = MockCredStore.return_value
        mock_instance.resolve.side_effect = CredentialNotFoundError("not found")
        events = await runner._poll_imap()

    assert events == []


# ---------------------------------------------------------------------------
# MailTriggerRunner — _poll_imap IMAP integration (mocked imaplib)
# ---------------------------------------------------------------------------

def _setup_imap_mock(mail_mock, folder_status="OK", search_status="OK", uid_list=b"1 2 3"):
    """Configure a mock imaplib.IMAP4_SSL instance."""
    mail_mock.select.return_value = (folder_status, [b"3"])
    mail_mock.search.return_value = (search_status, [uid_list])
    mail_mock.logout.return_value = ("BYE", [])
    return mail_mock


@pytest.mark.asyncio
async def test_poll_imap_returns_events_for_unseen_messages(tmp_path):
    """_poll_imap returns one event per unseen message UID."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
        folder="INBOX",
        server="imap.gmail.com",
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    with patch("brix.triggers.runners.CredentialStore") as MockCredStore, \
         patch("brix.triggers.runners.imaplib.IMAP4_SSL") as MockImap:
        mock_instance = MockCredStore.return_value
        mock_instance.resolve.return_value = "app-password-secret"

        imap_obj = MagicMock()
        _setup_imap_mock(imap_obj, uid_list=b"10 20 30")
        MockImap.return_value = imap_obj

        events = await runner._poll_imap()

    assert len(events) == 3
    assert all(e["folder"] == "INBOX" for e in events)
    assert all(e["email"] == "user@gmail.com" for e in events)
    assert all(e["server"] == "imap.gmail.com" for e in events)
    assert all(e["unseen_count"] == 3 for e in events)
    uids = [e["message_id"] for e in events]
    assert uids == ["10", "20", "30"]


@pytest.mark.asyncio
async def test_poll_imap_no_unseen_messages_returns_empty(tmp_path):
    """_poll_imap returns [] when there are no unseen messages."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    with patch("brix.triggers.runners.CredentialStore") as MockCredStore, \
         patch("brix.triggers.runners.imaplib.IMAP4_SSL") as MockImap:
        mock_instance = MockCredStore.return_value
        mock_instance.resolve.return_value = "app-password-secret"

        imap_obj = MagicMock()
        _setup_imap_mock(imap_obj, uid_list=b"")
        MockImap.return_value = imap_obj

        events = await runner._poll_imap()

    assert events == []


@pytest.mark.asyncio
async def test_poll_imap_select_failure_returns_empty(tmp_path):
    """_poll_imap returns [] when IMAP SELECT fails."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    with patch("brix.triggers.runners.CredentialStore") as MockCredStore, \
         patch("brix.triggers.runners.imaplib.IMAP4_SSL") as MockImap:
        mock_instance = MockCredStore.return_value
        mock_instance.resolve.return_value = "app-password-secret"

        imap_obj = MagicMock()
        _setup_imap_mock(imap_obj, folder_status="NO")
        MockImap.return_value = imap_obj

        events = await runner._poll_imap()

    assert events == []


@pytest.mark.asyncio
async def test_poll_imap_updates_last_check_timestamp(tmp_path):
    """_poll_imap persists last_check timestamp after successful poll."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    assert state.get_last_check("test-mail-trigger") is None

    with patch("brix.triggers.runners.CredentialStore") as MockCredStore, \
         patch("brix.triggers.runners.imaplib.IMAP4_SSL") as MockImap:
        mock_instance = MockCredStore.return_value
        mock_instance.resolve.return_value = "app-password-secret"

        imap_obj = MagicMock()
        _setup_imap_mock(imap_obj, uid_list=b"")
        MockImap.return_value = imap_obj

        before = time.time()
        await runner._poll_imap()
        after = time.time()

    last = state.get_last_check("test-mail-trigger")
    assert last is not None
    assert before <= last <= after + 1


@pytest.mark.asyncio
async def test_poll_imap_uses_since_from_last_check(tmp_path):
    """_poll_imap uses stored last_check as SINCE date in IMAP SEARCH."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
    )
    state = _make_state(tmp_path)
    # Set a known last_check timestamp: 2024-01-15
    state.set_last_check("test-mail-trigger", 1705276800.0)  # 2024-01-15 UTC
    runner = _make_runner(trigger, state)

    with patch("brix.triggers.runners.CredentialStore") as MockCredStore, \
         patch("brix.triggers.runners.imaplib.IMAP4_SSL") as MockImap:
        mock_instance = MockCredStore.return_value
        mock_instance.resolve.return_value = "secret"

        imap_obj = MagicMock()
        _setup_imap_mock(imap_obj, uid_list=b"")
        MockImap.return_value = imap_obj

        await runner._poll_imap()

    # Verify SEARCH was called with SINCE 15-Jan-2024
    imap_obj.search.assert_called_once()
    search_args = imap_obj.search.call_args
    criterion = search_args[0][1]  # second positional arg
    assert "SINCE" in criterion
    assert "15-Jan-2024" in criterion


@pytest.mark.asyncio
async def test_poll_imap_connection_error_returns_empty(tmp_path):
    """_poll_imap returns [] when IMAP connection raises an exception."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    with patch("brix.triggers.runners.CredentialStore") as MockCredStore, \
         patch("brix.triggers.runners.imaplib.IMAP4_SSL", side_effect=ConnectionRefusedError("refused")):
        mock_instance = MockCredStore.return_value
        mock_instance.resolve.return_value = "secret"

        events = await runner._poll_imap()

    assert events == []


@pytest.mark.asyncio
async def test_poll_imap_uses_default_server_and_folder(tmp_path):
    """_poll_imap falls back to imap.gmail.com / INBOX when not configured."""
    trigger = _make_trigger(
        provider="imap",
        email="user@gmail.com",
        app_password_credential="cred-uuid-1234",
        # no server or folder — use defaults
    )
    state = _make_state(tmp_path)
    runner = _make_runner(trigger, state)

    with patch("brix.triggers.runners.CredentialStore") as MockCredStore, \
         patch("brix.triggers.runners.imaplib.IMAP4_SSL") as MockImap:
        mock_instance = MockCredStore.return_value
        mock_instance.resolve.return_value = "secret"

        imap_obj = MagicMock()
        _setup_imap_mock(imap_obj, uid_list=b"1")
        MockImap.return_value = imap_obj

        events = await runner._poll_imap()

    MockImap.assert_called_once_with("imap.gmail.com")
    imap_obj.select.assert_called_once_with("INBOX", readonly=True)
    assert len(events) == 1
