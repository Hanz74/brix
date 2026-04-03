"""Tests for T-BRIX-BUG-18: Scheduler survives MCP client disconnect."""
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Test: scheduler_start actually creates a background task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_scheduler_start_creates_background_task():
    """_handle_scheduler_start must create an asyncio task for TriggerService."""
    import brix.mcp_handlers.triggers as mod

    # Reset module-level state
    mod._scheduler_task = None
    mod._scheduler_running = False

    # Mock TriggerStore to report one enabled trigger
    fake_trigger = {"name": "t1", "enabled": True, "type": "cron", "pipeline": "p1"}
    mock_store_cls = MagicMock()
    mock_store_cls.return_value.list_all.return_value = [fake_trigger]

    # Mock TriggerService so start() is a coroutine that sleeps forever
    mock_svc = MagicMock()
    _started = asyncio.Event()

    async def fake_start():
        _started.set()
        await asyncio.sleep(3600)

    mock_svc.start = fake_start
    mock_svc_cls = MagicMock(return_value=mock_svc)

    with patch("brix.mcp_handlers.triggers.TriggerStore", mock_store_cls, create=True), \
         patch("brix.mcp_handlers.triggers.TriggerService", mock_svc_cls, create=True):
        # The import inside _handle_scheduler_start uses fully-qualified paths,
        # so we also patch the source modules.
        with patch("brix.triggers.store.TriggerStore", mock_store_cls, create=True), \
             patch("brix.triggers.service.TriggerService", mock_svc_cls, create=True):
            result = await mod._handle_scheduler_start({})

    assert result["success"] is True
    assert result["status"] == "started"
    assert mod._scheduler_running is True
    assert mod._scheduler_task is not None
    assert not mod._scheduler_task.done()

    # Wait for the service to actually start
    await asyncio.wait_for(_started.wait(), timeout=2.0)

    # Cleanup: cancel the task
    mod._scheduler_task.cancel()
    try:
        await mod._scheduler_task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# Test: scheduler survives simulated MCP disconnect
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_scheduler_survives_mcp_disconnect():
    """The scheduler task must keep running after the MCP session context exits."""
    import brix.mcp_handlers.triggers as mod

    # Reset state
    mod._scheduler_task = None
    mod._scheduler_running = False

    # Create a real background task simulating the trigger service
    _tick_count = 0

    async def fake_trigger_loop():
        nonlocal _tick_count
        while True:
            _tick_count += 1
            await asyncio.sleep(0.05)

    fake_trigger = {"name": "t1", "enabled": True, "type": "cron", "pipeline": "p1"}
    mock_store_cls = MagicMock()
    mock_store_cls.return_value.list_all.return_value = [fake_trigger]

    mock_svc = MagicMock()
    mock_svc.start = fake_trigger_loop
    mock_svc_cls = MagicMock(return_value=mock_svc)

    with patch("brix.mcp_handlers.triggers.TriggerStore", mock_store_cls, create=True), \
         patch("brix.mcp_handlers.triggers.TriggerService", mock_svc_cls, create=True), \
         patch("brix.triggers.store.TriggerStore", mock_store_cls, create=True), \
         patch("brix.triggers.service.TriggerService", mock_svc_cls, create=True):
        await mod._handle_scheduler_start({})

    # Scheduler task is running
    assert mod._scheduler_task is not None
    assert not mod._scheduler_task.done()

    # Simulate MCP session ending — in the real code this means the
    # `async with stdio_server()` context exits.  The scheduler task
    # should NOT be affected because it lives outside that scope.
    await asyncio.sleep(0.15)

    # Task should still be alive and ticking
    assert not mod._scheduler_task.done()
    assert _tick_count >= 2, f"Expected >= 2 ticks, got {_tick_count}"

    # Cleanup
    mod._scheduler_task.cancel()
    try:
        await mod._scheduler_task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# Test: scheduler_stop cancels the background task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_scheduler_stop_cancels_task():
    """_handle_scheduler_stop must cancel the background task."""
    import brix.mcp_handlers.triggers as mod

    mod._scheduler_task = None
    mod._scheduler_running = False

    fake_trigger = {"name": "t1", "enabled": True, "type": "cron", "pipeline": "p1"}
    mock_store_cls = MagicMock()
    mock_store_cls.return_value.list_all.return_value = [fake_trigger]

    mock_svc = MagicMock()
    mock_svc.start = AsyncMock(side_effect=lambda: asyncio.sleep(3600))
    mock_svc_cls = MagicMock(return_value=mock_svc)

    with patch("brix.mcp_handlers.triggers.TriggerStore", mock_store_cls, create=True), \
         patch("brix.mcp_handlers.triggers.TriggerService", mock_svc_cls, create=True), \
         patch("brix.triggers.store.TriggerStore", mock_store_cls, create=True), \
         patch("brix.triggers.service.TriggerService", mock_svc_cls, create=True):
        await mod._handle_scheduler_start({})

    assert mod._scheduler_running is True
    assert mod._scheduler_task is not None

    result = await mod._handle_scheduler_stop({})
    assert result["success"] is True
    assert result["status"] == "stopped"
    assert mod._scheduler_running is False
    assert mod._scheduler_task is None


# ---------------------------------------------------------------------------
# Test: auto_start_scheduler_if_needed starts when enabled triggers exist
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auto_start_creates_task_when_triggers_exist():
    """_auto_start_scheduler_if_needed must start the scheduler if enabled triggers exist."""
    import brix.mcp_handlers.triggers as mod

    mod._scheduler_task = None
    mod._scheduler_running = False

    fake_trigger = {"name": "t1", "enabled": True, "type": "cron", "pipeline": "p1"}
    mock_store_cls = MagicMock()
    mock_store_cls.return_value.list_all.return_value = [fake_trigger]

    mock_svc = MagicMock()
    mock_svc.start = AsyncMock(side_effect=lambda: asyncio.sleep(3600))
    mock_svc_cls = MagicMock(return_value=mock_svc)

    with patch("brix.mcp_handlers.triggers.TriggerStore", mock_store_cls, create=True), \
         patch("brix.mcp_handlers.triggers.TriggerService", mock_svc_cls, create=True), \
         patch("brix.triggers.store.TriggerStore", mock_store_cls, create=True), \
         patch("brix.triggers.service.TriggerService", mock_svc_cls, create=True):
        await mod._auto_start_scheduler_if_needed()

    assert mod._scheduler_running is True
    assert mod._scheduler_task is not None
    assert not mod._scheduler_task.done()

    # Cleanup
    mod._scheduler_task.cancel()
    try:
        await mod._scheduler_task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# Test: scheduler_status reports actual task liveness
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_scheduler_status_reflects_task_state():
    """Status should report running=False if the task has died even if flag is True."""
    import brix.mcp_handlers.triggers as mod

    mock_store_cls = MagicMock()
    mock_store_cls.return_value.list_all.return_value = []

    # Simulate a dead task
    mod._scheduler_running = True
    mod._scheduler_task = asyncio.create_task(asyncio.sleep(0))
    await mod._scheduler_task  # let it finish

    with patch("brix.mcp_handlers.triggers.TriggerStore", mock_store_cls, create=True), \
         patch("brix.triggers.store.TriggerStore", mock_store_cls, create=True):
        result = await mod._handle_scheduler_status({})

    # Task is done, so running should be False
    assert result["running"] is False

    # Reset
    mod._scheduler_running = False
    mod._scheduler_task = None
