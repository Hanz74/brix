"""Shared subprocess utilities for runners (INBOX-510)."""

import asyncio

# Grace period (seconds) between SIGTERM and SIGKILL.
_TERMINATE_GRACE_SECONDS = 5


async def _terminate_subprocess(proc: asyncio.subprocess.Process) -> None:
    """Terminate a subprocess gracefully (SIGTERM), escalate to SIGKILL.

    1. Send SIGTERM and wait up to ``_TERMINATE_GRACE_SECONDS``.
    2. If the process is still alive after the grace period, send SIGKILL.
    3. Always ``await proc.wait()`` to reap the zombie.

    Safe to call even if the process has already exited.
    """
    if proc.returncode is not None:
        # Already exited — nothing to do.
        return

    try:
        proc.terminate()  # SIGTERM
    except ProcessLookupError:
        return

    try:
        await asyncio.wait_for(proc.wait(), timeout=_TERMINATE_GRACE_SECONDS)
    except asyncio.TimeoutError:
        # Still alive after grace period — force kill.
        try:
            proc.kill()  # SIGKILL
        except ProcessLookupError:
            pass
        await proc.wait()
