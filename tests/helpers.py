"""Shared test helper utilities for brix tests."""

import asyncio


def run_coro(coro):
    """Run an async coroutine synchronously. Shared helper for all test modules."""
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)
