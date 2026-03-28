"""Shared test helper utilities for brix tests."""

import asyncio


def run_coro(coro):
    """Run an async coroutine synchronously. Shared helper for all test modules."""
    return asyncio.get_event_loop().run_until_complete(coro)
