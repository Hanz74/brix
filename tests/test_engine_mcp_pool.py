"""Tests for T-BRIX-V4-BUG-01: engine.run() mcp_pool parameter.

Covers:
- Synchronous path (mcp_pool=None): engine opens and closes its own pool.
- External-pool path (mcp_pool=<open pool>): engine reuses the caller's pool
  without closing it — the critical fix for asyncio.create_task() dispatch.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.mcp_pool import McpConnectionPool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load(yaml_str: str):
    return PipelineLoader().load_from_string(yaml_str)


SIMPLE_PIPELINE = """
name: simple
steps:
  - id: greet
    type: cli
    args: ["echo", "hello"]
"""


# ---------------------------------------------------------------------------
# Synchronous path (mcp_pool=None) — original behaviour preserved
# ---------------------------------------------------------------------------

class TestSyncPath:
    """engine.run() with no mcp_pool opens its own pool and closes it."""

    async def test_run_succeeds_without_mcp_pool(self):
        """Normal run without passing mcp_pool still succeeds."""
        pipeline = _load(SIMPLE_PIPELINE)
        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.success is True
        assert result.steps["greet"].status == "ok"

    async def test_mcp_pool_cleaned_up_after_run(self):
        """After run() the engine's internal _mcp_pool reference is None."""
        pipeline = _load(SIMPLE_PIPELINE)
        engine = PipelineEngine()
        await engine.run(pipeline)

        # Pool should be detached after run completes
        assert engine._mcp_pool is None

    async def test_mcp_pool_cleaned_up_after_failed_run(self):
        """_mcp_pool is None even when the pipeline fails."""
        pipeline = _load("""
name: fail-pipeline
steps:
  - id: bad
    type: cli
    args: ["false"]
    on_error: stop
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.success is False
        assert engine._mcp_pool is None

    async def test_pool_opened_and_closed_once(self):
        """McpConnectionPool.__aenter__ and __aexit__ are each called once
        when mcp_pool is None."""
        pipeline = _load(SIMPLE_PIPELINE)
        engine = PipelineEngine()

        enter_calls = []
        exit_calls = []

        real_aenter = McpConnectionPool.__aenter__
        real_aexit = McpConnectionPool.__aexit__

        async def fake_aenter(self_pool):
            enter_calls.append(1)
            return await real_aenter(self_pool)

        async def fake_aexit(self_pool, *args):
            exit_calls.append(1)
            return await real_aexit(self_pool, *args)

        with (
            patch.object(McpConnectionPool, "__aenter__", fake_aenter),
            patch.object(McpConnectionPool, "__aexit__", fake_aexit),
        ):
            await engine.run(pipeline)

        assert len(enter_calls) == 1, "Pool should be entered exactly once"
        assert len(exit_calls) == 1, "Pool should be exited exactly once"


# ---------------------------------------------------------------------------
# External pool path (mcp_pool=<open pool>) — fix for create_task dispatch
# ---------------------------------------------------------------------------

class TestExternalPoolPath:
    """engine.run() with caller-supplied mcp_pool reuses it without closing."""

    async def test_run_with_external_pool_succeeds(self):
        """Passing an open pool works and run succeeds."""
        pipeline = _load(SIMPLE_PIPELINE)
        engine = PipelineEngine()

        async with McpConnectionPool() as pool:
            result = await engine.run(pipeline, mcp_pool=pool)

        assert result.success is True
        assert result.steps["greet"].status == "ok"

    async def test_external_pool_not_closed_by_engine(self):
        """The engine does NOT call __aexit__ on a caller-provided pool."""
        pipeline = _load(SIMPLE_PIPELINE)
        engine = PipelineEngine()

        # Create the pool manually so we can spy on its __aexit__ directly
        pool = McpConnectionPool()
        await pool.__aenter__()

        aexit_called = False
        original_aexit = pool.__aexit__

        async def spy_aexit(*args):
            nonlocal aexit_called
            aexit_called = True
            return await original_aexit(*args)

        pool.__aexit__ = spy_aexit  # type: ignore[method-assign]

        try:
            await engine.run(pipeline, mcp_pool=pool)
            # engine must NOT have called __aexit__
            assert not aexit_called, (
                "engine.run() must NOT close a caller-provided pool"
            )
        finally:
            # Manually close the pool (caller's responsibility)
            await original_aexit(None, None, None)

    async def test_external_pool_mcp_runner_wired(self):
        """The external pool is wired into McpRunner during run."""
        pipeline = _load(SIMPLE_PIPELINE)
        engine = PipelineEngine()

        wired_pools = []
        original_execute = engine._runners["cli"].execute

        # Spy: capture what pool is attached to McpRunner at run time
        async def spy_execute(step, context):
            wired_pools.append(engine._mcp_pool)
            return await original_execute(step, context)

        engine._runners["cli"].execute = spy_execute

        async with McpConnectionPool() as pool:
            await engine.run(pipeline, mcp_pool=pool)

        assert len(wired_pools) == 1
        assert wired_pools[0] is pool

    async def test_engine_pool_ref_cleared_after_external_run(self):
        """_mcp_pool is None after run even when pool was provided externally."""
        pipeline = _load(SIMPLE_PIPELINE)
        engine = PipelineEngine()

        async with McpConnectionPool() as pool:
            await engine.run(pipeline, mcp_pool=pool)

        assert engine._mcp_pool is None

    async def test_create_task_dispatch_does_not_crash(self):
        """Simulates the async dispatch pattern used by _handle_run_pipeline.

        The pool is opened OUTSIDE create_task; the background task receives
        the open pool and runs engine.run() without cancel-scope violations.
        """
        pipeline = _load(SIMPLE_PIPELINE)

        run_result = None

        bg_pool = McpConnectionPool()
        await bg_pool.__aenter__()

        async def _background():
            nonlocal run_result
            try:
                engine = PipelineEngine()
                run_result = await engine.run(pipeline, mcp_pool=bg_pool)
            finally:
                await bg_pool.__aexit__(None, None, None)

        task = asyncio.create_task(_background())
        await task  # should not raise

        assert run_result is not None
        assert run_result.success is True

    async def test_create_task_pool_closed_after_task_completes(self):
        """After the background task finishes, the pool's group is None."""
        pipeline = _load(SIMPLE_PIPELINE)

        bg_pool = McpConnectionPool()
        await bg_pool.__aenter__()

        async def _background():
            engine = PipelineEngine()
            await engine.run(pipeline, mcp_pool=bg_pool)
            await bg_pool.__aexit__(None, None, None)

        await asyncio.create_task(_background())

        assert bg_pool._group is None, "Pool group should be closed after task"
