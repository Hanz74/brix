"""Tests for brix package structure."""

import brix


def test_version():
    """Package exposes __version__."""
    assert brix.__version__  # version string exists and is truthy


def test_all_modules_importable():
    """All stub modules are importable without errors."""
    from brix import cli, engine, loader, context, models, registry, cache
    from brix.runners import base, python, http, cli as cli_runner, mcp, pipeline

    for mod in [cli, engine, loader, context, models, registry, cache,
                base, python, http, cli_runner, mcp, pipeline]:
        assert mod.__doc__ is not None
