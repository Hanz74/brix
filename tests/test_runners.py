"""Tests for brix.runners modules."""

from brix.runners import base


def test_base_runner_module_exists():
    """Base runner module is importable and has a docstring."""
    assert base.__doc__ is not None
