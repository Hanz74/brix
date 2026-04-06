import asyncio

import pytest

from brix.helper_registry import HelperRegistry
from brix.runners.python import PythonRunner


class _Step:
    """Minimal step stand-in for runner tests."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _FakeProc:
    def __init__(self):
        self.returncode = 0

    async def communicate(self, input=None):
        return b'{"ok": true}', b""


@pytest.mark.asyncio
async def test_python_runner_resolves_helper_name_via_registry(monkeypatch, tmp_path, isolated_db):
    helper_path = tmp_path / "att_download_file.py"
    helper_path.write_text(
        "import json\n"
        "print(json.dumps({'ok': True}))\n",
        encoding="utf-8",
    )

    registry = HelperRegistry(registry_path=tmp_path / "registry.yaml", db=isolated_db)
    registry.register("att_download_file", str(helper_path), description="download helper")
    monkeypatch.setattr("brix.helper_registry.HelperRegistry", lambda: registry)

    called = {}

    async def fake_create_subprocess_exec(*cmd, **kwargs):
        called["cmd"] = cmd
        called["kwargs"] = kwargs
        return _FakeProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    runner = PythonRunner()
    step = _Step(helper="att_download_file")

    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert called["cmd"][:2] == ("python3", str(helper_path))


@pytest.mark.asyncio
async def test_python_runner_unknown_helper_returns_clear_error(monkeypatch, tmp_path, isolated_db):
    registry = HelperRegistry(registry_path=tmp_path / "registry.yaml", db=isolated_db)
    monkeypatch.setattr("brix.helper_registry.HelperRegistry", lambda: registry)

    async def fail_if_called(*args, **kwargs):
        raise AssertionError("subprocess should not be started for unknown helper")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fail_if_called)

    runner = PythonRunner()
    step = _Step(helper="missing_helper")

    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert result["error"] == "Helper 'missing_helper' not found in registry"
