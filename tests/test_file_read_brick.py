"""Tests for the file.read brick."""

import asyncio
from types import SimpleNamespace

from brix.runners.file_io import FileReadRunner


def _make_step(**kwargs):
    defaults = {"timeout": None, "params": {}}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_read_text_file(tmp_path):
    test_file = tmp_path / "hello.txt"
    test_file.write_text("Hello, Brix!", encoding="utf-8")

    runner = FileReadRunner()
    step = _make_step(params={"path": str(test_file)})
    result = _run(runner.execute(step, None))

    assert result["success"] is True
    assert result["data"]["text"] == "Hello, Brix!"
    assert result["data"]["size"] == len("Hello, Brix!".encode("utf-8"))
    assert result["data"]["name"] == "hello.txt"
    assert result["data"]["encoding"] == "utf-8"


def test_read_utf8_with_umlauts(tmp_path):
    test_file = tmp_path / "umlaut.txt"
    test_file.write_text("Fähre grüßt", encoding="utf-8")

    runner = FileReadRunner()
    step = _make_step(params={"path": str(test_file), "encoding": "utf-8"})
    result = _run(runner.execute(step, None))

    assert result["success"] is True
    assert result["data"]["text"] == "Fähre grüßt"


def test_file_not_found_returns_error():
    runner = FileReadRunner()
    step = _make_step(params={"path": "/nonexistent/file.txt"})
    result = _run(runner.execute(step, None))

    assert result["success"] is False
    assert "not found" in result["error"].lower()


def test_empty_file_returns_empty_text(tmp_path):
    test_file = tmp_path / "empty.txt"
    test_file.write_text("", encoding="utf-8")

    runner = FileReadRunner()
    step = _make_step(params={"path": str(test_file)})
    result = _run(runner.execute(step, None))

    assert result["success"] is True
    assert result["data"]["text"] == ""
    assert result["data"]["size"] == 0
