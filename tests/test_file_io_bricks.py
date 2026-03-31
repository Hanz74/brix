"""Tests for File I/O bricks (T-BRIX-BRICK-02).

Tests:
- file.read_base64 (FileReadBase64Runner)
- file.write (FileWriteRunner)
- file.list (FileListRunner)
- file.load_json (FileLoadJsonRunner)
"""

import asyncio
import base64
import json
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from brix.runners.file_io import (
    FileListRunner,
    FileLoadJsonRunner,
    FileReadBase64Runner,
    FileWriteRunner,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_step(**kwargs):
    """Create a minimal step-like object for runner tests."""
    defaults = {"timeout": None, "params": {}}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# FileReadBase64Runner
# ---------------------------------------------------------------------------

class TestFileReadBase64Runner:

    def test_config_schema(self):
        runner = FileReadBase64Runner()
        schema = runner.config_schema()
        assert "path" in schema["properties"]
        assert "path" in schema["required"]

    def test_happy_path(self, tmp_path):
        runner = FileReadBase64Runner()
        test_file = tmp_path / "hello.txt"
        test_file.write_bytes(b"Hello, World!")

        step = _make_step(params={"path": str(test_file)})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["name"] == "hello.txt"
        assert data["size"] == 13
        decoded = base64.b64decode(data["base64"])
        assert decoded == b"Hello, World!"

    def test_binary_file(self, tmp_path):
        runner = FileReadBase64Runner()
        test_file = tmp_path / "data.bin"
        binary_content = bytes(range(256))
        test_file.write_bytes(binary_content)

        step = _make_step(params={"path": str(test_file)})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        decoded = base64.b64decode(result["data"]["base64"])
        assert decoded == binary_content

    def test_missing_path_param(self):
        runner = FileReadBase64Runner()
        step = _make_step(params={})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "requires 'path'" in result["error"]

    def test_file_not_found(self):
        runner = FileReadBase64Runner()
        step = _make_step(params={"path": "/nonexistent/file.txt"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "not found" in result["error"].lower()


# ---------------------------------------------------------------------------
# FileWriteRunner
# ---------------------------------------------------------------------------

class TestFileWriteRunner:

    def test_config_schema(self):
        runner = FileWriteRunner()
        schema = runner.config_schema()
        assert "path" in schema["properties"]
        assert "content" in schema["properties"]
        assert "mode" in schema["properties"]
        assert set(schema["required"]) == {"path", "content"}

    def test_write_text(self, tmp_path):
        runner = FileWriteRunner()
        out_file = tmp_path / "output.txt"

        step = _make_step(params={"path": str(out_file), "content": "Hello, Brix!"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["success"] is True
        assert data["path"] == str(out_file)
        assert out_file.read_text() == "Hello, Brix!"

    def test_write_binary(self, tmp_path):
        runner = FileWriteRunner()
        out_file = tmp_path / "data.bin"
        original = b"\x00\x01\x02\xff"
        b64_content = base64.b64encode(original).decode("ascii")

        step = _make_step(params={"path": str(out_file), "content": b64_content, "mode": "binary"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert out_file.read_bytes() == original

    def test_creates_parent_dirs(self, tmp_path):
        runner = FileWriteRunner()
        out_file = tmp_path / "sub" / "dir" / "file.txt"

        step = _make_step(params={"path": str(out_file), "content": "nested"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert out_file.read_text() == "nested"

    def test_write_dict_content(self, tmp_path):
        """Non-string content (dict) should be JSON-serialized."""
        runner = FileWriteRunner()
        out_file = tmp_path / "data.json"
        content = {"key": "value", "num": 42}

        step = _make_step(params={"path": str(out_file), "content": content})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        loaded = json.loads(out_file.read_text())
        assert loaded == content

    def test_missing_content(self, tmp_path):
        runner = FileWriteRunner()
        step = _make_step(params={"path": str(tmp_path / "x.txt")})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "requires 'content'" in result["error"]

    def test_missing_path(self):
        runner = FileWriteRunner()
        step = _make_step(params={"content": "hello"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "requires 'path'" in result["error"]


# ---------------------------------------------------------------------------
# FileListRunner
# ---------------------------------------------------------------------------

class TestFileListRunner:

    def test_config_schema(self):
        runner = FileListRunner()
        schema = runner.config_schema()
        assert "path" in schema["properties"]
        assert "pattern" in schema["properties"]
        assert "recursive" in schema["properties"]
        assert "path" in schema["required"]

    def test_list_directory(self, tmp_path):
        runner = FileListRunner()
        (tmp_path / "a.txt").write_text("aaa")
        (tmp_path / "b.pdf").write_text("bbb")
        (tmp_path / "c.txt").write_text("ccc")

        step = _make_step(params={"path": str(tmp_path)})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["count"] == 3
        names = [f["name"] for f in data["files"]]
        assert "a.txt" in names
        assert "b.pdf" in names

    def test_list_with_pattern(self, tmp_path):
        runner = FileListRunner()
        (tmp_path / "a.txt").write_text("aaa")
        (tmp_path / "b.pdf").write_text("bbb")
        (tmp_path / "c.txt").write_text("ccc")

        step = _make_step(params={"path": str(tmp_path), "pattern": "*.txt"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["count"] == 2
        names = [f["name"] for f in data["files"]]
        assert "a.txt" in names
        assert "c.txt" in names
        assert "b.pdf" not in names

    def test_list_recursive(self, tmp_path):
        runner = FileListRunner()
        (tmp_path / "top.txt").write_text("top")
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "deep.txt").write_text("deep")

        step = _make_step(params={"path": str(tmp_path), "pattern": "*.txt", "recursive": True})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        names = [f["name"] for f in result["data"]["files"]]
        assert "top.txt" in names
        assert "deep.txt" in names

    def test_file_metadata(self, tmp_path):
        runner = FileListRunner()
        (tmp_path / "test.txt").write_text("12345")

        step = _make_step(params={"path": str(tmp_path)})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        f = result["data"]["files"][0]
        assert f["name"] == "test.txt"
        assert f["size"] == 5
        assert "modified" in f
        assert f["path"] == str(tmp_path / "test.txt")

    def test_directory_not_found(self):
        runner = FileListRunner()
        step = _make_step(params={"path": "/nonexistent/directory"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_not_a_directory(self, tmp_path):
        runner = FileListRunner()
        f = tmp_path / "file.txt"
        f.write_text("not a dir")
        step = _make_step(params={"path": str(f)})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "not a directory" in result["error"].lower()

    def test_missing_path(self):
        runner = FileListRunner()
        step = _make_step(params={})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "requires 'path'" in result["error"]


# ---------------------------------------------------------------------------
# FileLoadJsonRunner
# ---------------------------------------------------------------------------

class TestFileLoadJsonRunner:

    def test_config_schema(self):
        runner = FileLoadJsonRunner()
        schema = runner.config_schema()
        assert "path" in schema["properties"]
        assert "path" in schema["required"]

    def test_load_dict(self, tmp_path):
        runner = FileLoadJsonRunner()
        data = {"name": "test", "values": [1, 2, 3]}
        f = tmp_path / "data.json"
        f.write_text(json.dumps(data))

        step = _make_step(params={"path": str(f)})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"] == data

    def test_load_list(self, tmp_path):
        runner = FileLoadJsonRunner()
        data = [{"id": 1}, {"id": 2}]
        f = tmp_path / "list.json"
        f.write_text(json.dumps(data))

        step = _make_step(params={"path": str(f)})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"] == data

    def test_invalid_json(self, tmp_path):
        runner = FileLoadJsonRunner()
        f = tmp_path / "bad.json"
        f.write_text("{not valid json")

        step = _make_step(params={"path": str(f)})
        result = _run(runner.execute(step, None))

        assert result["success"] is False
        assert "invalid json" in result["error"].lower()

    def test_file_not_found(self):
        runner = FileLoadJsonRunner()
        step = _make_step(params={"path": "/nonexistent/file.json"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_missing_path(self):
        runner = FileLoadJsonRunner()
        step = _make_step(params={})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "requires 'path'" in result["error"]
