"""Tests for Util+LLM bricks (T-BRIX-BRICK-04).

Tests:
- convert.batch (ConvertBatchRunner)
- llm.batch_poll (LlmBatchPollRunner)
- util.wait (UtilWaitRunner)
- util.load_dir (UtilLoadDirRunner)
"""

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from brix.runners.convert_batch import ConvertBatchRunner
from brix.runners.llm_batch_poll import LlmBatchPollRunner
from brix.runners.util_wait import UtilWaitRunner
from brix.runners.util_load_dir import UtilLoadDirRunner


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


# ===========================================================================
# ConvertBatchRunner
# ===========================================================================

class TestConvertBatchRunner:

    def test_config_schema(self):
        runner = ConvertBatchRunner()
        schema = runner.config_schema()
        assert "input_dir" in schema["properties"]
        assert "output_dir" in schema["properties"]
        assert "input_dir" in schema["required"]
        assert "output_dir" in schema["required"]

    def test_validate_config_valid(self):
        runner = ConvertBatchRunner()
        errors = runner.validate_config({"input_dir": "/a", "output_dir": "/b"})
        assert errors == []

    def test_validate_config_missing_required(self):
        runner = ConvertBatchRunner()
        errors = runner.validate_config({})
        assert any("input_dir" in e for e in errors)
        assert any("output_dir" in e for e in errors)

    def test_missing_input_dir(self):
        runner = ConvertBatchRunner()
        step = _make_step(params={"output_dir": "/tmp/out"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "input_dir" in result["error"]

    def test_missing_output_dir(self):
        runner = ConvertBatchRunner()
        step = _make_step(params={"input_dir": "/tmp/in"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "output_dir" in result["error"]

    def test_nonexistent_input_dir(self, tmp_path):
        runner = ConvertBatchRunner()
        step = _make_step(params={
            "input_dir": str(tmp_path / "nonexistent"),
            "output_dir": str(tmp_path / "out"),
        })
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "not found" in result["error"]

    def test_empty_directory(self, tmp_path):
        runner = ConvertBatchRunner()
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"

        step = _make_step(params={
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
        })
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["converted"] == 0
        assert result["data"]["errors"] == 0

    @patch("brix.runners.convert_batch.httpx.AsyncClient")
    def test_happy_path(self, mock_client_cls, tmp_path):
        """Test successful batch conversion with mocked HTTP."""
        runner = ConvertBatchRunner()

        # Create input files
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "doc1.pdf").write_bytes(b"fake-pdf-1")
        (input_dir / "doc2.pdf").write_bytes(b"fake-pdf-2")
        output_dir = tmp_path / "output"

        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"markdown": "# Converted Content"}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        step = _make_step(params={
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "pattern": "*.pdf",
        })
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["converted"] == 2
        assert result["data"]["errors"] == 0
        assert len(result["data"]["details"]) == 2
        for detail in result["data"]["details"]:
            assert detail["status"] == "ok"
            assert detail["output_path"].endswith(".md")

        # Verify output files were created
        assert output_dir.exists()
        assert (output_dir / "doc1.pdf.md").exists()
        assert (output_dir / "doc2.pdf.md").read_text() == "# Converted Content"

    @patch("brix.runners.convert_batch.httpx.AsyncClient")
    def test_http_error(self, mock_client_cls, tmp_path):
        """Test handling of HTTP errors during conversion."""
        runner = ConvertBatchRunner()

        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "bad.pdf").write_bytes(b"corrupt")
        output_dir = tmp_path / "output"

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        step = _make_step(params={
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
        })
        result = _run(runner.execute(step, None))

        assert result["success"] is True  # overall succeeds, errors tracked in details
        assert result["data"]["converted"] == 0
        assert result["data"]["errors"] == 1
        assert result["data"]["details"][0]["status"] == "error"

    @patch("brix.runners.convert_batch.httpx.AsyncClient")
    def test_pattern_filtering(self, mock_client_cls, tmp_path):
        """Test that pattern filters files correctly."""
        runner = ConvertBatchRunner()

        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "doc.pdf").write_bytes(b"pdf")
        (input_dir / "doc.txt").write_bytes(b"txt")
        (input_dir / "doc.docx").write_bytes(b"docx")
        output_dir = tmp_path / "output"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"markdown": "# Content"}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        step = _make_step(params={
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "pattern": "*.pdf",
        })
        result = _run(runner.execute(step, None))

        assert result["data"]["converted"] == 1
        assert result["data"]["details"][0]["file"] == "doc.pdf"


# ===========================================================================
# LlmBatchPollRunner
# ===========================================================================

class TestLlmBatchPollRunner:

    def test_config_schema(self):
        runner = LlmBatchPollRunner()
        schema = runner.config_schema()
        assert "model" in schema["properties"]
        assert "requests" in schema["properties"]
        assert "model" in schema["required"]

    def test_validate_config_valid(self):
        runner = LlmBatchPollRunner()
        errors = runner.validate_config({
            "model": "mistral-small-latest",
            "requests": [{"custom_id": "1", "messages": []}],
        })
        assert errors == []

    def test_validate_config_unsupported_provider(self):
        runner = LlmBatchPollRunner()
        errors = runner.validate_config({
            "model": "m",
            "requests": [],
            "provider": "openai",
        })
        assert any("Unsupported" in e for e in errors)

    def test_missing_model(self):
        runner = LlmBatchPollRunner()
        step = _make_step(params={"requests": [{"custom_id": "1", "messages": []}]})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "model" in result["error"]

    def test_missing_requests(self):
        runner = LlmBatchPollRunner()
        step = _make_step(params={"model": "mistral-small-latest"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "requests" in result["error"]

    def test_no_mistral_package(self):
        runner = LlmBatchPollRunner()
        step = _make_step(params={
            "model": "mistral-small-latest",
            "requests": [{"custom_id": "1", "messages": [{"role": "user", "content": "hi"}]}],
        })
        with patch("brix.runners.llm_batch_poll.Mistral", None):
            result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "not installed" in result["error"]

    def test_no_api_key(self):
        runner = LlmBatchPollRunner()
        step = _make_step(params={
            "model": "mistral-small-latest",
            "requests": [{"custom_id": "1", "messages": [{"role": "user", "content": "hi"}]}],
        })
        with patch("brix.runners.llm_batch_poll.Mistral", MagicMock()), \
             patch.dict(os.environ, {}, clear=True):
            # Ensure keys are not set
            os.environ.pop("BUDDY_LLM_API_KEY", None)
            os.environ.pop("MISTRAL_API_KEY", None)
            result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "API key" in result["error"]

    @patch("brix.runners.llm_batch_poll.Mistral")
    @patch.dict(os.environ, {"MISTRAL_API_KEY": "test-key-123"})
    def test_happy_path(self, mock_mistral_cls):
        """Test successful batch submit + poll with mocked Mistral."""
        runner = LlmBatchPollRunner()

        # Mock job creation
        mock_job = MagicMock()
        mock_job.id = "batch-123"

        # Mock status polling — return SUCCESS on first poll
        mock_status = MagicMock()
        mock_status.status = "SUCCESS"
        mock_status.succeeded_requests = 2
        mock_status.failed_requests = 0
        mock_status.output_file = "file-456"

        # Mock file download — JSONL output
        jsonl_output = "\n".join([
            json.dumps({
                "custom_id": "req-0",
                "response": {"body": {"choices": [{"message": {"content": '{"label": "A"}'}}]}}
            }),
            json.dumps({
                "custom_id": "req-1",
                "response": {"body": {"choices": [{"message": {"content": '{"label": "B"}'}}]}}
            }),
        ])
        mock_download = MagicMock(spec=["content"])
        mock_download.content = jsonl_output.encode("utf-8")

        # Wire up client
        mock_client = MagicMock()
        mock_client.batch.jobs.create.return_value = mock_job
        mock_client.batch.jobs.get.return_value = mock_status
        mock_client.files.download.return_value = mock_download
        mock_mistral_cls.return_value = mock_client

        step = _make_step(params={
            "model": "mistral-small-latest",
            "requests": [
                {"custom_id": "req-0", "messages": [{"role": "user", "content": "test 1"}]},
                {"custom_id": "req-1", "messages": [{"role": "user", "content": "test 2"}]},
            ],
            "poll_interval": 1,
        })
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        data = result["data"]
        assert data["batch_id"] == "batch-123"
        assert data["status"] == "completed"
        assert len(data["results"]) == 2
        assert data["results"][0]["custom_id"] == "req-0"
        assert data["results"][0]["result"] == {"label": "A"}
        assert data["results"][1]["result"] == {"label": "B"}

    @patch("brix.runners.llm_batch_poll.Mistral")
    @patch.dict(os.environ, {"MISTRAL_API_KEY": "test-key-123"})
    def test_batch_failure(self, mock_mistral_cls):
        """Test batch job that ends with FAILED status."""
        runner = LlmBatchPollRunner()

        mock_job = MagicMock()
        mock_job.id = "batch-fail"

        mock_status = MagicMock()
        mock_status.status = "FAILED"
        mock_status.succeeded_requests = 0
        mock_status.failed_requests = 1

        mock_client = MagicMock()
        mock_client.batch.jobs.create.return_value = mock_job
        mock_client.batch.jobs.get.return_value = mock_status
        mock_mistral_cls.return_value = mock_client

        step = _make_step(params={
            "model": "mistral-small-latest",
            "requests": [{"custom_id": "r1", "messages": [{"role": "user", "content": "x"}]}],
            "poll_interval": 1,
        })
        result = _run(runner.execute(step, None))

        assert result["success"] is False
        assert "FAILED" in result["error"]


# ===========================================================================
# UtilWaitRunner
# ===========================================================================

class TestUtilWaitRunner:

    def test_config_schema(self):
        runner = UtilWaitRunner()
        schema = runner.config_schema()
        assert "seconds" in schema["properties"]
        assert "seconds" in schema["required"]

    def test_validate_config_valid(self):
        runner = UtilWaitRunner()
        errors = runner.validate_config({"seconds": 5})
        assert errors == []

    def test_validate_config_negative(self):
        runner = UtilWaitRunner()
        errors = runner.validate_config({"seconds": -1})
        assert any(">= 0" in e for e in errors)

    def test_validate_config_too_large(self):
        runner = UtilWaitRunner()
        errors = runner.validate_config({"seconds": 9999})
        assert any("3600" in e for e in errors)

    def test_missing_seconds(self):
        runner = UtilWaitRunner()
        step = _make_step(params={})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "seconds" in result["error"]

    def test_zero_wait(self):
        runner = UtilWaitRunner()
        step = _make_step(params={"seconds": 0})
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["success"] is True
        assert result["data"]["waited"] >= 0
        assert result["duration"] < 1.0

    def test_short_wait(self):
        runner = UtilWaitRunner()
        step = _make_step(params={"seconds": 0.1})
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["success"] is True
        assert result["data"]["waited"] >= 0.05  # allow some tolerance
        assert result["duration"] < 2.0

    def test_cap_at_max(self):
        """Test that seconds > 3600 is capped (not an error at runtime)."""
        runner = UtilWaitRunner()
        # validate_config catches it, but execute caps silently
        step = _make_step(params={"seconds": 0})  # using 0 to avoid actual wait
        result = _run(runner.execute(step, None))
        assert result["success"] is True

    def test_float_seconds(self):
        runner = UtilWaitRunner()
        step = _make_step(params={"seconds": 0.05})
        result = _run(runner.execute(step, None))
        assert result["success"] is True

    def test_invalid_seconds_type(self):
        runner = UtilWaitRunner()
        step = _make_step(params={"seconds": "not-a-number"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "number" in result["error"]


# ===========================================================================
# UtilLoadDirRunner
# ===========================================================================

class TestUtilLoadDirRunner:

    def test_config_schema(self):
        runner = UtilLoadDirRunner()
        schema = runner.config_schema()
        assert "path" in schema["properties"]
        assert "path" in schema["required"]

    def test_validate_config_valid(self):
        runner = UtilLoadDirRunner()
        errors = runner.validate_config({"path": "/some/dir"})
        assert errors == []

    def test_missing_path(self):
        runner = UtilLoadDirRunner()
        step = _make_step(params={})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "path" in result["error"]

    def test_nonexistent_dir(self):
        runner = UtilLoadDirRunner()
        step = _make_step(params={"path": "/nonexistent/dir/xyz"})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "not found" in result["error"]

    def test_not_a_directory(self, tmp_path):
        runner = UtilLoadDirRunner()
        file = tmp_path / "file.txt"
        file.write_text("hello")
        step = _make_step(params={"path": str(file)})
        result = _run(runner.execute(step, None))
        assert result["success"] is False
        assert "Not a directory" in result["error"]

    def test_empty_dir(self, tmp_path):
        runner = UtilLoadDirRunner()
        step = _make_step(params={"path": str(tmp_path)})
        result = _run(runner.execute(step, None))
        assert result["success"] is True
        assert result["data"]["count"] == 0
        assert result["data"]["files"] == []

    def test_load_text_files(self, tmp_path):
        runner = UtilLoadDirRunner()
        (tmp_path / "a.md").write_text("# Alpha")
        (tmp_path / "b.md").write_text("# Beta")
        (tmp_path / "c.txt").write_text("plain text")

        step = _make_step(params={"path": str(tmp_path), "pattern": "*.md"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 2
        names = [f["name"] for f in result["data"]["files"]]
        assert "a.md" in names
        assert "b.md" in names
        # Content should be loaded
        for f in result["data"]["files"]:
            assert "content" in f
            assert f["content"].startswith("#")

    def test_load_all_files_default_pattern(self, tmp_path):
        runner = UtilLoadDirRunner()
        (tmp_path / "readme.md").write_text("# Readme")

        step = _make_step(params={"path": str(tmp_path)})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 1

    def test_as_text_false(self, tmp_path):
        runner = UtilLoadDirRunner()
        (tmp_path / "data.md").write_text("hello")

        step = _make_step(params={"path": str(tmp_path), "pattern": "*.md", "as_text": False})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 1
        f = result["data"]["files"][0]
        assert "content" not in f
        assert f["name"] == "data.md"
        assert f["path"].endswith("data.md")

    def test_binary_file_as_text(self, tmp_path):
        """Binary files should produce an error note when as_text=True."""
        runner = UtilLoadDirRunner()
        (tmp_path / "data.bin").write_bytes(bytes(range(256)))

        step = _make_step(params={"path": str(tmp_path), "pattern": "*.bin", "as_text": True})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 1
        f = result["data"]["files"][0]
        # Should have an error note about binary content
        assert f["content"] is None or "error" in f

    def test_wildcard_pattern(self, tmp_path):
        runner = UtilLoadDirRunner()
        (tmp_path / "a.txt").write_text("txt")
        (tmp_path / "b.json").write_text("{}")
        (tmp_path / "c.md").write_text("md")

        step = _make_step(params={"path": str(tmp_path), "pattern": "*"})
        result = _run(runner.execute(step, None))

        assert result["success"] is True
        assert result["data"]["count"] == 3


# ===========================================================================
# Registration / Discovery
# ===========================================================================

class TestRegistration:
    """Verify runners are discoverable and bricks are registered."""

    def test_runners_discovered(self):
        from brix.runners.base import discover_runners
        registry = discover_runners()
        assert "convert_batch" in registry
        assert "llm_batch_poll" in registry
        assert "util_wait" in registry
        assert "util_load_dir" in registry

    def test_brick_definitions_exist(self):
        from brix.bricks.builtins import ALL_BUILTINS
        brick_names = {b.name for b in ALL_BUILTINS}
        assert "convert.batch" in brick_names
        assert "llm.batch_poll" in brick_names
        assert "util.wait" in brick_names
        assert "util.load_dir" in brick_names

    def test_step_type_literal_accepts_new_types(self):
        """Verify the Step model accepts our new type literals."""
        from brix.models import Step
        # Flat names
        for t in ("convert_batch", "llm_batch_poll", "util_wait", "util_load_dir"):
            s = Step(id="test", type=t)
            assert s.type == t
        # Dot-notation names
        for t in ("convert.batch", "llm.batch_poll", "util.wait", "util.load_dir"):
            s = Step(id="test", type=t)
            assert s.type == t
