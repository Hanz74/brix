"""Tests for LlmBatchRunner — Mistral Batch API runner.

All tests mock the Mistral client; no real API calls are made.
"""
import asyncio
import json
import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from brix.runners.llm_batch import LlmBatchRunner, _strip_fences


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal stand-in for a pipeline Step object."""

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)
        # Ensure params is always present
        if not hasattr(self, "params"):
            self.params = {}


class _Context:
    """Minimal stand-in for a pipeline execution context."""

    def __init__(self, last_output: Any = None) -> None:
        self.last_output = last_output


def _make_job(job_id: str = "job-123") -> MagicMock:
    job = MagicMock()
    job.id = job_id
    return job


def _make_status(
    status: str = "SUCCESS",
    succeeded: int = 2,
    failed: int = 0,
    output_file: str = "file-abc",
) -> MagicMock:
    s = MagicMock()
    s.status = status
    s.succeeded_requests = succeeded
    s.failed_requests = failed
    s.output_file = output_file
    return s


def _make_jsonl_response(entries: list[dict]) -> bytes:
    """Build a fake JSONL output file content from a list of result dicts."""
    lines = []
    for entry in entries:
        lines.append(json.dumps(entry))
    return "\n".join(lines).encode("utf-8")


def _mock_download_response(content: bytes) -> MagicMock:
    resp = MagicMock()
    resp.read.return_value = content
    return resp


# ---------------------------------------------------------------------------
# Unit tests: pure helpers
# ---------------------------------------------------------------------------


def test_strip_fences_plain():
    """Text without fences is returned unchanged."""
    assert _strip_fences('{"a": 1}') == '{"a": 1}'


def test_strip_fences_json_fence():
    """```json ... ``` fences are removed."""
    text = "```json\n{\"a\": 1}\n```"
    assert _strip_fences(text) == '{"a": 1}'


def test_strip_fences_plain_fence():
    """Plain ``` ... ``` fences are removed."""
    text = "```\n{\"a\": 1}\n```"
    assert _strip_fences(text) == '{"a": 1}'


def test_strip_fences_no_newline():
    """Fences without inner newlines are handled."""
    text = "```json\n{\"x\": 2}```"
    result = _strip_fences(text)
    # Should strip fence wrapper
    assert "```" not in result
    assert '{"x": 2}' in result


# ---------------------------------------------------------------------------
# Runner metadata
# ---------------------------------------------------------------------------


def test_config_schema():
    runner = LlmBatchRunner()
    schema = runner.config_schema()
    assert schema["type"] == "object"
    assert "system_prompt" in schema["properties"]
    assert "user_template" in schema["properties"]
    assert "model" in schema["properties"]
    assert "output_schema" in schema["properties"]
    assert "batch_size" in schema["properties"]
    assert "timeout" in schema["properties"]
    assert "temperature" in schema["properties"]
    assert "system_prompt" in schema["required"]
    assert "user_template" in schema["required"]


def test_input_type():
    assert LlmBatchRunner().input_type() == "list[dict]"


def test_output_type():
    assert LlmBatchRunner().output_type() == "list[dict]"


def test_validate_config_missing_required():
    runner = LlmBatchRunner()
    errors = runner.validate_config({})
    assert any("system_prompt" in e for e in errors)
    assert any("user_template" in e for e in errors)


def test_validate_config_ok():
    runner = LlmBatchRunner()
    errors = runner.validate_config({"system_prompt": "sys", "user_template": "tpl"})
    assert errors == []


# ---------------------------------------------------------------------------
# Error cases (no real API calls)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_api_key(monkeypatch):
    """Runner returns error when API key env var is absent."""
    monkeypatch.delenv("BUDDY_LLM_API_KEY", raising=False)
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)

    runner = LlmBatchRunner()
    step = _Step(system_prompt="s", user_template="t")
    ctx = _Context(last_output=[{"id": "1", "text": "hello"}])

    # Patch Mistral to a non-None sentinel so the import check passes
    with patch("brix.runners.llm_batch.Mistral", MagicMock()):
        result = await runner.execute(step, ctx)

    assert result["success"] is False
    assert "API key" in result["error"]


@pytest.mark.asyncio
async def test_no_items(monkeypatch):
    """Runner returns error when item list is empty."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    runner = LlmBatchRunner()
    step = _Step(system_prompt="s", user_template="t")
    ctx = _Context(last_output=[])

    with patch("brix.runners.llm_batch.Mistral"):
        result = await runner.execute(step, ctx)

    assert result["success"] is False
    assert "no items" in result["error"].lower()


@pytest.mark.asyncio
async def test_missing_mistral_import(monkeypatch):
    """Runner returns error when mistralai is not installed."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    runner = LlmBatchRunner()
    step = _Step(system_prompt="s", user_template="t")
    ctx = _Context(last_output=[{"id": "1"}])

    with patch("brix.runners.llm_batch.Mistral", None):
        result = await runner.execute(step, ctx)

    assert result["success"] is False
    assert "mistralai" in result["error"].lower()


@pytest.mark.asyncio
async def test_missing_system_prompt(monkeypatch):
    """Runner returns error when system_prompt is missing."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    runner = LlmBatchRunner()
    step = _Step(user_template="say {{item.text}}")
    ctx = _Context(last_output=[{"id": "1"}])

    with patch("brix.runners.llm_batch.Mistral"):
        result = await runner.execute(step, ctx)

    assert result["success"] is False
    assert "system_prompt" in result["error"]


# ---------------------------------------------------------------------------
# Happy path: successful batch submission and result parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_successful_batch(monkeypatch):
    """Full happy path: submit → poll SUCCESS → parse JSONL output."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    items = [
        {"id": "doc-1", "text": "Hello"},
        {"id": "doc-2", "text": "World"},
    ]

    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "doc-1",
            "response": {
                "body": {
                    "choices": [{"message": {"content": '{"label": "positive"}'}}],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5},
                }
            },
        },
        {
            "custom_id": "doc-2",
            "response": {
                "body": {
                    "choices": [{"message": {"content": '{"label": "neutral"}'}}],
                    "usage": {"prompt_tokens": 12, "completion_tokens": 6},
                }
            },
        },
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.return_value = _make_job("job-001")
    mock_client.batch.jobs.get.return_value = _make_status(
        "SUCCESS", succeeded=2, output_file="file-001"
    )
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(
            system_prompt="Classify sentiment",
            user_template="Text: {{ item.text }}",
        )
        ctx = _Context(last_output=items)
        result = await runner.execute(step, ctx)

    assert result["success"] is True
    data = result["data"]
    assert len(data) == 2

    by_id = {d["custom_id"]: d for d in data}
    assert by_id["doc-1"]["result"] == {"label": "positive"}
    assert by_id["doc-2"]["result"] == {"label": "neutral"}
    assert result["total"] == 2


@pytest.mark.asyncio
async def test_batch_submission_jsonl_format(monkeypatch):
    """Batch job is submitted with correct JSONL-compatible request structure."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    captured_requests: list = []

    def fake_create(requests, model, endpoint):
        captured_requests.extend(requests)
        return _make_job("job-capture")

    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "item-0",
            "response": {
                "body": {
                    "choices": [{"message": {"content": "ok"}}],
                    "usage": {},
                }
            },
        }
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.side_effect = fake_create
    mock_client.batch.jobs.get.return_value = _make_status("SUCCESS", succeeded=1, output_file="f1")
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(
            system_prompt="You are helpful.",
            user_template="Process: {{ item.text }}",
        )
        ctx = _Context(last_output=[{"text": "sample data"}])
        result = await runner.execute(step, ctx)

    assert result["success"] is True
    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert "custom_id" in req
    assert "body" in req
    body = req["body"]
    assert body["messages"][0]["role"] == "system"
    assert body["messages"][0]["content"] == "You are helpful."
    assert body["messages"][1]["role"] == "user"
    assert "sample data" in body["messages"][1]["content"]


@pytest.mark.asyncio
async def test_markdown_fence_cleanup(monkeypatch):
    """JSON wrapped in Markdown fences is correctly parsed."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    items = [{"id": "x1", "text": "test"}]
    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "x1",
            "response": {
                "body": {
                    "choices": [{"message": {"content": "```json\n{\"score\": 42}\n```"}}],
                    "usage": {},
                }
            },
        }
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.return_value = _make_job("job-fence")
    mock_client.batch.jobs.get.return_value = _make_status("SUCCESS", succeeded=1, output_file="f2")
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(system_prompt="s", user_template="t")
        ctx = _Context(last_output=items)
        result = await runner.execute(step, ctx)

    assert result["success"] is True
    assert result["data"][0]["result"] == {"score": 42}


@pytest.mark.asyncio
async def test_single_item_error_in_output(monkeypatch):
    """Individual item errors in batch output are captured, not propagated."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    items = [{"id": "ok-1"}, {"id": "bad-2"}]
    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "ok-1",
            "response": {
                "body": {
                    "choices": [{"message": {"content": '{"ok": true}'}}],
                    "usage": {},
                }
            },
        },
        {
            "custom_id": "bad-2",
            "error": {"message": "Content filter triggered", "type": "content_filter"},
        },
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.return_value = _make_job("job-err")
    mock_client.batch.jobs.get.return_value = _make_status("SUCCESS", succeeded=1, failed=1, output_file="f3")
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(system_prompt="s", user_template="t")
        ctx = _Context(last_output=items)
        result = await runner.execute(step, ctx)

    # Overall job succeeded — individual item errors are in data
    assert result["success"] is True
    data = result["data"]
    by_id = {d["custom_id"]: d for d in data}
    assert by_id["ok-1"]["result"] == {"ok": True}
    assert by_id["bad-2"]["error"] is not None
    assert by_id["bad-2"]["result"] is None


@pytest.mark.asyncio
async def test_polling_loop_with_progress(monkeypatch):
    """Polling progresses through PENDING → SUCCESS and progress is reported."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    poll_call_count = 0

    def fake_get(job_id):
        nonlocal poll_call_count
        poll_call_count += 1
        if poll_call_count < 3:
            return _make_status("RUNNING", succeeded=0, failed=0, output_file=None)
        return _make_status("SUCCESS", succeeded=1, output_file="f4")

    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "item-0",
            "response": {
                "body": {
                    "choices": [{"message": {"content": "done"}}],
                    "usage": {},
                }
            },
        }
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.return_value = _make_job("job-poll")
    mock_client.batch.jobs.get.side_effect = fake_get
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    progress_snapshots: list[dict] = []
    original_report = LlmBatchRunner.report_progress

    def tracking_report(self, pct, msg="", done=0, total=0):
        progress_snapshots.append({"pct": pct, "msg": msg})
        original_report(self, pct, msg, done, total)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client), \
         patch("brix.runners.llm_batch._POLL_INTERVAL", 0), \
         patch.object(LlmBatchRunner, "report_progress", tracking_report):
        runner = LlmBatchRunner()
        step = _Step(system_prompt="s", user_template="t")
        ctx = _Context(last_output=[{"text": "hello"}])
        result = await runner.execute(step, ctx)

    assert result["success"] is True
    assert poll_call_count >= 3
    # Final progress must be 100%
    assert progress_snapshots[-1]["pct"] == 100.0


@pytest.mark.asyncio
async def test_timeout_handling(monkeypatch):
    """Batch runner returns error when timeout is exceeded during polling."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    def fake_get(job_id):
        return _make_status("RUNNING", succeeded=0, failed=0, output_file=None)

    mock_client = MagicMock()
    mock_client.batch.jobs.create.return_value = _make_job("job-timeout")
    mock_client.batch.jobs.get.side_effect = fake_get

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client), \
         patch("brix.runners.llm_batch._POLL_INTERVAL", 0):
        runner = LlmBatchRunner()
        # timeout=0 should trigger immediately after first poll
        step = _Step(system_prompt="s", user_template="t", timeout=0)
        ctx = _Context(last_output=[{"id": "t1"}])
        result = await runner.execute(step, ctx)

    assert result["success"] is False
    assert "Timeout" in result["error"] or "timeout" in result["error"].lower()
    assert "job_id" in result


@pytest.mark.asyncio
async def test_batch_job_failed_status(monkeypatch):
    """FAILED batch job status returns success=False."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    mock_client = MagicMock()
    mock_client.batch.jobs.create.return_value = _make_job("job-fail")
    mock_client.batch.jobs.get.return_value = _make_status("FAILED", succeeded=0, failed=2, output_file=None)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(system_prompt="s", user_template="t")
        ctx = _Context(last_output=[{"id": "x1"}, {"id": "x2"}])
        result = await runner.execute(step, ctx)

    assert result["success"] is False
    assert "FAILED" in result["error"]
    assert result["job_id"] == "job-fail"


@pytest.mark.asyncio
async def test_output_schema_forwarded(monkeypatch):
    """output_schema is forwarded as response_format to the batch request."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    captured_requests: list = []

    def fake_create(requests, model, endpoint):
        captured_requests.extend(requests)
        return _make_job("job-schema")

    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "item-0",
            "response": {
                "body": {
                    "choices": [{"message": {"content": '{"label": "x"}'}}],
                    "usage": {},
                }
            },
        }
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.side_effect = fake_create
    mock_client.batch.jobs.get.return_value = _make_status("SUCCESS", succeeded=1, output_file="fs")
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    schema = {"type": "object", "properties": {"label": {"type": "string"}}}

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(
            system_prompt="classify",
            user_template="{{ item.text }}",
            output_schema=schema,
        )
        ctx = _Context(last_output=[{"text": "data"}])
        result = await runner.execute(step, ctx)

    assert result["success"] is True
    req = captured_requests[0]
    rf = req["body"].get("response_format")
    assert rf is not None
    assert rf["type"] == "json_schema"
    assert rf["json_schema"]["schema"] == schema


@pytest.mark.asyncio
async def test_non_json_content_returned_as_string(monkeypatch):
    """Content that is not valid JSON is returned as a plain string."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "item-0",
            "response": {
                "body": {
                    "choices": [{"message": {"content": "This is plain text, not JSON."}}],
                    "usage": {},
                }
            },
        }
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.return_value = _make_job("job-plain")
    mock_client.batch.jobs.get.return_value = _make_status("SUCCESS", succeeded=1, output_file="fp")
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(system_prompt="s", user_template="t")
        ctx = _Context(last_output=[{"id": "p1"}])
        result = await runner.execute(step, ctx)

    assert result["success"] is True
    assert result["data"][0]["result"] == "This is plain text, not JSON."


@pytest.mark.asyncio
async def test_items_from_step_param(monkeypatch):
    """Items can be provided directly via step param 'items' when context is empty."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "item-0",
            "response": {
                "body": {
                    "choices": [{"message": {"content": "ok"}}],
                    "usage": {},
                }
            },
        }
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.return_value = _make_job("job-param")
    mock_client.batch.jobs.get.return_value = _make_status("SUCCESS", succeeded=1, output_file="fx")
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(
            system_prompt="s",
            user_template="t",
            items=[{"text": "from param"}],
        )
        result = await runner.execute(step, context=None)

    assert result["success"] is True


@pytest.mark.asyncio
async def test_custom_id_from_item_id(monkeypatch):
    """custom_id in batch request uses item['id'] when present."""
    monkeypatch.setenv("BUDDY_LLM_API_KEY", "test-key")

    captured: list = []

    def fake_create(requests, model, endpoint):
        captured.extend(requests)
        return _make_job("job-id")

    jsonl_output = _make_jsonl_response([
        {
            "custom_id": "my-custom-123",
            "response": {
                "body": {
                    "choices": [{"message": {"content": "ok"}}],
                    "usage": {},
                }
            },
        }
    ])

    mock_client = MagicMock()
    mock_client.batch.jobs.create.side_effect = fake_create
    mock_client.batch.jobs.get.return_value = _make_status("SUCCESS", succeeded=1, output_file="fid")
    mock_client.files.download.return_value = _mock_download_response(jsonl_output)

    with patch("brix.runners.llm_batch.Mistral", return_value=mock_client):
        runner = LlmBatchRunner()
        step = _Step(system_prompt="s", user_template="t")
        ctx = _Context(last_output=[{"id": "my-custom-123", "text": "hi"}])
        result = await runner.execute(step, ctx)

    assert result["success"] is True
    assert captured[0]["custom_id"] == "my-custom-123"
