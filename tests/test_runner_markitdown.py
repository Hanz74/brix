"""Tests for MarkitdownRunner.

All tests mock httpx — no real markitdown-mcp service calls are made.
"""
import asyncio
import base64
import json
import os
import tempfile
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from brix.runners.markitdown import MarkitdownRunner, _looks_like_file_path


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class _Step:
    """Minimal stand-in for a pipeline Step object."""

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)
        if not hasattr(self, "params"):
            self.params = {}


class _Context:
    """Minimal stand-in for a pipeline execution context."""

    def __init__(self, last_output: Any = None) -> None:
        self.last_output = last_output


def _mock_response(status_code: int = 200, body: dict | str | None = None) -> MagicMock:
    """Build a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    if isinstance(body, dict):
        resp.json.return_value = body
        resp.text = json.dumps(body)
    else:
        resp.json.side_effect = ValueError("not json")
        resp.text = body or ""
    return resp


def _make_runner() -> MarkitdownRunner:
    return MarkitdownRunner()


def _run(coro: Any) -> Any:
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# 1. config_schema / input_type / output_type
# ---------------------------------------------------------------------------


def test_config_schema_structure():
    runner = _make_runner()
    schema = runner.config_schema()
    assert schema["type"] == "object"
    props = schema["properties"]
    assert "input" in props
    assert "auto_extract" in props
    assert "language" in props
    assert "filename" in props
    assert "template" in props
    # input is not required (can come from context)
    assert schema.get("required", []) == []


def test_input_type():
    assert _make_runner().input_type() == "dict"


def test_output_type():
    assert _make_runner().output_type() == "dict"


# ---------------------------------------------------------------------------
# 2. File path → base64-encode → POST /v1/convert
# ---------------------------------------------------------------------------


def test_file_path_is_base64_encoded():
    """When input is a file path, the runner reads and base64-encodes it."""
    content = b"Hello PDF content"
    expected_b64 = base64.b64encode(content).decode("ascii")

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(content)
        f.flush()
        tmp_path = f.name

    try:
        step = _Step(input=tmp_path, language="de")
        ctx = _Context()

        service_response = {"markdown": "# Hello", "metadata": {"pages": 1}, "extracted": {}}
        mock_resp = _mock_response(200, service_response)

        captured_payload: dict = {}

        async def fake_post(url, **kwargs):
            captured_payload.update(kwargs.get("json", {}))
            return mock_resp

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(side_effect=fake_post)
            mock_client_cls.return_value = mock_client

            result = _run(_make_runner().execute(step, ctx))

        assert result["success"] is True
        assert captured_payload["content"] == expected_b64
        # filename should be detected automatically
        assert "filename" in captured_payload
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# 3. auto_extract=True → POST to /v1/extract
# ---------------------------------------------------------------------------


def test_auto_extract_uses_extract_endpoint():
    """auto_extract=True must POST to /v1/extract, not /v1/convert."""
    raw_b64 = base64.b64encode(b"dummy").decode("ascii")
    step = _Step(input=raw_b64, auto_extract=True, language="de")
    ctx = _Context()

    service_response = {"markdown": "extracted", "metadata": {}, "extracted": {"key": "val"}}
    mock_resp = _mock_response(200, service_response)

    captured_urls: list[str] = []

    async def fake_post(url, **kwargs):
        captured_urls.append(url)
        return mock_resp

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=fake_post)
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is True
    assert len(captured_urls) == 1
    assert captured_urls[0].endswith("/v1/extract")


def test_no_auto_extract_uses_convert_endpoint():
    """auto_extract=False (default) must POST to /v1/convert."""
    raw_b64 = base64.b64encode(b"dummy").decode("ascii")
    step = _Step(input=raw_b64, auto_extract=False)
    ctx = _Context()

    mock_resp = _mock_response(200, {"markdown": "hello", "metadata": {}, "extracted": {}})
    captured_urls: list[str] = []

    async def fake_post(url, **kwargs):
        captured_urls.append(url)
        return mock_resp

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=fake_post)
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is True
    assert captured_urls[0].endswith("/v1/convert")


# ---------------------------------------------------------------------------
# 4. Response parsing
# ---------------------------------------------------------------------------


def test_response_parsed_to_dict():
    """Response keys markdown/metadata/extracted are forwarded correctly."""
    raw_b64 = base64.b64encode(b"test").decode("ascii")
    step = _Step(input=raw_b64)
    ctx = _Context()

    service_response = {
        "markdown": "# Title\nBody text",
        "metadata": {"pages": 3, "author": "Alice"},
        "extracted": {"invoice_total": "99.00"},
    }
    mock_resp = _mock_response(200, service_response)

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is True
    data = result["data"]
    assert data["markdown"] == "# Title\nBody text"
    assert data["metadata"]["pages"] == 3
    assert data["extracted"]["invoice_total"] == "99.00"


def test_non_json_response_wrapped():
    """Non-JSON response text is wrapped under 'markdown' key."""
    raw_b64 = base64.b64encode(b"data").decode("ascii")
    step = _Step(input=raw_b64)
    ctx = _Context()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.side_effect = ValueError("not json")
    mock_resp.text = "plain text output"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is True
    assert result["data"]["markdown"] == "plain text output"
    assert result["data"]["metadata"] == {}
    assert result["data"]["extracted"] == {}


# ---------------------------------------------------------------------------
# 5. Error cases
# ---------------------------------------------------------------------------


def test_http_error_returns_failure():
    """A 4xx/5xx from the service should return success=False."""
    raw_b64 = base64.b64encode(b"data").decode("ascii")
    step = _Step(input=raw_b64)
    ctx = _Context()

    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is False
    assert "500" in result["error"]


def test_service_unreachable_returns_failure():
    """ConnectError from httpx results in success=False with descriptive error."""
    import httpx as _httpx

    raw_b64 = base64.b64encode(b"data").decode("ascii")
    step = _Step(input=raw_b64)
    ctx = _Context()

    async def fail_post(url, **kwargs):
        raise _httpx.ConnectError("Connection refused")

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_httpx.ConnectError("Connection refused"))
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is False
    assert "unreachable" in result["error"].lower() or "markitdown" in result["error"].lower()


def test_timeout_returns_failure():
    """TimeoutException from httpx results in success=False."""
    import httpx as _httpx

    raw_b64 = base64.b64encode(b"data").decode("ascii")
    step = _Step(input=raw_b64)
    ctx = _Context()

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_httpx.TimeoutException("timed out"))
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is False
    assert "timeout" in result["error"].lower() or "timed out" in result["error"].lower()


def test_no_input_no_context_returns_failure():
    """If neither input config nor previous step output is available, fail gracefully."""
    step = _Step()  # no input, no params
    ctx = _Context(last_output=None)

    result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is False
    assert "No input" in result["error"] or "input" in result["error"].lower()


# ---------------------------------------------------------------------------
# 6. Input from pipeline context (previous step output)
# ---------------------------------------------------------------------------


def test_input_from_context_last_output():
    """When no 'input' is set, runner uses context.last_output dict."""
    raw_b64 = base64.b64encode(b"from context").decode("ascii")
    step = _Step()  # no explicit input
    ctx = _Context(last_output={"base64": raw_b64, "filename": "doc.pdf"})

    service_response = {"markdown": "context content", "metadata": {}, "extracted": {}}
    mock_resp = _mock_response(200, service_response)

    captured_payload: dict = {}

    async def fake_post(url, **kwargs):
        captured_payload.update(kwargs.get("json", {}))
        return mock_resp

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=fake_post)
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is True
    assert captured_payload["content"] == raw_b64
    assert captured_payload.get("filename") == "doc.pdf"


# ---------------------------------------------------------------------------
# 7. ENV-based URL override
# ---------------------------------------------------------------------------


def test_custom_markitdown_url_from_env():
    """BRIX_MARKITDOWN_URL env var is respected."""
    raw_b64 = base64.b64encode(b"data").decode("ascii")
    step = _Step(input=raw_b64)
    ctx = _Context()

    mock_resp = _mock_response(200, {"markdown": "ok", "metadata": {}, "extracted": {}})
    captured_urls: list[str] = []

    async def fake_post(url, **kwargs):
        captured_urls.append(url)
        return mock_resp

    with patch.dict(os.environ, {"BRIX_MARKITDOWN_URL": "http://custom-host:9999"}):
        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(side_effect=fake_post)
            mock_client_cls.return_value = mock_client

            result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is True
    assert captured_urls[0].startswith("http://custom-host:9999")


# ---------------------------------------------------------------------------
# 8. report_progress called
# ---------------------------------------------------------------------------


def test_report_progress_called():
    """Runner must call report_progress at start and end."""
    raw_b64 = base64.b64encode(b"data").decode("ascii")
    step = _Step(input=raw_b64)
    ctx = _Context()

    mock_resp = _mock_response(200, {"markdown": "ok", "metadata": {}, "extracted": {}})

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client_cls.return_value = mock_client

        runner = _make_runner()
        _run(runner.execute(step, ctx))

    assert runner._progress is not None
    assert runner._progress["pct"] == 100.0


# ---------------------------------------------------------------------------
# 9. _looks_like_file_path helper
# ---------------------------------------------------------------------------


def test_looks_like_file_path_absolute():
    assert _looks_like_file_path("/tmp/file.pdf") is True


def test_looks_like_file_path_relative():
    assert _looks_like_file_path("./file.pdf") is True


def test_looks_like_file_path_base64_does_not_match():
    b64 = base64.b64encode(b"hello world test").decode("ascii")
    assert _looks_like_file_path(b64) is False


def test_looks_like_file_path_empty():
    assert _looks_like_file_path("") is False


# ---------------------------------------------------------------------------
# 10. template parameter forwarded in payload
# ---------------------------------------------------------------------------


def test_template_forwarded_in_payload():
    """The 'template' config value must appear in the POST payload."""
    raw_b64 = base64.b64encode(b"data").decode("ascii")
    step = _Step(input=raw_b64, template="invoice_v2")
    ctx = _Context()

    mock_resp = _mock_response(200, {"markdown": "ok", "metadata": {}, "extracted": {}})
    captured_payload: dict = {}

    async def fake_post(url, **kwargs):
        captured_payload.update(kwargs.get("json", {}))
        return mock_resp

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=fake_post)
        mock_client_cls.return_value = mock_client

        result = _run(_make_runner().execute(step, ctx))

    assert result["success"] is True
    assert captured_payload.get("template") == "invoice_v2"
