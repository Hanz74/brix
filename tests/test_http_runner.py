"""Tests for HTTP runner."""
import pytest
import httpx
from brix.runners.http import HttpRunner


class _Step:
    """Minimal step stand-in."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# URL validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_no_url():
    """Missing URL returns error."""
    runner = HttpRunner()
    step = _Step()
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "url" in result["error"].lower()
    assert result["duration"] == 0.0


@pytest.mark.asyncio
async def test_http_url_from_params():
    """URL can be provided via params['_url']."""
    runner = HttpRunner()
    # _url in params but connection will fail — just verify the URL is picked up
    # by checking the error is NOT "needs url"
    step = _Step(params={"_url": "http://127.0.0.1:1"})
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "url" not in result["error"].lower()  # different error (connection)


# ---------------------------------------------------------------------------
# Successful responses (monkeypatched)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_get_json(monkeypatch):
    """GET returning JSON is parsed into dict."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            200,
            json={"result": "ok"},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api")
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == {"result": "ok"}
    assert result["duration"] >= 0.0


@pytest.mark.asyncio
async def test_http_post_with_body(monkeypatch):
    """POST with dict body sends JSON and returns data."""
    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["method"] = method
        received["json"] = kwargs.get("json")
        return httpx.Response(
            201,
            json={"created": True},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api", method="POST", body={"name": "test"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert received["method"] == "POST"
    assert received["json"] == {"name": "test"}
    assert result["data"] == {"created": True}


@pytest.mark.asyncio
async def test_http_post_with_list_body(monkeypatch):
    """POST with list body sends JSON array."""
    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["json"] = kwargs.get("json")
        return httpx.Response(200, json=[], request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api", method="POST", body=[1, 2, 3])
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert received["json"] == [1, 2, 3]


@pytest.mark.asyncio
async def test_http_post_with_string_body(monkeypatch):
    """POST with string body sends raw content."""
    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["content"] = kwargs.get("content")
        return httpx.Response(200, text="ok", request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api", method="POST", body="raw string")
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert received["content"] == "raw string"


@pytest.mark.asyncio
async def test_http_with_headers(monkeypatch):
    """Custom headers are forwarded to the request."""
    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["headers"] = kwargs.get("headers")
        return httpx.Response(200, json={}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com", headers={"Authorization": "Bearer token123"})
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert received["headers"]["Authorization"] == "Bearer token123"


@pytest.mark.asyncio
async def test_http_text_response(monkeypatch):
    """Non-JSON content-type with plain text is returned as string."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            200,
            text="plain text response",
            headers={"content-type": "text/plain"},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/text")
    result = await runner.execute(step, context=None)
    assert result["success"] is True
    assert result["data"] == "plain text response"


@pytest.mark.asyncio
async def test_http_method_default_is_get(monkeypatch):
    """When method is omitted the request is sent as GET."""
    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["method"] = method
        return httpx.Response(200, json={}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com")
    await runner.execute(step, context=None)
    assert received["method"] == "GET"


@pytest.mark.asyncio
async def test_http_method_uppercase(monkeypatch):
    """Method is normalised to uppercase."""
    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["method"] = method
        return httpx.Response(200, json={}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com", method="put")
    await runner.execute(step, context=None)
    assert received["method"] == "PUT"


# ---------------------------------------------------------------------------
# Error status codes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_4xx_error(monkeypatch):
    """4xx status code returns success=False with status in error string."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(404, text="Not Found", request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/missing")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "404" in result["error"]


@pytest.mark.asyncio
async def test_http_5xx_error(monkeypatch):
    """5xx status code returns success=False."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(500, text="Internal Server Error", request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/error")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "500" in result["error"]


# ---------------------------------------------------------------------------
# Network / transport errors
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_connection_error(monkeypatch):
    """ConnectError is caught and returned as structured error."""
    async def mock_request(self, method, url, **kwargs):
        raise httpx.ConnectError("Connection refused")

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://unreachable.example.com")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "error" in result
    assert result["duration"] >= 0.0


@pytest.mark.asyncio
async def test_http_timeout_error(monkeypatch):
    """TimeoutException is caught and reported with elapsed duration."""
    async def mock_request(self, method, url, **kwargs):
        raise httpx.TimeoutException("timed out")

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://slow.example.com", timeout="5s")
    result = await runner.execute(step, context=None)
    assert result["success"] is False
    assert "Timeout" in result["error"]
    assert "5.0" in result["error"]
    assert result["duration"] >= 0.0


# ---------------------------------------------------------------------------
# Timeout parsing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_timeout_seconds(monkeypatch):
    """Timeout '30s' is accepted without error."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(200, json={}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com", timeout="30s")
    result = await runner.execute(step, context=None)
    assert result["success"] is True


@pytest.mark.asyncio
async def test_http_timeout_minutes(monkeypatch):
    """Timeout '2m' is accepted without error."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(200, json={}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com", timeout="2m")
    result = await runner.execute(step, context=None)
    assert result["success"] is True


# ---------------------------------------------------------------------------
# Auto-pagination (fetch_all_pages)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_fetch_all_pages_odata(monkeypatch):
    """fetch_all_pages=True follows @odata.nextLink across multiple pages."""
    call_count = 0

    async def mock_request(self, method, url, **kwargs):
        nonlocal call_count
        call_count += 1
        if "page=1" in url or call_count == 1:
            return httpx.Response(
                200,
                json={
                    "value": [{"id": 1}, {"id": 2}],
                    "@odata.nextLink": "https://example.com/api?page=2",
                },
                request=httpx.Request(method, url),
            )
        elif "page=2" in url:
            return httpx.Response(
                200,
                json={"value": [{"id": 3}, {"id": 4}]},
                request=httpx.Request(method, url),
            )
        return httpx.Response(200, json={"value": []}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    async def mock_get(self, url, **kwargs):
        return httpx.Response(
            200,
            json={"value": [{"id": 3}, {"id": 4}]},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api?page=1", fetch_all_pages=True)
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    data = result["data"]
    assert data["_pages"] == 2
    assert data["_total"] == 4
    assert len(data["value"]) == 4
    assert data["value"][0]["id"] == 1
    assert data["value"][3]["id"] == 4


@pytest.mark.asyncio
async def test_http_fetch_all_pages_link_header(monkeypatch):
    """fetch_all_pages=True follows RFC 5988 Link: <url>; rel="next" header."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            200,
            json={"value": [{"id": 10}, {"id": 20}]},
            headers={"link": '<https://example.com/api?page=2>; rel="next"'},
            request=httpx.Request(method, url),
        )

    async def mock_get(self, url, **kwargs):
        # Second page — no Link header
        return httpx.Response(
            200,
            json={"value": [{"id": 30}]},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)
    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api?page=1", fetch_all_pages=True)
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    data = result["data"]
    assert data["_pages"] == 2
    assert data["_total"] == 3
    assert len(data["value"]) == 3


@pytest.mark.asyncio
async def test_http_fetch_all_pages_single_page(monkeypatch):
    """fetch_all_pages=True with no next link returns single page as normal."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            200,
            json={"value": [{"id": 1}, {"id": 2}, {"id": 3}]},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api", fetch_all_pages=True)
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    data = result["data"]
    assert data["_pages"] == 1
    assert data["_total"] == 3
    assert len(data["value"]) == 3


@pytest.mark.asyncio
async def test_http_no_fetch_all_pages(monkeypatch):
    """fetch_all_pages=False (default) returns raw first-page response unchanged."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            200,
            json={
                "value": [{"id": 1}],
                "@odata.nextLink": "https://example.com/api?page=2",
            },
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api")
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    # Raw data — nextLink still present, no _pages/_total metadata
    assert "@odata.nextLink" in result["data"]
    assert "_pages" not in result["data"]
    assert "_total" not in result["data"]


# ---------------------------------------------------------------------------
# Rate-limit handling (T-BRIX-V3-07)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_429_rate_limited(monkeypatch):
    """429 response with Retry-After header sets rate_limited=True and retry_after."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            429,
            text="Too Many Requests",
            headers={"Retry-After": "10"},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://api.example.com/resource")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert result["rate_limited"] is True
    assert result["retry_after"] == 10
    assert "429" in result["error"]
    assert "10s" in result["error"]


@pytest.mark.asyncio
async def test_http_503_with_retry_after(monkeypatch):
    """503 response with Retry-After header is also treated as rate-limited."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            503,
            text="Service Unavailable",
            headers={"Retry-After": "30"},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://api.example.com/resource")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert result["rate_limited"] is True
    assert result["retry_after"] == 30
    assert "503" in result["error"]
    assert "30s" in result["error"]


@pytest.mark.asyncio
async def test_http_429_default_retry(monkeypatch):
    """429 without Retry-After header defaults to 5s wait."""
    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            429,
            text="Too Many Requests",
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://api.example.com/resource")
    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert result["rate_limited"] is True
    assert result["retry_after"] == 5
    assert "429" in result["error"]
    assert "5s" in result["error"]
