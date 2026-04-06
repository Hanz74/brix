"""Regression tests for RF-04 HTTP parameter handling."""
import httpx
import pytest

from brix.runners.http import HttpRunner


class _Step:
    """Minimal step stand-in."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


@pytest.mark.asyncio
async def test_http_string_headers_return_clean_error():
    runner = HttpRunner()
    step = _Step(url="https://example.com", headers="Authorization: Bearer token")

    result = await runner.execute(step, context=None)

    assert result["success"] is False
    assert result["duration"] == 0.0
    assert result["error"] == "HTTP 'headers' must be a dict, got str"


@pytest.mark.asyncio
async def test_http_dict_headers_work_normally(monkeypatch):
    received: dict = {}

    async def mock_request(self, method, url, **kwargs):
        received["headers"] = kwargs.get("headers")
        return httpx.Response(200, json={"ok": True}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    runner = HttpRunner()
    step = _Step(url="https://example.com", headers={"Authorization": "Bearer token"})
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert received["headers"] == {"Authorization": "Bearer token"}


@pytest.mark.asyncio
async def test_http_fetch_all_pages_string_false_is_false(monkeypatch):
    get_calls: list[str] = []

    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            200,
            json={
                "value": [{"id": 1}],
                "@odata.nextLink": "https://example.com/api?page=2",
            },
            request=httpx.Request(method, url),
        )

    async def mock_get(self, url, **kwargs):
        get_calls.append(url)
        return httpx.Response(200, json={"value": [{"id": 2}]}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)
    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api", params={"fetch_all_pages": "false"})
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert "@odata.nextLink" in result["data"]
    assert "_pages" not in result["data"]
    assert get_calls == []


@pytest.mark.asyncio
async def test_http_fetch_all_pages_bool_false_is_false(monkeypatch):
    get_calls: list[str] = []

    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            200,
            json={
                "value": [{"id": 1}],
                "@odata.nextLink": "https://example.com/api?page=2",
            },
            request=httpx.Request(method, url),
        )

    async def mock_get(self, url, **kwargs):
        get_calls.append(url)
        return httpx.Response(200, json={"value": [{"id": 2}]}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)
    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api", fetch_all_pages=False)
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert "@odata.nextLink" in result["data"]
    assert "_pages" not in result["data"]
    assert get_calls == []


@pytest.mark.asyncio
async def test_http_fetch_all_pages_string_true_is_true(monkeypatch):
    get_calls: list[str] = []

    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            200,
            json={
                "value": [{"id": 1}],
                "@odata.nextLink": "https://example.com/api?page=2",
            },
            request=httpx.Request(method, url),
        )

    async def mock_get(self, url, **kwargs):
        get_calls.append(url)
        return httpx.Response(200, json={"value": [{"id": 2}]}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)
    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    runner = HttpRunner()
    step = _Step(url="https://example.com/api", params={"fetch_all_pages": "true"})
    result = await runner.execute(step, context=None)

    assert result["success"] is True
    assert result["data"]["_pages"] == 2
    assert result["data"]["_total"] == 2
    assert get_calls == ["https://example.com/api?page=2"]
