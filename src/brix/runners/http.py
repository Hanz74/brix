"""HTTP runner — async REST/API calls via httpx."""
import json
import re
import time
from typing import Any

import httpx

from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout, get_default_timeout

# Regex to extract URL from Link: <url>; rel="next" header
_LINK_NEXT_RE = re.compile(r'<([^>]+)>\s*;\s*rel=["\']next["\']', re.IGNORECASE)


def _extract_next_link(response: httpx.Response, data: Any) -> str | None:
    """Return the next-page URL from OData or RFC 5988 Link header, or None."""
    # OData-style: @odata.nextLink in response body
    if isinstance(data, dict):
        next_link = data.get("@odata.nextLink")
        if next_link:
            return next_link

    # RFC 5988 Link header: <url>; rel="next"
    link_header = response.headers.get("link", "")
    if link_header:
        match = _LINK_NEXT_RE.search(link_header)
        if match:
            return match.group(1)

    return None


class HttpRunner(BaseRunner):
    """Executes HTTP requests via httpx."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "Request URL"},
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"], "description": "HTTP method (default GET)"},
                "headers": {"type": "object", "description": "HTTP headers"},
                "body": {"description": "Request body (dict → JSON, string → raw)"},
                "fetch_all_pages": {"type": "boolean", "description": "Follow OData / RFC-5988 pagination"},
                "timeout": {"type": "string", "description": "Timeout e.g. '30s'"},
            },
            "required": ["url"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "any"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        url = getattr(step, "url", None) or (getattr(step, "params", {}) or {}).get("_url")
        if not url:
            self.report_progress(0.0, "error: missing url")
            return {"success": False, "error": "HTTP step needs 'url' field", "duration": 0.0}

        method = getattr(step, "method", "GET") or "GET"
        headers = getattr(step, "headers", None) or (getattr(step, "params", {}) or {}).get("_headers")
        body = getattr(step, "body", None)
        fetch_all_pages = getattr(step, "fetch_all_pages", False)

        self.report_progress(0.0, f"Requesting {method} {url}")

        # Timeout
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else get_default_timeout("http")

        # Inject X-Brix-Run-Id correlation header (T-BRIX-V7-07)
        run_id = getattr(context, "run_id", None) if context is not None else None
        if run_id:
            if headers:
                headers = dict(headers)
            else:
                headers = {}
            headers.setdefault("X-Brix-Run-Id", run_id)

        try:
            async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                kwargs: dict = {}
                if headers:
                    kwargs["headers"] = headers
                if body is not None:
                    if isinstance(body, (dict, list)):
                        kwargs["json"] = body
                    else:
                        kwargs["content"] = str(body)

                response = await client.request(method.upper(), url, **kwargs)

                # Rate limit handling — before generic error check
                if response.status_code in (429, 503):
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_seconds = int(retry_after)
                        except ValueError:
                            wait_seconds = 5  # Default when header is not an integer
                    else:
                        wait_seconds = 5  # Default when header is absent
                    return {
                        "success": False,
                        "error": f"Rate limited (HTTP {response.status_code}). Retry after {wait_seconds}s",
                        "status_code": response.status_code,
                        "duration": time.monotonic() - start,
                        "retry_after": wait_seconds,
                        "rate_limited": True,
                    }

                # Check status code before pagination
                if response.status_code >= 400:
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code}: {response.text[:200]}",
                        "status_code": response.status_code,
                        "duration": time.monotonic() - start,
                    }

                data = _parse_response(response)

                if fetch_all_pages:
                    data = await _fetch_all_pages(client, response, data, headers)

        except httpx.TimeoutException:
            return {
                "success": False,
                "error": f"Timeout after {timeout_seconds}s",
                "duration": time.monotonic() - start,
            }
        except httpx.RequestError as e:
            return {
                "success": False,
                "error": f"Request error: {e}",
                "duration": time.monotonic() - start,
            }

        duration = time.monotonic() - start
        self.report_progress(100.0, f"Response {getattr(response, 'status_code', 200)}")
        return {"success": True, "data": data, "duration": duration}


def _parse_response(response: httpx.Response) -> Any:
    """Parse HTTP response body — prefer JSON, fall back to text."""
    try:
        return response.json()
    except (json.JSONDecodeError, ValueError):
        return response.text


async def _fetch_all_pages(
    client: httpx.AsyncClient,
    first_response: httpx.Response,
    first_data: Any,
    headers: dict | None,
) -> dict:
    """Follow pagination links and accumulate all results into a single response."""
    all_items: list = []

    # Extract items from first page
    if isinstance(first_data, dict) and "value" in first_data:
        all_items.extend(first_data["value"])
    elif isinstance(first_data, list):
        all_items.extend(first_data)
    else:
        # Non-list response — return as-is, cannot paginate
        return first_data

    page = 1
    next_url = _extract_next_link(first_response, first_data)

    while next_url:
        page += 1
        req_kwargs: dict = {}
        if headers:
            req_kwargs["headers"] = headers

        response = await client.get(next_url, **req_kwargs)

        if response.status_code >= 400:
            # Stop pagination on error; return what we have so far
            break

        data = _parse_response(response)

        if isinstance(data, dict) and "value" in data:
            all_items.extend(data["value"])
            next_url = _extract_next_link(response, data)
        elif isinstance(data, list):
            all_items.extend(data)
            next_url = _extract_next_link(response, data)
        else:
            break

    return {"value": all_items, "_pages": page, "_total": len(all_items)}
