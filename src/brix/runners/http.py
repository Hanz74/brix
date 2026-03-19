"""HTTP runner — async REST/API calls via httpx."""
import json
import time
from typing import Any

import httpx

from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout


class HttpRunner(BaseRunner):
    """Executes HTTP requests via httpx."""

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        url = getattr(step, "url", None) or (getattr(step, "params", {}) or {}).get("_url")
        if not url:
            return {"success": False, "error": "HTTP step needs 'url' field", "duration": 0.0}

        method = getattr(step, "method", "GET") or "GET"
        headers = getattr(step, "headers", None) or (getattr(step, "params", {}) or {}).get("_headers")
        body = getattr(step, "body", None)

        # Timeout
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else 60.0

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

        # Check status code
        if response.status_code >= 400:
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text[:200]}",
                "duration": duration,
            }

        # Parse response — prefer content-type, fall back to JSON attempt
        content_type = response.headers.get("content-type", "")
        if "json" in content_type:
            try:
                data = response.json()
            except (json.JSONDecodeError, ValueError):
                data = response.text
        else:
            try:
                data = response.json()
            except (json.JSONDecodeError, ValueError):
                data = response.text

        return {"success": True, "data": data, "duration": duration}
