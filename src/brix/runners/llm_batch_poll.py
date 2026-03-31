"""LLM Batch Poll runner — generic submit + poll wrapper for LLM batch APIs."""
import asyncio
import json
import os
import re
import time
from typing import Any

from brix.config import config
from brix.runners.base import BaseRunner

try:
    from mistralai import Mistral
except ImportError:
    Mistral = None  # type: ignore[assignment,misc]

# Regex to strip Markdown code fences from LLM output
_FENCE_RE = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", re.DOTALL)

_DEFAULT_TIMEOUT = 3600
_DEFAULT_POLL_INTERVAL = 30


def _strip_fences(text: str) -> str:
    """Remove Markdown code fences from *text* and return the inner content."""
    m = _FENCE_RE.match(text.strip())
    if m:
        return m.group(1).strip()
    return text.strip()


class LlmBatchPollRunner(BaseRunner):
    """Generic LLM batch submit + poll wrapper.

    Submits a batch of requests to a provider's batch API, polls until
    completion or timeout, and returns results.

    Currently supports: Mistral. Structured for future provider extension.

    Pipeline YAML example::

        - id: classify
          type: llm.batch_poll
          params:
            provider: "mistral"
            model: "mistral-small-latest"
            requests:
              - custom_id: "req-1"
                messages:
                  - role: "user"
                    content: "Classify this text: ..."
            timeout: 3600
            poll_interval: 30

    Returns::

        {
          "success": true,
          "data": {
            "results": [...],
            "batch_id": "...",
            "status": "completed",
            "duration": 45.2
          },
          "duration": 45.2
        }
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "provider": {
                    "type": "string",
                    "description": "LLM provider (default: 'mistral')",
                    "default": "mistral",
                },
                "model": {
                    "type": "string",
                    "description": "Model ID for the provider",
                },
                "requests": {
                    "type": "array",
                    "description": "List of request dicts with custom_id and messages",
                    "items": {"type": "object"},
                },
                "timeout": {
                    "type": "integer",
                    "description": f"Max seconds to wait for batch completion (default: {_DEFAULT_TIMEOUT})",
                    "default": _DEFAULT_TIMEOUT,
                },
                "poll_interval": {
                    "type": "integer",
                    "description": f"Seconds between status checks (default: {_DEFAULT_POLL_INTERVAL})",
                    "default": _DEFAULT_POLL_INTERVAL,
                },
            },
            "required": ["model", "requests"],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        provider = config.get("provider", "mistral")
        if provider not in ("mistral",):
            errors.append(f"Unsupported provider: '{provider}'. Currently supported: mistral")
        requests = config.get("requests")
        if requests is not None and not isinstance(requests, list):
            errors.append("'requests' must be a list of request objects")
        model = config.get("model")
        if model is not None and not isinstance(model, str):
            errors.append("'model' must be a string")
        return errors

    def input_type(self) -> str:
        return "list[dict]"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        provider = params.get("provider", "mistral")
        model = params.get("model")
        requests = params.get("requests")
        timeout = int(params.get("timeout", _DEFAULT_TIMEOUT) or _DEFAULT_TIMEOUT)
        poll_interval = int(params.get("poll_interval", _DEFAULT_POLL_INTERVAL) or _DEFAULT_POLL_INTERVAL)

        # Fall back to previous step output if no requests provided
        if not requests and context is not None and hasattr(context, "last_output"):
            raw = context.last_output
            if isinstance(raw, list):
                requests = raw

        if not model:
            self.report_progress(0.0, "error: missing model")
            return {"success": False, "error": "llm.batch_poll requires 'model'", "duration": time.monotonic() - start}
        if not requests:
            self.report_progress(0.0, "error: missing requests")
            return {"success": False, "error": "llm.batch_poll requires 'requests'", "duration": time.monotonic() - start}

        if provider == "mistral":
            return await self._run_mistral(model, requests, timeout, poll_interval, start)
        else:
            return {
                "success": False,
                "error": f"Unsupported provider: '{provider}'",
                "duration": time.monotonic() - start,
            }

    async def _run_mistral(
        self,
        model: str,
        requests: list[dict],
        timeout: int,
        poll_interval: int,
        start: float,
    ) -> dict:
        """Submit and poll a Mistral batch job."""
        if Mistral is None:
            self.report_progress(0.0, "error: mistralai not installed")
            return {
                "success": False,
                "error": "mistralai package is not installed. Install with: pip install mistralai",
                "duration": time.monotonic() - start,
            }

        api_key = os.environ.get("BUDDY_LLM_API_KEY") or os.environ.get("MISTRAL_API_KEY")
        if not api_key:
            self.report_progress(0.0, "error: no API key")
            return {
                "success": False,
                "error": "No Mistral API key found. Set BUDDY_LLM_API_KEY or MISTRAL_API_KEY.",
                "duration": time.monotonic() - start,
            }

        client = Mistral(api_key=api_key)

        # Build batch request objects
        batch_requests = []
        for idx, req in enumerate(requests):
            custom_id = req.get("custom_id", f"req-{idx}")
            messages = req.get("messages", [])
            body = {"model": model, "messages": messages}
            # Pass through optional fields
            for key in ("temperature", "max_tokens", "response_format"):
                if key in req:
                    body[key] = req[key]
            batch_requests.append({"custom_id": str(custom_id), "body": body})

        self.report_progress(0.0, f"Submitting batch of {len(batch_requests)} requests")

        # Submit batch job
        try:
            job = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.batch.jobs.create(
                    requests=batch_requests,
                    model=model,
                    endpoint="/v1/chat/completions",
                ),
            )
        except Exception as exc:
            return {
                "success": False,
                "error": f"Batch submission failed: {exc}",
                "duration": time.monotonic() - start,
            }

        batch_id = job.id

        # Poll until complete or timeout
        poll_start = time.monotonic()
        status_str = "unknown"
        while True:
            try:
                status_obj = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda jid=batch_id: client.batch.jobs.get(job_id=jid),
                )
            except Exception as exc:
                return {
                    "success": False,
                    "error": f"Batch status check failed: {exc}",
                    "batch_id": batch_id,
                    "duration": time.monotonic() - start,
                }

            status_str = status_obj.status
            elapsed = round(time.monotonic() - poll_start, 1)
            succeeded = getattr(status_obj, "succeeded_requests", 0) or 0
            failed_req = getattr(status_obj, "failed_requests", 0) or 0
            total_reqs = len(batch_requests)
            pct = ((succeeded + failed_req) / total_reqs * 100) if total_reqs else 0

            self.report_progress(
                pct=pct,
                msg=f"Batch {status_str} — {succeeded}/{total_reqs} done ({elapsed}s)",
                done=succeeded,
                total=total_reqs,
            )

            if status_str in ("SUCCESS", "FAILED", "TIMEOUT_EXCEEDED", "CANCELLED"):
                break

            if time.monotonic() - poll_start > timeout:
                duration = time.monotonic() - start
                return {
                    "success": True,
                    "data": {
                        "results": [],
                        "batch_id": batch_id,
                        "status": "timeout",
                        "duration": duration,
                    },
                    "duration": duration,
                }

            await asyncio.sleep(poll_interval)

        if status_str != "SUCCESS":
            duration = time.monotonic() - start
            return {
                "success": False,
                "error": f"Batch job {batch_id} ended with status: {status_str}",
                "batch_id": batch_id,
                "duration": duration,
            }

        # Download results
        output_file = getattr(status_obj, "output_file", None)
        if not output_file:
            return {
                "success": False,
                "error": f"Batch job {batch_id} has no output_file",
                "batch_id": batch_id,
                "duration": time.monotonic() - start,
            }

        try:
            raw_resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda fid=output_file: client.files.download(file_id=fid),
            )
        except Exception as exc:
            return {
                "success": False,
                "error": f"Failed to download results: {exc}",
                "batch_id": batch_id,
                "duration": time.monotonic() - start,
            }

        # Normalize to text
        if hasattr(raw_resp, "read"):
            try:
                raw_bytes = raw_resp.read()
            except Exception:
                raw_bytes = getattr(raw_resp, "content", b"")
        elif hasattr(raw_resp, "content"):
            raw_bytes = raw_resp.content
        elif hasattr(raw_resp, "__iter__"):
            chunks_buf = []
            for piece in raw_resp:
                if isinstance(piece, (bytes, bytearray)):
                    chunks_buf.append(piece)
                else:
                    chunks_buf.append(str(piece).encode("utf-8"))
            raw_bytes = b"".join(chunks_buf)
        else:
            raw_bytes = bytes(raw_resp)

        if isinstance(raw_bytes, (bytes, bytearray)):
            raw_text = raw_bytes.decode("utf-8")
        else:
            raw_text = str(raw_bytes)

        # Parse JSONL output
        results = []
        for line in raw_text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as exc:
                results.append({
                    "custom_id": None,
                    "result": None,
                    "error": f"JSON parse error: {exc}",
                })
                continue

            custom_id = entry.get("custom_id")
            if entry.get("error"):
                results.append({
                    "custom_id": custom_id,
                    "result": None,
                    "error": str(entry["error"]),
                })
                continue

            choices = entry.get("response", {}).get("body", {}).get("choices", [])
            content = ""
            if choices:
                content = choices[0].get("message", {}).get("content", "")

            # Try to parse JSON from content
            parsed: Any = None
            if content:
                cleaned = _strip_fences(content)
                try:
                    parsed = json.loads(cleaned)
                except (json.JSONDecodeError, ValueError):
                    parsed = content

            results.append({
                "custom_id": custom_id,
                "result": parsed,
            })

        duration = time.monotonic() - start
        self.report_progress(100.0, f"Done — {len(results)} results", done=len(results), total=len(batch_requests))
        return {
            "success": True,
            "data": {
                "results": results,
                "batch_id": batch_id,
                "status": "completed",
                "duration": duration,
            },
            "duration": duration,
        }
