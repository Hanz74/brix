"""LLM Batch runner — native Mistral Batch API integration."""
import asyncio
import json
import os
import re
import time
from typing import Any

from brix.runners.base import BaseRunner

try:
    from mistralai import Mistral
except ImportError:
    Mistral = None  # type: ignore[assignment,misc]

# Regex to strip Markdown code fences from LLM output (e.g. ```json ... ```)
_FENCE_RE = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", re.DOTALL)

# Default poll interval in seconds between Mistral batch status checks
_POLL_INTERVAL = 10


def _strip_fences(text: str) -> str:
    """Remove Markdown code fences from *text* and return the inner content."""
    m = _FENCE_RE.match(text.strip())
    if m:
        return m.group(1).strip()
    return text.strip()


def _render_jinja(template: str, item: dict, context: Any) -> str:
    """Render *template* as a Jinja2 string with *item* in scope."""
    # Lazy import to avoid hard dependency in import path
    try:
        from jinja2.sandbox import SandboxedEnvironment
    except ImportError:
        # Fallback: simple Python str.format_map (best-effort)
        return template

    env = SandboxedEnvironment()
    tmpl = env.from_string(template)
    return tmpl.render(item=item, **(item if isinstance(item, dict) else {}))


class LlmBatchRunner(BaseRunner):
    """Nativer Mistral Batch API Runner.

    Submits a list of items to the Mistral Batch API, polls until the job
    completes and returns a list of ``{custom_id, result}`` dicts.

    Each item's user message is rendered from *user_template* (Jinja2).
    The *system_prompt* can also be a Jinja2 template and is rendered once
    (without item context).

    If *output_schema* is provided it is forwarded as ``response_format``
    (structured output).
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "model": {
                    "type": "string",
                    "default": "mistral-small-latest",
                    "description": "Mistral model ID",
                },
                "system_prompt": {
                    "type": "string",
                    "description": "System prompt (Jinja2)",
                },
                "user_template": {
                    "type": "string",
                    "description": "User message template per item (Jinja2)",
                },
                "output_schema": {
                    "type": "object",
                    "description": "JSON Schema for structured output",
                },
                "batch_size": {
                    "type": "integer",
                    "default": 500,
                    "description": "Max items per Mistral batch job",
                },
                "timeout": {
                    "type": "integer",
                    "default": 600,
                    "description": "Max wait seconds for batch completion",
                },
                "temperature": {
                    "type": "number",
                    "default": 0.1,
                    "description": "Sampling temperature",
                },
                "max_tokens": {
                    "type": "integer",
                    "default": 1000,
                    "description": "Max tokens per response",
                },
            },
            "required": ["system_prompt", "user_template"],
        }

    def input_type(self) -> str:
        return "list[dict]"

    def output_type(self) -> str:
        return "list[dict]"

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        # --- Validate Mistral availability ---
        if Mistral is None:
            return {
                "success": False,
                "error": "mistralai package is not installed. Install it with: pip install mistralai",
                "duration": 0.0,
            }

        # --- API key ---
        api_key = os.environ.get("BUDDY_LLM_API_KEY") or os.environ.get("MISTRAL_API_KEY")
        if not api_key:
            return {
                "success": False,
                "error": "No Mistral API key found. Set BUDDY_LLM_API_KEY or MISTRAL_API_KEY.",
                "duration": 0.0,
            }

        # --- Config ---
        cfg = getattr(step, "params", {}) or {}
        # Also accept top-level step attributes (YAML style)
        def _get(key: str, default: Any = None) -> Any:
            val = getattr(step, key, None)
            if val is None:
                val = cfg.get(key, default)
            return val

        model: str = _get("model", "mistral-small-latest")
        system_prompt: str = _get("system_prompt", "")
        user_template: str = _get("user_template", "")
        output_schema: dict | None = _get("output_schema", None)
        batch_size: int = int(_get("batch_size", 500))
        timeout: int = int(_get("timeout", 600))
        temperature: float = float(_get("temperature", 0.1))
        max_tokens: int = int(_get("max_tokens", 1000))

        if not system_prompt or not user_template:
            return {
                "success": False,
                "error": "llm_batch runner requires 'system_prompt' and 'user_template'",
                "duration": 0.0,
            }

        # --- Items from previous step output or direct input ---
        items: list[dict] = []
        if context is not None and hasattr(context, "last_output"):
            raw = context.last_output
            if isinstance(raw, list):
                items = raw
            elif isinstance(raw, dict):
                items = [raw]
        if not items:
            # Fallback: items provided directly via step param
            items = _get("items", []) or []

        if not items:
            return {
                "success": False,
                "error": "llm_batch runner received no items to process",
                "duration": 0.0,
            }

        # --- Render system prompt once ---
        rendered_system = _render_jinja(system_prompt, {}, context)

        # --- Build request objects ---
        response_format = None
        if output_schema:
            response_format = {
                "type": "json_schema",
                "json_schema": {"schema": output_schema, "strict": True},
            }

        all_results: list[dict] = []

        # Process in batch_size chunks
        chunks = [items[i : i + batch_size] for i in range(0, len(items), batch_size)]
        total_items = len(items)

        client = Mistral(api_key=api_key)

        for chunk_idx, chunk in enumerate(chunks):
            batch_requests = []
            for idx, item in enumerate(chunk):
                custom_id = item.get("id") or item.get("custom_id") or f"item-{chunk_idx * batch_size + idx}"
                user_content = _render_jinja(user_template, item, context)
                req_body: dict = {
                    "model": model,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "messages": [
                        {"role": "system", "content": rendered_system},
                        {"role": "user", "content": user_content},
                    ],
                }
                if response_format:
                    req_body["response_format"] = response_format
                batch_requests.append({"custom_id": str(custom_id), "body": req_body})

            self.report_progress(
                pct=0.0,
                msg=f"Submitting batch chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk)} items)",
                done=chunk_idx * batch_size,
                total=total_items,
            )

            # Submit batch
            job = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda reqs=batch_requests: client.batch.jobs.create(
                    requests=reqs,
                    model=model,
                    endpoint="/v1/chat/completions",
                ),
            )

            # --- Polling loop ---
            poll_start = time.monotonic()
            while True:
                status_obj = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda jid=job.id: client.batch.jobs.get(job_id=jid),
                )
                elapsed = round(time.monotonic() - poll_start, 1)
                succeeded = getattr(status_obj, "succeeded_requests", 0) or 0
                failed_req = getattr(status_obj, "failed_requests", 0) or 0
                pct = (
                    ((succeeded + failed_req) / len(chunk) * 100)
                    if chunk
                    else 0
                )
                self.report_progress(
                    pct=pct,
                    msg=f"Batch {status_obj.status} — {succeeded}/{len(chunk)} done ({elapsed}s)",
                    done=chunk_idx * batch_size + succeeded,
                    total=total_items,
                )

                if status_obj.status in ("SUCCESS", "FAILED", "TIMEOUT_EXCEEDED", "CANCELLED"):
                    break

                if time.monotonic() - poll_start > timeout:
                    return {
                        "success": False,
                        "error": f"Timeout after {timeout}s waiting for batch job {job.id} (status: {status_obj.status})",
                        "job_id": job.id,
                        "duration": time.monotonic() - start,
                    }

                await asyncio.sleep(_POLL_INTERVAL)

            if status_obj.status != "SUCCESS":
                return {
                    "success": False,
                    "error": f"Batch job {job.id} ended with status: {status_obj.status}",
                    "job_id": job.id,
                    "duration": time.monotonic() - start,
                }

            # --- Download and parse results ---
            output_file = getattr(status_obj, "output_file", None)
            if not output_file:
                return {
                    "success": False,
                    "error": f"Batch job {job.id} has no output_file",
                    "job_id": job.id,
                    "duration": time.monotonic() - start,
                }

            raw_resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda fid=output_file: client.files.download(file_id=fid),
            )

            # Normalise to bytes then decode to str
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
            for line in raw_text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as exc:
                    all_results.append({
                        "custom_id": None,
                        "result": None,
                        "error": f"JSON parse error on output line: {exc}",
                        "raw_line": line[:200],
                    })
                    continue

                custom_id = entry.get("custom_id")
                if entry.get("error"):
                    all_results.append({
                        "custom_id": custom_id,
                        "result": None,
                        "error": str(entry["error"]),
                    })
                    continue

                choices = (
                    entry.get("response", {}).get("body", {}).get("choices", [])
                )
                content = ""
                if choices:
                    content = choices[0].get("message", {}).get("content", "")

                # Attempt to parse JSON from content (strip Markdown fences)
                parsed: Any = None
                if content:
                    cleaned = _strip_fences(content)
                    try:
                        parsed = json.loads(cleaned)
                    except (json.JSONDecodeError, ValueError):
                        parsed = content  # keep as string if not JSON

                usage = entry.get("response", {}).get("body", {}).get("usage", {})
                all_results.append({
                    "custom_id": custom_id,
                    "result": parsed,
                    "usage": usage,
                })

        duration = time.monotonic() - start
        self.report_progress(100.0, f"Done — {len(all_results)} results", done=total_items, total=total_items)
        return {
            "success": True,
            "data": all_results,
            "duration": duration,
            "total": total_items,
        }
