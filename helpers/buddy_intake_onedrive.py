#!/usr/bin/env python3
"""Process a single OneDrive file for buddy intake:
download → SHA256 hash → content-hash dedup → MarkItDown → classify → ingest.

Input (one item from foreach over prepare_onedrive.output):
    {
        "file_id": "...",
        "file_name": "invoice.pdf",
        "file_ext": ".pdf",
        "mime_type": "application/pdf",
        "size": 123456,
        "created_at": "2024-11-15",
        "folder_id": "...",
        "web_url": "...",
        "output_dir": "/host/root/docker/buddy/data/originals",
        "import_run_id": 123,     # optional, 0 = None
        "owner": "hans"
    }

Output:
    {
        "file_id": "...",
        "file_name": "...",
        "filename_saved": "...",
        "status": "ok|error|skipped",
        "reason": "...",        # only on skipped/error
        "classification": "...",
        "ingest_status_code": 201,
        "ingest_body": {...}
    }
"""
import asyncio
import base64
import hashlib
import json
import os
import re
import sys
from pathlib import Path

import httpx


MARKITDOWN_URL = "http://markitdown-mcp:8081/v1/convert"
BUDDY_API_URL = "http://buddy-api:8030/v1/ingest"
BUDDY_API_BASE = "http://buddy-api:8030"

# M365 download endpoint (used via httpx — not via MCP to avoid base64 payload issues at scale)
M365_DOWNLOAD_MCP_URL = None  # We use the m365 MCP tool result passed in as base64 param


def sanitize_filename(s: str, max_len: int = 40) -> str:
    s = re.sub(r"[^\w\s\-]", "_", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s.strip())
    return s[:max_len]


def make_onedrive_filename(date_str: str, file_id: str, file_name: str) -> str:
    """Build filename: {datum}_onedrive_{id_kurz}_{name}.{ext}"""
    date_part = date_str[:10] if date_str else "unknown-date"
    id_short = re.sub(r"[^\w\-]", "", file_id[:8])
    stem = sanitize_filename(Path(file_name).stem, max_len=30)
    ext = Path(file_name).suffix.lower().lstrip(".") or "bin"
    return f"{date_part}_onedrive_{id_short}_{stem}.{ext}"


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def check_hash_duplicate(client: httpx.AsyncClient, content_hash: str) -> bool:
    """Check if a document with this content_hash already exists."""
    try:
        resp = await client.get(
            f"{BUDDY_API_BASE}/v1/documents",
            params={"content_hash": content_hash},
            timeout=10.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", data if isinstance(data, list) else [])
            return len(items) > 0
    except Exception as e:
        sys.stderr.write(f"[buddy_intake_onedrive] hash dedup check failed: {e}\n")
    return False


async def convert_to_markdown(client: httpx.AsyncClient, data: bytes, filename: str) -> str | None:
    """Convert file bytes to markdown via MarkItDown REST API."""
    try:
        b64 = base64.b64encode(data).decode()
        resp = await client.post(MARKITDOWN_URL, json={
            "base64": b64,
            "filename": filename,
            "accuracy": "standard",
        }, timeout=120.0)
        result = resp.json()
        if result.get("success"):
            return result.get("markdown", "")
        return None
    except Exception as e:
        sys.stderr.write(f"[buddy_intake_onedrive] MarkItDown error for {filename}: {e}\n")
        return None


def classify_document(markdown: str, source_id: str, filename: str) -> dict:
    """Call buddy_classify logic directly (imported inline)."""
    helpers_dir = Path(__file__).parent
    sys.path.insert(0, str(helpers_dir))
    try:
        import buddy_classify as bc

        provider = os.environ.get("BUDDY_LLM_PROVIDER", "anthropic").lower()
        model = os.environ.get("BUDDY_LLM_MODEL", "")
        api_key = os.environ.get("BUDDY_LLM_API_KEY", "")
        base_url = os.environ.get("BUDDY_LLM_BASE_URL", "")

        classified = None
        method = "regex"

        if provider in ("anthropic", "openai", "local"):
            try:
                if provider == "anthropic" and api_key:
                    classified = bc.call_anthropic(
                        markdown=markdown, source="onedrive", source_id=source_id,
                        model=model, api_key=api_key,
                    )
                    method = "llm"
                elif provider == "openai" and api_key:
                    classified = bc.call_openai(
                        markdown=markdown, source="onedrive", source_id=source_id,
                        model=model, api_key=api_key, base_url=base_url or None,
                    )
                    method = "llm"
                elif provider == "local":
                    classified = bc.call_local(
                        markdown=markdown, source="onedrive", source_id=source_id,
                        model=model, base_url=base_url or None,
                    )
                    method = "llm"
            except Exception as exc:
                sys.stderr.write(f"[buddy_intake_onedrive] LLM classification failed: {exc}\n")
                classified = None

        if classified is None:
            classified = bc.classify_with_regex(markdown, "onedrive", source_id)
        else:
            classified["source"] = "onedrive"
            classified["source_id"] = source_id
            classified["raw_markdown"] = markdown
            classified["classification_method"] = method
            if "extra" not in classified or not isinstance(classified.get("extra"), dict):
                classified["extra"] = {}

        defaults = {
            "doc_type": "other", "category": "other", "amount": None,
            "currency": None, "party_name": None, "invoice_number": None,
            "doc_date": None, "due_date": None, "direction": None,
            "subject": filename or None, "summary": None, "extra": {},
            "source": "onedrive", "source_id": source_id,
            "raw_markdown": markdown, "classification_method": method,
        }
        for key, default in defaults.items():
            classified.setdefault(key, default)

        return classified
    finally:
        if str(helpers_dir) in sys.path:
            sys.path.remove(str(helpers_dir))


async def ingest_document(
    client: httpx.AsyncClient,
    classified: dict,
    original_path: str | None,
    import_run_id: int | None,
    owner: str,
    file_size: int | None,
    content_hash: str | None,
    mime_type: str | None,
    original_filename: str | None,
) -> dict:
    """POST classified document to buddy-api /v1/ingest."""
    payload: dict = {
        "source": "onedrive",
        "source_id": classified.get("source_id", ""),
        "owner": owner,
        "document_type": classified.get("doc_type"),
        "category": classified.get("category"),
        "subject": classified.get("subject"),
        "summary": classified.get("summary"),
        "amount": classified.get("amount"),
        "currency": classified.get("currency") or "EUR",
        "party_name": classified.get("party_name"),
        "invoice_number": classified.get("invoice_number"),
        "due_date": classified.get("due_date"),
        "direction": classified.get("direction"),
        "raw_markdown": classified.get("raw_markdown"),
        "extra": classified.get("extra", {}),
    }

    if classified.get("doc_date"):
        payload["document_date"] = classified["doc_date"]

    if content_hash:
        payload["content_hash"] = content_hash

    if import_run_id:
        payload["import_run_id"] = import_run_id

    # Convert container path to relative path for DB storage
    if original_path:
        rel = original_path
        base_prefix = "/host/root/docker/buddy/data/originals/"
        if original_path.startswith(base_prefix):
            rel = "originals/" + original_path[len(base_prefix):]
        payload["original_path"] = rel

    if original_path and original_filename:
        file_entry: dict = {
            "file_path": payload.get("original_path", original_path),
            "file_name": original_filename,
        }
        if file_size:
            file_entry["file_size"] = file_size
        if mime_type:
            file_entry["mime_type"] = mime_type
        if content_hash:
            file_entry["content_hash"] = content_hash
        payload["files"] = [file_entry]
    else:
        payload["files"] = []

    try:
        resp = await client.post(BUDDY_API_URL, json=payload, timeout=30.0)
        return {"status_code": resp.status_code, "body": resp.json()}
    except Exception as e:
        return {"status_code": 0, "error": str(e)}


async def process_file(params: dict) -> dict:
    file_id = params.get("file_id", "")
    file_name = params.get("file_name", "")
    file_ext = params.get("file_ext", "")
    mime_type = params.get("mime_type", "application/octet-stream")
    file_size = params.get("size", 0)
    created_at = params.get("created_at", "")
    folder_id = params.get("folder_id", "")
    web_url = params.get("web_url", "")
    output_dir = Path(params.get("output_dir", "/host/root/docker/buddy/data/originals"))
    import_run_id_raw = params.get("import_run_id", 0)
    import_run_id = int(import_run_id_raw) if import_run_id_raw else None
    if import_run_id == 0:
        import_run_id = None
    owner = params.get("owner", "hans")

    # The file content is passed as base64 from the M365 download step
    # The YAML passes content_base64 from the download MCP step result
    content_b64 = params.get("content_base64", "")

    if not content_b64:
        return {
            "file_id": file_id,
            "file_name": file_name,
            "status": "error",
            "reason": "no content_base64 provided — download step may have failed",
        }

    try:
        content_bytes = base64.b64decode(content_b64)
    except Exception as e:
        return {
            "file_id": file_id,
            "file_name": file_name,
            "status": "error",
            "reason": f"base64 decode failed: {e}",
        }

    content_hash = compute_sha256(content_bytes)

    async with httpx.AsyncClient() as client:
        # Content-hash dedup check
        if await check_hash_duplicate(client, content_hash):
            return {
                "file_id": file_id,
                "file_name": file_name,
                "status": "skipped",
                "reason": f"content_hash {content_hash[:12]}... already ingested",
            }

        # Build output filename
        filename = make_onedrive_filename(created_at, file_id, file_name)

        # Save original to disk
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / filename
        counter = 1
        while file_path.exists():
            file_path = output_dir / f"{file_path.stem}_{counter}{file_ext}"
            counter += 1

        try:
            file_path.write_bytes(content_bytes)
        except Exception as e:
            return {
                "file_id": file_id,
                "file_name": file_name,
                "status": "error",
                "reason": f"write failed: {e}",
            }

        # Convert to markdown
        markdown = await convert_to_markdown(client, content_bytes, filename)
        if not markdown:
            markdown = (
                f"# {file_name}\n\n"
                f"[Konvertierung fehlgeschlagen — Originalfile gespeichert unter {filename}]"
            )

        # Classify
        source_id = file_id
        classified = classify_document(markdown, source_id, file_name)
        classified.setdefault("extra", {})
        classified["extra"]["onedrive_file_id"] = file_id
        classified["extra"]["onedrive_folder_id"] = folder_id
        classified["extra"]["web_url"] = web_url
        classified["extra"]["original_filename"] = file_name

        # Ingest
        ingest_result = await ingest_document(
            client=client,
            classified=classified,
            original_path=str(file_path),
            import_run_id=import_run_id,
            owner=owner,
            file_size=file_size or len(content_bytes),
            content_hash=content_hash,
            mime_type=mime_type,
            original_filename=file_name,
        )

    status = "ok" if ingest_result.get("status_code") in (201, 409) else "error"
    result: dict = {
        "file_id": file_id,
        "file_name": file_name,
        "filename_saved": str(file_path),
        "status": status,
        "classification": classified.get("doc_type"),
        "ingest_status_code": ingest_result.get("status_code"),
        "ingest_body": ingest_result.get("body"),
    }
    if status == "error":
        result["reason"] = (
            ingest_result.get("error")
            or str(ingest_result.get("body", ""))
        )
    return result


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    result = asyncio.run(process_file(params))
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
