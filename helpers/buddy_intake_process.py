#!/usr/bin/env python3
"""Process a batch of emails for buddy intake: save originals, convert via MarkItDown, classify, ingest.

Handles both attachment-based documents and finance-relevant email bodies.
Uses httpx directly to avoid base64 payload explosion in foreach chains.

Input (one item from foreach over fetch_mails.output.value):
    {
        "mail": {mail object from M365},
        "attachments": [{attachment object}, ...],   # may be empty
        "output_dir": "/host/root/docker/buddy/data/originals",
        "import_run_id": 123,  # optional
        "owner": "hans"
    }

Output:
    {
        "mail_id": "...",
        "subject": "...",
        "processed": [
            {"type": "attachment|body", "filename": "...", "status": "ok|error|skipped", ...}
        ],
        "errors": [...]
    }
"""
import asyncio
import base64
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import httpx


MARKITDOWN_URL = "http://markitdown-mcp:8081/v1/convert"
BUDDY_API_URL = "http://buddy-api:8030/v1/ingest"

# Finance keywords for body-only detection
FINANCE_KEYWORDS = [
    "rechnung", "invoice", "zahlung", "payment", "mahnung", "reminder",
    "betrag", "amount", "fällig", "due date", "überweisung", "transfer",
    "kontoauszug", "bank statement", "lastschrift", "direct debit",
    "steuer", "tax", "versicherung", "insurance", "gehalt", "salary",
    "gutschrift", "credit note", "erstattung", "refund", "abrechnung",
]

# Attachment extensions to process
PROCESSABLE_EXTENSIONS = {".pdf", ".docx", ".doc", ".xlsx", ".xls", ".png", ".jpg", ".jpeg"}


def sanitize_filename(s: str, max_len: int = 40) -> str:
    """Sanitize a string for use in filenames."""
    s = re.sub(r"[^\w\s\-]", "_", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s.strip())
    return s[:max_len]


def make_filename(date_str: str, source_id: str, suffix: str, ext: str) -> str:
    """Build filename: {datum}_{outlook}_{source_id_kurz}_{suffix}.{ext}"""
    # Normalise date to YYYY-MM-DD
    date_part = date_str[:10] if date_str else "unknown-date"
    # Shorten source_id (first 8 chars of hash or raw ID)
    id_short = source_id[:8] if len(source_id) >= 8 else source_id
    id_short = re.sub(r"[^\w\-]", "", id_short)
    # Clean suffix
    suffix_clean = sanitize_filename(suffix, max_len=30)
    ext_clean = ext.lstrip(".").lower() or "bin"
    return f"{date_part}_outlook_{id_short}_{suffix_clean}.{ext_clean}"


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_finance_relevant_body(mail: dict) -> bool:
    """Check if email body contains finance-relevant keywords."""
    subject = (mail.get("subject") or "").lower()
    body = ""
    body_obj = mail.get("body", {})
    if isinstance(body_obj, dict):
        body = (body_obj.get("content") or "").lower()
    elif isinstance(body_obj, str):
        body = body_obj.lower()
    text = subject + " " + body
    return any(kw in text for kw in FINANCE_KEYWORDS)


async def convert_to_markdown(client: httpx.AsyncClient, data: bytes, filename: str, accuracy: str = "standard") -> str | None:
    """Convert file bytes to markdown via MarkItDown REST API."""
    try:
        b64 = base64.b64encode(data).decode()
        resp = await client.post(MARKITDOWN_URL, json={
            "base64": b64,
            "filename": filename,
            "accuracy": accuracy,
        }, timeout=120.0)
        result = resp.json()
        if result.get("success"):
            return result.get("markdown", "")
        return None
    except Exception as e:
        sys.stderr.write(f"[buddy_intake] MarkItDown error for {filename}: {e}\n")
        return None


def classify_document(markdown: str, source: str, source_id: str, filename: str) -> dict:
    """Call buddy_classify logic directly (imported inline to avoid subprocess)."""
    # Import classify functions from buddy_classify in same helpers dir
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
                        markdown=markdown, source=source, source_id=source_id,
                        model=model, api_key=api_key,
                    )
                    method = "llm"
                elif provider == "openai" and api_key:
                    classified = bc.call_openai(
                        markdown=markdown, source=source, source_id=source_id,
                        model=model, api_key=api_key, base_url=base_url or None,
                    )
                    method = "llm"
                elif provider == "local":
                    classified = bc.call_local(
                        markdown=markdown, source=source, source_id=source_id,
                        model=model, base_url=base_url or None,
                    )
                    method = "llm"
            except Exception as exc:
                sys.stderr.write(f"[buddy_intake] LLM classification failed: {exc}\n")
                classified = None

        if classified is None:
            classified = bc.classify_with_regex(markdown, source, source_id)
        else:
            classified["source"] = source
            classified["source_id"] = source_id
            classified["raw_markdown"] = markdown
            classified["classification_method"] = method
            if "extra" not in classified or not isinstance(classified.get("extra"), dict):
                classified["extra"] = {}

        # Ensure all defaults
        defaults = {
            "doc_type": "other", "category": "other", "amount": None,
            "currency": None, "party_name": None, "invoice_number": None,
            "doc_date": None, "due_date": None, "direction": None,
            "subject": filename or None, "summary": None, "extra": {},
            "source": source, "source_id": source_id,
            "raw_markdown": markdown, "classification_method": method,
        }
        for key, default in defaults.items():
            classified.setdefault(key, default)

        return classified
    finally:
        if str(helpers_dir) in sys.path:
            sys.path.remove(str(helpers_dir))


async def ingest_document(client: httpx.AsyncClient, classified: dict, original_path: str | None,
                          import_run_id: int | None, owner: str, file_size: int | None,
                          content_hash: str | None, mime_type: str | None,
                          original_filename: str | None) -> dict:
    """POST classified document to buddy-api /v1/ingest."""
    # Build IngestPayload matching models.py
    payload: dict = {
        "source": classified.get("source", "outlook"),
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

    # Convert original_path from container path to relative path for DB storage
    if original_path:
        # Store as relative path from /host/root/docker/buddy/ perspective
        # API expects relative path under BASE_DIR/originals/
        rel = original_path
        base_prefix = "/host/root/docker/buddy/data/originals/"
        if original_path.startswith(base_prefix):
            rel = "originals/" + original_path[len(base_prefix):]
        payload["original_path"] = rel

    # Add file entry if we have file metadata
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


async def process_attachment(
    client: httpx.AsyncClient,
    mail: dict,
    att: dict,
    output_dir: Path,
    import_run_id: int | None,
    owner: str,
) -> dict:
    """Process a single attachment: save → convert → classify → ingest."""
    mail_id = mail.get("id", "")
    mail_date = mail.get("receivedDateTime", "")[:10]
    att_name = att.get("name", "attachment")
    att_id = att.get("id", "")
    content_b64 = att.get("contentBytes", "")
    content_type = att.get("contentType", "application/octet-stream")

    # Check extension is processable
    suffix = Path(att_name).suffix.lower()
    if suffix not in PROCESSABLE_EXTENSIONS:
        return {"type": "attachment", "name": att_name, "status": "skipped",
                "reason": f"extension {suffix!r} not in processable list"}

    # Decode content
    if not content_b64:
        return {"type": "attachment", "name": att_name, "status": "skipped",
                "reason": "no content_bytes"}
    try:
        content_bytes = base64.b64decode(content_b64)
    except Exception as e:
        return {"type": "attachment", "name": att_name, "status": "error",
                "reason": f"base64 decode failed: {e}"}

    content_hash = compute_sha256(content_bytes)

    # Build output filename: {datum}_{outlook}_{source_id_kurz}_{att_name_clean}{ext}
    # Use attachment name (without extension) as suffix for clarity
    att_stem = sanitize_filename(Path(att_name).stem, max_len=30)
    filename = make_filename(mail_date, mail_id, att_stem, suffix.lstrip("."))

    # Save to disk
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / filename
    # Deduplicate
    counter = 1
    while file_path.exists():
        file_path = output_dir / f"{file_path.stem}_{counter}{suffix}"
        counter += 1

    try:
        file_path.write_bytes(content_bytes)
    except Exception as e:
        return {"type": "attachment", "name": att_name, "status": "error",
                "reason": f"write failed: {e}"}

    # Convert to markdown
    markdown = await convert_to_markdown(client, content_bytes, filename)
    if not markdown:
        # No markdown — still ingest with metadata only
        markdown = f"# {att_name}\n\n[Konvertierung fehlgeschlagen — Originalfile gespeichert unter {filename}]"

    # Classify
    source_id = f"{mail_id}::{att_id}"
    classified = classify_document(markdown, "outlook", source_id, att_name)

    # Add email subject to extra
    classified.setdefault("extra", {})
    classified["extra"]["mail_subject"] = mail.get("subject", "")
    classified["extra"]["mail_date"] = mail_date
    classified["extra"]["attachment_name"] = att_name

    # Ingest
    ingest_result = await ingest_document(
        client=client,
        classified=classified,
        original_path=str(file_path),
        import_run_id=import_run_id,
        owner=owner,
        file_size=len(content_bytes),
        content_hash=content_hash,
        mime_type=content_type,
        original_filename=att_name,
    )

    status = "ok" if ingest_result.get("status_code") in (201, 409) else "error"
    return {
        "type": "attachment",
        "name": att_name,
        "filename": filename,
        "status": status,
        "classification": classified.get("doc_type"),
        "ingest_status_code": ingest_result.get("status_code"),
        "ingest_body": ingest_result.get("body"),
    }


async def process_body(
    client: httpx.AsyncClient,
    mail: dict,
    output_dir: Path,
    import_run_id: int | None,
    owner: str,
) -> dict:
    """Process email body as document if it's finance-relevant and has no processable attachments."""
    mail_id = mail.get("id", "")
    mail_date = mail.get("receivedDateTime", "")[:10]
    subject = mail.get("subject", "unknown")

    if not is_finance_relevant_body(mail):
        return {"type": "body", "status": "skipped", "reason": "not finance-relevant"}

    # Extract body text
    body_obj = mail.get("body", {})
    if isinstance(body_obj, dict):
        body_content = body_obj.get("content", "")
        body_type = body_obj.get("contentType", "text").lower()
    else:
        body_content = str(body_obj)
        body_type = "text"

    if not body_content or len(body_content.strip()) < 50:
        return {"type": "body", "status": "skipped", "reason": "body too short"}

    # For HTML bodies, convert via MarkItDown; for text, use as-is
    if "html" in body_type:
        html_bytes = body_content.encode("utf-8")
        content_hash = compute_sha256(html_bytes)
        filename = make_filename(mail_date, mail_id, sanitize_filename(subject, 30), "html")
        # Save the HTML original
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / filename
        if not file_path.exists():
            file_path.write_bytes(html_bytes)
        markdown = await convert_to_markdown(client, html_bytes, filename)
        if not markdown:
            # Strip basic HTML tags as fallback
            markdown = re.sub(r"<[^>]+>", " ", body_content)
            markdown = re.sub(r"\s+", " ", markdown).strip()
    else:
        body_bytes = body_content.encode("utf-8")
        content_hash = compute_sha256(body_bytes)
        filename = make_filename(mail_date, mail_id, sanitize_filename(subject, 30), "txt")
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / filename
        if not file_path.exists():
            file_path.write_text(body_content, encoding="utf-8")
        markdown = body_content

    # Prepend subject as heading if not already present
    if not markdown.startswith("#"):
        markdown = f"# {subject}\n\n{markdown}"

    # Classify
    source_id = f"{mail_id}::body"
    classified = classify_document(markdown, "outlook", source_id, subject)
    classified.setdefault("extra", {})
    classified["extra"]["mail_subject"] = subject
    classified["extra"]["mail_date"] = mail_date
    classified["extra"]["body_type"] = body_type

    ingest_result = await ingest_document(
        client=client,
        classified=classified,
        original_path=str(file_path),
        import_run_id=import_run_id,
        owner=owner,
        file_size=len(body_content.encode("utf-8")),
        content_hash=content_hash,
        mime_type="text/html" if "html" in body_type else "text/plain",
        original_filename=filename,
    )

    status = "ok" if ingest_result.get("status_code") in (201, 409) else "error"
    return {
        "type": "body",
        "subject": subject,
        "filename": filename,
        "status": status,
        "classification": classified.get("doc_type"),
        "ingest_status_code": ingest_result.get("status_code"),
        "ingest_body": ingest_result.get("body"),
    }


async def process_mail(params: dict) -> dict:
    mail = params.get("mail", {})
    attachments = params.get("attachments", [])
    output_dir = Path(params.get("output_dir", "/host/root/docker/buddy/data/originals"))
    import_run_id = params.get("import_run_id")
    owner = params.get("owner", "hans")

    mail_id = mail.get("id", "unknown")
    subject = mail.get("subject", "")
    processed = []
    errors = []

    async with httpx.AsyncClient() as client:
        # Process attachments
        has_processable_attachment = False
        for att in attachments:
            if att.get("isInline"):
                continue
            att_name = att.get("name", "")
            suffix = Path(att_name).suffix.lower()
            if suffix in PROCESSABLE_EXTENSIONS:
                has_processable_attachment = True
                try:
                    result = await process_attachment(
                        client, mail, att, output_dir, import_run_id, owner
                    )
                    processed.append(result)
                    if result.get("status") == "error":
                        reason = result.get("reason") or f"ingest failed (status {result.get('ingest_status_code')})"
                        errors.append(f"attachment {att_name!r}: {reason}")
                except Exception as e:
                    err = {"type": "attachment", "name": att_name, "status": "error", "reason": str(e)}
                    processed.append(err)
                    errors.append(str(e))

        # Process body only if no processable attachments were found
        if not has_processable_attachment:
            try:
                body_result = await process_body(client, mail, output_dir, import_run_id, owner)
                processed.append(body_result)
                if body_result.get("status") == "error":
                    reason = body_result.get("reason") or f"ingest failed (status {body_result.get('ingest_status_code')})"
                    errors.append(f"body: {reason}")
            except Exception as e:
                err = {"type": "body", "status": "error", "reason": str(e)}
                processed.append(err)
                errors.append(str(e))

    return {
        "mail_id": mail_id,
        "subject": subject,
        "processed": processed,
        "errors": errors,
    }


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    result = asyncio.run(process_mail(params))
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
