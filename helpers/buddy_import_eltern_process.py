#!/usr/bin/env python3
"""Process a single Kontoauszug file (PDF/image) for the Eltern import pipeline.

Flow per file:
  1. Read file from disk
  2. SHA256 hash → duplicate check against buddy-api
  3. Save original to output_dir (if not already there)
  4. MarkItDown OCR → Markdown
  5. buddy_parse_kontoauszug → List of Transaction payloads
  6. POST each transaction to buddy-api /v1/ingest

Input (one item from foreach over list_files.output):
    {
        "filename":   "kontoauszug_2024_03.pdf",
        "path":       "/host/root/documents/eltern/kontoauszug_2024_03.pdf",
        "size":       123456,
        "account_id": 2,
        "owner":      "eltern",
        "output_dir": "/host/root/docker/buddy/data/originals"
    }

Output:
    {
        "filename":          "kontoauszug_2024_03.pdf",
        "path":              "...",
        "filename_saved":    "2024-03-01_upload_abc12345_kontoauszug_2024_03.pdf",
        "status":            "ok|error|skipped",
        "reason":            "...",       # only on skipped/error
        "transactions_found": 12,
        "transactions_ok":    11,
        "transactions_errors": 1,
        "ingest_results":    [...]
    }
"""
import asyncio
import base64
import hashlib
import json
import re
import sys
from pathlib import Path

import httpx


MARKITDOWN_URL = "http://markitdown-mcp:8081/v1/convert"
BUDDY_API_URL = "http://buddy-api:8030/v1/ingest"
BUDDY_API_BASE = "http://buddy-api:8030"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sanitize_filename(s: str, max_len: int = 40) -> str:
    s = re.sub(r"[^\w\s\-]", "_", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s.strip())
    return s[:max_len]


def make_eltern_filename(source_path: str, file_id_short: str, filename: str) -> str:
    """Build storage filename: upload_{id_short}_{sanitized}.{ext}"""
    stem = sanitize_filename(Path(filename).stem, max_len=30)
    ext = Path(filename).suffix.lower().lstrip(".") or "bin"
    return f"upload_{file_id_short}_{stem}.{ext}"


async def check_hash_duplicate(client: httpx.AsyncClient, content_hash: str) -> bool:
    """Return True if a document with this content_hash already exists in buddy-api."""
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
    except Exception as exc:
        sys.stderr.write(f"[buddy_import_eltern] hash dedup check failed: {exc}\n")
    return False


async def convert_to_markdown(client: httpx.AsyncClient, data: bytes, filename: str) -> str | None:
    """Convert file bytes to Markdown via MarkItDown REST API (OCR for images/PDFs)."""
    try:
        b64 = base64.b64encode(data).decode()
        resp = await client.post(
            MARKITDOWN_URL,
            json={"base64": b64, "filename": filename, "accuracy": "high"},
            timeout=180.0,
        )
        result = resp.json()
        if result.get("success"):
            return result.get("markdown", "")
    except Exception as exc:
        sys.stderr.write(f"[buddy_import_eltern] MarkItDown error for {filename}: {exc}\n")
    return None


async def parse_kontoauszug(markdown: str, account_id: int, source_path: str, owner: str) -> list[dict]:
    """Call buddy_parse_kontoauszug inline (same helpers/ dir)."""
    helpers_dir = Path(__file__).parent
    sys.path.insert(0, str(helpers_dir))
    try:
        import buddy_parse_kontoauszug as bpk  # type: ignore
        return bpk.parse_with_regex(markdown) or []
    except Exception as exc:
        sys.stderr.write(f"[buddy_import_eltern] parse_kontoauszug import error: {exc}\n")
        return []
    finally:
        if str(helpers_dir) in sys.path:
            sys.path.remove(str(helpers_dir))


def _build_transaction_payload(
    tx: dict,
    account_id: int,
    source_path: str,
    owner: str,
    parse_method: str,
) -> dict:
    """Build the full transaction ingest payload from a partial tx dict."""
    import hashlib as _hl
    booking_date = str(tx.get("booking_date", ""))
    value_date = str(tx.get("value_date") or booking_date)
    amount = float(tx.get("amount", 0.0))
    counterpart = str(tx.get("counterpart") or "")
    reference = str(tx.get("reference") or "")

    canonical = json.dumps(
        {"source": source_path, "booking_date": booking_date, "amount": amount, "reference": reference},
        sort_keys=True,
        ensure_ascii=False,
    )
    external_id = _hl.sha256(canonical.encode()).hexdigest()

    return {
        "type": "transaction",
        "account_id": account_id,
        "booking_date": booking_date,
        "value_date": value_date,
        "amount": amount,
        "counterpart": counterpart,
        "reference": reference,
        "external_id": external_id,
        "raw_data": {
            "owner": owner,
            "source": "ocr_upload",
            "source_path": source_path,
            "parse_method": parse_method,
        },
    }


async def ingest_transactions(
    client: httpx.AsyncClient,
    transactions: list[dict],
) -> list[dict]:
    """POST each transaction to buddy-api /v1/ingest. Returns list of results."""
    results = []
    for tx in transactions:
        try:
            resp = await client.post(BUDDY_API_URL, json=tx, timeout=30.0)
            status_code = resp.status_code
            body = resp.json() if resp.content else {}
            results.append({
                "external_id": tx.get("external_id", "")[:12],
                "booking_date": tx.get("booking_date"),
                "amount": tx.get("amount"),
                "status_code": status_code,
                "ok": status_code in (201, 409),
                "body": body,
            })
        except Exception as exc:
            results.append({
                "external_id": tx.get("external_id", "")[:12],
                "booking_date": tx.get("booking_date"),
                "amount": tx.get("amount"),
                "status_code": 0,
                "ok": False,
                "error": str(exc),
            })
    return results


# ---------------------------------------------------------------------------
# Main process function
# ---------------------------------------------------------------------------

async def process_file(params: dict) -> dict:
    filename = params.get("filename", "")
    path = params.get("path", "")
    account_id = int(params.get("account_id", 0))
    owner = params.get("owner", "eltern")
    output_dir = Path(params.get("output_dir", "/host/root/docker/buddy/data/originals"))

    if not path:
        return {
            "filename": filename,
            "status": "error",
            "reason": "path is required",
        }

    # Read file from disk
    try:
        content_bytes = Path(path).read_bytes()
    except Exception as exc:
        return {
            "filename": filename,
            "path": path,
            "status": "error",
            "reason": f"read failed: {exc}",
        }

    content_hash = compute_sha256(content_bytes)

    async with httpx.AsyncClient() as client:
        # Duplicate check (content-hash level)
        if await check_hash_duplicate(client, content_hash):
            return {
                "filename": filename,
                "path": path,
                "status": "skipped",
                "reason": f"content_hash {content_hash[:12]}... already ingested",
            }

        # Build storage filename
        id_short = content_hash[:8]
        filename_saved = make_eltern_filename(path, id_short, filename)

        # Save original to output_dir (if not already the same location)
        output_dir.mkdir(parents=True, exist_ok=True)
        dest_path = output_dir / filename_saved
        if not dest_path.exists():
            try:
                dest_path.write_bytes(content_bytes)
            except Exception as exc:
                return {
                    "filename": filename,
                    "path": path,
                    "status": "error",
                    "reason": f"save original failed: {exc}",
                }

        # MarkItDown OCR → Markdown
        markdown = await convert_to_markdown(client, content_bytes, filename)
        if not markdown:
            return {
                "filename": filename,
                "path": path,
                "filename_saved": filename_saved,
                "status": "error",
                "reason": "MarkItDown OCR returned no markdown",
                "transactions_found": 0,
                "transactions_ok": 0,
                "transactions_errors": 0,
            }

        # Parse Kontoauszug → raw transactions
        # Import parse module directly for LLM-capable parsing
        helpers_dir = Path(__file__).parent
        sys.path.insert(0, str(helpers_dir))
        try:
            import buddy_parse_kontoauszug as bpk  # type: ignore

            regex_txs = bpk.parse_with_regex(markdown)
            if len(regex_txs) < 2:
                sys.stderr.write(
                    f"[buddy_import_eltern] {filename}: regex found {len(regex_txs)} — trying LLM\n"
                )
                llm_txs = bpk.parse_with_llm(markdown)
                raw_txs = llm_txs if len(llm_txs) >= len(regex_txs) else regex_txs
                parse_method = "llm" if len(llm_txs) >= len(regex_txs) else "regex"
            else:
                raw_txs = regex_txs
                parse_method = "regex"

            # Validate + deduplicate
            valid_txs = [tx for tx in raw_txs if bpk._validate_tx(tx)]
            unique_txs = bpk._deduplicate(valid_txs)
        except Exception as exc:
            sys.stderr.write(f"[buddy_import_eltern] parse error for {filename}: {exc}\n")
            unique_txs = []
            parse_method = "none"
        finally:
            if str(helpers_dir) in sys.path:
                sys.path.remove(str(helpers_dir))

        if not unique_txs:
            return {
                "filename": filename,
                "path": path,
                "filename_saved": filename_saved,
                "status": "error",
                "reason": "no transactions extracted from markdown",
                "transactions_found": 0,
                "transactions_ok": 0,
                "transactions_errors": 0,
                "markdown_length": len(markdown),
            }

        # Build full ingest payloads
        tx_payloads = [
            _build_transaction_payload(tx, account_id, path, owner, parse_method)
            for tx in unique_txs
        ]

        # Ingest
        ingest_results = await ingest_transactions(client, tx_payloads)

    transactions_ok = sum(1 for r in ingest_results if r.get("ok"))
    transactions_errors = len(ingest_results) - transactions_ok

    return {
        "filename": filename,
        "path": path,
        "filename_saved": filename_saved,
        "status": "ok" if transactions_errors == 0 else "partial",
        "parse_method": parse_method,
        "transactions_found": len(unique_txs),
        "transactions_ok": transactions_ok,
        "transactions_errors": transactions_errors,
        "ingest_results": ingest_results[:50],  # cap for output size
    }


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
