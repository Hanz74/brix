#!/usr/bin/env python3
"""Prepare OneDrive file list for buddy intake pipeline.

Filters raw M365 list-folder-files results to processable file types,
normalises metadata, and performs a lightweight API-level dedup check
so we can skip files that are already ingested (by source_id).

Input:
    {
        "file_results": {items: [{success, data}, ...]},  # ForeachResult from list_files step
        "folder_ids": "id1,id2,..."  # original input, for logging
    }

Output:
    [
        {
            "file_id": "...",
            "file_name": "report.pdf",
            "file_ext": ".pdf",
            "mime_type": "application/pdf",
            "size": 123456,
            "created_at": "2024-11-15",
            "folder_id": "...",
            "web_url": "..."
        },
        ...
    ]
    (Duplicates already ingested are removed from the list.)
"""
import json
import sys
from pathlib import Path

import httpx

BUDDY_API_BASE = "http://buddy-api:8030"

# Extensions we can process (must stay in sync with buddy_intake_process.py)
PROCESSABLE_EXTENSIONS = {".pdf", ".docx", ".doc", ".xlsx", ".xls", ".png", ".jpg", ".jpeg"}


def check_already_ingested(file_id: str) -> bool:
    """Check if a document with this source_id already exists in buddy-api."""
    try:
        resp = httpx.get(
            f"{BUDDY_API_BASE}/v1/documents",
            params={"source": "onedrive", "source_id": file_id},
            timeout=10.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            # Expect {items: [...], total: N} or similar
            items = data.get("items", data if isinstance(data, list) else [])
            return len(items) > 0
    except Exception as e:
        sys.stderr.write(f"[buddy_prepare_onedrive] dedup check failed for {file_id}: {e}\n")
    return False


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    file_results = params.get("file_results", {})
    folder_ids_str = params.get("folder_ids", "")

    # ForeachResult shape: {items: [{success, data}, ...], summary: {...}}
    # Each data item is the raw M365 list-folder-files response: {value: [...]}
    raw_items = []
    if isinstance(file_results, dict):
        raw_items = file_results.get("items", [])
    elif isinstance(file_results, list):
        raw_items = file_results

    # Build flat file list from all folder responses
    # We need to track which folder_id each file came from.
    folder_ids = [fid.strip() for fid in folder_ids_str.split(",") if fid.strip()]
    all_files = []

    for idx, item in enumerate(raw_items):
        folder_id = folder_ids[idx] if idx < len(folder_ids) else "unknown"

        if not item.get("success", True):
            sys.stderr.write(
                f"[buddy_prepare_onedrive] folder {folder_id} listing failed: "
                f"{item.get('error', 'unknown')}\n"
            )
            continue

        data = item.get("data", item)
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, ValueError):
                data = {}

        files = data.get("value", [])
        if not files and isinstance(data, list):
            files = data

        for f in files:
            # Skip folders (OneDrive items that are folders have a "folder" key)
            if "folder" in f:
                continue

            file_id = f.get("id", "")
            name = f.get("name", "")
            ext = Path(name).suffix.lower()

            if ext not in PROCESSABLE_EXTENSIONS:
                sys.stderr.write(
                    f"[buddy_prepare_onedrive] skipping {name!r}: extension {ext!r} not processable\n"
                )
                continue

            # Extract date: prefer lastModifiedDateTime, fall back to createdDateTime
            date_str = (
                f.get("lastModifiedDateTime") or f.get("createdDateTime") or ""
            )
            date_part = date_str[:10] if date_str else "unknown-date"

            # Mime type from file object
            file_obj = f.get("file", {})
            mime_type = file_obj.get("mimeType", "application/octet-stream") if isinstance(file_obj, dict) else "application/octet-stream"

            all_files.append({
                "file_id": file_id,
                "file_name": name,
                "file_ext": ext,
                "mime_type": mime_type,
                "size": f.get("size", 0),
                "created_at": date_part,
                "folder_id": folder_id,
                "web_url": f.get("webUrl", ""),
            })

    # Dedup check: skip files already ingested in buddy-api
    filtered = []
    for f in all_files:
        if check_already_ingested(f["file_id"]):
            sys.stderr.write(
                f"[buddy_prepare_onedrive] skipping {f['file_name']!r}: already ingested (source_id={f['file_id']})\n"
            )
        else:
            filtered.append(f)

    sys.stderr.write(
        f"[buddy_prepare_onedrive] {len(all_files)} processable files found, "
        f"{len(all_files) - len(filtered)} already ingested, "
        f"{len(filtered)} to process\n"
    )

    print(json.dumps(filtered, ensure_ascii=False))


if __name__ == "__main__":
    main()
