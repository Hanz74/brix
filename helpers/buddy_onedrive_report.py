#!/usr/bin/env python3
"""Generate a summary report for the buddy OneDrive intake pipeline run.

Input:
    {
        "process_results": {items: [{success, data}, ...], summary: {...}},
        "folder_ids": "id1,id2,...",
        "recursive": false
    }

Output:
    {
        "total_files": N,
        "total_ok": N,
        "total_skipped": N,
        "total_errors": N,
        "by_classification": {"invoice": N, ...},
        "by_status": {"ok": N, "skipped": N, "error": N},
        "errors": [...],
        "folder_ids": "...",
        "recursive": false
    }
"""
import json
import sys
from collections import defaultdict


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    process_results = params.get("process_results", {})
    folder_ids = params.get("folder_ids", "")
    recursive = params.get("recursive", False)

    items = []
    if isinstance(process_results, dict):
        items = process_results.get("items", [])
    elif isinstance(process_results, list):
        items = process_results

    total_files = 0
    total_ok = 0
    total_skipped = 0
    total_errors = 0
    by_classification: dict = defaultdict(int)
    by_status: dict = defaultdict(int)
    all_errors = []

    for item in items:
        total_files += 1

        if not item.get("success", True):
            total_errors += 1
            all_errors.append({
                "file_id": "unknown",
                "error": item.get("error", "unknown error"),
            })
            continue

        data = item.get("data", item)
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, ValueError):
                data = {}

        file_id = data.get("file_id", "unknown")
        file_name = data.get("file_name", "unknown")
        status = data.get("status", "unknown")
        classification = data.get("classification") or "unclassified"

        by_status[status] += 1
        by_classification[classification] += 1

        if status == "ok":
            total_ok += 1
        elif status == "skipped":
            total_skipped += 1
        elif status == "error":
            total_errors += 1
            reason = data.get("reason", "")
            if reason:
                all_errors.append({
                    "file_id": file_id,
                    "file_name": file_name,
                    "error": reason,
                })

    report = {
        "total_files": total_files,
        "total_ok": total_ok,
        "total_skipped": total_skipped,
        "total_errors": total_errors,
        "by_classification": dict(by_classification),
        "by_status": dict(by_status),
        "errors": all_errors[:20],
        "folder_ids": folder_ids,
        "recursive": recursive,
    }

    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
