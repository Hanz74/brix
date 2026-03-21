#!/usr/bin/env python3
"""Generate a summary report for the buddy Eltern import pipeline run.

Input:
    {
        "process_results": {items: [{success, data}, ...], summary: {...}},
        "folder_path":     "/host/root/documents/eltern/...",
        "account_id":      2,
        "owner":           "eltern"
    }

Output:
    {
        "total_files":          N,
        "total_ok":             N,
        "total_partial":        N,
        "total_skipped":        N,
        "total_errors":         N,
        "transactions_found":   N,
        "transactions_ok":      N,
        "transactions_errors":  N,
        "by_parse_method":      {"regex": N, "llm": N},
        "by_status":            {"ok": N, "partial": N, "skipped": N, "error": N},
        "errors":               [...],
        "folder_path":          "...",
        "account_id":           2,
        "owner":                "eltern"
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
    folder_path = params.get("folder_path", "")
    account_id = params.get("account_id", 0)
    owner = params.get("owner", "eltern")

    # ForeachResult: {items: [{success, data}, ...], summary: {...}}
    items = []
    if isinstance(process_results, dict):
        items = process_results.get("items", [])
    elif isinstance(process_results, list):
        items = process_results

    total_files = 0
    total_ok = 0
    total_partial = 0
    total_skipped = 0
    total_errors = 0
    transactions_found = 0
    transactions_ok = 0
    transactions_errors = 0
    by_parse_method: dict = defaultdict(int)
    by_status: dict = defaultdict(int)
    all_errors = []

    for item in items:
        total_files += 1

        if not item.get("success", True):
            total_errors += 1
            all_errors.append({
                "filename": "unknown",
                "error": item.get("error", "unknown error"),
            })
            continue

        data = item.get("data", item)
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, ValueError):
                data = {}

        filename = data.get("filename", "unknown")
        status = data.get("status", "unknown")
        parse_method = data.get("parse_method", "unknown")

        by_status[status] += 1
        if parse_method and parse_method != "unknown":
            by_parse_method[parse_method] += 1

        transactions_found += int(data.get("transactions_found", 0))
        transactions_ok += int(data.get("transactions_ok", 0))
        transactions_errors += int(data.get("transactions_errors", 0))

        if status == "ok":
            total_ok += 1
        elif status == "partial":
            total_partial += 1
        elif status == "skipped":
            total_skipped += 1
        elif status == "error":
            total_errors += 1
            reason = data.get("reason", "")
            if reason:
                all_errors.append({"filename": filename, "error": reason})

    report = {
        "total_files": total_files,
        "total_ok": total_ok,
        "total_partial": total_partial,
        "total_skipped": total_skipped,
        "total_errors": total_errors,
        "transactions_found": transactions_found,
        "transactions_ok": transactions_ok,
        "transactions_errors": transactions_errors,
        "by_parse_method": dict(by_parse_method),
        "by_status": dict(by_status),
        "errors": all_errors[:20],
        "folder_path": folder_path,
        "account_id": account_id,
        "owner": owner,
    }

    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
