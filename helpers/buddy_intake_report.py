#!/usr/bin/env python3
"""Generate a summary report for the buddy intake pipeline run.

Input:
    {
        "process_results": {items: [...], summary: {...}},  # ForeachResult from process step
        "since": "2024-11-01",
        "keywords": "..."
    }

Output:
    {
        "total_mails": N,
        "total_processed": N,
        "total_ok": N,
        "total_skipped": N,
        "total_errors": N,
        "by_type": {"attachment": N, "body": N},
        "by_classification": {"invoice": N, ...},
        "by_status": {"ok": N, "skipped": N, "error": N},
        "errors": [...],
        "since": "...",
        "keywords": "..."
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
    since = params.get("since", "")
    keywords = params.get("keywords", "")

    # ForeachResult: {items: [{success, data}, ...], summary: {...}}
    items = []
    if isinstance(process_results, dict):
        items = process_results.get("items", [])

    total_mails = 0
    total_processed = 0
    total_ok = 0
    total_skipped = 0
    total_errors = 0
    by_type: dict = defaultdict(int)
    by_classification: dict = defaultdict(int)
    by_status: dict = defaultdict(int)
    all_errors = []

    for item in items:
        total_mails += 1
        if not item.get("success"):
            total_errors += 1
            all_errors.append({"mail_id": "unknown", "error": item.get("error", "unknown error")})
            continue

        data = item.get("data", {})
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, ValueError):
                data = {}

        mail_id = data.get("mail_id", "unknown")
        errors = data.get("errors", [])
        if errors:
            all_errors.extend({"mail_id": mail_id, "error": e} for e in errors)

        for proc in data.get("processed", []):
            total_processed += 1
            ptype = proc.get("type", "unknown")
            pstatus = proc.get("status", "unknown")
            pclass = proc.get("classification") or "unclassified"

            by_type[ptype] += 1
            by_status[pstatus] += 1
            by_classification[pclass] += 1

            if pstatus == "ok":
                total_ok += 1
            elif pstatus == "skipped":
                total_skipped += 1
            elif pstatus == "error":
                total_errors += 1
                reason = proc.get("reason", "")
                if reason:
                    all_errors.append({"mail_id": mail_id, "type": ptype, "error": reason})

    report = {
        "total_mails": total_mails,
        "total_processed": total_processed,
        "total_ok": total_ok,
        "total_skipped": total_skipped,
        "total_errors": total_errors,
        "by_type": dict(by_type),
        "by_classification": dict(by_classification),
        "by_status": dict(by_status),
        "errors": all_errors[:20],  # cap to avoid huge output
        "since": since,
        "keywords": keywords,
    }

    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
