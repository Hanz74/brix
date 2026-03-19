#!/usr/bin/env python3
"""Filter mails by keywords in subject or body preview.

Input: {mails: [...], keywords: "Rechnung,Invoice"}
Output: list of matching mail objects
"""
import json
import sys


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    mails = params.get("mails", [])
    keywords_str = params.get("keywords", "")

    if isinstance(mails, str):
        try:
            mails = json.loads(mails)
        except (json.JSONDecodeError, ValueError):
            mails = []

    # Parse keywords (comma-separated, case-insensitive)
    keywords = [k.strip().lower() for k in keywords_str.split(",") if k.strip()]

    if not keywords:
        # No keywords = pass through all mails
        print(json.dumps(mails))
        return

    matches = []
    for mail in mails:
        subject = (mail.get("subject") or "").lower()
        body_preview = (mail.get("bodyPreview") or "").lower()
        text = subject + " " + body_preview

        if any(kw in text for kw in keywords):
            matches.append(mail)

    print(json.dumps(matches))


if __name__ == "__main__":
    main()
