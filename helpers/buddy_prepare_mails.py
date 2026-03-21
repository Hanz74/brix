#!/usr/bin/env python3
"""Prepare mail list for buddy intake: filter by keywords and date, build per-mail items.

Merges mail objects with their attachment list results into items ready
for the processing foreach step.

Input:
    {
        "mails": [...],                  # list of mail objects from M365
        "attachment_results": {...},     # ForeachResult from list-mail-attachments step
        "keywords": "...",               # comma-separated keywords (optional)
        "since": "2024-11-01"            # filter by receivedDateTime (optional)
    }

Output: list of {mail, attachments} items
"""
import json
import sys
from datetime import date


FINANCE_KEYWORDS_DEFAULT = [
    "rechnung", "invoice", "zahlung", "payment", "mahnung", "reminder",
    "betrag", "amount", "fällig", "due date", "überweisung", "transfer",
    "kontoauszug", "bank statement", "lastschrift", "direct debit",
    "steuer", "tax", "versicherung", "insurance", "gehalt", "salary",
    "gutschrift", "credit note", "erstattung", "refund", "abrechnung",
    "quittung", "receipt", "kassenbon", "bestellung", "order",
]


def parse_keywords(kw_str: str) -> list[str]:
    if not kw_str or not kw_str.strip():
        return []
    return [k.strip().lower() for k in kw_str.split(",") if k.strip()]


def mail_matches_keywords(mail: dict, keywords: list[str]) -> bool:
    """Check if mail subject or body preview contains any keyword."""
    subject = (mail.get("subject") or "").lower()
    preview = (mail.get("bodyPreview") or "").lower()
    body = ""
    body_obj = mail.get("body", {})
    if isinstance(body_obj, dict):
        body = (body_obj.get("content") or "").lower()[:2000]
    text = subject + " " + preview + " " + body
    return any(kw in text for kw in keywords)


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    mails = params.get("mails", [])
    attachment_results = params.get("attachment_results", {})
    keywords_str = params.get("keywords", "")
    since = params.get("since", "")

    # Parse keywords: use custom if provided, else default finance keywords
    keywords = parse_keywords(keywords_str)
    if not keywords:
        keywords = FINANCE_KEYWORDS_DEFAULT

    # Parse since date for additional client-side filtering
    since_date = None
    if since:
        try:
            since_date = date.fromisoformat(since[:10])
        except ValueError:
            pass

    if isinstance(mails, str):
        try:
            mails = json.loads(mails)
        except (json.JSONDecodeError, ValueError):
            mails = []

    # attachment_results is a ForeachResult: {items: [{success, data}, ...], summary: {...}}
    att_items = []
    if isinstance(attachment_results, dict):
        att_items = attachment_results.get("items", [])

    items = []
    for i, mail in enumerate(mails):
        # Date filter (belt-and-suspenders on top of OData filter)
        received = mail.get("receivedDateTime", "")[:10]
        if since_date and received:
            try:
                if date.fromisoformat(received) < since_date:
                    continue
            except ValueError:
                pass

        # Keyword filter
        if not mail_matches_keywords(mail, keywords):
            continue

        # Extract attachments for this mail
        attachments = []
        if i < len(att_items):
            att_result = att_items[i]
            if att_result.get("success"):
                att_data = att_result.get("data", {})
                if isinstance(att_data, str):
                    try:
                        att_data = json.loads(att_data)
                    except (json.JSONDecodeError, ValueError):
                        att_data = {}
                if isinstance(att_data, dict):
                    attachments = att_data.get("value", [])

        items.append({
            "mail": mail,
            "attachments": attachments,
        })

    print(json.dumps(items))


if __name__ == "__main__":
    main()
