#!/usr/bin/env python3
"""Flatten mail + attachment data into a list of downloadable items.

Input: mails (list of mail objects), attachment_results (foreach result with items)
Output: flat list of {mail_subject, mail_date, attachment_id, message_id, name, size, content_type, content_bytes}
"""
import json
import sys
import base64


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

    if isinstance(mails, str):
        try:
            mails = json.loads(mails)
        except (json.JSONDecodeError, ValueError):
            mails = []

    # attachment_results is a ForeachResult: {items: [...], summary: {...}}
    att_items = attachment_results.get("items", []) if isinstance(attachment_results, dict) else []

    flat = []
    for i, mail in enumerate(mails):
        subject = mail.get("subject", "unknown")
        date = mail.get("receivedDateTime", "")[:10]
        message_id = mail.get("id", "")

        # Get the corresponding attachment result
        if i < len(att_items):
            att_result = att_items[i]
            if not att_result.get("success"):
                continue

            # att_result.data is the MCP response (text or structured)
            att_data = att_result.get("data", {})
            if isinstance(att_data, str):
                try:
                    att_data = json.loads(att_data)
                except (json.JSONDecodeError, ValueError):
                    continue

            # Graph API returns {value: [attachment, ...]}
            attachments = att_data.get("value", []) if isinstance(att_data, dict) else []

            for att in attachments:
                if att.get("isInline"):
                    continue  # Skip inline images

                # Only include PDF attachments
                name = att.get("name", "")
                if not name.lower().endswith(".pdf"):
                    continue

                item = {
                    "mail_subject": subject,
                    "mail_date": date,
                    "message_id": message_id,
                    "attachment_id": att.get("id", ""),
                    "name": att.get("name", "attachment"),
                    "size": att.get("size", 0),
                    "content_type": att.get("contentType", ""),
                    "content_bytes": att.get("contentBytes", ""),
                }
                flat.append(item)

    print(json.dumps(flat))


if __name__ == "__main__":
    main()
