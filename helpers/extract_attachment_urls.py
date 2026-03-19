#!/usr/bin/env python3
"""Extract attachment download URLs from M365 mail data."""
import json
import sys


def main():
    params = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
    messages = params.get("messages", [])

    if isinstance(messages, str):
        try:
            messages = json.loads(messages)
        except (json.JSONDecodeError, ValueError):
            messages = []

    attachments = []
    for msg in messages if isinstance(messages, list) else []:
        subject = msg.get("subject", "unknown")
        date = msg.get("receivedDateTime", "")[:10]
        for att in msg.get("attachments", []):
            attachments.append({
                "download_url": att.get("contentUrl", att.get("@microsoft.graph.downloadUrl", "")),
                "filename": att.get("name", "attachment"),
                "size": att.get("size", 0),
                "content_type": att.get("contentType", ""),
                "mail_subject": subject,
                "mail_date": date,
            })

    print(json.dumps(attachments))


if __name__ == "__main__":
    main()
