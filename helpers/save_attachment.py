#!/usr/bin/env python3
"""Save a single attachment to disk with structured filename.

Input: {attachment: {mail_subject, mail_date, name, content_bytes, ...}, output_dir: str}
Output: {path, filename, size, original_name}
"""
import base64
import json
import sys
from pathlib import Path


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    attachment = params.get("attachment", {})
    output_dir = params.get("output_dir", "./attachments")

    date = attachment.get("mail_date", "unknown-date")
    subject = attachment.get("mail_subject", "unknown")[:50]
    # Sanitize subject for filename
    subject = "".join(c if c.isalnum() or c in "._- " else "_" for c in subject)
    subject = subject.strip().replace(" ", "_")
    original_name = attachment.get("name", "attachment")

    structured_name = f"{date}_{subject}_{original_name}"

    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(output_dir) / structured_name

    # Decode content_bytes (base64 from Graph API)
    content_b64 = attachment.get("content_bytes", "")
    if content_b64:
        try:
            content = base64.b64decode(content_b64)
            file_path.write_bytes(content)
        except Exception as e:
            print(json.dumps({
                "error": f"Failed to decode/save: {e}",
                "filename": structured_name,
            }))
            return
    else:
        # No content — just create metadata
        file_path.write_text(f"[No content for {original_name}]")

    result = {
        "path": str(file_path.absolute()),
        "filename": structured_name,
        "size": file_path.stat().st_size,
        "original_name": original_name,
        "content_type": attachment.get("content_type", ""),
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
