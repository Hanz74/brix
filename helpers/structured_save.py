#!/usr/bin/env python3
"""Save files with structured filenames."""
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
    content = params.get("content", "")
    metadata = params.get("metadata", {})
    output_dir = params.get("output_dir", "./attachments")

    # Build structured filename: YYYY-MM-DD_Subject_Filename
    date = metadata.get("mail_date", "unknown-date")
    subject = metadata.get("mail_subject", "unknown")[:50]
    # Sanitize subject for filename
    subject = "".join(c if c.isalnum() or c in "._- " else "_" for c in subject)
    subject = subject.strip().replace(" ", "_")
    filename = metadata.get("filename", "attachment")

    structured_name = f"{date}_{subject}_{filename}"

    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Save file
    file_path = Path(output_dir) / structured_name

    if isinstance(content, bytes):
        file_path.write_bytes(content)
    elif isinstance(content, str):
        file_path.write_text(content)
    else:
        file_path.write_text(json.dumps(content))

    result = {
        "path": str(file_path),
        "filename": structured_name,
        "size": file_path.stat().st_size,
        "original_name": metadata.get("filename", ""),
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
