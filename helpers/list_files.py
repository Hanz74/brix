#!/usr/bin/env python3
"""List files in a directory, filtered by extension."""
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

    source_dir = Path(params["source_dir"])
    extensions = set(params.get("extensions", "pdf,docx,pptx,xlsx,png,jpg").split(","))

    files = []
    for f in sorted(source_dir.iterdir()):
        if f.is_file() and f.suffix.lstrip(".").lower() in extensions:
            files.append({
                "filename": f.name,
                "path": str(f),
                "size": f.stat().st_size,
            })

    print(json.dumps(files), file=sys.stdout)


if __name__ == "__main__":
    main()
