#!/usr/bin/env python3
"""Generate a summary report of downloaded attachments."""
import json
import sys


def main():
    params = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
    files = params.get("files", {})
    converted = params.get("converted", {})
    query = params.get("input_query", "")

    # Count files
    file_items = files.get("items", []) if isinstance(files, dict) else []
    successful_files = [f for f in file_items if isinstance(f, dict) and f.get("success")]
    failed_files = [f for f in file_items if isinstance(f, dict) and not f.get("success")]

    # Count conversions
    conv_items = converted.get("items", []) if isinstance(converted, dict) else []
    successful_conv = [c for c in conv_items if isinstance(c, dict) and c.get("success")]

    # Calculate total size
    total_size = sum(
        f.get("data", {}).get("size", 0)
        for f in successful_files
        if isinstance(f.get("data"), dict)
    )

    report = {
        "query": query,
        "total_files": len(successful_files),
        "failed_downloads": len(failed_files),
        "total_size_bytes": total_size,
        "total_size_human": _human_size(total_size),
        "converted": len(successful_conv),
        "files": [
            {
                "path": f.get("data", {}).get("path", ""),
                "filename": f.get("data", {}).get("filename", ""),
                "size": f.get("data", {}).get("size", 0),
            }
            for f in successful_files
            if isinstance(f.get("data"), dict)
        ],
    }

    print(json.dumps(report))


def _human_size(size_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


if __name__ == "__main__":
    main()
