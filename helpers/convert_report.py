"""Generate a summary report for folder conversion."""
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
    files = params.get("files", [])
    results = params.get("results", [])
    saved = params.get("saved", [])

    ok = len(saved)
    fail = len(files) - ok
    total_size = sum(f.get("size", 0) for f in files)
    total_md_size = sum(s.get("size", 0) for s in saved)

    report = {
        "total_files": len(files),
        "converted_ok": ok,
        "converted_fail": fail,
        "total_input_size_mb": round(total_size / 1024 / 1024, 2),
        "total_output_size_kb": round(total_md_size / 1024, 1),
        "files": saved,
    }

    print(json.dumps(report, indent=2), file=sys.stdout)


if __name__ == "__main__":
    main()
