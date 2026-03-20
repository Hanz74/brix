"""Save conversion results as .md files."""
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
    files = params.get("files", [])
    results = params.get("results", [])
    output_dir = Path(params["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    saved = []
    for file_info, result in zip(files, results):
        if isinstance(result, dict) and result.get("success") and result.get("markdown"):
            md_name = Path(file_info["filename"]).stem + ".md"
            md_path = output_dir / md_name
            md_path.write_text(result["markdown"], encoding="utf-8")
            saved.append({
                "source": file_info["filename"],
                "output": str(md_path),
                "size": len(result["markdown"]),
                "quality_grade": result.get("meta", {}).get("quality_grade", "?"),
                "scanned": result.get("meta", {}).get("scanned", False),
            })

    print(json.dumps(saved), file=sys.stdout)


if __name__ == "__main__":
    main()
