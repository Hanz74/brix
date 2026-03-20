#!/usr/bin/env python3
"""Convert files through MarkItDown REST API with parallel execution."""
import asyncio
import base64
import json
import sys
from pathlib import Path

import httpx


API_URL = "http://markitdown-mcp:8081/v1/convert"


async def convert_one(client, file_info, semaphore):
    async with semaphore:
        try:
            b64 = base64.b64encode(Path(file_info["path"]).read_bytes()).decode()
            resp = await client.post(API_URL, json={
                "base64": b64,
                "filename": file_info["filename"],
                "accuracy": file_info.get("accuracy", "standard"),
            }, timeout=120.0)
            data = resp.json()
            return {
                "filename": file_info["filename"],
                "success": data.get("success", False),
                "markdown": data.get("markdown", ""),
                "quality_grade": data.get("meta", {}).get("quality_grade", "?"),
                "scanned": data.get("meta", {}).get("scanned", False),
                "duration_ms": data.get("meta", {}).get("duration_ms", 0),
                "error": data.get("error", {}).get("message") if not data.get("success") else None,
            }
        except Exception as e:
            return {
                "filename": file_info["filename"],
                "success": False,
                "markdown": "",
                "error": str(e),
            }


async def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    files = params.get("files", [])
    accuracy = params.get("accuracy", "standard")
    concurrency = params.get("concurrency", 5)

    for f in files:
        f["accuracy"] = accuracy

    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        tasks = [convert_one(client, f, semaphore) for f in files]
        results = await asyncio.gather(*tasks)

    print(json.dumps(list(results)), file=sys.stdout)


if __name__ == "__main__":
    asyncio.run(main())
