#!/usr/bin/env python3
"""Debug: show raw params received."""
import json, sys

if len(sys.argv) > 1:
    raw = sys.argv[1]
elif not sys.stdin.isatty():
    raw = sys.stdin.read()
else:
    raw = "{}"

# Show what we got
result = {
    "raw_type": type(raw).__name__,
    "raw_length": len(raw),
    "raw_preview": raw[:200] if len(raw) > 200 else raw,
}
try:
    parsed = json.loads(raw)
    result["parsed_type"] = type(parsed).__name__
    result["parsed_keys"] = list(parsed.keys()) if isinstance(parsed, dict) else "not_dict"
    if isinstance(parsed, dict) and "data" in parsed:
        d = parsed["data"]
        result["data_type"] = type(d).__name__
        result["data_preview"] = str(d)[:200]
except:
    result["parse_error"] = True

print(json.dumps(result, indent=2))
