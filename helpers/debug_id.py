#!/usr/bin/env python3
"""Debug: show what extract_id returns."""
import json, sys

if len(sys.argv) > 1:
    params = json.loads(sys.argv[1])
elif not sys.stdin.isatty():
    params = json.loads(sys.stdin.read())
else:
    params = {}

data = params.get("data", {})
if isinstance(data, str):
    try:
        data = json.loads(data)
    except:
        pass

if isinstance(data, dict) and "value" in data:
    msgs = data["value"]
    if msgs:
        msg_id = msgs[0]["id"]
        # Output as JSON object with the ID for debugging
        print(json.dumps({
            "id": msg_id,
            "id_length": len(msg_id),
            "id_type": type(msg_id).__name__,
            "first_20": msg_id[:20],
            "last_20": msg_id[-20:],
        }))
