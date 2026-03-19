#!/usr/bin/env python3
"""Extract first message ID from list-mail-messages response."""
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
        print(msgs[0]["id"])
    else:
        print("")
else:
    print("")
