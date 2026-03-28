#!/usr/bin/env python3
"""Helper script that echoes the item from params back as JSON.

Used by foreach engine tests as a simple per-item processor.
"""
import json
import sys

if len(sys.argv) > 1:
    params = json.loads(sys.argv[1])
else:
    params = {}

item = params.get("item", None)
print(json.dumps({"value": item}))
