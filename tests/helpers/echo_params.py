#!/usr/bin/env python3
"""Echo params back as JSON."""
import json, sys
params = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
print(json.dumps({"received": params}))
