#!/usr/bin/env python3
"""Helper that spawns an inner subprocess — simulates INBOX-366 scenario.

The inner subprocess calls `python3 -c 'import sys; sys.exit(0)'`.
If the parent's stdin is an open pipe (not DEVNULL), the inner process
inherits it and can block on communicate() waiting for data that never comes.
With DEVNULL, the inner process gets immediate EOF and exits cleanly.
"""
import json
import subprocess
import sys


def main():
    params = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}

    # Spawn an inner subprocess — this is the pattern that used to hang
    result = subprocess.run(
        ["python3", "-c", "import json, sys; print(json.dumps({'inner': 'ok'}))"],
        capture_output=True,
        text=True,
        timeout=5,
    )

    inner_data = json.loads(result.stdout.strip()) if result.stdout.strip() else {}
    print(json.dumps({"outer": "ok", "inner_exit": result.returncode, "inner_data": inner_data, "params": params}))


if __name__ == "__main__":
    main()
