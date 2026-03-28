#!/usr/bin/env python3
"""Helper script that prints a JSON list of 101 items to stdout.

Used by performance-hint engine tests to trigger the >100 items sequential hint.
"""
import json

print(json.dumps(list(range(101))))
