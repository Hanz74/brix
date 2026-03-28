#!/usr/bin/env python3
"""Helper script that prints a JSON list of 51 items to stdout.

Used by performance-hint engine tests to trigger the >50 items parallel concurrency hint.
"""
import json

print(json.dumps(list(range(51))))
