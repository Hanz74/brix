#!/usr/bin/env python3
"""Helper script that prints a JSON list to stdout.

Used by foreach engine tests as a source of iteration items.
"""
import json

print(json.dumps(["item1", "item2", "item3"]))
