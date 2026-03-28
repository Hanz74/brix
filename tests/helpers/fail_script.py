#!/usr/bin/env python3
"""Script that always fails."""
import sys
print("something went wrong", file=sys.stderr)
sys.exit(1)
