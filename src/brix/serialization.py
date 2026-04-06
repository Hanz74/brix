"""Helpers for converting runtime values into JSON-safe structures."""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from typing import Any
from uuid import UUID


def sanitize_for_json(obj: Any) -> Any:
    """Recursively convert common Python runtime types into JSON-safe values."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, set):
        return [sanitize_for_json(item) for item in obj]
    if isinstance(obj, tuple):
        return [sanitize_for_json(item) for item in obj]
    if isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    if isinstance(obj, dict):
        return {str(key): sanitize_for_json(value) for key, value in obj.items()}
    return obj


def sanitize_row(row: dict) -> dict:
    """Convert a DB row dict into a JSON-safe dict."""
    return {key: sanitize_for_json(value) for key, value in row.items()}


def json_default(obj: Any) -> Any:
    """`json.dumps(default=...)` hook backed by `sanitize_for_json`."""
    sanitized = sanitize_for_json(obj)
    if sanitized is obj:
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
    return sanitized


def json_dumps(obj: Any, **kwargs: Any) -> str:
    """Serialize arbitrary runtime values after sanitizing them for JSON."""
    return json.dumps(sanitize_for_json(obj), **kwargs)
