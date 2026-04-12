"""Canonical progress normalization for long-running external jobs."""
from __future__ import annotations

from typing import Any, Mapping


def _first(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _retry_state(entry: Mapping[str, Any]) -> str | None:
    explicit = _first(entry, "retry_state")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().lower()
    if entry.get("retry_pending") is True:
        return "pending"
    if entry.get("retry_applied") is True:
        return "applied"
    initial_mode = _first(entry, "initial_mode")
    final_mode = _first(entry, "final_mode")
    if initial_mode and final_mode and initial_mode != final_mode:
        return "applied"
    if entry.get("retry_on_low_quality") is True or entry.get("retry_enabled") is True:
        return "eligible"
    return "none"


def canonicalize_external_job_progress(progress: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normalize ad-hoc progress payloads into the canonical external-job shape."""
    entry = dict(progress or {})

    processed = _as_int(_first(entry, "processed", "done", "current", "items_processed")) or 0
    total = _as_int(_first(entry, "total", "items_total")) or 0
    page_current = _as_int(_first(entry, "page_current", "current_page", "page"))
    page_total = _as_int(_first(entry, "page_total", "total_pages", "pages_total"))
    explicit_percent = _as_float(_first(entry, "percent", "pct", "progress"))

    if total > 0:
        percent = round(processed / total * 100, 1)
        progress_kind = "item"
    elif page_current is not None and page_total and page_total > 0:
        percent = round(page_current / page_total * 100, 1)
        progress_kind = "page"
    else:
        percent = round(explicit_percent or 0.0, 1)
        progress_kind = "generic"

    canonical: dict[str, Any] = {
        "processed": processed,
        "total": total,
        "percent": percent,
        "progress_kind": progress_kind,
        "pct": round(explicit_percent if explicit_percent is not None else percent, 1),
        "done": processed,
        "msg": _first(entry, "message", "msg") or "",
    }

    optional_fields = {
        "service": _first(entry, "service"),
        "status": _first(entry, "status"),
        "eta_seconds": _as_float(_first(entry, "eta_seconds")),
        "message": _first(entry, "message", "msg"),
        "stage": _first(entry, "stage", "current_stage", "phase", "pipeline_step"),
        "current_stage": _first(entry, "current_stage", "stage", "phase", "pipeline_step"),
        "attempt": _as_int(_first(entry, "attempt", "attempt_number", "attempt_index")),
        "attempt_count": _as_int(_first(entry, "attempt_count")),
        "mode": _first(entry, "mode", "attempt_mode"),
        "retry_state": _retry_state(entry),
        "retry_reason": _first(entry, "retry_reason"),
        "request_id": _first(entry, "request_id"),
        "job_id": _first(entry, "job_id"),
        "page_current": page_current,
        "page_total": page_total,
        "upstream_attempt": _as_int(_first(entry, "upstream_attempt")),
        "metadata": entry.get("metadata") if isinstance(entry.get("metadata"), Mapping) else None,
    }
    for key, value in optional_fields.items():
        if value not in (None, ""):
            canonical[key] = value

    updated_at = _as_float(_first(entry, "_updated_at", "updated_at"))
    if updated_at is not None:
        canonical["_updated_at"] = updated_at

    return canonical
