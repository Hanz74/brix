"""Debounce logic for triggers (T-BRIX-DB-22).

When a TriggerConfig has a ``debounce`` field (e.g. "5m"), events are not fired
immediately.  Instead:

1. Each incoming event updates ``last_event_at`` and sets ``scheduled_at`` to
   ``now + debounce_seconds``.
2. When the trigger is evaluated again:
   - If ``scheduled_at`` is in the future → not yet ready.
   - If ``scheduled_at`` has passed **and** no newer events arrived → fire.
   - If newer events arrived after ``scheduled_at`` was set → the window was
     reset; recalculate ``scheduled_at``.

This implements "quiet-period debouncing": the pipeline fires only after the
source has been silent for the full debounce window.

The state is persisted in the ``debounce_state`` DB table:
    - trigger_name  TEXT PRIMARY KEY
    - last_event_at TEXT NOT NULL
    - scheduled_at  TEXT NOT NULL
"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any, Optional


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_duration_seconds(duration_str: str) -> float:
    """Parse duration string like '5s', '2m', '1h' into seconds."""
    s = duration_str.strip()
    if s.endswith("h"):
        return float(s[:-1]) * 3600.0
    if s.endswith("m"):
        return float(s[:-1]) * 60.0
    if s.endswith("s"):
        return float(s[:-1])
    return float(s)


def _ensure_table(db: Any) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS debounce_state (
        trigger_name   TEXT PRIMARY KEY,
        last_event_at  TEXT NOT NULL,
        scheduled_at   TEXT NOT NULL
    )
    """
    with db._connect() as conn:
        conn.execute(ddl)


def record_event(db: Any, trigger_name: str, debounce: str) -> dict:
    """Record an incoming event for the trigger and update the debounce window.

    Returns the new debounce state dict.
    """
    _ensure_table(db)
    now = _now_utc()
    debounce_secs = _parse_duration_seconds(debounce)
    scheduled_at = now + timedelta(seconds=debounce_secs)
    last_event_at_iso = now.isoformat()
    scheduled_at_iso = scheduled_at.isoformat()

    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO debounce_state (trigger_name, last_event_at, scheduled_at)
            VALUES (?, ?, ?)
            ON CONFLICT(trigger_name) DO UPDATE SET
                last_event_at = excluded.last_event_at,
                scheduled_at  = excluded.scheduled_at
            """,
            (trigger_name, last_event_at_iso, scheduled_at_iso),
        )
    return {
        "trigger_name": trigger_name,
        "last_event_at": last_event_at_iso,
        "scheduled_at": scheduled_at_iso,
    }


def is_ready_to_fire(db: Any, trigger_name: str) -> bool:
    """Check whether the debounce window has elapsed and the pipeline should fire.

    Returns True if:
    - There is a pending debounce state for this trigger, AND
    - ``scheduled_at`` is in the past (the quiet window has elapsed).

    After returning True the caller is responsible for clearing the state via
    ``clear_state()``.
    """
    _ensure_table(db)
    with db._connect() as conn:
        row = conn.execute(
            "SELECT scheduled_at FROM debounce_state WHERE trigger_name = ?",
            (trigger_name,),
        ).fetchone()
    if row is None:
        return False
    scheduled_at_str = row[0]
    try:
        scheduled_at = datetime.fromisoformat(scheduled_at_str)
    except ValueError:
        return False
    if scheduled_at.tzinfo is None:
        scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
    return _now_utc() >= scheduled_at


def clear_state(db: Any, trigger_name: str) -> None:
    """Remove the debounce state for a trigger after it has fired."""
    _ensure_table(db)
    with db._connect() as conn:
        conn.execute("DELETE FROM debounce_state WHERE trigger_name = ?", (trigger_name,))


def get_state(db: Any, trigger_name: str) -> Optional[dict]:
    """Return the current debounce state dict or None if not present."""
    _ensure_table(db)
    with db._connect() as conn:
        row = conn.execute(
            "SELECT trigger_name, last_event_at, scheduled_at FROM debounce_state WHERE trigger_name = ?",
            (trigger_name,),
        ).fetchone()
    if row is None:
        return None
    return {
        "trigger_name": row[0],
        "last_event_at": row[1],
        "scheduled_at": row[2],
    }
