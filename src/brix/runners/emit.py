"""Emit runner — publish events to the event bus (T-BRIX-DB-22)."""
import json
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from brix.runners.base import BaseRunner
from brix.db import BrixDB


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EmitRunner(BaseRunner):
    """Emit a named event to the persistent event bus.

    Events are stored in the ``event_bus`` DB table and can be consumed by
    subsequent pipeline runs (or future event-triggered pipelines).

    Pipeline YAML example::

        - id: emit_order
          type: emit
          event: order.received
          data: "{{ input }}"

    Returns::

        {
          "event_id": "uuid",
          "event_name": "order.received",
          "emitted_at": "2026-...",
          "data": {...}
        }
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "event": {
                    "type": "string",
                    "description": "Event name (e.g. 'order.received')",
                },
                "data": {
                    "description": "Data payload to emit (Jinja2 rendered before execution)",
                },
            },
            "required": ["event"],
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        event_name: str = getattr(step, "event", None) or (
            (getattr(step, "params", None) or {}).get("event", "")
        )
        if not event_name:
            return {
                "success": False,
                "error": "EmitRunner: 'event' config field is required",
                "duration": time.monotonic() - start,
            }

        # Resolve data — may be pre-rendered by the engine or raw
        data: Any = getattr(step, "data", None)
        if data is None:
            data = (getattr(step, "params", None) or {}).get("data")
        if data is None and hasattr(context, "last_output"):
            data = context.last_output

        self.report_progress(0.0, f"emitting event '{event_name}'")

        db = BrixDB()
        _ensure_event_bus_table(db)

        event_id = str(uuid4())
        emitted_at = _now_iso()
        _insert_event(db, event_id, event_name, data, emitted_at)

        self.report_progress(100.0, f"event '{event_name}' emitted — id={event_id}")
        return {
            "success": True,
            "data": {
                "event_id": event_id,
                "event_name": event_name,
                "emitted_at": emitted_at,
                "data": data,
            },
            "duration": time.monotonic() - start,
        }


# ---------------------------------------------------------------------------
# DB helpers — event_bus table
# ---------------------------------------------------------------------------

def _ensure_event_bus_table(db: Any) -> None:
    """Create event_bus table if it does not exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS event_bus (
        id          TEXT PRIMARY KEY,
        event_name  TEXT NOT NULL,
        data        TEXT,
        emitted_at  TEXT NOT NULL,
        consumed    INTEGER NOT NULL DEFAULT 0
    )
    """
    with db._connect() as conn:
        conn.execute(ddl)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_bus_name ON event_bus(event_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_bus_consumed ON event_bus(consumed)")


def _insert_event(db: Any, event_id: str, event_name: str, data: Any, emitted_at: str) -> None:
    data_json = json.dumps(data) if data is not None else None
    with db._connect() as conn:
        conn.execute(
            "INSERT INTO event_bus (id, event_name, data, emitted_at, consumed) VALUES (?, ?, ?, ?, 0)",
            (event_id, event_name, data_json, emitted_at),
        )


def consume_events(db: Any, event_name: str, limit: int = 100) -> list[dict]:
    """Read and mark as consumed all pending events matching event_name.

    Returns a list of event dicts with keys: id, event_name, data, emitted_at.
    """
    _ensure_event_bus_table(db)
    with db._connect() as conn:
        rows = conn.execute(
            "SELECT id, event_name, data, emitted_at FROM event_bus "
            "WHERE event_name = ? AND consumed = 0 ORDER BY emitted_at ASC LIMIT ?",
            (event_name, limit),
        ).fetchall()
        if rows:
            ids = [r[0] for r in rows]
            placeholders = ",".join("?" * len(ids))
            conn.execute(
                f"UPDATE event_bus SET consumed = 1 WHERE id IN ({placeholders})",
                ids,
            )
    return [
        {
            "id": r[0],
            "event_name": r[1],
            "data": json.loads(r[2]) if r[2] else None,
            "emitted_at": r[3],
        }
        for r in rows
    ]


def peek_events(db: Any, event_name: str, consumed: bool = False, limit: int = 100) -> list[dict]:
    """Read events without marking them consumed."""
    _ensure_event_bus_table(db)
    with db._connect() as conn:
        rows = conn.execute(
            "SELECT id, event_name, data, emitted_at, consumed FROM event_bus "
            "WHERE event_name = ? AND consumed = ? ORDER BY emitted_at ASC LIMIT ?",
            (event_name, 1 if consumed else 0, limit),
        ).fetchall()
    return [
        {
            "id": r[0],
            "event_name": r[1],
            "data": json.loads(r[2]) if r[2] else None,
            "emitted_at": r[3],
            "consumed": bool(r[4]),
        }
        for r in rows
    ]
