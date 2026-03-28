"""Queue/Buffer runner — collect items until threshold or time window (T-BRIX-DB-22)."""
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from brix.runners.base import BaseRunner
from brix.db import BrixDB


def _parse_duration_seconds(duration_str: str) -> float:
    """Parse duration string like '5s', '2m', '1h' into seconds as float."""
    s = duration_str.strip()
    if s.endswith("h"):
        return float(s[:-1]) * 3600.0
    if s.endswith("m"):
        return float(s[:-1]) * 60.0
    if s.endswith("s"):
        return float(s[:-1])
    # Plain number — assume seconds
    return float(s)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class QueueRunner(BaseRunner):
    """Collect items into a persistent buffer until a threshold or time window.

    Items are stored in the ``queue_buffer`` DB table.  When the threshold
    (``collect_until``) or time window (``collect_for``) is reached the runner
    returns the buffered array and clears the buffer.  Otherwise it returns a
    *waiting* response without clearing the buffer so subsequent pipeline runs
    continue to accumulate.

    Pipeline YAML example (count-based)::

        - id: buffer_invoices
          type: queue
          queue_name: invoice_queue
          collect_until: 10        # flush after 10 items
          flush_to: process_batch  # step_id to pass results to (informational)

    Pipeline YAML example (time-based)::

        - id: buffer_events
          type: queue
          queue_name: event_queue
          collect_for: "5m"        # flush after 5-minute window has elapsed

    Returns when buffer is NOT yet full::

        {
          "buffered": 3,
          "threshold": 10,
          "waiting": true
        }

    Returns when buffer IS flushed::

        {
          "items": [...],
          "flushed": 10,
          "waiting": false
        }
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "queue_name": {
                    "type": "string",
                    "description": "Unique name for this queue/buffer",
                },
                "collect_until": {
                    "type": "integer",
                    "description": "Flush after this many items are buffered",
                },
                "collect_for": {
                    "type": "string",
                    "description": "Flush after this time window (e.g. '5m', '1h')",
                },
                "flush_to": {
                    "type": "string",
                    "description": "Informational: step_id to pass flushed items to",
                },
                "pipeline_name": {
                    "type": "string",
                    "description": "Optional pipeline name stored in buffer metadata",
                },
            },
            "required": ["queue_name"],
        }

    def input_type(self) -> str:
        return "any"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        queue_name: str = getattr(step, "queue_name", None) or (
            (getattr(step, "params", None) or {}).get("queue_name", "default_queue")
        )
        collect_until: int | None = getattr(step, "collect_until", None)
        collect_for: str | None = getattr(step, "collect_for", None)
        flush_to: str | None = getattr(step, "flush_to", None)  # noqa: F841 — informational

        # Determine pipeline name for metadata
        pipeline_name = getattr(step, "pipeline_name", None) or ""
        if not pipeline_name:
            for attr in ("pipeline_name", "_pipeline_name"):
                if hasattr(context, attr):
                    pipeline_name = getattr(context, attr) or ""
                    break

        # Resolve the new item from step input (passed via context.last_output)
        new_item: Any = None
        if hasattr(context, "last_output"):
            new_item = context.last_output
        if new_item is None and hasattr(context, "input_data"):
            new_item = context.input_data

        self.report_progress(0.0, f"queue={queue_name} — checking buffer")

        db = BrixDB()

        # Ensure table exists
        _ensure_queue_buffer_table(db)

        # Load existing buffer
        row = _get_buffer(db, queue_name)
        if row is None:
            items: list = []
            created_at: str = _now_iso()
        else:
            items = json.loads(row["items"]) if row["items"] else []
            created_at = row["created_at"]

        # Append new item if provided
        if new_item is not None:
            items.append(new_item)

        # Determine whether to flush
        should_flush = False
        flush_reason = ""

        if collect_until is not None and len(items) >= collect_until:
            should_flush = True
            flush_reason = f"count threshold {collect_until} reached"
        elif collect_for is not None:
            window_seconds = _parse_duration_seconds(collect_for)
            created_dt = datetime.fromisoformat(created_at)
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - created_dt).total_seconds()
            if elapsed >= window_seconds:
                should_flush = True
                flush_reason = f"time window {collect_for} elapsed ({elapsed:.1f}s)"

        if should_flush:
            # Clear the buffer
            _delete_buffer(db, queue_name)
            flushed_count = len(items)
            self.report_progress(100.0, f"flushed {flushed_count} items — {flush_reason}")
            return {
                "success": True,
                "data": {
                    "items": items,
                    "flushed": flushed_count,
                    "waiting": False,
                    "queue_name": queue_name,
                    "flush_reason": flush_reason,
                },
                "duration": time.monotonic() - start,
            }
        else:
            # Persist updated buffer
            _upsert_buffer(db, queue_name, items, created_at, pipeline_name)
            threshold = collect_until if collect_until is not None else None
            self.report_progress(
                50.0,
                f"buffered {len(items)} items — waiting for threshold",
            )
            return {
                "success": True,
                "data": {
                    "buffered": len(items),
                    "threshold": threshold,
                    "waiting": True,
                    "queue_name": queue_name,
                },
                "duration": time.monotonic() - start,
            }


# ---------------------------------------------------------------------------
# DB helpers — queue_buffer table
# ---------------------------------------------------------------------------

def _ensure_queue_buffer_table(db: Any) -> None:
    """Create queue_buffer table if it does not exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS queue_buffer (
        queue_name   TEXT PRIMARY KEY,
        items        TEXT NOT NULL DEFAULT '[]',
        created_at   TEXT NOT NULL,
        pipeline_name TEXT NOT NULL DEFAULT ''
    )
    """
    with db._connect() as conn:
        conn.execute(ddl)


def _get_buffer(db: Any, queue_name: str) -> dict | None:
    with db._connect() as conn:
        row = conn.execute(
            "SELECT queue_name, items, created_at, pipeline_name FROM queue_buffer WHERE queue_name = ?",
            (queue_name,),
        ).fetchone()
    if row is None:
        return None
    return {
        "queue_name": row[0],
        "items": row[1],
        "created_at": row[2],
        "pipeline_name": row[3],
    }


def _upsert_buffer(
    db: Any,
    queue_name: str,
    items: list,
    created_at: str,
    pipeline_name: str,
) -> None:
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO queue_buffer (queue_name, items, created_at, pipeline_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(queue_name) DO UPDATE SET
                items = excluded.items,
                pipeline_name = excluded.pipeline_name
            """,
            (queue_name, json.dumps(items), created_at, pipeline_name),
        )


def _delete_buffer(db: Any, queue_name: str) -> None:
    with db._connect() as conn:
        conn.execute("DELETE FROM queue_buffer WHERE queue_name = ?", (queue_name,))
