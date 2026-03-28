"""Trigger state persistence — backed by BrixDB (brix.db).

All data is stored in the central brix.db rather than a separate triggers.db.
This allows JOIN queries between pipeline runs and trigger events.

Migration from an existing triggers.db is handled by
``BrixDB.migrate_from_triggers_db()``, which is idempotent and safe to call
repeatedly.
"""
from __future__ import annotations

import json as _json
from pathlib import Path
from typing import Optional

# Keep for backward-compatibility (tests may reference this path)
TRIGGER_DB_PATH = Path.home() / ".brix" / "triggers.db"


class TriggerState:
    """Facade over BrixDB for trigger-related state.

    Parameters
    ----------
    db_path:
        Kept for backward compatibility (tests pass a tmp_path here).
        When provided, a BrixDB instance is created pointing at that path.
        When omitted, the shared default BrixDB (``~/.brix/brix.db``) is used.
    db:
        An explicit BrixDB instance to use.  Takes priority over ``db_path``.
    """

    def __init__(self, db_path: Optional[Path] = None, db=None) -> None:
        if db is not None:
            self._db = db
        else:
            from brix.db import BrixDB
            if db_path is not None:
                self._db = BrixDB(db_path=db_path)
            else:
                self._db = BrixDB()

    # ------------------------------------------------------------------
    # Dedup
    # ------------------------------------------------------------------

    def is_deduped(self, trigger_id: str, dedupe_key: str) -> bool:
        return self._db.trigger_state_is_deduped(trigger_id, dedupe_key)

    def record_fired(
        self,
        trigger_id: str,
        dedupe_key: str,
        run_id: Optional[str] = None,
    ) -> None:
        self._db.trigger_state_record_fired(trigger_id, dedupe_key, run_id)

    # ------------------------------------------------------------------
    # Pipeline events
    # ------------------------------------------------------------------

    def record_pipeline_completion(
        self,
        pipeline_name: str,
        run_id: str,
        status: str,
        result=None,
        input=None,
    ) -> None:
        """Record a pipeline_done event after Engine.run() completes."""
        self._db.pipeline_event_record(pipeline_name, run_id, status, result=result, input=input)

    def record_pipeline_event(
        self,
        run_id: str,
        pipeline_name: str,
        status: str,
        result_json: Optional[str] = None,
        input_json: Optional[str] = None,
    ) -> None:
        self._db.pipeline_event_record_raw(run_id, pipeline_name, status, result_json, input_json)

    def get_unprocessed_events(
        self,
        pipeline_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        return self._db.pipeline_event_get_unprocessed(pipeline_name, status)

    def mark_event_processed(self, event_id: int) -> None:
        self._db.pipeline_event_mark_processed(event_id)

    # ------------------------------------------------------------------
    # Last-check timestamps
    # ------------------------------------------------------------------

    def get_last_check(self, trigger_id: str) -> Optional[float]:
        """Return the Unix timestamp of the last poll for this trigger, or None."""
        return self._db.trigger_meta_get_last_check(trigger_id)

    def set_last_check(self, trigger_id: str, ts: float) -> None:
        """Persist the last poll timestamp for this trigger."""
        self._db.trigger_meta_set_last_check(trigger_id, ts)
