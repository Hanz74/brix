"""SQLite run history and statistics.

Runs are persisted in the central ~/.brix/brix.db (via BrixDB).
The legacy HISTORY_DB_PATH constant is kept for backwards-compatibility but
new instances write to brix.db by default.
"""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from brix.db import BrixDB, BRIX_DB_PATH

# Default DB path for RunHistory instances.
# Tests can patch this module-level constant to redirect storage.
# In production, this points to the central brix.db.
HISTORY_DB_PATH = BRIX_DB_PATH

# Known error patterns → actionable hints
_ERROR_HINTS: list[tuple[list[str], str]] = [
    (
        ["params", "not defined"],
        "Inline Python steps have no 'params' dict. Use Jinja2 {{ input.x }} directly.",
    ),
    (
        ["ModuleNotFoundError"],
        "Module not installed in container. Add it to requirements.txt or Dockerfile.",
    ),
    (
        ["No such file", "helpers/"],
        "Helper script not found. Is it in the helpers/ volume-mount?",
    ),
    (
        ["No such file"],
        "File or script path not found. Verify the path is correct and volume-mounted.",
    ),
    (
        ["Timeout"],
        "Step exceeded its timeout. Increase the timeout value or optimise the operation.",
    ),
    (
        ["HTTP 4"],
        "HTTP client error. Check the URL, authentication headers, and request body.",
    ),
    (
        ["HTTP 5"],
        "HTTP server error. The remote API returned an error — check logs on that service.",
    ),
    (
        ["Rate limited"],
        "Rate limit hit. Use on_error=retry with backoff, or add a delay between calls.",
    ),
    (
        ["MCP error"],
        "MCP tool call failed. Verify the server is running and the tool parameters are correct.",
    ),
    (
        ["NameError"],
        "Python NameError. Check for typos in variable names or missing imports.",
    ),
    (
        ["KeyError"],
        "Python KeyError. A dict key is missing — use .get() or check the data structure.",
    ),
    (
        ["foreach failed"],
        "One or more foreach items failed. Inspect individual item errors for details.",
    ),
    (
        ["asyncio.run"],
        "Brix runs async internally. Use synchronous httpx or subprocess in helpers, not asyncio.run().",
    ),
    (
        ["JSONDecodeError"],
        "Helper stdout must be valid JSON. Remove debug print() statements before json.dumps() output.",
    ),
    (
        ["Expecting value"],
        "Helper stdout must be valid JSON. Remove debug print() statements before json.dumps() output.",
    ),
    (
        ["UndefinedError"],
        "Jinja2 template variable not found. Add | default('') or check the step ID reference.",
    ),
    (
        ["is undefined"],
        "Jinja2 template variable not found. Add | default('') or check the step ID reference.",
    ),
    (
        ["too large"],
        "Payload too large. Pass file paths instead of base64 content in foreach steps.",
    ),
    (
        ["MemoryError"],
        "Payload too large. Pass file paths instead of base64 content in foreach steps.",
    ),
]


def _error_hint(step_id: str, error_message: str) -> str | None:
    """Return a hint string if the error matches a known pattern, else None."""
    combined = f"{step_id} {error_message}"
    for keywords, hint in _ERROR_HINTS:
        if all(kw in combined for kw in keywords):
            return hint
    return None


class RunHistory:
    """SQLite-backed run history.

    Delegates storage to the central BrixDB (brix.db).  When a custom
    ``db_path`` is supplied (e.g. in tests) a local BrixDB instance is used
    against that path so tests remain fully isolated.
    """

    def __init__(self, db_path: Path = None):
        # Use the provided path, or fall back to the module-level HISTORY_DB_PATH
        # (which tests can patch to redirect storage to a temp directory).
        resolved_path = Path(db_path) if db_path is not None else HISTORY_DB_PATH
        self._db = BrixDB(db_path=resolved_path)
        # Expose db_path for code that reads the attribute directly.
        self.db_path = self._db.db_path

    # ------------------------------------------------------------------
    # Internal helper kept for backward compat with methods that call it
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path))

    def record_start(self, run_id: str, pipeline: str, version: str = None,
                     input_data: dict = None, triggered_by: str = "cli",
                     idempotency_key: str = None, environment: dict = None,
                     container_id: str = None):
        self._db.record_run_start(
            run_id=run_id,
            pipeline=pipeline,
            version=version,
            input_data=input_data,
            triggered_by=triggered_by,
            idempotency_key=idempotency_key,
            environment=environment,
            container_id=container_id,
        )

    def find_by_idempotency_key(self, key: str, within_hours: int = 24) -> Optional[dict]:
        """Return the most recent finished successful run for *key* within *within_hours*.

        Returns None when no matching run is found.
        """
        return self._db.find_run_by_idempotency_key(key, within_hours=within_hours)

    def record_finish(self, run_id: str, success: bool, duration: float,
                      steps: dict = None, result_summary: dict = None,
                      cost_usd: float = None):
        self._db.record_run_finish(
            run_id=run_id,
            success=success,
            duration=duration,
            steps=steps,
            result_summary=result_summary,
            cost_usd=cost_usd,
        )

    def cancel_run(self, run_id: str, reason: str = "", cancelled_by: str = "user") -> bool:
        """Mark a run as cancelled. Returns True if the run was found and updated."""
        return self._db.cancel_run(run_id=run_id, reason=reason, cancelled_by=cancelled_by)

    def clean_orphaned_runs(self, max_age_hours: int = 24) -> int:
        """Mark stale unfinished runs as cancelled. Returns count of updated rows."""
        return self._db.clean_orphaned_runs(max_age_hours=max_age_hours)

    def get_recent(self, limit: int = 10) -> list[dict]:
        return self._db.get_recent_runs(limit=limit)

    def get_run(self, run_id: str) -> Optional[dict]:
        return self._db.get_run(run_id)

    def get_result(self, run_id: str) -> tuple[object, bool]:
        """Return (parsed_result, truncated) for a completed run.

        *parsed_result* is the deserialized pipeline output stored in
        result_summary, or None if the run does not exist / has no output.
        *truncated* is True when the raw JSON exceeded 10 KB and was not
        deserialized (the raw string is then returned as the first value so
        callers can decide what to show).
        """
        _TRUNCATE_BYTES = 10 * 1024  # 10 KB

        row = self.get_run(run_id)
        if row is None:
            return None, False
        raw = row.get("result_summary")
        if raw is None:
            return None, False
        # raw is a JSON string stored by record_finish
        if len(raw.encode()) > _TRUNCATE_BYTES:
            return raw, True
        try:
            return json.loads(raw), False
        except (json.JSONDecodeError, TypeError):
            return raw, False

    def get_stats(self, pipeline: str = None) -> dict:
        with self._connect() as conn:
            if pipeline:
                rows = conn.execute(
                    "SELECT success, duration FROM runs WHERE pipeline=? AND finished_at IS NOT NULL",
                    (pipeline,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT success, duration FROM runs WHERE finished_at IS NOT NULL"
                ).fetchall()

            if not rows:
                return {"total_runs": 0, "success_rate": 0, "avg_duration": 0}

            total = len(rows)
            successes = sum(1 for r in rows if r[0])
            durations = [r[1] for r in rows if r[1] is not None]

            return {
                "total_runs": total,
                "success_rate": round(successes / total * 100, 1) if total else 0,
                "avg_duration": round(sum(durations) / len(durations), 2) if durations else 0,
                "successes": successes,
                "failures": total - successes,
            }

    def get_step_stats(self, pipeline: str) -> list[dict]:
        """Return per-step analytics for a pipeline across all finished runs.

        Each entry contains:
          step_id, runs (count where step appeared), successes, failures,
          skips, avg_duration, min_duration, max_duration, avg_items.
        Only runs that have steps_data are considered.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT steps_data FROM runs WHERE pipeline=? AND finished_at IS NOT NULL AND steps_data IS NOT NULL",
                (pipeline,),
            ).fetchall()

        # Accumulate per-step metrics
        step_acc: dict[str, dict] = {}
        for (steps_json,) in rows:
            try:
                steps = json.loads(steps_json)
            except (json.JSONDecodeError, TypeError):
                continue
            for step_id, data in steps.items():
                if step_id not in step_acc:
                    step_acc[step_id] = {
                        "runs": 0,
                        "successes": 0,
                        "failures": 0,
                        "skips": 0,
                        "durations": [],
                        "items_list": [],
                    }
                acc = step_acc[step_id]
                acc["runs"] += 1
                status = data.get("status", "")
                if status == "ok":
                    acc["successes"] += 1
                elif status == "skipped":
                    acc["skips"] += 1
                else:
                    acc["failures"] += 1
                dur = data.get("duration")
                if dur is not None:
                    try:
                        acc["durations"].append(float(dur))
                    except (TypeError, ValueError):
                        pass
                items = data.get("items")
                if items is not None:
                    try:
                        acc["items_list"].append(int(items))
                    except (TypeError, ValueError):
                        pass

        result = []
        for step_id, acc in step_acc.items():
            durations = acc["durations"]
            items_list = acc["items_list"]
            result.append({
                "step_id": step_id,
                "runs": acc["runs"],
                "successes": acc["successes"],
                "failures": acc["failures"],
                "skips": acc["skips"],
                "avg_duration": round(sum(durations) / len(durations), 2) if durations else None,
                "min_duration": round(min(durations), 2) if durations else None,
                "max_duration": round(max(durations), 2) if durations else None,
                "avg_items": round(sum(items_list) / len(items_list)) if items_list else None,
            })
        return result

    def get_run_errors(
        self,
        run_id: str | None = None,
        pipeline: str | None = None,
        last: int = 1,
    ) -> list[dict]:
        """Return error details for one or more runs.

        If *run_id* is given, return errors from that specific run.
        If *pipeline* is given (and no *run_id*), return errors from the last
        *last* failed runs of that pipeline.

        Each entry has: run_id, step_id, error_message, hint.
        """
        if run_id:
            runs_data = []
            row = self.get_run(run_id)
            if row:
                runs_data.append(row)
        elif pipeline:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT * FROM runs WHERE pipeline=? AND success=0 AND finished_at IS NOT NULL "
                    "ORDER BY started_at DESC LIMIT ?",
                    (pipeline, last),
                ).fetchall()
            runs_data = [dict(r) for r in rows]
        else:
            return []

        errors: list[dict] = []
        for run in runs_data:
            rid = run.get("run_id", "")
            steps_json = run.get("steps_data")
            if not steps_json:
                continue
            try:
                steps = json.loads(steps_json)
            except (json.JSONDecodeError, TypeError):
                continue
            for step_id, data in steps.items():
                if data.get("status") != "error":
                    continue
                err_msg = data.get("error_message") or data.get("errors") or "unknown error"
                if not isinstance(err_msg, str):
                    err_msg = str(err_msg)
                errors.append({
                    "run_id": rid,
                    "step_id": step_id,
                    "error_message": err_msg,
                    "hint": _error_hint(step_id, err_msg),
                })
        return errors

    def get_run_log(self, run_id: str) -> list[dict]:
        """Return a structured execution log for a run.

        Each entry covers one step: step_id, status, duration, error_message.
        Steps are returned in the order they appear in steps_data.
        """
        row = self.get_run(run_id)
        if not row:
            return []
        steps_json = row.get("steps_data")
        if not steps_json:
            return []
        try:
            steps = json.loads(steps_json)
        except (json.JSONDecodeError, TypeError):
            return []

        log: list[dict] = []
        for step_id, data in steps.items():
            entry: dict = {
                "step_id": step_id,
                "status": data.get("status", "unknown"),
                "duration": data.get("duration"),
                "items": data.get("items"),
                "errors": data.get("errors"),
            }
            if data.get("error_message"):
                entry["error_message"] = data["error_message"]
            log.append(entry)
        return log

    def get_step_outputs(self, run_id: str) -> list[dict]:
        """Return all persisted step execution data for a run (T-BRIX-V7-04).

        Each entry contains: run_id, step_id, output, rendered_params,
        stderr_text, context, created_at.

        Returns an empty list when no step outputs have been persisted for
        this run (e.g. no steps had persist_output=true and BRIX_DEBUG was not set).
        """
        return self._db.get_step_outputs(run_id)

    def delete_run(self, run_id: str) -> bool:
        """Delete a single run from history by run_id.

        Returns True if a row was deleted, False if no matching run was found.
        """
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
            return cursor.rowcount > 0

    def annotate(self, run_id: str, notes: str) -> bool:
        """Attach or replace free-text notes on a run.

        Returns True if the run was found and updated, False if not found.
        """
        return self._db.annotate_run(run_id, notes)

    def search(
        self,
        pipeline: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        """Filter runs by pipeline, status, and/or time range.

        Parameters
        ----------
        pipeline:
            Exact pipeline name. Omit for all pipelines.
        status:
            ``'success'``, ``'failure'``, or ``'running'``.
        since:
            ISO-8601 start timestamp (inclusive).
        until:
            ISO-8601 end timestamp (inclusive).
        limit:
            Maximum rows to return (default 50).
        """
        return self._db.search_runs(
            pipeline=pipeline,
            status=status,
            since=since,
            until=until,
            limit=limit,
        )

    def cleanup(self, older_than_days: int = 30) -> int:
        """Delete runs older than N days. Returns count deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM runs WHERE started_at < datetime('now', ?)",
                (f"-{older_than_days} days",)
            )
            return cursor.rowcount
