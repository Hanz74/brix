"""SQLite run history and statistics."""
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

HISTORY_DB_PATH = Path.home() / ".brix" / "history.db"


class RunHistory:
    """SQLite-backed run history."""

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or HISTORY_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline TEXT NOT NULL,
                    version TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration REAL,
                    success INTEGER,
                    input_data TEXT,
                    steps_data TEXT,
                    result_summary TEXT,
                    triggered_by TEXT DEFAULT 'cli'
                )
            """)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path))

    def record_start(self, run_id: str, pipeline: str, version: str = None,
                     input_data: dict = None, triggered_by: str = "cli"):
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO runs (run_id, pipeline, version, started_at, input_data, triggered_by) VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, pipeline, version, datetime.utcnow().isoformat(),
                 json.dumps(input_data) if input_data else None, triggered_by)
            )

    def record_finish(self, run_id: str, success: bool, duration: float,
                      steps: dict = None, result_summary: dict = None):
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET finished_at=?, duration=?, success=?, steps_data=?, result_summary=? WHERE run_id=?",
                (datetime.utcnow().isoformat(), duration, int(success),
                 json.dumps(steps, default=str) if steps else None,
                 json.dumps(result_summary, default=str) if result_summary else None,
                 run_id)
            )

    def get_recent(self, limit: int = 10) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_run(self, run_id: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row else None

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

    def cleanup(self, older_than_days: int = 30) -> int:
        """Delete runs older than N days. Returns count deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM runs WHERE started_at < datetime('now', ?)",
                (f"-{older_than_days} days",)
            )
            return cursor.rowcount
