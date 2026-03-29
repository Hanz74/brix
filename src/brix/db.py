"""Brix central SQLite index — ~/.brix/brix.db

This module provides the single central database for all Brix metadata:

  - runs        (migrated from history.db — pipeline run records)
  - pipelines   (index of pipeline YAML files)
  - helpers     (index of helper registry entries)
  - pipeline_helpers  (many-to-many: which helpers a pipeline uses)
  - object_versions   (content history — prepared for T-BRIX-V5-07)
  - app_log     (application log entries — T-BRIX-V7-08)

Files remain the Source of Truth; the DB is an always-up-to-date index.
Sync happens:
  - On startup via BrixDB.sync_all()
  - Atomically on every create/update/delete via the per-module helpers

Migration:
  - Existing runs from history.db are imported once (idempotent)
  - registry.yaml helpers are imported once (idempotent)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from brix.config import config as _brix_config

BRIX_DB_PATH = Path.home() / ".brix" / "brix.db"
HISTORY_DB_PATH = Path.home() / ".brix" / "history.db"
REGISTRY_YAML_PATH = Path.home() / ".brix" / "helpers" / "registry.yaml"
PIPELINES_DIR = Path.home() / ".brix" / "pipelines"
CONTAINER_PIPELINES_DIR = Path(_brix_config.CONTAINER_PIPELINES_DIR)

# Retention defaults (overridable via env vars)
_DEFAULT_RETENTION_DAYS = 30
_DEFAULT_RETENTION_MAX_MB = 500


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS runs (
        run_id       TEXT PRIMARY KEY,
        pipeline     TEXT NOT NULL,
        version      TEXT,
        started_at   TEXT NOT NULL,
        finished_at  TEXT,
        duration     REAL,
        success      INTEGER,
        input_data   TEXT,
        steps_data   TEXT,
        result_summary TEXT,
        triggered_by TEXT DEFAULT 'cli',
        notes        TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipelines (
        id           TEXT PRIMARY KEY,
        name         TEXT NOT NULL UNIQUE,
        path         TEXT NOT NULL,
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL,
        requirements_json TEXT DEFAULT '[]'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS helpers (
        id               TEXT PRIMARY KEY,
        name             TEXT NOT NULL UNIQUE,
        script_path      TEXT NOT NULL,
        description      TEXT DEFAULT '',
        requirements_json TEXT DEFAULT '[]',
        input_schema_json TEXT DEFAULT '{}',
        output_schema_json TEXT DEFAULT '{}',
        created_at       TEXT NOT NULL,
        updated_at       TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_helpers (
        pipeline_id  TEXT NOT NULL,
        helper_id    TEXT NOT NULL,
        PRIMARY KEY (pipeline_id, helper_id),
        FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
        FOREIGN KEY (helper_id)   REFERENCES helpers(id)   ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS object_versions (
        id          TEXT PRIMARY KEY,
        type        TEXT NOT NULL,
        name        TEXT NOT NULL,
        version_id  TEXT NOT NULL,
        content     TEXT NOT NULL,
        created_at  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        id                TEXT PRIMARY KEY,
        timestamp         TEXT NOT NULL,
        tool              TEXT NOT NULL,
        source_session    TEXT,
        source_model      TEXT,
        source_agent      TEXT,
        arguments_summary TEXT
    )
    """,
    # V6-10: Agent-Kontext-Persistenz
    """
    CREATE TABLE IF NOT EXISTS agent_sessions (
        session_id            TEXT PRIMARY KEY,
        summary               TEXT NOT NULL DEFAULT '',
        active_pipeline       TEXT,
        last_run_id           TEXT,
        pending_decisions_json TEXT NOT NULL DEFAULT '[]',
        updated_at            TEXT NOT NULL
    )
    """,
    # V6-11: Resource-Claims (distributed locking)
    """
    CREATE TABLE IF NOT EXISTS resource_locks (
        resource_id TEXT PRIMARY KEY,
        run_id      TEXT NOT NULL,
        claimed_at  TEXT NOT NULL,
        expires_at  TEXT NOT NULL
    )
    """,
    # V6-12: Blackboard — shared KV-State
    """
    CREATE TABLE IF NOT EXISTS shared_state (
        key        TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    # V7-08: Application Log
    """
    CREATE TABLE IF NOT EXISTS app_log (
        id         TEXT PRIMARY KEY,
        timestamp  TEXT NOT NULL,
        level      TEXT NOT NULL,
        component  TEXT NOT NULL,
        message    TEXT NOT NULL
    )
    """,
    # V7-04: Step Outputs — persisted execution data per step
    """
    CREATE TABLE IF NOT EXISTS step_outputs (
        id                   TEXT PRIMARY KEY,
        run_id               TEXT NOT NULL,
        step_id              TEXT NOT NULL,
        output_json          TEXT,
        rendered_params_json TEXT,
        stderr_text          TEXT,
        context_json         TEXT,
        created_at           TEXT NOT NULL
    )
    """,
    # T-BRIX-MOD-02: Alert rules and history (consolidated from alerting.py)
    """
    CREATE TABLE IF NOT EXISTS alert_rules (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        condition   TEXT NOT NULL,
        channel     TEXT NOT NULL,
        config      TEXT NOT NULL DEFAULT '{}',
        enabled     INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alert_history (
        id          TEXT PRIMARY KEY,
        rule_id     TEXT NOT NULL,
        rule_name   TEXT NOT NULL,
        condition   TEXT NOT NULL,
        channel     TEXT NOT NULL,
        pipeline    TEXT,
        run_id      TEXT,
        message     TEXT NOT NULL,
        fired_at    TEXT NOT NULL
    )
    """,
    # T-BRIX-MOD-02: Trigger tables (consolidated from triggers/store.py)
    """
    CREATE TABLE IF NOT EXISTS triggers (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        type        TEXT NOT NULL,
        config_json TEXT NOT NULL DEFAULT '{}',
        pipeline    TEXT NOT NULL,
        enabled     INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        last_fired_at TEXT,
        last_run_id   TEXT,
        last_status   TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trigger_groups (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL DEFAULT '',
        triggers_json TEXT NOT NULL DEFAULT '[]',
        enabled     INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
    """,
    # T-BRIX-MOD-03: Trigger state tables (consolidated from triggers/state.py)
    """
    CREATE TABLE IF NOT EXISTS trigger_state (
        trigger_id TEXT NOT NULL,
        dedupe_key TEXT NOT NULL,
        run_id     TEXT,
        fired_at   REAL,
        status     TEXT DEFAULT 'fired',
        PRIMARY KEY (trigger_id, dedupe_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_events (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id        TEXT NOT NULL,
        pipeline_name TEXT NOT NULL,
        status        TEXT NOT NULL,
        result_json   TEXT,
        input_json    TEXT,
        fired_at      REAL NOT NULL,
        processed     INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trigger_meta (
        trigger_id TEXT PRIMARY KEY,
        last_check REAL
    )
    """,
    # T-BRIX-V7-10: Registry System — 6 knowledge registries
    """
    CREATE TABLE IF NOT EXISTS registry_templates (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_patterns (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_schemas (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_error_patterns (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_best_practices (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_lessons_learned (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
    """,
    # T-BRIX-DB-05b: Named DB-Connections
    """
    CREATE TABLE IF NOT EXISTS connections (
        id                TEXT PRIMARY KEY,
        name              TEXT UNIQUE NOT NULL,
        driver            TEXT NOT NULL DEFAULT 'postgresql',
        dsn_credential_id TEXT,
        env_var           TEXT,
        description       TEXT DEFAULT '',
        created_at        TEXT NOT NULL,
        updated_at        TEXT
    )
    """,
    # T-BRIX-DB-05d: Deprecated Step-Type Usage Tracking
    """
    CREATE TABLE IF NOT EXISTS deprecated_usage (
        pipeline_name  TEXT NOT NULL,
        step_id        TEXT NOT NULL,
        old_type       TEXT NOT NULL,
        new_type       TEXT NOT NULL,
        last_seen      TEXT NOT NULL,
        PRIMARY KEY (pipeline_name, step_id)
    )
    """,
    # T-BRIX-DB-06: DB-First — brick_definitions
    """
    CREATE TABLE IF NOT EXISTS brick_definitions (
        name TEXT PRIMARY KEY,
        runner TEXT NOT NULL,
        namespace TEXT DEFAULT '',
        category TEXT DEFAULT '',
        description TEXT DEFAULT '',
        when_to_use TEXT DEFAULT '',
        when_NOT_to_use TEXT DEFAULT '',
        aliases TEXT DEFAULT '[]',
        input_type TEXT DEFAULT '*',
        output_type TEXT DEFAULT '*',
        config_schema TEXT DEFAULT '{}',
        examples TEXT DEFAULT '[]',
        related_connector TEXT DEFAULT '',
        system BOOLEAN DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """,
    # T-BRIX-DB-06: DB-First — connector_definitions
    """
    CREATE TABLE IF NOT EXISTS connector_definitions (
        name TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        description TEXT DEFAULT '',
        required_mcp_server TEXT DEFAULT '',
        required_mcp_tools TEXT DEFAULT '[]',
        output_schema TEXT DEFAULT '{}',
        parameters TEXT DEFAULT '[]',
        related_pipelines TEXT DEFAULT '[]',
        related_helpers TEXT DEFAULT '[]',
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """,
    # T-BRIX-DB-06: DB-First — mcp_tool_schemas
    """
    CREATE TABLE IF NOT EXISTS mcp_tool_schemas (
        name TEXT PRIMARY KEY,
        description TEXT DEFAULT '',
        input_schema TEXT DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """,
    # T-BRIX-DB-06: DB-First — help_topics
    """
    CREATE TABLE IF NOT EXISTS help_topics (
        name TEXT PRIMARY KEY,
        title TEXT DEFAULT '',
        content TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """,
    # T-BRIX-DB-06: DB-First — keyword_taxonomies
    """
    CREATE TABLE IF NOT EXISTS keyword_taxonomies (
        category TEXT NOT NULL,
        keyword TEXT NOT NULL,
        language TEXT DEFAULT 'de',
        mapped_to TEXT DEFAULT '',
        PRIMARY KEY (category, keyword)
    )
    """,
    # T-BRIX-DB-06: DB-First — type_compatibility
    """
    CREATE TABLE IF NOT EXISTS type_compatibility (
        output_type TEXT NOT NULL,
        compatible_input TEXT NOT NULL,
        PRIMARY KEY (output_type, compatible_input)
    )
    """,
    # T-BRIX-DB-07: Run-Persistenz — vollständige Execution-Daten
    """
    CREATE TABLE IF NOT EXISTS step_executions (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        step_type TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        input_data TEXT DEFAULT '',
        output_data TEXT DEFAULT '',
        error_detail TEXT DEFAULT '',
        data_source TEXT DEFAULT '',
        started_at TEXT,
        ended_at TEXT,
        duration_ms INTEGER DEFAULT 0,
        persist_data BOOLEAN DEFAULT 1,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS foreach_item_executions (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        item_index INTEGER NOT NULL,
        item_input TEXT DEFAULT '',
        item_output TEXT DEFAULT '',
        status TEXT DEFAULT 'success',
        error_detail TEXT DEFAULT '',
        duration_ms INTEGER DEFAULT 0,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_inputs (
        run_id TEXT PRIMARY KEY,
        input_params TEXT DEFAULT '{}',
        trigger_data TEXT DEFAULT '{}',
        created_at TEXT NOT NULL
    )
    """,
    # T-BRIX-DB-13: Managed Variables
    """
    CREATE TABLE IF NOT EXISTS variables (
        name TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        description TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """,
    # T-BRIX-DB-13: Persistent Data Store
    """
    CREATE TABLE IF NOT EXISTS persistent_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        pipeline_name TEXT DEFAULT '',
        updated_at TEXT NOT NULL
    )
    """,
    # T-BRIX-DB-21: Resilience — Circuit Breaker state
    """
    CREATE TABLE IF NOT EXISTS circuit_breaker_state (
        brick_name TEXT PRIMARY KEY,
        failure_count INTEGER DEFAULT 0,
        last_failure TEXT,
        cooldown_until TEXT,
        updated_at TEXT
    )
    """,
    # T-BRIX-DB-21: Resilience — Rate Limiter state
    """
    CREATE TABLE IF NOT EXISTS rate_limiter_state (
        brick_name TEXT PRIMARY KEY,
        call_timestamps TEXT DEFAULT '[]',
        updated_at TEXT
    )
    """,
    # T-BRIX-DB-21: Resilience — Brick Cache (TTL-based)
    """
    CREATE TABLE IF NOT EXISTS brick_cache (
        cache_key TEXT PRIMARY KEY,
        output_data TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )
    """,
    # T-BRIX-DB-22: Advanced Flow — Event Bus
    """
    CREATE TABLE IF NOT EXISTS event_bus (
        id          TEXT PRIMARY KEY,
        event_name  TEXT NOT NULL,
        data        TEXT,
        emitted_at  TEXT NOT NULL,
        consumed    INTEGER NOT NULL DEFAULT 0
    )
    """,
    # T-BRIX-DB-22: Advanced Flow — Queue/Buffer
    """
    CREATE TABLE IF NOT EXISTS queue_buffer (
        queue_name    TEXT PRIMARY KEY,
        items         TEXT NOT NULL DEFAULT '[]',
        created_at    TEXT NOT NULL,
        pipeline_name TEXT NOT NULL DEFAULT ''
    )
    """,
    # T-BRIX-DB-22: Advanced Flow — Debounce State
    """
    CREATE TABLE IF NOT EXISTS debounce_state (
        trigger_name   TEXT PRIMARY KEY,
        last_event_at  TEXT NOT NULL,
        scheduled_at   TEXT NOT NULL
    )
    """,
    # T-BRIX-DB-23: Brick-Komposition Profiles/Mixins
    """
    CREATE TABLE IF NOT EXISTS profiles (
        name        TEXT PRIMARY KEY,
        config      TEXT NOT NULL DEFAULT '{}',
        description TEXT DEFAULT '',
        created_at  TEXT NOT NULL,
        updated_at  TEXT
    )
    """,
    # T-BRIX-DB-24: Step Pins — Mock-Daten für Testing
    """
    CREATE TABLE IF NOT EXISTS step_pins (
        pipeline_name   TEXT NOT NULL,
        step_id         TEXT NOT NULL,
        pinned_data     TEXT NOT NULL,
        pinned_from_run TEXT DEFAULT '',
        created_at      TEXT NOT NULL,
        PRIMARY KEY (pipeline_name, step_id)
    )
    """,
    # T-BRIX-ORG-02: Org Registry — known projects, tags, groups
    """
    CREATE TABLE IF NOT EXISTS org_registry (
        id          TEXT PRIMARY KEY,
        entry_type  TEXT NOT NULL,
        name        TEXT NOT NULL,
        description TEXT DEFAULT '',
        metadata    TEXT DEFAULT '{}',
        created_at  TEXT NOT NULL,
        UNIQUE (entry_type, name)
    )
    """,
]

# Valid registry type names → table names mapping (T-BRIX-V7-10)
REGISTRY_TYPES: dict[str, str] = {
    "templates": "registry_templates",
    "patterns": "registry_patterns",
    "schemas": "registry_schemas",
    "error_patterns": "registry_error_patterns",
    "best_practices": "registry_best_practices",
    "lessons_learned": "registry_lessons_learned",
}


# ---------------------------------------------------------------------------
# Core connection helper
# ---------------------------------------------------------------------------

class BrixDB:
    """Central SQLite index for Brix.

    Usage
    -----
    db = BrixDB()            # uses ~/.brix/brix.db
    db = BrixDB(path)        # custom path (tests)
    db.sync_all()            # resync YAML files → DB
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = Path(db_path) if db_path else BRIX_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @staticmethod
    def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
        """Check if a column exists in a table."""
        cursor = conn.execute(f"PRAGMA table_info({table})")
        return any(row[1] == column for row in cursor.fetchall())

    def _init_schema(self) -> None:
        with self._connect() as conn:
            for ddl in _DDL:
                conn.execute(ddl)
            # Idempotent migration: add notes column if not present (v5.1+)
            try:
                conn.execute("ALTER TABLE runs ADD COLUMN notes TEXT")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: add cost_usd column if not present (v6.21+)
            try:
                conn.execute("ALTER TABLE runs ADD COLUMN cost_usd REAL")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: add idempotency_key column (T-BRIX-V6-22)
            try:
                conn.execute("ALTER TABLE runs ADD COLUMN idempotency_key TEXT")
            except Exception:
                pass  # Column already exists — ignore
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_runs_idempotency_key "
                    "ON runs (idempotency_key, started_at)"
                )
            except Exception:
                pass
            # Idempotent migration: add cancel columns (T-BRIX-V6-BUG-03)
            try:
                conn.execute("ALTER TABLE runs ADD COLUMN cancel_reason TEXT")
            except Exception:
                pass  # Column already exists — ignore
            try:
                conn.execute("ALTER TABLE runs ADD COLUMN cancelled_by TEXT")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: step_outputs index (T-BRIX-V7-04)
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_step_outputs_run_id "
                    "ON step_outputs (run_id)"
                )
            except Exception:
                pass
            # Idempotent migration: environment_json column (T-BRIX-V7-05)
            try:
                conn.execute("ALTER TABLE runs ADD COLUMN environment_json TEXT")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: container_id column (T-BRIX-V7-07)
            try:
                conn.execute("ALTER TABLE runs ADD COLUMN container_id TEXT")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: T-BRIX-DB-07 indexes
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_step_executions_run_id "
                    "ON step_executions (run_id)"
                )
            except Exception:
                pass
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_foreach_item_executions_run_step "
                    "ON foreach_item_executions (run_id, step_id)"
                )
            except Exception:
                pass
            # Idempotent migration: last_progress column for step_executions (T-BRIX-DB-14)
            try:
                conn.execute(
                    "ALTER TABLE step_executions ADD COLUMN last_progress TEXT DEFAULT ''"
                )
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: secret column for variables (T-BRIX-DB-26)
            try:
                conn.execute(
                    "ALTER TABLE variables ADD COLUMN secret INTEGER DEFAULT 0"
                )
            except Exception:
                pass  # Column already exists — ignore

        # T-BRIX-DB-27: Run structured migrations after DDL baseline
        from brix.migrations import run_pending_migrations
        run_pending_migrations(self)

    # ------------------------------------------------------------------
    # Deprecated Usage Tracking (T-BRIX-DB-05d)
    # ------------------------------------------------------------------

    def record_deprecated_usage(
        self,
        pipeline_name: str,
        step_id: str,
        old_type: str,
        new_type: str,
    ) -> None:
        """Record (or update) a deprecated step-type usage entry."""
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO deprecated_usage (pipeline_name, step_id, old_type, new_type, last_seen)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(pipeline_name, step_id) DO UPDATE SET
                       old_type=excluded.old_type,
                       new_type=excluded.new_type,
                       last_seen=excluded.last_seen""",
                (pipeline_name, step_id, old_type, new_type, now),
            )

    def get_deprecated_usage(self) -> list[dict]:
        """Return all deprecated usage entries ordered by last_seen DESC."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT pipeline_name, step_id, old_type, new_type, last_seen "
                "FROM deprecated_usage ORDER BY last_seen DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_deprecated_count(self) -> int:
        """Return the total number of distinct deprecated step usages."""
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM deprecated_usage").fetchone()
        return row[0] if row else 0

    # ------------------------------------------------------------------
    # Step Outputs (T-BRIX-V7-04)
    # ------------------------------------------------------------------

    def save_step_output(
        self,
        run_id: str,
        step_id: str,
        output: Any = None,
        rendered_params: Any = None,
        stderr_text: Optional[str] = None,
        context_snapshot: Any = None,
    ) -> None:
        """Persist execution data for a single step.

        Parameters
        ----------
        run_id:
            The run this step belongs to.
        step_id:
            Step identifier within the pipeline.
        output:
            The step's output data (result["data"]).  Serialised to JSON.
        rendered_params:
            The resolved Jinja2 parameter values used for this step.
        stderr_text:
            Raw stderr captured by the Python runner (or None for other runner types).
        context_snapshot:
            A lightweight snapshot of the pipeline context (keys + types only).
        """
        row_id = str(uuid4())
        now = _now_iso()

        def _safe_json(value: Any) -> Optional[str]:
            if value is None:
                return None
            try:
                return json.dumps(value)
            except (TypeError, ValueError):
                return json.dumps(str(value))

        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO step_outputs
                   (id, run_id, step_id, output_json, rendered_params_json,
                    stderr_text, context_json, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    row_id,
                    run_id,
                    step_id,
                    _safe_json(output),
                    _safe_json(rendered_params),
                    stderr_text,
                    _safe_json(context_snapshot),
                    now,
                ),
            )

    def get_step_output(self, run_id: str, step_id: str) -> Optional[dict]:
        """Return the persisted execution data for one step, or None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM step_outputs WHERE run_id=? AND step_id=? ORDER BY created_at DESC LIMIT 1",
                (run_id, step_id),
            ).fetchone()
        if row is None:
            return None
        return self._deserialize_step_output(dict(row))

    def get_step_outputs(self, run_id: str) -> list[dict]:
        """Return all persisted step execution data for a run, ordered by creation time."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM step_outputs WHERE run_id=? ORDER BY created_at ASC",
                (run_id,),
            ).fetchall()
        return [self._deserialize_step_output(dict(r)) for r in rows]

    @staticmethod
    def _deserialize_step_output(row: dict) -> dict:
        """Deserialise JSON columns in a step_outputs row."""
        for col in ("output_json", "rendered_params_json", "context_json"):
            raw = row.pop(col, None)
            key = col[: -5] if col.endswith("_json") else col  # strip trailing _json
            try:
                row[key] = json.loads(raw) if raw is not None else None
            except (json.JSONDecodeError, TypeError):
                row[key] = raw
        return row

    def get_step_durations(
        self,
        pipeline: str,
        step_id: str,
        limit: int = 10,
    ) -> list[float]:
        """Return the last *limit* durations (seconds) for a given step across runs of *pipeline*.

        Only considers finished, successful runs where the step completed
        with status 'ok'. Returns durations in chronological order (oldest first).
        Used for regression detection (T-BRIX-V7-07).
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT r.steps_data
                   FROM runs r
                   WHERE r.pipeline = ?
                     AND r.success = 1
                     AND r.finished_at IS NOT NULL
                   ORDER BY r.started_at DESC
                   LIMIT ?""",
                (pipeline, limit),
            ).fetchall()

        durations: list[float] = []
        for row in reversed(rows):  # chronological order
            raw = row["steps_data"]
            if not raw:
                continue
            try:
                steps_data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            step_entry = steps_data.get(step_id)
            if not step_entry:
                continue
            if isinstance(step_entry, dict) and step_entry.get("status") == "ok":
                dur = step_entry.get("duration")
                if dur is not None:
                    try:
                        durations.append(float(dur))
                    except (TypeError, ValueError):
                        pass
        return durations

    def get_run_timeline(self, run_id: str) -> list[dict]:
        """Return a chronological timeline of steps for *run_id*.

        Each entry has: step_id, status, start_time, end_time, duration.
        start_time/end_time are estimated from run start + cumulative durations
        when no per-step timestamps are available.
        Used by the brix__get_timeline MCP tool (T-BRIX-V7-07).
        """
        run = self.get_run(run_id)
        if run is None:
            return []

        raw_steps = run.get("steps_data")
        if not raw_steps:
            return []

        try:
            steps_data = json.loads(raw_steps)
        except (json.JSONDecodeError, TypeError):
            return []

        run_started_at = run.get("started_at", "")

        # Build timeline: estimate wall-clock times from cumulative durations
        # starting at run_started_at.
        try:
            from datetime import datetime, timezone, timedelta
            base_dt = datetime.fromisoformat(run_started_at.replace("Z", "+00:00"))
        except Exception:
            base_dt = None

        timeline: list[dict] = []
        cursor_seconds: float = 0.0

        for step_id, entry in steps_data.items():
            if not isinstance(entry, dict):
                continue
            dur = entry.get("duration") or 0.0
            try:
                dur = float(dur)
            except (TypeError, ValueError):
                dur = 0.0

            if base_dt is not None:
                start_dt = base_dt + timedelta(seconds=cursor_seconds)
                end_dt = start_dt + timedelta(seconds=dur)
                start_time = start_dt.isoformat()
                end_time = end_dt.isoformat()
            else:
                start_time = None
                end_time = None

            resource_usage = entry.get("resource_usage")

            record: dict = {
                "step_id": step_id,
                "status": entry.get("status", "unknown"),
                "start_time": start_time,
                "end_time": end_time,
                "duration": dur,
            }
            if entry.get("error_message"):
                record["error_message"] = entry["error_message"]
            if resource_usage:
                record["resource_usage"] = resource_usage

            timeline.append(record)
            cursor_seconds += dur

        return timeline

    # ------------------------------------------------------------------
    # Run-Persistenz: Step Executions (T-BRIX-DB-07)
    # ------------------------------------------------------------------

    _MAX_DATA_BYTES = 1_048_576  # 1 MB JSON-Daten-Limit

    @staticmethod
    def _truncate_if_large(value: Any, label: str = "data") -> str:
        """Serialize value to JSON and truncate if it exceeds 1 MB."""
        try:
            serialized = json.dumps(value)
        except (TypeError, ValueError):
            serialized = json.dumps(str(value))
        if len(serialized.encode("utf-8")) > BrixDB._MAX_DATA_BYTES:
            return json.dumps({"__truncated__": True, "label": label, "hint": "data exceeded 1MB limit"})
        return serialized

    def record_step_execution(
        self,
        run_id: str,
        step_id: str,
        step_type: str = "",
        status: str = "success",
        input_data: Any = None,
        output_data: Any = None,
        error_detail: Any = None,
        data_source: str = "",
        started_at: Optional[str] = None,
        ended_at: Optional[str] = None,
        duration_ms: int = 0,
        persist_data: bool = True,
    ) -> None:
        """Record execution data for a single step (best-effort)."""
        now = _now_iso()
        row_id = str(uuid4())

        if persist_data:
            input_str = self._truncate_if_large(input_data, "input_data") if input_data is not None else ""
            output_str = self._truncate_if_large(output_data, "output_data") if output_data is not None else ""
        else:
            input_str = ""
            output_str = ""

        if error_detail is not None:
            try:
                error_str = json.dumps(error_detail)
            except (TypeError, ValueError):
                error_str = json.dumps(str(error_detail))
        else:
            error_str = ""

        try:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO step_executions
                       (id, run_id, step_id, step_type, status, input_data, output_data,
                        error_detail, data_source, started_at, ended_at, duration_ms,
                        persist_data, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        row_id, run_id, step_id, step_type, status,
                        input_str, output_str, error_str, data_source or "",
                        started_at, ended_at, duration_ms, 1 if persist_data else 0, now,
                    ),
                )
        except Exception:
            pass  # Never crash pipeline over persistence

    def record_foreach_item(
        self,
        run_id: str,
        step_id: str,
        item_index: int,
        item_input: Any = None,
        item_output: Any = None,
        status: str = "success",
        error_detail: Any = None,
        duration_ms: int = 0,
    ) -> None:
        """Record execution data for a single foreach item (best-effort)."""
        now = _now_iso()
        row_id = str(uuid4())

        input_str = self._truncate_if_large(item_input, "item_input") if item_input is not None else ""
        output_str = self._truncate_if_large(item_output, "item_output") if item_output is not None else ""
        if error_detail is not None:
            try:
                error_str = json.dumps(error_detail)
            except (TypeError, ValueError):
                error_str = json.dumps(str(error_detail))
        else:
            error_str = ""

        try:
            with self._connect() as conn:
                conn.execute(
                    """INSERT INTO foreach_item_executions
                       (id, run_id, step_id, item_index, item_input, item_output,
                        status, error_detail, duration_ms, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (row_id, run_id, step_id, item_index, input_str, output_str,
                     status, error_str, duration_ms, now),
                )
        except Exception:
            pass  # Never crash pipeline over persistence

    def record_run_input(
        self,
        run_id: str,
        input_params: Any = None,
        trigger_data: Any = None,
    ) -> None:
        """Persist the input params and trigger data for a run (best-effort)."""
        now = _now_iso()
        try:
            params_str = json.dumps(input_params) if input_params is not None else "{}"
        except (TypeError, ValueError):
            params_str = "{}"
        try:
            trigger_str = json.dumps(trigger_data) if trigger_data is not None else "{}"
        except (TypeError, ValueError):
            trigger_str = "{}"

        try:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO run_inputs (run_id, input_params, trigger_data, created_at)
                       VALUES (?,?,?,?)""",
                    (run_id, params_str, trigger_str, now),
                )
        except Exception:
            pass  # Never crash pipeline over persistence

    def get_step_executions(self, run_id: str, step_id: Optional[str] = None) -> list[dict]:
        """Return step execution records for a run, optionally filtered by step_id."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            if step_id is not None:
                rows = conn.execute(
                    "SELECT * FROM step_executions WHERE run_id=? AND step_id=? ORDER BY created_at ASC",
                    (run_id, step_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM step_executions WHERE run_id=? ORDER BY created_at ASC",
                    (run_id,),
                ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            for col in ("input_data", "output_data", "error_detail"):
                raw = d.get(col, "")
                if raw:
                    try:
                        d[col] = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        pass  # leave as string
            result.append(d)
        return result

    def update_step_progress(
        self,
        run_id: str,
        step_id: str,
        pct: float,
        msg: str = "",
        done: int = 0,
        total: int = 0,
    ) -> None:
        """Persist the latest progress snapshot for a step execution (T-BRIX-DB-14).

        Updates the most recent step_executions row matching (run_id, step_id).
        Best-effort — never raises.
        """
        now = _now_iso()
        progress_payload = json.dumps({
            "step_id": step_id,
            "pct": pct,
            "msg": msg,
            "done": done,
            "total": total,
            "updated_at": now,
        })
        try:
            with self._connect() as conn:
                conn.execute(
                    """UPDATE step_executions
                       SET last_progress = ?
                       WHERE run_id = ? AND step_id = ?
                         AND id = (
                             SELECT id FROM step_executions
                             WHERE run_id = ? AND step_id = ?
                             ORDER BY created_at DESC LIMIT 1
                         )""",
                    (progress_payload, run_id, step_id, run_id, step_id),
                )
        except Exception:
            pass  # Never crash pipeline over persistence

    def get_step_progress(self, run_id: str) -> list[dict]:
        """Return last_progress entries for all steps of a run (T-BRIX-DB-14).

        Returns a list of progress dicts ordered by step creation time.
        Only entries with non-empty last_progress are included.
        """
        try:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """SELECT step_id, last_progress, started_at
                       FROM step_executions
                       WHERE run_id = ? AND last_progress != ''
                       ORDER BY created_at ASC""",
                    (run_id,),
                ).fetchall()
            result = []
            for row in rows:
                raw = row["last_progress"]
                if raw:
                    try:
                        entry = json.loads(raw)
                        result.append(entry)
                    except (json.JSONDecodeError, TypeError):
                        pass
            return result
        except Exception:
            return []

    def get_foreach_items(self, run_id: str, step_id: str) -> list[dict]:
        """Return foreach item execution records for a run+step."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM foreach_item_executions WHERE run_id=? AND step_id=? ORDER BY item_index ASC",
                (run_id, step_id),
            ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            for col in ("item_input", "item_output", "error_detail"):
                raw = d.get(col, "")
                if raw:
                    try:
                        d[col] = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        pass
            result.append(d)
        return result

    def get_run_input(self, run_id: str) -> Optional[dict]:
        """Return the persisted run input for run_id, or None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM run_inputs WHERE run_id=?",
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        for col in ("input_params", "trigger_data"):
            raw = d.get(col, "{}")
            try:
                d[col] = json.loads(raw) if raw else {}
            except (json.JSONDecodeError, TypeError):
                pass
        return d

    # ------------------------------------------------------------------
    # Migration helpers (idempotent)
    # ------------------------------------------------------------------

    def migrate_from_history_db(self, history_db_path: Optional[Path] = None) -> int:
        """Copy runs from legacy history.db into brix.db.

        Returns the number of rows imported.
        Skips rows whose run_id already exists (idempotent).
        """
        src = Path(history_db_path) if history_db_path else HISTORY_DB_PATH
        if not src.exists():
            return 0
        try:
            src_conn = sqlite3.connect(str(src))
            src_conn.row_factory = sqlite3.Row
            rows = src_conn.execute("SELECT * FROM runs").fetchall()
            src_conn.close()
        except Exception:
            return 0

        imported = 0
        with self._connect() as conn:
            for row in rows:
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO runs
                           (run_id, pipeline, version, started_at, finished_at,
                            duration, success, input_data, steps_data,
                            result_summary, triggered_by)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            row["run_id"], row["pipeline"], row["version"],
                            row["started_at"], row["finished_at"],
                            row["duration"], row["success"],
                            row["input_data"], row["steps_data"],
                            row["result_summary"],
                            row["triggered_by"] if "triggered_by" in row.keys() else "cli",
                        ),
                    )
                    if conn.execute(
                        "SELECT changes()"
                    ).fetchone()[0]:
                        imported += 1
                except Exception:
                    continue
        return imported

    def migrate_from_registry_yaml(self, registry_path: Optional[Path] = None) -> int:
        """Import helpers from legacy registry.yaml into brix.db.

        Returns the number of helpers imported.
        Skips helpers whose name already exists (idempotent).
        """
        try:
            import yaml as _yaml
        except ImportError:
            return 0

        src = Path(registry_path) if registry_path else REGISTRY_YAML_PATH
        if not src.exists():
            return 0
        try:
            raw = _yaml.safe_load(src.read_text()) or {}
        except Exception:
            return 0
        if not isinstance(raw, dict):
            return 0

        imported = 0
        now = _now_iso()
        with self._connect() as conn:
            for name, data in raw.items():
                if not isinstance(data, dict):
                    continue
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO helpers
                           (id, name, script_path, description,
                            requirements_json, input_schema_json, output_schema_json,
                            created_at, updated_at)
                           VALUES (?,?,?,?,?,?,?,?,?)""",
                        (
                            data.get("id") or str(uuid4()),
                            name,
                            data.get("script", ""),
                            data.get("description", ""),
                            json.dumps(data.get("requirements", [])),
                            json.dumps(data.get("input_schema", {})),
                            json.dumps(data.get("output_schema", {})),
                            data.get("created_at") or now,
                            data.get("updated_at") or now,
                        ),
                    )
                    if conn.execute("SELECT changes()").fetchone()[0]:
                        imported += 1
                except Exception:
                    continue
        return imported

    def sync_pipelines_from_dirs(
        self,
        pipeline_dirs: Optional[list[Path]] = None,
    ) -> int:
        """Scan YAML pipeline files and upsert metadata into the pipelines table.

        Also resolves helper references in each pipeline and updates
        the pipeline_helpers join table.

        Returns the number of pipelines upserted.
        """
        dirs = pipeline_dirs if pipeline_dirs is not None else [
            PIPELINES_DIR, CONTAINER_PIPELINES_DIR
        ]
        try:
            import yaml as _yaml
        except ImportError:
            return 0

        upserted = 0
        seen_names: set[str] = set()

        for d in dirs:
            d = Path(d)
            if not d.exists():
                continue
            for yaml_file in sorted(d.glob("*.yaml")) + sorted(d.glob("*.yml")):
                try:
                    raw = _yaml.safe_load(yaml_file.read_text()) or {}
                except Exception:
                    continue
                name = raw.get("name") or yaml_file.stem
                if name in seen_names:
                    continue
                seen_names.add(name)

                now = _now_iso()
                pipeline_id = raw.get("id") or str(uuid4())
                requirements = raw.get("requirements", [])
                created_at = raw.get("created_at") or now
                updated_at = raw.get("updated_at") or now

                with self._connect() as conn:
                    # Preserve existing id if already indexed
                    existing = conn.execute(
                        "SELECT id, created_at FROM pipelines WHERE name=?", (name,)
                    ).fetchone()
                    if existing:
                        pipeline_id = existing[0]
                        created_at = existing[1]

                    conn.execute(
                        """INSERT INTO pipelines (id, name, path, created_at, updated_at, requirements_json)
                           VALUES (?,?,?,?,?,?)
                           ON CONFLICT(name) DO UPDATE SET
                             path=excluded.path,
                             updated_at=excluded.updated_at,
                             requirements_json=excluded.requirements_json
                        """,
                        (
                            pipeline_id, name, str(yaml_file),
                            created_at, updated_at,
                            json.dumps(requirements if isinstance(requirements, list) else []),
                        ),
                    )
                    upserted += 1

                    # Resolve helper references
                    self._sync_pipeline_helpers(conn, pipeline_id, raw)

        return upserted

    def _sync_pipeline_helpers(
        self, conn: sqlite3.Connection, pipeline_id: str, raw: dict
    ) -> None:
        """Update pipeline_helpers for a single pipeline.

        Scans all steps for ``helper:`` or ``script:`` fields that refer to
        a registered helper name, and inserts the join-table rows.
        """
        # Collect all helper names referenced in the pipeline steps
        steps = raw.get("steps", [])
        if not isinstance(steps, list):
            return

        helper_names: set[str] = set()
        for step in steps:
            if not isinstance(step, dict):
                continue
            h = step.get("helper")
            if h and isinstance(h, str):
                helper_names.add(h)

        if not helper_names:
            return

        # Remove old links for this pipeline
        conn.execute(
            "DELETE FROM pipeline_helpers WHERE pipeline_id=?", (pipeline_id,)
        )

        for hname in helper_names:
            row = conn.execute(
                "SELECT id FROM helpers WHERE name=?", (hname,)
            ).fetchone()
            if row:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO pipeline_helpers (pipeline_id, helper_id) VALUES (?,?)",
                        (pipeline_id, row[0]),
                    )
                except Exception:
                    pass

    def sync_all(
        self,
        history_db_path: Optional[Path] = None,
        registry_path: Optional[Path] = None,
        pipeline_dirs: Optional[list[Path]] = None,
    ) -> dict[str, int]:
        """Full sync: migrate legacy data + scan current YAML files.

        Returns a summary dict with counts of imported/upserted items.
        """
        runs_migrated = self.migrate_from_history_db(history_db_path)
        helpers_migrated = self.migrate_from_registry_yaml(registry_path)
        pipelines_synced = self.sync_pipelines_from_dirs(pipeline_dirs)
        # T-BRIX-MOD-03: migrate trigger state from legacy triggers.db if it exists
        trigger_counts = self.migrate_from_triggers_db()
        return {
            "runs_migrated": runs_migrated,
            "helpers_migrated": helpers_migrated,
            "pipelines_synced": pipelines_synced,
            "trigger_state_migrated": trigger_counts["trigger_state"],
            "pipeline_events_migrated": trigger_counts["pipeline_events"],
            "trigger_meta_migrated": trigger_counts["trigger_meta"],
        }

    # ------------------------------------------------------------------
    # Runs CRUD (delegates to/from history.py)
    # ------------------------------------------------------------------

    def record_run_start(
        self,
        run_id: str,
        pipeline: str,
        version: Optional[str] = None,
        input_data: Optional[dict] = None,
        triggered_by: str = "cli",
        idempotency_key: Optional[str] = None,
        environment: Optional[dict] = None,
        container_id: Optional[str] = None,
    ) -> None:
        env_json: Optional[str] = None
        if environment is not None:
            try:
                env_json = json.dumps(environment)
            except (TypeError, ValueError):
                env_json = json.dumps(str(environment))
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO runs
                   (run_id, pipeline, version, started_at, input_data, triggered_by,
                    idempotency_key, environment_json, container_id)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    run_id, pipeline, version,
                    _now_iso(),
                    json.dumps(input_data) if input_data else None,
                    triggered_by,
                    idempotency_key,
                    env_json,
                    container_id,
                ),
            )

    def find_run_by_idempotency_key(
        self,
        key: str,
        within_hours: int = 24,
    ) -> Optional[dict]:
        """Return the most recent finished successful run matching *key* within *within_hours*.

        Returns None if no matching run is found.
        Only considers runs that have finished successfully (finished_at IS NOT NULL and success=1).
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT * FROM runs
                   WHERE idempotency_key = ?
                     AND finished_at IS NOT NULL
                     AND success = 1
                     AND datetime(started_at) >= datetime('now', ?)
                   ORDER BY started_at DESC
                   LIMIT 1""",
                (key, f"-{within_hours} hours"),
            ).fetchone()
            return dict(row) if row else None

    def record_run_finish(
        self,
        run_id: str,
        success: bool,
        duration: float,
        steps: Optional[dict] = None,
        result_summary: Any = None,
        cost_usd: Optional[float] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE runs SET finished_at=?, duration=?, success=?,
                   steps_data=?, result_summary=?, cost_usd=? WHERE run_id=?""",
                (
                    _now_iso(), duration, int(success),
                    json.dumps(steps, default=str) if steps else None,
                    json.dumps(result_summary, default=str) if result_summary else None,
                    cost_usd,
                    run_id,
                ),
            )

    def get_run(self, run_id: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            return dict(row) if row else None

    def save_run_environment(self, run_id: str, environment: dict) -> None:
        """Persist an environment snapshot for a run (T-BRIX-V7-05).

        Parameters
        ----------
        run_id:
            The run to annotate.
        environment:
            Dict containing python_version, installed_packages, mcp_servers, and
            any other environment details captured at run start.
        """
        try:
            env_json = json.dumps(environment)
        except (TypeError, ValueError):
            env_json = json.dumps(str(environment))
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET environment_json=? WHERE run_id=?",
                (env_json, run_id),
            )

    def get_run_environment(self, run_id: str) -> Optional[dict]:
        """Return the environment snapshot for a run, or None if not recorded."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT environment_json FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None or row[0] is None:
            return None
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return None

    def get_recent_runs(self, limit: int = 10) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_run(self, run_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
            return cursor.rowcount > 0

    def annotate_run(self, run_id: str, notes: str) -> bool:
        """Attach or replace notes on a run. Returns True if the run was found."""
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE runs SET notes=? WHERE run_id=?", (notes, run_id)
            )
            return cursor.rowcount > 0

    def search_runs(
        self,
        pipeline: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        """Filter runs by pipeline name, status, and/or time range.

        Parameters
        ----------
        pipeline:
            Exact pipeline name filter. Omit for all pipelines.
        status:
            ``'success'``, ``'failure'``, or ``'running'`` (not yet finished).
        since:
            ISO-8601 timestamp — only runs started at or after this time.
        until:
            ISO-8601 timestamp — only runs started before or at this time.
        limit:
            Maximum rows returned (default 50).
        """
        clauses: list[str] = []
        params: list[Any] = []

        if pipeline:
            clauses.append("pipeline = ?")
            params.append(pipeline)

        if status == "success":
            clauses.append("success = 1")
        elif status == "failure":
            clauses.append("success = 0")
            clauses.append("finished_at IS NOT NULL")
        elif status == "running":
            clauses.append("finished_at IS NULL")

        if since:
            clauses.append("started_at >= ?")
            params.append(since)
        if until:
            clauses.append("started_at <= ?")
            params.append(until)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT * FROM runs {where} ORDER BY started_at DESC LIMIT ?",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def cleanup_runs(self, older_than_days: int = 30) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM runs WHERE started_at < datetime('now', ?)",
                (f"-{older_than_days} days",),
            )
            return cursor.rowcount

    def cancel_run(
        self,
        run_id: str,
        reason: str = "",
        cancelled_by: str = "user",
    ) -> bool:
        """Mark a run as cancelled in history.

        Sets finished_at = now(), success = 0, cancel_reason, cancelled_by.
        Returns True if the run was found and updated.
        """
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE runs
                   SET finished_at=?, success=0, cancel_reason=?, cancelled_by=?
                   WHERE run_id=? AND finished_at IS NULL""",
                (_now_iso(), reason, cancelled_by, run_id),
            )
            return cursor.rowcount > 0

    def clean_orphaned_runs(self, max_age_hours: int = 24) -> int:
        """Mark runs that never finished as cancelled after *max_age_hours*.

        A run is considered orphaned when ``finished_at IS NULL`` and
        ``started_at`` is older than *max_age_hours*.  Returns the number of
        runs updated.
        """
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE runs
                   SET finished_at=?, success=0,
                       cancel_reason='orphaned (no heartbeat)',
                       cancelled_by='brix-cleanup'
                   WHERE finished_at IS NULL
                     AND datetime(started_at) < datetime('now', ?)""",
                (_now_iso(), f"-{max_age_hours} hours"),
            )
            return cursor.rowcount

    def get_monthly_cost_usd(self, year: Optional[int] = None, month: Optional[int] = None) -> float:
        """Return the total cost_usd for all runs in the given calendar month.

        If *year* and *month* are omitted, the current UTC month is used.
        Returns 0.0 when there are no cost-tracked runs.
        """
        now = datetime.now(timezone.utc)
        y = year if year is not None else now.year
        m = month if month is not None else now.month
        # ISO prefix match: "YYYY-MM"
        month_prefix = f"{y:04d}-{m:02d}"
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(cost_usd), 0.0) FROM runs WHERE started_at LIKE ?",
                (f"{month_prefix}%",),
            ).fetchone()
        return float(row[0]) if row else 0.0

    # ------------------------------------------------------------------
    # Pipelines CRUD
    # ------------------------------------------------------------------

    def upsert_pipeline(
        self,
        name: str,
        path: str,
        requirements: Optional[list[str]] = None,
        pipeline_id: Optional[str] = None,
        yaml_content: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> str:
        """Insert or update a pipeline index entry. Returns the pipeline id."""
        now = _now_iso()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id, created_at FROM pipelines WHERE name=?", (name,)
            ).fetchone()
            if existing:
                pid = existing[0]
                created_at = existing[1]
            else:
                pid = pipeline_id or str(uuid4())
                created_at = now

            # Build dynamic column list based on which optional columns exist
            has_yaml_content = self._column_exists(conn, "pipelines", "yaml_content")
            has_project = self._column_exists(conn, "pipelines", "project")
            has_tags = self._column_exists(conn, "pipelines", "tags")
            has_group = self._column_exists(conn, "pipelines", "group_name")

            cols = ["id", "name", "path", "created_at", "updated_at", "requirements_json"]
            vals: list = [pid, name, path, created_at, now, json.dumps(requirements or [])]
            updates = [
                "path=excluded.path",
                "updated_at=excluded.updated_at",
                "requirements_json=excluded.requirements_json",
            ]

            if has_yaml_content and yaml_content is not None:
                cols.append("yaml_content")
                vals.append(yaml_content)
                updates.append("yaml_content=excluded.yaml_content")

            if has_project and project is not None:
                cols.append("project")
                vals.append(project)
                updates.append("project=excluded.project")

            if has_tags and tags is not None:
                cols.append("tags")
                vals.append(json.dumps(tags))
                updates.append("tags=excluded.tags")

            if has_group and group_name is not None:
                cols.append("group_name")
                vals.append(group_name)
                updates.append("group_name=excluded.group_name")

            placeholders = ",".join("?" * len(cols))
            col_str = ",".join(cols)
            update_str = ",".join(updates)

            conn.execute(
                f"""INSERT INTO pipelines ({col_str})
                   VALUES ({placeholders})
                   ON CONFLICT(name) DO UPDATE SET {update_str}
                """,
                vals,
            )
        return pid

    def delete_pipeline(self, name: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM pipelines WHERE name=?", (name,))
            return cursor.rowcount > 0

    def get_pipeline(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM pipelines WHERE name=?", (name,)
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            result["requirements"] = json.loads(result.get("requirements_json") or "[]")
            return result

    def list_pipelines(
        self,
        project: Optional[str] = None,
        group_name: Optional[str] = None,
        tags: Optional[list] = None,
    ) -> list[dict]:
        """Return pipelines, optionally filtered by project, group_name, or tags."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM pipelines ORDER BY name"
            ).fetchall()
            out = []
            has_project = self._column_exists(conn, "pipelines", "project")
            has_tags = self._column_exists(conn, "pipelines", "tags")
            has_group = self._column_exists(conn, "pipelines", "group_name")

        for row in rows:
            d = dict(row)
            d["requirements"] = json.loads(d.get("requirements_json") or "[]")
            if has_project:
                d.setdefault("project", "")
            if has_tags:
                raw_tags = d.get("tags") or "[]"
                try:
                    d["tags"] = json.loads(raw_tags) if isinstance(raw_tags, str) else raw_tags
                except (json.JSONDecodeError, TypeError):
                    d["tags"] = []
            if has_group:
                d.setdefault("group_name", "")

            # Apply filters
            if project is not None and d.get("project", "") != project:
                continue
            if group_name is not None and d.get("group_name", "") != group_name:
                continue
            if tags is not None:
                pipeline_tags = d.get("tags", [])
                if not any(t in pipeline_tags for t in tags):
                    continue

            out.append(d)
        return out

    def pipeline_set_project(self, name: str, project: str) -> bool:
        """Update the project field for a pipeline. Returns True if updated."""
        with self._connect() as conn:
            if not self._column_exists(conn, "pipelines", "project"):
                return False
            cursor = conn.execute(
                "UPDATE pipelines SET project=? WHERE name=?", (project, name)
            )
            return cursor.rowcount > 0

    def delete_pipelines_by_project(self, project: str) -> int:
        """Delete all pipelines with the given project. Returns count deleted."""
        with self._connect() as conn:
            if not self._column_exists(conn, "pipelines", "project"):
                return 0
            cursor = conn.execute(
                "DELETE FROM pipelines WHERE project=?", (project,)
            )
            return cursor.rowcount

    def get_project_stats(self) -> dict[str, dict]:
        """Return per-project counts for pipelines and helpers.

        Returns {project: {pipelines: N, helpers: M}}.
        """
        stats: dict[str, dict] = {}

        with self._connect() as conn:
            has_p_project = self._column_exists(conn, "pipelines", "project")
            has_h_project = self._column_exists(conn, "helpers", "project")

            if has_p_project:
                rows = conn.execute(
                    "SELECT COALESCE(project,'') as proj, COUNT(*) as cnt "
                    "FROM pipelines GROUP BY proj"
                ).fetchall()
                for row in rows:
                    proj = row[0] or ""
                    stats.setdefault(proj, {"pipelines": 0, "helpers": 0})
                    stats[proj]["pipelines"] = row[1]

            if has_h_project:
                rows = conn.execute(
                    "SELECT COALESCE(project,'') as proj, COUNT(*) as cnt "
                    "FROM helpers GROUP BY proj"
                ).fetchall()
                for row in rows:
                    proj = row[0] or ""
                    stats.setdefault(proj, {"pipelines": 0, "helpers": 0})
                    stats[proj]["helpers"] = row[1]

        return stats

    # ------------------------------------------------------------------
    # Org Registry — known projects, tags, groups (T-BRIX-ORG-02)
    # ------------------------------------------------------------------

    def org_registry_upsert(self, entry_type: str, name: str, description: str = "", metadata: Optional[dict] = None) -> str:
        """Insert or update an org registry entry. Returns the entry id."""
        now = _now_iso()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM org_registry WHERE entry_type=? AND name=?",
                (entry_type, name),
            ).fetchone()
            eid = existing[0] if existing else str(uuid4())
            conn.execute(
                """
                INSERT INTO org_registry (id, entry_type, name, description, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(entry_type, name) DO UPDATE SET
                    description=excluded.description,
                    metadata=excluded.metadata
                """,
                (eid, entry_type, name, description, json.dumps(metadata or {}), now),
            )
        return eid

    def org_registry_list(self, entry_type: Optional[str] = None) -> list[dict]:
        """List org registry entries, optionally filtered by entry_type."""
        with self._connect() as conn:
            if entry_type:
                rows = conn.execute(
                    "SELECT id, entry_type, name, description, metadata, created_at "
                    "FROM org_registry WHERE entry_type=? ORDER BY name",
                    (entry_type,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, entry_type, name, description, metadata, created_at "
                    "FROM org_registry ORDER BY entry_type, name"
                ).fetchall()
        result = []
        for row in rows:
            try:
                meta = json.loads(row[4]) if row[4] else {}
            except Exception:
                meta = {}
            result.append({
                "id": row[0],
                "entry_type": row[1],
                "name": row[2],
                "description": row[3],
                "metadata": meta,
                "created_at": row[5],
            })
        return result

    def org_registry_delete(self, entry_type: str, name: str) -> bool:
        """Delete an org registry entry. Returns True if deleted."""
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM org_registry WHERE entry_type=? AND name=?",
                (entry_type, name),
            )
        return cur.rowcount > 0

    def org_registry_seed_defaults(self) -> None:
        """Seed default known projects, tags, and groups if they don't exist yet."""
        default_projects = [
            ("buddy", "Dokumenten-Verarbeitung — E-Mail-Intake, OneDrive, Klassifizierung"),
            ("cody", "Projektmanagement — Tasks, Gatekeeper, Pipelines"),
            ("utility", "Allgemeine Tools — Konvertierung, Download, Transformation"),
            ("system", "Brix-interne Pipelines — Wartung, Migrations, Health-Checks"),
        ]
        default_tags = [
            ("intake", "Daten-Eingang / Ingest-Pipelines"),
            ("extraction", "Daten-Extraktion aus Dokumenten"),
            ("classification", "Klassifizierung und Kategorisierung"),
            ("monitoring", "Überwachung und Alerting"),
            ("scheduled", "Zeitgesteuerte Ausführung"),
            ("one-shot", "Einmalige / manuelle Ausführung"),
            ("conversion", "Format-Konvertierung (PDF, DOCX, etc.)"),
            ("notification", "Benachrichtigungen und Alerts"),
            ("batch", "Batch-Verarbeitung großer Mengen"),
        ]
        default_groups = [
            ("onedrive-chain", "scan→download→classify→extract — OneDrive-Dokumentenverarbeitung"),
            ("outlook-intake", "fetch→classify→move→process — Outlook E-Mail-Intake"),
        ]
        for name, desc in default_projects:
            self.org_registry_upsert("project", name, desc)
        for name, desc in default_tags:
            self.org_registry_upsert("tag", name, desc)
        for name, desc in default_groups:
            self.org_registry_upsert("group", name, desc)

    # ------------------------------------------------------------------
    # Helpers CRUD
    # ------------------------------------------------------------------

    def upsert_helper(
        self,
        name: str,
        script_path: str,
        description: str = "",
        requirements: Optional[list[str]] = None,
        input_schema: Optional[dict] = None,
        output_schema: Optional[dict] = None,
        helper_id: Optional[str] = None,
        code: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> str:
        """Insert or update a helper index entry. Returns the helper id."""
        now = _now_iso()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id, created_at FROM helpers WHERE name=?", (name,)
            ).fetchone()
            if existing:
                hid = existing[0]
                created_at = existing[1]
            else:
                hid = helper_id or str(uuid4())
                created_at = now

            has_code_col = self._column_exists(conn, "helpers", "code")
            has_project = self._column_exists(conn, "helpers", "project")
            has_tags = self._column_exists(conn, "helpers", "tags")
            has_group = self._column_exists(conn, "helpers", "group_name")

            # Build dynamic column list
            cols = [
                "id", "name", "script_path", "description",
                "requirements_json", "input_schema_json", "output_schema_json",
                "created_at", "updated_at",
            ]
            vals: list = [
                hid, name, script_path, description,
                json.dumps(requirements or []),
                json.dumps(input_schema or {}),
                json.dumps(output_schema or {}),
                created_at, now,
            ]
            updates = [
                "script_path=excluded.script_path",
                "description=excluded.description",
                "requirements_json=excluded.requirements_json",
                "input_schema_json=excluded.input_schema_json",
                "output_schema_json=excluded.output_schema_json",
                "updated_at=excluded.updated_at",
            ]

            if has_code_col and code is not None:
                cols.append("code")
                vals.append(code)
                updates.append("code=excluded.code")

            if has_project and project is not None:
                cols.append("project")
                vals.append(project)
                updates.append("project=excluded.project")

            if has_tags and tags is not None:
                cols.append("tags")
                vals.append(json.dumps(tags))
                updates.append("tags=excluded.tags")

            if has_group and group_name is not None:
                cols.append("group_name")
                vals.append(group_name)
                updates.append("group_name=excluded.group_name")

            placeholders = ",".join("?" * len(cols))
            col_str = ",".join(cols)
            update_str = ",".join(updates)
            conn.execute(
                f"""INSERT INTO helpers ({col_str})
                   VALUES ({placeholders})
                   ON CONFLICT(name) DO UPDATE SET {update_str}
                """,
                vals,
            )
        return hid

    def delete_helper(self, name: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM helpers WHERE name=?", (name,))
            return cursor.rowcount > 0

    def get_helper(self, name: str) -> Optional[dict]:
        """Get helper by name or by UUID."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM helpers WHERE name=?", (name,)
            ).fetchone()
            if not row:
                # UUID fallback
                row = conn.execute(
                    "SELECT * FROM helpers WHERE id=?", (name,)
                ).fetchone()
            if not row:
                return None
            return self._helper_row_to_dict(dict(row))

    def list_helpers(
        self,
        project: Optional[str] = None,
        group_name: Optional[str] = None,
        tags: Optional[list] = None,
    ) -> list[dict]:
        """Return helpers, optionally filtered by project, group_name, or tags."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM helpers ORDER BY name"
            ).fetchall()
            has_project = self._column_exists(conn, "helpers", "project")
            has_tags = self._column_exists(conn, "helpers", "tags")
            has_group = self._column_exists(conn, "helpers", "group_name")

        out = []
        for row in rows:
            d = self._helper_row_to_dict(dict(row))
            if has_project:
                d.setdefault("project", "")
            if has_tags:
                raw_tags = d.get("tags") or "[]"
                try:
                    d["tags"] = json.loads(raw_tags) if isinstance(raw_tags, str) else raw_tags
                except (json.JSONDecodeError, TypeError):
                    d["tags"] = []
            if has_group:
                d.setdefault("group_name", "")

            # Apply filters
            if project is not None and d.get("project", "") != project:
                continue
            if group_name is not None and d.get("group_name", "") != group_name:
                continue
            if tags is not None:
                helper_tags = d.get("tags", [])
                if not any(t in helper_tags for t in tags):
                    continue

            out.append(d)
        return out

    def helper_set_project(self, name: str, project: str) -> bool:
        """Update the project field for a helper. Returns True if updated."""
        with self._connect() as conn:
            if not self._column_exists(conn, "helpers", "project"):
                return False
            cursor = conn.execute(
                "UPDATE helpers SET project=? WHERE name=?", (project, name)
            )
            return cursor.rowcount > 0

    def delete_helpers_by_project(self, project: str) -> int:
        """Delete all helpers with the given project. Returns count deleted."""
        with self._connect() as conn:
            if not self._column_exists(conn, "helpers", "project"):
                return 0
            cursor = conn.execute(
                "DELETE FROM helpers WHERE project=?", (project,)
            )
            return cursor.rowcount

    @staticmethod
    def _helper_row_to_dict(row: dict) -> dict:
        row["requirements"] = json.loads(row.get("requirements_json") or "[]")
        row["input_schema"] = json.loads(row.get("input_schema_json") or "{}")
        row["output_schema"] = json.loads(row.get("output_schema_json") or "{}")
        return row

    def get_pipeline_yaml_content(self, name: str) -> Optional[str]:
        """Return the stored YAML content for a pipeline, or None if not stored."""
        with self._connect() as conn:
            if not self._column_exists(conn, "pipelines", "yaml_content"):
                return None
            row = conn.execute(
                "SELECT yaml_content FROM pipelines WHERE name=?", (name,)
            ).fetchone()
            if row and row[0]:
                return row[0]
            return None

    def get_helper_code(self, name: str) -> Optional[str]:
        """Return the stored code for a helper, or None if not stored."""
        with self._connect() as conn:
            if not self._column_exists(conn, "helpers", "code"):
                return None
            row = conn.execute(
                "SELECT code FROM helpers WHERE name=?", (name,)
            ).fetchone()
            if not row:
                # UUID fallback
                row = conn.execute(
                    "SELECT code FROM helpers WHERE id=?", (name,)
                ).fetchone()
            if row and row[0]:
                return row[0]
            return None

    def count_pipelines_with_content(self) -> int:
        """Count pipelines that have yaml_content stored."""
        with self._connect() as conn:
            if not self._column_exists(conn, "pipelines", "yaml_content"):
                return 0
            row = conn.execute(
                "SELECT COUNT(*) FROM pipelines WHERE yaml_content IS NOT NULL AND yaml_content != ''"
            ).fetchone()
            return row[0] if row else 0

    def count_helpers_with_code(self) -> int:
        """Count helpers that have code stored."""
        with self._connect() as conn:
            if not self._column_exists(conn, "helpers", "code"):
                return 0
            row = conn.execute(
                "SELECT COUNT(*) FROM helpers WHERE code IS NOT NULL AND code != ''"
            ).fetchone()
            return row[0] if row else 0

    # ------------------------------------------------------------------
    # Pipeline-Helper relationships
    # ------------------------------------------------------------------

    def get_pipeline_helpers(self, pipeline_name: str) -> list[dict]:
        """Return all helpers used by a named pipeline."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT h.* FROM helpers h
                   JOIN pipeline_helpers ph ON ph.helper_id = h.id
                   JOIN pipelines p ON p.id = ph.pipeline_id
                   WHERE p.name = ?
                   ORDER BY h.name""",
                (pipeline_name,),
            ).fetchall()
            return [self._helper_row_to_dict(dict(r)) for r in rows]

    # ------------------------------------------------------------------
    # Object Versions (prepared for T-BRIX-V5-07)
    # ------------------------------------------------------------------

    def record_object_version(
        self,
        obj_type: str,
        name: str,
        content: Any,
        version_id: Optional[str] = None,
    ) -> str:
        """Store an immutable snapshot of an object. Returns the version id."""
        vid = version_id or str(uuid4())
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO object_versions (id, type, name, version_id, content, created_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    str(uuid4()),
                    obj_type, name, vid,
                    json.dumps(content, default=str),
                    _now_iso(),
                ),
            )
        return vid

    def get_object_versions(self, obj_type: str, name: str) -> list[dict]:
        """Return all versions for an object, newest first."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM object_versions
                   WHERE type=? AND name=?
                   ORDER BY created_at DESC""",
                (obj_type, name),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_object_version(self, version_id: str) -> Optional[dict]:
        """Return a single version record by version_id, or None."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM object_versions WHERE version_id=?",
                (version_id,),
            ).fetchone()
            return dict(row) if row else None

    def trim_object_versions(
        self,
        obj_type: str,
        name: str,
        keep: int = 10,
    ) -> int:
        """Delete oldest versions beyond *keep* for the given object.

        Returns the number of rows deleted.
        """
        with self._connect() as conn:
            # Find the created_at threshold: keep the newest *keep* rows
            rows = conn.execute(
                """SELECT created_at FROM object_versions
                   WHERE type=? AND name=?
                   ORDER BY created_at DESC
                   LIMIT 1 OFFSET ?""",
                (obj_type, name, keep - 1),
            ).fetchone()
            if rows is None:
                # Fewer than *keep* versions exist — nothing to delete
                return 0
            threshold = rows[0]
            cursor = conn.execute(
                """DELETE FROM object_versions
                   WHERE type=? AND name=? AND created_at < ?""",
                (obj_type, name, threshold),
            )
            return cursor.rowcount

    def cleanup_all_versions(self, keep: int = 10) -> int:
        """Delete oldest versions across ALL objects, keeping *keep* per object.

        Returns the total number of rows deleted.
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            pairs = conn.execute(
                "SELECT DISTINCT type, name FROM object_versions"
            ).fetchall()

        total_deleted = 0
        for pair in pairs:
            total_deleted += self.trim_object_versions(
                pair["type"], pair["name"], keep=keep
            )
        return total_deleted

    # ------------------------------------------------------------------
    # Audit Log (T-BRIX-V6-01)
    # ------------------------------------------------------------------

    def write_audit_entry(
        self,
        tool: str,
        source: Optional[dict] = None,
        arguments_summary: Optional[str] = None,
    ) -> str:
        """Write one entry to audit_log.  Returns the entry id.

        Parameters
        ----------
        tool:
            The MCP tool name that was invoked (e.g. 'brix__create_pipeline').
        source:
            Optional dict with keys 'session', 'model', 'agent' identifying
            the caller.  Missing keys are stored as NULL.
        arguments_summary:
            Short human-readable summary of the relevant arguments
            (e.g. pipeline name, helper name).  Truncated to 500 chars.
        """
        src = source or {}
        entry_id = str(uuid4())
        summary = (arguments_summary or "")[:500]
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO audit_log
                   (id, timestamp, tool, source_session, source_model,
                    source_agent, arguments_summary)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    entry_id,
                    _now_iso(),
                    tool,
                    src.get("session"),
                    src.get("model"),
                    src.get("agent"),
                    summary or None,
                ),
            )
        return entry_id

    def get_audit_log(self, limit: int = 100) -> list[dict]:
        """Return the most recent *limit* audit entries, newest-first."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Agent Sessions (V6-10)
    # ------------------------------------------------------------------

    def save_agent_context(
        self,
        session_id: str,
        summary: str,
        active_pipeline: Optional[str] = None,
        last_run_id: Optional[str] = None,
        pending_decisions: Optional[list] = None,
    ) -> None:
        """Upsert an agent session context record."""
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO agent_sessions
                   (session_id, summary, active_pipeline, last_run_id,
                    pending_decisions_json, updated_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(session_id) DO UPDATE SET
                     summary=excluded.summary,
                     active_pipeline=excluded.active_pipeline,
                     last_run_id=excluded.last_run_id,
                     pending_decisions_json=excluded.pending_decisions_json,
                     updated_at=excluded.updated_at
                """,
                (
                    session_id,
                    summary or "",
                    active_pipeline,
                    last_run_id,
                    json.dumps(pending_decisions or []),
                    _now_iso(),
                ),
            )

    def restore_agent_context(self, session_id: str) -> Optional[dict]:
        """Return the stored agent context, or None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM agent_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            result["pending_decisions"] = json.loads(
                result.get("pending_decisions_json") or "[]"
            )
            return result

    def list_agent_sessions(self) -> list[dict]:
        """Return all agent sessions, newest-updated first."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM agent_sessions ORDER BY updated_at DESC"
            ).fetchall()
            out = []
            for row in rows:
                d = dict(row)
                d["pending_decisions"] = json.loads(d.get("pending_decisions_json") or "[]")
                out.append(d)
            return out

    def delete_agent_session(self, session_id: str) -> bool:
        """Delete an agent session. Returns True if it existed."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM agent_sessions WHERE session_id=?", (session_id,)
            )
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # Resource Locks (V6-11)
    # ------------------------------------------------------------------

    def claim_resource(
        self,
        resource_id: str,
        run_id: str,
        ttl_minutes: int = 30,
    ) -> dict:
        """Attempt to acquire a lock on *resource_id*.

        Returns ``{"claimed": True}`` on success or
        ``{"claimed": False, "held_by": run_id, "expires_at": iso}`` on conflict.
        Expired locks are automatically released before the claim attempt.
        """
        from datetime import timedelta

        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        expires_dt = now_dt + timedelta(minutes=ttl_minutes)
        expires = expires_dt.isoformat()

        with self._connect() as conn:
            # Clean up any expired lock for this resource first
            conn.execute(
                "DELETE FROM resource_locks WHERE resource_id=? AND expires_at < ?",
                (resource_id, now),
            )
            # Try to insert the new lock
            try:
                conn.execute(
                    """INSERT INTO resource_locks (resource_id, run_id, claimed_at, expires_at)
                       VALUES (?,?,?,?)""",
                    (resource_id, run_id, now, expires),
                )
                return {"claimed": True, "resource_id": resource_id, "run_id": run_id, "expires_at": expires}
            except sqlite3.IntegrityError:
                # Lock already held by someone else
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT * FROM resource_locks WHERE resource_id=?", (resource_id,)
                ).fetchone()
                if row:
                    return {
                        "claimed": False,
                        "resource_id": resource_id,
                        "held_by": row["run_id"],
                        "expires_at": row["expires_at"],
                    }
                # Race: expired between delete and insert — retry once
                conn.execute(
                    """INSERT OR IGNORE INTO resource_locks (resource_id, run_id, claimed_at, expires_at)
                       VALUES (?,?,?,?)""",
                    (resource_id, run_id, now, expires),
                )
                return {"claimed": True, "resource_id": resource_id, "run_id": run_id, "expires_at": expires}

    def check_resource(self, resource_id: str) -> dict:
        """Return lock status for *resource_id*.

        Expired locks are ignored (treated as free).
        Returns ``{"locked": bool, "run_id": str|None, "expires_at": str|None}``.
        """
        now = _now_iso()
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM resource_locks WHERE resource_id=? AND expires_at >= ?",
                (resource_id, now),
            ).fetchone()
            if row:
                return {
                    "locked": True,
                    "resource_id": resource_id,
                    "run_id": row["run_id"],
                    "claimed_at": row["claimed_at"],
                    "expires_at": row["expires_at"],
                }
            return {"locked": False, "resource_id": resource_id, "run_id": None, "expires_at": None}

    def release_resource(self, resource_id: str) -> bool:
        """Release a lock on *resource_id*. Returns True if a lock existed."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM resource_locks WHERE resource_id=?", (resource_id,)
            )
            return cursor.rowcount > 0

    def list_resource_locks(self) -> list[dict]:
        """Return all active (non-expired) resource locks."""
        now = _now_iso()
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM resource_locks WHERE expires_at >= ? ORDER BY claimed_at",
                (now,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Shared State / Blackboard (V6-12)
    # ------------------------------------------------------------------

    def state_set(self, key: str, value: Any) -> None:
        """Set a key in the shared blackboard (upsert)."""
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO shared_state (key, value_json, updated_at)
                   VALUES (?,?,?)
                   ON CONFLICT(key) DO UPDATE SET
                     value_json=excluded.value_json,
                     updated_at=excluded.updated_at
                """,
                (key, json.dumps(value, default=str), _now_iso()),
            )

    def state_get(self, key: str) -> Optional[Any]:
        """Get a value from the shared blackboard. Returns None if not found."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM shared_state WHERE key=?", (key,)
            ).fetchone()
            if row is None:
                return None
            return json.loads(row[0])

    def state_list(self, prefix: Optional[str] = None) -> list[dict]:
        """List all shared-state entries, optionally filtered by key prefix.

        Returns list of ``{"key": str, "value": Any, "updated_at": str}``.
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            if prefix:
                rows = conn.execute(
                    "SELECT * FROM shared_state WHERE key LIKE ? ORDER BY key",
                    (prefix + "%",),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM shared_state ORDER BY key"
                ).fetchall()
            out = []
            for row in rows:
                out.append({
                    "key": row["key"],
                    "value": json.loads(row["value_json"]),
                    "updated_at": row["updated_at"],
                })
            return out

    def state_delete(self, key: str) -> bool:
        """Delete a key from the shared blackboard. Returns True if it existed."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM shared_state WHERE key=?", (key,))
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # Application Log (T-BRIX-V7-08)
    # ------------------------------------------------------------------

    def write_app_log(
        self,
        level: str,
        component: str,
        message: str,
    ) -> str:
        """Insert one entry into app_log.  Returns the entry id.

        Parameters
        ----------
        level:
            Log level string: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.
        component:
            Source component name (e.g. 'engine', 'scheduler', 'mcp_server').
        message:
            Human-readable log message. Truncated to 2000 chars.
        """
        entry_id = str(uuid4())
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO app_log (id, timestamp, level, component, message)
                   VALUES (?,?,?,?,?)""",
                (entry_id, _now_iso(), level.upper(), component, message[:2000]),
            )
        return entry_id

    def get_app_log(
        self,
        level: Optional[str] = None,
        since: Optional[str] = None,
        component: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        """Query app_log entries.

        Parameters
        ----------
        level:
            Filter by exact level string (e.g. 'ERROR').  Case-insensitive.
        since:
            ISO-8601 timestamp — only entries at or after this time.
        component:
            Filter by component name (exact match).
        limit:
            Maximum rows returned (default 50).
        """
        clauses: list[str] = []
        params: list[Any] = []

        if level:
            clauses.append("level = ?")
            params.append(level.upper())
        if since:
            clauses.append("timestamp >= ?")
            params.append(since)
        if component:
            clauses.append("component = ?")
            params.append(component)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT * FROM app_log {where} ORDER BY timestamp DESC LIMIT ?",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Retention Policy (T-BRIX-V7-08)
    # ------------------------------------------------------------------

    def clean_retention(
        self,
        max_days: Optional[int] = None,
        max_mb: Optional[float] = None,
    ) -> dict:
        """Delete old runs + app_log entries to enforce retention limits.

        Two passes:
        1. Age-based: delete runs (+ their step outputs) older than *max_days*.
           Also purge app_log entries older than *max_days*.
        2. Size-based: if the DB file exceeds *max_mb* MB, delete the oldest
           finished runs in FIFO order until the file is within the limit.

        Reads defaults from env vars ``BRIX_RETENTION_DAYS`` and
        ``BRIX_RETENTION_MAX_MB`` when the parameters are ``None``.

        Returns a summary dict:
        ``{"runs_deleted_age": int, "runs_deleted_size": int,
           "app_log_deleted": int, "db_size_mb": float}``
        """
        if max_days is None:
            try:
                max_days = int(os.environ.get("BRIX_RETENTION_DAYS", _DEFAULT_RETENTION_DAYS))
            except (ValueError, TypeError):
                max_days = _DEFAULT_RETENTION_DAYS

        if max_mb is None:
            try:
                max_mb = float(os.environ.get("BRIX_RETENTION_MAX_MB", _DEFAULT_RETENTION_MAX_MB))
            except (ValueError, TypeError):
                max_mb = float(_DEFAULT_RETENTION_MAX_MB)

        runs_deleted_age = 0
        app_log_deleted = 0
        runs_deleted_size = 0

        # Pass 1: age-based deletion
        with self._connect() as conn:
            # Collect run_ids to be deleted so we can cascade to execution tables
            old_run_rows = conn.execute(
                "SELECT run_id FROM runs WHERE started_at < datetime('now', ?)",
                (f"-{max_days} days",),
            ).fetchall()
            old_run_ids = [r[0] for r in old_run_rows]

            # Delete execution data BEFORE deleting runs (T-BRIX-DB-07)
            if old_run_ids:
                ph = ",".join("?" * len(old_run_ids))
                conn.execute(f"DELETE FROM step_executions WHERE run_id IN ({ph})", old_run_ids)
                conn.execute(f"DELETE FROM foreach_item_executions WHERE run_id IN ({ph})", old_run_ids)
                conn.execute(f"DELETE FROM run_inputs WHERE run_id IN ({ph})", old_run_ids)

            cursor = conn.execute(
                "DELETE FROM runs WHERE started_at < datetime('now', ?)",
                (f"-{max_days} days",),
            )
            runs_deleted_age = cursor.rowcount

            cursor2 = conn.execute(
                "DELETE FROM app_log WHERE timestamp < datetime('now', ?)",
                (f"-{max_days} days",),
            )
            app_log_deleted = cursor2.rowcount

            # Cleanup orphaned deprecated_usage entries
            conn.execute(
                "DELETE FROM deprecated_usage WHERE pipeline_name NOT IN (SELECT name FROM pipelines)"
            )

        # Pass 2: size-based FIFO deletion
        db_size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
        db_size_mb = db_size_bytes / (1024 * 1024)

        if db_size_mb > max_mb:
            # Delete oldest finished runs in batches of 100 until size is OK
            while db_size_mb > max_mb:
                with self._connect() as conn:
                    # Find the 100 oldest finished runs
                    rows = conn.execute(
                        """SELECT run_id FROM runs
                           WHERE finished_at IS NOT NULL
                           ORDER BY started_at ASC
                           LIMIT 100"""
                    ).fetchall()
                    if not rows:
                        break
                    run_ids = [r[0] for r in rows]
                    placeholders = ",".join("?" * len(run_ids))
                    # Delete execution data BEFORE deleting runs (T-BRIX-DB-07)
                    conn.execute(f"DELETE FROM step_executions WHERE run_id IN ({placeholders})", run_ids)
                    conn.execute(f"DELETE FROM foreach_item_executions WHERE run_id IN ({placeholders})", run_ids)
                    conn.execute(f"DELETE FROM run_inputs WHERE run_id IN ({placeholders})", run_ids)
                    cursor = conn.execute(
                        f"DELETE FROM runs WHERE run_id IN ({placeholders})",
                        run_ids,
                    )
                    runs_deleted_size += cursor.rowcount
                    conn.execute("VACUUM")

                # Re-check size
                db_size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
                db_size_mb = db_size_bytes / (1024 * 1024)

                if cursor.rowcount == 0:
                    break  # Nothing left to delete

        # Final size after cleanup
        db_size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
        db_size_mb = db_size_bytes / (1024 * 1024)

        return {
            "runs_deleted_age": runs_deleted_age,
            "runs_deleted_size": runs_deleted_size,
            "app_log_deleted": app_log_deleted,
            "db_size_mb": round(db_size_mb, 3),
        }

    # ------------------------------------------------------------------
    # Registry System (T-BRIX-V7-10)
    # ------------------------------------------------------------------

    def _registry_table(self, registry_type: str) -> str:
        """Return the table name for a registry type, or raise ValueError."""
        table = REGISTRY_TYPES.get(registry_type)
        if not table:
            valid = ", ".join(sorted(REGISTRY_TYPES.keys()))
            raise ValueError(
                f"Unknown registry_type '{registry_type}'. Valid types: {valid}"
            )
        return table

    def registry_add(
        self,
        registry_type: str,
        name: str,
        content: Any,
        tags: Optional[list] = None,
        description: str = "",
        entry_id: Optional[str] = None,
        project: Optional[str] = None,
        group_name: Optional[str] = None,
    ) -> str:
        """Add a new entry to a registry. Returns the entry id."""
        table = self._registry_table(registry_type)
        now = _now_iso()
        eid = entry_id or str(uuid4())
        with self._connect() as conn:
            cols = ["id", "name", "description", "content", "tags", "created_at", "updated_at"]
            vals: list = [
                eid,
                name,
                description,
                json.dumps(content) if not isinstance(content, str) else content,
                json.dumps(tags or []),
                now,
                now,
            ]
            if project is not None and self._column_exists(conn, table, "project"):
                cols.append("project")
                vals.append(project)
            if group_name is not None and self._column_exists(conn, table, "group_name"):
                cols.append("group_name")
                vals.append(group_name)
            placeholders = ",".join("?" * len(cols))
            conn.execute(
                f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})",
                vals,
            )
        return eid

    def registry_get(self, registry_type: str, name_or_id: str) -> Optional[dict]:
        """Get a registry entry by name or id. Returns None if not found."""
        table = self._registry_table(registry_type)
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                f"SELECT * FROM {table} WHERE name=?", (name_or_id,)
            ).fetchone()
            if not row:
                row = conn.execute(
                    f"SELECT * FROM {table} WHERE id=?", (name_or_id,)
                ).fetchone()
        if row is None:
            return None
        return self._registry_row_to_dict(dict(row))

    def registry_list(
        self,
        registry_type: str,
        tag_filter: Optional[str] = None,
    ) -> list[dict]:
        """List all entries in a registry, optionally filtered by tag."""
        table = self._registry_table(registry_type)
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT * FROM {table} ORDER BY name"
            ).fetchall()
        results = [self._registry_row_to_dict(dict(r)) for r in rows]
        if tag_filter:
            results = [
                r for r in results
                if tag_filter in r.get("tags", [])
            ]
        return results

    def registry_update(
        self,
        registry_type: str,
        name_or_id: str,
        content: Any = None,
        tags: Optional[list] = None,
        description: Optional[str] = None,
        project: Optional[str] = None,
        group_name: Optional[str] = None,
    ) -> Optional[dict]:
        """Update an existing registry entry. Returns updated entry or None if not found."""
        table = self._registry_table(registry_type)
        entry = self.registry_get(registry_type, name_or_id)
        if entry is None:
            return None
        now = _now_iso()
        new_content = json.dumps(content) if content is not None and not isinstance(content, str) else (content if content is not None else json.dumps(entry["content"]))
        new_tags = json.dumps(tags) if tags is not None else json.dumps(entry["tags"])
        new_description = description if description is not None else entry["description"]
        with self._connect() as conn:
            set_parts = ["content=?", "tags=?", "description=?", "updated_at=?"]
            vals: list = [new_content, new_tags, new_description, now]
            if project is not None and self._column_exists(conn, table, "project"):
                set_parts.append("project=?")
                vals.append(project)
            if group_name is not None and self._column_exists(conn, table, "group_name"):
                set_parts.append("group_name=?")
                vals.append(group_name)
            vals.append(entry["id"])
            conn.execute(
                f"UPDATE {table} SET {', '.join(set_parts)} WHERE id=?",
                vals,
            )
        return self.registry_get(registry_type, entry["id"])

    def registry_delete(self, registry_type: str, name_or_id: str) -> bool:
        """Delete a registry entry by name or id. Returns True if deleted."""
        table = self._registry_table(registry_type)
        entry = self.registry_get(registry_type, name_or_id)
        if entry is None:
            return False
        with self._connect() as conn:
            cursor = conn.execute(
                f"DELETE FROM {table} WHERE id=?", (entry["id"],)
            )
            return cursor.rowcount > 0

    def registry_search(
        self,
        query: str,
        registry_types: Optional[list[str]] = None,
    ) -> list[dict]:
        """Full-text search across registry entries (name, description, content, tags).

        Searches all registry types by default, or a subset if *registry_types* is given.
        Returns entries sorted by registry_type then name, each with a 'registry_type' field.
        """
        types_to_search = registry_types if registry_types else list(REGISTRY_TYPES.keys())
        results: list[dict] = []
        q_lower = query.lower()
        for rtype in types_to_search:
            try:
                table = self._registry_table(rtype)
            except ValueError:
                continue
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    f"SELECT * FROM {table} ORDER BY name"
                ).fetchall()
            for row in rows:
                entry = self._registry_row_to_dict(dict(row))
                # Search in name, description, tags, and serialized content
                haystack = " ".join([
                    entry.get("name", ""),
                    entry.get("description", ""),
                    " ".join(entry.get("tags", [])),
                    json.dumps(entry.get("content", "")),
                ]).lower()
                if q_lower in haystack:
                    entry["registry_type"] = rtype
                    results.append(entry)
        return results

    @staticmethod
    def _registry_row_to_dict(row: dict) -> dict:
        """Deserialize JSON columns in a registry row."""
        for col in ("content", "tags"):
            raw = row.get(col)
            if isinstance(raw, str):
                try:
                    row[col] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    pass  # leave as-is if not valid JSON
        # T-BRIX-ORG-01: ensure org fields are present
        row.setdefault("project", "")
        row.setdefault("group_name", "")
        return row

    # ------------------------------------------------------------------
    # Alert Rules (T-BRIX-MOD-02)
    # ------------------------------------------------------------------

    def alert_rule_add(
        self,
        name: str,
        condition: str,
        channel: str,
        config: Optional[dict] = None,
        rule_id: Optional[str] = None,
        created_at: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> dict:
        """Insert a new alert rule. Returns the row as dict."""
        rid = rule_id or str(uuid4())
        now = created_at or _now_iso()
        cfg_json = json.dumps(config or {})
        with self._connect() as conn:
            cols = ["id", "name", "condition", "channel", "config", "enabled", "created_at"]
            vals: list = [rid, name, condition, channel, cfg_json, 1, now]
            if project is not None and self._column_exists(conn, "alert_rules", "project"):
                cols.append("project")
                vals.append(project)
            if tags is not None and self._column_exists(conn, "alert_rules", "tags"):
                cols.append("tags")
                vals.append(json.dumps(tags))
            if group_name is not None and self._column_exists(conn, "alert_rules", "group_name"):
                cols.append("group_name")
                vals.append(group_name)
            placeholders = ",".join("?" * len(cols))
            conn.execute(
                f"INSERT INTO alert_rules ({','.join(cols)}) VALUES ({placeholders})",
                vals,
            )
        return self.alert_rule_get(rid)  # type: ignore[return-value]

    def alert_rule_get(self, rule_id: str) -> Optional[dict]:
        """Return an alert rule by ID, or None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM alert_rules WHERE id=?", (rule_id,)
            ).fetchone()
        if not row:
            return None
        return self._alert_rule_row_to_dict(dict(row))

    def alert_rule_list(self) -> list[dict]:
        """Return all alert rules ordered by created_at."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM alert_rules ORDER BY created_at"
            ).fetchall()
        return [self._alert_rule_row_to_dict(dict(r)) for r in rows]

    def alert_rule_update(
        self,
        rule_id: str,
        name: Optional[str] = None,
        condition: Optional[str] = None,
        channel: Optional[str] = None,
        config: Optional[dict] = None,
        enabled: Optional[bool] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> Optional[dict]:
        """Update fields of an existing alert rule. Returns updated dict or None."""
        existing = self.alert_rule_get(rule_id)
        if existing is None:
            return None
        updates: dict[str, Any] = {}
        if name is not None:
            updates["name"] = name
        if condition is not None:
            updates["condition"] = condition
        if channel is not None:
            updates["channel"] = channel
        if config is not None:
            updates["config"] = json.dumps(config)
        if enabled is not None:
            updates["enabled"] = int(enabled)
        if project is not None:
            updates["project"] = project
        if tags is not None:
            updates["tags"] = json.dumps(tags)
        if group_name is not None:
            updates["group_name"] = group_name
        if not updates:
            return existing
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [rule_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE alert_rules SET {set_clause} WHERE id=?", values)
        return self.alert_rule_get(rule_id)

    def alert_rule_delete(self, rule_id: str) -> bool:
        """Delete an alert rule by ID. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM alert_rules WHERE id=?", (rule_id,)
            )
            return cursor.rowcount > 0

    def alert_history_add(
        self,
        rule_id: str,
        rule_name: str,
        condition: str,
        channel: str,
        message: str,
        pipeline: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        """Persist an alert firing to history."""
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO alert_history
                   (id, rule_id, rule_name, condition, channel, pipeline, run_id, message, fired_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    str(uuid4()),
                    rule_id,
                    rule_name,
                    condition,
                    channel,
                    pipeline,
                    run_id,
                    message,
                    _now_iso(),
                ),
            )

    def alert_history_list(self, limit: int = 20) -> list[dict]:
        """Return the most recent alert history entries, newest first."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM alert_history ORDER BY fired_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _alert_rule_row_to_dict(row: dict) -> dict:
        row["config"] = json.loads(row.get("config") or "{}")
        row["enabled"] = bool(row.get("enabled", 1))
        # T-BRIX-ORG-01: deserialize org fields
        raw_tags = row.get("tags")
        if isinstance(raw_tags, str):
            try:
                row["tags"] = json.loads(raw_tags)
            except (json.JSONDecodeError, TypeError):
                row["tags"] = []
        elif raw_tags is None:
            row["tags"] = []
        row.setdefault("project", "")
        row.setdefault("group_name", "")
        return row

    # ------------------------------------------------------------------
    # Triggers (T-BRIX-MOD-02)
    # ------------------------------------------------------------------

    def trigger_add(
        self,
        name: str,
        type: str,
        config: dict,
        pipeline: str,
        enabled: bool = True,
        trigger_id: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> dict:
        """Insert a new trigger. Returns the row as dict."""
        tid = trigger_id or str(uuid4())
        now = _now_iso()
        with self._connect() as conn:
            has_project = self._column_exists(conn, "triggers", "project")
            has_tags = self._column_exists(conn, "triggers", "tags")
            has_group = self._column_exists(conn, "triggers", "group_name")

            cols = ["id", "name", "type", "config_json", "pipeline", "enabled", "created_at", "updated_at"]
            vals: list = [tid, name, type, json.dumps(config), pipeline, int(enabled), now, now]

            if has_project and project is not None:
                cols.append("project")
                vals.append(project)
            if has_tags and tags is not None:
                cols.append("tags")
                vals.append(json.dumps(tags))
            if has_group and group_name is not None:
                cols.append("group_name")
                vals.append(group_name)

            placeholders = ",".join("?" * len(cols))
            try:
                conn.execute(
                    f"INSERT INTO triggers ({','.join(cols)}) VALUES ({placeholders})",
                    vals,
                )
            except sqlite3.IntegrityError:
                raise ValueError(f"Trigger with name '{name}' already exists.")
        return self.trigger_get(name)  # type: ignore[return-value]

    def trigger_get(self, name: str) -> Optional[dict]:
        """Get a trigger by name or UUID. Returns None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM triggers WHERE name=?", (name,)
            ).fetchone()
            if not row:
                row = conn.execute(
                    "SELECT * FROM triggers WHERE id=?", (name,)
                ).fetchone()
            return self._trigger_row_to_dict(dict(row)) if row else None

    def trigger_list(self) -> list[dict]:
        """Return all triggers sorted by name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM triggers ORDER BY name"
            ).fetchall()
        return [self._trigger_row_to_dict(dict(r)) for r in rows]

    def trigger_update(
        self,
        name: str,
        config: Optional[dict] = None,
        enabled: Optional[bool] = None,
        pipeline: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Optional[dict]:
        """Partially update a trigger. Returns updated dict or None if not found."""
        existing = self.trigger_get(name)
        if existing is None:
            return None
        updates: dict[str, Any] = {"updated_at": _now_iso()}
        if config is not None:
            updates["config_json"] = json.dumps(config)
        if enabled is not None:
            updates["enabled"] = int(enabled)
        if pipeline is not None:
            updates["pipeline"] = pipeline
        if description is not None:
            updates["description"] = description

        with self._connect() as conn:
            if project is not None and self._column_exists(conn, "triggers", "project"):
                updates["project"] = project
            if tags is not None and self._column_exists(conn, "triggers", "tags"):
                updates["tags"] = json.dumps(tags)
            if group_name is not None and self._column_exists(conn, "triggers", "group_name"):
                updates["group_name"] = group_name

            set_clause = ", ".join(f"{k}=?" for k in updates)
            values = list(updates.values()) + [existing["id"]]
            conn.execute(
                f"UPDATE triggers SET {set_clause} WHERE id=?", values
            )
        return self.trigger_get(existing["id"])

    def trigger_delete(self, name: str) -> bool:
        """Delete a trigger by name or UUID. Returns True if deleted."""
        existing = self.trigger_get(name)
        if existing is None:
            return False
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM triggers WHERE id=?", (existing["id"],)
            )
            return cursor.rowcount > 0

    def trigger_record_fired(
        self,
        name: str,
        run_id: Optional[str] = None,
        status: str = "fired",
    ) -> None:
        """Update last_fired_at, last_run_id, last_status after a trigger fires."""
        existing = self.trigger_get(name)
        if existing is None:
            return
        with self._connect() as conn:
            conn.execute(
                """UPDATE triggers
                   SET last_fired_at=?, last_run_id=?, last_status=?, updated_at=?
                   WHERE id=?""",
                (_now_iso(), run_id, status, _now_iso(), existing["id"]),
            )

    @staticmethod
    def _trigger_row_to_dict(row: dict) -> dict:
        row["config"] = json.loads(row.pop("config_json", "{}") or "{}")
        row["enabled"] = bool(row["enabled"])
        # T-BRIX-ORG-01: ensure org fields
        row.setdefault("project", "")
        raw_tags = row.get("tags", "[]")
        if isinstance(raw_tags, str):
            try:
                row["tags"] = json.loads(raw_tags)
            except (json.JSONDecodeError, TypeError):
                row["tags"] = []
        row.setdefault("group_name", "")
        return row

    # ------------------------------------------------------------------
    # Trigger Groups (T-BRIX-MOD-02)
    # ------------------------------------------------------------------

    def trigger_group_add(
        self,
        name: str,
        triggers: list[str],
        description: str = "",
        enabled: bool = True,
        group_id: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> dict:
        """Insert a new trigger group. Returns the row as dict."""
        gid = group_id or str(uuid4())
        now = _now_iso()
        with self._connect() as conn:
            cols = ["id", "name", "description", "triggers_json", "enabled", "created_at", "updated_at"]
            vals: list = [gid, name, description, json.dumps(triggers), int(enabled), now, now]
            if project is not None and self._column_exists(conn, "trigger_groups", "project"):
                cols.append("project")
                vals.append(project)
            if tags is not None and self._column_exists(conn, "trigger_groups", "tags"):
                cols.append("tags")
                vals.append(json.dumps(tags))
            if group_name is not None and self._column_exists(conn, "trigger_groups", "group_name"):
                cols.append("group_name")
                vals.append(group_name)
            placeholders = ",".join("?" * len(cols))
            try:
                conn.execute(
                    f"INSERT INTO trigger_groups ({','.join(cols)}) VALUES ({placeholders})",
                    vals,
                )
            except sqlite3.IntegrityError:
                raise ValueError(f"Trigger group with name '{name}' already exists.")
        return self.trigger_group_get(name)  # type: ignore[return-value]

    def trigger_group_get(self, name: str) -> Optional[dict]:
        """Get a trigger group by name or UUID. Returns None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM trigger_groups WHERE name=?", (name,)
            ).fetchone()
            if not row:
                row = conn.execute(
                    "SELECT * FROM trigger_groups WHERE id=?", (name,)
                ).fetchone()
            return self._trigger_group_row_to_dict(dict(row)) if row else None

    def trigger_group_list(self) -> list[dict]:
        """Return all trigger groups sorted by name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM trigger_groups ORDER BY name"
            ).fetchall()
        return [self._trigger_group_row_to_dict(dict(r)) for r in rows]

    def trigger_group_update(
        self,
        name: str,
        triggers: Optional[list[str]] = None,
        description: Optional[str] = None,
        enabled: Optional[bool] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> Optional[dict]:
        """Partially update a trigger group. Returns updated dict or None if not found."""
        existing = self.trigger_group_get(name)
        if existing is None:
            return None
        updates: dict[str, Any] = {"updated_at": _now_iso()}
        if triggers is not None:
            updates["triggers_json"] = json.dumps(triggers)
        if description is not None:
            updates["description"] = description
        if enabled is not None:
            updates["enabled"] = int(enabled)
        if project is not None:
            updates["project"] = project
        if tags is not None:
            updates["tags"] = json.dumps(tags)
        if group_name is not None:
            updates["group_name"] = group_name
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [existing["id"]]
        with self._connect() as conn:
            conn.execute(
                f"UPDATE trigger_groups SET {set_clause} WHERE id=?", values
            )
        return self.trigger_group_get(existing["id"])

    def trigger_group_delete(self, name: str) -> bool:
        """Delete a trigger group by name or UUID. Returns True if deleted."""
        existing = self.trigger_group_get(name)
        if existing is None:
            return False
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM trigger_groups WHERE id=?", (existing["id"],)
            )
            return cursor.rowcount > 0

    @staticmethod
    def _trigger_group_row_to_dict(row: dict) -> dict:
        row["triggers"] = json.loads(row.pop("triggers_json", "[]") or "[]")
        row["enabled"] = bool(row["enabled"])
        # T-BRIX-ORG-01: deserialize org fields
        raw_tags = row.get("tags")
        if isinstance(raw_tags, str):
            try:
                row["tags"] = json.loads(raw_tags)
            except (json.JSONDecodeError, TypeError):
                row["tags"] = []
        elif raw_tags is None:
            row["tags"] = []
        row.setdefault("project", "")
        row.setdefault("group_name", "")
        return row

    # ------------------------------------------------------------------
    # Trigger State (T-BRIX-MOD-03 — migrated from triggers/state.py)
    # ------------------------------------------------------------------

    def trigger_state_is_deduped(self, trigger_id: str, dedupe_key: str) -> bool:
        """Return True if this (trigger_id, dedupe_key) pair has already been recorded."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM trigger_state WHERE trigger_id=? AND dedupe_key=?",
                (trigger_id, dedupe_key),
            ).fetchone()
            return row is not None

    def trigger_state_record_fired(
        self,
        trigger_id: str,
        dedupe_key: str,
        run_id: Optional[str] = None,
    ) -> None:
        """Record that a trigger fired for a given dedupe_key."""
        import time as _time
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO trigger_state "
                "(trigger_id, dedupe_key, run_id, fired_at, status) VALUES (?, ?, ?, ?, 'fired')",
                (trigger_id, dedupe_key, run_id, _time.time()),
            )

    def pipeline_event_record(
        self,
        pipeline_name: str,
        run_id: str,
        status: str,
        result: Any = None,
        input: Any = None,
    ) -> None:
        """Record a pipeline completion event (pipeline_done)."""
        import time as _time
        result_json = json.dumps(result, default=str) if result is not None else None
        input_json = json.dumps(input, default=str) if input is not None else None
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO pipeline_events "
                "(run_id, pipeline_name, status, result_json, input_json, fired_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, pipeline_name, status, result_json, input_json, _time.time()),
            )

    def pipeline_event_record_raw(
        self,
        run_id: str,
        pipeline_name: str,
        status: str,
        result_json: Optional[str] = None,
        input_json: Optional[str] = None,
    ) -> None:
        """Record a pipeline event with pre-serialised JSON strings."""
        import time as _time
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO pipeline_events "
                "(run_id, pipeline_name, status, result_json, input_json, fired_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, pipeline_name, status, result_json, input_json, _time.time()),
            )

    def pipeline_event_get_unprocessed(
        self,
        pipeline_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        """Return all unprocessed pipeline events, optionally filtered."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            query = "SELECT * FROM pipeline_events WHERE processed=0"
            params: list[Any] = []
            if pipeline_name:
                query += " AND pipeline_name=?"
                params.append(pipeline_name)
            if status and status != "any":
                query += " AND status=?"
                params.append(status)
            return [dict(r) for r in conn.execute(query, params).fetchall()]

    def pipeline_event_mark_processed(self, event_id: int) -> None:
        """Mark a pipeline event as processed."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE pipeline_events SET processed=1 WHERE id=?", (event_id,)
            )

    def trigger_meta_get_last_check(self, trigger_id: str) -> Optional[float]:
        """Return the Unix timestamp of the last poll for this trigger, or None."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_check FROM trigger_meta WHERE trigger_id=?",
                (trigger_id,),
            ).fetchone()
            return row[0] if row else None

    def trigger_meta_set_last_check(self, trigger_id: str, ts: float) -> None:
        """Persist the last poll timestamp for this trigger."""
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO trigger_meta (trigger_id, last_check) VALUES (?, ?)",
                (trigger_id, ts),
            )

    def migrate_from_triggers_db(
        self, triggers_db_path: Optional[Path] = None
    ) -> dict[str, int]:
        """Copy data from legacy triggers.db into brix.db.

        Imports trigger_state, pipeline_events, and trigger_meta rows.
        Idempotent: skips rows that already exist.
        Returns a dict with counts of imported rows per table.
        """
        from pathlib import Path as _Path
        src = _Path(triggers_db_path) if triggers_db_path else (Path.home() / ".brix" / "triggers.db")
        if not src.exists():
            return {"trigger_state": 0, "pipeline_events": 0, "trigger_meta": 0}

        try:
            src_conn = sqlite3.connect(str(src))
            src_conn.row_factory = sqlite3.Row
        except Exception:
            return {"trigger_state": 0, "pipeline_events": 0, "trigger_meta": 0}

        counts: dict[str, int] = {"trigger_state": 0, "pipeline_events": 0, "trigger_meta": 0}
        try:
            # trigger_state
            try:
                rows = src_conn.execute("SELECT * FROM trigger_state").fetchall()
                with self._connect() as conn:
                    for row in rows:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO trigger_state "
                                "(trigger_id, dedupe_key, run_id, fired_at, status) "
                                "VALUES (?,?,?,?,?)",
                                (
                                    row["trigger_id"], row["dedupe_key"],
                                    row["run_id"], row["fired_at"],
                                    row["status"] if "status" in row.keys() else "fired",
                                ),
                            )
                            if conn.execute("SELECT changes()").fetchone()[0]:
                                counts["trigger_state"] += 1
                        except Exception:
                            continue
            except Exception:
                pass

            # pipeline_events
            try:
                rows = src_conn.execute("SELECT * FROM pipeline_events").fetchall()
                with self._connect() as conn:
                    for row in rows:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO pipeline_events "
                                "(id, run_id, pipeline_name, status, result_json, input_json, fired_at, processed) "
                                "VALUES (?,?,?,?,?,?,?,?)",
                                (
                                    row["id"], row["run_id"], row["pipeline_name"],
                                    row["status"],
                                    row["result_json"] if "result_json" in row.keys() else None,
                                    row["input_json"] if "input_json" in row.keys() else None,
                                    row["fired_at"],
                                    row["processed"] if "processed" in row.keys() else 0,
                                ),
                            )
                            if conn.execute("SELECT changes()").fetchone()[0]:
                                counts["pipeline_events"] += 1
                        except Exception:
                            continue
            except Exception:
                pass

            # trigger_meta
            try:
                rows = src_conn.execute("SELECT * FROM trigger_meta").fetchall()
                with self._connect() as conn:
                    for row in rows:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO trigger_meta (trigger_id, last_check) VALUES (?,?)",
                                (row["trigger_id"], row["last_check"]),
                            )
                            if conn.execute("SELECT changes()").fetchone()[0]:
                                counts["trigger_meta"] += 1
                        except Exception:
                            continue
            except Exception:
                pass
        finally:
            src_conn.close()

        return counts

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — brick_definitions
    # ------------------------------------------------------------------

    def brick_definitions_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM brick_definitions").fetchone()
        return row[0] if row else 0

    @staticmethod
    def _brick_row_enrich_org(d: dict) -> dict:
        """Enrich a brick_definitions row dict with parsed org fields."""
        # org_tags
        raw_tags = d.get("org_tags", "[]")
        if isinstance(raw_tags, str):
            try:
                d["org_tags"] = json.loads(raw_tags)
            except (json.JSONDecodeError, TypeError):
                d["org_tags"] = []
        d.setdefault("org_tags", [])
        # project & group_name
        d.setdefault("project", "")
        d.setdefault("group_name", "")
        return d

    def brick_definitions_list(self) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM brick_definitions ORDER BY name").fetchall()
        return [self._brick_row_enrich_org(dict(r)) for r in rows]

    def brick_definitions_get(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM brick_definitions WHERE name = ?", (name,)
            ).fetchone()
        if row is None:
            return None
        return self._brick_row_enrich_org(dict(row))

    def brick_definitions_upsert(self, record: dict) -> None:
        now = _now_iso()
        with self._connect() as conn:
            has_org_tags = self._column_exists(conn, "brick_definitions", "org_tags")

            cols = [
                "name", "runner", "namespace", "category", "description", "when_to_use",
                "when_NOT_to_use", "aliases", "input_type", "output_type", "config_schema",
                "examples", "related_connector", "system", "created_at", "updated_at",
            ]
            vals: list = [
                record["name"],
                record.get("runner", ""),
                record.get("namespace", ""),
                record.get("category", ""),
                record.get("description", ""),
                record.get("when_to_use", ""),
                record.get("when_NOT_to_use", ""),
                json.dumps(record.get("aliases", [])),
                record.get("input_type", "*"),
                record.get("output_type", "*"),
                json.dumps(record.get("config_schema", {})),
                json.dumps(record.get("examples", [])),
                record.get("related_connector", ""),
                int(bool(record.get("system", False))),
                now,
                now,
            ]
            updates = [
                "runner=excluded.runner",
                "namespace=excluded.namespace",
                "category=excluded.category",
                "description=excluded.description",
                "when_to_use=excluded.when_to_use",
                "when_NOT_to_use=excluded.when_NOT_to_use",
                "aliases=excluded.aliases",
                "input_type=excluded.input_type",
                "output_type=excluded.output_type",
                "config_schema=excluded.config_schema",
                "examples=excluded.examples",
                "related_connector=excluded.related_connector",
                "system=excluded.system",
                "updated_at=excluded.updated_at",
            ]

            if has_org_tags and record.get("org_tags") is not None:
                cols.append("org_tags")
                vals.append(json.dumps(record["org_tags"]))
                updates.append("org_tags=excluded.org_tags")

            has_project = self._column_exists(conn, "brick_definitions", "project")
            has_group = self._column_exists(conn, "brick_definitions", "group_name")
            if has_project and record.get("project") is not None:
                cols.append("project")
                vals.append(record["project"])
                updates.append("project=excluded.project")
            if has_group and record.get("group_name") is not None:
                cols.append("group_name")
                vals.append(record["group_name"])
                updates.append("group_name=excluded.group_name")

            placeholders = ",".join("?" * len(cols))
            update_str = ",".join(updates)
            conn.execute(
                f"""INSERT INTO brick_definitions ({','.join(cols)})
                   VALUES ({placeholders})
                   ON CONFLICT(name) DO UPDATE SET {update_str}""",
                vals,
            )

    def brick_definitions_delete(self, name: str) -> bool:
        """Delete a brick_definition by name. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM brick_definitions WHERE name=?", (name,))
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — connector_definitions
    # ------------------------------------------------------------------

    def connector_definitions_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM connector_definitions").fetchone()
        return row[0] if row else 0

    def connector_definitions_list(self) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM connector_definitions ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def connector_definitions_get(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM connector_definitions WHERE name = ?", (name,)
            ).fetchone()
        return dict(row) if row else None

    def connector_definitions_upsert(self, record: dict) -> None:
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO connector_definitions
                   (name, type, description, required_mcp_server, required_mcp_tools,
                    output_schema, parameters, related_pipelines, related_helpers,
                    created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(name) DO UPDATE SET
                       type=excluded.type,
                       description=excluded.description,
                       required_mcp_server=excluded.required_mcp_server,
                       required_mcp_tools=excluded.required_mcp_tools,
                       output_schema=excluded.output_schema,
                       parameters=excluded.parameters,
                       related_pipelines=excluded.related_pipelines,
                       related_helpers=excluded.related_helpers,
                       updated_at=excluded.updated_at""",
                (
                    record["name"],
                    record.get("type", ""),
                    record.get("description", ""),
                    record.get("required_mcp_server") or "",
                    json.dumps(record.get("required_mcp_tools", [])),
                    json.dumps(record.get("output_schema", {})),
                    json.dumps(record.get("parameters", [])),
                    json.dumps(record.get("related_pipelines", [])),
                    json.dumps(record.get("related_helpers", [])),
                    now,
                    now,
                ),
            )

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — mcp_tool_schemas
    # ------------------------------------------------------------------

    def mcp_tool_schemas_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM mcp_tool_schemas").fetchone()
        return row[0] if row else 0

    def mcp_tool_schemas_list(self) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM mcp_tool_schemas ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def mcp_tool_schemas_get(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM mcp_tool_schemas WHERE name = ?", (name,)
            ).fetchone()
        return dict(row) if row else None

    def mcp_tool_schemas_upsert(self, record: dict) -> None:
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO mcp_tool_schemas (name, description, input_schema, created_at, updated_at)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(name) DO UPDATE SET
                       description=excluded.description,
                       input_schema=excluded.input_schema,
                       updated_at=excluded.updated_at""",
                (
                    record["name"],
                    record.get("description", ""),
                    json.dumps(record.get("input_schema", {})),
                    now,
                    now,
                ),
            )

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — help_topics
    # ------------------------------------------------------------------

    def help_topics_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM help_topics").fetchone()
        return row[0] if row else 0

    def help_topics_list(self) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM help_topics ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def help_topics_get(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM help_topics WHERE name = ?", (name,)
            ).fetchone()
        return dict(row) if row else None

    def help_topics_upsert(self, record: dict) -> None:
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO help_topics (name, title, content, created_at, updated_at)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(name) DO UPDATE SET
                       title=excluded.title,
                       content=excluded.content,
                       updated_at=excluded.updated_at""",
                (
                    record["name"],
                    record.get("title", record["name"]),
                    record.get("content", ""),
                    now,
                    now,
                ),
            )

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — keyword_taxonomies
    # ------------------------------------------------------------------

    def keyword_taxonomies_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM keyword_taxonomies").fetchone()
        return row[0] if row else 0

    def keyword_taxonomies_list(self, category: Optional[str] = None) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            if category:
                rows = conn.execute(
                    "SELECT * FROM keyword_taxonomies WHERE category = ? ORDER BY keyword",
                    (category,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM keyword_taxonomies ORDER BY category, keyword"
                ).fetchall()
        return [dict(r) for r in rows]

    def keyword_taxonomies_upsert(self, category: str, keyword: str, language: str = "de", mapped_to: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO keyword_taxonomies (category, keyword, language, mapped_to)
                   VALUES (?,?,?,?)
                   ON CONFLICT(category, keyword) DO UPDATE SET
                       language=excluded.language,
                       mapped_to=excluded.mapped_to""",
                (category, keyword, language, mapped_to),
            )

    def keyword_taxonomies_as_dict(self) -> dict[str, dict[str, list[str]]]:
        """Return keyword taxonomies as nested dict: {category: {mapped_to: [keywords]}}."""
        rows = self.keyword_taxonomies_list()
        result: dict[str, dict[str, list[str]]] = {}
        for row in rows:
            cat = row["category"]
            mapped = row["mapped_to"]
            kw = row["keyword"]
            if cat not in result:
                result[cat] = {}
            if mapped not in result[cat]:
                result[cat][mapped] = []
            result[cat][mapped].append(kw)
        return result

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — type_compatibility
    # ------------------------------------------------------------------

    def type_compatibility_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM type_compatibility").fetchone()
        return row[0] if row else 0

    def type_compatibility_list(self) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM type_compatibility ORDER BY output_type, compatible_input"
            ).fetchall()
        return [dict(r) for r in rows]

    def type_compatibility_upsert(self, output_type: str, compatible_input: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO type_compatibility (output_type, compatible_input) VALUES (?,?)",
                (output_type, compatible_input),
            )

    def type_compatibility_as_dict(self) -> dict[str, list[str]]:
        """Return type_compatibility as {output_type: [compatible_inputs]}."""
        rows = self.type_compatibility_list()
        result: dict[str, list[str]] = {}
        for row in rows:
            out_type = row["output_type"]
            if out_type not in result:
                result[out_type] = []
            result[out_type].append(row["compatible_input"])
        return result

    # ------------------------------------------------------------------
    # Managed Variables (T-BRIX-DB-13)
    # ------------------------------------------------------------------

    def variable_set(
        self,
        name: str,
        value: str,
        description: str = "",
        secret: bool = False,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> None:
        """Create or update a managed variable (upsert).

        When secret=True the value is Fernet-encrypted before storage.
        """
        from brix.credential_store import _encrypt
        stored_value = _encrypt(value) if secret else value
        now = _now_iso()
        with self._connect() as conn:
            has_project = self._column_exists(conn, "variables", "project")
            has_tags = self._column_exists(conn, "variables", "tags")
            has_group = self._column_exists(conn, "variables", "group_name")

            existing = conn.execute(
                "SELECT created_at FROM variables WHERE name=?", (name,)
            ).fetchone()
            if existing:
                sets = ["value=?", "description=?", "updated_at=?", "secret=?"]
                vals: list = [stored_value, description, now, 1 if secret else 0]
                if has_project and project is not None:
                    sets.append("project=?")
                    vals.append(project)
                if has_tags and tags is not None:
                    sets.append("tags=?")
                    vals.append(json.dumps(tags))
                if has_group and group_name is not None:
                    sets.append("group_name=?")
                    vals.append(group_name)
                vals.append(name)
                conn.execute(
                    f"UPDATE variables SET {', '.join(sets)} WHERE name=?",
                    vals,
                )
            else:
                cols = ["name", "value", "description", "created_at", "updated_at", "secret"]
                vals2: list = [name, stored_value, description, now, now, 1 if secret else 0]
                if has_project and project is not None:
                    cols.append("project")
                    vals2.append(project)
                if has_tags and tags is not None:
                    cols.append("tags")
                    vals2.append(json.dumps(tags))
                if has_group and group_name is not None:
                    cols.append("group_name")
                    vals2.append(group_name)
                placeholders = ",".join("?" * len(cols))
                conn.execute(
                    f"INSERT INTO variables ({','.join(cols)}) VALUES ({placeholders})",
                    vals2,
                )

    def variable_get(self, name: str) -> Optional[str]:
        """Return the decrypted value of a managed variable, or None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT value, secret FROM variables WHERE name=?", (name,)
            ).fetchone()
        if row is None:
            return None
        if row["secret"]:
            from brix.credential_store import _decrypt
            return _decrypt(row["value"])
        return row["value"]

    def variable_get_raw(self, name: str) -> Optional[dict]:
        """Return the raw row dict for a variable (value not decrypted), or None.

        Internal use — for context.py to track secret values.
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM variables WHERE name=?", (name,)
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        # Ensure org fields are always present
        d.setdefault("project", "")
        raw_tags = d.get("tags", "[]")
        if isinstance(raw_tags, str):
            try:
                d["tags"] = json.loads(raw_tags)
            except (json.JSONDecodeError, TypeError):
                d["tags"] = []
        d.setdefault("group_name", "")
        return d

    def variable_list(
        self,
        project: Optional[str] = None,
        group_name: Optional[str] = None,
        tags: Optional[list] = None,
    ) -> list[dict]:
        """Return all managed variables as list of dicts.

        For secret variables the 'value' field is returned as '***SECRET***'.
        The 'secret' field (bool) indicates whether the variable is encrypted.
        Optionally filtered by project, group_name, or tags.
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM variables ORDER BY name"
            ).fetchall()
        result = []
        for r in rows:
            row_dict = dict(r)
            row_dict["secret"] = bool(row_dict.get("secret", 0))
            if row_dict["secret"]:
                row_dict["value"] = "***SECRET***"
            # Ensure org fields
            row_dict.setdefault("project", "")
            raw_tags = row_dict.get("tags", "[]")
            if isinstance(raw_tags, str):
                try:
                    row_dict["tags"] = json.loads(raw_tags)
                except (json.JSONDecodeError, TypeError):
                    row_dict["tags"] = []
            row_dict.setdefault("group_name", "")
            # Apply filters
            if project is not None and row_dict.get("project", "") != project:
                continue
            if group_name is not None and row_dict.get("group_name", "") != group_name:
                continue
            if tags is not None:
                var_tags = row_dict.get("tags", [])
                if not any(t in var_tags for t in tags):
                    continue
            result.append(row_dict)
        return result

    def variable_delete(self, name: str) -> bool:
        """Delete a managed variable. Returns True if it existed."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM variables WHERE name=?", (name,))
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # Persistent Data Store (T-BRIX-DB-13)
    # ------------------------------------------------------------------

    def store_set(self, key: str, value: str, pipeline_name: str = "") -> None:
        """Create or update a persistent store entry (upsert)."""
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO persistent_store (key, value, pipeline_name, updated_at)
                   VALUES (?,?,?,?)
                   ON CONFLICT(key) DO UPDATE SET
                     value=excluded.value,
                     pipeline_name=excluded.pipeline_name,
                     updated_at=excluded.updated_at""",
                (key, value, pipeline_name, now),
            )

    def store_get(self, key: str) -> Optional[str]:
        """Return the value from persistent store, or None if not found."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM persistent_store WHERE key=?", (key,)
            ).fetchone()
        return row[0] if row else None

    def store_list(self, pipeline_name: Optional[str] = None) -> list[dict]:
        """Return persistent store entries, optionally filtered by pipeline_name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            if pipeline_name is not None:
                rows = conn.execute(
                    "SELECT key, value, pipeline_name, updated_at "
                    "FROM persistent_store WHERE pipeline_name=? ORDER BY key",
                    (pipeline_name,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT key, value, pipeline_name, updated_at "
                    "FROM persistent_store ORDER BY key"
                ).fetchall()
        return [dict(r) for r in rows]

    def store_delete(self, key: str) -> bool:
        """Delete a persistent store entry. Returns True if it existed."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM persistent_store WHERE key=?", (key,))
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # Resilience: Circuit Breaker (T-BRIX-DB-21)
    # ------------------------------------------------------------------

    def cb_get(self, brick_name: str) -> Optional[dict]:
        """Return circuit breaker state for the given brick, or None."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT brick_name, failure_count, last_failure, cooldown_until, updated_at "
                "FROM circuit_breaker_state WHERE brick_name=?",
                (brick_name,),
            ).fetchone()
        return dict(row) if row else None

    def cb_upsert(
        self,
        brick_name: str,
        failure_count: int,
        last_failure: Optional[str],
        cooldown_until: Optional[str],
    ) -> None:
        """Insert or update circuit breaker state."""
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO circuit_breaker_state
                   (brick_name, failure_count, last_failure, cooldown_until, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(brick_name) DO UPDATE SET
                     failure_count=excluded.failure_count,
                     last_failure=excluded.last_failure,
                     cooldown_until=excluded.cooldown_until,
                     updated_at=excluded.updated_at""",
                (brick_name, failure_count, last_failure, cooldown_until, now),
            )

    def cb_reset(self, brick_name: str) -> None:
        """Reset circuit breaker failure count and clear cooldown."""
        self.cb_upsert(brick_name, 0, None, None)

    # ------------------------------------------------------------------
    # Resilience: Rate Limiter (T-BRIX-DB-21)
    # ------------------------------------------------------------------

    def rl_get_timestamps(self, brick_name: str) -> list:
        """Return list of ISO timestamp strings for the given brick."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT call_timestamps FROM rate_limiter_state WHERE brick_name=?",
                (brick_name,),
            ).fetchone()
        if not row:
            return []
        try:
            return json.loads(row[0]) or []
        except Exception:
            return []

    def rl_set_timestamps(self, brick_name: str, timestamps: list) -> None:
        """Persist the list of ISO timestamp strings for rate limiter."""
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO rate_limiter_state (brick_name, call_timestamps, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(brick_name) DO UPDATE SET
                     call_timestamps=excluded.call_timestamps,
                     updated_at=excluded.updated_at""",
                (brick_name, json.dumps(timestamps), now),
            )

    # ------------------------------------------------------------------
    # Resilience: Brick Cache (T-BRIX-DB-21)
    # ------------------------------------------------------------------

    def bcache_get(self, cache_key: str) -> Optional[Any]:
        """Return cached output if key exists and has not expired, else None."""
        now = _now_iso()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT output_data FROM brick_cache WHERE cache_key=? AND expires_at > ?",
                (cache_key, now),
            ).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return row[0]

    def bcache_set(self, cache_key: str, output_data: Any, expires_at: str) -> None:
        """Insert or replace a cache entry."""
        now = _now_iso()
        try:
            serialized = json.dumps(output_data)
        except Exception:
            serialized = str(output_data)
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO brick_cache (cache_key, output_data, created_at, expires_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(cache_key) DO UPDATE SET
                     output_data=excluded.output_data,
                     created_at=excluded.created_at,
                     expires_at=excluded.expires_at""",
                (cache_key, serialized, now, expires_at),
            )

    def bcache_delete(self, cache_key: str) -> None:
        """Delete a single cache entry."""
        with self._connect() as conn:
            conn.execute("DELETE FROM brick_cache WHERE cache_key=?", (cache_key,))

    def bcache_purge_expired(self) -> int:
        """Remove all expired cache entries. Returns the number deleted."""
        now = _now_iso()
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM brick_cache WHERE expires_at <= ?", (now,))
            return cursor.rowcount

    # ------------------------------------------------------------------
    # Profiles / Mixins (T-BRIX-DB-23)
    # ------------------------------------------------------------------

    def profile_set(
        self,
        name: str,
        config: dict,
        description: str = "",
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> dict:
        """Create or update a profile. Returns the stored profile dict."""
        now = _now_iso()
        config_json = json.dumps(config)
        with self._connect() as conn:
            has_project = self._column_exists(conn, "profiles", "project")
            has_tags = self._column_exists(conn, "profiles", "tags")
            has_group = self._column_exists(conn, "profiles", "group_name")

            cols = ["name", "config", "description", "created_at", "updated_at"]
            vals: list = [name, config_json, description, now, now]
            updates = [
                "config=excluded.config",
                "description=excluded.description",
                "updated_at=excluded.updated_at",
            ]

            if has_project and project is not None:
                cols.append("project")
                vals.append(project)
                updates.append("project=excluded.project")
            if has_tags and tags is not None:
                cols.append("tags")
                vals.append(json.dumps(tags))
                updates.append("tags=excluded.tags")
            if has_group and group_name is not None:
                cols.append("group_name")
                vals.append(group_name)
                updates.append("group_name=excluded.group_name")

            placeholders = ",".join("?" * len(cols))
            update_str = ",".join(updates)
            conn.execute(
                f"""INSERT INTO profiles ({','.join(cols)})
                   VALUES ({placeholders})
                   ON CONFLICT(name) DO UPDATE SET {update_str}""",
                vals,
            )
        return self.profile_get(name)

    @staticmethod
    def _profile_enrich_org(d: dict) -> dict:
        """Enrich a profile dict with parsed org fields."""
        try:
            d["config"] = json.loads(d["config"])
        except (json.JSONDecodeError, TypeError):
            d["config"] = {}
        d.setdefault("project", "")
        raw_tags = d.get("tags", "[]")
        if isinstance(raw_tags, str):
            try:
                d["tags"] = json.loads(raw_tags)
            except (json.JSONDecodeError, TypeError):
                d["tags"] = []
        d.setdefault("group_name", "")
        return d

    def profile_get(self, name: str) -> Optional[dict]:
        """Return a profile by name, or None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM profiles WHERE name=?", (name,)
            ).fetchone()
        if not row:
            return None
        return self._profile_enrich_org(dict(row))

    def profile_list(self) -> list[dict]:
        """Return all profiles ordered by name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM profiles ORDER BY name"
            ).fetchall()
        return [self._profile_enrich_org(dict(row)) for row in rows]

    def profile_delete(self, name: str) -> bool:
        """Delete a profile by name. Returns True if found and deleted."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM profiles WHERE name=?", (name,))
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # Step Pins — Mock-Daten für Testing (T-BRIX-DB-24)
    # ------------------------------------------------------------------

    def pin_step(
        self,
        pipeline_name: str,
        step_id: str,
        data: Any,
        from_run: str = "",
    ) -> dict:
        """Pin (upsert) mock output data for a step.

        Parameters
        ----------
        pipeline_name:
            Name of the pipeline that owns the step.
        step_id:
            Step identifier within the pipeline.
        data:
            The mock output to return instead of executing the step.
        from_run:
            Optional run_id from which the data was captured.

        Returns
        -------
        The stored pin record as a dict.
        """
        now = _now_iso()
        data_json = json.dumps(data)
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO step_pins (pipeline_name, step_id, pinned_data, pinned_from_run, created_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(pipeline_name, step_id) DO UPDATE SET
                       pinned_data=excluded.pinned_data,
                       pinned_from_run=excluded.pinned_from_run,
                       created_at=excluded.created_at""",
                (pipeline_name, step_id, data_json, from_run, now),
            )
        return {
            "pipeline_name": pipeline_name,
            "step_id": step_id,
            "pinned_data": data,
            "pinned_from_run": from_run,
            "created_at": now,
        }

    def unpin_step(self, pipeline_name: str, step_id: str) -> bool:
        """Remove a pin. Returns True if found and deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM step_pins WHERE pipeline_name=? AND step_id=?",
                (pipeline_name, step_id),
            )
            return cursor.rowcount > 0

    def get_pin(self, pipeline_name: str, step_id: str) -> Optional[dict]:
        """Return the pin record for a step, or None if not pinned."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM step_pins WHERE pipeline_name=? AND step_id=?",
                (pipeline_name, step_id),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        try:
            result["pinned_data"] = json.loads(result["pinned_data"])
        except (json.JSONDecodeError, TypeError):
            pass
        return result

    def get_pins(self, pipeline_name: str) -> list[dict]:
        """Return all pins for a pipeline, ordered by step_id."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM step_pins WHERE pipeline_name=? ORDER BY step_id",
                (pipeline_name,),
            ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            try:
                d["pinned_data"] = json.loads(d["pinned_data"])
            except (json.JSONDecodeError, TypeError):
                pass
            out.append(d)
        return out
