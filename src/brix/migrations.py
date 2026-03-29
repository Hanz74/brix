"""Schema migration system for brix.db.

Provides a versioned, forward-only migration infrastructure for BrixDB.
Version 0 represents the current baseline state (all tables created via
CREATE TABLE IF NOT EXISTS in _DDL). Future schema changes are added here
as numbered migrations instead of inline ALTER TABLE calls in _init_schema.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brix.db import BrixDB

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Migration Definitions
# ---------------------------------------------------------------------------
# Each entry must have:
#   version (int)  — monotonically increasing, starting at 1
#   name    (str)  — short human-readable slug
#   up      (str)  — SQL to apply the migration (idempotent where possible)
#   down    (str)  — SQL to reverse it (may be empty string if irreversible)
#
# Version 0 = current baseline (all CREATE TABLE IF NOT EXISTS in _DDL).
# Existing inline ALTER TABLE calls in _init_schema are kept as-is for
# backward compatibility with databases created before this system existed.
# New schema changes from this point forward go here.
# ---------------------------------------------------------------------------

MIGRATIONS: list[dict] = [
    {
        "version": 1,
        "name": "add_yaml_content_to_pipelines",
        "up": "ALTER TABLE pipelines ADD COLUMN yaml_content TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 2,
        "name": "add_code_to_helpers",
        "up": "ALTER TABLE helpers ADD COLUMN code TEXT DEFAULT ''",
        "down": "",
    },
    # T-BRIX-ORG-01: Project organisation — project, tags, group columns
    {
        "version": 3,
        "name": "add_project_to_pipelines",
        "up": "ALTER TABLE pipelines ADD COLUMN project TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 4,
        "name": "add_tags_to_pipelines",
        "up": "ALTER TABLE pipelines ADD COLUMN tags TEXT DEFAULT '[]'",
        "down": "",
    },
    {
        "version": 5,
        "name": "add_group_name_to_pipelines",
        "up": "ALTER TABLE pipelines ADD COLUMN group_name TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 6,
        "name": "add_project_to_helpers",
        "up": "ALTER TABLE helpers ADD COLUMN project TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 7,
        "name": "add_tags_to_helpers",
        "up": "ALTER TABLE helpers ADD COLUMN tags TEXT DEFAULT '[]'",
        "down": "",
    },
    {
        "version": 8,
        "name": "add_group_name_to_helpers",
        "up": "ALTER TABLE helpers ADD COLUMN group_name TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 9,
        "name": "add_project_to_variables",
        "up": "ALTER TABLE variables ADD COLUMN project TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 10,
        "name": "add_tags_to_variables",
        "up": "ALTER TABLE variables ADD COLUMN tags TEXT DEFAULT '[]'",
        "down": "",
    },
    {
        "version": 11,
        "name": "add_group_name_to_variables",
        "up": "ALTER TABLE variables ADD COLUMN group_name TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 12,
        "name": "add_project_to_triggers",
        "up": "ALTER TABLE triggers ADD COLUMN project TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 13,
        "name": "add_tags_to_triggers",
        "up": "ALTER TABLE triggers ADD COLUMN tags TEXT DEFAULT '[]'",
        "down": "",
    },
    {
        "version": 14,
        "name": "add_group_name_to_triggers",
        "up": "ALTER TABLE triggers ADD COLUMN group_name TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 15,
        "name": "add_tags_to_brick_definitions",
        "up": "ALTER TABLE brick_definitions ADD COLUMN org_tags TEXT DEFAULT '[]'",
        "down": "",
    },
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _table_exists(conn, table_name: str) -> bool:
    """Return True if a table with the given name exists."""
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row and row[0])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_migrations_table(db: "BrixDB") -> None:
    """Create the schema_migrations tracking table if it does not exist.

    The table stores one row per applied migration with the version number,
    migration name, and ISO-8601 timestamp of when it was applied.
    """
    with db._connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version     INTEGER PRIMARY KEY,
                name        TEXT NOT NULL,
                applied_at  TEXT NOT NULL
            )
            """
        )


def get_current_version(db: "BrixDB") -> int:
    """Return the highest applied migration version (0 if none applied yet).

    Returns 0 when the schema_migrations table is empty or does not exist,
    meaning the database is at the baseline state.
    """
    with db._connect() as conn:
        if not _table_exists(conn, "schema_migrations"):
            return 0
        row = conn.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def run_pending_migrations(db: "BrixDB") -> list[dict]:
    """Apply all pending migrations in order.

    Skips migrations whose version is <= the current applied version.
    Returns a list of migration records that were applied in this call.
    Each record contains ``version``, ``name``, and ``applied_at``.
    """
    ensure_migrations_table(db)
    current = get_current_version(db)

    applied: list[dict] = []
    for migration in sorted(MIGRATIONS, key=lambda m: m["version"]):
        if migration["version"] <= current:
            continue  # already applied

        version = migration["version"]
        name = migration["name"]
        up_sql = migration.get("up", "")

        logger.info("Applying migration v%d: %s", version, name)
        try:
            with db._connect() as conn:
                if up_sql:
                    conn.execute(up_sql)
                from brix.db import _now_iso  # avoid circular at module level
                applied_at = _now_iso()
                conn.execute(
                    "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
                    (version, name, applied_at),
                )
        except Exception as exc:
            logger.error("Migration v%d failed: %s", version, exc)
            raise RuntimeError(f"Migration v{version} ({name}) failed: {exc}") from exc

        applied.append({"version": version, "name": name, "applied_at": applied_at})
        logger.info("Migration v%d applied successfully", version)

    return applied


def rollback_migration(db: "BrixDB", version: int) -> bool:
    """Rollback a specific migration by version number.

    Executes the ``down`` SQL for the migration and removes its row from
    schema_migrations.  Returns True on success, False if the migration
    was not applied or does not exist in MIGRATIONS.

    Note: SQLite has limited DDL rollback support.  Migrations whose
    ``down`` is an empty string are considered irreversible and will still
    remove the tracking row (marking them as rolled back) but will not
    alter the schema.
    """
    ensure_migrations_table(db)

    # Check the migration exists in our definitions
    migration = next((m for m in MIGRATIONS if m["version"] == version), None)
    if migration is None:
        logger.warning("rollback_migration: version %d not found in MIGRATIONS", version)
        return False

    # Check it's actually applied
    with db._connect() as conn:
        row = conn.execute(
            "SELECT version FROM schema_migrations WHERE version = ?", (version,)
        ).fetchone()
    if row is None:
        logger.warning("rollback_migration: version %d was not applied", version)
        return False

    down_sql = migration.get("down", "")
    logger.info("Rolling back migration v%d: %s", version, migration["name"])
    try:
        with db._connect() as conn:
            if down_sql:
                conn.execute(down_sql)
            conn.execute(
                "DELETE FROM schema_migrations WHERE version = ?", (version,)
            )
    except Exception as exc:
        logger.error("Rollback of v%d failed: %s", version, exc)
        raise RuntimeError(f"Rollback of v{version} failed: {exc}") from exc

    logger.info("Migration v%d rolled back", version)
    return True


def get_migration_status(db: "BrixDB") -> dict:
    """Return a status summary of the migration system.

    Provides current version, list of applied migrations, list of pending
    migrations, and database file size in bytes.
    """
    ensure_migrations_table(db)

    with db._connect() as conn:
        rows = conn.execute(
            "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
        ).fetchall()

    applied_versions = {row[0] for row in rows}
    applied = [{"version": r[0], "name": r[1], "applied_at": r[2]} for r in rows]
    pending = [
        {"version": m["version"], "name": m["name"]}
        for m in sorted(MIGRATIONS, key=lambda m: m["version"])
        if m["version"] not in applied_versions
    ]

    # DB size
    db_size_bytes = 0
    try:
        db_size_bytes = db.db_path.stat().st_size
    except Exception:
        pass

    current_version = max(applied_versions) if applied_versions else 0

    return {
        "current_version": current_version,
        "applied": applied,
        "pending": pending,
        "total_migrations": len(MIGRATIONS),
        "db_size_bytes": db_size_bytes,
        "db_path": str(db.db_path),
    }
