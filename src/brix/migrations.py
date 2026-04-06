"""Schema migration system for brix.db.

Provides a versioned, forward-only migration infrastructure for BrixDB.
Version 0 represents the current baseline state (all tables created via
CREATE TABLE IF NOT EXISTS in _DDL). Future schema changes are added here
as numbered migrations instead of inline ALTER TABLE calls in _init_schema.
"""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import yaml

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
    # T-BRIX-ORG-01: project + group_name for brick_definitions
    {
        "version": 16,
        "name": "add_project_to_brick_definitions",
        "up": "ALTER TABLE brick_definitions ADD COLUMN project TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 17,
        "name": "add_group_name_to_brick_definitions",
        "up": "ALTER TABLE brick_definitions ADD COLUMN group_name TEXT DEFAULT ''",
        "down": "",
    },
    # T-BRIX-ORG-01: project/tags/group for connections
    {
        "version": 18,
        "name": "add_project_to_connections",
        "up": "ALTER TABLE connections ADD COLUMN project TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 19,
        "name": "add_tags_to_connections",
        "up": "ALTER TABLE connections ADD COLUMN tags TEXT DEFAULT '[]'",
        "down": "",
    },
    {
        "version": 20,
        "name": "add_group_name_to_connections",
        "up": "ALTER TABLE connections ADD COLUMN group_name TEXT DEFAULT ''",
        "down": "",
    },
    # T-BRIX-ORG-01: project/tags/group for profiles
    {
        "version": 21,
        "name": "add_project_to_profiles",
        "up": "ALTER TABLE profiles ADD COLUMN project TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 22,
        "name": "add_tags_to_profiles",
        "up": "ALTER TABLE profiles ADD COLUMN tags TEXT DEFAULT '[]'",
        "down": "",
    },
    {
        "version": 23,
        "name": "add_group_name_to_profiles",
        "up": "ALTER TABLE profiles ADD COLUMN group_name TEXT DEFAULT ''",
        "down": "",
    },
    # Missing org fields: description on pipelines/triggers, tags on brick_definitions
    {
        "version": 24,
        "name": "add_description_to_pipelines",
        "up": "ALTER TABLE pipelines ADD COLUMN description TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 25,
        "name": "add_description_to_triggers",
        "up": "ALTER TABLE triggers ADD COLUMN description TEXT DEFAULT ''",
        "down": "",
    },
    {
        "version": 26,
        "name": "add_tags_to_brick_definitions",
        "up": "ALTER TABLE brick_definitions ADD COLUMN tags TEXT DEFAULT '[]'",
        "down": "",
    },
    # Remaining entities: trigger_groups, alert_rules, registry_*
    {"version": 27, "name": "add_org_to_trigger_groups_project", "up": "ALTER TABLE trigger_groups ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 28, "name": "add_org_to_trigger_groups_tags", "up": "ALTER TABLE trigger_groups ADD COLUMN tags TEXT DEFAULT '[]'", "down": ""},
    {"version": 29, "name": "add_org_to_trigger_groups_group", "up": "ALTER TABLE trigger_groups ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    {"version": 30, "name": "add_org_to_alert_rules_project", "up": "ALTER TABLE alert_rules ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 31, "name": "add_org_to_alert_rules_tags", "up": "ALTER TABLE alert_rules ADD COLUMN tags TEXT DEFAULT '[]'", "down": ""},
    {"version": 32, "name": "add_org_to_alert_rules_group", "up": "ALTER TABLE alert_rules ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    {"version": 33, "name": "add_org_to_alert_rules_description", "up": "ALTER TABLE alert_rules ADD COLUMN description TEXT DEFAULT ''", "down": ""},
    {"version": 34, "name": "add_org_to_registry_project", "up": "ALTER TABLE registry_best_practices ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 35, "name": "add_org_to_registry_group", "up": "ALTER TABLE registry_best_practices ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    {"version": 36, "name": "add_org_to_registry_error_project", "up": "ALTER TABLE registry_error_patterns ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 37, "name": "add_org_to_registry_error_group", "up": "ALTER TABLE registry_error_patterns ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    {"version": 38, "name": "add_org_to_registry_lessons_project", "up": "ALTER TABLE registry_lessons_learned ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 39, "name": "add_org_to_registry_lessons_group", "up": "ALTER TABLE registry_lessons_learned ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    {"version": 40, "name": "add_org_to_registry_patterns_project", "up": "ALTER TABLE registry_patterns ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 41, "name": "add_org_to_registry_patterns_group", "up": "ALTER TABLE registry_patterns ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    {"version": 42, "name": "add_org_to_registry_schemas_project", "up": "ALTER TABLE registry_schemas ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 43, "name": "add_org_to_registry_schemas_group", "up": "ALTER TABLE registry_schemas ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    {"version": 44, "name": "add_org_to_registry_templates_project", "up": "ALTER TABLE registry_templates ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 45, "name": "add_org_to_registry_templates_group", "up": "ALTER TABLE registry_templates ADD COLUMN group_name TEXT DEFAULT ''", "down": "",
    },
    # T-BRIX-PERF-01: Performance indexes for common query patterns
    {"version": 46, "name": "idx_runs_pipeline", "up": "CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON runs (pipeline)", "down": "DROP INDEX IF EXISTS idx_runs_pipeline"},
    {"version": 47, "name": "idx_runs_started_at", "up": "CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs (started_at DESC)", "down": "DROP INDEX IF EXISTS idx_runs_started_at"},
    {"version": 48, "name": "idx_object_versions_type_name", "up": "CREATE INDEX IF NOT EXISTS idx_object_versions_type_name ON object_versions (type, name)", "down": "DROP INDEX IF EXISTS idx_object_versions_type_name"},
    {"version": 49, "name": "idx_app_log_timestamp", "up": "CREATE INDEX IF NOT EXISTS idx_app_log_timestamp ON app_log (timestamp DESC)", "down": "DROP INDEX IF EXISTS idx_app_log_timestamp"},
    {"version": 50, "name": "idx_persistent_store_pipeline", "up": "CREATE INDEX IF NOT EXISTS idx_persistent_store_pipeline ON persistent_store (pipeline_name)", "down": "DROP INDEX IF EXISTS idx_persistent_store_pipeline"},
    # T-BRIX-SCHEMA-01: Add org fields to connector_definitions (last entity without them)
    {"version": 51, "name": "add_org_to_connector_definitions_project", "up": "ALTER TABLE connector_definitions ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 52, "name": "add_org_to_connector_definitions_tags", "up": "ALTER TABLE connector_definitions ADD COLUMN tags TEXT DEFAULT '[]'", "down": ""},
    {"version": 53, "name": "add_org_to_connector_definitions_group", "up": "ALTER TABLE connector_definitions ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    # T-BRIX-SCHEMA-03: Add project to runs table + backfill from pipelines
    {"version": 54, "name": "add_project_to_runs", "up": "ALTER TABLE runs ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 55, "name": "idx_runs_project", "up": "CREATE INDEX IF NOT EXISTS idx_runs_project ON runs (project)", "down": "DROP INDEX IF EXISTS idx_runs_project"},
    {"version": 56, "name": "backfill_runs_project", "up": "UPDATE runs SET project = COALESCE((SELECT p.project FROM pipelines p WHERE p.name = runs.pipeline), '') WHERE project = ''", "down": ""},
    # T-BRIX-SCHEMA-03: Add org fields to persistent_store
    {"version": 57, "name": "add_project_to_persistent_store", "up": "ALTER TABLE persistent_store ADD COLUMN project TEXT DEFAULT ''", "down": ""},
    {"version": 58, "name": "add_tags_to_persistent_store", "up": "ALTER TABLE persistent_store ADD COLUMN tags TEXT DEFAULT '[]'", "down": ""},
    {"version": 59, "name": "add_group_name_to_persistent_store", "up": "ALTER TABLE persistent_store ADD COLUMN group_name TEXT DEFAULT ''", "down": ""},
    # T-BRIX-DBQUAL-01: Backfill pipeline_helpers from all stored pipeline YAMLs
    {"version": 60, "name": "backfill_pipeline_helpers", "up": "", "up_fn": "_backfill_pipeline_helpers", "down": "DELETE FROM pipeline_helpers"},
    # T-BRIX-TIPS-01: Tips table — move hardcoded get_tips content to DB
    {
        "version": 61,
        "name": "create_tips_table",
        "up": """CREATE TABLE IF NOT EXISTS tips (
            id         TEXT PRIMARY KEY,
            category   TEXT NOT NULL,
            title      TEXT NOT NULL,
            content    TEXT NOT NULL,
            priority   INTEGER DEFAULT 5,
            is_active  INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )""",
        "down": "DROP TABLE IF EXISTS tips",
    },
    {
        "version": 62,
        "name": "seed_tips_from_hardcoded",
        "up": "",
        "up_fn": "_seed_tips_from_hardcoded",
        "down": "DELETE FROM tips",
    },
    # T-BRIX-DEBT-01: Rename all plural table names to singular
    {
        "version": 63,
        "name": "rename_tables_plural_to_singular",
        "up": "",
        "up_fn": "_rename_tables_to_singular",
        "down": "",
    },
    # T-BRIX-LOG-01: Register get_app_log MCP tool schema
    {
        "version": 64,
        "name": "register_get_app_log_tool",
        "up": "",
        "up_fn": "_register_get_app_log_tool",
        "down": "DELETE FROM mcp_tool_schema WHERE name = 'brix__get_app_log'",
    },
    # T-BRIX-COMPAT-01: Backward-compat views for brix-ui (reads DB directly with plural names)
    {
        "version": 65,
        "name": "create_plural_compat_views",
        "up": "",
        "up_fn": "_create_plural_compat_views",
        "down": "",
    },
    # T-BRIX-DBF-04: Add category column to help_topic for MCP CRUD
    {
        "version": 66,
        "name": "add_category_to_help_topic",
        "up": "ALTER TABLE help_topic ADD COLUMN category TEXT DEFAULT ''",
        "down": "",
    },
    # Register ALL missing MCP tool schemas from _HANDLERS
    {
        "version": 67,
        "name": "register_new_mcp_tool_schemas",
        "up": "",
        "up_fn": "_register_new_tool_schemas_v67",
        "down": "",
    },
    # Backfill real input_schema for 43 tools that had empty schemas
    {
        "version": 68,
        "name": "backfill_tool_input_schemas",
        "up": "",
        "up_fn": "_backfill_tool_input_schemas_v68",
        "down": "",
    },
    # T-BRIX-SCHED-03: Update brix__trigger tool schema + triggers help topic for schedule type
    {
        "version": 69,
        "name": "update_trigger_schema_and_help_for_schedule",
        "up": "",
        "up_fn": "_update_trigger_schema_and_help_v69",
        "down": "",
    },
    {
        "version": 70,
        "name": "add_db_only_pipeline_persistence_tables",
        "up": "",
        "up_fn": "_add_db_only_pipeline_persistence_v70",
        "down": "",
    },
    {
        "version": 71,
        "name": "normalize_pipeline_steps_rows",
        "up": "",
        "up_fn": "_normalize_pipeline_steps_v71",
        "down": "",
    },
    {
        "version": 72,
        "name": "add_nested_step_json_columns",
        "up": "",
        "up_fn": "_add_nested_step_json_columns_v72",
        "down": "",
    },
    {
        "version": 73,
        "name": "refresh_db_only_persistence_docs_content",
        "up": "",
        "up_fn": "_refresh_db_only_persistence_docs_v73",
        "down": "",
    },
    {
        "version": 74,
        "name": "add_missing_pipeline_step_runner_columns",
        "up": "",
        "up_fn": "_add_missing_pipeline_step_runner_columns_v74",
        "down": "",
    },
]


def _add_db_only_pipeline_persistence_v70(db: "BrixDB") -> None:
    """Create normalized pipeline persistence tables and extend pipeline metadata."""
    from brix.db import (
        _PIPELINE_CREDENTIAL_DDL,
        _PIPELINE_INPUT_DDL,
        _PIPELINE_STEP_DDL,
        _PIPELINE_STEP_INDEX_DDL,
    )

    pipeline_columns = [
        ("version", "TEXT DEFAULT '1.0.0'"),
        ("brix_version", "TEXT"),
        ("kind", "TEXT"),
        ("extends", "TEXT"),
        ("is_template", "INTEGER NOT NULL DEFAULT 0"),
        ("compositor_mode", "INTEGER NOT NULL DEFAULT 0"),
        ("allow_code", "INTEGER NOT NULL DEFAULT 1"),
        ("strict_bricks", "INTEGER NOT NULL DEFAULT 0"),
        ("test_mode", "INTEGER NOT NULL DEFAULT 0"),
        ("idempotency_key", "TEXT"),
        ("template_params_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("blueprint_params_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("error_handling_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("retry_profiles_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("notify_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("groups_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("output_json", "TEXT"),
        ("output_slots_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("migration_status", "TEXT DEFAULT NULL"),
    ]

    with db._connect() as conn:
        conn.execute(_PIPELINE_INPUT_DDL)
        conn.execute(_PIPELINE_CREDENTIAL_DDL)
        conn.execute(_PIPELINE_STEP_DDL)
        for ddl in _PIPELINE_STEP_INDEX_DDL:
            conn.execute(ddl)

        for column_name, column_sql in pipeline_columns:
            try:
                conn.execute(
                    f"ALTER TABLE pipeline ADD COLUMN {column_name} {column_sql}"
                )
            except Exception as exc:
                err = str(exc).lower()
                if "duplicate column" not in err:
                    raise


def _pipeline_metadata_updates_from_raw(raw: dict, pipeline_row: dict) -> dict[str, object]:
    """Build normalized pipeline metadata UPDATE values from legacy YAML."""
    from brix.db import _PIPELINE_BOOL_COLUMNS, _PIPELINE_JSON_COLUMNS, _json_dumps

    updates: dict[str, object] = {
        "version": raw.get("version") or pipeline_row.get("version") or "1.0.0",
        "brix_version": raw.get("brix_version", pipeline_row.get("brix_version")),
        "kind": raw.get("kind", pipeline_row.get("kind")),
        "extends": raw.get("extends", pipeline_row.get("extends")),
        "idempotency_key": raw.get("idempotency_key", pipeline_row.get("idempotency_key")),
        "description": raw.get("description", pipeline_row.get("description") or ""),
        "project": raw.get("project", pipeline_row.get("project") or ""),
        "group_name": raw.get("group", pipeline_row.get("group_name") or ""),
    }

    raw_tags = raw.get("tags", pipeline_row.get("tags") or [])
    if isinstance(raw_tags, str):
        try:
            raw_tags = json.loads(raw_tags)
        except Exception:
            raw_tags = []
    updates["tags"] = _json_dumps(raw_tags if isinstance(raw_tags, list) else [])

    bool_defaults = {
        "is_template": False,
        "compositor_mode": False,
        "allow_code": True,
        "strict_bricks": False,
        "test_mode": False,
    }
    for column, field in _PIPELINE_BOOL_COLUMNS.items():
        value = raw.get(field)
        if value is None:
            value = pipeline_row.get(column)
        if value is None:
            value = bool_defaults[field]
        updates[column] = int(bool(value))

    json_defaults = {
        "template_params_json": {},
        "blueprint_params_json": [],
        "error_handling_json": {},
        "retry_profiles_json": {},
        "notify_json": {},
        "groups_json": {},
        "output_json": None,
        "output_slots_json": {},
        "requirements_json": [],
    }
    for column, field in _PIPELINE_JSON_COLUMNS.items():
        value = raw.get(field)
        if value is None:
            existing = pipeline_row.get(column)
            if existing not in (None, ""):
                updates[column] = existing
                continue
            value = json_defaults[column]
        updates[column] = None if value is None else _json_dumps(value)

    return updates


def _set_pipeline_migration_status(
    db: "BrixDB",
    pipeline_id: str,
    status: str,
) -> None:
    from brix.db import _now_iso

    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET migration_status=?, updated_at=? WHERE id=?",
            (status, _now_iso(), pipeline_id),
        )


def _normalize_pipeline_steps_common(
    db: "BrixDB",
    *,
    log_prefix: str,
) -> dict[str, int]:
    """Normalize legacy YAML-backed pipeline rows into step/input/credential tables."""
    from brix.db import _now_iso

    summary = {"migrated": 0, "failed": 0, "skipped": 0}

    with db._connect() as conn:
        conn.row_factory = None
        rows = conn.execute(
            """
            SELECT id, name, yaml_content, migration_status
            FROM pipeline
            WHERE yaml_content IS NOT NULL AND yaml_content != ''
            ORDER BY name ASC
            """
        ).fetchall()

    for pipeline_id, pipeline_name, yaml_content, migration_status in rows:
        if migration_status == "v71_complete":
            summary["skipped"] += 1
            continue

        with db._connect() as conn:
            existing_steps = conn.execute(
                "SELECT COUNT(*) FROM pipeline_step WHERE pipeline_id=?",
                (pipeline_id,),
            ).fetchone()[0]
        if existing_steps:
            summary["skipped"] += 1
            continue

        try:
            raw = yaml.safe_load(yaml_content) or {}
            if not isinstance(raw, dict):
                raise ValueError("pipeline YAML did not parse to a mapping")

            steps = raw.get("steps") or []
            credentials = raw.get("credentials") or {}
            pipeline_input = raw.get("input") or {}

            if not isinstance(steps, list):
                raise ValueError("pipeline steps must be a list")
            if not isinstance(credentials, dict):
                raise ValueError("pipeline credentials must be a mapping")
            if not isinstance(pipeline_input, dict):
                raise ValueError("pipeline input must be a mapping")
            if any(not isinstance(step, dict) for step in steps):
                raise ValueError("pipeline steps must contain only mappings")

            with db._connect() as conn:
                conn.row_factory = None
                pipeline_row_result = conn.execute(
                    "SELECT * FROM pipeline WHERE id=?",
                    (pipeline_id,),
                )
                columns = [column[0] for column in pipeline_row_result.description]
                pipeline_row = dict(zip(columns, pipeline_row_result.fetchone()))

                for step_order, step in enumerate(steps):
                    db.upsert_step(pipeline_id, step, step_order=step_order, conn=conn)

                for alias, credential in credentials.items():
                    if isinstance(credential, str):
                        env_ref = credential
                        refresh = None
                    elif isinstance(credential, dict):
                        env_ref = credential.get("env") or ""
                        refresh = credential.get("refresh")
                    else:
                        raise ValueError(f"credential '{alias}' must be a string or mapping")
                    db.upsert_pipeline_credential(
                        pipeline_id,
                        alias,
                        env_ref,
                        refresh=refresh,
                        conn=conn,
                    )

                for input_key, input_spec in pipeline_input.items():
                    if not isinstance(input_spec, dict):
                        raise ValueError(f"input '{input_key}' must be a mapping")
                    db.upsert_pipeline_input(
                        pipeline_id,
                        input_key,
                        input_spec.get("type") or "string",
                        default_value=input_spec.get("default"),
                        description=input_spec.get("description"),
                        conn=conn,
                    )

                metadata_updates = _pipeline_metadata_updates_from_raw(raw, pipeline_row)
                metadata_updates["migration_status"] = "v71_complete"
                metadata_updates["updated_at"] = _now_iso()
                assignments = ", ".join(f"{column}=?" for column in metadata_updates)
                conn.execute(
                    f"UPDATE pipeline SET {assignments} WHERE id=?",
                    [*metadata_updates.values(), pipeline_id],
                )

            summary["migrated"] += 1
        except (yaml.YAMLError, ValueError, TypeError) as exc:
            _set_pipeline_migration_status(db, pipeline_id, "v71_failed")
            logger.warning(
                "%s: failed to normalize pipeline '%s': %s",
                log_prefix,
                pipeline_name,
                exc,
            )
            summary["failed"] += 1

    logger.info(
        "%s: migrated=%d failed=%d skipped=%d",
        log_prefix,
        summary["migrated"],
        summary["failed"],
        summary["skipped"],
    )
    return summary


def _normalize_pipeline_steps_v71(db: "BrixDB") -> None:
    """Normalize legacy YAML-backed pipelines into step/input/credential rows."""
    # Ensure nested step JSON columns exist (v72 adds them, but v71 needs them first)
    _add_nested_step_json_columns_v72(db)
    _normalize_pipeline_steps_common(db, log_prefix="migration v71")


def _add_nested_step_json_columns_v72(db: "BrixDB") -> None:
    """Add JSON columns needed to persist nested step containers in pipeline_step."""
    columns = [
        ("choices_json", "TEXT"),
        ("default_steps_json", "TEXT"),
        ("sequence_json", "TEXT"),
        ("sub_steps_json", "TEXT"),
    ]
    with db._connect() as conn:
        for column, column_type in columns:
            if not db._column_exists(conn, "pipeline_step", column):
                conn.execute(
                    f"ALTER TABLE pipeline_step ADD COLUMN {column} {column_type}"
                )


def _add_missing_pipeline_step_runner_columns_v74(db: "BrixDB") -> None:
    """Add pipeline_step columns required by merge/switch/error_handler/repeat."""
    columns = [
        ("inputs_json", "TEXT"),
        ("merge_mode", "TEXT"),
        ("merge_key", "TEXT"),
        ("switch_field", "TEXT"),
        ("cases_json", "TEXT"),
        ("switch_default", "TEXT"),
        ("try_step", "TEXT"),
        ("handler_step", "TEXT"),
        ("delay", "REAL"),
    ]
    with db._connect() as conn:
        for column, column_type in columns:
            if not db._column_exists(conn, "pipeline_step", column):
                conn.execute(
                    f"ALTER TABLE pipeline_step ADD COLUMN {column} {column_type}"
                )


def _load_seed_data_for_migration() -> dict | None:
    """Load seed-data.json from common runtime or repo locations."""
    from pathlib import Path

    seed_paths = [
        Path("/app/seed-data.json"),
        Path(__file__).parent.parent.parent / "seed-data.json",
    ]
    for sp in seed_paths:
        if sp.exists():
            try:
                with open(sp) as f:
                    return json.load(f)
            except Exception:
                continue
    return None


def _db_only_tip_payloads_v73() -> list[dict]:
    """Return tip content updates for DB-only persistence guidance."""
    return [
        {
            "title": "KERN-REGEL",
            "category": "KERN-REGEL",
            "content": (
                "IMMER Brix MCP-Tools nutzen. KEINE Workarounds. KEINE manuellen Pipeline-Dateien.\n"
                "Pipelines leben in brix.db als normale DB-Zeilen (`pipeline`, `pipeline_step`,\n"
                "`pipeline_credential`, `pipeline_input`).\n"
                "`yaml_content` ist nur Backup fuer Rollback/Export-Kompatibilitaet.\n"
                "KEIN docker exec. KEIN YAML fuer normale CRUD. KEIN Container rebuild.\n"
                "KEIN Bash(cat ~/.brix/...)       → nutze get_run_log / get_run_status\n"
                "KEIN Bash(python3 -c ...)        → nutze create_helper\n"
                "KEIN Bash(rm -f ...)             → nutze brix__delete_run / brix clean"
            ),
            "priority": 10,
        },
        {
            "title": "TOP-5 ANTI-PATTERNS",
            "category": "TOP-5 ANTI-PATTERNS",
            "content": (
                "delete_pipeline + create_pipeline   →  update_step / update_pipeline / add_step\n"
                "Pipeline-YAML als Persistence sehen →  falsch: normale CRUD arbeitet auf DB-Zeilen\n"
                "YAML manuell schreiben              →  nur noch fuer Import/Export/Restore-Kompatibilitaet\n"
                "brix run via Bash                   →  brix__run_pipeline\n"
                "base64 in foreach-Loops             →  Dateipfade als Strings uebergeben\n"
                "concurrency: '{{ input.n }}'        →  concurrency muss int sein (kein Jinja2!)"
            ),
            "priority": 9,
        },
        {
            "title": "Trigger type selection guide",
            "category": "triggers",
            "content": (
                "Choose the right trigger type:\n"
                "  schedule      - Recurring cron jobs (e.g. daily report at 9am)\n"
                "  pipeline_done - Chain pipelines (run B after A completes)\n"
                "  http_poll     - Poll external API at intervals\n"
                "  mail          - Monitor inbox for new messages\n"
                "  file          - Watch filesystem for changes\n"
                "  event         - React to internal Brix events\n\n"
                "schedules.yaml existiert nicht mehr als normaler Persistenzpfad.\n"
                "Use brix__trigger(action='add', type='schedule', config={cron, timezone})."
            ),
            "priority": 5,
        },
        {
            "title": "DB-only Pipeline Persistence",
            "category": "architecture",
            "content": (
                "Pipeline-CRUD arbeitet auf DB-Zeilen, nicht auf Pipeline-Dateien.\n"
                "Source of Truth:\n"
                "  - pipeline\n"
                "  - pipeline_step\n"
                "  - pipeline_credential\n"
                "  - pipeline_input\n"
                "`yaml_content` bleibt als Backup/Mirror fuer Rollback und Export erhalten.\n"
                "Wenn du Pipeline-Inhalt sehen oder aendern willst:\n"
                "  - get_pipeline / list_pipelines\n"
                "  - update_pipeline / add_step / update_step / remove_step"
            ),
            "priority": 10,
        },
        {
            "title": "BRIX_STEP_SOURCE Toggle",
            "category": "architecture",
            "content": (
                "BRIX_STEP_SOURCE steuert, wo Step-Definitionen gelesen werden:\n"
                "  db   = DB-Zeilen sind aktiv\n"
                "  dual = DB-Zeilen + Vergleich mit yaml_content\n"
                "  yaml = Legacy-Leseweg aus yaml_content\n"
                "Nutze im Normalbetrieb `db`.\n"
                "`dual` ist fuer Paritaetschecks waehrend Migration/Debugging.\n"
                "`yaml` nur fuer Legacy-Faelle."
            ),
            "priority": 6,
        },
    ]


def _refresh_db_only_persistence_docs_v73(db: "BrixDB") -> None:
    """Refresh tips, help topics, and tool schema text for DB-only persistence."""
    seed_data = _load_seed_data_for_migration()
    if seed_data is None:
        logger.warning("migration v73: seed-data.json not found, skipping help/topic/schema refresh")
        return

    tool_names = {
        "brix__delete_pipeline",
        "brix__rename_pipeline",
        "brix__rollback",
        "brix__diagnose_run",
        "brix__get_pipeline",
    }
    topic_names = {
        "quick-start",
        "anti-patterns",
        "triggers",
        "registries",
        "error-patterns",
        "pipeline-persistence",
    }
    tips = _db_only_tip_payloads_v73()

    with db._connect() as conn:
        for ts in seed_data.get("mcp_tool_schemas", []):
            if ts.get("name") not in tool_names:
                continue
            conn.execute(
                "UPDATE mcp_tool_schema SET description = ?, updated_at = datetime('now') WHERE name = ?",
                (ts["description"], ts["name"]),
            )

        for topic in seed_data.get("help_topics", []):
            if topic.get("name") not in topic_names:
                continue
            conn.execute(
                """INSERT INTO help_topic (name, title, content, category, created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
                   ON CONFLICT(name) DO UPDATE SET
                       title=excluded.title,
                       content=excluded.content,
                       category=excluded.category,
                       updated_at=datetime('now')""",
                (
                    topic["name"],
                    topic.get("title", topic["name"]),
                    topic.get("content", ""),
                    topic.get("category", ""),
                ),
            )

        for tip in tips:
            row = conn.execute("SELECT id FROM tip WHERE title = ?", (tip["title"],)).fetchone()
            if row:
                conn.execute(
                    "UPDATE tip SET category = ?, content = ?, priority = ?, updated_at = datetime('now') WHERE id = ?",
                    (tip["category"], tip["content"], tip["priority"], row[0]),
                )
            else:
                from uuid import uuid4

                conn.execute(
                    """INSERT INTO tip (id, category, title, content, priority, is_active, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))""",
                    (
                        str(uuid4()),
                        tip["category"],
                        tip["title"],
                        tip["content"],
                        tip["priority"],
                    ),
                )


def _register_new_tool_schemas_v67(db: "BrixDB") -> None:
    """Register ALL MCP tool schemas from _HANDLERS that are missing in DB.

    Dynamically reads _HANDLERS to ensure every handler has a DB schema.
    This prevents the problem where new handlers are added in code but
    never registered in the DB — making them invisible to MCP clients.
    """
    try:
        from brix.mcp_server import _HANDLERS
    except ImportError:
        # Circular import during test DB init — skip, startup_sync handles it
        logger.warning("migration v67: skipped (circular import — startup_sync will handle)")
        return

    with db._connect() as conn:
        db_tools = {r[0] for r in conn.execute("SELECT name FROM mcp_tool_schema").fetchall()}
        registered = 0
        for name in sorted(_HANDLERS.keys()):
            if name not in db_tools:
                conn.execute(
                    "INSERT INTO mcp_tool_schema (name, description, input_schema, created_at, updated_at) "
                    "VALUES (?, ?, '{}', datetime('now'), datetime('now'))",
                    (name, f"MCP tool: {name}"),
                )
                logger.info("migration v67: registered tool '%s'", name)
                registered += 1
        if registered:
            logger.info("migration v67: registered %d missing tool schemas", registered)


def _backfill_tool_input_schemas_v68(db: "BrixDB") -> None:
    """Backfill real input_schema for 43 tools that were registered with empty schemas.

    Reads schemas from seed-data.json and UPDATEs existing rows.
    Only updates rows whose input_schema is empty ('{}' or '').
    """
    import json
    from pathlib import Path

    # Try loading from seed-data.json (available in container and dev)
    seed_paths = [
        Path("/app/seed-data.json"),           # Docker container
        Path(__file__).parent.parent.parent / "seed-data.json",  # Dev checkout
    ]
    seed_data = None
    for sp in seed_paths:
        if sp.exists():
            try:
                with open(sp) as f:
                    seed_data = json.load(f)
                break
            except Exception:
                continue

    if seed_data is None:
        logger.warning("migration v68: seed-data.json not found, skipping schema backfill")
        return

    tool_schemas = seed_data.get("mcp_tool_schemas", [])
    schema_map = {}
    for ts in tool_schemas:
        s = ts.get("input_schema", {})
        if isinstance(s, str):
            try:
                s = json.loads(s)
            except (json.JSONDecodeError, TypeError):
                s = {}
        if s and s != {}:
            schema_map[ts["name"]] = json.dumps(s)

    with db._connect() as conn:
        # Find rows with empty schemas
        rows = conn.execute(
            "SELECT name, input_schema FROM mcp_tool_schema"
        ).fetchall()
        updated = 0
        for name, current_schema in rows:
            # Check if current schema is empty
            is_empty = not current_schema or current_schema.strip() in ("", "{}", "null")
            if not is_empty:
                try:
                    parsed = json.loads(current_schema)
                    is_empty = parsed == {} or parsed is None
                except (json.JSONDecodeError, TypeError):
                    is_empty = True

            if is_empty and name in schema_map:
                conn.execute(
                    "UPDATE mcp_tool_schema SET input_schema = ?, updated_at = datetime('now') WHERE name = ?",
                    (schema_map[name], name),
                )
                updated += 1
                logger.info("migration v68: backfilled schema for '%s'", name)

        if updated:
            logger.info("migration v68: backfilled %d tool schemas", updated)


def _update_trigger_schema_and_help_v69(db: "BrixDB") -> None:
    """T-BRIX-SCHED-03: Update brix__trigger tool schema and triggers help topic.

    - Updates brix__trigger input_schema to include schedule in type enum
      and full property definitions (name, type, pipeline, config, etc.)
    - Updates triggers help topic to document all 6 trigger types incl. schedule
    - Inserts a tip about trigger type selection
    """
    import json
    from pathlib import Path

    seed_paths = [
        Path("/app/seed-data.json"),
        Path(__file__).parent.parent.parent / "seed-data.json",
    ]
    seed_data = None
    for sp in seed_paths:
        if sp.exists():
            try:
                with open(sp) as f:
                    seed_data = json.load(f)
                break
            except Exception:
                continue

    if seed_data is None:
        logger.warning("migration v69: seed-data.json not found, skipping")
        return

    with db._connect() as conn:
        # 1) Update brix__trigger tool schema from seed-data.json
        for ts in seed_data.get("mcp_tool_schemas", []):
            if ts["name"] == "brix__trigger":
                schema_json = json.dumps(ts.get("input_schema", {}))
                conn.execute(
                    "UPDATE mcp_tool_schema SET description = ?, input_schema = ?, "
                    "updated_at = datetime('now') WHERE name = ?",
                    (ts["description"], schema_json, "brix__trigger"),
                )
                logger.info("migration v69: updated brix__trigger tool schema")
                break

        # 2) Update triggers help topic from seed-data.json
        for ht in seed_data.get("help_topics", []):
            if ht["name"] == "triggers":
                conn.execute(
                    "UPDATE help_topic SET title = ?, content = ?, "
                    "updated_at = datetime('now') WHERE name = ?",
                    (ht["title"], ht["content"], "triggers"),
                )
                logger.info("migration v69: updated triggers help topic")
                break

        # 3) Insert trigger-type-selection tip (idempotent)
        tip_title = "Trigger type selection guide"
        existing = conn.execute(
            "SELECT id FROM tip WHERE title = ?", (tip_title,)
        ).fetchone()
        if not existing:
            from uuid import uuid4
            conn.execute(
                "INSERT INTO tip (id, category, title, content, priority, is_active, created_at, updated_at) "
                "VALUES (?, 'triggers', ?, ?, 5, 1, datetime('now'), datetime('now'))",
                (
                    str(uuid4()),
                    tip_title,
                    "Choose the right trigger type:\n"
                    "  schedule      - Recurring cron jobs (e.g. daily report at 9am)\n"
                    "  pipeline_done - Chain pipelines (run B after A completes)\n"
                    "  http_poll     - Poll external API at intervals\n"
                    "  mail          - Monitor inbox for new messages\n"
                    "  file          - Watch filesystem for changes\n"
                    "  event         - React to internal Brix events\n\n"
                    "Schedule triggers replace schedules.yaml (deprecated).\n"
                    "Use brix__trigger(action='add', type='schedule', config={cron, timezone}).",
                ),
            )
            logger.info("migration v69: inserted trigger-type-selection tip")


def _create_plural_compat_views(db: "BrixDB") -> None:
    """T-BRIX-COMPAT-01: Create read-only views with old plural names.

    Brix-UI mounts the SQLite DB directly and queries with plural table names.
    These views map the old plural names to the new singular tables.
    Idempotent: uses CREATE VIEW IF NOT EXISTS.
    """
    view_map = {
        "agent_sessions": "agent_session",
        "alert_rules": "alert_rule",
        "brick_definitions": "brick_definition",
        "connections": "connection",
        "connector_definitions": "connector_definition",
        "foreach_item_executions": "foreach_item_execution",
        "help_topics": "help_topic",
        "helpers": "helper",
        "keyword_taxonomies": "keyword_taxonomy",
        "mcp_tool_schemas": "mcp_tool_schema",
        "object_versions": "object_version",
        "pipeline_events": "pipeline_event",
        "pipeline_helpers": "pipeline_helper",
        "pipelines": "pipeline",
        "profiles": "profile",
        "registry_best_practices": "registry_best_practice",
        "registry_error_patterns": "registry_error_pattern",
        "registry_lessons_learned": "registry_lesson_learned",
        "registry_patterns": "registry_pattern",
        "registry_schemas": "registry_schema",
        "registry_templates": "registry_template",
        "resource_locks": "resource_lock",
        "run_inputs": "run_input",
        "runs": "run",
        "schema_migrations": "schema_migration",
        "step_executions": "step_execution",
        "step_outputs": "step_output",
        "step_pins": "step_pin",
        "tips": "tip",
        "trigger_groups": "trigger_group",
        "triggers": "trigger",
        "variables": "variable",
    }
    with db._connect() as conn:
        for plural, singular in view_map.items():
            if _table_exists(conn, singular):
                conn.execute(f"CREATE VIEW IF NOT EXISTS [{plural}] AS SELECT * FROM [{singular}]")
                logger.info("compat_views: created view '%s' -> '%s'", plural, singular)


def _rename_tables_to_singular(db: "BrixDB") -> None:
    """T-BRIX-DEBT-01: Rename all plural DB table names to singular.

    Idempotent: skips tables that have already been renamed
    (old name does not exist or new name already exists).
    schema_migrations is NOT renamed here because it is the migration
    tracking table itself and renaming it mid-migration would break
    the INSERT that records this migration as applied.
    """
    rename_map = {
        "agent_sessions": "agent_session",
        "alert_rules": "alert_rule",
        "brick_definitions": "brick_definition",
        "connections": "connection",
        "connector_definitions": "connector_definition",
        "foreach_item_executions": "foreach_item_execution",
        "help_topics": "help_topic",
        "helpers": "helper",
        "keyword_taxonomies": "keyword_taxonomy",
        "mcp_tool_schemas": "mcp_tool_schema",
        "object_versions": "object_version",
        "pipeline_events": "pipeline_event",
        "pipeline_helpers": "pipeline_helper",
        "pipelines": "pipeline",
        "profiles": "profile",
        "registry_best_practices": "registry_best_practice",
        "registry_error_patterns": "registry_error_pattern",
        "registry_lessons_learned": "registry_lesson_learned",
        "registry_patterns": "registry_pattern",
        "registry_schemas": "registry_schema",
        "registry_templates": "registry_template",
        "resource_locks": "resource_lock",
        "run_inputs": "run_input",
        "runs": "run",
        "step_executions": "step_execution",
        "step_outputs": "step_output",
        "step_pins": "step_pin",
        "tips": "tip",
        "trigger_groups": "trigger_group",
        "triggers": "trigger",
        "variables": "variable",
    }

    with db._connect() as conn:
        for old_name, new_name in rename_map.items():
            # Check if old table exists
            if not _table_exists(conn, old_name):
                logger.info(
                    "rename_tables: '%s' does not exist (already renamed or fresh DB), skipping",
                    old_name,
                )
                continue
            # Check if new table already exists (shouldn't on existing DB, but be safe)
            if _table_exists(conn, new_name):
                logger.warning(
                    "rename_tables: both '%s' and '%s' exist — skipping rename",
                    old_name, new_name,
                )
                continue
            conn.execute(f"ALTER TABLE [{old_name}] RENAME TO [{new_name}]")
            logger.info("rename_tables: renamed '%s' → '%s'", old_name, new_name)


def _register_get_app_log_tool(db: "BrixDB") -> None:
    """T-BRIX-LOG-01: Register brix__get_app_log tool schema."""
    import json as _json
    schema = {
        "type": "object",
        "properties": {
            "component": {
                "type": "string",
                "description": "Filter by component name (e.g. 'scheduler', 'trigger', 'watchdog', 'startup_sync')."
            },
            "level": {
                "type": "string",
                "enum": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                "description": "Filter by log level."
            },
            "since": {
                "type": "string",
                "description": "ISO-8601 timestamp — only entries at or after this time."
            },
            "limit": {
                "type": "integer",
                "default": 50,
                "description": "Maximum number of entries to return (default 50)."
            },
        },
    }
    with db._connect() as conn:
        from brix.db import _now_iso
        conn.execute(
            """INSERT INTO mcp_tool_schema (name, description, input_schema, created_at, updated_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT(name) DO UPDATE SET
                   description=excluded.description,
                   input_schema=excluded.input_schema,
                   updated_at=excluded.updated_at""",
            (
                "brix__get_app_log",
                "Query the structured application event log. Returns scheduler, trigger, watchdog, and startup events. Filter by component, level, or time range.",
                _json.dumps(schema),
                _now_iso(),
                _now_iso(),
            ),
        )
    logger.info("Registered brix__get_app_log tool schema")


def _seed_tips_from_hardcoded(db: "BrixDB") -> None:
    """Seed the tips table with the previously hardcoded static tip blocks."""
    from uuid import uuid4
    from brix.db import _now_iso

    tips = [
        ("BRICK-FIRST", "BRICK-FIRST — HÖCHSTE PRIORITÄT",
         "Nutze Brick-Namen (db.query, flow.filter, llm.batch etc.) statt alte Runner-Namen\n"
         "(python, http, mcp). Alte Namen funktionieren noch aber sind deprecated.\n"
         "KEIN create_helper für Standardaufgaben — nutze bestehende Bricks:\n"
         "  db.query         → Datenbankabfragen\n"
         "  db.upsert        → Daten in DB schreiben (INSERT/UPSERT)\n"
         "  db.exec          → SQL DML ausfuehren (UPDATE/DELETE/INSERT mit Commit)\n"
         "  llm.batch        → LLM-Extraktion über viele Dokumente\n"
         "  markitdown.convert → Dokumente/PDFs in Markdown konvertieren\n"
         "  extract.specialist → Regex-Extraktion mit Schema\n"
         "  source.fetch     → Daten von Connectors holen (Outlook, OneDrive, ...)\n"
         "  flow.filter      → Listen filtern\n"
         "  file.read        → Textdatei lesen\n"
         "  file.read_base64 → Binaerdatei als Base64 lesen\n"
         "discover() zeigt alle verfügbaren Brick-Kategorien.", 10),
        ("COMPOSITOR-REGEL", "COMPOSITOR-REGEL",
         "IMMER search_helpers + search_pipelines aufrufen BEVOR ein neuer Helper\n"
         "oder eine neue Pipeline erstellt wird.\n"
         "Bestehende Bausteine wiederverwenden statt duplizieren!\n"
         "1. search_helpers(query=...) — nach ähnlichen Helpers suchen\n"
         "2. search_pipelines(query=...) — nach ähnlichen Pipelines suchen\n"
         "3. Erst dann create_helper / create_pipeline aufrufen", 9),
        ("PROFILES & VARIABLES", "PROFILES & VARIABLES",
         "Profiles nutzen statt Config duplizieren: create_profile → step.profile\n"
         "Variables für Runtime-Config: set_variable → {{ var.name }} in Pipelines\n"
         "Persistent Store für Run-übergreifende Daten: store.key", 7),
        ("KERN-REGEL", "KERN-REGEL",
         "IMMER Brix MCP-Tools nutzen. KEINE Workarounds. KEINE manuellen Pipeline-Dateien.\n"
         "Pipelines leben in brix.db als normale DB-Zeilen (`pipeline`, `pipeline_step`,\n"
         "`pipeline_credential`, `pipeline_input`).\n"
         "`yaml_content` ist nur Backup fuer Rollback/Export-Kompatibilitaet.\n"
         "KEIN docker exec. KEIN YAML fuer normale CRUD. KEIN Container rebuild.\n"
         "KEIN Bash(cat ~/.brix/...)       → nutze get_run_log / get_run_status\n"
         "KEIN Bash(python3 -c ...)        → nutze create_helper\n"
         "KEIN Bash(rm -f ...)             → nutze brix__delete_run / brix clean", 10),
        ("PIPELINE-PERSISTENZ", "PIPELINE-PERSISTENZ",
         "Pipelines werden DB-only gespeichert.\n"
         "YAML ist nur fuer Bundle-Export/-Import und Legacy-Kompatibilitaet.\n"
         "Normale Pipeline-CRUD nur ueber create_pipeline / update_pipeline /\n"
         "add_step / update_step / remove_step nutzen.", 9),
        ("HILFE VERFÜGBAR", "HILFE VERFÜGBAR",
         "Für Details: brix__get_help(topic)\n"
         "Topics: 'quick-start', 'step-types', 'step-referenzen', 'helper-scripts',\n"
         "        'debugging', 'credentials', 'versioning', 'alerting', 'triggers',\n"
         "        'advanced-features', 'foreach', 'flow-control',\n"
         "        'brick-first', 'db-bricks', 'llm-bricks', 'source-bricks',\n"
         "        'resilience', 'variables', 'profiles', 'testing'", 6),
        ("STEP-OUTPUT REFERENZIEREN", "STEP-OUTPUT REFERENZIEREN",
         "{{ step_id.output }}        ✅  ganzer Step-Output\n"
         "{{ step_id.output.field }}  ✅  einzelnes Feld\n"
         "{{ input.param }}           ✅  Pipeline-Input-Parameter\n"
         "{{ item }} / {{ item.x }}   ✅  foreach-Element\n"
         "{{ step_id.results }}       ✅  foreach-Items (selectattr/map)\n"
         "{{ steps.step_id.data }}    ❌  FALSCH: kein 'steps.' Prefix, kein 'data'!\n"
         "{{ step_id.data }}          ❌  FALSCH: Feld heißt 'output', nicht 'data'!", 8),
        ("COMPOSITOR-MODE", "COMPOSITOR-MODE (T-BRIX-V8-07)",
         "Pipelines mit compositor_mode: true erlauben KEIN python/cli.\n"
         "Nutze Bricks und mcp_call statt Custom-Code.\n"
         "Override möglich: allow_code: true auf Pipeline-Ebene.\n"
         "compose_pipeline(compositor_mode=true) → LLM-sichere Brick-only Pipeline.", 7),
        ("TOP-5 ANTI-PATTERNS", "TOP-5 ANTI-PATTERNS",
         "delete_pipeline + create_pipeline   →  update_step / update_pipeline / add_step\n"
         "Pipeline-YAML als Persistence sehen →  falsch: normale CRUD arbeitet auf DB-Zeilen\n"
         "YAML manuell schreiben              →  nur noch fuer Import/Export/Restore-Kompatibilitaet\n"
         "brix run via Bash                   →  brix__run_pipeline\n"
         "base64 in foreach-Loops             →  Dateipfade als Strings uebergeben\n"
         "concurrency: '{{ input.n }}'        →  concurrency muss int sein (kein Jinja2!)", 9),
        ("architecture", "DB-only Pipeline Persistence",
         "Pipeline-CRUD arbeitet auf DB-Zeilen, nicht auf Pipeline-Dateien.\n"
         "Source of Truth:\n"
         "  - pipeline\n"
         "  - pipeline_step\n"
         "  - pipeline_credential\n"
         "  - pipeline_input\n"
         "`yaml_content` bleibt als Backup/Mirror fuer Rollback und Export erhalten.\n"
         "Wenn du Pipeline-Inhalt sehen oder aendern willst:\n"
         "  - get_pipeline / list_pipelines\n"
         "  - update_pipeline / add_step / update_step / remove_step", 10),
        ("architecture", "BRIX_STEP_SOURCE Toggle",
         "BRIX_STEP_SOURCE steuert, wo Step-Definitionen gelesen werden:\n"
         "  db   = DB-Zeilen sind aktiv\n"
         "  dual = DB-Zeilen + Vergleich mit yaml_content\n"
         "  yaml = Legacy-Leseweg aus yaml_content\n"
         "Nutze im Normalbetrieb `db`.\n"
         "`dual` ist fuer Paritaetschecks waehrend Migration/Debugging.\n"
         "`yaml` nur fuer Legacy-Faelle.", 6),
        ("DEBUGGING", "DEBUGGING",
         "Bei Fehler: brix__get_run_errors(run_id) → LLM-optimierte Fehleranalyse\n"
         "Dann:       brix__diagnose_run(run_id)   → Schritt-für-Schritt-Diagnose\n"
         "Auto-Fix:   brix__auto_fix_step(run_id, step_id) → ModuleNotFoundError / Timeout / UndefinedError", 8),
        ("TOOL-KATEGORIEN", "TOOL-KATEGORIEN",
         "Standalone:  create_pipeline / update_pipeline / list_pipelines / get_pipeline / search_pipelines\n"
         "             create_helper / update_helper / list_helpers / get_helper / search_helpers\n"
         "             run_pipeline / get_run_status / get_run_errors / get_run_log / cancel_run\n"
         "             add_step / get_step / update_step / remove_step\n"
         "             compose_pipeline / plan_pipeline / get_tips / get_help / discover / health\n"
         "Konsolidiert (action-Parameter!):\n"
         "  brix__trigger(action: add/get/list/update/delete/test)\n"
         "  brix__alert(action: add/list/update/delete/history)\n"
         "  brix__credential(action: add/get/list/update/delete/rotate/search)\n"
         "  brix__server(action: add/list/update/remove/health/refresh)\n"
         "  brix__state(action: get/set/list/delete)\n"
         "  brix__trigger_group(action: add/get/list/start/stop/delete/update)\n"
         "  brix__registry(action: add/get/list/update/delete/search)\n"
         "  brix__org(action: create/list/delete/seed)\n"
         "WICHTIG: NICHT trigger_update sondern trigger(action='update')!", 8),
        ("PFAD-KONVENTION", "PFAD-KONVENTION",
         "Host /root/... → Brix /host/root/... (Container-Dateisystem-Präfix!)", 7),
    ]

    now = _now_iso()
    # Try new singular name first, fall back to old plural for compat
    table = "tip"
    with db._connect() as conn:
        try:
            conn.execute("SELECT 1 FROM tip LIMIT 1")
        except Exception:
            table = "tips"
        for category, title, content, priority in tips:
            conn.execute(
                f"""INSERT INTO {table} (id, category, title, content, priority, is_active, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 1, ?, ?)""",
                (str(uuid4()), category, title, content, priority, now, now),
            )


def _backfill_pipeline_helpers(db: "BrixDB") -> None:
    """Backfill pipeline_helpers join table for all pipelines with yaml_content."""
    import yaml as _yaml
    with db._connect() as conn:
        # Try new singular name first, fall back to old plural for compat
        try:
            rows = conn.execute(
                "SELECT id, name, yaml_content FROM pipeline WHERE yaml_content IS NOT NULL AND yaml_content != ''"
            ).fetchall()
        except Exception:
            rows = conn.execute(
                "SELECT id, name, yaml_content FROM pipelines WHERE yaml_content IS NOT NULL AND yaml_content != ''"
            ).fetchall()
        for row in rows:
            pipeline_id, pipeline_name, yaml_content = row[0], row[1], row[2]
            try:
                raw = _yaml.safe_load(yaml_content) or {}
            except Exception:
                continue
            db._sync_pipeline_helpers(conn, pipeline_id, raw)


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

def _migrations_table_name(conn) -> str:
    """Return the actual name of the migrations tracking table.

    Checks for both old ('schema_migrations') and new ('schema_migration')
    names to support the transition period during T-BRIX-DEBT-01.
    """
    if _table_exists(conn, "schema_migration"):
        return "schema_migration"
    return "schema_migrations"


def ensure_migrations_table(db: "BrixDB") -> None:
    """Create the schema_migration tracking table if it does not exist.

    The table stores one row per applied migration with the version number,
    migration name, and ISO-8601 timestamp of when it was applied.
    Supports both old ('schema_migrations') and new ('schema_migration') names.
    """
    with db._connect() as conn:
        name = _migrations_table_name(conn)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {name} (
                version     INTEGER PRIMARY KEY,
                name        TEXT NOT NULL,
                applied_at  TEXT NOT NULL
            )
            """
        )


def get_current_version(db: "BrixDB") -> int:
    """Return the highest applied migration version (0 if none applied yet).

    Returns 0 when the schema_migration table is empty or does not exist,
    meaning the database is at the baseline state.
    """
    with db._connect() as conn:
        name = _migrations_table_name(conn)
        if not _table_exists(conn, name):
            return 0
        row = conn.execute(
            f"SELECT MAX(version) FROM {name}"
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
                    try:
                        conn.execute(up_sql)
                    except Exception as sql_exc:
                        err_msg = str(sql_exc).lower()
                        # Idempotent: skip "duplicate column" errors from ALTER TABLE
                        if "duplicate column" in err_msg:
                            logger.info("Migration v%d: column already exists, skipping", version)
                        # T-BRIX-DEBT-01: skip "no such table" errors from old migrations
                        # targeting plural table names that have been renamed to singular
                        elif "no such table" in err_msg:
                            logger.info("Migration v%d: table not found (likely renamed), skipping", version)
                        else:
                            raise

                # Support Python callables for complex migrations (T-BRIX-DBQUAL-01)
                up_fn_name = migration.get("up_fn")
                if up_fn_name:
                    fn = globals().get(up_fn_name)
                    if fn and callable(fn):
                        fn(db)

                from brix.db import _now_iso  # avoid circular at module level
                applied_at = _now_iso()
                mig_table = _migrations_table_name(conn)
                conn.execute(
                    f"INSERT OR IGNORE INTO {mig_table} (version, name, applied_at) VALUES (?, ?, ?)",
                    (version, name, applied_at),
                )
        except Exception as exc:
            logger.error("Migration v%d failed: %s", version, exc)
            raise RuntimeError(f"Migration v{version} ({name}) failed: {exc}") from exc

        applied.append({"version": version, "name": name, "applied_at": applied_at})
        logger.info("Migration v%d applied successfully", version)

    # T-BRIX-DEBT-01: After all migrations, rename schema_migrations → schema_migration
    # This must happen LAST because the migration loop above writes to schema_migrations.
    _rename_schema_migrations_table(db)

    return applied


def _rename_schema_migrations_table(db: "BrixDB") -> None:
    """Rename schema_migrations to schema_migration if not already done."""
    with db._connect() as conn:
        if _table_exists(conn, "schema_migrations") and not _table_exists(conn, "schema_migration"):
            conn.execute("ALTER TABLE schema_migrations RENAME TO schema_migration")
            logger.info("rename_tables: renamed 'schema_migrations' → 'schema_migration'")


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
        mig_table = _migrations_table_name(conn)
        row = conn.execute(
            f"SELECT version FROM {mig_table} WHERE version = ?", (version,)
        ).fetchone()
    if row is None:
        logger.warning("rollback_migration: version %d was not applied", version)
        return False

    down_sql = migration.get("down", "")
    logger.info("Rolling back migration v%d: %s", version, migration["name"])
    try:
        with db._connect() as conn:
            mig_table = _migrations_table_name(conn)
            if down_sql:
                conn.execute(down_sql)
            conn.execute(
                f"DELETE FROM {mig_table} WHERE version = ?", (version,)
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
        mig_table = _migrations_table_name(conn)
        rows = conn.execute(
            f"SELECT version, name, applied_at FROM {mig_table} ORDER BY version"
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
