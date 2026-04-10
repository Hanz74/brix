"""Brix central SQLite database — ~/.brix/brix.db

This module provides the authoritative database for Brix operational metadata:

  - runs        (migrated from history.db — pipeline run records)
  - pipelines   (DB-authored pipeline metadata)
  - helpers     (DB-authored helper registry entries)
  - pipeline_helpers  (many-to-many helper usage)
  - object_versions   (content history — prepared for T-BRIX-V5-07)
  - app_log     (application log entries — T-BRIX-V7-08)

The DB is the source of truth. Files are non-authoritative export, bundle,
backup, debug, or legacy-import artifacts only.
Sync happens:
  - On startup via BrixDB.sync_all()
  - Atomically on every create/update/delete via the per-module helpers

Migration:
  - Existing runs from history.db are imported once (idempotent)
  - registry.yaml helpers are imported once as legacy input (idempotent)
"""

from __future__ import annotations

import json
import logging
import os
import re as _re
import sqlite3
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from brix.config import config as _brix_config
from brix.serialization import json_dumps

logger = logging.getLogger(__name__)

BRIX_DB_PATH = Path(os.environ["BRIX_DB_PATH"]) if os.environ.get("BRIX_DB_PATH") else Path.home() / ".brix" / "brix.db"
HISTORY_DB_PATH = Path.home() / ".brix" / "history.db"
REGISTRY_YAML_PATH = Path.home() / ".brix" / "helpers" / "registry.yaml"
PIPELINES_DIR = Path.home() / ".brix" / "pipelines"
CONTAINER_PIPELINES_DIR = Path(_brix_config.CONTAINER_PIPELINES_DIR)

# Retention defaults (overridable via env vars)
_DEFAULT_RETENTION_DAYS = 30
_DEFAULT_RETENTION_MAX_MB = 500


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_semver(version: str) -> tuple[int, ...]:
    """Parse a semver string into a comparable tuple of ints."""
    parts: list[int] = []
    for p in version.split("."):
        try:
            parts.append(int(p))
        except (ValueError, TypeError):
            parts.append(0)
    return tuple(parts)


def _normalize_helper_ref(ref: str) -> str:
    """Normalise a helper/script reference to a bare helper name.

    Strips leading path components (``helpers/``, ``./helpers/``, ``/app/helpers/``)
    and trailing ``.py`` so that ``helpers/my_helper.py`` becomes ``my_helper``.
    """
    import posixpath
    # Take the basename to strip directory prefixes
    name = posixpath.basename(ref)
    # Strip .py extension
    if name.endswith(".py"):
        name = name[:-3]
    return name


_STEP_FIELD_TO_COLUMN: dict[str, str] = {
    "id": "step_key",
    "type": "step_type",
    "pipeline": "sub_pipeline",
    "success": "success_on_stop",
    "to": "notify_to",
    "when": "when_expr",
    "until": "until_expr",
    "foreach": "foreach_expr",
    "while_condition": "while_expr",
    "inputs": "inputs_json",
    "mode": "merge_mode",
    "key": "merge_key",
    "field": "switch_field",
    "cases": "cases_json",
    "default": "switch_default",
}

_STEP_COLUMN_TO_FIELD: dict[str, str] = {
    value: key for key, value in _STEP_FIELD_TO_COLUMN.items() if key != "success"
}

_STEP_JSON_COLUMNS: set[str] = {
    "headers_json",
    "body_json",
    "args_json",
    "inputs_json",
    "choices_json",
    "cases_json",
    "default_steps_json",
    "pipelines_json",
    "shared_params_json",
    "sequence_json",
    "sub_steps_json",
    "values_json",
    "params_json",
    "requirements_json",
    "input_schema_json",
    "output_schema_json",
    "rules_json",
    "config_json",
    "depends_on_json",
    "cache_json",
    "circuit_breaker_json",
    "rate_limit_json",
    "compensate_json",
    "data_json",
}

_STEP_BOOL_COLUMNS: set[str] = {
    "enabled",
    "shell",
    "persist",
    "success_on_stop",
    "parallel",
    "flat_output",
    "fetch_all_pages",
    "progress",
    "persist_output",
    "pause_before",
    "persist_data",
    "stream",
    "unwrap_json",
}

_STEP_STRUCTURAL_COLUMNS: set[str] = {
    "pipeline_id",
    "parent_step_id",
    "container",
    "branch_key",
    "branch_when",
    "position",
    "created_at",
    "updated_at",
}

_STEP_ALLOWED_COLUMNS_EXCLUDED: set[str] = _STEP_STRUCTURAL_COLUMNS | {"id"}

_PIPELINE_JSON_COLUMNS: dict[str, str] = {
    "template_params_json": "template_params",
    "blueprint_params_json": "blueprint_params",
    "error_handling_json": "error_handling",
    "retry_profiles_json": "retry_profiles",
    "notify_json": "notify",
    "groups_json": "groups",
    "output_json": "output",
    "output_slots_json": "output_slots",
    "requirements_json": "requirements",
}

_PIPELINE_BOOL_COLUMNS: dict[str, str] = {
    "is_template": "is_template",
    "compositor_mode": "compositor_mode",
    "allow_code": "allow_code",
    "strict_bricks": "strict_bricks",
    "test_mode": "test_mode",
}


def _json_dumps(value: Any) -> str:
    return json_dumps(value)


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


_SPECIALIST_STEP_TYPES = {"specialist", "extract.specialist"}

def _step_config_top_level_fields() -> tuple[str, ...]:
    """Return Step model fields that may be promoted from config to top-level."""
    from brix.models import Step

    return tuple(
        field_name
        for field_name in Step.model_fields.keys()
        if field_name not in {"id", "type", "config", "params"}
    )


_STEP_CONFIG_TOP_LEVEL_FIELDS: tuple[str, ...] = _step_config_top_level_fields()


def merge_step_config_into_params(step: dict[str, Any]) -> dict[str, Any]:
    """Merge DB-backed ``config`` into runner-facing step fields.

    DB-backed brick steps persist generic config in ``config_json`` / ``step.config``,
    while most engine paths and runners read ``step.params``. On the read path we
    preserve ``config`` but also expose the same keys via ``params`` when:

    - ``config`` is a non-empty dict
    - ``params`` is empty / missing
    - the step is not a specialist step, where ``config`` is semantic input

    Independently of that legacy fallback, a nested ``config.params`` value is
    surfaced through ``step.params`` for all step types:

    - ``dict``: merged into ``step.params`` with ``config.params`` taking
      precedence
    - ``list``: replaces ``step.params`` directly
    - ``None``: leaves ``step.params`` unchanged

    This lets DB-backed steps persist runner arguments under ``config.params``
    while still exposing them through ``step.params``.

    Additionally, any dedicated top-level Step field stored inside
    ``step.config`` is promoted back to ``step.<field>``. This keeps DB
    readback aligned with the engine's Step-model-driven config handling.
    Config remains the source of truth and overrides any existing top-level
    value, including create-time defaults from the Step model / DB schema.
    """
    step_type = step.get("type")
    config = step.get("config")
    params = step.get("params")
    nested_config_params = config.get("params") if isinstance(config, dict) else None

    if (
        step_type not in _SPECIALIST_STEP_TYPES
        and isinstance(config, dict)
        and config
        and not isinstance(nested_config_params, dict)
        and not params
    ):
        step["params"] = dict(config)
        params = step["params"]

    if isinstance(nested_config_params, list):
        step["params"] = list(nested_config_params)
    elif isinstance(nested_config_params, dict):
        base_params = params if isinstance(params, dict) else {}
        step["params"] = {**base_params, **nested_config_params}

    # Promote any dedicated Step field stored inside config so runners and the
    # engine see the same top-level shape they would get from YAML loading.
    if isinstance(config, dict):
        for field in _STEP_CONFIG_TOP_LEVEL_FIELDS:
            if config.get(field) is not None:
                step[field] = config[field]

    return step


def step_dict_to_row(step_dict: dict) -> dict:
    """Convert a step dict using model field names into DB column names."""
    row: dict[str, Any] = {}
    for key, value in step_dict.items():
        column = _STEP_FIELD_TO_COLUMN.get(key)
        if column is None and f"{key}_json" in _STEP_JSON_COLUMNS:
            column = f"{key}_json"
        if column is None:
            if key in _STEP_ALLOWED_COLUMNS:
                column = key
            else:
                logger.debug("step_dict_to_row: skipping unknown step field '%s'", key)
                continue
        if column in _STEP_JSON_COLUMNS:
            row[column] = _json_dumps(value)
        elif column in _STEP_BOOL_COLUMNS:
            row[column] = None if value is None else int(bool(value))
        else:
            row[column] = value
    return row


def step_row_to_dict(row: dict) -> dict:
    """Convert a DB step row into a step dict using model field names."""
    step: dict[str, Any] = {}
    has_config_json = "config_json" in row
    parsed_config = _json_loads(row.get("config_json")) if has_config_json else None

    for key, value in row.items():
        if key == "id":
            continue
        if key in _STEP_STRUCTURAL_COLUMNS:
            continue
        if key == "config" and has_config_json:
            # Prefer the raw DB JSON column when both shapes are present.
            continue
        if key in _STEP_JSON_COLUMNS and key.endswith("_json"):
            field = key[:-5]
        else:
            field = _STEP_COLUMN_TO_FIELD.get(key, key)
        if key in _STEP_JSON_COLUMNS:
            step[field] = _json_loads(value)
        elif key in _STEP_BOOL_COLUMNS:
            step[field] = None if value is None else bool(value)
        else:
            step[field] = value
    if has_config_json:
        step["config"] = parsed_config
    return merge_step_config_into_params(step)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_PIPELINE_INPUT_DDL = """
    CREATE TABLE IF NOT EXISTS pipeline_input (
        pipeline_id   TEXT NOT NULL,
        input_key     TEXT NOT NULL,
        type          TEXT NOT NULL,
        default_json  TEXT,
        description   TEXT,
        PRIMARY KEY (pipeline_id, input_key),
        FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE
    )
"""

_PIPELINE_CREDENTIAL_DDL = """
    CREATE TABLE IF NOT EXISTS pipeline_credential (
        pipeline_id   TEXT NOT NULL,
        alias         TEXT NOT NULL,
        env_ref       TEXT NOT NULL,
        refresh_json  TEXT,
        PRIMARY KEY (pipeline_id, alias),
        FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE
    )
"""

_PIPELINE_STEP_DDL = """
    CREATE TABLE IF NOT EXISTS pipeline_step (
        id                   TEXT PRIMARY KEY,
        pipeline_id          TEXT NOT NULL,
        step_key             TEXT NOT NULL,
        parent_step_id       TEXT,
        container            TEXT NOT NULL DEFAULT 'steps',
        branch_key           TEXT,
        branch_when          TEXT,
        position             INTEGER NOT NULL,
        step_type            TEXT NOT NULL,
        enabled              INTEGER NOT NULL DEFAULT 1,
        script               TEXT,
        helper               TEXT,
        url                  TEXT,
        method               TEXT DEFAULT 'GET',
        headers_json         TEXT,
        body_json            TEXT,
        command              TEXT,
        args_json            TEXT,
        shell                INTEGER NOT NULL DEFAULT 0,
        server               TEXT,
        tool                 TEXT,
        sub_pipeline         TEXT,
        inputs_json          TEXT,
        merge_mode           TEXT,
        merge_key            TEXT,
        switch_field         TEXT,
        cases_json           TEXT,
        switch_default       TEXT,
        try_step             TEXT,
        handler_step         TEXT,
        pipelines_json       TEXT,
        shared_params_json   TEXT NOT NULL DEFAULT '{}',
        values_json          TEXT,
        persist              INTEGER NOT NULL DEFAULT 0,
        message              TEXT,
        success_on_stop      INTEGER NOT NULL DEFAULT 1,
        channel              TEXT,
        notify_to            TEXT,
        approval_timeout     TEXT DEFAULT '24h',
        on_timeout           TEXT DEFAULT 'stop',
        choices_json         TEXT,
        default_steps_json   TEXT,
        until_expr           TEXT,
        while_expr           TEXT,
        max_iterations       INTEGER NOT NULL DEFAULT 100,
        delay                REAL,
        sequence_json        TEXT,
        sub_steps_json       TEXT,
        params_json          TEXT,
        foreach_expr         TEXT,
        parallel             INTEGER NOT NULL DEFAULT 0,
        concurrency          INTEGER NOT NULL DEFAULT 10,
        batch_size           INTEGER NOT NULL DEFAULT 0,
        flat_output          INTEGER NOT NULL DEFAULT 0,
        when_expr            TEXT,
        else_of              TEXT,
        on_error             TEXT,
        retry_profile        TEXT,
        timeout              TEXT,
        fetch_all_pages      INTEGER NOT NULL DEFAULT 0,
        progress             INTEGER NOT NULL DEFAULT 0,
        requirements_json    TEXT NOT NULL DEFAULT '[]',
        input_schema_json    TEXT NOT NULL DEFAULT '{}',
        output_schema_json   TEXT NOT NULL DEFAULT '{}',
        rules_json           TEXT,
        config_json          TEXT,
        depends_on_json      TEXT NOT NULL DEFAULT '[]',
        cache_json           TEXT,
        circuit_breaker_json TEXT,
        rate_limit_json      TEXT,
        compensate_json      TEXT,
        persist_output       INTEGER NOT NULL DEFAULT 0,
        pause_before         INTEGER NOT NULL DEFAULT 0,
        persist_data         INTEGER NOT NULL DEFAULT 1,
        profile              TEXT,
        queue_name           TEXT,
        collect_until        INTEGER,
        collect_for          TEXT,
        flush_to             TEXT,
        event                TEXT,
        data_json            TEXT,
        stream               INTEGER NOT NULL DEFAULT 0,
        unwrap_json          INTEGER,
        created_at           TEXT NOT NULL,
        updated_at           TEXT NOT NULL,
        FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
        FOREIGN KEY (parent_step_id) REFERENCES pipeline_step(id) ON DELETE CASCADE,
        UNIQUE (pipeline_id, step_key)
    )
"""

_PIPELINE_STEP_INDEX_DDL = [
    """
    CREATE INDEX IF NOT EXISTS idx_pipeline_step_pipeline_id
        ON pipeline_step (pipeline_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pipeline_step_parent_container_position
        ON pipeline_step (parent_step_id, container, position)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pipeline_step_pipeline_step_key
        ON pipeline_step (pipeline_id, step_key)
    """,
]

_PIPELINE_STEP_COLUMNS: frozenset[str] = frozenset()

_STEP_ALLOWED_COLUMNS: frozenset[str] = frozenset()

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS run (
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
        notes        TEXT,
        cost_usd     REAL,
        idempotency_key TEXT,
        cancel_reason TEXT,
        cancelled_by TEXT,
        environment_json TEXT,
        container_id TEXT,
        project      TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline (
        id           TEXT PRIMARY KEY,
        name         TEXT NOT NULL UNIQUE,
        path         TEXT NOT NULL,
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL,
        version      TEXT DEFAULT '1.0.0',
        brix_version TEXT,
        kind         TEXT,
        extends      TEXT,
        is_template  INTEGER NOT NULL DEFAULT 0,
        compositor_mode INTEGER NOT NULL DEFAULT 0,
        allow_code   INTEGER NOT NULL DEFAULT 1,
        strict_bricks INTEGER NOT NULL DEFAULT 0,
        test_mode    INTEGER NOT NULL DEFAULT 0,
        idempotency_key TEXT,
        template_params_json TEXT NOT NULL DEFAULT '{}',
        blueprint_params_json TEXT NOT NULL DEFAULT '[]',
        error_handling_json TEXT NOT NULL DEFAULT '{}',
        retry_profiles_json TEXT NOT NULL DEFAULT '{}',
        notify_json TEXT NOT NULL DEFAULT '{}',
        groups_json TEXT NOT NULL DEFAULT '{}',
        output_json TEXT,
        output_slots_json TEXT NOT NULL DEFAULT '{}',
        migration_status TEXT DEFAULT NULL,
        requirements_json TEXT DEFAULT '[]',
        yaml_content TEXT DEFAULT '',
        project      TEXT DEFAULT '',
        tags         TEXT DEFAULT '[]',
        group_name   TEXT DEFAULT '',
        description  TEXT DEFAULT ''
    )
    """,
    _PIPELINE_INPUT_DDL,
    _PIPELINE_CREDENTIAL_DDL,
    _PIPELINE_STEP_DDL,
    *_PIPELINE_STEP_INDEX_DDL,
    """
    CREATE TABLE IF NOT EXISTS helper (
        id               TEXT PRIMARY KEY,
        name             TEXT NOT NULL UNIQUE,
        script_path      TEXT NOT NULL,
        description      TEXT DEFAULT '',
        requirements_json TEXT DEFAULT '[]',
        input_schema_json TEXT DEFAULT '{}',
        output_schema_json TEXT DEFAULT '{}',
        created_at       TEXT NOT NULL,
        updated_at       TEXT NOT NULL,
        code             TEXT DEFAULT '',
        content_hash     TEXT DEFAULT '',
        project          TEXT DEFAULT '',
        tags             TEXT DEFAULT '[]',
        group_name       TEXT DEFAULT '',
        imports_json     TEXT DEFAULT '[]'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_helper (
        pipeline_id  TEXT NOT NULL,
        helper_id    TEXT NOT NULL,
        PRIMARY KEY (pipeline_id, helper_id),
        FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
        FOREIGN KEY (helper_id)   REFERENCES helper(id)   ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS object_version (
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
    CREATE TABLE IF NOT EXISTS agent_session (
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
    CREATE TABLE IF NOT EXISTS resource_lock (
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
    CREATE TABLE IF NOT EXISTS step_output (
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
    CREATE TABLE IF NOT EXISTS alert_rule (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        condition   TEXT NOT NULL,
        channel     TEXT NOT NULL,
        config      TEXT NOT NULL DEFAULT '{}',
        enabled     INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT NOT NULL,
        project     TEXT DEFAULT '',
        tags        TEXT DEFAULT '[]',
        group_name  TEXT DEFAULT '',
        description TEXT DEFAULT ''
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
    # T-BRIX-MOD-02: Trigger tables (consolidated from trigger/store.py)
    """
    CREATE TABLE IF NOT EXISTS trigger (
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
        last_status   TEXT,
        project     TEXT DEFAULT '',
        tags        TEXT DEFAULT '[]',
        group_name  TEXT DEFAULT '',
        description TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trigger_group (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL DEFAULT '',
        triggers_json TEXT NOT NULL DEFAULT '[]',
        enabled     INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        project     TEXT DEFAULT '',
        tags        TEXT DEFAULT '[]',
        group_name  TEXT DEFAULT ''
    )
    """,
    # T-BRIX-MOD-03: Trigger state tables (consolidated from trigger/state.py)
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
    CREATE TABLE IF NOT EXISTS pipeline_event (
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
    CREATE TABLE IF NOT EXISTS registry_template (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        project     TEXT DEFAULT '',
        group_name  TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_pattern (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        project     TEXT DEFAULT '',
        group_name  TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_schema (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        project     TEXT DEFAULT '',
        group_name  TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_error_pattern (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        project     TEXT DEFAULT '',
        group_name  TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_best_practice (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        project     TEXT DEFAULT '',
        group_name  TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS registry_lesson_learned (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        content     TEXT NOT NULL DEFAULT '{}',
        tags        TEXT NOT NULL DEFAULT '[]',
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        project     TEXT DEFAULT '',
        group_name  TEXT DEFAULT ''
    )
    """,
    # T-BRIX-DB-05b: Named DB-Connections
    """
    CREATE TABLE IF NOT EXISTS connection (
        id                TEXT PRIMARY KEY,
        name              TEXT UNIQUE NOT NULL,
        driver            TEXT NOT NULL DEFAULT 'postgresql',
        dsn_credential_id TEXT,
        env_var           TEXT,
        description       TEXT DEFAULT '',
        created_at        TEXT NOT NULL,
        updated_at        TEXT,
        project           TEXT DEFAULT '',
        tags              TEXT DEFAULT '[]',
        group_name        TEXT DEFAULT ''
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
    CREATE TABLE IF NOT EXISTS brick_definition (
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
        updated_at TEXT,
        org_tags TEXT DEFAULT '[]',
        project TEXT DEFAULT '',
        group_name TEXT DEFAULT '',
        tags TEXT DEFAULT '[]'
    )
    """,
    # T-BRIX-DB-06: DB-First — connector_definitions
    """
    CREATE TABLE IF NOT EXISTS connector_definition (
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
        updated_at TEXT,
        project TEXT DEFAULT '',
        tags TEXT DEFAULT '[]',
        group_name TEXT DEFAULT ''
    )
    """,
    # T-BRIX-DB-06: DB-First — mcp_tool_schemas
    """
    CREATE TABLE IF NOT EXISTS mcp_tool_schema (
        name TEXT PRIMARY KEY,
        description TEXT DEFAULT '',
        input_schema TEXT DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """,
    # T-BRIX-FDB-01: MCP servers stored in DB
    """
    CREATE TABLE IF NOT EXISTS mcp_server (
        name TEXT PRIMARY KEY,
        command TEXT NOT NULL DEFAULT '',
        args_json TEXT DEFAULT '[]',
        env_json TEXT DEFAULT '{}',
        tools_prefix TEXT DEFAULT '',
        transport TEXT DEFAULT 'stdio',
        url TEXT DEFAULT '',
        unwrap_json INTEGER DEFAULT 0,
        description TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    # T-BRIX-DB-06: DB-First — help_topics
    """
    CREATE TABLE IF NOT EXISTS help_topic (
        name TEXT PRIMARY KEY,
        title TEXT DEFAULT '',
        content TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """,
    # T-BRIX-DB-06: DB-First — keyword_taxonomies
    """
    CREATE TABLE IF NOT EXISTS keyword_taxonomy (
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
    CREATE TABLE IF NOT EXISTS step_execution (
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
        created_at TEXT NOT NULL,
        last_progress TEXT DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS foreach_item_execution (
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
    CREATE TABLE IF NOT EXISTS run_input (
        run_id TEXT PRIMARY KEY,
        input_params TEXT DEFAULT '{}',
        trigger_data TEXT DEFAULT '{}',
        created_at TEXT NOT NULL
    )
    """,
    # T-BRIX-DB-13: Managed Variables
    """
    CREATE TABLE IF NOT EXISTS variable (
        name TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        description TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT,
        secret INTEGER DEFAULT 0,
        project TEXT DEFAULT '',
        tags TEXT DEFAULT '[]',
        group_name TEXT DEFAULT ''
    )
    """,
    # T-BRIX-DB-13: Persistent Data Store
    """
    CREATE TABLE IF NOT EXISTS persistent_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        pipeline_name TEXT DEFAULT '',
        updated_at TEXT NOT NULL,
        project TEXT DEFAULT '',
        tags TEXT DEFAULT '[]',
        group_name TEXT DEFAULT ''
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
    CREATE TABLE IF NOT EXISTS profile (
        name        TEXT PRIMARY KEY,
        config      TEXT NOT NULL DEFAULT '{}',
        description TEXT DEFAULT '',
        created_at  TEXT NOT NULL,
        updated_at  TEXT,
        project     TEXT DEFAULT '',
        tags        TEXT DEFAULT '[]',
        group_name  TEXT DEFAULT ''
    )
    """,
    # T-BRIX-FDB-02: Environment profiles stored in DB
    """
    CREATE TABLE IF NOT EXISTS env_profile (
        name TEXT PRIMARY KEY,
        is_default INTEGER DEFAULT 0,
        env_json TEXT DEFAULT '{}',
        input_defaults_json TEXT DEFAULT '{}',
        description TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    # T-BRIX-DB-24: Step Pins — Mock-Daten für Testing
    """
    CREATE TABLE IF NOT EXISTS step_pin (
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
    # T-BRIX-CHANGELOG-01: Changelog entries
    """
    CREATE TABLE IF NOT EXISTS changelog_entry (
        id          TEXT PRIMARY KEY,
        version     TEXT NOT NULL,
        timestamp   TEXT NOT NULL,
        type        TEXT NOT NULL CHECK(type IN ('breaking','feature','fix','refactor','docs')),
        title       TEXT NOT NULL,
        description TEXT DEFAULT '',
        task_id     TEXT,
        commit_sha  TEXT,
        created_at  TEXT NOT NULL
    )
    """,
]

_PIPELINE_STEP_COLUMNS = frozenset(
    _re.findall(r"^\s+([a-z_][a-z0-9_]*)\s+", _PIPELINE_STEP_DDL, flags=_re.MULTILINE)
)

_STEP_ALLOWED_COLUMNS = frozenset(
    column for column in _PIPELINE_STEP_COLUMNS
    if column not in _STEP_ALLOWED_COLUMNS_EXCLUDED
)

_KNOWN_TABLES: frozenset[str] = frozenset(
    _re.findall(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)", " ".join(_DDL))
) | frozenset({
    # Tables created via migrations (not in _DDL)
    "tip", "schema_migration", "changelog_entry",
})


def _safe_table(name: str) -> str:
    """Validate *name* against the allowlist of known tables.

    Returns *name* unchanged when valid; raises ``ValueError`` for anything
    else.  This is defence-in-depth — callers already resolve table names via
    internal dicts, but the explicit check silences static-analysis warnings
    about SQL string interpolation **and** adds a hard runtime guard.
    """
    if name not in _KNOWN_TABLES:
        raise ValueError(
            f"Unknown table '{name}'. "
            f"Allowed tables: {', '.join(sorted(_KNOWN_TABLES))}"
        )
    return name


# Valid registry type names → table names mapping (T-BRIX-V7-10)
REGISTRY_TYPES: dict[str, str] = {
    "templates": "registry_template",
    "patterns": "registry_pattern",
    "schemas": "registry_schema",
    "error_patterns": "registry_error_pattern",
    "best_practices": "registry_best_practice",
    "lessons_learned": "registry_lesson_learned",
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
    db.sync_all()            # import legacy files into DB-owned state
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
        cursor = conn.execute(f"PRAGMA table_info({_safe_table(table)})")
        return any(row[1] == column for row in cursor.fetchall())

    # T-BRIX-DEBT-01: new singular → old plural mapping for DDL skip logic
    _SINGULAR_TO_PLURAL: dict[str, str] = {
        "agent_session": "agent_sessions",
        "alert_rule": "alert_rules",
        "brick_definition": "brick_definitions",
        "connection": "connections",
        "connector_definition": "connector_definitions",
        "foreach_item_execution": "foreach_item_executions",
        "help_topic": "help_topics",
        "helper": "helpers",
        "keyword_taxonomy": "keyword_taxonomies",
        "mcp_tool_schema": "mcp_tool_schemas",
        "object_version": "object_versions",
        "pipeline_event": "pipeline_events",
        "pipeline_helper": "pipeline_helpers",
        "pipeline": "pipelines",
        "profile": "profiles",
        "registry_best_practice": "registry_best_practices",
        "registry_error_pattern": "registry_error_patterns",
        "registry_lesson_learned": "registry_lessons_learned",
        "registry_pattern": "registry_patterns",
        "registry_schema": "registry_schemas",
        "registry_template": "registry_templates",
        "resource_lock": "resource_locks",
        "run_input": "run_inputs",
        "run": "runs",
        "step_execution": "step_executions",
        "step_output": "step_outputs",
        "step_pin": "step_pins",
        "trigger_group": "trigger_groups",
        "trigger": "triggers",
        "variable": "variables",
    }

    def _init_schema(self) -> None:
        with self._connect() as conn:
            # T-BRIX-DEBT-01: Detect pre-v63 DB (has old plural table names).
            # If old tables exist, skip creating new singular ones — migration v63
            # will rename old -> new.  On fresh DB, old tables don't exist so DDL
            # creates the new singular tables directly.
            _pre_v63 = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='runs'"
            ).fetchone()[0] > 0

            for ddl in _DDL:
                if _pre_v63:
                    m = _re.search(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)", ddl)
                    if m:
                        new_name = m.group(1)
                        old_name = self._SINGULAR_TO_PLURAL.get(new_name)
                        if old_name:
                            old_exists = conn.execute(
                                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                                (old_name,),
                            ).fetchone()[0] > 0
                            if old_exists:
                                continue  # Skip — migration v63 will rename the old table
                conn.execute(ddl)
            # Idempotent migration: add notes column if not present (v5.1+)
            try:
                conn.execute("ALTER TABLE run ADD COLUMN notes TEXT")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: add cost_usd column if not present (v6.21+)
            try:
                conn.execute("ALTER TABLE run ADD COLUMN cost_usd REAL")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: add idempotency_key column (T-BRIX-V6-22)
            try:
                conn.execute("ALTER TABLE run ADD COLUMN idempotency_key TEXT")
            except Exception:
                pass  # Column already exists — ignore
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_runs_idempotency_key "
                    "ON run (idempotency_key, started_at)"
                )
            except Exception:
                pass
            # Idempotent migration: add cancel columns (T-BRIX-V6-BUG-03)
            try:
                conn.execute("ALTER TABLE run ADD COLUMN cancel_reason TEXT")
            except Exception:
                pass  # Column already exists — ignore
            try:
                conn.execute("ALTER TABLE helper ADD COLUMN content_hash TEXT DEFAULT ''")
            except Exception:
                pass  # Column already exists — ignore
            try:
                conn.execute("ALTER TABLE run ADD COLUMN cancelled_by TEXT")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: step_outputs index (T-BRIX-V7-04)
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_step_outputs_run_id "
                    "ON step_output (run_id)"
                )
            except Exception:
                pass
            # Idempotent migration: environment_json column (T-BRIX-V7-05)
            try:
                conn.execute("ALTER TABLE run ADD COLUMN environment_json TEXT")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: container_id column (T-BRIX-V7-07)
            try:
                conn.execute("ALTER TABLE run ADD COLUMN container_id TEXT")
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: T-BRIX-DB-07 indexes
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_step_executions_run_id "
                    "ON step_execution (run_id)"
                )
            except Exception:
                pass
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_foreach_item_executions_run_step "
                    "ON foreach_item_execution (run_id, step_id)"
                )
            except Exception:
                pass
            # Idempotent migration: last_progress column for step_executions (T-BRIX-DB-14)
            try:
                conn.execute(
                    "ALTER TABLE step_execution ADD COLUMN last_progress TEXT DEFAULT ''"
                )
            except Exception:
                pass  # Column already exists — ignore
            # Idempotent migration: secret column for variables (T-BRIX-DB-26)
            try:
                conn.execute(
                    "ALTER TABLE variable ADD COLUMN secret INTEGER DEFAULT 0"
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
                return json_dumps(value)
            except (TypeError, ValueError):
                return json_dumps(str(value))

        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO step_output
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
                "SELECT * FROM step_output WHERE run_id=? AND step_id=? ORDER BY created_at DESC LIMIT 1",
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
                "SELECT * FROM step_output WHERE run_id=? ORDER BY created_at ASC",
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
                   FROM run r
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
            from datetime import datetime, timedelta
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
            serialized = json_dumps(value)
        except (TypeError, ValueError):
            serialized = json_dumps(str(value))
        if len(serialized.encode("utf-8")) > BrixDB._MAX_DATA_BYTES:
            return json_dumps({"__truncated__": True, "label": label, "hint": "data exceeded 1MB limit"})
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
                error_str = json_dumps(error_detail)
            except (TypeError, ValueError):
                error_str = json_dumps(str(error_detail))
        else:
            error_str = ""

        try:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO step_execution
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
                error_str = json_dumps(error_detail)
            except (TypeError, ValueError):
                error_str = json_dumps(str(error_detail))
        else:
            error_str = ""

        try:
            with self._connect() as conn:
                conn.execute(
                    """INSERT INTO foreach_item_execution
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
            params_str = json_dumps(input_params) if input_params is not None else "{}"
        except (TypeError, ValueError):
            params_str = "{}"
        try:
            trigger_str = json_dumps(trigger_data) if trigger_data is not None else "{}"
        except (TypeError, ValueError):
            trigger_str = "{}"

        try:
            with self._connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO run_input (run_id, input_params, trigger_data, created_at)
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
                    "SELECT * FROM step_execution WHERE run_id=? AND step_id=? ORDER BY created_at ASC",
                    (run_id, step_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM step_execution WHERE run_id=? ORDER BY created_at ASC",
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
                    """UPDATE step_execution
                       SET last_progress = ?
                       WHERE run_id = ? AND step_id = ?
                         AND id = (
                             SELECT id FROM step_execution
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
                       FROM step_execution
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
                "SELECT * FROM foreach_item_execution WHERE run_id=? AND step_id=? ORDER BY item_index ASC",
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
                "SELECT * FROM run_input WHERE run_id=?",
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
    # Migration helper (idempotent)
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
            rows = src_conn.execute("SELECT * FROM run").fetchall()
            src_conn.close()
        except Exception:
            return 0

        imported = 0
        with self._connect() as conn:
            for row in rows:
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO run
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
                        """INSERT OR IGNORE INTO helper
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
        """Import legacy pipeline YAML files into the DB.

        File inputs are non-authoritative import sources. Existing DB rows keep
        their identity, and live authoring paths must not depend on this scan.
        Helper references are resolved for imported pipeline metadata.

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
                        "SELECT id, created_at FROM pipeline WHERE name=?", (name,)
                    ).fetchone()
                    if existing:
                        pipeline_id = existing[0]
                        created_at = existing[1]

                    conn.execute(
                        """INSERT INTO pipeline (id, name, path, created_at, updated_at, requirements_json)
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

    @staticmethod
    def _extract_helper_refs(steps: list) -> set[str]:
        """Extract all helper/script names referenced in steps, recursively.

        Looks at each step for:
        - ``helper`` (top-level field)
        - ``script`` (top-level field, basename without .py / path prefix)
        - ``params.helper`` (nested in params dict)
        - ``params.script`` (nested in params dict)

        Recurses into nested step containers:
        - repeat  -> ``sequence``
        - choose  -> ``choices[].steps`` and ``default_steps``
        - parallel -> ``sub_steps``
        """
        helper_names: set[str] = set()

        for step in steps:
            if not isinstance(step, dict):
                continue

            # Direct helper/script fields
            for field in ("helper", "script"):
                val = step.get(field)
                if val and isinstance(val, str):
                    helper_names.add(_normalize_helper_ref(val))

            # Params-level helper/script fields
            params = step.get("params")
            if isinstance(params, dict):
                for field in ("helper", "script"):
                    val = params.get(field)
                    if val and isinstance(val, str):
                        helper_names.add(_normalize_helper_ref(val))

            # Recurse into nested step containers
            # repeat -> sequence
            seq = step.get("sequence")
            if isinstance(seq, list):
                helper_names |= BrixDB._extract_helper_refs(seq)
            # choose -> choices[].steps + default_steps
            choices = step.get("choices")
            if isinstance(choices, list):
                for choice in choices:
                    if isinstance(choice, dict):
                        branch_steps = choice.get("steps")
                        if isinstance(branch_steps, list):
                            helper_names |= BrixDB._extract_helper_refs(branch_steps)
            default_steps = step.get("default_steps")
            if isinstance(default_steps, list):
                helper_names |= BrixDB._extract_helper_refs(default_steps)
            # parallel -> sub_steps
            sub_steps = step.get("sub_steps")
            if isinstance(sub_steps, list):
                helper_names |= BrixDB._extract_helper_refs(sub_steps)

        return helper_names

    def _sync_pipeline_helpers(
        self, conn: sqlite3.Connection, pipeline_id: str, raw: dict
    ) -> None:
        """Update pipeline_helper for a single pipeline.

        Scans all steps (recursively) for helper/script references and
        inserts the corresponding join-table rows.
        """
        steps = raw.get("steps", [])
        if not isinstance(steps, list):
            steps = []

        helper_names = self._extract_helper_refs(steps)

        # Always delete old links (even when no helpers found — handles removal)
        conn.execute(
            "DELETE FROM pipeline_helper WHERE pipeline_id=?", (pipeline_id,)
        )

        for hname in helper_names:
            row = conn.execute(
                "SELECT id FROM helper WHERE name=?", (hname,)
            ).fetchone()
            if row:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO pipeline_helper (pipeline_id, helper_id) VALUES (?,?)",
                        (pipeline_id, row[0]),
                    )
                except Exception:
                    pass

    def refresh_pipeline_deps(self, pipeline_name: str) -> None:
        """Refresh pipeline_helper from pipeline_step rows (DB-only, no yaml_content)."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, migration_status FROM pipeline WHERE name=?",
                (pipeline_name,),
            ).fetchone()
            if not row:
                return
            pipeline_id = row[0]
            migration_status = row[1]

            if migration_status != "v71_complete":
                # Un-migrated pipeline — skip silently and leave pipeline_helper as-is
                logger.warning(
                    "refresh_pipeline_deps: pipeline '%s' not fully migrated (status=%s), skipping",
                    pipeline_name,
                    migration_status,
                )
                return

            helper_rows = conn.execute(
                """SELECT DISTINCT helper
                   FROM pipeline_step
                   WHERE pipeline_id=? AND helper IS NOT NULL AND helper != ''""",
                (pipeline_id,),
            ).fetchall()
            conn.execute(
                "DELETE FROM pipeline_helper WHERE pipeline_id=?",
                (pipeline_id,),
            )
            for helper_row in helper_rows:
                helper_name = _normalize_helper_ref(helper_row[0])
                row = conn.execute(
                    "SELECT id FROM helper WHERE name=?",
                    (helper_name,),
                ).fetchone()
                if row:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO pipeline_helper (pipeline_id, helper_id) VALUES (?,?)",
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
        """Full sync: migrate legacy data and import file mirrors.

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
        project: Optional[str] = None,
    ) -> None:
        env_json: Optional[str] = None
        if environment is not None:
            try:
                env_json = json_dumps(environment)
            except (TypeError, ValueError):
                env_json = json_dumps(str(environment))
        # T-BRIX-SCHEMA-03: resolve project from pipeline if not provided
        resolved_project = project or ""
        if not resolved_project:
            try:
                p = self.get_pipeline(pipeline)
                if p:
                    resolved_project = p.get("project", "")
            except Exception:
                pass
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO run
                   (run_id, pipeline, version, started_at, input_data, triggered_by,
                    idempotency_key, environment_json, container_id, project)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    run_id, pipeline, version,
                    _now_iso(),
                    json_dumps(input_data) if input_data else None,
                    triggered_by,
                    idempotency_key,
                    env_json,
                    container_id,
                    resolved_project,
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
                """SELECT * FROM run
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
                """UPDATE run SET finished_at=?, duration=?, success=?,
                   steps_data=?, result_summary=?, cost_usd=? WHERE run_id=?""",
                (
                    _now_iso(), duration, int(success),
                    json_dumps(steps) if steps else None,
                    json_dumps(result_summary) if result_summary else None,
                    cost_usd,
                    run_id,
                ),
            )

    def get_run(self, run_id: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM run WHERE run_id=?", (run_id,)
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
            env_json = json_dumps(environment)
        except (TypeError, ValueError):
            env_json = json_dumps(str(environment))
        with self._connect() as conn:
            conn.execute(
                "UPDATE run SET environment_json=? WHERE run_id=?",
                (env_json, run_id),
            )

    def get_run_environment(self, run_id: str) -> Optional[dict]:
        """Return the environment snapshot for a run, or None if not recorded."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT environment_json FROM run WHERE run_id=?", (run_id,)
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
                "SELECT * FROM run ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_run(self, run_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM run WHERE run_id=?", (run_id,))
            return cursor.rowcount > 0

    def annotate_run(self, run_id: str, notes: str) -> bool:
        """Attach or replace notes on a run. Returns True if the run was found."""
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE run SET notes=? WHERE run_id=?", (notes, run_id)
            )
            return cursor.rowcount > 0

    def search_runs(
        self,
        pipeline: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: int = 50,
        project: Optional[str] = None,
    ) -> list[dict]:
        """Filter runs by pipeline name, status, project, and/or time range.

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
        project:
            Filter by project name (T-BRIX-SCHEMA-03). Omit for all projects.
        """
        clauses: list[str] = []
        params: list[Any] = []

        if pipeline:
            clauses.append("pipeline = ?")
            params.append(pipeline)

        if project is not None:
            clauses.append("project = ?")
            params.append(project)

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
                f"SELECT * FROM run {where} ORDER BY started_at DESC LIMIT ?",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def cleanup_runs(self, older_than_days: int = 30) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM run WHERE started_at < datetime('now', ?)",
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
                """UPDATE run
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
                """UPDATE run
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
                "SELECT COALESCE(SUM(cost_usd), 0.0) FROM run WHERE started_at LIKE ?",
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
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
        # yaml_content is intentionally removed — DB-First, no live yaml_content writes
        # The column is retained for rollback safety but is never written from code.
        **_ignored_kwargs: object,
    ) -> str:
        """Insert or update a pipeline index entry. Returns the pipeline id."""
        now = _now_iso()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id, created_at FROM pipeline WHERE name=?", (name,)
            ).fetchone()
            if existing:
                pid = existing[0]
                created_at = existing[1]
            else:
                pid = pipeline_id or str(uuid4())
                created_at = now

            # Build dynamic column list based on which optional columns exist
            has_project = self._column_exists(conn, "pipeline", "project")
            has_tags = self._column_exists(conn, "pipeline", "tags")
            has_group = self._column_exists(conn, "pipeline", "group_name")

            cols = ["id", "name", "path", "created_at", "updated_at", "requirements_json"]
            vals: list = [pid, name, path, created_at, now, json.dumps(requirements or [])]
            updates = [
                "path=excluded.path",
                "updated_at=excluded.updated_at",
                "requirements_json=excluded.requirements_json",
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
            col_str = ",".join(cols)
            update_str = ",".join(updates)

            conn.execute(
                f"""INSERT INTO pipeline ({col_str})
                   VALUES ({placeholders})
                   ON CONFLICT(name) DO UPDATE SET {update_str}
                """,
                vals,
            )
        return pid

    def delete_pipeline(self, name: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM pipeline WHERE name=?", (name,))
            return cursor.rowcount > 0

    def upsert_step(
        self,
        pipeline_id: str,
        step_dict: dict,
        step_order: int,
        conn: Optional[sqlite3.Connection] = None,
    ) -> str:
        """Insert or update one normalized pipeline_step row."""
        now = _now_iso()
        row = step_dict_to_row(step_dict)
        step_key = row["step_key"]

        conn_ctx = nullcontext(conn) if conn is not None else self._connect()
        with conn_ctx as active_conn:
            existing = active_conn.execute(
                "SELECT id, created_at FROM pipeline_step WHERE pipeline_id=? AND step_key=?",
                (pipeline_id, step_key),
            ).fetchone()
            row_id = existing[0] if existing else str(uuid4())
            created_at = existing[1] if existing else now

            row.update(
                {
                    "id": row_id,
                    "pipeline_id": pipeline_id,
                    "position": step_order,
                    "created_at": created_at,
                    "updated_at": now,
                }
            )

            cols = list(row.keys())
            placeholders = ",".join("?" for _ in cols)
            updates = ",".join(
                f"{col}=excluded.{col}" for col in cols if col not in {"id", "created_at"}
            )
            active_conn.execute(
                f"""INSERT INTO pipeline_step ({",".join(cols)})
                   VALUES ({placeholders})
                   ON CONFLICT(pipeline_id, step_key) DO UPDATE SET {updates}""",
                [row[col] for col in cols],
            )
        return row_id

    def get_steps(self, pipeline_id: str) -> list[dict]:
        """Return all steps for a pipeline ordered by position."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM pipeline_step WHERE pipeline_id=? ORDER BY position ASC",
                (pipeline_id,),
            ).fetchall()
        return [step_row_to_dict(dict(row)) for row in rows]

    def get_step_by_id(self, pipeline_id: str, step_id: str) -> dict | None:
        """Return one step by model-visible step id."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM pipeline_step WHERE pipeline_id=? AND step_key=?",
                (pipeline_id, step_id),
            ).fetchone()
        return step_row_to_dict(dict(row)) if row else None

    def update_step_row(self, pipeline_id: str, step_id: str, updates: dict) -> bool:
        """Partially update one pipeline_step row."""
        if not updates:
            return self.get_step_by_id(pipeline_id, step_id) is not None

        row_updates = step_dict_to_row(updates)
        if not row_updates:
            return self.get_step_by_id(pipeline_id, step_id) is not None

        row_updates["updated_at"] = _now_iso()
        assignments = ", ".join(f"{column}=?" for column in row_updates)
        values = list(row_updates.values()) + [pipeline_id, step_id]

        with self._connect() as conn:
            cursor = conn.execute(
                f"""UPDATE pipeline_step
                    SET {assignments}
                    WHERE pipeline_id=? AND step_key=?""",
                values,
            )
            return cursor.rowcount > 0

    def delete_step_row(self, pipeline_id: str, step_id: str) -> bool:
        """Delete one step row by model-visible step id."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM pipeline_step WHERE pipeline_id=? AND step_key=?",
                (pipeline_id, step_id),
            )
            return cursor.rowcount > 0

    def reorder_steps(self, pipeline_id: str, step_ids: list[str]) -> None:
        """Rewrite position for the given step ids in the provided order."""
        with self._connect() as conn:
            for position, step_id in enumerate(step_ids):
                conn.execute(
                    """UPDATE pipeline_step
                       SET position=?, updated_at=?
                       WHERE pipeline_id=? AND step_key=?""",
                    (position, _now_iso(), pipeline_id, step_id),
                )

    def upsert_pipeline_credential(
        self,
        pipeline_id: str,
        name: str,
        env: str,
        refresh: Any = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        """Insert or update one normalized pipeline credential row."""
        conn_ctx = nullcontext(conn) if conn is not None else self._connect()
        with conn_ctx as active_conn:
            active_conn.execute(
                """INSERT INTO pipeline_credential (pipeline_id, alias, env_ref, refresh_json)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(pipeline_id, alias) DO UPDATE SET
                       env_ref=excluded.env_ref,
                       refresh_json=excluded.refresh_json""",
                (pipeline_id, name, env, _json_dumps(refresh) if refresh is not None else None),
            )

    def get_pipeline_credentials(self, pipeline_id: str) -> list[dict]:
        """Return pipeline credentials ordered by alias."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT pipeline_id, alias, env_ref, refresh_json
                   FROM pipeline_credential
                   WHERE pipeline_id=?
                   ORDER BY alias ASC""",
                (pipeline_id,),
            ).fetchall()
        result: list[dict] = []
        for row in rows:
            item = dict(row)
            item["name"] = item.pop("alias")
            item["env"] = item.pop("env_ref")
            item["refresh"] = _json_loads(item.pop("refresh_json"))
            result.append(item)
        return result

    def delete_pipeline_credentials(self, pipeline_id: str) -> int:
        """Delete all credentials for a pipeline."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM pipeline_credential WHERE pipeline_id=?",
                (pipeline_id,),
            )
            return cursor.rowcount

    def upsert_pipeline_input(
        self,
        pipeline_id: str,
        name: str,
        param_type: str,
        default_value: Any = None,
        description: Optional[str] = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> None:
        """Insert or update one normalized pipeline input row."""
        conn_ctx = nullcontext(conn) if conn is not None else self._connect()
        with conn_ctx as active_conn:
            active_conn.execute(
                """INSERT INTO pipeline_input (pipeline_id, input_key, type, default_json, description)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(pipeline_id, input_key) DO UPDATE SET
                       type=excluded.type,
                       default_json=excluded.default_json,
                       description=excluded.description""",
                (
                    pipeline_id,
                    name,
                    param_type,
                    _json_dumps(default_value) if default_value is not None else None,
                    description,
                ),
            )

    def get_pipeline_inputs(self, pipeline_id: str) -> list[dict]:
        """Return pipeline inputs ordered by input key."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT pipeline_id, input_key, type, default_json, description
                   FROM pipeline_input
                   WHERE pipeline_id=?
                   ORDER BY input_key ASC""",
                (pipeline_id,),
            ).fetchall()
        result: list[dict] = []
        for row in rows:
            item = dict(row)
            item["name"] = item.pop("input_key")
            item["default"] = _json_loads(item.pop("default_json"))
            result.append(item)
        return result

    def delete_pipeline_inputs(self, pipeline_id: str) -> int:
        """Delete all input rows for a pipeline."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM pipeline_input WHERE pipeline_id=?",
                (pipeline_id,),
            )
            return cursor.rowcount

    def pipeline_to_dict(self, pipeline_id: str) -> dict | None:
        """Reconstruct a pipeline dict from normalized DB rows."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            pipeline_row = conn.execute(
                "SELECT * FROM pipeline WHERE id=?",
                (pipeline_id,),
            ).fetchone()

        if not pipeline_row:
            return None

        row = dict(pipeline_row)
        result: dict[str, Any] = {
            "name": row["name"],
            "version": row.get("version") or "1.0.0",
            "description": row.get("description"),
            "brix_version": row.get("brix_version"),
            "kind": row.get("kind"),
            "extends": row.get("extends"),
            "idempotency_key": row.get("idempotency_key"),
            "project": row.get("project", ""),
        }

        for column, field in _PIPELINE_BOOL_COLUMNS.items():
            result[field] = bool(row.get(column))
        for column, field in _PIPELINE_JSON_COLUMNS.items():
            fallback = [] if column in {"blueprint_params_json", "requirements_json"} else {}
            if column == "output_json":
                fallback = None
            value = _json_loads(row.get(column))
            result[field] = fallback if value is None else value

        raw_tags = row.get("tags") or "[]"
        result["tags"] = _json_loads(raw_tags) if isinstance(raw_tags, str) else raw_tags
        if row.get("group_name"):
            result["group"] = row["group_name"]

        inputs: dict[str, Any] = {}
        for item in self.get_pipeline_inputs(pipeline_id):
            inputs[item["name"]] = {
                "type": item["type"],
                "default": item["default"],
                "description": item["description"],
            }
        result["input"] = inputs

        credentials: dict[str, Any] = {}
        for item in self.get_pipeline_credentials(pipeline_id):
            cred = {"env": item["env"]}
            if item["refresh"] is not None:
                cred["refresh"] = item["refresh"]
            credentials[item["name"]] = cred
        result["credentials"] = credentials
        result["steps"] = self.get_steps(pipeline_id)
        return result

    def get_pipeline(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM pipeline WHERE name=?", (name,)
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
                "SELECT * FROM pipeline ORDER BY name"
            ).fetchall()
            out = []
            has_project = self._column_exists(conn, "pipeline", "project")
            has_tags = self._column_exists(conn, "pipeline", "tags")
            has_group = self._column_exists(conn, "pipeline", "group_name")

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
            if not self._column_exists(conn, "pipeline", "project"):
                return False
            cursor = conn.execute(
                "UPDATE pipeline SET project=? WHERE name=?", (project, name)
            )
            return cursor.rowcount > 0

    def delete_pipelines_by_project(self, project: str) -> int:
        """Delete all pipelines with the given project. Returns count deleted."""
        with self._connect() as conn:
            if not self._column_exists(conn, "pipeline", "project"):
                return 0
            cursor = conn.execute(
                "DELETE FROM pipeline WHERE project=?", (project,)
            )
            return cursor.rowcount

    def get_project_stats(self) -> dict[str, dict]:
        """Return per-project counts for pipelines and helpers.

        Returns {project: {pipelines: N, helpers: M}}.
        """
        stats: dict[str, dict] = {}

        with self._connect() as conn:
            has_p_project = self._column_exists(conn, "pipeline", "project")
            has_h_project = self._column_exists(conn, "helper", "project")

            if has_p_project:
                rows = conn.execute(
                    "SELECT COALESCE(project,'') as proj, COUNT(*) as cnt "
                    "FROM pipeline GROUP BY proj"
                ).fetchall()
                for row in rows:
                    proj = row[0] or ""
                    stats.setdefault(proj, {"pipelines": 0, "helpers": 0})
                    stats[proj]["pipelines"] = row[1]

            if has_h_project:
                rows = conn.execute(
                    "SELECT COALESCE(project,'') as proj, COUNT(*) as cnt "
                    "FROM helper GROUP BY proj"
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
        content_hash: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
        imports: Optional[list[str]] = None,
    ) -> str:
        """Insert or update a helper index entry. Returns the helper id."""
        now = _now_iso()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id, created_at FROM helper WHERE name=?", (name,)
            ).fetchone()
            if existing:
                hid = existing[0]
                created_at = existing[1]
            else:
                hid = helper_id or str(uuid4())
                created_at = now

            has_code_col = self._column_exists(conn, "helper", "code")
            has_content_hash = self._column_exists(conn, "helper", "content_hash")
            has_project = self._column_exists(conn, "helper", "project")
            has_tags = self._column_exists(conn, "helper", "tags")
            has_group = self._column_exists(conn, "helper", "group_name")

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

            if has_content_hash and content_hash is not None:
                cols.append("content_hash")
                vals.append(content_hash)
                updates.append("content_hash=excluded.content_hash")

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

            has_imports = self._column_exists(conn, "helper", "imports_json")
            if has_imports and imports is not None:
                cols.append("imports_json")
                vals.append(json.dumps(imports))
                updates.append("imports_json=excluded.imports_json")

            placeholders = ",".join("?" * len(cols))
            col_str = ",".join(cols)
            update_str = ",".join(updates)
            conn.execute(
                f"""INSERT INTO helper ({col_str})
                   VALUES ({placeholders})
                   ON CONFLICT(name) DO UPDATE SET {update_str}
                """,
                vals,
            )
        return hid

    def delete_helper(self, name: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM helper WHERE name=?", (name,))
            return cursor.rowcount > 0

    def get_helper(self, name: str) -> Optional[dict]:
        """Get helper by name or by UUID."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM helper WHERE name=?", (name,)
            ).fetchone()
            if not row:
                # UUID fallback
                row = conn.execute(
                    "SELECT * FROM helper WHERE id=?", (name,)
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
                "SELECT * FROM helper ORDER BY name"
            ).fetchall()
            has_project = self._column_exists(conn, "helper", "project")
            has_tags = self._column_exists(conn, "helper", "tags")
            has_group = self._column_exists(conn, "helper", "group_name")

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
            if not self._column_exists(conn, "helper", "project"):
                return False
            cursor = conn.execute(
                "UPDATE helper SET project=? WHERE name=?", (project, name)
            )
            return cursor.rowcount > 0

    def delete_helpers_by_project(self, project: str) -> int:
        """Delete all helpers with the given project. Returns count deleted."""
        with self._connect() as conn:
            if not self._column_exists(conn, "helper", "project"):
                return 0
            cursor = conn.execute(
                "DELETE FROM helper WHERE project=?", (project,)
            )
            return cursor.rowcount

    @staticmethod
    def _helper_row_to_dict(row: dict) -> dict:
        row["requirements"] = json.loads(row.get("requirements_json") or "[]")
        row["input_schema"] = json.loads(row.get("input_schema_json") or "{}")
        row["output_schema"] = json.loads(row.get("output_schema_json") or "{}")
        row["content_hash"] = row.get("content_hash") or ""
        row["imports"] = json.loads(row.get("imports_json") or "[]")
        return row

    def get_pipeline_yaml_content(self, name: str) -> Optional[str]:
        """DEPRECATED — always returns None (T-BRIX-DBO-18).

        yaml_content is no longer written from live code paths.  The column is
        kept in the DB schema for rollback safety but is not populated by any
        active code path.  All callers should read pipeline data from
        pipeline_step rows via get_steps() instead.
        """
        return None

    def get_helper_code(self, name: str) -> Optional[str]:
        """Return the stored code for a helper, or None if not stored."""
        with self._connect() as conn:
            if not self._column_exists(conn, "helper", "code"):
                return None
            row = conn.execute(
                "SELECT code FROM helper WHERE name=?", (name,)
            ).fetchone()
            if not row:
                # UUID fallback
                row = conn.execute(
                    "SELECT code FROM helper WHERE id=?", (name,)
                ).fetchone()
            if row and row[0]:
                return row[0]
            return None

    def count_pipelines_with_content(self) -> int:
        """DEPRECATED — always returns 0 (T-BRIX-DBO-18).

        yaml_content is no longer written from live code paths.  This method
        is retained for interface compatibility only.
        """
        return 0

    def count_helpers_with_code(self) -> int:
        """Count helpers that have code stored."""
        with self._connect() as conn:
            if not self._column_exists(conn, "helper", "code"):
                return 0
            row = conn.execute(
                "SELECT COUNT(*) FROM helper WHERE code IS NOT NULL AND code != ''"
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
                """SELECT h.* FROM helper h
                   JOIN pipeline_helper ph ON ph.helper_id = h.id
                   JOIN pipeline p ON p.id = ph.pipeline_id
                   WHERE p.name = ?
                   ORDER BY h.name""",
                (pipeline_name,),
            ).fetchall()
            return [self._helper_row_to_dict(dict(r)) for r in rows]

    def find_pipelines_referencing_helper(self, helper_name: str) -> list[str]:
        """Return pipeline names whose step rows reference ``helper_name``."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT DISTINCT p.name
                   FROM pipeline_step ps
                   JOIN pipeline p ON p.id = ps.pipeline_id
                   WHERE ps.helper = ?
                   ORDER BY p.name""",
                (helper_name,),
            ).fetchall()
        return [str(row[0]) for row in rows]

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
                """INSERT INTO object_version (id, type, name, version_id, content, created_at)
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
                """SELECT * FROM object_version
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
                "SELECT * FROM object_version WHERE version_id=?",
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
                """SELECT created_at FROM object_version
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
                """DELETE FROM object_version
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
                "SELECT DISTINCT type, name FROM object_version"
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
                """INSERT INTO agent_session
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
                "SELECT * FROM agent_session WHERE session_id=?", (session_id,)
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
                "SELECT * FROM agent_session ORDER BY updated_at DESC"
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
                "DELETE FROM agent_session WHERE session_id=?", (session_id,)
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
                "DELETE FROM resource_lock WHERE resource_id=? AND expires_at < ?",
                (resource_id, now),
            )
            # Try to insert the new lock
            try:
                conn.execute(
                    """INSERT INTO resource_lock (resource_id, run_id, claimed_at, expires_at)
                       VALUES (?,?,?,?)""",
                    (resource_id, run_id, now, expires),
                )
                return {"claimed": True, "resource_id": resource_id, "run_id": run_id, "expires_at": expires}
            except sqlite3.IntegrityError:
                # Lock already held by someone else
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT * FROM resource_lock WHERE resource_id=?", (resource_id,)
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
                    """INSERT OR IGNORE INTO resource_lock (resource_id, run_id, claimed_at, expires_at)
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
                "SELECT * FROM resource_lock WHERE resource_id=? AND expires_at >= ?",
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
                "DELETE FROM resource_lock WHERE resource_id=?", (resource_id,)
            )
            return cursor.rowcount > 0

    def list_resource_locks(self) -> list[dict]:
        """Return all active (non-expired) resource locks."""
        now = _now_iso()
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM resource_lock WHERE expires_at >= ? ORDER BY claimed_at",
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
                "SELECT run_id FROM run WHERE started_at < datetime('now', ?)",
                (f"-{max_days} days",),
            ).fetchall()
            old_run_ids = [r[0] for r in old_run_rows]

            # Delete execution data BEFORE deleting runs (T-BRIX-DB-07)
            if old_run_ids:
                ph = ",".join("?" * len(old_run_ids))
                conn.execute(f"DELETE FROM step_execution WHERE run_id IN ({ph})", old_run_ids)
                conn.execute(f"DELETE FROM foreach_item_execution WHERE run_id IN ({ph})", old_run_ids)
                conn.execute(f"DELETE FROM run_input WHERE run_id IN ({ph})", old_run_ids)

            cursor = conn.execute(
                "DELETE FROM run WHERE started_at < datetime('now', ?)",
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
                "DELETE FROM deprecated_usage WHERE pipeline_name NOT IN (SELECT name FROM pipeline)"
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
                        """SELECT run_id FROM run
                           WHERE finished_at IS NOT NULL
                           ORDER BY started_at ASC
                           LIMIT 100"""
                    ).fetchall()
                    if not rows:
                        break
                    run_ids = [r[0] for r in rows]
                    placeholders = ",".join("?" * len(run_ids))
                    # Delete execution data BEFORE deleting runs (T-BRIX-DB-07)
                    conn.execute(f"DELETE FROM step_execution WHERE run_id IN ({placeholders})", run_ids)
                    conn.execute(f"DELETE FROM foreach_item_execution WHERE run_id IN ({placeholders})", run_ids)
                    conn.execute(f"DELETE FROM run_input WHERE run_id IN ({placeholders})", run_ids)
                    cursor = conn.execute(
                        f"DELETE FROM run WHERE run_id IN ({placeholders})",
                        run_ids,
                    )
                    runs_deleted_size += cursor.rowcount
                    conn.execute("VACUUM")

                # Re-check size
                db_size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
                db_size_mb = db_size_bytes / (1024 * 1024)

                if cursor.rowcount == 0:
                    break  # Nothing left to delete

        # Pass 3: Mark stuck/zombie runs as finished (T-BRIX-BUG-03)
        # Runs with finished_at IS NULL and started_at older than 24h are
        # either zombies (pipeline deleted) or stuck.  Close them out.
        zombie_cleaned = 0
        with self._connect() as conn:
            cursor_z = conn.execute(
                """UPDATE run
                   SET finished_at = datetime('now'),
                       success = 0,
                       notes = COALESCE(notes || ' | ', '') || 'zombie_cleaned by retention'
                   WHERE finished_at IS NULL
                     AND started_at < datetime('now', '-1 day')""",
            )
            zombie_cleaned = cursor_z.rowcount

        # Pass 4: Remove test pipelines and their data (T-BRIX-BUG-04)
        test_pipelines_deleted = 0
        test_runs_deleted = 0
        with self._connect() as conn:
            # Find test pipeline names
            tp_rows = conn.execute(
                "SELECT name FROM pipeline WHERE name LIKE 'test-%' OR name LIKE 'xtest-%'"
            ).fetchall()
            tp_names = [r[0] for r in tp_rows]

            if tp_names:
                ph = ",".join("?" * len(tp_names))
                # Collect run_ids belonging to these pipelines
                run_rows = conn.execute(
                    f"SELECT run_id FROM run WHERE pipeline IN ({ph})", tp_names
                ).fetchall()
                run_ids = [r[0] for r in run_rows]

                if run_ids:
                    rph = ",".join("?" * len(run_ids))
                    conn.execute(f"DELETE FROM step_output WHERE run_id IN ({rph})", run_ids)
                    conn.execute(f"DELETE FROM step_execution WHERE run_id IN ({rph})", run_ids)
                    conn.execute(f"DELETE FROM foreach_item_execution WHERE run_id IN ({rph})", run_ids)
                    conn.execute(f"DELETE FROM run_input WHERE run_id IN ({rph})", run_ids)

                cursor_tr = conn.execute(
                    f"DELETE FROM run WHERE pipeline IN ({ph})", tp_names
                )
                test_runs_deleted = cursor_tr.rowcount

                cursor_tp = conn.execute(
                    f"DELETE FROM pipeline WHERE name IN ({ph})", tp_names
                )
                test_pipelines_deleted = cursor_tp.rowcount

        # Final size after cleanup
        db_size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
        db_size_mb = db_size_bytes / (1024 * 1024)

        return {
            "runs_deleted_age": runs_deleted_age,
            "runs_deleted_size": runs_deleted_size,
            "app_log_deleted": app_log_deleted,
            "zombie_cleaned": zombie_cleaned,
            "test_pipelines_deleted": test_pipelines_deleted,
            "test_runs_deleted": test_runs_deleted,
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
        return _safe_table(table)

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
            if project is not None and self._column_exists(conn, "alert_rule", "project"):
                cols.append("project")
                vals.append(project)
            if tags is not None and self._column_exists(conn, "alert_rule", "tags"):
                cols.append("tags")
                vals.append(json.dumps(tags))
            if group_name is not None and self._column_exists(conn, "alert_rule", "group_name"):
                cols.append("group_name")
                vals.append(group_name)
            placeholders = ",".join("?" * len(cols))
            conn.execute(
                f"INSERT INTO alert_rule ({','.join(cols)}) VALUES ({placeholders})",
                vals,
            )
        return self.alert_rule_get(rid)  # type: ignore[return-value]

    def alert_rule_get(self, rule_id: str) -> Optional[dict]:
        """Return an alert rule by ID, or None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM alert_rule WHERE id=?", (rule_id,)
            ).fetchone()
        if not row:
            return None
        return self._alert_rule_row_to_dict(dict(row))

    def alert_rule_list(self) -> list[dict]:
        """Return all alert rules ordered by created_at."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM alert_rule ORDER BY created_at"
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
            conn.execute(f"UPDATE alert_rule SET {set_clause} WHERE id=?", values)
        return self.alert_rule_get(rule_id)

    def alert_rule_delete(self, rule_id: str) -> bool:
        """Delete an alert rule by ID. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM alert_rule WHERE id=?", (rule_id,)
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
            has_project = self._column_exists(conn, "trigger", "project")
            has_tags = self._column_exists(conn, "trigger", "tags")
            has_group = self._column_exists(conn, "trigger", "group_name")

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
                    f"INSERT INTO trigger ({','.join(cols)}) VALUES ({placeholders})",
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
                "SELECT * FROM trigger WHERE name=?", (name,)
            ).fetchone()
            if not row:
                row = conn.execute(
                    "SELECT * FROM trigger WHERE id=?", (name,)
                ).fetchone()
            return self._trigger_row_to_dict(dict(row)) if row else None

    def trigger_list(self) -> list[dict]:
        """Return all triggers sorted by name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM trigger ORDER BY name"
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
            if project is not None and self._column_exists(conn, "trigger", "project"):
                updates["project"] = project
            if tags is not None and self._column_exists(conn, "trigger", "tags"):
                updates["tags"] = json.dumps(tags)
            if group_name is not None and self._column_exists(conn, "trigger", "group_name"):
                updates["group_name"] = group_name

            set_clause = ", ".join(f"{k}=?" for k in updates)
            values = list(updates.values()) + [existing["id"]]
            conn.execute(
                f"UPDATE trigger SET {set_clause} WHERE id=?", values
            )
        return self.trigger_get(existing["id"])

    def trigger_delete(self, name: str) -> bool:
        """Delete a trigger by name or UUID. Returns True if deleted."""
        existing = self.trigger_get(name)
        if existing is None:
            return False
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM trigger WHERE id=?", (existing["id"],)
            )
            return cursor.rowcount > 0

    def trigger_record_fired(
        self,
        name: str,
        run_id: Optional[str] = None,
        status: str = "fired",
        fired_at: Optional[str] = None,
    ) -> None:
        """Update last_fired_at, last_run_id, last_status after a trigger fires."""
        existing = self.trigger_get(name)
        if existing is None:
            return
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """UPDATE trigger
                   SET last_fired_at=?, last_run_id=?, last_status=?, updated_at=?
                   WHERE id=?""",
                (fired_at or now, run_id, status, now, existing["id"]),
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
            if project is not None and self._column_exists(conn, "trigger_group", "project"):
                cols.append("project")
                vals.append(project)
            if tags is not None and self._column_exists(conn, "trigger_group", "tags"):
                cols.append("tags")
                vals.append(json.dumps(tags))
            if group_name is not None and self._column_exists(conn, "trigger_group", "group_name"):
                cols.append("group_name")
                vals.append(group_name)
            placeholders = ",".join("?" * len(cols))
            try:
                conn.execute(
                    f"INSERT INTO trigger_group ({','.join(cols)}) VALUES ({placeholders})",
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
                "SELECT * FROM trigger_group WHERE name=?", (name,)
            ).fetchone()
            if not row:
                row = conn.execute(
                    "SELECT * FROM trigger_group WHERE id=?", (name,)
                ).fetchone()
            return self._trigger_group_row_to_dict(dict(row)) if row else None

    def trigger_group_list(self) -> list[dict]:
        """Return all trigger groups sorted by name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM trigger_group ORDER BY name"
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
                f"UPDATE trigger_group SET {set_clause} WHERE id=?", values
            )
        return self.trigger_group_get(existing["id"])

    def trigger_group_delete(self, name: str) -> bool:
        """Delete a trigger group by name or UUID. Returns True if deleted."""
        existing = self.trigger_group_get(name)
        if existing is None:
            return False
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM trigger_group WHERE id=?", (existing["id"],)
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
    # Trigger State (T-BRIX-MOD-03 — migrated from trigger/state.py)
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
        result_json = json_dumps(result) if result is not None else None
        input_json = json_dumps(input) if input is not None else None
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO pipeline_event "
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
                "INSERT INTO pipeline_event "
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
            query = "SELECT * FROM pipeline_event WHERE processed=0"
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
                "UPDATE pipeline_event SET processed=1 WHERE id=?", (event_id,)
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
                rows = src_conn.execute("SELECT * FROM pipeline_event").fetchall()
                with self._connect() as conn:
                    for row in rows:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO pipeline_event "
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
            row = conn.execute("SELECT COUNT(*) FROM brick_definition").fetchone()
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
            rows = conn.execute("SELECT * FROM brick_definition ORDER BY name").fetchall()
        return [self._brick_row_enrich_org(dict(r)) for r in rows]

    def brick_definitions_get(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM brick_definition WHERE name = ?", (name,)
            ).fetchone()
        if row is None:
            return None
        return self._brick_row_enrich_org(dict(row))

    def brick_definitions_upsert(self, record: dict) -> None:
        now = _now_iso()
        with self._connect() as conn:
            has_org_tags = self._column_exists(conn, "brick_definition", "org_tags")

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

            has_project = self._column_exists(conn, "brick_definition", "project")
            has_group = self._column_exists(conn, "brick_definition", "group_name")
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
                f"""INSERT INTO brick_definition ({','.join(cols)})
                   VALUES ({placeholders})
                   ON CONFLICT(name) DO UPDATE SET {update_str}""",
                vals,
            )

    def brick_definitions_delete(self, name: str) -> bool:
        """Delete a brick_definition by name. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM brick_definition WHERE name=?", (name,))
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — connector_definitions
    # ------------------------------------------------------------------

    def connector_definitions_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM connector_definition").fetchone()
        return row[0] if row else 0

    def connector_definitions_list(self) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM connector_definition ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def connector_definitions_get(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM connector_definition WHERE name = ?", (name,)
            ).fetchone()
        return dict(row) if row else None

    def connector_definitions_upsert(self, record: dict) -> None:
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO connector_definition
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
            row = conn.execute("SELECT COUNT(*) FROM mcp_tool_schema").fetchone()
        return row[0] if row else 0

    def mcp_tool_schemas_list(self) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM mcp_tool_schema ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def mcp_tool_schemas_get(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM mcp_tool_schema WHERE name = ?", (name,)
            ).fetchone()
        return dict(row) if row else None

    def mcp_tool_schemas_upsert(self, record: dict) -> None:
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO mcp_tool_schema (name, description, input_schema, created_at, updated_at)
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

    def mcp_tool_schemas_delete(self, name: str) -> bool:
        """Delete a tool schema by name. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM mcp_tool_schema WHERE name = ?", (name,))
        return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # T-BRIX-FDB-01: MCP servers
    # ------------------------------------------------------------------

    @staticmethod
    def _mcp_server_row_to_dict(row: dict) -> dict:
        """Parse JSON/bool fields in an mcp_server row."""
        args_raw = row.get("args_json", "[]")
        env_raw = row.get("env_json", "{}")

        try:
            row["args"] = json.loads(args_raw) if args_raw else []
        except (json.JSONDecodeError, TypeError):
            row["args"] = []

        try:
            row["env"] = json.loads(env_raw) if env_raw else {}
        except (json.JSONDecodeError, TypeError):
            row["env"] = {}

        row["unwrap_json"] = bool(row.get("unwrap_json", 0))
        row.pop("args_json", None)
        row.pop("env_json", None)
        return row

    def upsert_mcp_server(
        self,
        name: str,
        command: str,
        args: Optional[list] = None,
        env: Optional[dict] = None,
        tools_prefix: str = "",
        transport: str = "stdio",
        url: str = "",
        unwrap_json: bool = False,
        description: str = "",
    ) -> dict:
        """Create or update an MCP server definition."""
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO mcp_server
                   (name, command, args_json, env_json, tools_prefix, transport,
                    url, unwrap_json, description, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET
                     command=excluded.command,
                     args_json=excluded.args_json,
                     env_json=excluded.env_json,
                     tools_prefix=excluded.tools_prefix,
                     transport=excluded.transport,
                     url=excluded.url,
                     unwrap_json=excluded.unwrap_json,
                     description=excluded.description,
                     updated_at=excluded.updated_at""",
                (
                    name,
                    command,
                    json.dumps(args or []),
                    json.dumps(env or {}),
                    tools_prefix,
                    transport,
                    url,
                    1 if unwrap_json else 0,
                    description,
                    now,
                    now,
                ),
            )
        return self.get_mcp_server(name) or {}

    def get_mcp_server(self, name: str) -> Optional[dict]:
        """Return an MCP server definition by name, or None."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM mcp_server WHERE name=?",
                (name,),
            ).fetchone()
        if row is None:
            return None
        return self._mcp_server_row_to_dict(dict(row))

    def list_mcp_servers(self) -> list[dict]:
        """Return all MCP server definitions ordered by name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM mcp_server ORDER BY name"
            ).fetchall()
        return [self._mcp_server_row_to_dict(dict(row)) for row in rows]

    def delete_mcp_server(self, name: str) -> bool:
        """Delete an MCP server definition by name."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM mcp_server WHERE name=?", (name,))
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — help_topics
    # ------------------------------------------------------------------

    def help_topics_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM help_topic").fetchone()
        return row[0] if row else 0

    def help_topics_list(self, category: Optional[str] = None) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            if category:
                rows = conn.execute(
                    "SELECT * FROM help_topic WHERE category = ? ORDER BY name",
                    (category,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM help_topic ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def help_topics_get(self, name: str) -> Optional[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM help_topic WHERE name = ?", (name,)
            ).fetchone()
        return dict(row) if row else None

    def help_topics_upsert(self, record: dict) -> None:
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO help_topic (name, title, content, category, created_at, updated_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(name) DO UPDATE SET
                       title=excluded.title,
                       content=excluded.content,
                       category=excluded.category,
                       updated_at=excluded.updated_at""",
                (
                    record["name"],
                    record.get("title", record["name"]),
                    record.get("content", ""),
                    record.get("category", ""),
                    now,
                    now,
                ),
            )

    def help_topics_delete(self, name: str) -> bool:
        """Delete a help topic by name. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM help_topic WHERE name = ?", (name,))
        return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # T-BRIX-TIPS-01: Tips — DB-managed tips for get_tips
    # ------------------------------------------------------------------

    def tip_create(self, category: str, title: str, content: str,
                   priority: int = 5, is_active: bool = True) -> dict:
        """Create a new tip and return it."""
        now = _now_iso()
        tip_id = str(uuid4())
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO tip (id, category, title, content, priority, is_active, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (tip_id, category, title, content, priority, 1 if is_active else 0, now, now),
            )
        return {"id": tip_id, "category": category, "title": title,
                "content": content, "priority": priority, "is_active": is_active,
                "created_at": now, "updated_at": now}

    def tip_get(self, tip_id: str) -> Optional[dict]:
        """Get a single tip by ID."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM tip WHERE id = ?", (tip_id,)).fetchone()
        return dict(row) if row else None

    def tip_update(self, tip_id: str, **fields) -> Optional[dict]:
        """Update a tip. Returns the updated tip or None if not found."""
        allowed = {"category", "title", "content", "priority", "is_active"}
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return self.tip_get(tip_id)
        if "is_active" in updates:
            updates["is_active"] = 1 if updates["is_active"] else 0
        updates["updated_at"] = _now_iso()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [tip_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE tip SET {set_clause} WHERE id = ?", values)
        return self.tip_get(tip_id)

    def tip_delete(self, tip_id: str) -> bool:
        """Delete a tip by ID. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM tip WHERE id = ?", (tip_id,))
        return cursor.rowcount > 0

    def tip_list(self, category: Optional[str] = None, active_only: bool = True) -> list[dict]:
        """List tips, optionally filtered by category and active status."""
        clauses: list[str] = []
        params: list[Any] = []
        if active_only:
            clauses.append("is_active = 1")
        if category:
            clauses.append("category = ?")
            params.append(category)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT * FROM tip{where} ORDER BY priority DESC, category, title",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # T-BRIX-DB-06: DB-First — keyword_taxonomies
    # ------------------------------------------------------------------

    def keyword_taxonomies_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM keyword_taxonomy").fetchone()
        return row[0] if row else 0

    def keyword_taxonomies_list(self, category: Optional[str] = None) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            if category:
                rows = conn.execute(
                    "SELECT * FROM keyword_taxonomy WHERE category = ? ORDER BY keyword",
                    (category,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM keyword_taxonomy ORDER BY category, keyword"
                ).fetchall()
        return [dict(r) for r in rows]

    def keyword_taxonomies_upsert(self, category: str, keyword: str, language: str = "de", mapped_to: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO keyword_taxonomy (category, keyword, language, mapped_to)
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

    def keyword_taxonomies_delete(self, category: str, keyword: str) -> bool:
        """Delete a keyword taxonomy entry. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM keyword_taxonomy WHERE category = ? AND keyword = ?",
                (category, keyword),
            )
        return cursor.rowcount > 0

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

    def type_compatibility_delete(self, output_type: str, compatible_input: str) -> bool:
        """Delete a type compatibility entry. Returns True if deleted."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM type_compatibility WHERE output_type = ? AND compatible_input = ?",
                (output_type, compatible_input),
            )
        return cursor.rowcount > 0

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
            has_project = self._column_exists(conn, "variable", "project")
            has_tags = self._column_exists(conn, "variable", "tags")
            has_group = self._column_exists(conn, "variable", "group_name")

            existing = conn.execute(
                "SELECT created_at FROM variable WHERE name=?", (name,)
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
                    f"UPDATE variable SET {', '.join(sets)} WHERE name=?",
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
                    f"INSERT INTO variable ({','.join(cols)}) VALUES ({placeholders})",
                    vals2,
                )

    def variable_get(self, name: str) -> Optional[str]:
        """Return the decrypted value of a managed variable, or None if not found."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT value, secret FROM variable WHERE name=?", (name,)
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
                "SELECT * FROM variable WHERE name=?", (name,)
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
                "SELECT * FROM variable ORDER BY name"
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
            cursor = conn.execute("DELETE FROM variable WHERE name=?", (name,))
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
            serialized = json_dumps(output_data)
        except Exception:
            serialized = json_dumps(str(output_data))
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
            has_project = self._column_exists(conn, "profile", "project")
            has_tags = self._column_exists(conn, "profile", "tags")
            has_group = self._column_exists(conn, "profile", "group_name")

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
                f"""INSERT INTO profile ({','.join(cols)})
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
                "SELECT * FROM profile WHERE name=?", (name,)
            ).fetchone()
        if not row:
            return None
        return self._profile_enrich_org(dict(row))

    def profile_list(self) -> list[dict]:
        """Return all profiles ordered by name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM profile ORDER BY name"
            ).fetchall()
        return [self._profile_enrich_org(dict(row)) for row in rows]

    def profile_delete(self, name: str) -> bool:
        """Delete a profile by name. Returns True if found and deleted."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM profile WHERE name=?", (name,))
            return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # T-BRIX-FDB-02: Environment profiles
    # ------------------------------------------------------------------

    @staticmethod
    def _env_profile_row_to_dict(row: dict) -> dict:
        """Parse JSON/bool fields in an env_profile row."""
        env_raw = row.get("env_json", "{}")
        defaults_raw = row.get("input_defaults_json", "{}")

        try:
            row["env"] = json.loads(env_raw) if env_raw else {}
        except (json.JSONDecodeError, TypeError):
            row["env"] = {}

        try:
            row["input_defaults"] = json.loads(defaults_raw) if defaults_raw else {}
        except (json.JSONDecodeError, TypeError):
            row["input_defaults"] = {}

        row["is_default"] = bool(row.get("is_default", 0))
        row.pop("env_json", None)
        row.pop("input_defaults_json", None)
        return row

    def upsert_env_profile(
        self,
        name: str,
        env: Optional[dict] = None,
        input_defaults: Optional[dict] = None,
        is_default: bool = False,
        description: str = "",
    ) -> dict:
        """Create or update an environment profile."""
        now = _now_iso()
        with self._connect() as conn:
            if is_default:
                conn.execute(
                    "UPDATE env_profile SET is_default=0, updated_at=? WHERE is_default=1 AND name != ?",
                    (now, name),
                )

            conn.execute(
                """INSERT INTO env_profile
                   (name, is_default, env_json, input_defaults_json, description, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET
                     is_default=excluded.is_default,
                     env_json=excluded.env_json,
                     input_defaults_json=excluded.input_defaults_json,
                     description=excluded.description,
                     updated_at=excluded.updated_at""",
                (
                    name,
                    1 if is_default else 0,
                    json.dumps(env or {}),
                    json.dumps(input_defaults or {}),
                    description,
                    now,
                    now,
                ),
            )
        return self.get_env_profile(name) or {}

    def get_env_profile(self, name: str) -> Optional[dict]:
        """Return an environment profile by name, or None."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM env_profile WHERE name=?",
                (name,),
            ).fetchone()
        if row is None:
            return None
        return self._env_profile_row_to_dict(dict(row))

    def list_env_profiles(self) -> list[dict]:
        """Return all environment profiles ordered by default first, then name."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM env_profile ORDER BY is_default DESC, name"
            ).fetchall()
        return [self._env_profile_row_to_dict(dict(row)) for row in rows]

    def get_default_env_profile(self) -> Optional[dict]:
        """Return the default environment profile, or None if none is marked."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM env_profile WHERE is_default=1 ORDER BY name LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return self._env_profile_row_to_dict(dict(row))

    def delete_env_profile(self, name: str) -> bool:
        """Delete an environment profile by name."""
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM env_profile WHERE name=?", (name,))
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
        data_json = json_dumps(data)
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO step_pin (pipeline_name, step_id, pinned_data, pinned_from_run, created_at)
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
                "DELETE FROM step_pin WHERE pipeline_name=? AND step_id=?",
                (pipeline_name, step_id),
            )
            return cursor.rowcount > 0

    def get_pin(self, pipeline_name: str, step_id: str) -> Optional[dict]:
        """Return the pin record for a step, or None if not pinned."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM step_pin WHERE pipeline_name=? AND step_id=?",
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
                "SELECT * FROM step_pin WHERE pipeline_name=? ORDER BY step_id",
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

    # ------------------------------------------------------------------
    # Changelog — T-BRIX-CHANGELOG-01
    # ------------------------------------------------------------------

    def add_changelog_entry(
        self,
        version: str,
        type: str,
        title: str,
        *,
        description: str = "",
        task_id: str | None = None,
        commit_sha: str | None = None,
        timestamp: str | None = None,
    ) -> dict:
        """Insert a changelog entry. Returns the stored row as a dict."""
        entry_id = str(uuid4())
        now = _now_iso()
        ts = timestamp or now
        valid_types = ("breaking", "feature", "fix", "refactor", "docs")
        if type not in valid_types:
            raise ValueError(f"Invalid changelog type '{type}'. Must be one of: {', '.join(valid_types)}")
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO changelog_entry
                   (id, version, timestamp, type, title, description, task_id, commit_sha, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (entry_id, version, ts, type, title, description, task_id, commit_sha, now),
            )
        return {
            "id": entry_id,
            "version": version,
            "timestamp": ts,
            "type": type,
            "title": title,
            "description": description,
            "task_id": task_id,
            "commit_sha": commit_sha,
            "created_at": now,
        }

    def list_changelog(
        self,
        *,
        since: str | None = None,
        type: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """List changelog entries, optionally filtered.

        Parameters
        ----------
        since:
            If given, only return entries whose version is >= *since*
            (lexicographic comparison on semver strings works for
            same-length versions).  Entries are filtered by comparing
            the ``version`` field.
        type:
            Filter to a specific entry type (breaking/feature/fix/refactor/docs).
        limit:
            Maximum number of entries to return (default 50).

        Returns
        -------
        List of changelog entry dicts, ordered by version DESC, timestamp DESC.
        """
        clauses: list[str] = []
        params: list[Any] = []

        if type:
            clauses.append("type = ?")
            params.append(type)

        where = ""
        if clauses:
            where = "WHERE " + " AND ".join(clauses)

        sql = f"SELECT * FROM changelog_entry {where} ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()

        entries = [dict(r) for r in rows]

        entries.sort(
            key=lambda entry: (
                _parse_semver(entry.get("version", "")),
                entry.get("timestamp", ""),
            ),
            reverse=True,
        )

        # Apply semver-aware 'since' filter in Python (lexicographic SQL
        # comparison fails for versions like "9.0.0" vs "10.0.0").
        if since:
            since_tuple = _parse_semver(since)
            entries = [e for e in entries if _parse_semver(e["version"]) >= since_tuple]

        return entries
