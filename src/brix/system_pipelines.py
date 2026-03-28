"""System Pipeline Templates — T-BRIX-DB-16.

Defines built-in system pipelines that are seeded into the DB on first start.
These are real pipelines that run through the Brix engine.

The _system/ prefix is a convention: system pipelines cannot be deleted
but can be updated/customised by the user.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System Pipeline Definitions
# ---------------------------------------------------------------------------

SYSTEM_PIPELINES: list[dict] = [
    {
        "name": "_system/alert-check",
        "description": "Prüft Alert-Regeln und benachrichtigt bei Überschreitung",
        "version": "1.0.0",
        "compositor_mode": True,
        "steps": [
            {
                "id": "check",
                "type": "db.query",
                "config": {
                    "connection": "brix-internal",
                    "query": "SELECT * FROM alert_rules WHERE enabled = 1",
                },
            },
            {
                "id": "evaluate",
                "type": "flow.filter",
                "config": {"where": "{{ item.condition_met }}"},
            },
            {
                "id": "notify",
                "type": "action.notify",
                "foreach": "{{ evaluate.output }}",
                "config": {
                    "channel": "log",
                    "message": "Alert: {{ item.name }}",
                },
            },
        ],
    },
    {
        "name": "_system/retention-cleanup",
        "description": "Löscht alte Runs und Execution-Daten basierend auf Retention-Policy",
        "version": "1.0.0",
        "steps": [
            {
                "id": "cleanup",
                "type": "db.query",
                "config": {
                    "connection": "brix-internal",
                    "query": (
                        "SELECT count(*) as old_runs FROM runs "
                        "WHERE started_at < datetime('now', '-30 days')"
                    ),
                },
            },
        ],
    },
    {
        "name": "_system/health-report",
        "description": "Generiert Health-Report und loggt Status",
        "version": "1.0.0",
        "steps": [
            {
                "id": "check",
                "type": "flow.set",
                "config": {"key": "status", "value": "healthy"},
            },
            {
                "id": "log",
                "type": "action.notify",
                "config": {
                    "channel": "log",
                    "message": "System health: {{ check.output.status }}",
                },
            },
        ],
    },
    {
        "name": "_system/db-stats",
        "description": "Sammelt Datenbankstatistiken und gibt einen Überblick aus",
        "version": "1.0.0",
        "steps": [
            {
                "id": "count_runs",
                "type": "db.query",
                "config": {
                    "connection": "brix-internal",
                    "query": "SELECT count(*) as total_runs FROM runs",
                },
            },
            {
                "id": "count_pipelines",
                "type": "db.query",
                "config": {
                    "connection": "brix-internal",
                    "query": "SELECT count(*) as total_pipelines FROM pipelines",
                },
            },
            {
                "id": "report",
                "type": "action.notify",
                "config": {
                    "channel": "log",
                    "message": (
                        "DB Stats — Runs: {{ count_runs.output.total_runs }}, "
                        "Pipelines: {{ count_pipelines.output.total_pipelines }}"
                    ),
                },
            },
        ],
    },
]

# Set of names protected from deletion
SYSTEM_PIPELINE_NAMES: frozenset[str] = frozenset(
    p["name"] for p in SYSTEM_PIPELINES
)

SYSTEM_PREFIX = "_system/"


def is_system_pipeline(name: str) -> bool:
    """Return True if the pipeline name belongs to the _system/ namespace."""
    return name.startswith(SYSTEM_PREFIX)


def seed_system_pipelines(pipeline_store) -> int:
    """Seed system pipelines into the pipeline store if not already present.

    Only seeds pipelines that do not exist yet — idempotent.
    Returns the number of newly seeded pipelines.

    Note: Pipeline names with '/' (e.g. '_system/health-report') require the
    pipelines directory to have a '_system/' subdirectory.  This function
    creates it automatically.
    """
    from pathlib import Path

    # Ensure the _system/ subdirectory exists inside pipelines_dir
    system_subdir = Path(pipeline_store.pipelines_dir) / "_system"
    system_subdir.mkdir(parents=True, exist_ok=True)

    seeded = 0
    for defn in SYSTEM_PIPELINES:
        name = defn["name"]
        if pipeline_store.exists(name):
            logger.debug("System pipeline '%s' already exists — skipping", name)
            continue
        try:
            pipeline_store.save(defn, name=name)
            logger.debug("Seeded system pipeline '%s'", name)
            seeded += 1
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to seed system pipeline '%s': %s", name, exc)
    if seeded:
        logger.info("System pipelines seeded: %d", seeded)
    return seeded
