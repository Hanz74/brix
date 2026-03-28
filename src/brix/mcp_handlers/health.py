"""Health check handler module — T-BRIX-DB-25.

Provides a single brix__health tool that returns a snapshot of all
Brix subsystems: DB, runners, bricks, pipelines, deprecated step-types,
triggers, and retention policy.
"""
from __future__ import annotations

import os
from pathlib import Path

from brix.db import BrixDB, _DEFAULT_RETENTION_DAYS, _DEFAULT_RETENTION_MAX_MB
from brix.runners.base import discover_runners
from brix.pipeline_store import PipelineStore
from brix.mcp_handlers._shared import _registry, _pipeline_dir


# ---------------------------------------------------------------------------
# Internal sub-checks
# ---------------------------------------------------------------------------

def _check_db() -> dict:
    """Return DB health: size_mb, table counts, status."""
    try:
        db = BrixDB()
        db_path = db.db_path
        size_bytes = db_path.stat().st_size if db_path.exists() else 0
        size_mb = round(size_bytes / (1024 * 1024), 3)

        tables: dict[str, int] = {}
        table_names = [
            "runs", "pipelines", "helpers", "object_versions",
            "alert_rules", "triggers", "variables", "persistent_store",
        ]
        with db._connect() as conn:
            existing = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            for t in table_names:
                if t in existing:
                    row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
                    tables[t] = row[0] if row else 0

        return {
            "status": "ok",
            "path": str(db_path),
            "size_mb": size_mb,
            "tables": tables,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _check_runners() -> dict:
    """Return runner health: total loaded, list of runner names."""
    try:
        runners = discover_runners()
        names = sorted(runners.keys())
        return {
            "status": "ok",
            "total": len(names),
            "runners": names,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _check_bricks() -> dict:
    """Return brick health: total, system vs. custom, broken refs."""
    try:
        all_bricks = _registry.list_all()
        total = len(all_bricks)
        system_count = sum(1 for b in all_bricks if getattr(b, "system", False))
        custom_count = total - system_count

        # Detect broken extends references
        brick_names = {b.name for b in all_bricks}
        broken_refs: list[str] = []
        for b in all_bricks:
            extends = getattr(b, "extends", None)
            if extends and extends not in brick_names:
                broken_refs.append(f"{b.name} -> {extends}")

        status = "warn" if broken_refs else "ok"
        result: dict = {
            "status": status,
            "total": total,
            "system": system_count,
            "custom": custom_count,
        }
        if broken_refs:
            result["broken_extends"] = broken_refs
        return result
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _check_pipelines() -> dict:
    """Return pipeline health: total pipelines, recent run counts."""
    try:
        store = PipelineStore(pipelines_dir=_pipeline_dir())
        all_pipelines = store.list_all()
        total = len(all_pipelines)

        # Count recent runs (last 24h) from DB
        recent_runs = 0
        try:
            db = BrixDB()
            with db._connect() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM runs WHERE started_at >= datetime('now', '-1 day')"
                ).fetchone()
                recent_runs = row[0] if row else 0
        except Exception:
            pass

        return {
            "status": "ok",
            "total": total,
            "recent_runs_24h": recent_runs,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _check_deprecated() -> dict:
    """Return deprecated step-type usage count."""
    try:
        db = BrixDB()
        count = db.get_deprecated_count()
        status = "warn" if count > 0 else "ok"
        return {
            "status": status,
            "count": count,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _check_triggers() -> dict:
    """Return trigger health: total, enabled, failing (last_status == 'error')."""
    try:
        db = BrixDB()
        triggers = db.trigger_list()
        total = len(triggers)
        enabled = sum(1 for t in triggers if t.get("enabled", True))
        failing = [
            t["name"]
            for t in triggers
            if t.get("last_status") == "error"
        ]
        status = "warn" if failing else "ok"
        result: dict = {
            "status": status,
            "total": total,
            "enabled": enabled,
        }
        if failing:
            result["failing"] = failing
        return result
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _check_retention() -> dict:
    """Return retention policy health: current DB size vs. configured max."""
    try:
        db = BrixDB()

        try:
            max_days = int(os.environ.get("BRIX_RETENTION_DAYS", _DEFAULT_RETENTION_DAYS))
        except (ValueError, TypeError):
            max_days = int(_DEFAULT_RETENTION_DAYS)

        try:
            max_mb = float(os.environ.get("BRIX_RETENTION_MAX_MB", _DEFAULT_RETENTION_MAX_MB))
        except (ValueError, TypeError):
            max_mb = float(_DEFAULT_RETENTION_MAX_MB)

        size_bytes = db.db_path.stat().st_size if db.db_path.exists() else 0
        size_mb = round(size_bytes / (1024 * 1024), 3)
        pct = round((size_mb / max_mb) * 100, 1) if max_mb > 0 else 0.0

        if pct >= 90:
            status = "error"
        elif pct >= 70:
            status = "warn"
        else:
            status = "ok"

        return {
            "status": status,
            "db_size_mb": size_mb,
            "max_mb": max_mb,
            "used_pct": pct,
            "retention_days": max_days,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


# ---------------------------------------------------------------------------
# Overall status aggregation
# ---------------------------------------------------------------------------

def _aggregate_overall(subsystems: dict) -> str:
    """Compute overall status from subsystem statuses.

    error > warn > ok
    """
    statuses = [v.get("status", "ok") for v in subsystems.values()]
    if "error" in statuses:
        return "error"
    if "warn" in statuses:
        return "warn"
    return "ok"


# ---------------------------------------------------------------------------
# MCP handler
# ---------------------------------------------------------------------------

async def _handle_health(arguments: dict) -> dict:
    """Return Gesamt-Status of Brix on one call — T-BRIX-DB-25.

    Collects status from all subsystems:
    - db: size, table counts
    - runners: loaded runner names
    - bricks: total, system/custom, broken extends refs
    - pipelines: total count, recent runs (24h)
    - deprecated: legacy step-type usage count
    - triggers: total, enabled, failing
    - retention: DB size vs. max_mb policy

    Returns overall = "ok" | "warn" | "error".
    """
    subsystems: dict[str, dict] = {
        "db": _check_db(),
        "runners": _check_runners(),
        "bricks": _check_bricks(),
        "pipelines": _check_pipelines(),
        "deprecated": _check_deprecated(),
        "triggers": _check_triggers(),
        "retention": _check_retention(),
    }
    overall = _aggregate_overall(subsystems)
    return {
        "overall": overall,
        **subsystems,
    }
