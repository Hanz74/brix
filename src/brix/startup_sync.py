"""Startup Disk-DB Sync for Brix — T-BRIX-INTEGRITY-01.

Runs on every container start (after migrations and seeding, before serving).
Idempotent: safe to run multiple times with the same result.

Syncs:
  - Helpers: disk files not in DB get auto-registered
  - Pipelines: disk YAML not in DB get imported
  - Descriptions: empty DB descriptions backfilled from YAML content
  - Orphans: triggers pointing to missing pipelines, helpers without files
  - Test artifacts: known test helper files cleaned from ~/.brix/helpers/
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from brix.db import BrixDB

logger = logging.getLogger(__name__)

# Directories to scan for helpers
_HELPER_SEARCH_PATHS = [
    Path.home() / ".brix" / "helpers",
    Path("/app/helpers"),
]

# Directories to scan for pipelines (recursive for _system/ etc.)
_PIPELINE_SEARCH_PATHS = [
    Path.home() / ".brix" / "pipelines",
    Path("/app/pipelines"),
]

# Test artifact patterns for helper files — matched against the stem (no .py)
_TEST_HELPER_PATTERNS = (
    "my-helper",
    "my_helper",
    "no-project-helper",
    "no_project_helper",
    "org_helper",
    "with-project-helper",
    "with_project_helper",
    "test_mcp_output",
)

_TEST_HELPER_PREFIXES = (
    "debug_",
    "test_",
    "xtest_",
    "mock_",
)


def _is_test_helper(name: str) -> bool:
    """Return True if a helper filename (stem) looks like a test artifact."""
    name_lower = name.lower().replace("-", "_")
    if name_lower in (p.replace("-", "_") for p in _TEST_HELPER_PATTERNS):
        return True
    return any(name_lower.startswith(prefix) for prefix in _TEST_HELPER_PREFIXES)


def _scan_helper_files() -> dict[str, Path]:
    """Return {helper_name: Path} for all .py files in helper dirs."""
    found: dict[str, Path] = {}
    for search_dir in _HELPER_SEARCH_PATHS:
        if not search_dir.exists():
            continue
        for f in sorted(search_dir.glob("*.py")):
            name = f.stem
            if name.startswith("__"):
                continue
            if name not in found:
                found[name] = f
    return found


def _scan_pipeline_files() -> dict[str, Path]:
    """Return {pipeline_name: Path} for all YAML files in pipeline dirs (recursive)."""
    found: dict[str, Path] = {}
    for search_dir in _PIPELINE_SEARCH_PATHS:
        if not search_dir.exists():
            continue
        for ext in ("**/*.yaml", "**/*.yml"):
            for f in sorted(search_dir.glob(ext)):
                name = f.stem
                if name not in found:
                    found[name] = f
    return found


def _extract_description_from_yaml(yaml_text: str) -> str:
    """Extract description from pipeline YAML content string."""
    try:
        raw = yaml.safe_load(yaml_text) or {}
    except Exception:
        return ""
    if not isinstance(raw, dict):
        return ""
    return (raw.get("description") or "").strip()


def run_startup_sync(db: "BrixDB") -> dict:
    """Run disk-DB sync at startup. Returns summary dict.

    This function is idempotent and safe to call on every container start.
    """
    summary = {
        "helpers_registered": 0,
        "pipelines_imported": 0,
        "descriptions_backfilled": 0,
        "orphan_triggers": 0,
        "orphan_helpers": 0,
        "test_artifacts_cleaned": 0,
    }

    try:
        summary["helpers_registered"] = _sync_helpers(db)
    except Exception as exc:
        logger.warning("startup_sync: helper sync failed: %s", exc)

    try:
        summary["pipelines_imported"] = _sync_pipelines(db)
    except Exception as exc:
        logger.warning("startup_sync: pipeline sync failed: %s", exc)

    try:
        summary["descriptions_backfilled"] = _backfill_descriptions(db)
    except Exception as exc:
        logger.warning("startup_sync: description backfill failed: %s", exc)

    try:
        orphans = _detect_orphans(db)
        summary["orphan_triggers"] = orphans["triggers"]
        summary["orphan_helpers"] = orphans["helpers"]
    except Exception as exc:
        logger.warning("startup_sync: orphan detection failed: %s", exc)

    try:
        summary["test_artifacts_cleaned"] = _cleanup_test_artifacts()
    except Exception as exc:
        logger.warning("startup_sync: test artifact cleanup failed: %s", exc)

    # Log the summary
    logger.info(
        "startup_sync: helpers_registered=%d, pipelines_imported=%d, "
        "descriptions_backfilled=%d, orphan_triggers=%d, orphan_helpers=%d, "
        "test_artifacts_cleaned=%d",
        summary["helpers_registered"],
        summary["pipelines_imported"],
        summary["descriptions_backfilled"],
        summary["orphan_triggers"],
        summary["orphan_helpers"],
        summary["test_artifacts_cleaned"],
    )

    return summary


def _sync_helpers(db: "BrixDB") -> int:
    """Register helper .py files from disk that are not yet in the DB."""
    disk_helpers = _scan_helper_files()
    db_helpers = {h["name"] for h in db.list_helpers()}

    registered = 0
    for name, path in disk_helpers.items():
        if name in db_helpers:
            continue
        if _is_test_helper(name):
            continue
        try:
            db.upsert_helper(
                name=name,
                script_path=str(path),
                description="",
            )
            logger.info("startup_sync: registered helper '%s' from %s", name, path)
            registered += 1
        except Exception as exc:
            logger.warning("startup_sync: failed to register helper '%s': %s", name, exc)

    return registered


def _sync_pipelines(db: "BrixDB") -> int:
    """Import pipeline YAML files from disk that are not yet in the DB."""
    from brix.seed import _is_test_pipeline

    disk_pipelines = _scan_pipeline_files()
    db_pipelines = {p["name"] for p in db.list_pipelines()}

    imported = 0
    for name, path in disk_pipelines.items():
        if name in db_pipelines:
            continue
        if _is_test_pipeline(name):
            continue
        try:
            content = path.read_text(encoding="utf-8")
            raw = yaml.safe_load(content) or {}
            # Use the YAML name field if present, otherwise file stem
            yaml_name = raw.get("name") or name
            if yaml_name in db_pipelines:
                continue

            description = (raw.get("description") or "").strip()
            project = (raw.get("project") or "").strip()
            tags_raw = raw.get("tags")
            tags = tags_raw if isinstance(tags_raw, list) else None

            db.upsert_pipeline(
                name=yaml_name,
                path=str(path),
                yaml_content=content,
                project=project or None,
                tags=tags,
            )
            # Also update description if available
            if description:
                _set_pipeline_description(db, yaml_name, description)

            logger.info("startup_sync: imported pipeline '%s' from %s", yaml_name, path)
            db_pipelines.add(yaml_name)
            imported += 1
        except Exception as exc:
            logger.warning("startup_sync: failed to import pipeline '%s': %s", name, exc)

    return imported


def _set_pipeline_description(db: "BrixDB", name: str, description: str) -> None:
    """Set the description column on a pipeline row."""
    import sqlite3
    with db._connect() as conn:
        if db._column_exists(conn, "pipeline", "description"):
            conn.execute(
                "UPDATE pipeline SET description=? WHERE name=?",
                (description, name),
            )


def _backfill_descriptions(db: "BrixDB") -> int:
    """For pipelines with empty description but yaml_content containing one, backfill."""
    import sqlite3

    with db._connect() as conn:
        if not db._column_exists(conn, "pipeline", "description"):
            return 0
        if not db._column_exists(conn, "pipeline", "yaml_content"):
            return 0

        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT name, yaml_content FROM pipeline
               WHERE (description IS NULL OR description = '')
                 AND yaml_content IS NOT NULL AND yaml_content != ''"""
        ).fetchall()

    backfilled = 0
    for row in rows:
        name = row["name"]
        yaml_content = row["yaml_content"]
        desc = _extract_description_from_yaml(yaml_content)
        if desc:
            _set_pipeline_description(db, name, desc)
            logger.info("startup_sync: backfilled description for pipeline '%s'", name)
            backfilled += 1

    return backfilled


def _detect_orphans(db: "BrixDB") -> dict:
    """Detect orphaned triggers and helpers. Logs warnings."""
    import sqlite3

    result = {"triggers": 0, "helpers": 0}

    # Orphan triggers: triggers pointing to non-existent pipelines
    pipeline_names = {p["name"] for p in db.list_pipelines()}
    with db._connect() as conn:
        conn.row_factory = sqlite3.Row
        try:
            triggers = conn.execute("SELECT name, pipeline FROM trigger").fetchall()
        except sqlite3.OperationalError:
            triggers = []

    for t in triggers:
        if t["pipeline"] not in pipeline_names:
            logger.warning(
                "startup_sync: orphan trigger '%s' points to non-existent pipeline '%s'",
                t["name"], t["pipeline"],
            )
            result["triggers"] += 1

    # Orphan helpers: DB entries whose script file doesn't exist on disk
    for h in db.list_helpers():
        script_path = h.get("script_path", "")
        if script_path and not Path(script_path).exists():
            logger.warning(
                "startup_sync: orphan helper '%s' — file not found: %s",
                h["name"], script_path,
            )
            result["helpers"] += 1

    return result


def _cleanup_test_artifacts() -> int:
    """Delete test helper files from ~/.brix/helpers/ only."""
    cleanup_dir = Path.home() / ".brix" / "helpers"
    if not cleanup_dir.exists():
        return 0

    cleaned = 0
    for f in sorted(cleanup_dir.glob("*.py")):
        if _is_test_helper(f.stem):
            try:
                f.unlink()
                logger.info("startup_sync: cleaned test artifact %s", f)
                cleaned += 1
            except Exception as exc:
                logger.warning("startup_sync: failed to clean %s: %s", f, exc)

    return cleaned
