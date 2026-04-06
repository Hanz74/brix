"""DB-Integrity checks for Brix — T-BRIX-INT-01.

Runs at container start after seed_if_empty().
Detects and auto-fixes common DB inconsistencies.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brix.db import BrixDB

logger = logging.getLogger(__name__)

# Pipeline search paths mirrored from seed.py
_PIPELINE_SEARCH_PATHS = [
    Path.home() / ".brix" / "pipelines",
    Path("/app/pipelines"),
]


def _collect_yaml_files() -> dict[str, Path]:
    """Return {pipeline_name: Path} for all YAML files on disk."""
    found: dict[str, Path] = {}
    for search_dir in _PIPELINE_SEARCH_PATHS:
        if not search_dir.exists():
            continue
        for ext in ("*.yaml", "*.yml"):
            for f in sorted(search_dir.glob(ext)):
                name = f.stem
                if name not in found:
                    found[name] = f
    return found


def _is_test_pipeline(name: str) -> bool:
    """Return True if the name looks like a test/development artifact."""
    from brix.seed import _is_test_pipeline as _seed_is_test
    return _seed_is_test(name)


def run_integrity_checks(db: "BrixDB") -> dict:
    """Run all DB consistency checks and apply auto-fixes where possible.

    Returns:
        {
            "ok": bool,
            "issues": [{"code": str, "message": str, "severity": str}],
            "auto_fixed": [str],
        }
    """
    issues: list[dict] = []
    auto_fixed: list[str] = []

    try:
        _check_pipelines_without_steps(db, issues, auto_fixed)
    except Exception as exc:
        logger.warning("integrity: check_pipelines_without_steps failed: %s", exc)

    try:
        _check_test_pipelines_in_db(db, issues, auto_fixed)
    except Exception as exc:
        logger.warning("integrity: check_test_pipelines_in_db failed: %s", exc)

    try:
        _check_entities_without_project(db, issues)
    except Exception as exc:
        logger.warning("integrity: check_entities_without_project failed: %s", exc)

    try:
        _check_orphaned_deprecated_usage(db, issues, auto_fixed)
    except Exception as exc:
        logger.warning("integrity: check_orphaned_deprecated_usage failed: %s", exc)

    try:
        _check_brick_references(db, issues)
    except Exception as exc:
        logger.warning("integrity: check_brick_references failed: %s", exc)

    try:
        _check_helper_references(db, issues)
    except Exception as exc:
        logger.warning("integrity: check_helper_references failed: %s", exc)

    try:
        org_issues = _check_entity_org_metadata(db)
        issues.extend(org_issues)
    except Exception as exc:
        logger.warning("integrity: check_entity_org_metadata failed: %s", exc)

    if issues:
        summary = "; ".join(f"[{i['code']}] {i['message']}" for i in issues)
        logger.warning("integrity: %d issue(s) found: %s", len(issues), summary)
    if auto_fixed:
        logger.info("integrity: auto-fixed %d item(s): %s", len(auto_fixed), auto_fixed)

    return {
        "ok": len(issues) == 0,
        "issues": issues,
        "auto_fixed": auto_fixed,
    }


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_pipelines_without_steps(
    db: "BrixDB",
    issues: list[dict],
    auto_fixed: list[str],
) -> None:
    """Check that each pipeline has at least one pipeline_step row (T-BRIX-DBO-18).

    yaml_content is no longer the source of truth.  Pipelines that have zero
    step rows are flagged with code "NO_STEP_ROWS" — they may have been created
    without steps or failed to migrate from YAML.
    """
    pipelines_without_steps: list[str] = []

    for p in db.list_pipelines():
        pipeline_id = p.get("id")
        if not pipeline_id:
            continue
        try:
            step_rows = db.get_steps(pipeline_id)
        except Exception:
            continue
        if len(step_rows) == 0:
            pipelines_without_steps.append(p["name"])

    if pipelines_without_steps:
        issues.append({
            "code": "NO_STEP_ROWS",
            "message": (
                f"{len(pipelines_without_steps)} pipeline(s) have 0 step rows in DB: "
                + ", ".join(pipelines_without_steps[:5])
                + ("..." if len(pipelines_without_steps) > 5 else "")
            ),
            "severity": "warning",
            "pipelines": pipelines_without_steps,
        })


def _check_test_pipelines_in_db(
    db: "BrixDB",
    issues: list[dict],
    auto_fixed: list[str],
) -> None:
    """Find test/dev pipelines in the production DB and delete them."""
    test_names = [p["name"] for p in db.list_pipelines() if _is_test_pipeline(p["name"])]
    if not test_names:
        return

    deleted = 0
    for name in test_names:
        try:
            db.delete_pipeline(name)
            auto_fixed.append(f"test_pipeline_deleted:{name}")
            deleted += 1
            logger.debug("integrity: deleted test pipeline '%s'", name)
        except Exception as exc:
            logger.warning("integrity: could not delete test pipeline '%s': %s", name, exc)

    if deleted < len(test_names):
        remaining = len(test_names) - deleted
        issues.append({
            "code": "TEST_PIPELINE_IN_DB",
            "message": (
                f"{remaining} test pipeline(s) could not be removed: "
                + ", ".join(test_names[:5])
            ),
            "severity": "warning",
            "pipelines": test_names,
        })


def _check_entities_without_project(
    db: "BrixDB",
    issues: list[dict],
) -> None:
    """Report pipelines and helpers that have no project assigned."""
    no_proj_pipelines = [
        p["name"] for p in db.list_pipelines() if not p.get("project")
    ]
    no_proj_helpers = [
        h["name"] for h in db.list_helpers() if not h.get("project")
    ]

    if no_proj_pipelines:
        issues.append({
            "code": "ENTITY_NO_PROJECT",
            "message": (
                f"{len(no_proj_pipelines)} pipeline(s) have no project assigned. "
                "Use update_pipeline to set a project."
            ),
            "severity": "info",
            "pipelines": no_proj_pipelines,
        })

    if no_proj_helpers:
        issues.append({
            "code": "HELPER_NO_PROJECT",
            "message": (
                f"{len(no_proj_helpers)} helper(s) have no project assigned. "
                "Use update_helper to set a project."
            ),
            "severity": "info",
            "helpers": no_proj_helpers,
        })


def _check_orphaned_deprecated_usage(
    db: "BrixDB",
    issues: list[dict],
    auto_fixed: list[str],
) -> None:
    """Find deprecated_usage entries referencing non-existent pipelines and delete them."""
    try:
        entries = db.get_deprecated_usage()
    except Exception:
        return  # Table may not exist yet

    if not entries:
        return

    existing_names = {p["name"] for p in db.list_pipelines()}
    orphaned = [e for e in entries if e["pipeline_name"] not in existing_names]

    if not orphaned:
        return

    deleted = 0
    failed = []
    for entry in orphaned:
        try:
            import sqlite3
            with db._connect() as conn:  # type: ignore[attr-defined]
                conn.execute(
                    "DELETE FROM deprecated_usage WHERE pipeline_name=? AND step_id=?",
                    (entry["pipeline_name"], entry["step_id"]),
                )
            auto_fixed.append(
                f"deprecated_usage_deleted:{entry['pipeline_name']}/{entry['step_id']}"
            )
            deleted += 1
        except Exception as exc:
            failed.append(entry["pipeline_name"])
            logger.warning(
                "integrity: could not delete orphaned deprecated_usage '%s': %s",
                entry["pipeline_name"],
                exc,
            )

    if failed:
        issues.append({
            "code": "ORPHANED_DEPRECATED_USAGE",
            "message": (
                f"{len(failed)} orphaned deprecated_usage entries could not be deleted: "
                + ", ".join(failed[:5])
            ),
            "severity": "warning",
            "pipelines": failed,
        })


def _check_brick_references(
    db: "BrixDB",
    issues: list[dict],
) -> None:
    """Find pipeline steps that reference non-existent brick types.

    Reads step types from pipeline_step DB rows (T-BRIX-DBO-18 — no yaml_content).
    """
    # Build set of known brick names from DB
    known_bricks: set[str] = set()
    try:
        for b in db.brick_definitions_list():
            known_bricks.add(b["name"])
            # Also add aliases
            for alias in (b.get("aliases") or []):
                known_bricks.add(alias)
    except Exception:
        return  # Can't check without brick DB

    if not known_bricks:
        return  # No bricks to check against

    # Also accept all legacy step types (backward-compat)
    from brix.seed import LEGACY_STEP_TYPE_MAP
    all_known = known_bricks | set(LEGACY_STEP_TYPE_MAP.keys()) | set(LEGACY_STEP_TYPE_MAP.values())

    # Built-in runner names that are always valid
    builtin_runners = {
        "python", "http", "cli", "mcp", "pipeline", "pipeline_group",
        "filter", "transform", "set", "stop", "choose", "parallel", "repeat",
        "notify", "approval", "validate", "specialist", "db_query", "db_upsert",
        "llm_batch", "markitdown", "source", "switch", "merge", "error_handler",
        "wait", "dedup", "aggregate", "flatten", "diff", "respond", "queue", "emit",
        "script.python", "http.request", "mcp.call", "script.cli",
        "flow.filter", "flow.transform", "flow.set", "flow.repeat",
        "flow.choose", "flow.parallel", "flow.pipeline", "flow.pipeline_group",
        "flow.validate", "flow.switch", "flow.merge", "flow.error_handler",
        "flow.wait", "flow.dedup", "flow.aggregate", "flow.flatten", "flow.diff",
        "action.notify", "action.approval", "action.respond",
        "extract.specialist",
        "db.query", "db.upsert", "llm.batch", "markitdown.convert", "source.fetch",
    }
    all_known = all_known | builtin_runners

    bad_refs: list[str] = []

    for p in db.list_pipelines():
        pipeline_id = p.get("id")
        if not pipeline_id:
            continue
        try:
            step_rows = db.get_steps(pipeline_id)
        except Exception:
            continue

        for step_row in step_rows:
            # step_row_to_dict maps column step_type→"type", step_key→"id"
            step_type = step_row.get("type", "")
            step_id = step_row.get("id", "?")
            if step_type and step_type not in all_known:
                bad_refs.append(f"{p['name']}/{step_id}:{step_type}")

    if bad_refs:
        issues.append({
            "code": "UNKNOWN_BRICK_REF",
            "message": (
                f"{len(bad_refs)} step(s) reference unknown brick types: "
                + ", ".join(bad_refs[:5])
                + ("..." if len(bad_refs) > 5 else "")
            ),
            "severity": "warning",
            "steps": bad_refs,
        })


def _check_helper_references(
    db: "BrixDB",
    issues: list[dict],
) -> None:
    """Find pipeline steps that reference non-existent helpers.

    Reads helper references from pipeline_step DB rows (T-BRIX-DBO-18 — no yaml_content).
    """
    known_helpers = {h["name"] for h in db.list_helpers()}
    if not known_helpers:
        return  # No helpers registered — skip check

    bad_refs: list[str] = []

    for p in db.list_pipelines():
        pipeline_id = p.get("id")
        if not pipeline_id:
            continue
        try:
            step_rows = db.get_steps(pipeline_id)
        except Exception:
            continue

        for step_row in step_rows:
            # step_row_to_dict maps column step_key→"id"
            helper_ref = step_row.get("helper")
            step_id = step_row.get("id", "?")
            if helper_ref and isinstance(helper_ref, str) and helper_ref not in known_helpers:
                bad_refs.append(
                    f"{p['name']}/{step_id}:helper={helper_ref}"
                )

    if bad_refs:
        issues.append({
            "code": "UNKNOWN_HELPER_REF",
            "message": (
                f"{len(bad_refs)} step(s) reference unknown helpers: "
                + ", ".join(bad_refs[:5])
                + ("..." if len(bad_refs) > 5 else "")
            ),
            "severity": "warning",
            "steps": bad_refs,
        })


def _check_entity_org_metadata(db: "BrixDB") -> list[dict]:
    """Check pipeline, helper, and trigger_group rows for missing project/description.

    Returns a list of issue dicts, each with keys:
        code, severity, entity_type, entity_name, message
    """
    issues: list[dict] = []

    _TABLES = [
        ("pipeline", "pipeline"),
        ("helper", "helper"),
        ("trigger_group", "trigger_group"),
    ]

    for table, entity_type in _TABLES:
        try:
            with db._connect() as conn:  # type: ignore[attr-defined]
                import sqlite3 as _sqlite3
                conn.row_factory = _sqlite3.Row
                # Check table exists
                tbl_check = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if not tbl_check:
                    continue

                # table comes from _TABLES allowlist above — safe for interpolation
                _ALLOWED = {t[0] for t in _TABLES}
                assert table in _ALLOWED, f"unexpected table: {table}"  # noqa: S101
                rows = conn.execute(  # noqa: S608
                    f"SELECT name, project, description FROM {table}"  # nosec B608
                ).fetchall()

        except Exception as exc:
            logger.warning("integrity: org_metadata check failed for table '%s': %s", table, exc)
            continue

        # pipeline and helper missing-project is already reported by
        # _check_entities_without_project (codes ENTITY_NO_PROJECT /
        # HELPER_NO_PROJECT).  Only report MISSING_PROJECT here for entity
        # types NOT covered by that check (e.g. trigger_group).
        _skip_project_check = {"pipeline", "helper"}

        for row in rows:
            name = row["name"]
            project = (row["project"] or "").strip()
            description = (row["description"] or "").strip()

            if not project and entity_type not in _skip_project_check:
                issues.append({
                    "code": "MISSING_PROJECT",
                    "severity": "info",
                    "entity_type": entity_type,
                    "entity_name": name,
                    "message": (
                        f"{entity_type} '{name}' has no project assigned."
                    ),
                })

            if not description:
                issues.append({
                    "code": "MISSING_DESCRIPTION",
                    "severity": "info",
                    "entity_type": entity_type,
                    "entity_name": name,
                    "message": (
                        f"{entity_type} '{name}' has no description."
                    ),
                })

    return issues


