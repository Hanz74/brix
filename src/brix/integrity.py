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
        _check_pipelines_without_yaml(db, issues, auto_fixed)
    except Exception as exc:
        logger.warning("integrity: check_pipelines_without_yaml failed: %s", exc)

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

def _check_pipelines_without_yaml(
    db: "BrixDB",
    issues: list[dict],
    auto_fixed: list[str],
) -> None:
    """Find pipelines in DB that have no yaml_content. Try to auto-fix from disk."""
    import yaml as _yaml

    yaml_files = _collect_yaml_files()
    fixed = 0
    missing = []

    for p in db.list_pipelines():
        name = p["name"]
        content = db.get_pipeline_yaml_content(name)
        if content:
            continue  # already has content

        # Try to import from disk
        if name in yaml_files:
            try:
                raw = yaml_files[name].read_text(encoding="utf-8")
                data = _yaml.safe_load(raw) or {}
                requirements = data.get("requirements", [])
                if not isinstance(requirements, list):
                    requirements = []
                db.upsert_pipeline(
                    name=name,
                    path=str(yaml_files[name]),
                    requirements=requirements,
                    yaml_content=raw,
                )
                auto_fixed.append(f"pipeline_yaml_imported:{name}")
                fixed += 1
                logger.debug("integrity: imported yaml_content for pipeline '%s'", name)
            except Exception as exc:
                missing.append(name)
                logger.warning(
                    "integrity: could not import yaml for pipeline '%s': %s", name, exc
                )
        else:
            missing.append(name)

    if missing:
        issues.append({
            "code": "PIPELINE_NO_YAML",
            "message": (
                f"{len(missing)} pipeline(s) have no yaml_content and no disk file: "
                + ", ".join(missing[:5])
                + ("..." if len(missing) > 5 else "")
            ),
            "severity": "warning",
            "pipelines": missing,
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
    """Find pipeline steps that reference non-existent brick types."""
    import yaml as _yaml

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
        yaml_content = db.get_pipeline_yaml_content(p["name"])
        if not yaml_content:
            continue
        try:
            data = _yaml.safe_load(yaml_content) or {}
        except Exception:
            continue

        steps = data.get("steps", [])
        if not isinstance(steps, list):
            continue

        _collect_bad_brick_refs(p["name"], steps, all_known, bad_refs)

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


def _collect_bad_brick_refs(
    pipeline_name: str,
    steps: list,
    known: set[str],
    bad_refs: list[str],
) -> None:
    """Recursively check step types against known bricks."""
    for step in steps:
        if not isinstance(step, dict):
            continue
        step_type = step.get("type", "")
        if step_type and step_type not in known:
            bad_refs.append(f"{pipeline_name}/{step.get('id', '?')}:{step_type}")

        # Recurse into nested structures
        if "sequence" in step and isinstance(step["sequence"], list):
            _collect_bad_brick_refs(pipeline_name, step["sequence"], known, bad_refs)
        if "choices" in step and isinstance(step["choices"], list):
            for choice in step["choices"]:
                if isinstance(choice, dict) and "steps" in choice:
                    _collect_bad_brick_refs(pipeline_name, choice["steps"], known, bad_refs)
        if "default_steps" in step and isinstance(step["default_steps"], list):
            _collect_bad_brick_refs(pipeline_name, step["default_steps"], known, bad_refs)
        if "sub_steps" in step and isinstance(step["sub_steps"], list):
            _collect_bad_brick_refs(pipeline_name, step["sub_steps"], known, bad_refs)


def _check_helper_references(
    db: "BrixDB",
    issues: list[dict],
) -> None:
    """Find pipeline steps that reference non-existent helpers."""
    import yaml as _yaml

    known_helpers = {h["name"] for h in db.list_helpers()}
    if not known_helpers:
        return  # No helpers registered — skip check

    bad_refs: list[str] = []

    for p in db.list_pipelines():
        yaml_content = db.get_pipeline_yaml_content(p["name"])
        if not yaml_content:
            continue
        try:
            data = _yaml.safe_load(yaml_content) or {}
        except Exception:
            continue

        steps = data.get("steps", [])
        if not isinstance(steps, list):
            continue

        _collect_bad_helper_refs(p["name"], steps, known_helpers, bad_refs)

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


def _collect_bad_helper_refs(
    pipeline_name: str,
    steps: list,
    known_helpers: set[str],
    bad_refs: list[str],
) -> None:
    """Recursively check helper references in steps."""
    for step in steps:
        if not isinstance(step, dict):
            continue
        helper_ref = step.get("helper")
        if helper_ref and isinstance(helper_ref, str) and helper_ref not in known_helpers:
            bad_refs.append(
                f"{pipeline_name}/{step.get('id', '?')}:helper={helper_ref}"
            )

        # Recurse into nested structures
        if "sequence" in step and isinstance(step["sequence"], list):
            _collect_bad_helper_refs(pipeline_name, step["sequence"], known_helpers, bad_refs)
        if "choices" in step and isinstance(step["choices"], list):
            for choice in step["choices"]:
                if isinstance(choice, dict) and "steps" in choice:
                    _collect_bad_helper_refs(pipeline_name, choice["steps"], known_helpers, bad_refs)
        if "default_steps" in step and isinstance(step["default_steps"], list):
            _collect_bad_helper_refs(pipeline_name, step["default_steps"], known_helpers, bad_refs)
        if "sub_steps" in step and isinstance(step["sub_steps"], list):
            _collect_bad_helper_refs(pipeline_name, step["sub_steps"], known_helpers, bad_refs)
