"""Help and tips handler module."""
from __future__ import annotations

import logging

from brix.mcp_handlers._shared import (
    _registry,
    _pipeline_dir,
)
from brix.pipeline_store import PipelineStore

logger = logging.getLogger(__name__)


_HELPER_STORAGE_NOTE = (
    "## Helper Storage since v8.0.0\n\n"
    "Helpers sind seit v8.0.0 DB-only.\n"
    "Der `code`-Wert in der DB ist die Source of Truth.\n"
    "Das Feld `script` ist legacy Metadaten und zeigt nicht verlaesslich an, ob Code existiert.\n"
    "`get_helper` zeigt deshalb `has_code`, `code_length`, `code_preview` und `source='db'`.\n"
    "`create_helper` schreibt Inline-Code direkt in die DB und registriert den Helper.\n"
    "`register_helper` ist nur fuer externe, bereits vorhandene Script-Pfade gedacht.\n"
    "SDK-Nutzung sollte Helper ebenfalls ueber die DB-backed Registry erstellen oder aktualisieren.\n"
    "Nutze `legacy_script_path` nur fuer Alt-Kontext oder Migrationen."
)


def _get_help_topics() -> tuple[dict[str, str], dict[str, str]]:
    """Return (topics_dict, descriptions_dict) — from DB (DB-First).

    Falls back to empty dicts if DB is not available.
    """
    topics: dict[str, str] = {}
    descriptions: dict[str, str] = {}
    try:
        from brix.db import BrixDB
        db = BrixDB()
        rows = db.help_topics_list()
        if rows:
            for r in rows:
                topics[r["name"]] = r["content"]
                descriptions[r["name"]] = r["title"]
    except Exception as e:
        logger.debug("Could not load help_topics from DB: %s", e)
    helper_topic = topics.get("helpers", "")
    if _HELPER_STORAGE_NOTE not in helper_topic:
        topics["helpers"] = (
            f"{helper_topic}\n\n{_HELPER_STORAGE_NOTE}".strip()
            if helper_topic
            else f"=== Helper-Scripts ===\n\n{_HELPER_STORAGE_NOTE}"
        )
        descriptions.setdefault(
            "helpers",
            "create_helper, register_helper, get_helper, DB-only helper storage.",
        )
    return topics, descriptions


def _recent_and_custom_bricks(all_bricks: list) -> list[str]:
    """Highlight custom bricks and recently added bricks in get_tips.

    Fix 4: list_all() now reloads custom bricks from DB on every call (Fix 1),
    so we derive custom bricks directly from the already-fresh all_bricks list
    instead of a separate raw DB query.
    """
    lines: list[str] = []
    # Custom bricks: system=False in the already-refreshed all_bricks list
    custom = [b for b in all_bricks if not getattr(b, "system", True)]
    if custom:
        lines.append("## CUSTOM BRICKS (vom User/LLM erstellt)")
        for b in custom:
            ns = b.get("namespace", "") if isinstance(b, dict) else getattr(b, "namespace", "")
            desc = (b.get("description", "") if isinstance(b, dict) else getattr(b, "description", ""))[:60]
            name = b.get("name", "") if isinstance(b, dict) else b.name
            lines.append(f"  - {name} [{ns}] — {desc}")
        lines.append("  Nutze diese BEVOR du einen neuen Helper schreibst!")
        lines.append("")

    # Recently added (last 7 days) — check created_at if available
    try:
        from datetime import datetime, timedelta, timezone
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        recent = []
        for b in all_bricks:
            created = getattr(b, "created_at", None)
            if created:
                try:
                    if isinstance(created, str):
                        # Parse ISO format
                        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    else:
                        dt = created
                    if dt > cutoff:
                        recent.append(b)
                except (ValueError, TypeError):
                    pass
        if recent:
            lines.append("## NEU HINZUGEFÜGT (letzte 7 Tage)")
            for b in recent:
                lines.append(f"  - {b.name} — {getattr(b, 'description', '')[:60]}")
            lines.append("")
    except Exception:
        pass

    return lines


def _load_db_tips() -> list[str]:
    """Load tips from the DB tips table and format them as lines.

    Returns formatted lines: ## category / ### title / content
    Grouped by category, ordered by priority DESC.
    """
    lines: list[str] = []
    try:
        from brix.db import BrixDB
        db = BrixDB()
        tips = db.tip_list(active_only=True)
        if not tips:
            return lines
        current_category = None
        for tip in tips:
            cat = tip["category"]
            if cat != current_category:
                lines.append(f"## {cat}")
                current_category = cat
            # For single-tip categories, put content directly under ##
            # For multi-tip categories, use ### title
            same_cat_tips = [t for t in tips if t["category"] == cat]
            if len(same_cat_tips) > 1:
                lines.append(f"### {tip['title']}")
            content_lines = tip["content"].split("\n")
            for cl in content_lines:
                lines.append(f"  {cl}" if cl.strip() else "")
            lines.append("")
    except Exception as e:
        logger.debug("Could not load tips from DB: %s", e)
    return lines


def _load_registry_content() -> list[str]:
    """Load lessons learned, error patterns, and best practices from registry tables.

    Returns formatted markdown lines for inclusion in get_tips output.
    Returns empty list if no entries exist (no empty headers).
    """
    lines: list[str] = []
    try:
        from brix.db import BrixDB
        db = BrixDB()

        # --- Lessons Learned ---
        lessons = db.registry_list("lessons_learned")
        if lessons:
            lines.append("## LESSONS LEARNED")
            for entry in lessons:
                lines.append(f"### {entry['name']}")
                if entry.get("description"):
                    lines.append(f"  {entry['description']}")
                content = entry.get("content")
                if content and isinstance(content, dict):
                    for k, v in content.items():
                        lines.append(f"  {k}: {v}")
                elif content and isinstance(content, str):
                    for cl in content.split("\n"):
                        lines.append(f"  {cl}" if cl.strip() else "")
                lines.append("")

        # --- Error Patterns ---
        errors = db.registry_list("error_patterns")
        if errors:
            lines.append("## ERROR PATTERNS")
            for entry in errors:
                lines.append(f"### {entry['name']}")
                if entry.get("description"):
                    lines.append(f"  {entry['description']}")
                content = entry.get("content")
                if content and isinstance(content, dict):
                    if "solution" in content:
                        lines.append(f"  Solution: {content['solution']}")
                    for k, v in content.items():
                        if k != "solution":
                            lines.append(f"  {k}: {v}")
                elif content and isinstance(content, str):
                    for cl in content.split("\n"):
                        lines.append(f"  {cl}" if cl.strip() else "")
                lines.append("")

        # --- Best Practices ---
        practices = db.registry_list("best_practices")
        if practices:
            lines.append("## BEST PRACTICES")
            for entry in practices:
                lines.append(f"### {entry['name']}")
                if entry.get("description"):
                    lines.append(f"  {entry['description']}")
                content = entry.get("content")
                if content and isinstance(content, dict):
                    for k, v in content.items():
                        lines.append(f"  {k}: {v}")
                elif content and isinstance(content, str):
                    for cl in content.split("\n"):
                        lines.append(f"  {cl}" if cl.strip() else "")
                lines.append("")

        # --- Workaround Patterns ---
        patterns = [
            entry
            for entry in db.registry_list("patterns")
            if (
                "workaround" in entry.get("tags", [])
                or (
                    isinstance(entry.get("content"), dict)
                    and entry["content"].get("kind") == "workaround_pattern"
                )
            )
        ]
        if patterns:
            lines.append("## WORKAROUND PATTERNS")
            for entry in patterns:
                lines.append(f"### {entry['name']}")
                if entry.get("description"):
                    lines.append(f"  {entry['description']}")
                content = entry.get("content")
                if isinstance(content, dict):
                    trigger_codes = content.get("trigger_codes") or []
                    if trigger_codes:
                        lines.append(f"  Trigger codes: {', '.join(trigger_codes)}")
                    if content.get("repair_hint"):
                        lines.append(f"  Repair hint: {content['repair_hint']}")
                lines.append("")

    except Exception as e:
        logger.debug("Could not load registry content: %s", e)
    return lines


def _build_authoring_guidance() -> tuple[list[str], dict]:
    """Build actionable metadata/reuse guidance for get_tips."""
    try:
        from brix.connections import ConnectionManager
        from brix.db import BrixDB
        from brix.helper_inventory import build_helper_inventory
        from brix.metadata_enforcement import assess_metadata_enforcement

        db = BrixDB()
        metadata_gaps: list[dict] = []

        def collect(entity_type: str, entity_name: str, base_data: dict, metadata: dict | None = None) -> None:
            assessment = assess_metadata_enforcement(
                entity_type,
                base_data=base_data,
                existing_metadata=metadata or {},
                operation="update",
            )
            if not assessment.violations:
                return
            severities = {violation.severity for violation in assessment.violations}
            if "error" in severities:
                highest = "error"
            elif "warning" in severities:
                highest = "warning"
            else:
                highest = "info"
            metadata_gaps.append(
                {
                    "entity_type": entity_type,
                    "entity_id": entity_name,
                    "missing_fields": [violation.field for violation in assessment.violations],
                    "highest_severity": highest,
                    "repair_prompts": list(assessment.repair_prompts),
                }
            )

        for pipeline in db.list_pipelines():
            collect(
                "pipeline",
                str(pipeline.get("name") or ""),
                {
                    "project": pipeline.get("project", ""),
                    "description": pipeline.get("description", ""),
                },
                db.entity_metadata_get("pipeline", str(pipeline.get("name") or "")) or {},
            )

        for brick in db.brick_definitions_list():
            collect(
                "brick",
                str(brick.get("name") or ""),
                {
                    "description": brick.get("description", ""),
                    "input_type": brick.get("input_type", ""),
                    "output_type": brick.get("output_type", ""),
                    "when_NOT_to_use": brick.get("when_NOT_to_use", ""),
                    "examples": brick.get("examples", []),
                    "project": brick.get("project", ""),
                },
                db.entity_metadata_get("brick", str(brick.get("name") or "")) or {},
            )

        for helper in db.list_helpers():
            collect(
                "helper",
                str(helper.get("name") or ""),
                {
                    "description": helper.get("description", ""),
                    "project": helper.get("project", ""),
                    "reason_not_a_brick": helper.get("reason_not_a_brick", ""),
                    "brick_candidate_ref": helper.get("brick_candidate_ref", ""),
                    "governance_status": helper.get("governance_status", ""),
                },
                db.entity_metadata_get("helper", str(helper.get("name") or "")) or {},
            )

        for connection in ConnectionManager(db).list():
            collect(
                "connection",
                str(connection.get("name") or ""),
                {
                    "description": connection.get("description", ""),
                    "project": connection.get("project", ""),
                },
                db.entity_metadata_get("connection", str(connection.get("name") or "")) or {},
            )

        for topic in db.help_topics_list():
            collect(
                "help_topic",
                str(topic.get("name") or ""),
                {"title": topic.get("title", "")},
                db.entity_metadata_get("help_topic", str(topic.get("name") or "")) or {},
            )

        for entry in db.knowledge_entity_list():
            if entry.get("entity_type") not in {"intent", "task", "decision", "workaround", "reuse"}:
                continue
            collect(
                str(entry.get("entity_type") or ""),
                str(entry.get("name") or entry.get("id") or ""),
                {
                    "title": entry.get("title", ""),
                    "project": entry.get("project", ""),
                    "owner": entry.get("owner", ""),
                    "rationale": entry.get("rationale", ""),
                    "lifecycle_stage": entry.get("lifecycle_stage", ""),
                    "status": entry.get("status", ""),
                },
                db.entity_metadata_get(str(entry.get("entity_type") or ""), str(entry.get("name") or "")) or {},
            )

        severity_rank = {"error": 0, "warning": 1, "info": 2}
        entity_rank = {
            "pipeline": 0,
            "helper": 1,
            "brick": 2,
            "connection": 3,
            "help_topic": 4,
            "intent": 5,
            "decision": 6,
            "task": 7,
            "workaround": 8,
            "reuse": 9,
        }
        metadata_gaps.sort(
            key=lambda item: (
                severity_rank.get(item["highest_severity"], 3),
                entity_rank.get(item["entity_type"], 99),
                -len(item["missing_fields"]),
                item["entity_id"],
            )
        )

        inventory = build_helper_inventory(db)
        reuse_candidates = [
            {
                "helper": item.name,
                "family": item.family,
                "migration_candidacy": item.migration_candidacy,
                "signals": list(item.signals),
                "used_by_pipelines": list(item.used_by_pipelines),
            }
            for item in inventory.items
            if item.strategic_category == "brick_candidate"
        ]
        reuse_candidates.sort(key=lambda item: (-len(item["used_by_pipelines"]), item["helper"]))

        lines: list[str] = []
        if metadata_gaps or reuse_candidates:
            lines.extend(
                [
                    "## AUTHORING GUIDANCE",
                    "  Nutze `brix__get_missing_metadata`, `brix__repair_component_metadata` und `brix__record_reuse_decision` bevor du neue Komponenten baust oder aktivierst.",
                    "",
                ]
            )
        if metadata_gaps:
            lines.append("### METADATA GAPS")
            for item in metadata_gaps[:5]:
                lines.append(
                    f"  - {item['entity_type']}:{item['entity_id']} "
                    f"[{item['highest_severity']}] fehlt: {', '.join(item['missing_fields'])}"
                )
                lines.append(
                    f"    get_missing_metadata(entity_type='{item['entity_type']}', entity_id='{item['entity_id']}')"
                )
                lines.append(
                    f"    repair_component_metadata(entity_type='{item['entity_type']}', entity_id='{item['entity_id']}', ...)"
                )
            lines.append("")
        if reuse_candidates:
            lines.append("### REUSE / BRICK CANDIDATES")
            for item in reuse_candidates[:5]:
                used_by = ", ".join(item["used_by_pipelines"]) if item["used_by_pipelines"] else "noch in keiner DB-Pipeline"
                lines.append(
                    f"  - helper:{item['helper']} Familie={item['family']} candidacy={item['migration_candidacy']} usage={used_by}"
                )
                lines.append(
                    "    Prüfe Brick-Promotion oder dokumentiere die Entscheidung mit "
                    "`record_reuse_decision(entity_type='helper', entity_id='...', decision_outcome='...')`."
                )
            lines.append("")

        return (
            lines,
            {
                "metadata_gaps": metadata_gaps[:10],
                "reuse_candidates": reuse_candidates[:10],
                "tool_recommendations": [
                    "brix__get_missing_metadata",
                    "brix__repair_component_metadata",
                    "brix__record_reuse_decision",
                ],
            },
        )
    except Exception as exc:
        logger.debug("Could not build authoring guidance: %s", exc)
        return [], {"metadata_gaps": [], "reuse_candidates": [], "tool_recommendations": []}


async def _handle_get_tips(arguments: dict) -> dict:
    """Return usage tips and best practices for Brix."""
    # Gather brick categories
    all_bricks = _registry.list_all()
    categories: dict[str, int] = {}
    for b in all_bricks:
        categories[b.category] = categories.get(b.category, 0) + 1

    category_lines = [
        f"  - {cat}: {count} brick(s)" for cat, count in sorted(categories.items())
    ]

    # List saved pipelines (from all search paths, respecting current PIPELINE_DIR)
    _tips_store = PipelineStore(pipelines_dir=_pipeline_dir())
    pipeline_names = [p["name"] for p in _tips_store.list_all()]

    # Check for legacy step-type usage (T-BRIX-DB-05d)
    legacy_alert_lines: list[str] = []
    try:
        from brix.db import BrixDB as _BrixDB
        _dep_db = _BrixDB()
        _dep_count = _dep_db.get_deprecated_count()
        if _dep_count > 0:
            _dep_entries = _dep_db.get_deprecated_usage()
            legacy_alert_lines.append(
                f"⚠️  LEGACY ALERT: {_dep_count} Step(s) nutzen alte Step-Types. "
                "Nutze update_step um zu migrieren:"
            )
            for entry in _dep_entries:
                legacy_alert_lines.append(
                    f"  - Pipeline '{entry['pipeline_name']}' Step '{entry['step_id']}': "
                    f"'{entry['old_type']}' → '{entry['new_type']}'"
                )
            legacy_alert_lines.append("")
    except Exception:
        pass  # Never break get_tips over tracking errors

    # T-BRIX-ORG-01: Project overview
    project_overview_lines: list[str] = []
    try:
        from brix.db import BrixDB as _BrixDB
        _proj_db = _BrixDB()
        proj_stats = _proj_db.get_project_stats()
        if proj_stats:
            project_overview_lines.append("## PROJEKTE")
            for proj, counts in sorted(proj_stats.items()):
                proj_label = proj if proj else "(unassigned)"
                p_cnt = counts.get("pipelines", 0)
                h_cnt = counts.get("helpers", 0)
                project_overview_lines.append(
                    f"  - {proj_label}: {p_cnt} pipeline(s), {h_cnt} helper(s)"
                )
            project_overview_lines.append(
                "  Nutze list_pipelines(project=...) oder list_helpers(project=...) zum Filtern."
            )
            project_overview_lines.append("")

        # T-BRIX-ORG-02: Show available project/tag/group definitions
        try:
            org_entries = _proj_db.org_registry_list()
            if org_entries:
                known_projects = [e for e in org_entries if e["entry_type"] == "project"]
                known_tags = [e for e in org_entries if e["entry_type"] == "tag"]
                known_groups = [e for e in org_entries if e["entry_type"] == "group"]
                if known_projects:
                    project_overview_lines.append("## BEKANNTE PROJEKTE (für 'project' Parameter)")
                    for p in known_projects:
                        project_overview_lines.append(f"  - {p['name']}: {p['description']}")
                    project_overview_lines.append("")
                if known_tags:
                    tag_names = ", ".join(t["name"] for t in known_tags)
                    project_overview_lines.append(f"## BEKANNTE TAGS: {tag_names}")
                    project_overview_lines.append("")
                if known_groups:
                    project_overview_lines.append("## BEKANNTE GROUPS")
                    for g in known_groups:
                        project_overview_lines.append(f"  - {g['name']}: {g['description']}")
                    project_overview_lines.append("")
            else:
                project_overview_lines.append(
                    "HINT: Nutze brix__org(action='seed') um Standard-Projekte/Tags/Groups zu laden."
                )
                project_overview_lines.append("")
        except Exception:
            pass

        # Warn about entities without project
        try:
            no_proj_p = sum(
                1 for p, c in proj_stats.items() if not p
                for _ in range(c.get("pipelines", 0))
            )
            no_proj_h = sum(
                1 for p, c in proj_stats.items() if not p
                for _ in range(c.get("helpers", 0))
            )
            if no_proj_p > 0 or no_proj_h > 0:
                project_overview_lines.append(
                    f"⚠ {no_proj_p} Pipeline(s) und {no_proj_h} Helper haben kein Projekt. "
                    "Nutze update_pipeline/update_helper um project zuzuordnen."
                )
                project_overview_lines.append("")
        except Exception:
            pass
    except Exception:
        pass  # Never break get_tips

    # T-BRIX-INT-01: Show integrity issues at the top of tips
    integrity_alert_lines: list[str] = []
    try:
        from brix.db import BrixDB as _BrixDB
        from brix.integrity import run_integrity_checks as _run_integrity
        _int_db = _BrixDB()
        _int_result = _run_integrity(_int_db)
        if not _int_result["ok"]:
            _n_issues = len(_int_result["issues"])
            integrity_alert_lines.append(
                f"⚠ INTEGRITY: {_n_issues} Problem(e) gefunden. "
                "Führe brix__run_integrity_check aus für Details."
            )
            for _iss in _int_result["issues"]:
                integrity_alert_lines.append(f"  - [{_iss['code']}] {_iss['message']}")
            integrity_alert_lines.append("")
    except Exception:
        pass  # Never break get_tips

    # T-BRIX-TIPS-01: Load static tips from DB instead of hardcoded lines
    db_tip_lines = _load_db_tips()

    # T-BRIX-TIPS-02: Load registry content (lessons, error patterns, best practices)
    registry_lines = _load_registry_content()
    guidance_lines, guidance = _build_authoring_guidance()

    helper_tip_lines = [
        "## HELPERS (v8.0.0+)",
        "  Helpers sind DB-only. Der DB-`code` ist die Source of Truth.",
        "  Das alte `script`-Feld ist legacy Metadaten und kein verlaesslicher Code-Indikator.",
        "  `get_helper` zeigt `has_code`, `code_length`, `code_preview` und `source='db'`.",
        "  `list_helpers(include_inventory=true)` zeigt Helper-Familien, Brick-Kandidaten und fehlende Metadaten.",
        "  `list_helpers(include_reuse_candidates=true)` zeigt wiederholte Helper-, SQL- und Step-Sequenz-Kandidaten.",
        "",
    ]
    try:
        from brix.helper_inventory import build_helper_inventory

        inventory = build_helper_inventory()
        summary = inventory.summary
        if summary.get("total", 0) > 0:
            helper_tip_lines.extend(
                [
                    "## HELPER-INVENTAR",
                    (
                        f"  Total: {summary.get('total', 0)}, "
                        f"Brick-Kandidaten: {summary.get('brick_candidate', 0)}, "
                        f"Legacy-Review: {summary.get('legacy_review', 0)}, "
                        f"Metadata offen: {summary.get('metadata_required', 0)}"
                    ),
                    "  Nutze `list_helpers(include_inventory=true)` bevor du neue Helper erstellst.",
                    "",
                ]
            )
    except Exception:
        pass

    tips = [
        *integrity_alert_lines,
        *legacy_alert_lines,
        *project_overview_lines,
        *guidance_lines,
        "=== Brix Quick Reference ===",
        "",
        *db_tip_lines,
        *helper_tip_lines,
        *registry_lines,
        "## VERFÜGBARE BRICK-KATEGORIEN",
        *category_lines,
        f"  Total bricks: {len(all_bricks)}",
        "",
        *_recent_and_custom_bricks(all_bricks),
        "",
        "## GESPEICHERTE PIPELINES",
        (
            "\n".join(f"  - {name}" for name in pipeline_names)
            if pipeline_names
            else "  (keine — brix__create_pipeline nutzen)"
        ),
    ]

    return {
        "tips": tips,
        "brick_count": len(all_bricks),
        "pipeline_count": len(pipeline_names),
        "categories": list(categories.keys()),
        "guidance": guidance,
    }


# ------------------------------------------------------------------
# T-BRIX-TIPS-01: Tip CRUD MCP handlers
# ------------------------------------------------------------------

async def _handle_create_tip(arguments: dict) -> dict:
    """Create a new tip."""
    from brix.db import BrixDB
    category = arguments.get("category")
    title = arguments.get("title")
    content = arguments.get("content")
    if not category or not title or not content:
        return {"success": False, "error": "category, title, and content are required."}
    priority = arguments.get("priority", 5)
    is_active = arguments.get("is_active", True)
    db = BrixDB()
    tip = db.tip_create(category, title, content, priority, is_active)
    return {"success": True, "tip": tip}


async def _handle_update_tip(arguments: dict) -> dict:
    """Update an existing tip."""
    from brix.db import BrixDB
    tip_id = arguments.get("id")
    if not tip_id:
        return {"success": False, "error": "id is required."}
    fields = {k: v for k, v in arguments.items() if k != "id" and k != "source"}
    db = BrixDB()
    tip = db.tip_update(tip_id, **fields)
    if tip is None:
        return {"success": False, "error": f"Tip '{tip_id}' not found."}
    return {"success": True, "tip": tip}


async def _handle_delete_tip(arguments: dict) -> dict:
    """Delete a tip."""
    from brix.db import BrixDB
    tip_id = arguments.get("id")
    if not tip_id:
        return {"success": False, "error": "id is required."}
    db = BrixDB()
    deleted = db.tip_delete(tip_id)
    if not deleted:
        return {"success": False, "error": f"Tip '{tip_id}' not found."}
    return {"success": True, "deleted": tip_id}


async def _handle_list_tips(arguments: dict) -> dict:
    """List tips with optional filters."""
    from brix.db import BrixDB
    category = arguments.get("category")
    active_only = arguments.get("active_only", True)
    db = BrixDB()
    tips = db.tip_list(category=category, active_only=active_only)
    return {"success": True, "tips": tips, "count": len(tips)}


async def _handle_get_help(arguments: dict) -> dict:
    """Return detailed help for a specific topic, or list all topics."""
    topic = arguments.get("topic")
    topics, descriptions = _get_help_topics()

    if not topic:
        topic_list = [
            f"  {name:<20} — {desc}"
            for name, desc in descriptions.items()
        ]
        return {
            "topics": list(descriptions.keys()),
            "descriptions": descriptions,
            "message": (
                "Kein Topic angegeben. Verfügbare Topics:\n\n"
                + "\n".join(topic_list)
                + "\n\nNutzung: brix__get_help(topic='quick-start')"
            ),
        }

    if topic not in topics:
        available = ", ".join(f"'{t}'" for t in sorted(topics.keys()))
        return {
            "error": f"Unbekanntes Topic: '{topic}'. Verfügbare Topics: {available}",
            "available_topics": sorted(topics.keys()),
        }

    return {
        "topic": topic,
        "content": topics[topic],
        "description": descriptions.get(topic, ""),
        "all_topics": list(descriptions.keys()),
    }
