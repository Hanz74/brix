"""Help and tips handler module."""
from __future__ import annotations

import logging

from brix.mcp_handlers._shared import (
    _registry,
    _pipeline_dir,
)
from brix.pipeline_store import PipelineStore

logger = logging.getLogger(__name__)


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
        lines.append(f"  Nutze diese BEVOR du einen neuen Helper schreibst!")
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

    tips = [
        *legacy_alert_lines,
        *project_overview_lines,
        "=== Brix Quick Reference ===",
        "",
        "## BRICK-FIRST — HÖCHSTE PRIORITÄT",
        "  Nutze Brick-Namen (db.query, flow.filter, llm.batch etc.) statt alte Runner-Namen",
        "  (python, http, mcp). Alte Namen funktionieren noch aber sind deprecated.",
        "  KEIN create_helper für Standardaufgaben — nutze bestehende Bricks:",
        "    db.query         → Datenbankabfragen",
        "    db.upsert        → Daten in DB schreiben",
        "    llm.batch        → LLM-Extraktion über viele Dokumente",
        "    markitdown.convert → Dokumente/PDFs in Markdown konvertieren",
        "    extract.specialist → Regex-Extraktion mit Schema",
        "    source.fetch     → Daten von Connectors holen (Outlook, OneDrive, ...)",
        "    flow.filter      → Listen filtern",
        "  discover() zeigt alle verfügbaren Brick-Kategorien.",
        "",
        "## COMPOSITOR-REGEL",
        "  IMMER search_helpers + search_pipelines aufrufen BEVOR ein neuer Helper",
        "  oder eine neue Pipeline erstellt wird.",
        "  Bestehende Bausteine wiederverwenden statt duplizieren!",
        "  1. search_helpers(query=...) — nach ähnlichen Helpers suchen",
        "  2. search_pipelines(query=...) — nach ähnlichen Pipelines suchen",
        "  3. Erst dann create_helper / create_pipeline aufrufen",
        "",
        "## PROFILES & VARIABLES",
        "  Profiles nutzen statt Config duplizieren: create_profile → step.profile",
        "  Variables für Runtime-Config: set_variable → {{ var.name }} in Pipelines",
        "  Persistent Store für Run-übergreifende Daten: store.key",
        "",
        "## KERN-REGEL",
        "  IMMER Brix MCP-Tools nutzen. KEINE Workarounds. KEINE manuellen Dateien.",
        "  KEIN docker exec. KEIN YAML schreiben. KEIN Container rebuild.",
        "  KEIN Bash(cat ~/.brix/...)       → nutze get_run_log / get_run_status",
        "  KEIN Bash(python3 -c ...)        → nutze create_helper",
        "  KEIN Bash(rm -f ...)             → nutze brix__delete_run / brix clean",
        "",
        "## HILFE VERFÜGBAR",
        "  Für Details: brix__get_help(topic)",
        "  Topics: 'quick-start', 'step-types', 'step-referenzen', 'helper-scripts',",
        "          'debugging', 'credentials', 'versioning', 'alerting', 'triggers',",
        "          'advanced-features', 'foreach', 'flow-control',",
        "          'brick-first', 'db-bricks', 'llm-bricks', 'source-bricks',",
        "          'resilience', 'variables', 'profiles', 'testing'",
        "",
        "## STEP-OUTPUT REFERENZIEREN",
        "  {{ step_id.output }}        ✅  ganzer Step-Output",
        "  {{ step_id.output.field }}  ✅  einzelnes Feld",
        "  {{ input.param }}           ✅  Pipeline-Input-Parameter",
        "  {{ item }} / {{ item.x }}   ✅  foreach-Element",
        "  {{ step_id.results }}       ✅  foreach-Items (selectattr/map)",
        "  {{ steps.step_id.data }}    ❌  FALSCH: kein 'steps.' Prefix, kein 'data'!",
        "  {{ step_id.data }}          ❌  FALSCH: Feld heißt 'output', nicht 'data'!",
        "",
        "## COMPOSITOR-MODE (T-BRIX-V8-07)",
        "  Pipelines mit compositor_mode: true erlauben KEIN python/cli.",
        "  Nutze Bricks und mcp_call statt Custom-Code.",
        "  Override möglich: allow_code: true auf Pipeline-Ebene.",
        "  compose_pipeline(compositor_mode=true) → LLM-sichere Brick-only Pipeline.",
        "",
        "## TOP-5 ANTI-PATTERNS",
        "  delete_pipeline + create_pipeline  →  update_step / update_pipeline / add_step",
        "  YAML manuell schreiben             →  brix__create_pipeline mit steps inline",
        "  brix run via Bash                  →  brix__run_pipeline",
        "  base64 in foreach-Loops            →  Dateipfade als Strings übergeben",
        "  concurrency: '{{ input.n }}'       →  concurrency muss int sein (kein Jinja2!)",
        "",
        "## DEBUGGING",
        "  Bei Fehler: brix__get_run_errors(run_id) → LLM-optimierte Fehleranalyse",
        "  Dann:       brix__diagnose_run(run_id)   → Schritt-für-Schritt-Diagnose",
        "  Auto-Fix:   brix__auto_fix_step(run_id, step_id) → ModuleNotFoundError / Timeout / UndefinedError",
        "",
        "## TOOL-KATEGORIEN",
        "  Pipeline:    create / get / list / search / update / delete / rename / validate / test",
        "  Steps:       add_step / get_step / update_step / remove_step",
        "  Helper:      create / get / list / search / update / delete / rename / register",
        "  Runs:        run_pipeline / get_run_status / get_run_errors / get_run_log / cancel_run / run_annotate / run_search",
        "  Credentials: credential_add / list / get / update / delete / rotate / search",
        "  Versioning:  get_versions / diff_versions / rollback",
        "  Alerts:      alert_add / list / update / delete / alert_history",
        "  Triggers:    trigger_add / list / get / update / delete / test + scheduler_start/stop/status",
        "  Servers:     server_add / server_list / server_update / server_remove / server_refresh / server_health",
        "  State/Lock:  state_set / get / list / delete | claim_resource / check / release",
        "  Context:     save_agent_context / restore_agent_context",
        "  Insights:    get_insights / get_proactive_suggestions",
        "",
        "## PFAD-KONVENTION",
        "  Host /root/... → Brix /host/root/... (Container-Dateisystem-Präfix!)",
        "",
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
    }


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
