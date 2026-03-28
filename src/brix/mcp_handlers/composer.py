"""Intent-to-Pipeline Assembly handler module.

T-BRIX-V8-01 — compose_pipeline(goal): Describes what a pipeline should do
in natural language, then discovers matching bricks/helpers/pipelines and
assembles a structured proposal.

T-BRIX-V8-02 — plan_pipeline(goal): Formalized reasoning phase (ReAct Reason
step) before compose_pipeline. Decomposes a goal into atomic steps, recommends
the best brick/helper/pipeline per step with rationale, checks constraints, and
estimates complexity.

T-BRIX-DB-06: Keyword taxonomies are read from DB (keyword_taxonomies table).
Code dicts (_SOURCE_KEYWORDS etc.) are kept as fallback.
"""
from __future__ import annotations

import difflib
import logging
import re

from brix.mcp_handlers._shared import (
    _registry,
    _audit_db,
    _extract_source,
    _source_summary,
    _pipeline_dir,
)
from brix.helper_registry import HelperRegistry
from brix.pipeline_store import PipelineStore
from brix.connectors import CONNECTOR_REGISTRY, _get_registry as _get_connector_registry, list_connectors
from brix.bricks.types import is_compatible, suggest_converter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Keyword taxonomies — deutsche + englische Begriffe
# ---------------------------------------------------------------------------

_SOURCE_KEYWORDS: dict[str, list[str]] = {
    "outlook": [
        "outlook", "mail", "email", "e-mail", "mails", "emails", "inbox",
        "imap", "smtp", "posteingang", "nachricht", "nachrichten", "m365",
        "microsoft", "exchange", "office365",
    ],
    "gmail": [
        "gmail", "google mail", "googlemail",
    ],
    "onedrive": [
        "onedrive", "one drive", "sharepoint", "share point", "cloud storage",
        "cloudspeicher", "cloudlaufwerk",
    ],
    "paypal": [
        "paypal", "pay pal", "payment", "zahlung", "zahlungen", "transaktion",
        "transaktionen",
    ],
    "sparkasse": [
        "sparkasse", "bank", "banking", "konto", "kontoauszug", "buchung",
        "buchungen", "sepa", "überweisung", "girokonto",
    ],
    "file": [
        "file", "files", "datei", "dateien", "local", "lokal", "ordner",
        "folder", "verzeichnis", "directory", "disk", "festplatte",
    ],
    "pdf": [
        "pdf", "document", "dokument", "dokumente", "documents", "rechnung",
        "invoice", "rechnungen", "invoices", "scan", "scans",
    ],
    "database": [
        "database", "db", "datenbank", "sql", "sqlite", "postgres",
        "postgresql", "mysql", "tabelle", "table",
    ],
    "api": [
        "api", "rest", "http", "https", "endpoint", "webhook", "request",
        "anfrage",
    ],
}

_ACTION_KEYWORDS: dict[str, list[str]] = {
    "download": [
        "download", "herunterladen", "laden", "fetch", "abrufen", "holen",
        "abholen", "pull", "synchronize", "synchronisieren", "sync",
    ],
    "extract": [
        "extract", "extrahieren", "parsen", "parse", "auslesen", "lesen",
        "entnehmen", "gewinnen", "auswerten", "analyze", "analysieren",
        "analyse",
    ],
    "convert": [
        "convert", "konvertieren", "umwandeln", "transform", "transformieren",
        "übersetzen", "translate",
    ],
    "classify": [
        "classify", "klassifizieren", "kategorisieren", "categorize",
        "sortieren", "sort", "einordnen", "zuordnen",
    ],
    "filter": [
        "filter", "filtern", "select", "auswählen", "suchen", "search",
        "finden", "find",
    ],
    "ingest": [
        "ingest", "ingesten", "importieren", "import", "einlesen", "laden",
        "einpflegen", "speichern", "save", "store", "ablegen",
    ],
    "send": [
        "send", "senden", "verschicken", "schicken", "übermitteln",
        "weiterleiten", "forward",
    ],
    "notify": [
        "notify", "benachrichtigen", "notification", "benachrichtigung",
        "alert", "meldung", "alarm",
    ],
    "process": [
        "process", "verarbeiten", "verarbeitung", "processing", "bearbeiten",
        "behandeln", "handle",
    ],
    "generate": [
        "generate", "generieren", "erstellen", "create", "erzeugen",
        "produzieren", "produce", "bauen", "build",
    ],
    "move": [
        "move", "verschieben", "bewegen", "copy", "kopieren",
        "archivieren", "archive",
    ],
    "scan": [
        "scan", "scannen", "durchsuchen", "crawl", "crawlen",
        "durchgehen", "überprüfen", "check",
    ],
    "tag": [
        "tag", "taggen", "markieren", "label", "labeln", "annotieren",
        "annotate",
    ],
}

_TARGET_KEYWORDS: dict[str, list[str]] = {
    "database": [
        "database", "db", "datenbank", "sql", "sqlite", "postgres", "mysql",
        "tabelle", "table", "speichern", "store", "persistieren", "persist",
    ],
    "file": [
        "file", "datei", "csv", "txt", "json", "yaml", "xml", "diskette",
        "disk", "lokal", "local", "ordner", "folder",
    ],
    "markdown": [
        "markdown", "md", "text", "dokument", "document", "notiz", "note",
        "report", "bericht",
    ],
    "pdf": [
        "pdf", "document", "dokument",
    ],
    "json": [
        "json", "jsonl", "structured", "strukturiert", "ausgabe", "output",
    ],
    "email": [
        "email", "mail", "e-mail", "nachricht", "message",
    ],
    "api": [
        "api", "webhook", "rest", "http", "endpoint",
    ],
}


# ---------------------------------------------------------------------------
# DB-First keyword loading (T-BRIX-DB-06)
# ---------------------------------------------------------------------------

def _get_keyword_dicts() -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, list[str]]]:
    """Return (source_kws, action_kws, target_kws) — from DB if available, else from code."""
    try:
        from brix.db import BrixDB
        db = BrixDB()
        if db.keyword_taxonomies_count() > 0:
            rows = db.keyword_taxonomies_list()
            source_kws: dict[str, list[str]] = {}
            action_kws: dict[str, list[str]] = {}
            target_kws: dict[str, list[str]] = {}
            for row in rows:
                cat = row["category"]
                mapped = row["mapped_to"]
                kw = row["keyword"]
                if cat == "source":
                    source_kws.setdefault(mapped, []).append(kw)
                elif cat == "action":
                    action_kws.setdefault(mapped, []).append(kw)
                elif cat == "target":
                    target_kws.setdefault(mapped, []).append(kw)
            return source_kws, action_kws, target_kws
    except Exception as e:
        logger.debug("Could not load keyword_taxonomies from DB: %s", e)
    return _SOURCE_KEYWORDS, _ACTION_KEYWORDS, _TARGET_KEYWORDS


# ---------------------------------------------------------------------------
# Intent parsing
# ---------------------------------------------------------------------------

def _parse_intent(goal: str) -> dict[str, list[str]]:
    """Extract source, action, and target keywords from a natural-language goal.

    Returns dict with keys 'sources', 'actions', 'targets' each containing
    a list of matched category names (deduped, ordered by first occurrence).
    """
    goal_lower = goal.lower()
    source_kws, action_kws, target_kws = _get_keyword_dicts()

    sources: list[str] = []
    for category, keywords in source_kws.items():
        if any(kw in goal_lower for kw in keywords):
            if category not in sources:
                sources.append(category)

    actions: list[str] = []
    for category, keywords in action_kws.items():
        if any(kw in goal_lower for kw in keywords):
            if category not in actions:
                actions.append(category)

    targets: list[str] = []
    for category, keywords in target_kws.items():
        if any(kw in goal_lower for kw in keywords):
            if category not in targets:
                targets.append(category)

    return {
        "sources": sources,
        "actions": actions,
        "targets": targets,
    }


# ---------------------------------------------------------------------------
# Relevance scoring helpers
# ---------------------------------------------------------------------------

def _word_overlap(text_a: str, text_b: str) -> float:
    """Return Jaccard similarity between word sets of two strings."""
    tokens_a = set(re.sub(r"[_\-]", " ", text_a.lower()).split())
    tokens_b = set(re.sub(r"[_\-]", " ", text_b.lower()).split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def _keyword_hit_score(text: str, intent: dict) -> float:
    """Score a text string against parsed intent keywords.

    Counts how many intent keyword *values* appear in the text and normalises
    by total possible matches (0.0 – 1.0).
    """
    text_lower = text.lower()
    hit_count = 0
    total = 0

    all_keyword_lists = (
        list(_SOURCE_KEYWORDS.values())
        + list(_ACTION_KEYWORDS.values())
        + list(_TARGET_KEYWORDS.values())
    )

    # Collect the flat list of activated keywords from the parsed intent
    active_keywords: list[str] = []
    for cat in intent.get("sources", []):
        active_keywords.extend(_SOURCE_KEYWORDS.get(cat, []))
    for cat in intent.get("actions", []):
        active_keywords.extend(_ACTION_KEYWORDS.get(cat, []))
    for cat in intent.get("targets", []):
        active_keywords.extend(_TARGET_KEYWORDS.get(cat, []))

    if not active_keywords:
        return 0.0

    for kw in active_keywords:
        total += 1
        if kw in text_lower:
            hit_count += 1

    return hit_count / total if total else 0.0


def _name_score(name: str, goal: str) -> float:
    """SequenceMatcher ratio between normalised name tokens and goal."""
    name_clean = re.sub(r"[_\-]", " ", name.lower())
    goal_lower = goal.lower()
    return difflib.SequenceMatcher(None, name_clean, goal_lower).ratio()


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def _discover_pipelines(intent: dict, goal: str) -> list[dict]:
    """Find existing pipelines relevant to the intent."""
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    all_pipelines = store.list_all()

    matches: list[dict] = []
    for p in all_pipelines:
        name = p.get("name", "")
        description = p.get("description", "") or ""
        combined = f"{name} {description}"

        kw_score = _keyword_hit_score(combined, intent)
        name_overlap = _word_overlap(name, goal)
        desc_overlap = _word_overlap(description, goal) if description else 0.0

        score = max(kw_score * 0.6 + name_overlap * 0.25 + desc_overlap * 0.15, 0.0)

        if score > 0.05:
            # Build a human-readable reason
            reasons = []
            if kw_score > 0.1:
                reasons.append(f"keyword-match={kw_score:.0%}")
            if name_overlap > 0.1:
                reasons.append(f"name-overlap={name_overlap:.0%}")
            if desc_overlap > 0.1:
                reasons.append(f"desc-overlap={desc_overlap:.0%}")

            matches.append({
                "type": "pipeline",
                "name": name,
                "description": description,
                "relevance": round(min(score, 1.0), 2),
                "reason": ", ".join(reasons) if reasons else "partial match",
            })

    matches.sort(key=lambda m: m["relevance"], reverse=True)
    return matches[:10]


def _discover_helpers(intent: dict, goal: str) -> list[dict]:
    """Find existing helpers relevant to the intent."""
    registry = HelperRegistry()
    all_helpers = registry.list_all()

    matches: list[dict] = []
    for h in all_helpers:
        name = h.name
        description = h.description or ""
        combined = f"{name} {description}"

        kw_score = _keyword_hit_score(combined, intent)
        name_overlap = _word_overlap(name, goal)
        desc_overlap = _word_overlap(description, goal) if description else 0.0

        score = max(kw_score * 0.6 + name_overlap * 0.25 + desc_overlap * 0.15, 0.0)

        if score > 0.05:
            reasons = []
            if kw_score > 0.1:
                reasons.append(f"keyword-match={kw_score:.0%}")
            if name_overlap > 0.1:
                reasons.append(f"name-overlap={name_overlap:.0%}")
            if desc_overlap > 0.1:
                reasons.append(f"desc-overlap={desc_overlap:.0%}")

            matches.append({
                "type": "helper",
                "name": name,
                "description": description,
                "relevance": round(min(score, 1.0), 2),
                "reason": ", ".join(reasons) if reasons else "partial match",
            })

    matches.sort(key=lambda m: m["relevance"], reverse=True)
    return matches[:10]


def _discover_bricks(intent: dict, goal: str) -> list[dict]:
    """Find built-in bricks relevant to the intent.

    Scoring:
    - keyword hit score against name + description + when_to_use (0.55 weight)
    - alias match: goal tokens found in aliases (0.25 weight, higher than desc)
    - name overlap (0.20 weight)
    - Penalty: if goal keywords appear in when_NOT_to_use, reduce score
    """
    all_bricks = _registry.list_all()
    goal_lower = goal.lower()
    goal_tokens = set(re.sub(r"[^a-z0-9\s]", "", goal_lower).split())

    matches: list[dict] = []
    for b in all_bricks:
        name = b.name
        description = b.description or ""
        when_to_use = b.when_to_use or ""
        when_not_to_use = b.when_NOT_to_use or ""
        alias_text = " ".join(b.aliases) if b.aliases else ""
        combined = f"{name} {description} {when_to_use}"

        kw_score = _keyword_hit_score(combined, intent)
        name_overlap = _word_overlap(name, goal)

        # Alias matching: check how many goal tokens appear in the aliases
        alias_lower = alias_text.lower()
        alias_tokens = set(re.sub(r"[^a-z0-9\s]", "", alias_lower).split())
        alias_hit_count = len(goal_tokens & alias_tokens)
        alias_score = min(alias_hit_count / max(len(goal_tokens), 1), 1.0)

        # Also check if any full alias phrase appears in the goal
        full_alias_match = any(
            alias.lower() in goal_lower for alias in b.aliases
        )
        if full_alias_match:
            alias_score = max(alias_score, 0.5)

        # Negative factor: goal keywords appearing in when_NOT_to_use
        # Only count distinctive words (>= 7 chars) to avoid penalising on
        # domain words like "emails", "files", "fetch" that appear everywhere.
        not_to_use_lower = when_not_to_use.lower()
        not_to_use_tokens = set(
            t for t in re.sub(r"[^a-z0-9\s]", "", not_to_use_lower).split()
            if len(t) >= 7
        )
        significant_goal_tokens = {t for t in goal_tokens if len(t) >= 7}
        negative_overlap = len(significant_goal_tokens & not_to_use_tokens)
        negative_penalty = min(negative_overlap / max(len(significant_goal_tokens), 1), 0.4) if significant_goal_tokens else 0.0

        score = max(
            kw_score * 0.55 + alias_score * 0.25 + name_overlap * 0.20 - negative_penalty,
            0.0,
        )

        if score > 0.05:
            reasons = []
            if kw_score > 0.1:
                reasons.append(f"keyword-match={kw_score:.0%}")
            if alias_score > 0.1:
                reasons.append(f"alias-match={alias_score:.0%}")
            if name_overlap > 0.1:
                reasons.append(f"name-overlap={name_overlap:.0%}")
            if negative_penalty > 0.0:
                reasons.append(f"when-not-to-use-penalty={negative_penalty:.0%}")

            matches.append({
                "type": "brick",
                "name": name,
                "description": description,
                "category": b.category,
                "relevance": round(min(score, 1.0), 2),
                "reason": ", ".join(reasons) if reasons else "partial match",
            })

    matches.sort(key=lambda m: m["relevance"], reverse=True)
    return matches[:8]


def _discover_connectors(intent: dict, goal: str) -> list[dict]:
    """Find source connectors relevant to the intent.

    Matches connectors against parsed source categories and goal keywords.
    Returns a list of connector summaries sorted by relevance.
    """
    goal_lower = goal.lower()
    sources = intent.get("sources", [])

    matches: list[dict] = []
    for connector in list_connectors():
        # Direct name match with parsed sources
        name_hit = connector.name in sources
        # Keyword overlap between connector description and goal
        desc_overlap = _word_overlap(connector.description, goal)
        # Check if any related pipeline/helper name appears in the goal
        related_hit = any(
            rp.replace("-", " ").replace("_", " ") in goal_lower
            for rp in connector.related_pipelines + connector.related_helpers
        )

        score = (
            (0.6 if name_hit else 0.0)
            + desc_overlap * 0.3
            + (0.1 if related_hit else 0.0)
        )

        if score > 0.05 or name_hit:
            matches.append({
                "type": "connector",
                "name": connector.name,
                "connector_type": connector.type,
                "description": connector.description,
                "required_mcp_server": connector.required_mcp_server,
                "related_pipelines": connector.related_pipelines,
                "relevance": round(min(score, 1.0), 2),
                "reason": (
                    "direct source match" if name_hit
                    else f"description-overlap={desc_overlap:.0%}"
                ),
            })

    matches.sort(key=lambda m: m["relevance"], reverse=True)
    return matches[:6]


# ---------------------------------------------------------------------------
# Pipeline assembly
# ---------------------------------------------------------------------------

def _assemble_pipeline(
    intent: dict,
    matches: list[dict],
    pipeline_name: str,
    goal: str,
) -> dict:
    """Build a proposed pipeline structure from discovered matches.

    Arranges steps in the logical order: Source → Transform → Action → Target.
    Steps that cannot be mapped to an existing component are marked as
    NEEDS_IMPLEMENTATION.
    """
    pipeline_matches = [m for m in matches if m["type"] == "pipeline"]
    helper_matches = [m for m in matches if m["type"] == "helper"]
    brick_matches = [m for m in matches if m["type"] == "brick"]

    steps: list[dict] = []

    # --- Phase 1: SOURCE step ---
    sources = intent.get("sources", [])
    if sources:
        source_name = sources[0]
        # Try to find a pipeline or helper that covers this source
        source_match = next(
            (m for m in pipeline_matches + helper_matches if source_name in m["name"].lower()),
            None,
        )
        if source_match is None:
            # Fall back to best pipeline match overall
            source_match = pipeline_matches[0] if pipeline_matches else None

        if source_match:
            ref_prefix = "pipeline" if source_match["type"] == "pipeline" else "helper"
            steps.append({
                "id": "fetch",
                "type": "python" if source_match["type"] == "helper" else "pipeline",
                "description": f"Fetch/read from {source_name}",
                "from": f"{ref_prefix}:{source_match['name']}",
                "status": "AVAILABLE",
            })
        else:
            # Find a relevant brick
            source_brick = next(
                (m for m in brick_matches if any(
                    kw in m["name"].lower() for kw in ["http", "file", "mcp", "cli"]
                )),
                brick_matches[0] if brick_matches else None,
            )
            if source_brick:
                steps.append({
                    "id": "fetch",
                    "type": source_brick["name"],
                    "description": f"Fetch/read from {source_name}",
                    "from": f"brick:{source_brick['name']}",
                    "status": "AVAILABLE",
                })
            else:
                steps.append({
                    "id": "fetch",
                    "type": "source.fetch",
                    "description": f"Fetch/read from {source_name}",
                    "status": "NEEDS_IMPLEMENTATION",
                    "note": "Configure source.fetch with appropriate connector.",
                })
    else:
        # No source identified — use best pipeline/helper match as input
        best_match = (pipeline_matches + helper_matches + brick_matches)
        if best_match:
            first = best_match[0]
            ref_prefix = first["type"]
            steps.append({
                "id": "fetch",
                "type": "source.fetch" if first["type"] == "helper" else first["type"],
                "description": "Fetch/read input data",
                "from": f"{ref_prefix}:{first['name']}",
                "status": "AVAILABLE",
            })
        else:
            steps.append({
                "id": "fetch",
                "type": "source.fetch",
                "description": "Fetch/read input data",
                "status": "NEEDS_IMPLEMENTATION",
                "note": "Configure source.fetch with appropriate connector, or use db.query for DB sources.",
            })

    # --- Phase 2: TRANSFORM / EXTRACT steps (based on action keywords) ---
    actions = intent.get("actions", [])
    transform_actions = [a for a in actions if a in ("extract", "convert", "classify", "filter", "tag", "process")]

    for action in transform_actions[:2]:  # Max 2 transform steps to keep it concise
        # Look for a helper that matches this action
        action_helper = next(
            (m for m in helper_matches if action in m["name"].lower()),
            None,
        )
        if action_helper:
            steps.append({
                "id": action,
                "type": "python",
                "description": f"{action.capitalize()} the fetched data",
                "from": f"helper:{action_helper['name']}",
                "status": "AVAILABLE",
            })
        else:
            # Look for a matching brick
            action_brick = next(
                (m for m in brick_matches if action in m["name"].lower()),
                None,
            )
            if action_brick:
                steps.append({
                    "id": action,
                    "type": action_brick["name"],
                    "description": f"{action.capitalize()} the fetched data",
                    "from": f"brick:{action_brick['name']}",
                    "status": "AVAILABLE",
                })
            else:
                # Use brick alternative for action
                _ACTION_TO_BRICK: dict[str, str] = {
                    "extract": "extract.specialist",
                    "convert": "markitdown.convert",
                    "classify": "llm.batch",
                    "filter": "flow.filter",
                    "process": "flow.transform",
                    "tag": "llm.batch",
                }
                brick_type = _ACTION_TO_BRICK.get(action, "flow.transform")
                steps.append({
                    "id": action,
                    "type": brick_type,
                    "description": f"{action.capitalize()} the fetched data",
                    "status": "NEEDS_IMPLEMENTATION",
                    "note": f"Suggested brick: {brick_type}. Use get_brick_schema for parameters.",
                })

    # --- Phase 3: TARGET / STORE step ---
    targets = intent.get("targets", [])
    store_actions = [a for a in actions if a in ("ingest", "send", "notify", "move", "generate", "download", "scan")]

    if targets or store_actions:
        target_name = targets[0] if targets else (store_actions[0] if store_actions else "output")

        # Look for a helper that matches this target
        target_helper = next(
            (m for m in helper_matches if target_name in m["name"].lower()),
            None,
        )
        if target_helper:
            steps.append({
                "id": "store",
                "type": "python",
                "description": f"Store result in {target_name}",
                "from": f"helper:{target_helper['name']}",
                "status": "AVAILABLE",
            })
        else:
            _TARGET_TO_BRICK: dict[str, str] = {
                "database": "db.upsert",
                "db": "db.upsert",
                "file": "file.write",
                "markdown": "file.write",
                "json": "file.write",
                "email": "mcp_call",
                "api": "http.post",
            }
            brick_type = _TARGET_TO_BRICK.get(target_name, "db.upsert")
            steps.append({
                "id": "store",
                "type": brick_type,
                "description": f"Store result in {target_name}",
                "status": "NEEDS_IMPLEMENTATION",
                "note": f"Suggested brick: {brick_type}. Use get_brick_schema for parameters.",
            })

    return {
        "name": pipeline_name,
        "steps": steps,
    }


# ---------------------------------------------------------------------------
# Coverage calculation
# ---------------------------------------------------------------------------

def _calculate_coverage(steps: list[dict]) -> str:
    """Calculate percentage of steps that are AVAILABLE (not NEEDS_IMPLEMENTATION)."""
    if not steps:
        return "0%"
    available = sum(1 for s in steps if s.get("status") == "AVAILABLE")
    pct = int((available / len(steps)) * 100)
    return f"{pct}%"


def _collect_missing(steps: list[dict]) -> list[str]:
    """Return human-readable descriptions of steps that need implementation."""
    return [
        f"{s['id'].capitalize()} step needs implementation: {s.get('description', s['id'])}"
        for s in steps
        if s.get("status") == "NEEDS_IMPLEMENTATION"
    ]


def _check_step_type_compatibility(steps: list[dict]) -> list[dict]:
    """Check type compatibility between consecutive pipeline steps.

    Each step dict may have 'output_type' and 'input_type' fields set by
    the assembly step (from brick schema lookups). For steps without explicit
    types, no check is performed.

    Returns a list of type_check records:
        {
            "step_from": <step id N>,
            "step_to": <step id N+1>,
            "output_type": <output of step N>,
            "input_type": <input of step N+1>,
            "compatible": bool,
            "suggestion": str | None,
        }
    """
    checks: list[dict] = []
    for i in range(len(steps) - 1):
        step_from = steps[i]
        step_to = steps[i + 1]
        out_type = step_from.get("output_type") or ""
        in_type = step_to.get("input_type") or ""

        # Skip check when type information is unavailable
        if not out_type and not in_type:
            continue

        compatible = is_compatible(out_type, in_type)
        suggestion = None if compatible else suggest_converter(out_type, in_type)

        checks.append({
            "step_from": step_from.get("id", f"step_{i}"),
            "step_to": step_to.get("id", f"step_{i + 1}"),
            "output_type": out_type,
            "input_type": in_type,
            "compatible": compatible,
            "suggestion": suggestion,
        })

    return checks


def _enrich_steps_with_types(steps: list[dict]) -> list[dict]:
    """Annotate assembled pipeline steps with brick type information where possible.

    Looks up the brick's input_type / output_type from the registry and adds
    them to the step dict.  Steps without a brick reference (status=NEEDS_IMPLEMENTATION
    or type=pipeline/helper) are left without type annotations.
    """
    for step in steps:
        from_ref = step.get("from", "")
        if from_ref.startswith("brick:"):
            brick_name = from_ref[len("brick:"):]
            brick = _registry.get(brick_name)
            if brick:
                if brick.input_type:
                    step["input_type"] = brick.input_type
                if brick.output_type:
                    step["output_type"] = brick.output_type
    return steps


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

async def _handle_compose_pipeline(arguments: dict) -> dict:
    """Intent-to-Pipeline Assembly: analyse a natural-language goal and
    produce a structured pipeline proposal built from existing bricks,
    helpers, and pipelines.
    """
    goal = (arguments.get("goal") or "").strip()
    if not goal:
        return {
            "success": False,
            "error": "Parameter 'goal' is required.",
        }

    name = (arguments.get("name") or "").strip()
    compositor_mode = bool(arguments.get("compositor_mode", True))
    source = _extract_source(arguments)

    # 1. Parse intent
    intent = _parse_intent(goal)

    # Derive a pipeline name from the goal if not provided
    if not name:
        # Slugify the first 5 words of the goal
        words = re.sub(r"[^a-z0-9\s]", "", goal.lower()).split()
        name = "-".join(words[:5]) or "composed-pipeline"

    # 2. Discover relevant components
    pipeline_matches = _discover_pipelines(intent, goal)
    helper_matches = _discover_helpers(intent, goal)
    brick_matches = _discover_bricks(intent, goal)
    connector_matches = _discover_connectors(intent, goal)

    all_matches = pipeline_matches + helper_matches + brick_matches
    # Sort combined list by relevance descending
    all_matches.sort(key=lambda m: m["relevance"], reverse=True)

    # 3. Assemble proposed pipeline
    proposed = _assemble_pipeline(intent, all_matches, name, goal)

    # Apply compositor_mode flag to proposed pipeline metadata (T-BRIX-V8-07)
    if compositor_mode:
        proposed["compositor_mode"] = True
        proposed["allow_code"] = False
        # Replace any python/cli steps that were proposed with Brick alternatives
        _BRICK_ALTERNATIVES: dict[str, str] = {
            "extract": "extract.specialist",
            "convert": "markitdown.convert",
            "classify": "llm.batch",
            "filter": "flow.filter",
            "transform": "flow.transform",
            "ingest": "db.upsert",
            "store": "db.upsert",
            "query": "db.query",
            "fetch": "source.fetch",
            "scan": "source.fetch",
        }
        for step in proposed.get("steps", []):
            if step.get("type") in ("python", "cli"):
                # Suggest a brick alternative based on the step id/action
                step_id_lower = step.get("id", "").lower()
                brick_alt = next(
                    (brick for kw, brick in _BRICK_ALTERNATIVES.items() if kw in step_id_lower),
                    None,
                )
                if brick_alt:
                    step["type"] = brick_alt
                    step["status"] = "AVAILABLE"
                    step["note"] = (
                        f"compositor_mode: replaced python/cli with brick '{brick_alt}'. "
                        "Configure via config block. Use get_brick_schema for parameters."
                    )
                else:
                    step["type"] = "mcp_call"
                    step["status"] = "NEEDS_IMPLEMENTATION"
                    step["note"] = (
                        "compositor_mode: python/cli not allowed. "
                        "Use a built-in brick (db.query, llm.batch, flow.filter, etc.) or mcp_call. "
                        "Call list_bricks() to discover available bricks."
                    )

    # 3b. Enrich steps with brick type info + run type compatibility checks
    _enrich_steps_with_types(proposed["steps"])
    type_checks = _check_step_type_compatibility(proposed["steps"])

    # 4. Coverage + missing analysis
    coverage = _calculate_coverage(proposed["steps"])
    missing = _collect_missing(proposed["steps"])

    # 5. Next steps
    next_steps: list[str] = [
        "Review the proposed pipeline steps above",
    ]
    if missing:
        next_steps.append(f"Implement {len(missing)} missing step(s): {', '.join(m.split(':')[0] for m in missing)}")
    if pipeline_matches:
        next_steps.append(
            f"Consider reusing existing pipeline '{pipeline_matches[0]['name']}' "
            f"(relevance: {pipeline_matches[0]['relevance']:.0%})"
        )
    if connector_matches:
        next_steps.append(
            f"Connector '{connector_matches[0]['name']}' matches this goal — "
            f"call brix__get_connector(name='{connector_matches[0]['name']}') for details."
        )
    next_steps.append("Call brix__create_pipeline to finalise the pipeline once steps are ready")

    # Audit log
    _audit_db.write_audit_entry(
        tool="brix__compose_pipeline",
        source=source,
        arguments_summary=_source_summary(source, goal=goal[:80]),
    )

    result: dict = {
        "success": True,
        "goal": goal,
        "parsed_intent": intent,
        "matches": all_matches[:15],  # Top 15 across all types
        "connectors": connector_matches,  # Matching source connectors (T-BRIX-V8-04)
        "proposed_pipeline": proposed,
        "coverage": coverage,
        "missing": missing,
        "type_checks": type_checks,  # T-BRIX-V8-06: type compatibility between steps
        "next_steps": next_steps,
    }
    if compositor_mode:
        result["compositor_mode"] = True
    return result


# ---------------------------------------------------------------------------
# T-BRIX-V8-02 — plan_pipeline: Formalized Reason Phase
# ---------------------------------------------------------------------------

# Mapping from intent category to human-readable step action phrases.
_SOURCE_ACTION_PHRASES: dict[str, str] = {
    "outlook": "Fetch emails from Outlook",
    "gmail": "Fetch emails from Gmail",
    "onedrive": "Read files from OneDrive",
    "paypal": "Fetch PayPal transactions",
    "sparkasse": "Fetch Sparkasse bank transactions",
    "file": "Read local files",
    "pdf": "Read PDF documents",
    "database": "Query database",
    "api": "Call HTTP/REST API",
}

_ACTION_PHRASES: dict[str, str] = {
    "download": "Download and save items",
    "extract": "Extract structured data",
    "convert": "Convert / transform data format",
    "classify": "Classify items into categories",
    "filter": "Filter and select relevant items",
    "ingest": "Ingest data into target store",
    "send": "Send output (email / notification)",
    "notify": "Send notification",
    "process": "Process and enrich data",
    "generate": "Generate output artefacts",
    "move": "Move / archive items",
    "scan": "Scan and inspect items",
    "tag": "Tag / annotate items",
}

_TARGET_PHRASES: dict[str, str] = {
    "database": "Store results in database",
    "file": "Write results to local file",
    "markdown": "Write results as Markdown document",
    "pdf": "Write results as PDF",
    "json": "Write results as JSON",
    "email": "Send results via email",
    "api": "Push results to API endpoint",
}

# Constraints that are checkable without an LLM.
_CONSTRAINT_CHECKERS: dict[str, str] = {
    "no python scripts": "python",
    "only built-in bricks": "python",
    "no helpers": "helper",
    "no pipelines": "pipeline",
    "only pipelines": "brick",   # violation if we rely on bricks-only
}

# Category label → which keyword dicts they map to
_CATEGORY_OF_SOURCE = "source"
_CATEGORY_OF_ACTION = "transform"
_CATEGORY_OF_TARGET = "output"


def _confidence_level(relevance: float) -> str:
    """Map a 0-1 relevance score to a human-readable confidence label.

    Thresholds:
    - high   (>= 0.55): strong keyword + name overlap with the action phrase
    - medium (>= 0.10): some overlap found — existing component is a plausible match
    - low    (<  0.10): no meaningful overlap — a new implementation is likely needed
    """
    if relevance >= 0.55:
        return "high"
    if relevance >= 0.10:
        return "medium"
    return "low"


def _best_recommendation(
    action: str, category: str, all_matches: list[dict]
) -> tuple[dict, float] | tuple[None, float]:
    """Return the best existing match for a given action description plus its score.

    Searches helpers, pipelines, and bricks for a match whose name or
    description contains action-related keywords.

    Returns a (match_dict, combined_score) tuple.  combined_score is a
    0-1 value blending token overlap with the action phrase and the
    match's own discovery relevance score — suitable for passing to
    _confidence_level().  Returns (None, 0.0) when no match is found.
    """
    action_lower = action.lower()
    tokens = set(re.sub(r"[^a-z0-9\s]", "", action_lower).split())

    scored: list[tuple[float, dict]] = []
    for m in all_matches:
        name_tokens = set(re.sub(r"[_\-]", " ", m["name"].lower()).split())
        desc_tokens = set(re.sub(r"[^a-z0-9\s]", "", (m.get("description") or "").lower()).split())
        combined_tokens = name_tokens | desc_tokens
        overlap = len(tokens & combined_tokens) / max(len(tokens), 1)
        if overlap > 0:
            # Blend token overlap (60 %) with discovery relevance (40 %)
            combined = overlap * 0.6 + m.get("relevance", 0.0) * 0.4
            scored.append((combined, m))

    if not scored:
        return None, 0.0

    scored.sort(key=lambda t: t[0], reverse=True)
    best_score, best_match = scored[0]
    return best_match, best_score


def _build_alternatives(action: str, primary_match: dict | None, all_matches: list[dict]) -> list[dict]:
    """Return up to 2 alternative matches for a step, excluding the primary."""
    action_lower = action.lower()
    tokens = set(re.sub(r"[^a-z0-9\s]", "", action_lower).split())
    primary_name = primary_match["name"] if primary_match else None

    scored: list[tuple[float, dict]] = []
    for m in all_matches:
        if m["name"] == primary_name:
            continue
        name_tokens = set(re.sub(r"[_\-]", " ", m["name"].lower()).split())
        desc_tokens = set(re.sub(r"[^a-z0-9\s]", "", (m.get("description") or "").lower()).split())
        combined_tokens = name_tokens | desc_tokens
        overlap = len(tokens & combined_tokens) / max(len(tokens), 1)
        if overlap > 0:
            scored.append((overlap, m))

    scored.sort(key=lambda t: (t[0], t[1]["relevance"]), reverse=True)

    alternatives: list[dict] = []
    for _, m in scored[:2]:
        note_parts = []
        if m["type"] == "pipeline":
            note_parts.append("Higher-level reuse, less flexibility")
        elif m["type"] == "helper":
            note_parts.append("Python helper, more customisable")
        elif m["type"] == "brick":
            note_parts.append("Built-in brick, minimal setup")
        alternatives.append({
            "type": m["type"],
            "name": m["name"],
            "note": note_parts[0] if note_parts else "Alternative match",
        })
    return alternatives


def _check_constraints(constraints: list[str], plan_steps: list[dict]) -> list[str]:
    """Return a list of constraint violation messages.

    Checks each constraint against the recommendation types used in the plan.
    """
    violations: list[str] = []
    for constraint in constraints:
        c_lower = constraint.lower().strip()

        if "no python" in c_lower or "only built-in" in c_lower or "no helpers" in c_lower:
            # Flag any step whose primary recommendation uses python/helper
            for step in plan_steps:
                rec = step.get("recommendation", {})
                rec_type = rec.get("type", "")
                if "no python" in c_lower and rec_type in ("helper",):
                    violations.append(
                        f"Constraint '{constraint}' violated: step '{step['action']}' "
                        f"recommends a Python helper ('{rec.get('name', '?')}')."
                    )
                if ("only built-in" in c_lower or "no helpers" in c_lower) and rec_type == "helper":
                    violations.append(
                        f"Constraint '{constraint}' violated: step '{step['action']}' "
                        f"recommends helper '{rec.get('name', '?')}' (not a built-in brick)."
                    )

        if "no pipelines" in c_lower:
            for step in plan_steps:
                rec = step.get("recommendation", {})
                if rec.get("type") == "pipeline":
                    violations.append(
                        f"Constraint '{constraint}' violated: step '{step['action']}' "
                        f"recommends pipeline '{rec.get('name', '?')}'."
                    )

        if "must be idempotent" in c_lower:
            # Heuristic: pipelines are generally idempotent, raw HTTP/MCP calls might not be
            for step in plan_steps:
                rec = step.get("recommendation", {})
                if rec.get("type") == "brick" and "http" in rec.get("name", "").lower():
                    violations.append(
                        f"Constraint '{constraint}': step '{step['action']}' uses raw HTTP "
                        f"brick '{rec.get('name', '?')}' which may not be idempotent."
                    )

    return violations


def _estimate_complexity(total_steps: int, new_steps: int) -> str:
    """Estimate overall pipeline complexity based on step counts."""
    if total_steps <= 3 and new_steps == 0:
        return "simple"
    if total_steps <= 7 or new_steps <= 2:
        return "moderate"
    return "complex"


async def _handle_plan_pipeline(arguments: dict) -> dict:
    """Formalized Reason Phase: decompose a goal into atomic steps, recommend
    bricks/helpers/pipelines per step with rationale, check constraints, and
    estimate complexity.

    This is the ReAct 'Think' step — call it BEFORE compose_pipeline or
    create_pipeline for non-trivial goals.
    """
    goal = (arguments.get("goal") or "").strip()
    if not goal:
        return {
            "success": False,
            "error": "Parameter 'goal' is required.",
        }

    constraints: list[str] = arguments.get("constraints") or []
    source = _extract_source(arguments)

    # ------------------------------------------------------------------
    # 1. Parse intent (reuse existing function)
    # ------------------------------------------------------------------
    intent = _parse_intent(goal)

    # ------------------------------------------------------------------
    # 2. Discover all components (reuse existing discovery functions)
    # ------------------------------------------------------------------
    pipeline_matches = _discover_pipelines(intent, goal)
    helper_matches = _discover_helpers(intent, goal)
    brick_matches = _discover_bricks(intent, goal)
    all_matches = pipeline_matches + helper_matches + brick_matches
    all_matches.sort(key=lambda m: m["relevance"], reverse=True)

    # ------------------------------------------------------------------
    # 3. Goal decomposition — build ordered list of atomic steps
    # ------------------------------------------------------------------
    steps: list[dict] = []
    order = 0

    # --- SOURCE step(s) ---
    for src in intent.get("sources", []):
        order += 1
        action_phrase = _SOURCE_ACTION_PHRASES.get(src, f"Fetch data from {src}")
        primary, primary_score = _best_recommendation(action_phrase, "source", all_matches)
        if primary is None:
            # Try a generic fetch via the highest relevance pipeline or brick
            primary = next(
                (m for m in all_matches if m["type"] in ("pipeline", "brick")),
                None,
            )
            primary_score = primary["relevance"] if primary else 0.0
        alternatives = _build_alternatives(action_phrase, primary, all_matches)

        if primary:
            rec = {
                "type": primary["type"],
                "name": primary["name"],
                "confidence": _confidence_level(primary_score),
                "rationale": (
                    primary.get("description") or
                    f"Best existing match for '{action_phrase}'"
                )[:120],
            }
            is_new = False
        else:
            rec = {
                "type": "python",
                "name": f"helper_{src}_fetch",
                "confidence": "low",
                "rationale": "No existing component found — a new Python helper is needed.",
            }
            is_new = True

        steps.append({
            "order": order,
            "action": action_phrase,
            "category": "source",
            "recommendation": rec,
            "alternatives": alternatives,
            "needs_implementation": is_new,
        })

    # If no explicit source found, add a generic input step
    if not intent.get("sources"):
        order += 1
        primary = next((m for m in all_matches if m["type"] in ("pipeline", "helper")), None)
        if primary:
            rec = {
                "type": primary["type"],
                "name": primary["name"],
                "confidence": _confidence_level(primary["relevance"]),
                "rationale": (
                    primary.get("description") or
                    f"Best overall match (relevance {primary['relevance']:.0%})"
                )[:120],
            }
            is_new = False
        else:
            rec = {
                "type": "python",
                "name": "helper_input_fetch",
                "confidence": "low",
                "rationale": "No existing component found — a new Python helper is needed.",
            }
            is_new = True
        steps.append({
            "order": order,
            "action": "Fetch / read input data",
            "category": "source",
            "recommendation": rec,
            "alternatives": [],
            "needs_implementation": is_new,
        })

    # --- TRANSFORM / PROCESS steps ---
    transform_actions = [
        a for a in intent.get("actions", [])
        if a in ("extract", "convert", "classify", "filter", "tag", "process")
    ]
    for action in transform_actions[:3]:  # Cap at 3 transform steps
        order += 1
        action_phrase = _ACTION_PHRASES.get(action, f"{action.capitalize()} data")
        primary, primary_score = _best_recommendation(action_phrase, "transform", all_matches)
        alternatives = _build_alternatives(action_phrase, primary, all_matches)

        if primary:
            rec = {
                "type": primary["type"],
                "name": primary["name"],
                "confidence": _confidence_level(primary_score),
                "rationale": (
                    primary.get("description") or
                    f"Best existing match for '{action_phrase}'"
                )[:120],
            }
            is_new = False
        else:
            rec = {
                "type": "python",
                "name": f"helper_{action}",
                "confidence": "low",
                "rationale": f"No existing component found for '{action}' — a new Python helper is needed.",
            }
            is_new = True

        steps.append({
            "order": order,
            "action": action_phrase,
            "category": "transform",
            "recommendation": rec,
            "alternatives": alternatives,
            "needs_implementation": is_new,
        })

    # --- OUTPUT / STORE step(s) ---
    store_actions = [
        a for a in intent.get("actions", [])
        if a in ("ingest", "send", "notify", "move", "generate", "download", "scan")
    ]
    for target in intent.get("targets", [])[:2]:
        order += 1
        action_phrase = _TARGET_PHRASES.get(target, f"Store result as {target}")
        primary, primary_score = _best_recommendation(action_phrase, "output", all_matches)
        alternatives = _build_alternatives(action_phrase, primary, all_matches)

        if primary:
            rec = {
                "type": primary["type"],
                "name": primary["name"],
                "confidence": _confidence_level(primary_score),
                "rationale": (
                    primary.get("description") or
                    f"Best existing match for '{action_phrase}'"
                )[:120],
            }
            is_new = False
        else:
            rec = {
                "type": "python",
                "name": f"helper_{target}_store",
                "confidence": "low",
                "rationale": "No existing component found — a new Python helper is needed.",
            }
            is_new = True

        steps.append({
            "order": order,
            "action": action_phrase,
            "category": "output",
            "recommendation": rec,
            "alternatives": alternatives,
            "needs_implementation": is_new,
        })

    # If there are store actions but no explicit target, add a generic output step
    if store_actions and not intent.get("targets"):
        order += 1
        action = store_actions[0]
        action_phrase = _ACTION_PHRASES.get(action, f"{action.capitalize()} output")
        primary, primary_score = _best_recommendation(action_phrase, "output", all_matches)
        alternatives = _build_alternatives(action_phrase, primary, all_matches)

        if primary:
            rec = {
                "type": primary["type"],
                "name": primary["name"],
                "confidence": _confidence_level(primary_score),
                "rationale": (
                    primary.get("description") or
                    f"Best existing match for '{action_phrase}'"
                )[:120],
            }
            is_new = False
        else:
            rec = {
                "type": "python",
                "name": f"helper_{action}_output",
                "confidence": "low",
                "rationale": "No existing component found — a new Python helper is needed.",
            }
            is_new = True

        steps.append({
            "order": order,
            "action": action_phrase,
            "category": "output",
            "recommendation": rec,
            "alternatives": alternatives,
            "needs_implementation": is_new,
        })

    # ------------------------------------------------------------------
    # 4. Constraint checking
    # ------------------------------------------------------------------
    constraint_violations = _check_constraints(constraints, steps)

    # ------------------------------------------------------------------
    # 5. Complexity estimation
    # ------------------------------------------------------------------
    total_steps = len(steps)
    new_steps = sum(1 for s in steps if s["needs_implementation"])
    existing_steps = total_steps - new_steps
    complexity = _estimate_complexity(total_steps, new_steps)

    # ------------------------------------------------------------------
    # 6. Warnings
    # ------------------------------------------------------------------
    warnings: list[str] = []
    for s in steps:
        if s["needs_implementation"]:
            warnings.append(
                f"Step {s['order']} ('{s['action']}') requires a new implementation: "
                f"{s['recommendation']['rationale']}"
            )
    if constraint_violations:
        warnings.append(f"{len(constraint_violations)} constraint violation(s) detected.")

    # ------------------------------------------------------------------
    # 7. T-BRIX-V8-06: when_NOT_to_use confidence penalty
    #    If the goal keywords overlap with a recommended brick's when_NOT_to_use,
    #    downgrade confidence and add a warning.
    # ------------------------------------------------------------------
    goal_lower_plan = goal.lower()
    goal_tokens_plan = set(re.sub(r"[^a-z0-9\s]", "", goal_lower_plan).split())
    for step in steps:
        rec = step.get("recommendation", {})
        if rec.get("type") == "brick":
            brick = _registry.get(rec["name"])
            if brick and brick.when_NOT_to_use:
                not_to_use_tokens = set(
                    t for t in re.sub(r"[^a-z0-9\s]", "", brick.when_NOT_to_use.lower()).split()
                    if len(t) >= 7
                )
                significant_goal_tokens_plan = {t for t in goal_tokens_plan if len(t) >= 7}
                overlap = significant_goal_tokens_plan & not_to_use_tokens
                if len(overlap) >= 2:
                    # Downgrade confidence by one level
                    current = rec.get("confidence", "medium")
                    if current == "high":
                        rec["confidence"] = "medium"
                    elif current == "medium":
                        rec["confidence"] = "low"
                    rec["when_not_to_use_warning"] = (
                        f"Goal keywords {sorted(overlap)[:3]} appear in brick's "
                        f"when_NOT_to_use — check if this brick is the right choice."
                    )
                    warnings.append(
                        f"Step {step['order']} ('{step['action']}'): brick '{rec['name']}' "
                        f"may not be appropriate — goal overlaps with when_NOT_to_use."
                    )

    # ------------------------------------------------------------------
    # 8. T-BRIX-V8-06: Type chain — show data flow between steps
    # ------------------------------------------------------------------
    type_chain: list[dict] = []
    for step in steps:
        rec = step.get("recommendation", {})
        brick_name = rec.get("name", "")
        brick = _registry.get(brick_name) if rec.get("type") == "brick" else None
        entry: dict = {
            "order": step["order"],
            "action": step["action"],
            "brick": brick_name if brick else None,
            "input_type": brick.input_type if brick else None,
            "output_type": brick.output_type if brick else None,
        }
        type_chain.append(entry)

    # Annotate with compatibility between consecutive entries
    for i in range(len(type_chain) - 1):
        curr = type_chain[i]
        nxt = type_chain[i + 1]
        out_type = curr.get("output_type") or ""
        in_type = nxt.get("input_type") or ""
        if out_type and in_type:
            compatible = is_compatible(out_type, in_type)
            curr["compatible_with_next"] = compatible
            if not compatible:
                curr["converter_suggestion"] = suggest_converter(out_type, in_type)

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------
    _audit_db.write_audit_entry(
        tool="brix__plan_pipeline",
        source=source,
        arguments_summary=_source_summary(source, goal=goal[:80]),
    )

    return {
        "success": True,
        "goal": goal,
        "plan": {
            "steps": steps,
            "complexity": complexity,
            "total_steps": total_steps,
            "existing_steps": existing_steps,
            "new_steps": new_steps,
            "constraint_violations": constraint_violations,
            "warnings": warnings,
            "type_chain": type_chain,  # T-BRIX-V8-06: data flow between steps
        },
        "next_action": "Call compose_pipeline or create_pipeline to build this",
    }
