"""DB-first brick-candidate detection for repeated helper and pipeline logic."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

from brix.db import BrixDB
from brix.helper_inventory import build_helper_inventory


@dataclass(frozen=True)
class BrickCandidate:
    """A detected opportunity to promote repeated logic into a reusable brick."""

    kind: str
    title: str
    domain: str
    confidence: str
    evidence_count: int
    suggested_brick: str
    evidence: tuple[dict[str, Any], ...] = ()
    signals: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "title": self.title,
            "domain": self.domain,
            "confidence": self.confidence,
            "evidence_count": self.evidence_count,
            "suggested_brick": self.suggested_brick,
            "evidence": [dict(item) for item in self.evidence],
            "signals": list(self.signals),
        }


@dataclass(frozen=True)
class BrickCandidateReport:
    """Complete detector output."""

    candidates: tuple[BrickCandidate, ...] = ()
    summary: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidates": [candidate.as_dict() for candidate in self.candidates],
            "summary": dict(self.summary),
        }


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug[:48] or "candidate"


def _sql_verb(sql: str) -> str:
    match = re.search(r"\b(select|insert|update|delete|upsert|merge)\b", sql, re.IGNORECASE)
    return match.group(1).lower() if match else "sql"


def normalize_sql_pattern(sql: str) -> str:
    """Return a stable SQL pattern with literals collapsed."""
    normalized = re.sub(r"'(?:''|[^'])*'", "?", sql)
    normalized = re.sub(r'"(?:""|[^"])*"', "?", normalized)
    normalized = re.sub(r"\b\d+(?:\.\d+)?\b", "?", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized


def _step_query(step: dict[str, Any]) -> str:
    config = step.get("config") if isinstance(step.get("config"), dict) else {}
    params = step.get("params") if isinstance(step.get("params"), dict) else {}
    for source in (step, config, params):
        query = source.get("query") if isinstance(source, dict) else None
        if isinstance(query, str) and query.strip():
            return query.strip()
    return ""


def _step_type(step: dict[str, Any]) -> str:
    return str(step.get("type") or "").strip() or "unknown"


def detect_helper_usage_candidates(db: BrixDB) -> list[BrickCandidate]:
    """Detect helpers reused enough to deserve brick review."""
    inventory = build_helper_inventory(db)
    candidates: list[BrickCandidate] = []
    for item in inventory.items:
        if item.strategic_category != "brick_candidate":
            continue
        if len(item.used_by_pipelines) < 2 and item.migration_candidacy != "high":
            continue
        candidates.append(
            BrickCandidate(
                kind="repeated_helper_usage",
                title=f"Helper '{item.name}' is reused across pipelines",
                domain=item.domain,
                confidence="high" if len(item.used_by_pipelines) > 1 else "medium",
                evidence_count=len(item.used_by_pipelines),
                suggested_brick=f"{item.domain}.{_safe_slug(item.name)}",
                evidence=tuple(
                    {"helper": item.name, "pipeline": pipeline}
                    for pipeline in item.used_by_pipelines
                ),
                signals=tuple(dict.fromkeys((*item.signals, "helper_reuse"))),
            )
        )
    return candidates


def _pipeline_steps(db: BrixDB) -> list[tuple[str, list[dict[str, Any]]]]:
    pipelines: list[tuple[str, list[dict[str, Any]]]] = []
    for pipeline in db.list_pipelines():
        pipeline_id = str(pipeline.get("id") or "")
        if not pipeline_id:
            continue
        pipelines.append((str(pipeline.get("name") or pipeline_id), db.get_steps(pipeline_id)))
    return pipelines


def detect_sql_pattern_candidates(db: BrixDB, *, min_occurrences: int = 2) -> list[BrickCandidate]:
    """Detect repeated SQL templates embedded in DB steps."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for pipeline_name, steps in _pipeline_steps(db):
        for step in steps:
            if _step_type(step) not in {"db.query", "db.exec", "db.upsert"}:
                continue
            query = _step_query(step)
            if not query:
                continue
            pattern = normalize_sql_pattern(query)
            grouped.setdefault(pattern, []).append(
                {"pipeline": pipeline_name, "step_id": step.get("id"), "step_type": _step_type(step)}
            )

    candidates: list[BrickCandidate] = []
    for pattern, evidence in sorted(grouped.items()):
        if len(evidence) < min_occurrences:
            continue
        digest = hashlib.sha1(pattern.encode("utf-8")).hexdigest()[:8]
        verb = _sql_verb(pattern)
        candidates.append(
            BrickCandidate(
                kind="repeated_sql_pattern",
                title=f"Repeated {verb.upper()} SQL pattern appears {len(evidence)} times",
                domain="db",
                confidence="high" if len({e["pipeline"] for e in evidence}) > 1 else "medium",
                evidence_count=len(evidence),
                suggested_brick=f"db.{verb}_{digest}",
                evidence=tuple(evidence),
                signals=("embedded_sql", "repeated_sql_template", f"sql_verb:{verb}"),
            )
        )
    return candidates


def detect_step_sequence_candidates(
    db: BrixDB,
    *,
    window_size: int = 3,
    min_occurrences: int = 2,
) -> list[BrickCandidate]:
    """Detect repeated contiguous step-type sequences across DB pipelines."""
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for pipeline_name, steps in _pipeline_steps(db):
        if len(steps) < window_size:
            continue
        types = [_step_type(step) for step in steps]
        for index in range(0, len(types) - window_size + 1):
            window = tuple(types[index:index + window_size])
            grouped.setdefault(window, []).append(
                {
                    "pipeline": pipeline_name,
                    "step_ids": [step.get("id") for step in steps[index:index + window_size]],
                    "step_types": list(window),
                }
            )

    candidates: list[BrickCandidate] = []
    for sequence, evidence in sorted(grouped.items()):
        if len(evidence) < min_occurrences:
            continue
        slug = _safe_slug("_".join(sequence))
        candidates.append(
            BrickCandidate(
                kind="repeated_step_sequence",
                title=f"Repeated step sequence: {' -> '.join(sequence)}",
                domain="flow",
                confidence="high" if len({e["pipeline"] for e in evidence}) > 1 else "medium",
                evidence_count=len(evidence),
                suggested_brick=f"flow.{slug}",
                evidence=tuple(evidence),
                signals=("repeated_step_sequence", f"window_size:{window_size}"),
            )
        )
    return candidates


def detect_brick_candidates(db: BrixDB | None = None) -> BrickCandidateReport:
    """Generate all current brick candidates from DB-backed pipelines and helpers."""
    db = db or BrixDB()
    candidates = [
        *detect_helper_usage_candidates(db),
        *detect_sql_pattern_candidates(db),
        *detect_step_sequence_candidates(db),
    ]
    candidates.sort(key=lambda candidate: (candidate.kind, candidate.suggested_brick, candidate.title))
    summary: dict[str, int] = {"total": len(candidates)}
    for candidate in candidates:
        summary[candidate.kind] = summary.get(candidate.kind, 0) + 1
    return BrickCandidateReport(candidates=tuple(candidates), summary=summary)


def filter_brick_candidate_report(
    report: BrickCandidateReport,
    *,
    helper_names: set[str] | None = None,
    pipeline_names: set[str] | None = None,
) -> BrickCandidateReport:
    """Return a candidate report scoped to helper and/or pipeline evidence."""
    filtered: list[BrickCandidate] = []
    for candidate in report.candidates:
        evidence = candidate.evidence
        if helper_names is not None and candidate.kind == "repeated_helper_usage":
            evidence = tuple(item for item in evidence if item.get("helper") in helper_names)
            if not evidence:
                continue
        if pipeline_names is not None and candidate.kind in {"repeated_sql_pattern", "repeated_step_sequence"}:
            evidence = tuple(item for item in evidence if item.get("pipeline") in pipeline_names)
            if not evidence:
                continue
        filtered.append(
            BrickCandidate(
                kind=candidate.kind,
                title=candidate.title,
                domain=candidate.domain,
                confidence=candidate.confidence,
                evidence_count=len(evidence),
                suggested_brick=candidate.suggested_brick,
                evidence=evidence,
                signals=candidate.signals,
            )
        )

    summary: dict[str, int] = {"total": len(filtered)}
    for candidate in filtered:
        summary[candidate.kind] = summary.get(candidate.kind, 0) + 1
    return BrickCandidateReport(candidates=tuple(filtered), summary=summary)
