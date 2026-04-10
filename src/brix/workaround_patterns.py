from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from brix.db import BrixDB


@dataclass(frozen=True)
class WorkaroundPattern:
    name: str
    title: str
    description: str
    trigger_codes: tuple[str, ...]
    severity: str = "warning"
    repair_hint: str = ""
    rationale: str = ""
    requires_annotation: bool = False

    def as_registry_content(self) -> dict[str, Any]:
        return {
            "kind": "workaround_pattern",
            "title": self.title,
            "description": self.description,
            "trigger_codes": list(self.trigger_codes),
            "severity": self.severity,
            "repair_hint": self.repair_hint,
            "rationale": self.rationale,
            "requires_annotation": self.requires_annotation,
        }


@dataclass(frozen=True)
class WorkaroundPatternMatch:
    pattern: WorkaroundPattern
    finding_code: str
    finding_severity: str
    step_id: str | None
    field: str | None


@dataclass(frozen=True)
class WorkaroundAnnotationAssessment:
    patterns: tuple[str, ...]
    missing_fields: tuple[str, ...]

    @property
    def blocking(self) -> bool:
        return bool(self.patterns and self.missing_fields)


DEFAULT_WORKAROUND_PATTERNS: tuple[WorkaroundPattern, ...] = (
    WorkaroundPattern(
        name="db_query_used_for_dml",
        title="db.query used for DML",
        description="Using db.query for INSERT/UPDATE/DELETE is a workaround because the brick is SELECT-only.",
        trigger_codes=("DB_QUERY_DML",),
        severity="error",
        repair_hint="Replace db.query with db.exec and use the db.exec contract for DML statements.",
        rationale="DML over db.query hides commit semantics and normalizes a known anti-pattern.",
        requires_annotation=True,
    ),
    WorkaroundPattern(
        name="runner_fields_outside_config",
        title="Runner fields kept as top-level compatibility inputs",
        description="Top-level runner-specific fields are transitional compatibility shims and should move under config.",
        trigger_codes=("RUNNER_TOP_LEVEL_FIELD_COMPAT",),
        severity="warning",
        repair_hint="Move runner-specific top-level fields into config so the brick schema remains the single contract owner.",
        rationale="Leaving runner semantics at the Step top level preserves a legacy workaround shape.",
    ),
    WorkaroundPattern(
        name="helper_without_brick_justification",
        title="Helper without brick justification",
        description="Using a helper without brick-first justification is a workaround against the reusable component model.",
        trigger_codes=("HELPER_GOVERNANCE_INCOMPLETE",),
        severity="warning",
        repair_hint="Record reason_not_a_brick or brick_candidate_ref, or replace the helper with a reusable brick.",
        rationale="Helpers must be explicit brick exceptions rather than hidden pipeline-local logic carriers.",
        requires_annotation=True,
    ),
)


def ensure_default_workaround_patterns(db: BrixDB) -> int:
    inserted = 0
    for pattern in DEFAULT_WORKAROUND_PATTERNS:
        existing = db.registry_get("patterns", pattern.name)
        if existing is not None:
            continue
        db.registry_add(
            "patterns",
            pattern.name,
            content=pattern.as_registry_content(),
            tags=["workaround", "governance"],
            description=pattern.description,
        )
        inserted += 1
    return inserted


def load_workaround_patterns(db: BrixDB | None = None) -> list[WorkaroundPattern]:
    db = db or BrixDB()
    patterns: dict[str, WorkaroundPattern] = {pattern.name: pattern for pattern in DEFAULT_WORKAROUND_PATTERNS}
    try:
        for entry in db.registry_list("patterns"):
            content = entry.get("content") if isinstance(entry, dict) else {}
            tags = entry.get("tags", []) if isinstance(entry, dict) else []
            if not isinstance(content, dict):
                continue
            if content.get("kind") != "workaround_pattern" and "workaround" not in tags:
                continue
            name = str(entry.get("name") or "").strip()
            if not name:
                continue
            trigger_codes = content.get("trigger_codes") or []
            if not isinstance(trigger_codes, list) or not trigger_codes:
                continue
            patterns[name] = WorkaroundPattern(
                name=name,
                title=str(content.get("title") or name),
                description=str(content.get("description") or entry.get("description") or ""),
                trigger_codes=tuple(str(code) for code in trigger_codes if str(code).strip()),
                severity=str(content.get("severity") or "warning"),
                repair_hint=str(content.get("repair_hint") or ""),
                rationale=str(content.get("rationale") or ""),
                requires_annotation=bool(content.get("requires_annotation", False)),
            )
    except Exception:
        pass
    return list(patterns.values())


def detect_workaround_pattern_matches(findings: list[Any], db: BrixDB | None = None) -> list[WorkaroundPatternMatch]:
    trigger_map: dict[str, list[WorkaroundPattern]] = {}
    for pattern in load_workaround_patterns(db):
        for trigger_code in pattern.trigger_codes:
            trigger_map.setdefault(trigger_code, []).append(pattern)

    matches: list[WorkaroundPatternMatch] = []
    seen: set[tuple[str, str | None, str | None]] = set()
    for finding in findings:
        finding_code = _finding_value(finding, "code", "")
        finding_severity = _finding_value(finding, "severity", "warning")
        step_id = _finding_value(finding, "step_id")
        field = _finding_value(finding, "field")
        patterns = trigger_map.get(finding_code, [])
        for pattern in patterns:
            key = (pattern.name, step_id, field)
            if key in seen:
                continue
            seen.add(key)
            matches.append(
                WorkaroundPatternMatch(
                    pattern=pattern,
                    finding_code=finding_code,
                    finding_severity=finding_severity,
                    step_id=step_id,
                    field=field,
                )
            )
    return matches


def assess_workaround_annotation(
    findings: list[Any],
    metadata: dict[str, Any] | None,
    db: BrixDB | None = None,
) -> WorkaroundAnnotationAssessment:
    matches = detect_workaround_pattern_matches(findings, db)
    actionable_matches = [match for match in matches if match.pattern.requires_annotation]
    pattern_names = tuple(sorted({match.pattern.name for match in actionable_matches}))
    metadata = metadata or {}
    missing_fields = tuple(
        field_name
        for field_name in ("owner", "replacement_plan", "expiry_condition")
        if not str(metadata.get(field_name) or "").strip()
    )
    return WorkaroundAnnotationAssessment(patterns=pattern_names, missing_fields=missing_fields)


def _finding_value(finding: Any, key: str, default: Any = None) -> Any:
    if isinstance(finding, dict):
        return finding.get(key, default)
    return getattr(finding, key, default)
