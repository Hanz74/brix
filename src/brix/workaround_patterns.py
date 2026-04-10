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

    def as_registry_content(self) -> dict[str, Any]:
        return {
            "kind": "workaround_pattern",
            "title": self.title,
            "description": self.description,
            "trigger_codes": list(self.trigger_codes),
            "severity": self.severity,
            "repair_hint": self.repair_hint,
            "rationale": self.rationale,
        }


@dataclass(frozen=True)
class WorkaroundPatternMatch:
    pattern: WorkaroundPattern
    finding_code: str
    finding_severity: str
    step_id: str | None
    field: str | None


DEFAULT_WORKAROUND_PATTERNS: tuple[WorkaroundPattern, ...] = (
    WorkaroundPattern(
        name="db_query_used_for_dml",
        title="db.query used for DML",
        description="Using db.query for INSERT/UPDATE/DELETE is a workaround because the brick is SELECT-only.",
        trigger_codes=("DB_QUERY_DML",),
        severity="error",
        repair_hint="Replace db.query with db.exec and use the db.exec contract for DML statements.",
        rationale="DML over db.query hides commit semantics and normalizes a known anti-pattern.",
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
        patterns = trigger_map.get(getattr(finding, "code", ""), [])
        for pattern in patterns:
            key = (pattern.name, getattr(finding, "step_id", None), getattr(finding, "field", None))
            if key in seen:
                continue
            seen.add(key)
            matches.append(
                WorkaroundPatternMatch(
                    pattern=pattern,
                    finding_code=getattr(finding, "code", ""),
                    finding_severity=getattr(finding, "severity", "warning"),
                    step_id=getattr(finding, "step_id", None),
                    field=getattr(finding, "field", None),
                )
            )
    return matches
