from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Iterable

from brix.brick_candidate_detector import detect_brick_candidates
from brix.db import BrixDB
from brix.semantic_retrieval import semantic_search

ALLOWED_REUSE_OUTCOMES: tuple[str, ...] = (
    "reused_existing_component",
    "modified_existing_component",
    "new_component_justified",
)


@dataclass(frozen=True)
class ReuseAssessment:
    entity_type: str
    entity_name: str
    decision_outcome: str
    rationale: str
    reviewed_components: tuple[str, ...]
    similar_cases: tuple[dict[str, Any], ...]
    similar_components: tuple[dict[str, Any], ...]
    pattern_candidates: tuple[dict[str, Any], ...]
    blocking_reason: str | None = None

    @property
    def blocking(self) -> bool:
        return self.blocking_reason is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "entity_name": self.entity_name,
            "decision_outcome": self.decision_outcome,
            "rationale": self.rationale,
            "reviewed_components": list(self.reviewed_components),
            "similar_cases": [dict(item) for item in self.similar_cases],
            "similar_components": [dict(item) for item in self.similar_components],
            "pattern_candidates": [dict(item) for item in self.pattern_candidates],
            "blocking": self.blocking,
            "blocking_reason": self.blocking_reason,
        }


def assess_reuse_for_creation(
    *,
    entity_type: str,
    entity_name: str,
    description: str,
    project: str = "",
    owner: str = "",
    decision_outcome: str = "",
    rationale: str = "",
    reviewed_components: Iterable[str] | None = None,
    db: BrixDB | None = None,
    similar_components_override: Iterable[dict[str, Any]] | None = None,
) -> ReuseAssessment:
    db = db or BrixDB()
    query_text = " ".join(part for part in (entity_name, description) if part).strip()
    similar_cases = tuple(_similar_cases(query_text))
    if similar_components_override is None:
        similar_components = tuple(_similar_components(entity_type, entity_name, description, db))
    else:
        similar_components = tuple(similar_components_override)
    pattern_candidates = tuple(_pattern_candidates(description or entity_name, db))

    reviewed = tuple(str(item).strip() for item in (reviewed_components or []) if str(item).strip())
    normalized_outcome = str(decision_outcome or "").strip()
    normalized_rationale = str(rationale or "").strip()
    substantial_matches = bool(similar_components)

    if normalized_outcome and normalized_outcome not in ALLOWED_REUSE_OUTCOMES:
        return ReuseAssessment(
            entity_type=entity_type,
            entity_name=entity_name,
            decision_outcome=normalized_outcome,
            rationale=normalized_rationale,
            reviewed_components=reviewed,
            similar_cases=similar_cases,
            similar_components=similar_components,
            pattern_candidates=pattern_candidates,
            blocking_reason=(
                "Invalid reuse_decision_outcome. Use one of: "
                + ", ".join(ALLOWED_REUSE_OUTCOMES)
            ),
        )

    if not normalized_outcome:
        if substantial_matches:
            return ReuseAssessment(
                entity_type=entity_type,
                entity_name=entity_name,
                decision_outcome="",
                rationale="",
                reviewed_components=reviewed,
                similar_cases=similar_cases,
                similar_components=similar_components,
                pattern_candidates=pattern_candidates,
                blocking_reason=(
                    "Reuse review is required because similar components or repeated patterns already exist. "
                    "Set reuse_decision_outcome and reuse_reviewed_components explicitly."
                ),
            )
        normalized_outcome = "new_component_justified"
        normalized_rationale = "No similar reusable component or repeated pattern was detected automatically."

    if normalized_outcome in {"reused_existing_component", "modified_existing_component"} and not reviewed:
        return ReuseAssessment(
            entity_type=entity_type,
            entity_name=entity_name,
            decision_outcome=normalized_outcome,
            rationale=normalized_rationale,
            reviewed_components=reviewed,
            similar_cases=similar_cases,
            similar_components=similar_components,
            pattern_candidates=pattern_candidates,
            blocking_reason="reuse_reviewed_components must list the compared component references for this outcome.",
        )

    if normalized_outcome == "new_component_justified" and not normalized_rationale:
        return ReuseAssessment(
            entity_type=entity_type,
            entity_name=entity_name,
            decision_outcome=normalized_outcome,
            rationale=normalized_rationale,
            reviewed_components=reviewed,
            similar_cases=similar_cases,
            similar_components=similar_components,
            pattern_candidates=pattern_candidates,
            blocking_reason="reuse_rationale is required when a new component is justified.",
        )

    return ReuseAssessment(
        entity_type=entity_type,
        entity_name=entity_name,
        decision_outcome=normalized_outcome,
        rationale=normalized_rationale,
        reviewed_components=reviewed,
        similar_cases=similar_cases,
        similar_components=similar_components,
        pattern_candidates=pattern_candidates,
    )


def blocking_reuse_response(assessment: ReuseAssessment) -> dict[str, Any]:
    return {
        "success": False,
        "error": assessment.blocking_reason or "Reuse review is incomplete.",
        "reuse_review": assessment.as_dict(),
        "repair_prompts": _reuse_repair_prompts(assessment),
    }


def apply_reuse_result(result: dict[str, Any], assessment: ReuseAssessment) -> dict[str, Any]:
    result["reuse_review"] = assessment.as_dict()
    if not assessment.blocking and assessment.decision_outcome == "new_component_justified":
        result.setdefault("warnings", []).append(
            "REUSE REVIEW: no close reusable match was detected automatically; new component justification was recorded."
        )
    return result


def persist_reuse_review(
    *,
    db: BrixDB,
    assessment: ReuseAssessment,
    project: str = "",
    owner: str = "",
) -> dict[str, Any]:
    review_name = _reuse_entity_name(assessment.entity_type, assessment.entity_name)
    title = f"Reuse review for {assessment.entity_type} '{assessment.entity_name}'"
    payload = {
        "component_type": assessment.entity_type,
        "component_name": assessment.entity_name,
        "decision_outcome": assessment.decision_outcome,
        "rationale": assessment.rationale,
        "reviewed_components": list(assessment.reviewed_components),
        "similar_cases": [dict(item) for item in assessment.similar_cases],
        "similar_components": [dict(item) for item in assessment.similar_components],
        "pattern_candidates": [dict(item) for item in assessment.pattern_candidates],
    }
    existing = db.knowledge_entity_get(review_name)
    if existing is None:
        review = db.knowledge_entity_add(
            "reuse",
            review_name,
            title,
            summary=assessment.rationale,
            rationale=assessment.rationale,
            lifecycle_stage="active",
            status=assessment.decision_outcome,
            owner=owner,
            project=project,
            content=payload,
        )
    else:
        review = db.knowledge_entity_update(
            review_name,
            title=title,
            summary=assessment.rationale,
            rationale=assessment.rationale,
            lifecycle_stage="active",
            status=assessment.decision_outcome,
            owner=owner or existing.get("owner", ""),
            project=project or existing.get("project", ""),
            content=payload,
        ) or existing

    existing_links = db.knowledge_link_list(entity_type="reuse", entity_id=review["id"])
    if not any(
        link["relation_type"] == "documents"
        and link["target_entity_type"] == assessment.entity_type
        and link["target_entity_id"] == assessment.entity_name
        for link in existing_links
    ):
        db.knowledge_link_add("reuse", review["id"], "documents", assessment.entity_type, assessment.entity_name)

    for ref in assessment.reviewed_components:
        parsed = _parse_component_ref(ref)
        if parsed is None:
            continue
        target_type, target_id = parsed
        if any(
            link["relation_type"] == "compared_against"
            and link["target_entity_type"] == target_type
            and link["target_entity_id"] == target_id
            for link in existing_links
        ):
            continue
        try:
            db.knowledge_link_add("reuse", review["id"], "compared_against", target_type, target_id)
        except ValueError:
            continue
    return review


def extract_reuse_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    reviewed = arguments.get("reuse_reviewed_components") or []
    if not isinstance(reviewed, list):
        reviewed = [reviewed]
    return {
        "decision_outcome": arguments.get("reuse_decision_outcome", ""),
        "rationale": arguments.get("reuse_rationale", ""),
        "reviewed_components": reviewed,
    }


def _similar_cases(query_text: str) -> list[dict[str, Any]]:
    if not query_text.strip():
        return []
    try:
        result = semantic_search(query_text, entity_types=["intent", "decision", "reuse", "task"], limit=5)
    except Exception:
        return []
    return result.get("matches", [])


def _similar_components(
    entity_type: str,
    entity_name: str,
    description: str,
    db: BrixDB,
) -> list[dict[str, Any]]:
    query = " ".join(part for part in (entity_name, description) if part).strip()
    if entity_type == "pipeline":
        related: list[dict[str, Any]] = []
        for pipeline in db.list_pipelines():
            candidate_name = str(pipeline.get("name") or "")
            if not candidate_name or candidate_name == entity_name:
                continue
            candidate_desc = str(pipeline.get("description") or "")
            score = max(
                SequenceMatcher(None, entity_name.lower(), candidate_name.lower()).ratio(),
                _token_similarity(query, f"{candidate_name} {candidate_desc}"),
            )
            if score < 0.75:
                continue
            related.append(
                {
                    "entity_type": "pipeline",
                    "entity_id": candidate_name,
                    "reason": f"score={score:.2f}",
                }
            )
        return related[:5]

    query_lower = query.lower()
    related: list[dict[str, Any]] = []
    for brick in db.brick_definitions_list():
        brick_name = str(brick.get("name") or "")
        if not brick_name or brick_name == entity_name:
            continue
        haystack = " ".join(
            [
                brick_name,
                str(brick.get("description") or ""),
                str(brick.get("when_to_use") or ""),
                str(brick.get("when_NOT_to_use") or ""),
            ]
        ).lower()
        score = max(
            SequenceMatcher(None, entity_name.lower(), brick_name.lower()).ratio(),
            _token_similarity(query_lower, haystack),
        )
        if score < 0.75:
            continue
        related.append(
            {
                "entity_type": "brick",
                "entity_id": brick_name,
                "reason": f"score={score:.2f}",
            }
        )
    return related[:5]


def _pattern_candidates(query: str, db: BrixDB) -> list[dict[str, Any]]:
    tokens = {token for token in re.findall(r"[a-z0-9]+", query.lower()) if len(token) > 2}
    if not tokens:
        return []
    report = detect_brick_candidates(db).as_dict()
    matches: list[dict[str, Any]] = []
    for candidate in report.get("candidates", []):
        haystack = " ".join(
            [
                candidate.get("title", ""),
                candidate.get("suggested_brick", ""),
                " ".join(candidate.get("signals", [])),
            ]
        ).lower()
        if not any(token in haystack for token in tokens):
            continue
        matches.append(candidate)
    return matches[:5]


def _token_similarity(left: str, right: str) -> float:
    left_tokens = {token for token in re.findall(r"[a-z0-9]+", left.lower()) if len(token) > 2}
    right_tokens = {token for token in re.findall(r"[a-z0-9]+", right.lower()) if len(token) > 2}
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = left_tokens & right_tokens
    union = left_tokens | right_tokens
    return len(intersection) / len(union)


def _reuse_entity_name(entity_type: str, entity_name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", entity_name.lower()).strip("-") or "component"
    return f"reuse-{entity_type}-{slug}"


def _parse_component_ref(ref: str) -> tuple[str, str] | None:
    if ":" not in ref:
        return None
    entity_type, entity_id = ref.split(":", 1)
    entity_type = entity_type.strip()
    entity_id = entity_id.strip()
    if entity_type not in {"pipeline", "brick", "helper", "intent", "decision", "reuse"}:
        return None
    if not entity_id:
        return None
    return entity_type, entity_id


def _reuse_repair_prompts(assessment: ReuseAssessment) -> list[str]:
    prompts = [
        "Set reuse_decision_outcome to one of: reused_existing_component, modified_existing_component, new_component_justified.",
        "List compared components in reuse_reviewed_components, e.g. ['pipeline:buddy-hmk-extract', 'brick:source.download_to_file'].",
    ]
    if assessment.similar_components:
        prompts.append(
            "Review the detected similar components before creating a new one."
        )
    if assessment.pattern_candidates:
        prompts.append(
            "Review repeated-pattern candidates; a new brick may not be justified if an existing promotion path already exists."
        )
    return prompts
