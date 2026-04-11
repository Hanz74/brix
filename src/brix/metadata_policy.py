from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class MetadataRequirement:
    field: str
    description: str
    severity_if_active: str = "error"
    severity_if_draft: str = "warning"
    alternatives: tuple[str, ...] = ()


@dataclass(frozen=True)
class MetadataViolation:
    entity_type: str
    field: str
    severity: str
    message: str
    alternatives: tuple[str, ...] = ()


REQUIRED_METADATA_MATRIX: dict[str, tuple[MetadataRequirement, ...]] = {
    "pipeline": (
        MetadataRequirement("project", "Project ownership."),
        MetadataRequirement("description", "Human-readable behavior summary."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement("purpose", "Operational purpose / why it exists."),
        MetadataRequirement("source_intent_id", "Linked source intent."),
        MetadataRequirement("lifecycle_stage", "Lifecycle status such as draft/active."),
    ),
    "brick": (
        MetadataRequirement("description", "Human-readable behavior summary."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement("input_type", "Input contract."),
        MetadataRequirement("output_type", "Output contract."),
        MetadataRequirement("when_NOT_to_use", "Anti-pattern guidance."),
        MetadataRequirement("examples", "Concrete usage examples.", severity_if_draft="info"),
    ),
    "helper": (
        MetadataRequirement("description", "Human-readable behavior summary."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement(
            "reason_not_a_brick",
            "Brick-first justification or reusable brick reference.",
            alternatives=("brick_candidate_ref",),
        ),
    ),
    "connection": (
        MetadataRequirement("description", "Human-readable behavior summary."),
        MetadataRequirement("project", "Project ownership."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement("usage_scope", "Intended operational scope."),
    ),
    "help_topic": (
        MetadataRequirement("owner", "Responsible owner or maintainer."),
        MetadataRequirement("version_relevance", "Version or release relevance."),
        MetadataRequirement("linked_topic", "Linked brick or topic domain."),
    ),
    "intent": (
        MetadataRequirement("project", "Project ownership."),
        MetadataRequirement("title", "Readable title."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement("lifecycle_stage", "Lifecycle status such as draft/active."),
    ),
    "task": (
        MetadataRequirement("project", "Project ownership."),
        MetadataRequirement("title", "Readable title."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement("lifecycle_stage", "Lifecycle status such as draft/active."),
    ),
    "decision": (
        MetadataRequirement("project", "Project ownership."),
        MetadataRequirement("title", "Readable title."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement("rationale", "Decision rationale."),
        MetadataRequirement("lifecycle_stage", "Lifecycle status such as draft/active."),
    ),
    "workaround": (
        MetadataRequirement("project", "Project ownership."),
        MetadataRequirement("title", "Readable title."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement("replacement_plan", "Replacement or removal plan."),
        MetadataRequirement("expiry_condition", "Expiry condition or review trigger."),
    ),
    "reuse": (
        MetadataRequirement("project", "Project ownership."),
        MetadataRequirement("title", "Readable title."),
        MetadataRequirement("owner", "Responsible owner or team."),
        MetadataRequirement("decision_outcome", "Explicit reuse outcome.", alternatives=("status",)),
    ),
}


def enforcement_severity_map() -> dict[str, dict[str, dict[str, str]]]:
    return {
        entity_type: {
            requirement.field: {
                "active": requirement.severity_if_active,
                "draft": requirement.severity_if_draft,
            }
            for requirement in requirements
        }
        for entity_type, requirements in REQUIRED_METADATA_MATRIX.items()
    }


def is_active_like(entity_type: str, data: dict[str, Any]) -> bool:
    lifecycle_stage = str(data.get("lifecycle_stage") or "").strip().lower()
    if lifecycle_stage in {"active", "resolved", "governed"}:
        return True

    for key in ("status", "governance_status"):
        value = str(data.get(key) or "").strip().lower()
        if value in {"active", "accepted", "stable", "governed"}:
            return True

    if entity_type == "helper":
        return str(data.get("governance_status") or "").strip().lower() == "governed"
    return False


def evaluate_required_metadata(
    entity_type: str,
    data: dict[str, Any],
) -> list[MetadataViolation]:
    requirements = REQUIRED_METADATA_MATRIX.get(entity_type, ())
    if not requirements:
        valid = ", ".join(sorted(REQUIRED_METADATA_MATRIX))
        raise ValueError(f"Unknown entity_type '{entity_type}'. Valid types: {valid}")

    active_like = is_active_like(entity_type, data)
    violations: list[MetadataViolation] = []
    for requirement in requirements:
        if _has_requirement_value(data, requirement):
            continue
        severity = requirement.severity_if_active if active_like else requirement.severity_if_draft
        alternatives = requirement.alternatives
        alt_text = f" or one of {', '.join(alternatives)}" if alternatives else ""
        violations.append(
            MetadataViolation(
                entity_type=entity_type,
                field=requirement.field,
                severity=severity,
                message=(
                    f"{entity_type} missing required metadata '{requirement.field}'"
                    f"{alt_text}: {requirement.description}"
                ),
                alternatives=alternatives,
            )
        )
    return violations


def _has_requirement_value(data: dict[str, Any], requirement: MetadataRequirement) -> bool:
    candidate_fields = (requirement.field, *requirement.alternatives)
    for field in candidate_fields:
        value = data.get(field)
        if _is_present(value):
            return True
    return False


def _is_present(value: Optional[Any]) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True
