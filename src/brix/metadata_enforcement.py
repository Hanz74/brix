from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from brix.metadata_policy import MetadataViolation, evaluate_required_metadata, is_active_like

SUPPLEMENTAL_METADATA_FIELDS: tuple[str, ...] = (
    "owner",
    "purpose",
    "source_intent_id",
    "lifecycle_stage",
    "status",
    "usage_scope",
    "version_relevance",
    "linked_topic",
    "replacement_plan",
    "expiry_condition",
)

_FIELD_EXAMPLES: dict[str, str] = {
    "owner": "owner='team-brix'",
    "purpose": "purpose='Describe why this component exists and who depends on it.'",
    "source_intent_id": "source_intent_id='intent-123'",
    "lifecycle_stage": "lifecycle_stage='draft' or lifecycle_stage='active'",
    "status": "status='draft' or status='stable'",
    "usage_scope": "usage_scope='hmk extraction only'",
    "version_relevance": "version_relevance='>=14.0.0'",
    "linked_topic": "linked_topic='validator / db.exec'",
    "replacement_plan": "replacement_plan='replace with brick source.download_to_file'",
    "expiry_condition": "expiry_condition='remove after HMK migration completes'",
}


@dataclass(frozen=True)
class MetadataEnforcementResult:
    entity_type: str
    merged_data: dict[str, Any]
    stored_metadata: dict[str, str]
    violations: tuple[MetadataViolation, ...]
    blocking: tuple[MetadataViolation, ...]
    repair_prompts: tuple[str, ...]
    draft_enforced: bool

    @property
    def warnings(self) -> list[str]:
        messages = [violation.message for violation in self.violations if violation not in self.blocking]
        if self.draft_enforced and self.violations:
            messages.insert(
                0,
                (
                    f"{self.entity_type} remains draft until required metadata is completed. "
                    "Use the repair prompts below and then promote it to active/governed."
                ),
            )
        return messages

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "draft_enforced": self.draft_enforced,
            "violations": [
                {
                    "field": violation.field,
                    "severity": violation.severity,
                    "message": violation.message,
                    "alternatives": list(violation.alternatives),
                }
                for violation in self.violations
            ],
            "blocking_fields": [violation.field for violation in self.blocking],
            "repair_prompts": list(self.repair_prompts),
            "stored_metadata": dict(self.stored_metadata),
        }


def extract_supplemental_metadata(arguments: dict[str, Any]) -> dict[str, str]:
    return {
        field: str(arguments.get(field) or "").strip()
        for field in SUPPLEMENTAL_METADATA_FIELDS
        if field in arguments and arguments.get(field) is not None
    }


def assess_metadata_enforcement(
    entity_type: str,
    *,
    base_data: dict[str, Any],
    incoming_metadata: dict[str, Any] | None = None,
    existing_data: dict[str, Any] | None = None,
    existing_metadata: dict[str, Any] | None = None,
    operation: str = "create",
) -> MetadataEnforcementResult:
    merged_data: dict[str, Any] = {}
    for source in (existing_data or {}, existing_metadata or {}, base_data, incoming_metadata or {}):
        merged_data.update({key: value for key, value in source.items() if value is not None})

    requested_active = is_active_like(entity_type, merged_data)
    violations = tuple(evaluate_required_metadata(entity_type, merged_data))
    if operation == "update" and requested_active:
        blocking = tuple(violation for violation in violations if violation.severity == "error")
    else:
        blocking = ()

    draft_enforced = bool(violations) and not blocking
    stored_metadata = {
        field: str(merged_data.get(field) or "").strip()
        for field in SUPPLEMENTAL_METADATA_FIELDS
        if str(merged_data.get(field) or "").strip()
    }
    if draft_enforced and "lifecycle_stage" in SUPPLEMENTAL_METADATA_FIELDS:
        stored_metadata.setdefault("lifecycle_stage", "draft")
        merged_data.setdefault("lifecycle_stage", "draft")
    if draft_enforced and entity_type == "helper":
        merged_data["governance_status"] = "draft"

    return MetadataEnforcementResult(
        entity_type=entity_type,
        merged_data=merged_data,
        stored_metadata=stored_metadata,
        violations=violations,
        blocking=blocking,
        repair_prompts=tuple(_repair_prompt(entity_type, violation) for violation in violations),
        draft_enforced=draft_enforced,
    )


def apply_metadata_result(result: dict[str, Any], assessment: MetadataEnforcementResult) -> dict[str, Any]:
    if assessment.violations:
        result["metadata_policy"] = assessment.as_dict()
    if assessment.warnings:
        result.setdefault("warnings", [])
        result["warnings"].extend(assessment.warnings)
    if assessment.repair_prompts:
        result["repair_prompts"] = list(assessment.repair_prompts)
    return result


def blocking_metadata_response(assessment: MetadataEnforcementResult) -> dict[str, Any]:
    return {
        "success": False,
        "error": (
            "Required metadata is incomplete for an active/governed entity. "
            "Complete the missing fields or downgrade it to draft first."
        ),
        "metadata_policy": assessment.as_dict(),
        "repair_prompts": list(assessment.repair_prompts),
    }


def _repair_prompt(entity_type: str, violation: MetadataViolation) -> str:
    alternatives = ""
    if violation.alternatives:
        alternatives = f" Alternative fields: {', '.join(violation.alternatives)}."
    example = _FIELD_EXAMPLES.get(violation.field, f"{violation.field}='...value...'")
    return (
        f"Repair {entity_type}.{violation.field}: set {example}.{alternatives} "
        f"Reason: {violation.message}"
    )
