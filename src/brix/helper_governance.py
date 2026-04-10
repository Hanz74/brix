"""Helper governance rules for brick-first helper creation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

REQUIRED_HELPER_METADATA = ("description", "input_schema", "output_schema", "project", "tags")
HELPER_JUSTIFICATION_FIELDS = ("reason_not_a_brick", "brick_candidate_ref")


@dataclass(frozen=True)
class HelperGovernance:
    """Governance assessment for one helper."""

    status: str
    missing_metadata: tuple[str, ...]
    missing_justification: bool
    reason_not_a_brick: str = ""
    brick_candidate_ref: str = ""

    @property
    def is_complete(self) -> bool:
        return not self.missing_metadata and not self.missing_justification

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "missing_metadata": list(self.missing_metadata),
            "missing_justification": self.missing_justification,
            "reason_not_a_brick": self.reason_not_a_brick,
            "brick_candidate_ref": self.brick_candidate_ref,
            "is_complete": self.is_complete,
        }


def assess_helper_governance(data: dict[str, Any]) -> HelperGovernance:
    """Assess helper metadata completeness and brick justification."""
    missing: list[str] = []
    for field in REQUIRED_HELPER_METADATA:
        value = data.get(field)
        if field == "description":
            if not isinstance(value, str) or len(value.strip()) < 10:
                missing.append(field)
        elif field in {"input_schema", "output_schema"}:
            if not isinstance(value, dict) or not value:
                missing.append(field)
        elif field == "tags":
            if not isinstance(value, list) or not value:
                missing.append(field)
        elif not value:
            missing.append(field)

    reason = str(data.get("reason_not_a_brick") or "").strip()
    candidate_ref = str(data.get("brick_candidate_ref") or "").strip()
    missing_justification = not reason and not candidate_ref
    status = "governed" if not missing and not missing_justification else "draft"
    return HelperGovernance(
        status=status,
        missing_metadata=tuple(missing),
        missing_justification=missing_justification,
        reason_not_a_brick=reason,
        brick_candidate_ref=candidate_ref,
    )


def governance_warnings(governance: HelperGovernance) -> list[str]:
    """Return user-facing governance warnings."""
    warnings: list[str] = []
    if governance.missing_metadata:
        warnings.append(
            "HELPER GOVERNANCE: Missing metadata fields: "
            + ", ".join(governance.missing_metadata)
            + ". Helper remains draft until metadata is complete."
        )
    if governance.missing_justification:
        warnings.append(
            "HELPER GOVERNANCE: Provide 'reason_not_a_brick' or 'brick_candidate_ref' "
            "so the helper is explicit migration debt, not silent product design."
        )
    return warnings
