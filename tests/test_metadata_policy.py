from brix.metadata_policy import (
    REQUIRED_METADATA_MATRIX,
    enforcement_severity_map,
    evaluate_required_metadata,
)
import pytest


def test_metadata_matrix_covers_governed_entity_types():
    assert "pipeline" in REQUIRED_METADATA_MATRIX
    assert "brick" in REQUIRED_METADATA_MATRIX
    assert "helper" in REQUIRED_METADATA_MATRIX
    assert "intent" in REQUIRED_METADATA_MATRIX
    assert "decision" in REQUIRED_METADATA_MATRIX


def test_active_pipeline_missing_metadata_escalates_to_errors():
    violations = evaluate_required_metadata(
        "pipeline",
        {
            "project": "buddy",
            "description": "",
            "owner": "",
            "purpose": "",
            "source_intent_id": "",
            "lifecycle_stage": "active",
        },
    )

    severities = {item.field: item.severity for item in violations}
    assert severities["description"] == "error"
    assert severities["owner"] == "error"
    assert severities["purpose"] == "error"
    assert severities["source_intent_id"] == "error"


def test_draft_helper_metadata_stays_non_blocking_until_governed():
    draft_violations = evaluate_required_metadata(
        "helper",
        {
            "description": "Legacy helper",
            "owner": "",
            "governance_status": "draft",
        },
    )
    governed_violations = evaluate_required_metadata(
        "helper",
        {
            "description": "Legacy helper",
            "owner": "",
            "governance_status": "governed",
        },
    )

    assert {item.field: item.severity for item in draft_violations} == {
        "owner": "warning",
        "reason_not_a_brick": "warning",
    }
    assert {item.field: item.severity for item in governed_violations} == {
        "owner": "error",
        "reason_not_a_brick": "error",
    }


def test_alternative_metadata_fields_satisfy_requirement():
    violations = evaluate_required_metadata(
        "helper",
        {
            "description": "Candidate helper",
            "owner": "platform",
            "brick_candidate_ref": "source.download_to_file",
            "governance_status": "governed",
        },
    )
    assert violations == []


def test_enforcement_severity_map_is_explicit():
    severity_map = enforcement_severity_map()
    assert severity_map["pipeline"]["project"]["active"] == "error"
    assert severity_map["helper"]["reason_not_a_brick"]["active"] == "error"
    assert severity_map["brick"]["examples"]["draft"] == "info"


def test_unknown_entity_types_are_rejected():
    with pytest.raises(ValueError, match="Unknown entity_type"):
        evaluate_required_metadata("nonexistent", {})
