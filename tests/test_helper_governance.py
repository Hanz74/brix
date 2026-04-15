"""Tests for helper governance requirements."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from brix.db import BrixDB
from brix.helper_governance import assess_helper_governance
from brix.helper_registry import HelperRegistry
from brix.mcp_handlers.helpers import (
    _handle_create_helper,
    _handle_get_helper,
    _handle_list_helpers,
    _handle_update_helper,
)
from brix.mcp_handlers.pipelines import _handle_rollback
from brix.models import Pipeline, Step
from brix.validator import PipelineValidator


def test_assess_helper_governance_requires_metadata_and_justification() -> None:
    draft = assess_helper_governance({"description": "short"})
    assert draft.status == "draft"
    assert "project" in draft.missing_metadata
    assert draft.missing_justification is True

    governed = assess_helper_governance(
        {
            "description": "Narrow one-off helper for a vendor-specific edge case.",
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
            "project": "buddy",
            "tags": ["extract"],
            "reason_not_a_brick": "Vendor-specific one-off logic, not reusable yet.",
        }
    )
    assert governed.status == "governed"
    assert governed.is_complete is True


def test_create_helper_surfaces_and_persists_governance(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "governance.db")
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.db.BrixDB", lambda: db)
    monkeypatch.setattr("brix.mcp_handlers.helpers._audit_db.write_audit_entry", lambda **_: None)
    monkeypatch.setattr("brix.mcp_handlers.helpers._find_similar_helpers", lambda *_: [])

    result = asyncio.run(
        _handle_create_helper(
            {
                "name": "governed_helper",
                "code": "def run(data): return data",
                "description": "Narrow one-off helper for a vendor-specific edge case.",
                "input_schema": {"type": "object"},
                "output_schema": {"type": "object"},
                "project": "buddy",
                "tags": ["extract"],
                "owner": "team-brix",
                "reason_not_a_brick": "Vendor-specific one-off logic, not reusable yet.",
            }
        )
    )

    assert result["success"] is True
    assert result["governance"]["status"] == "governed"
    row = db.get_helper("governed_helper")
    assert row["governance_status"] == "governed"
    assert row["reason_not_a_brick"]

    fetched = asyncio.run(_handle_get_helper({"name": "governed_helper"}))
    assert fetched["helper"]["governance_status"] == "governed"
    assert fetched["helper"]["reason_not_a_brick"]


def test_create_helper_without_justification_is_draft_not_silent(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "draft.db")
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.db.BrixDB", lambda: db)
    monkeypatch.setattr("brix.mcp_handlers.helpers._audit_db.write_audit_entry", lambda **_: None)
    monkeypatch.setattr("brix.mcp_handlers.helpers._find_similar_helpers", lambda *_: [])

    result = asyncio.run(
        _handle_create_helper(
            {
                "name": "draft_helper",
                "code": "def run(data): return data",
                "description": "Helper with schemas but no brick justification.",
                "input_schema": {"type": "object"},
                "output_schema": {"type": "object"},
                "project": "buddy",
                "tags": ["utility"],
            }
        )
    )

    assert result["success"] is True
    assert result["governance"]["status"] == "draft"
    assert any("reason_not_a_brick" in warning for warning in result["warnings"])
    assert db.get_helper("draft_helper")["governance_status"] == "draft"


def test_update_helper_can_complete_governance(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "update.db")
    registry = HelperRegistry(db=db)
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.db.BrixDB", lambda: db)
    monkeypatch.setattr("brix.mcp_handlers.helpers._audit_db.write_audit_entry", lambda **_: None)
    registry.register(
        name="needs_governance",
        description="Helper with schemas but no brick justification.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
    )

    result = asyncio.run(
        _handle_update_helper(
            {
                "name": "needs_governance",
                "project": "buddy",
                "tags": ["utility"],
                "owner": "team-brix",
                "reason_not_a_brick": "Temporary adapter until a stable brick contract emerges.",
            }
        )
    )

    assert result["success"] is True
    assert db.get_helper("needs_governance")["governance_status"] == "governed"


def test_update_helper_recomputes_governance_when_schemas_change(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "schema_update.db")
    registry = HelperRegistry(db=db)
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.db.BrixDB", lambda: db)
    monkeypatch.setattr("brix.mcp_handlers.helpers._audit_db.write_audit_entry", lambda **_: None)
    registry.register(
        name="schema_governance",
        description="Helper missing schemas but otherwise justified.",
        code="def run(data): return data",
    )
    registry.update(
        "schema_governance",
        project="buddy",
        tags=["utility"],
        reason_not_a_brick="Temporary adapter until a stable brick contract emerges.",
    )

    result = asyncio.run(
        _handle_update_helper(
                {
                    "name": "schema_governance",
                    "input_schema": {"type": "object"},
                    "output_schema": {"type": "object"},
                    "owner": "team-brix",
                }
            )
        )

    assert result["success"] is True
    assert db.get_helper("schema_governance")["governance_status"] == "governed"


def test_validator_warns_when_referenced_helper_governance_is_incomplete(tmp_path) -> None:
    entry = HelperRegistry(db=BrixDB(db_path=tmp_path / "validator.db")).register(
        name="draft_helper",
        description="Helper with schemas but no governance justification.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
    )
    with (
        patch("brix.helper_registry.HelperRegistry.get", return_value=entry),
        patch("brix.validator.PipelineValidator._check_brick_config_schema", lambda self, ctx, result: None),
    ):
        result = PipelineValidator().validate(
            Pipeline(name="p", steps=[Step(id="h", type="script.python", helper="draft_helper")]),
            level="standard",
        )

    assert any(finding.code == "HELPER_GOVERNANCE_INCOMPLETE" for finding in result.findings)


def test_list_helpers_with_filters_preserves_governance_fields(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "list_helpers.db")
    registry = HelperRegistry(db=db)
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.db.BrixDB", lambda: db)
    registry.register(
        name="governed_helper",
        description="Filtered helper listing should keep governance fields.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        project="buddy",
        tags=["utility"],
        group_name="hmk",
        reason_not_a_brick="Helper is still vendor-specific and not reusable enough.",
        governance_status="governed",
    )

    result = asyncio.run(_handle_list_helpers({"project": "buddy"}))

    assert result["count"] == 1
    helper = result["helpers"][0]
    assert helper["created_at"]
    assert helper["updated_at"]
    assert helper["governance_status"] == "governed"
    assert helper["reason_not_a_brick"]
    assert helper["brick_candidate_ref"] == ""


def test_helper_rollback_preserves_governance_metadata(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "rollback.db")
    registry = HelperRegistry(db=db)
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.db.BrixDB", lambda: db)
    monkeypatch.setattr("brix.mcp_handlers.helpers._audit_db.write_audit_entry", lambda **_: None)
    monkeypatch.setattr("brix.mcp_handlers._shared._managed_helper_dir", lambda: tmp_path)

    registry.register(
        name="rollback_helper",
        description="Governed helper that should survive rollback intact.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        project="buddy",
        tags=["utility"],
        group_name="hmk",
        reason_not_a_brick="Temporary helper until a shared brick family lands.",
        governance_status="governed",
    )
    db.record_object_version(
        obj_type="helper",
        name="rollback_helper",
        content={
            "code": db.get_helper_code("rollback_helper"),
            "meta": db.get_helper("rollback_helper"),
        },
    )
    version_id = db.get_object_versions("helper", "rollback_helper")[0]["version_id"]
    db.delete_helper("rollback_helper")

    result = asyncio.run(
        _handle_rollback(
            {
                "type": "helper",
                "name": "rollback_helper",
                "version_id": version_id,
            }
        )
    )

    assert result["success"] is True
    restored = db.get_helper("rollback_helper")
    assert restored["project"] == "buddy"
    assert restored["tags"] == ["utility"]
    assert restored["group_name"] == "hmk"
    assert restored["reason_not_a_brick"]
    assert restored["governance_status"] == "governed"


def test_helper_rollback_updates_existing_helper_governance(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "rollback_existing.db")
    registry = HelperRegistry(db=db)
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.db.BrixDB", lambda: db)
    monkeypatch.setattr("brix.mcp_handlers._shared._managed_helper_dir", lambda: tmp_path)

    registry.register(
        name="rollback_existing",
        description="Original governed helper state.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        project="buddy",
        tags=["utility"],
        group_name="hmk",
        reason_not_a_brick="Original governance rationale.",
        governance_status="governed",
    )
    db.record_object_version(
        obj_type="helper",
        name="rollback_existing",
        content={
            "code": db.get_helper_code("rollback_existing"),
            "meta": db.get_helper("rollback_existing"),
        },
    )
    version_id = db.get_object_versions("helper", "rollback_existing")[0]["version_id"]

    registry.update(
        "rollback_existing",
        project="cody",
        tags=["changed"],
        group_name="other",
        reason_not_a_brick="Changed governance rationale.",
        governance_status="draft",
    )

    result = asyncio.run(
        _handle_rollback(
            {
                "type": "helper",
                "name": "rollback_existing",
                "version_id": version_id,
            }
        )
    )

    assert result["success"] is True
    restored = db.get_helper("rollback_existing")
    assert restored["project"] == "buddy"
    assert restored["tags"] == ["utility"]
    assert restored["group_name"] == "hmk"
    assert restored["reason_not_a_brick"] == "Original governance rationale."
    assert restored["governance_status"] == "governed"
