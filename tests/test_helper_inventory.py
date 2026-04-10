"""Tests for DB-first helper inventory and clustering."""

from __future__ import annotations

import asyncio

from brix.db import BrixDB
from brix.helper_inventory import build_helper_inventory, classify_helper_family, filter_helper_inventory
from brix.helper_registry import HelperRegistry
from brix.mcp_handlers.helpers import _handle_list_helpers


def test_helper_inventory_assigns_every_helper_a_strategic_category(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "helpers.db")
    db.upsert_helper(
        name="extract_invoice_fields",
        script_path="",
        description="Extract invoice fields from document text.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data):\n    return {'invoice': data}\n",
        project="buddy",
        tags=["extract"],
    )
    db.upsert_helper(
        name="legacy_download",
        script_path="/app/helpers/legacy_download.py",
        description="Download a file through legacy script path.",
    )

    pipeline_a = db.upsert_pipeline(name="a", path="/tmp/a.yaml")
    db.upsert_step(
        pipeline_id=pipeline_a,
        step_dict={"id": "extract", "type": "script.python", "helper": "extract_invoice_fields"},
        step_order=0,
    )
    pipeline_b = db.upsert_pipeline(name="b", path="/tmp/b.yaml")
    db.upsert_step(
        pipeline_id=pipeline_b,
        step_dict={"id": "extract", "type": "script.python", "helper": "extract_invoice_fields"},
        step_order=0,
    )

    inventory = build_helper_inventory(db)
    by_name = {item.name: item for item in inventory.items}

    assert set(by_name) == {"extract_invoice_fields", "legacy_download"}
    assert all(item.strategic_category for item in inventory.items)
    assert by_name["extract_invoice_fields"].family == "extraction"
    assert by_name["extract_invoice_fields"].strategic_category == "brick_candidate"
    assert by_name["extract_invoice_fields"].migration_candidacy == "high"
    assert by_name["legacy_download"].strategic_category == "legacy_review"


def test_helper_inventory_builds_family_clusters(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "clusters.db")
    db.upsert_helper(
        name="classify_mail",
        script_path="",
        description="Classify incoming mail by category.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        tags=["classification"],
    )
    db.upsert_helper(
        name="save_rows",
        script_path="",
        description="Persist rows into the database.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        tags=["db"],
    )

    inventory = build_helper_inventory(db).as_dict()
    clusters = {cluster["family"]: cluster for cluster in inventory["clusters"]}

    assert "classification" in clusters
    assert "persistence" in clusters
    assert clusters["classification"]["helpers"] == ["classify_mail"]
    assert inventory["summary"]["total"] == 2
    assert inventory["summary"]["brick_candidate"] == 2


def test_filter_helper_inventory_scopes_summary_and_clusters(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "filtered.db")
    db.upsert_helper(
        name="classify_mail",
        script_path="",
        description="Classify mail.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        tags=["classification"],
        project="buddy",
    )
    db.upsert_helper(
        name="notify_team",
        script_path="",
        description="Notify team.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        tags=["notification"],
        project="cody",
    )

    scoped = filter_helper_inventory(build_helper_inventory(db), {"classify_mail"}).as_dict()

    assert scoped["summary"] == {"total": 1, "brick_candidate": 1}
    assert [item["name"] for item in scoped["helpers"]] == ["classify_mail"]
    assert [cluster["family"] for cluster in scoped["clusters"]] == ["classification"]


def test_helper_registry_legacy_adapter_preserves_org_metadata(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "registry.db")
    registry = HelperRegistry(db=db)
    registry.register(
        name="org_helper",
        script="",
        description="Helper with org metadata.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
    )
    registry.update("org_helper", project="buddy", tags=["extract"], group_name="hmk")

    data = registry._load()
    raw = data["org_helper"]
    raw["name"] = "renamed_org_helper"
    data["renamed_org_helper"] = raw
    del data["org_helper"]
    registry._save(data)

    renamed = db.get_helper("renamed_org_helper")
    assert renamed is not None
    assert renamed["project"] == "buddy"
    assert renamed["tags"] == ["extract"]
    assert renamed["group_name"] == "hmk"


def test_classify_helper_family_uses_stored_code_as_signal() -> None:
    family, domain, signals = classify_helper_family(
        {
            "name": "generic_helper",
            "description": "",
            "tags": [],
            "script_path": "",
            "code": "def run(payload):\n    return extract(payload)\n",
        }
    )

    assert family == "extraction"
    assert domain == "extract"
    assert "keyword:extract" in signals


def test_list_helpers_can_include_inventory(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "mcp_helpers.db")
    db.upsert_helper(
        name="normalize_pdf",
        script_path="",
        description="Convert and normalize PDF files.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        tags=["conversion"],
    )

    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.helper_inventory.BrixDB", lambda: db)
    result = asyncio.run(_handle_list_helpers({"include_inventory": True}))

    assert result["success"] is True
    assert result["inventory_summary"]["total"] == 1
    assert result["helpers"][0]["inventory"]["family"] == "conversion"
    assert result["helpers"][0]["inventory"]["strategic_category"] == "brick_candidate"
