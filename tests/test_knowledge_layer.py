import json

import pytest

from brix.db import BrixDB


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "knowledge.db")


def test_knowledge_entity_roundtrip_and_lifecycle_rules(db):
    entry = db.knowledge_entity_add(
        "intent",
        "hmk-fix-intent",
        "Stabilize HMK extraction flow",
        raw_text="Please stop the workaround spiral and make HMK stable.",
        summary="Stabilize HMK extraction without workarounds.",
        rationale="Original user intent should stay queryable.",
        lifecycle_stage="draft",
        project="buddy",
        tags=["hmk", "stability"],
        content={"source": "user-request"},
    )

    assert entry["entity_type"] == "intent"
    assert entry["title"] == "Stabilize HMK extraction flow"
    assert entry["tags"] == ["hmk", "stability"]
    assert entry["content"] == {"source": "user-request"}

    updated = db.knowledge_entity_update(
        "hmk-fix-intent",
        lifecycle_stage="active",
        status="accepted",
        owner="platform",
    )
    assert updated is not None
    assert updated["lifecycle_stage"] == "active"
    assert updated["status"] == "accepted"
    assert updated["owner"] == "platform"

    with pytest.raises(ValueError, match="cannot move backwards"):
        db.knowledge_entity_update("hmk-fix-intent", lifecycle_stage="draft")

    listed = db.knowledge_entity_list(entity_type="intent", project="buddy")
    assert [item["name"] for item in listed] == ["hmk-fix-intent"]


def test_knowledge_links_join_product_entities_and_findings(db):
    intent = db.knowledge_entity_add(
        "intent",
        "hmk-download-intent",
        "Download and persist HMK documents",
        project="buddy",
    )
    decision = db.knowledge_entity_add(
        "decision",
        "hmk-brick-decision",
        "Prefer reusable source/download bricks",
        lifecycle_stage="active",
        project="buddy",
    )

    db.upsert_pipeline(
        name="buddy-hmk-extract",
        path="/tmp/buddy-hmk-extract.yaml",
        project="buddy",
        tags=["extraction"],
    )
    db.upsert_helper(
        name="att_onedrive_save",
        script_path="/tmp/att_onedrive_save.py",
        description="Legacy HMK helper",
        project="buddy",
        tags=["legacy"],
    )
    db.brick_definitions_upsert(
        {
            "name": "source.download_to_file",
            "runner": "python",
            "namespace": "source",
            "category": "source",
            "description": "Download a file to local staging.",
            "when_to_use": "Use for persistent source downloads.",
            "aliases": ["download_to_file"],
            "input_type": "*",
            "output_type": "document_payload",
            "config_schema": {},
            "examples": [],
            "system": False,
            "project": "utility",
            "group_name": "source",
        }
    )
    changelog = db.add_changelog_entry(
        version="13.4.0",
        type="refactor",
        title="Introduce HMK knowledge layer",
        task_id="T-3.1.2",
        commit_sha="deadbeef",
    )
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-hmk-001",
                "buddy-hmk-extract",
                0,
                "2026-04-10T00:00:00+00:00",
                "2026-04-10T00:01:00+00:00",
                60.0,
                "{}",
                json.dumps(
                    {
                        "save_results": {
                            "status": "error",
                            "error_message": "db.exec failed",
                        }
                    }
                ),
                "",
                "",
                "{}",
            ),
        )

    db.knowledge_link_add(
        "intent",
        intent["id"],
        "created_for",
        "pipeline",
        "buddy-hmk-extract",
        metadata={"confidence": "high"},
    )
    db.knowledge_link_add(
        "decision",
        decision["id"],
        "replaces",
        "helper",
        "att_onedrive_save",
    )
    db.knowledge_link_add(
        "decision",
        decision["id"],
        "candidate_for_reuse",
        "brick",
        "source.download_to_file",
    )
    db.knowledge_link_add(
        "decision",
        decision["id"],
        "documents",
        "changelog",
        changelog["id"],
    )
    db.knowledge_link_add(
        "decision",
        decision["id"],
        "fixed_by",
        "run",
        "run-hmk-001",
    )
    db.knowledge_link_add(
        "decision",
        decision["id"],
        "failed_at",
        "finding",
        "run-hmk-001:save_results",
    )

    pipeline_ctx = db.knowledge_context("pipeline", "buddy-hmk-extract")
    assert pipeline_ctx["entity"]["name"] == "buddy-hmk-extract"
    assert pipeline_ctx["related"][0]["entity"]["name"] == "hmk-download-intent"

    decision_ctx = db.knowledge_context("decision", decision["id"])
    related_types = {
        (item["relation_type"], item["entity_type"]) for item in decision_ctx["related"]
    }
    assert ("replaces", "helper") in related_types
    assert ("candidate_for_reuse", "brick") in related_types
    assert ("documents", "changelog") in related_types
    assert ("fixed_by", "run") in related_types
    assert ("failed_at", "finding") in related_types

    pipeline_links = db.knowledge_link_list(entity_type="pipeline")
    assert len(pipeline_links) == 1
    assert pipeline_links[0]["target_entity_type"] == "pipeline"

    helper_id = db.get_helper("att_onedrive_save")["id"]
    pipeline_id = db.get_pipeline("buddy-hmk-extract")["id"]
    with db._connect() as conn:
        conn.execute("UPDATE helper SET name=? WHERE id=?", ("att_onedrive_save_v2", helper_id))
        conn.execute("UPDATE pipeline SET name=? WHERE id=?", ("buddy-hmk-extract-v2", pipeline_id))

    renamed_helper_ctx = db.knowledge_context("helper", "att_onedrive_save_v2")
    renamed_pipeline_ctx = db.knowledge_context("pipeline", "buddy-hmk-extract-v2")
    assert any(item["entity_type"] == "decision" for item in renamed_helper_ctx["related"])
    assert any(item["entity_type"] == "intent" for item in renamed_pipeline_ctx["related"])


def test_knowledge_links_validate_references(db):
    intent = db.knowledge_entity_add(
        "intent",
        "hmk-missing-ref",
        "Test missing reference validation",
    )

    with pytest.raises(ValueError, match="Unknown target entity"):
        db.knowledge_link_add(
            "intent",
            intent["id"],
            "created_for",
            "pipeline",
            "does-not-exist",
        )


def test_knowledge_query_and_integrity_report(db):
    intent = db.knowledge_entity_add(
        "intent",
        "hmk-query-intent",
        "Investigate HMK extraction failures",
        summary="Find prior cases and linked components for HMK.",
        project="buddy",
        tags=["hmk", "diagnostics"],
    )
    decision = db.knowledge_entity_add(
        "decision",
        "hmk-query-decision",
        "Use linked knowledge during diagnosis",
        project="buddy",
    )
    db.upsert_pipeline(
        name="buddy-hmk-diagnose",
        path="/tmp/buddy-hmk-diagnose.yaml",
        project="buddy",
        tags=["diagnostics"],
    )
    db.knowledge_link_add(
        "intent",
        intent["id"],
        "created_for",
        "pipeline",
        "buddy-hmk-diagnose",
    )
    db.knowledge_link_add(
        "decision",
        decision["id"],
        "documents",
        "intent",
        intent["id"],
    )

    result = db.knowledge_query(query="HMK", project="buddy")
    assert {entry["name"] for entry in result["knowledge_entities"]} == {
        "hmk-query-decision",
        "hmk-query-intent",
    }
    assert result["related_knowledge"] == []
    assert {entry["name"] for entry in result["component_entities"]} == {"buddy-hmk-diagnose"}
    assert result["integrity_issues"] == []

    with db._connect() as conn:
        conn.execute("DELETE FROM pipeline WHERE name=?", ("buddy-hmk-diagnose",))
    issues = db.knowledge_link_integrity_report()
    assert len(issues) == 1
    assert issues[0]["relation_type"] == "created_for"
    assert issues[0]["target_entity_type"] == "pipeline"
    assert issues[0]["missing_target"] is True


def test_knowledge_query_scopes_integrity_and_supports_findings(db):
    intent = db.knowledge_entity_add(
        "intent",
        "hmk-findings-intent",
        "Trace HMK findings",
        project="buddy",
    )
    db.knowledge_entity_add(
        "intent",
        "foreign-intent",
        "Unrelated project intent",
        project="utility",
    )
    db.upsert_pipeline(
        name="buddy-hmk-findings",
        path="/tmp/buddy-hmk-findings.yaml",
        project="buddy",
        tags=["diagnostics"],
    )
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json, project
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-hmk-findings-001",
                "buddy-hmk-findings",
                0,
                "2026-04-10T00:00:00+00:00",
                "2026-04-10T00:01:00+00:00",
                60.0,
                "{}",
                json.dumps(
                    {
                        "extract": {
                            "status": "error",
                            "error_message": "HMK parse failure",
                        }
                    }
                ),
                "",
                "cli",
                "{}",
                "buddy",
            ),
        )
    valid_link = db.knowledge_link_add(
        "intent",
        intent["id"],
        "failed_at",
        "finding",
        "run-hmk-findings-001:extract",
    )
    broken_link = db.knowledge_link_add(
        "intent",
        "foreign-intent",
        "created_for",
        "pipeline",
        "buddy-hmk-findings",
    )
    with db._connect() as conn:
        conn.execute("DELETE FROM pipeline WHERE name=?", ("buddy-hmk-findings",))

    result = db.knowledge_query(entity_types=["finding"], query="parse", project="buddy")
    assert {entry["id"] for entry in result["component_entities"]} == {"run-hmk-findings-001:extract"}
    assert result["integrity_issues"] == []
    assert any(link["id"] == valid_link["id"] for link in result["links"])
    assert all(issue["link_id"] != broken_link["id"] for issue in result["integrity_issues"])

    default_result = db.knowledge_query(query="parse", project="buddy")
    assert {entry["id"] for entry in default_result["component_entities"]} == {"run-hmk-findings-001:extract"}
