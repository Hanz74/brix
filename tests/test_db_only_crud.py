from __future__ import annotations

import json

import pytest

from brix.db import BrixDB, step_dict_to_row, step_row_to_dict


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "db_only_crud.db")


def _make_step(step_id: str, **overrides):
    step = {
        "id": step_id,
        "type": "http.request",
        "enabled": True,
        "method": "POST",
        "headers": {"Authorization": "Bearer token"},
        "body": {"hello": "world"},
        "shell": False,
        "pipeline": "child-pipeline",
        "shared_params": {"region": "eu"},
        "persist": True,
        "success_on_stop": False,
        "to": "alerts",
        "until": "{{ done }}",
        "while_condition": "{{ keep_going }}",
        "params": {"limit": 5},
        "foreach": "{{ items }}",
        "parallel": True,
        "flat_output": False,
        "when": "{{ should_run }}",
        "timeout": "30s",
        "fetch_all_pages": True,
        "progress": True,
        "requirements": ["httpx>=0.28"],
        "input_schema": {"url": "str"},
        "output_schema": {"status": "int"},
        "rules": [{"field": "status", "rule": "required"}],
        "config": {"mode": "strict"},
        "depends_on": ["prepare"],
        "cache": {"ttl": "1h"},
        "circuit_breaker": {"max_failures": 3},
        "rate_limit": {"max_calls": 10, "per": "1m"},
        "compensate": {"type": "flow.set", "values": {"rolled_back": True}},
        "persist_output": True,
        "pause_before": False,
        "persist_data": True,
        "data": {"payload": 1},
        "stream": False,
        "unwrap_json": True,
    }
    step.update(overrides)
    return step


def test_upsert_step_and_get_step_by_id_roundtrip(db):
    pipeline_id = db.upsert_pipeline("roundtrip-pipeline", "/tmp/roundtrip.yaml")
    step = _make_step("fetch")

    row_id = db.upsert_step(pipeline_id, step, step_order=0)
    loaded = db.get_step_by_id(pipeline_id, "fetch")

    assert isinstance(row_id, str)
    assert loaded is not None
    assert loaded["id"] == "fetch"
    assert loaded["type"] == "http.request"
    assert loaded["headers"] == {"Authorization": "Bearer token"}
    assert loaded["body"] == {"hello": "world"}
    assert loaded["pipeline"] == "child-pipeline"
    assert loaded["to"] == "alerts"
    assert loaded["when"] == "{{ should_run }}"
    assert loaded["until"] == "{{ done }}"
    assert loaded["foreach"] == "{{ items }}"
    assert loaded["while_condition"] == "{{ keep_going }}"
    assert loaded["params"] == {"limit": 5}
    assert loaded["depends_on"] == ["prepare"]
    assert loaded["unwrap_json"] is True


def test_update_step_row_single_field_timeout_null_persists(db):
    pipeline_id = db.upsert_pipeline("timeout-pipeline", "/tmp/timeout.yaml")
    db.upsert_step(pipeline_id, _make_step("fetch", timeout="45s"), step_order=0)

    updated = db.update_step_row(pipeline_id, "fetch", {"timeout": None})
    loaded = db.get_step_by_id(pipeline_id, "fetch")

    assert updated is True
    assert loaded is not None
    assert loaded["timeout"] is None


def test_update_step_row_does_not_overwrite_other_fields(db):
    pipeline_id = db.upsert_pipeline("partial-update-pipeline", "/tmp/partial.yaml")
    original = _make_step("fetch", timeout="45s")
    db.upsert_step(pipeline_id, original, step_order=0)

    updated = db.update_step_row(pipeline_id, "fetch", {"timeout": None})
    loaded = db.get_step_by_id(pipeline_id, "fetch")

    assert updated is True
    assert loaded is not None
    assert loaded["timeout"] is None
    assert loaded["method"] == "POST"
    assert loaded["body"] == {"hello": "world"}
    assert loaded["headers"] == {"Authorization": "Bearer token"}
    assert loaded["depends_on"] == ["prepare"]


def test_delete_step_row_removes_step(db):
    pipeline_id = db.upsert_pipeline("delete-step-pipeline", "/tmp/delete-step.yaml")
    db.upsert_step(pipeline_id, _make_step("fetch"), step_order=0)

    deleted = db.delete_step_row(pipeline_id, "fetch")

    assert deleted is True
    assert db.get_step_by_id(pipeline_id, "fetch") is None


def test_reorder_steps_changes_step_order(db):
    pipeline_id = db.upsert_pipeline("reorder-pipeline", "/tmp/reorder.yaml")
    db.upsert_step(pipeline_id, _make_step("first", method="GET"), step_order=0)
    db.upsert_step(pipeline_id, _make_step("second", method="PUT"), step_order=1)
    db.upsert_step(pipeline_id, _make_step("third", method="PATCH"), step_order=2)

    db.reorder_steps(pipeline_id, ["third", "first", "second"])
    steps = db.get_steps(pipeline_id)

    assert [step["id"] for step in steps] == ["third", "first", "second"]


def test_step_dict_to_row_and_step_row_to_dict_roundtrip():
    step = _make_step("roundtrip", timeout=None)

    row = step_dict_to_row(step)
    roundtrip = step_row_to_dict(row)

    assert row["notify_to"] == "alerts"
    assert row["sub_pipeline"] == "child-pipeline"
    assert row["when_expr"] == "{{ should_run }}"
    assert row["until_expr"] == "{{ done }}"
    assert row["foreach_expr"] == "{{ items }}"
    assert json.loads(row["params_json"]) == {"limit": 5}
    assert row["enabled"] == 1
    assert roundtrip == step


def test_pipeline_to_dict_reconstructs_complete_pipeline(db):
    pipeline_id = db.upsert_pipeline("reconstruct-pipeline", "/tmp/reconstruct.yaml")
    with db._connect() as conn:
        conn.execute(
            """
            UPDATE pipeline
            SET version=?,
                description=?,
                brix_version=?,
                kind=?,
                extends=?,
                is_template=?,
                compositor_mode=?,
                allow_code=?,
                strict_bricks=?,
                test_mode=?,
                idempotency_key=?,
                template_params_json=?,
                blueprint_params_json=?,
                error_handling_json=?,
                retry_profiles_json=?,
                notify_json=?,
                groups_json=?,
                output_json=?,
                output_slots_json=?,
                requirements_json=?,
                project=?,
                tags=?,
                group_name=?
            WHERE id=?
            """,
            (
                "2.0.0",
                "Reconstructed from row tables",
                "9.9.9",
                "template",
                "base-template",
                1,
                1,
                0,
                1,
                1,
                "{{ input.request_id }}",
                json.dumps({"base_url": "https://example.com"}),
                json.dumps([{"name": "env", "type": "string"}]),
                json.dumps({"on_error": "continue"}),
                json.dumps({"default": {"max": 5, "backoff": "linear"}}),
                json.dumps({"mattermost": {"enabled": True, "webhook_url": "https://hook"}}),
                json.dumps({"core": [{"id": "seed", "type": "flow.set"}]}),
                json.dumps({"result": "{{ fetch.output }}"}),
                json.dumps({"status": "{{ fetch.output.status }}"}),
                json.dumps(["httpx>=0.28"]),
                "system",
                json.dumps(["db-only", "crud"]),
                "ops",
                pipeline_id,
            ),
        )

    db.upsert_pipeline_input(
        pipeline_id,
        "request_id",
        "string",
        default_value="req-1",
        description="Request identifier",
    )
    db.upsert_pipeline_credential(
        pipeline_id,
        "api_key",
        "API_KEY",
        refresh={"type": "oauth2_client_credentials"},
    )
    db.upsert_step(pipeline_id, _make_step("fetch"), step_order=0)

    pipeline_dict = db.pipeline_to_dict(pipeline_id)

    assert pipeline_dict is not None
    assert pipeline_dict["name"] == "reconstruct-pipeline"
    assert pipeline_dict["version"] == "2.0.0"
    assert pipeline_dict["description"] == "Reconstructed from row tables"
    assert pipeline_dict["is_template"] is True
    assert pipeline_dict["allow_code"] is False
    assert pipeline_dict["template_params"] == {"base_url": "https://example.com"}
    assert pipeline_dict["blueprint_params"] == [{"name": "env", "type": "string"}]
    assert pipeline_dict["error_handling"] == {"on_error": "continue"}
    assert pipeline_dict["notify"] == {
        "mattermost": {"enabled": True, "webhook_url": "https://hook"}
    }
    assert pipeline_dict["input"]["request_id"] == {
        "type": "string",
        "default": "req-1",
        "description": "Request identifier",
    }
    assert pipeline_dict["credentials"]["api_key"] == {
        "env": "API_KEY",
        "refresh": {"type": "oauth2_client_credentials"},
    }
    assert [step["id"] for step in pipeline_dict["steps"]] == ["fetch"]
    assert pipeline_dict["group"] == "ops"
    assert pipeline_dict["tags"] == ["db-only", "crud"]


def test_pipeline_credential_crud_roundtrip(db):
    pipeline_id = db.upsert_pipeline("credential-pipeline", "/tmp/credential.yaml")

    db.upsert_pipeline_credential(
        pipeline_id,
        "api_key",
        "API_KEY",
        refresh={"type": "oauth2_client_credentials"},
    )
    db.upsert_pipeline_credential(pipeline_id, "signing_key", "SIGNING_KEY")

    credentials = db.get_pipeline_credentials(pipeline_id)

    assert credentials == [
        {
            "pipeline_id": pipeline_id,
            "name": "api_key",
            "env": "API_KEY",
            "refresh": {"type": "oauth2_client_credentials"},
        },
        {
            "pipeline_id": pipeline_id,
            "name": "signing_key",
            "env": "SIGNING_KEY",
            "refresh": None,
        },
    ]
    assert db.delete_pipeline_credentials(pipeline_id) == 2
    assert db.get_pipeline_credentials(pipeline_id) == []


def test_pipeline_input_crud_roundtrip(db):
    pipeline_id = db.upsert_pipeline("input-pipeline", "/tmp/input.yaml")

    db.upsert_pipeline_input(
        pipeline_id,
        "limit",
        "integer",
        default_value=25,
        description="Batch size",
    )
    db.upsert_pipeline_input(
        pipeline_id,
        "dry_run",
        "boolean",
        default_value=True,
    )

    inputs = db.get_pipeline_inputs(pipeline_id)

    assert inputs == [
        {
            "pipeline_id": pipeline_id,
            "name": "dry_run",
            "type": "boolean",
            "default": True,
            "description": None,
        },
        {
            "pipeline_id": pipeline_id,
            "name": "limit",
            "type": "integer",
            "default": 25,
            "description": "Batch size",
        },
    ]
    assert db.delete_pipeline_inputs(pipeline_id) == 2
    assert db.get_pipeline_inputs(pipeline_id) == []
