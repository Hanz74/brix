import json

import pytest
from pydantic import ValidationError

from brix.models import Pipeline, Step


def test_step_db_roundtrip_http_request():
    step = Step(
        id="fetch",
        type="http.request",
        url="https://api.example.com/items",
        method="POST",
        headers={"Authorization": "Bearer token"},
        body={"limit": 5, "active": True},
        params={"timeout": 30},
        foreach="{{ items }}",
        when="{{ should_run }}",
        timeout="30s",
    )

    row = step.to_db_dict()
    roundtrip = Step.from_db_row(row)

    assert row["step_key"] == "fetch"
    assert row["step_type"] == "http.request"
    assert json.loads(row["headers_json"]) == {"Authorization": "Bearer token"}
    assert json.loads(row["body_json"]) == {"limit": 5, "active": True}
    assert roundtrip.model_dump() == step.model_dump()


def test_step_db_roundtrip_script_python_with_helper():
    step = Step(
        id="transform",
        type="script.python",
        helper="normalize_payload",
        script="helpers/normalize_payload.py",
        params={"mode": "strict"},
        requirements=["httpx>=0.28"],
        progress=True,
        persist_output=True,
    )

    row = step.to_db_dict()
    roundtrip = Step.from_db_row(row)

    assert row["helper"] == "normalize_payload"
    assert row["script"] == "helpers/normalize_payload.py"
    assert json.loads(row["requirements_json"]) == ["httpx>=0.28"]
    assert roundtrip.model_dump() == step.model_dump()


def test_step_db_roundtrip_flow_choose_with_choices():
    step = Step(
        id="route",
        type="flow.choose",
        choices=[
            {
                "when": "{{ input.kind == 'email' }}",
                "steps": [{"id": "email_branch", "type": "flow.set", "values": {"channel": "email"}}],
            },
            {
                "when": "{{ input.kind == 'sms' }}",
                "steps": [{"id": "sms_branch", "type": "flow.set", "values": {"channel": "sms"}}],
            },
        ],
        default_steps=[
            {"id": "fallback_branch", "type": "flow.set", "values": {"channel": "other"}}
        ],
    )

    row = step.to_db_dict()
    roundtrip = Step.from_db_row(row)

    assert roundtrip.model_dump() == step.model_dump()


def test_pipeline_from_db_with_steps_credentials_and_input():
    step_rows = [
        Step(id="fetch", type="http.request", url="https://api.example.com").to_db_dict(),
        Step(id="decide", type="flow.choose", choices=[{"when": "{{ ok }}", "steps": []}]).to_db_dict(),
        Step(id="notify", type="action.notify", channel="email", to="ops@example.com").to_db_dict(),
    ]
    pipeline_row = {
        "name": "db-only-pipeline",
        "version": "2.0.0",
        "description": "Reconstructed from normalized rows",
        "brix_version": "9.9.9",
        "kind": "template",
        "extends": "base-template",
        "idempotency_key": "{{ input.request_id }}",
        "is_template": 1,
        "compositor_mode": 1,
        "allow_code": 0,
        "strict_bricks": 1,
        "test_mode": 1,
        "template_params_json": json.dumps({"base_url": "https://example.com"}),
        "blueprint_params_json": json.dumps(
            [{"name": "env", "type": "string", "description": "Deployment environment"}]
        ),
        "error_handling_json": json.dumps({"on_error": "continue"}),
        "retry_profiles_json": json.dumps({"default": {"max": 5, "backoff": "linear"}}),
        "notify_json": json.dumps(
            {"mattermost": {"enabled": True, "webhook_url": "https://hook.example.com"}}
        ),
        "groups_json": json.dumps({"core": [{"id": "seed", "type": "flow.set"}]}),
        "output_json": json.dumps({"result": "{{ fetch.output }}"}),
        "output_slots_json": json.dumps({"status": "{{ notify.output.status }}"}),
        "requirements_json": json.dumps(["httpx>=0.28"]),
    }
    credential_rows = [
        {"alias": "api_key", "env_ref": "API_KEY", "refresh_json": None},
        {
            "alias": "oauth",
            "env_ref": "OAUTH_TOKEN",
            "refresh_json": json.dumps({"type": "oauth2_client_credentials"}),
        },
    ]
    input_rows = [
        {
            "input_key": "request_id",
            "type": "string",
            "default_json": json.dumps("req-1"),
            "description": "Request identifier",
        }
    ]

    pipeline = Pipeline.from_db(
        pipeline_row,
        step_rows,
        credential_rows=credential_rows,
        input_rows=input_rows,
    )

    assert pipeline.name == "db-only-pipeline"
    assert pipeline.version == "2.0.0"
    assert pipeline.is_template is True
    assert pipeline.compositor_mode is True
    assert pipeline.allow_code is False
    assert pipeline.strict_bricks is True
    assert pipeline.test_mode is True
    assert pipeline.template_params == {"base_url": "https://example.com"}
    assert pipeline.blueprint_params[0].name == "env"
    assert pipeline.error_handling.on_error == "continue"
    assert pipeline.retry_profiles["default"].max == 5
    assert pipeline.notify.mattermost.enabled is True
    assert pipeline.groups == {"core": [{"id": "seed", "type": "flow.set"}]}
    assert pipeline.output == {"result": "{{ fetch.output }}"}
    assert pipeline.output_slots == {"status": "{{ notify.output.status }}"}
    assert pipeline.requirements == ["httpx>=0.28"]
    assert {
        key: value.model_dump(exclude_none=True)
        for key, value in pipeline.credentials.items()
    } == {
        "api_key": {"env": "API_KEY"},
        "oauth": {
            "env": "OAUTH_TOKEN",
            "refresh": {"type": "oauth2_client_credentials"},
        },
    }
    assert {key: value.model_dump() for key, value in pipeline.input.items()} == {
        "request_id": {
            "type": "string",
            "default": "req-1",
            "description": "Request identifier",
        }
    }
    assert [step.id for step in pipeline.steps] == ["fetch", "decide", "notify"]


def test_step_db_roundtrip_with_all_optional_fields_empty():
    step = Step(id="minimal", type="flow.set")

    row = step.to_db_dict()
    roundtrip = Step.from_db_row(row)

    assert roundtrip.model_dump() == step.model_dump()


def test_step_from_invalid_db_row_raises_validation_error():
    invalid_row = {
        "step_key": "broken",
        "enabled": 1,
        "concurrency": 0,
    }

    with pytest.raises(ValidationError):
        Step.from_db_row(invalid_row)
