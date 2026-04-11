"""Policy for reducing runner-specific top-level Step fields.

The Step model still accepts historical runner-specific fields at top level for
compatibility. Brick-first semantics treat those fields as compatibility inputs;
their canonical home is the brick config schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from brix.models import Step

CONTROL_STEP_FIELDS = frozenset(
    {
        "id",
        "type",
        "enabled",
        "params",
        "config",
        "foreach",
        "parallel",
        "concurrency",
        "batch_size",
        "flat_output",
        "when",
        "else_of",
        "on_error",
        "retry_profile",
        "timeout",
        "progress",
        "requirements",
    }
)

CONTRACT_STEP_FIELDS = frozenset({"input_schema", "output_schema"})

RUNNER_FIELD_OWNERS: dict[str, str] = {
    # Python/script
    "script": "script.python",
    "helper": "script.python",
    # HTTP
    "url": "http.request",
    "method": "http.request",
    "headers": "http.request",
    "body": "http.request",
    "fetch_all_pages": "http.request",
    # CLI
    "command": "script.cli",
    "args": "script.cli",
    "shell": "script.cli",
    # MCP
    "server": "mcp.call",
    "tool": "mcp.call",
    # Database
    "connection": "db.*",
    "query": "db.query|db.exec",
    "table": "db.upsert",
    "conflict_key": "db.upsert",
    "set_columns": "db.upsert",
    # Sub-pipelines and groups
    "pipeline": "flow.pipeline",
    "pipeline_name": "flow.pipeline",
    "pipelines": "flow.pipeline_group",
    "shared_params": "flow.pipeline_group",
    # Flow/control runners with historical top-level payloads
    "values": "flow.set",
    "persist": "flow.set",
    "message": "flow.stop|action.notify",
    "success_on_stop": "flow.stop",
    "channel": "action.notify",
    "to": "action.notify",
    "approval_timeout": "action.approval",
    "on_timeout": "action.approval",
    "choices": "flow.choose",
    "default_steps": "flow.choose",
    "sub_steps": "flow.parallel",
    "until": "flow.repeat",
    "while_condition": "flow.repeat",
    "max_iterations": "flow.repeat",
    "sequence": "flow.repeat",
    "delay": "flow.repeat",
    "seconds": "flow.wait",
    "poll_interval": "flow.wait",
    "inputs": "flow.merge",
    "mode": "flow.merge",
    "key": "flow.merge",
    "field": "flow.switch",
    "cases": "flow.switch",
    "default": "flow.switch",
    "try_step": "flow.error_handler",
    "handler_step": "flow.error_handler",
    "rules": "validate",
}


@dataclass(frozen=True)
class StepFieldMigrationPolicy:
    """Migration policy for one historical runner-specific Step field."""

    field: str
    owner: str
    canonical_home: str
    status: str
    compatibility_rule: str

    def as_dict(self) -> dict[str, str]:
        return {
            "field": self.field,
            "owner": self.owner,
            "canonical_home": self.canonical_home,
            "status": self.status,
            "compatibility_rule": self.compatibility_rule,
        }


RUNNER_SPECIFIC_TOP_LEVEL_FIELDS: tuple[str, ...] = tuple(
    field
    for field in Step.model_fields
    if field not in CONTROL_STEP_FIELDS
    and field not in CONTRACT_STEP_FIELDS
    and field in RUNNER_FIELD_OWNERS
)

FIELD_MIGRATION_POLICIES: dict[str, StepFieldMigrationPolicy] = {
    field: StepFieldMigrationPolicy(
        field=field,
        owner=RUNNER_FIELD_OWNERS[field],
        canonical_home=f"config.{field}",
        status="compatibility",
        compatibility_rule=(
            f"Accept top-level '{field}' for historical inputs, but prefer "
            f"'config.{field}' for new brick-first definitions."
        ),
    )
    for field in RUNNER_SPECIFIC_TOP_LEVEL_FIELDS
}


def is_runner_specific_top_level_field(field: str) -> bool:
    """Return True if a Step field is runner-specific compatibility surface."""
    return field in FIELD_MIGRATION_POLICIES


def get_field_migration_policy(field: str) -> StepFieldMigrationPolicy | None:
    """Return the migration policy for a top-level Step field."""
    return FIELD_MIGRATION_POLICIES.get(field)


def list_field_migration_policies() -> list[StepFieldMigrationPolicy]:
    """Return all field migration policies in stable field order."""
    return [FIELD_MIGRATION_POLICIES[field] for field in sorted(FIELD_MIGRATION_POLICIES)]


def explicit_runner_specific_fields(step: Step) -> dict[str, Any]:
    """Return explicitly supplied runner-specific top-level fields for a step.

    Pydantic defaults are intentionally ignored. Only fields that were present
    in the submitted/persisted shape are migration debt signals.
    """
    explicit_fields = getattr(step, "model_fields_set", set())
    config = getattr(step, "config", None)
    config_dict = config if isinstance(config, dict) else {}
    return {
        field: getattr(step, field)
        for field in sorted(RUNNER_SPECIFIC_TOP_LEVEL_FIELDS)
        if field in explicit_fields
        and getattr(step, field) is not None
        and getattr(step, field) != Step.model_fields[field].default
        and config_dict.get(field) != getattr(step, field)
    }
