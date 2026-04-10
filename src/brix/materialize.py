"""Canonical step materialization helpers."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from brix.db import _STEP_CONFIG_TOP_LEVEL_FIELDS, merge_step_config_into_params
from brix.models import Step
from brix.step_field_policy import explicit_runner_specific_fields

_RESERVED_RENDER_KEYS: tuple[str, ...] = (
    "_config",
    "_params",
    "_url",
    "_command",
    "_args",
    "_headers",
    "_body",
    "_pipeline",
    "_values",
)


@dataclass(frozen=True)
class MaterializedStep:
    """Canonical effective step view shared across validator and engine."""

    step: Step
    raw_step: dict[str, Any]
    raw_type: str
    effective_type: str
    raw_config: Any
    effective_config: dict[str, Any]
    raw_params: dict[str, Any] | list[Any] | None
    effective_params: dict[str, Any] | list[Any] | None
    effective_step_fields: dict[str, Any]
    promoted_fields: dict[str, dict[str, Any]]
    defaulted_fields: dict[str, dict[str, Any]]
    dependency_refs: dict[str, Any]
    policy_flags: dict[str, bool]
    provenance: dict[str, Any]
    rendering_warnings: tuple[str, ...] = ()
    wrapper_keys: tuple[str, ...] = _RESERVED_RENDER_KEYS


def _legacy_aliases() -> dict[str, str]:
    # Avoid module import cycle at import time.
    from brix.engine import LEGACY_ALIASES

    return LEGACY_ALIASES


def _effective_config(normalized_step: dict[str, Any]) -> dict[str, Any]:
    config = normalized_step.get("config")
    return dict(config) if isinstance(config, dict) else {}


def _effective_params(normalized_step: dict[str, Any]) -> dict[str, Any] | list[Any] | None:
    params = normalized_step.get("params")
    if isinstance(params, dict):
        return dict(params)
    if isinstance(params, list):
        return list(params)
    return params


def _promoted_fields(raw_step: dict[str, Any], normalized_step: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_config = raw_step.get("config")
    if not isinstance(raw_config, dict):
        return {}

    promoted: dict[str, dict[str, Any]] = {}
    for field in _STEP_CONFIG_TOP_LEVEL_FIELDS:
        if raw_config.get(field) is None:
            continue
        promoted[field] = {
            "source": f"config.{field}",
            "target": f"step.{field}",
            "raw_top_level": raw_step.get(field),
            "effective_value": normalized_step.get(field),
            "reason": "config-precedence",
        }
    return promoted


def _defaulted_fields(step: Step, raw_step: dict[str, Any]) -> dict[str, dict[str, Any]]:
    defaulted: dict[str, dict[str, Any]] = {}
    for field_name, field_info in Step.model_fields.items():
        if field_name in raw_step:
            continue
        value = getattr(step, field_name, None)
        if field_info.default_factory is not None:
            default = field_info.default_factory()
        else:
            default = field_info.default
        if value == default and value is not None:
            defaulted[field_name] = {
                "value": value,
                "source": "step-default",
            }
    return defaulted


def _dependency_refs(step: Step, normalized_step: dict[str, Any]) -> dict[str, Any]:
    return {
        "pipeline": normalized_step.get("pipeline", getattr(step, "pipeline", None)),
        "helper": normalized_step.get("helper", getattr(step, "helper", None)),
        "connection": normalized_step.get("connection", getattr(step, "connection", None)),
        "server": normalized_step.get("server", getattr(step, "server", None)),
        "tool": normalized_step.get("tool", getattr(step, "tool", None)),
        "when": getattr(step, "when", None),
        "foreach": getattr(step, "foreach", None),
        "depends_on": tuple(getattr(step, "depends_on", None) or ()),
    }


def _effective_step_fields(step: Step, normalized_step: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for field_name in _STEP_CONFIG_TOP_LEVEL_FIELDS:
        value = normalized_step.get(field_name, getattr(step, field_name, None))
        if value is not None:
            fields[field_name] = value
    return fields


def materialize_step(step: Step, *, raw_step: dict[str, Any] | None = None) -> MaterializedStep:
    """Return the canonical effective view for one step."""

    raw = deepcopy(raw_step if raw_step is not None else step.model_dump())
    normalized = merge_step_config_into_params(deepcopy(raw))
    raw_type = raw.get("type", step.type)
    aliases = _legacy_aliases()
    effective_type = aliases.get(raw_type, raw_type)

    effective_config = _effective_config(normalized)
    effective_params = _effective_params(normalized)

    promoted = _promoted_fields(raw, normalized)
    defaults = _defaulted_fields(step, raw)
    effective_step_fields = _effective_step_fields(step, normalized)
    dependency_refs = _dependency_refs(step, normalized)
    explicit_runner_fields = explicit_runner_specific_fields(step)
    policy_flags = {
        "uses_legacy_alias": raw_type != effective_type,
        "has_promoted_fields": bool(promoted),
        "has_defaulted_fields": bool(defaults),
        "has_runner_specific_top_level_fields": bool(explicit_runner_fields),
        "uses_conditional_refs": bool(step.when or step.foreach or getattr(step, "depends_on", None)),
    }
    provenance = {
        "persisted_from": "step-model" if raw_step is None else "external-raw-step",
        "normalizers": (
            "merge_step_config_into_params",
            "materialize_step",
        ),
        "runner_specific_top_level_fields": tuple(explicit_runner_fields),
    }

    return MaterializedStep(
        step=step,
        raw_step=raw,
        raw_type=raw_type,
        effective_type=effective_type,
        raw_config=raw.get("config"),
        effective_config=effective_config,
        raw_params=raw.get("params"),
        effective_params=effective_params,
        effective_step_fields=effective_step_fields,
        promoted_fields=promoted,
        defaulted_fields=defaults,
        dependency_refs=dependency_refs,
        policy_flags=policy_flags,
        provenance=provenance,
    )
