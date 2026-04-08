"""Pipeline validation without execution."""
import difflib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from pydantic_core import PydanticUndefined

import jinja2
from jinja2 import nodes
from jinja2.visitor import NodeVisitor
import jsonschema
from jsonschema import ValidationError
import yaml

from brix.models import Pipeline, Step
from brix.cache import SchemaCache
from brix.bricks.registry import BrickRegistry
from brix.engine import LEGACY_ALIASES
from brix.pipeline_store import PipelineStore
from brix.connections import ConnectionManager


_STEP_CONFIG_CONFLICT_FIELDS = tuple(
    field_name
    for field_name in Step.model_fields.keys()
    if field_name not in {"id", "type", "config", "params"}
)

_STEP_CONFIG_CONFLICT_DEFAULTS = {
    field_name: (
        field_info.default_factory()
        if field_info.default_factory is not None
        else field_info.default
    )
    for field_name, field_info in Step.model_fields.items()
    if field_name in _STEP_CONFIG_CONFLICT_FIELDS
    and (
        field_info.default_factory is not None
        or field_info.default is not PydanticUndefined
    )
}


# ---------------------------------------------------------------------------
# Default linting rules (T-BRIX-V6-16)
# ---------------------------------------------------------------------------

_DEFAULT_LINT_RULES = [
    {
        "id": "max-mcp-concurrency",
        "description": "MCP steps should not exceed concurrency 5",
        "type": "mcp",
        "check": "max_concurrency",
        "max": 5,
        "severity": "warning",
    },
    {
        "id": "no-base64-foreach",
        "description": "base64 in foreach params leads to OOM on large batches",
        "check": "no_base64_foreach",
        "severity": "warning",
    },
    {
        "id": "progress-on-long-timeout",
        "description": "Steps with timeout > 60s should enable progress:true",
        "check": "progress_on_long_timeout",
        "timeout_threshold_seconds": 60,
        "severity": "warning",
    },
]


@dataclass(frozen=True)
class StepAnalysis:
    """Normalized, shape-safe read view over one step for validation."""

    step: Step
    index: int
    effective_type: str
    params: dict[str, Any] | list[Any] | None
    config: dict[str, Any]

    @property
    def params_dict(self) -> dict[str, Any]:
        return self.params if isinstance(self.params, dict) else {}

    @property
    def params_list(self) -> list[Any]:
        return self.params if isinstance(self.params, list) else []

    @property
    def has_params(self) -> bool:
        return bool(self.params_dict) or bool(self.params_list)

    def params_values(self) -> list[Any]:
        if self.params_dict:
            return list(self.params_dict.values())
        return list(self.params_list)

    def params_items(self) -> list[tuple[Any, Any]]:
        if self.params_dict:
            return list(self.params_dict.items())
        return []

    def params_keys(self) -> set[Any]:
        if self.params_dict:
            return set(self.params_dict.keys())
        return set()

    def params_get(self, key: str, default: Any = None) -> Any:
        return self.params_dict.get(key, default)


@dataclass(frozen=True)
class ValidationContext:
    """Shared per-validate() analysis context."""

    pipeline: Pipeline
    analyses: tuple[StepAnalysis, ...]
    by_step_id: dict[str, StepAnalysis]

    @classmethod
    def from_pipeline(cls, pipeline: Pipeline) -> "ValidationContext":
        analyses = tuple(
            StepAnalysis(
                step=step,
                index=index,
                effective_type=LEGACY_ALIASES.get(step.type, step.type),
                params=getattr(step, "params", None),
                config=getattr(step, "config", None)
                if isinstance(getattr(step, "config", None), dict)
                else {},
            )
            for index, step in enumerate(pipeline.steps)
        )
        return cls(
            pipeline=pipeline,
            analyses=analyses,
            by_step_id={analysis.step.id: analysis for analysis in analyses},
        )

    def for_step(self, step: Step) -> StepAnalysis:
        return self.by_step_id.get(step.id) or StepAnalysis(
            step=step,
            index=-1,
            effective_type=LEGACY_ALIASES.get(step.type, step.type),
            params=getattr(step, "params", None),
            config=getattr(step, "config", None)
            if isinstance(getattr(step, "config", None), dict)
            else {},
        )


class ValidationResult:
    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.infos: list[str] = []  # info-level hints (non-actionable)
        self.checks: list[str] = []  # successful checks

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def add_check(self, msg: str):
        self.checks.append(msg)

    def add_info(self, msg: str, hint: str | None = None, schema_ref: str | None = None):
        self.infos.append(self._format_finding(msg, hint=hint, schema_ref=schema_ref))

    @staticmethod
    def _format_finding(msg: str, hint: str | None = None, schema_ref: str | None = None) -> str:
        parts = [msg]
        if hint:
            parts.append(f"Hint: {hint}")
        if schema_ref:
            parts.append(f"Schema: {schema_ref}")
        return " | ".join(parts)

    def add_error(self, msg: str, hint: str | None = None, schema_ref: str | None = None):
        self.errors.append(self._format_finding(msg, hint=hint, schema_ref=schema_ref))

    def add_warning(self, msg: str, hint: str | None = None, schema_ref: str | None = None):
        self.warnings.append(self._format_finding(msg, hint=hint, schema_ref=schema_ref))


class PipelineValidator:
    _CONFIG_CONFLICT_FIELDS = _STEP_CONFIG_CONFLICT_FIELDS
    _CONFIG_CONFLICT_DEFAULTS = _STEP_CONFIG_CONFLICT_DEFAULTS

    def __init__(self, cache: SchemaCache = None, lint_rules: list = None):
        self.cache = cache or SchemaCache()
        # lint_rules: explicit list (for testing), otherwise load from disk + defaults
        self._lint_rules: list[dict] | None = lint_rules
        self._runner_config_schemas: dict[str, dict] | None = None
        self._validation_ctx: ValidationContext | None = None

    @staticmethod
    def _params_values(step_or_analysis: Step | StepAnalysis) -> list[Any]:
        if isinstance(step_or_analysis, StepAnalysis):
            return step_or_analysis.params_values()
        return StepAnalysis(
            step=step_or_analysis,
            index=-1,
            effective_type=LEGACY_ALIASES.get(step_or_analysis.type, step_or_analysis.type),
            params=getattr(step_or_analysis, "params", None),
            config=getattr(step_or_analysis, "config", None)
            if isinstance(getattr(step_or_analysis, "config", None), dict)
            else {},
        ).params_values()

    @staticmethod
    def _params_items(step_or_analysis: Step | StepAnalysis) -> list[tuple[Any, Any]]:
        if isinstance(step_or_analysis, StepAnalysis):
            return step_or_analysis.params_items()
        return StepAnalysis(
            step=step_or_analysis,
            index=-1,
            effective_type=LEGACY_ALIASES.get(step_or_analysis.type, step_or_analysis.type),
            params=getattr(step_or_analysis, "params", None),
            config=getattr(step_or_analysis, "config", None)
            if isinstance(getattr(step_or_analysis, "config", None), dict)
            else {},
        ).params_items()

    @staticmethod
    def _params_keys(step_or_analysis: Step | StepAnalysis) -> set[Any]:
        if isinstance(step_or_analysis, StepAnalysis):
            return step_or_analysis.params_keys()
        return StepAnalysis(
            step=step_or_analysis,
            index=-1,
            effective_type=LEGACY_ALIASES.get(step_or_analysis.type, step_or_analysis.type),
            params=getattr(step_or_analysis, "params", None),
            config=getattr(step_or_analysis, "config", None)
            if isinstance(getattr(step_or_analysis, "config", None), dict)
            else {},
        ).params_keys()

    @staticmethod
    def _params_get(params_or_analysis: Any, key: str, default: Any = None) -> Any:
        if isinstance(params_or_analysis, StepAnalysis):
            return params_or_analysis.params_get(key, default)
        if isinstance(params_or_analysis, dict):
            return params_or_analysis.get(key, default)
        return default

    def _load_lint_rules(self) -> list[dict]:
        """Load lint rules from ~/.brix/lint_rules.yaml merged with defaults (T-BRIX-V6-16)."""
        if self._lint_rules is not None:
            return self._lint_rules
        rules = list(_DEFAULT_LINT_RULES)
        rules_path = Path.home() / ".brix" / "lint_rules.yaml"
        if rules_path.exists():
            try:
                data = yaml.safe_load(rules_path.read_text()) or {}
                extra = data.get("rules", [])
                if isinstance(extra, list):
                    rules.extend(extra)
            except Exception:
                pass  # Malformed file — use defaults only
        return rules

    def validate(
        self,
        pipeline: Pipeline,
        pipeline_dir: Path = None,
        level: str = "standard",
    ) -> ValidationResult:
        if level not in {"quick", "standard", "deep"}:
            raise ValueError("level must be one of: quick, standard, deep")

        result = ValidationResult()
        self._validation_ctx = ValidationContext.from_pipeline(pipeline)

        # 1. Step IDs unique
        step_ids = [analysis.step.id for analysis in self._validation_ctx.analyses]
        if len(step_ids) != len(set(step_ids)):
            result.add_error("Duplicate step IDs found")
        else:
            result.add_check("Step IDs are unique")

        # 2. Step references valid (no dangling {{ step.output }})
        for analysis in self._validation_ctx.analyses:
            self._check_step_references(analysis, step_ids, pipeline, result)

        # 3. MCP steps have server + tool
        for analysis in self._validation_ctx.analyses:
            step = analysis.step
            if analysis.effective_type == "mcp.call":
                if not step.server:
                    result.add_error(f"Step '{step.id}': MCP step needs 'server'")
                if not step.tool:
                    result.add_error(f"Step '{step.id}': MCP step needs 'tool'")
                # Check if server is registered
                if step.server:
                    try:
                        from brix.server_manager import ServerManager

                        if ServerManager().get(step.server) is None:
                            result.add_warning(
                                f"Step '{step.id}': Server '{step.server}' not registered"
                            )
                    except Exception:
                        result.add_warning(
                            f"Could not verify server '{step.server}' in DB"
                        )
                # Check tool against cache
                if step.server and step.tool:
                    cached_tools = self.cache.get_tool_names(step.server)
                    if cached_tools and step.tool not in cached_tools:
                        result.add_warning(
                            f"Step '{step.id}': Tool '{step.tool}' not in cached schema for '{step.server}'"
                        )

        # 4. Python scripts exist
        if pipeline_dir:
            for step in pipeline.steps:
                if step.type == "python" and step.script:
                    script_path = pipeline_dir / step.script
                    if not script_path.exists():
                        # Try absolute
                        if not Path(step.script).exists():
                            result.add_error(
                                f"Step '{step.id}': Script not found: {step.script}"
                            )
                        else:
                            result.add_check(f"Step '{step.id}': Script exists")
                    else:
                        result.add_check(f"Step '{step.id}': Script exists")

        # 5. Credentials
        for key, cred in pipeline.credentials.items():
            self._check_credential(key, cred.env, result)

        # 6. when + default check
        for analysis in self._validation_ctx.analyses:
            if analysis.step.when:
                self._check_when_default(analysis, pipeline.steps, result)

        # 8. MCP step params vs cached tool schema (required params)
        for analysis in self._validation_ctx.analyses:
            if analysis.effective_type == "mcp.call" and analysis.step.server and analysis.step.tool:
                self._check_mcp_params(analysis, result)

        # 7. Output references valid
        if pipeline.output:
            input_keys = set(pipeline.input.keys())
            for key, ref in pipeline.output.items():
                for step_id in step_ids:
                    if step_id in ref:
                        break
                else:
                    if "{{" in ref:
                        # Allow references to input.* (pipeline input params)
                        refs = re.findall(r'\{\{\s*(\w+)\.', str(ref))
                        if any(r == "input" or r in input_keys for r in refs):
                            pass  # Valid input reference
                        else:
                            result.add_warning(
                                f"Output '{key}': may reference non-existent step"
                            )

        # 10. Proactive hints: when + else_of on same step (T-BRIX-V5-03)
        for step in pipeline.steps:
            if step.when and step.else_of:
                result.add_warning(
                    f"Step '{step.id}': has both 'when' and 'else_of' — these are mutually exclusive. "
                    f"'else_of' already implies a condition (runs only when the referenced step was skipped)."
                )

        # 11. Proactive hints: on_error:continue on HTTP/MCP steps (T-BRIX-V5-03)
        for analysis in self._validation_ctx.analyses:
            step = analysis.step
            if step.on_error == "continue" and analysis.effective_type in ("http.request", "mcp.call"):
                result.add_warning(
                    f"Step '{step.id}': on_error: continue on a {analysis.effective_type} step — "
                    f"consider on_error: retry for transient errors."
                )

        # 9b. Helper references — check registry and validate input_schema (T-BRIX-V4-BUG-12)
        for analysis in self._validation_ctx.analyses:
            if getattr(analysis.step, "helper", None):
                self._check_helper_reference(analysis, result)

        # 9. Requirements — warn if packages not installed (T-BRIX-V4-BUG-11)
        if pipeline.requirements:
            from brix.deps import check_requirements
            missing = check_requirements(pipeline.requirements)
            if missing:
                for req in missing:
                    result.add_warning(
                        f"Requirement '{req}' is not installed — will be auto-installed at runtime"
                    )
            else:
                result.add_check(f"All {len(pipeline.requirements)} requirement(s) installed")

        # 12. Schema-Contracts: inter-step output→input schema compatibility (T-BRIX-V6-13)
        self._check_schema_contracts(pipeline, result)

        # 13. Pipeline Linting Rules (T-BRIX-V6-16)
        self._run_lint_rules(pipeline, result)

        # 14. Preflight validation (E-BRIX-PREFLIGHT)
        if level in {"standard", "deep"}:
            self._check_deprecated_step_types(pipeline, result)
            self._check_config_param_misplacement(pipeline, result)
            self._check_config_toplevel_conflicts(pipeline, result)
            self._check_brick_config_schema(pipeline, result)
            self._check_jinja_ast(pipeline, result)
            self._check_sub_pipeline_existence(pipeline, result)
            self._check_connection_existence(pipeline, result)

        # 15. Deep preflight: active connection tests
        if level == "deep":
            self._check_connection_health(pipeline, result)

        # 16–25. Extended validation checks (T-BRIX-VAL-01 through T-BRIX-VAL-10)
        if level in {"standard", "deep"}:
            self._check_missing_output_in_refs(pipeline, result)       # VAL-01
            self._check_tojson_on_string(pipeline, result)             # VAL-02
            self._check_helper_without_code(pipeline, result)          # VAL-03
            self._check_unused_steps(pipeline, result)                 # VAL-04
            self._check_sub_pipeline_output_mismatch(pipeline, result) # VAL-05
            self._check_foreach_on_non_list(pipeline, result)          # VAL-06
            self._check_db_query_dml(pipeline, result)                 # VAL-07
            self._check_duplicate_ids_across_sub_pipelines(pipeline, result)  # VAL-08
            self._check_large_helper_without_schema(pipeline, result)  # VAL-09
            self._check_cross_helper_imports(pipeline, result)         # VAL-10
            self._check_step_output_type_compatibility(pipeline, result)  # VAL-11

        if result.is_valid:
            result.add_check("Pipeline is valid")

        self._validation_ctx = None
        return result

    @staticmethod
    def _effective_policy_level(pipeline: Pipeline) -> str:
        """Return the active validation policy with strict_bricks back-compat."""
        if pipeline.policy_level == "locked":
            return "locked"
        if pipeline.policy_level == "strict" or pipeline.strict_bricks:
            return "strict"
        return "permissive"

    def validate_input_params(self, pipeline: "Pipeline", user_input: dict) -> "ValidationResult":
        """Validate that all required pipeline input params are present in user_input.

        Required params are those defined in pipeline.input with no default value.
        Returns a ValidationResult — callers should check is_valid and errors.
        """
        result = ValidationResult()
        for key, param in pipeline.input.items():
            if param.default is None and key not in user_input:
                result.add_error(f"Missing required input parameter: '{key}'")
        if result.is_valid:
            result.add_check("All required input parameters present")
        return result

    @staticmethod
    def _schema_ref(brick_name: str | None) -> str | None:
        if not brick_name:
            return None
        return f'get_brick_schema(name="{brick_name}")'

    def _step_schema_ref(self, step: Any) -> str | None:
        if isinstance(step, StepAnalysis):
            return self._schema_ref(step.effective_type)
        step_type = getattr(step, "type", None) or ""
        return self._schema_ref(LEGACY_ALIASES.get(step_type, step_type))

    @staticmethod
    def _type_name(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int) and not isinstance(value, bool):
            return "integer"
        if isinstance(value, float):
            return "number"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "object"
        return type(value).__name__

    def _check_credential(self, key: str, env_ref: str, result: "ValidationResult") -> None:
        """Check a credential reference: UUID → CredentialStore, else → os.environ."""
        from brix.credential_store import is_credential_uuid, CredentialStore, CredentialNotFoundError

        if is_credential_uuid(env_ref):
            try:
                CredentialStore().get(env_ref)
                result.add_check(f"Credential '{key}' (uuid: {env_ref}): found in store")
            except CredentialNotFoundError:
                result.add_warning(
                    f"Credential '{key}' (uuid: {env_ref}): NOT FOUND in credential store"
                )
            except Exception:
                result.add_warning(
                    f"Credential '{key}' (uuid: {env_ref}): could not verify in credential store"
                )
        else:
            env_val = os.environ.get(env_ref)
            if env_val:
                result.add_check(f"Credential '{key}' (env: {env_ref}): set")
            else:
                result.add_warning(f"Credential '{key}' (env: {env_ref}): NOT SET")

    def _check_step_references(self, step_or_analysis, all_step_ids, pipeline, result):
        """Check that step references point to earlier steps."""
        analysis = step_or_analysis if isinstance(step_or_analysis, StepAnalysis) else self._validation_ctx.for_step(step_or_analysis)
        step = analysis.step
        step_idx = analysis.index if analysis.index >= 0 else next(i for i, s in enumerate(pipeline.steps) if s.id == step.id)
        earlier_ids = set(all_step_ids[:step_idx])
        input_keys = set(pipeline.input.keys())

        # Check foreach, params, when, etc. for {{ step_id.output }} references
        fields_to_check = [step.foreach]
        if analysis.has_params:
            fields_to_check.extend(str(v) for v in self._params_values(analysis))
        if step.when:
            fields_to_check.append(step.when)

        for field in fields_to_check:
            if field and "{{" in str(field):
                # Extract referenced step IDs
                refs = re.findall(r'\{\{\s*(\w+)\.output', str(field))
                for ref in refs:
                    if ref not in earlier_ids and ref != "input" and ref not in input_keys:
                        if ref in all_step_ids:
                            result.add_error(
                                f"Step '{step.id}' references future step '{ref}'",
                                hint=f"Only earlier step IDs are available here: {sorted(earlier_ids)}",
                            )
                        elif ref not in ["item", "credentials"]:
                            result.add_warning(
                                f"Step '{step.id}' references unknown '{ref}'",
                                hint=f"Available step IDs: {all_step_ids}",
                            )

    def _check_when_default(self, when_step_or_analysis, all_steps, result):
        """Warn if a conditional step is referenced without | default."""
        when_analysis = (
            when_step_or_analysis
            if isinstance(when_step_or_analysis, StepAnalysis)
            else self._validation_ctx.for_step(when_step_or_analysis)
        )
        when_step = when_analysis.step
        for step in all_steps:
            if step.id == when_step.id:
                continue
            fields_to_check = []
            analysis = self._validation_ctx.for_step(step)
            if analysis.has_params:
                fields_to_check.extend(str(v) for v in self._params_values(analysis))
            if step.foreach:
                fields_to_check.append(step.foreach)

            for field in fields_to_check:
                if when_step.id in str(field) and "output" in str(field):
                    if "default" not in str(field):
                        result.add_warning(
                            f"Step '{step.id}' references conditional step '{when_step.id}' "
                            f"without | default() — may fail if skipped (D-16)",
                            hint=f"Use something like {{ {when_step.id}.output | default(...) }} when '{when_step.id}' may be skipped.",
                        )

    def _check_helper_reference(self, step_or_analysis, result) -> None:
        """Validate a step's ``helper`` field against the HelperRegistry (T-BRIX-V4-BUG-12).

        Checks:
        - The referenced helper exists in the registry.
        - If the helper declares an ``input_schema``, warn about step params that
          do not appear in the schema (schema mismatch) — skips Jinja2 templates.
        """
        from brix.helper_registry import HelperRegistry
        analysis = step_or_analysis if isinstance(step_or_analysis, StepAnalysis) else self._validation_ctx.for_step(step_or_analysis)
        step = analysis.step
        registry = HelperRegistry()
        entry = registry.get(step.helper)

        if entry is None:
            result.add_error(
                f"Step '{step.id}': Helper '{step.helper}' not found in registry. "
                f"Register it with: brix__register_helper",
                hint="Use list_helpers() to inspect registered helpers or register the missing helper first.",
                schema_ref=self._schema_ref("script.python"),
            )
            return

        result.add_check(f"Step '{step.id}': Helper '{step.helper}' found in registry")

        # Schema validation — warn on params not declared in input_schema
        input_schema = entry.input_schema or {}
        schema_properties = input_schema.get("properties", {})
        if schema_properties and analysis.has_params:
            for param_key, param_val in self._params_items(analysis):
                # Skip Jinja2-template values — considered dynamically supplied
                if "{{" in str(param_val):
                    continue
                if param_key not in schema_properties:
                    result.add_warning(
                        f"Step '{step.id}': param '{param_key}' is not declared in "
                        f"helper '{step.helper}' input_schema (T-BRIX-V4-BUG-12)",
                        hint=f"Check helper '{step.helper}' input_schema or remove the unexpected param.",
                        schema_ref=self._schema_ref("script.python"),
                    )

    def _check_mcp_params(self, step_or_analysis, result):
        """Warn if required MCP tool params are not supplied in the step definition (T-BRIX-V4-21).

        Looks up the cached tool schema and checks whether any schema-required
        params are missing from the step's params dict.  Params that use Jinja2
        templates (``{{ ... }}``) are considered dynamically supplied and are
        not flagged.
        """
        analysis = step_or_analysis if isinstance(step_or_analysis, StepAnalysis) else self._validation_ctx.for_step(step_or_analysis)
        step = analysis.step
        cached_tools = self.cache.load_tools(step.server)
        if not cached_tools:
            return  # No schema cached — skip check

        # Find the matching tool definition
        tool_def = next(
            (t for t in cached_tools if t.get("name") == step.tool),
            None,
        )
        if not tool_def:
            return  # Tool not in cache (already warned by check #3)

        input_schema = tool_def.get("inputSchema") or tool_def.get("input_schema") or {}
        schema_required: list[str] = input_schema.get("required", [])
        if not schema_required:
            return

        provided_keys = self._params_keys(analysis) if analysis.has_params else set()
        for req_key in schema_required:
            if req_key not in provided_keys:
                result.add_warning(
                    f"Step '{step.id}': MCP tool '{step.tool}' requires param '{req_key}' "
                    f"but it is not set in step params (T-BRIX-V4-21)",
                    hint=f"Add '{req_key}' to step params or make it optional in the tool schema.",
                    schema_ref=self._schema_ref("mcp.call"),
                )

    def _get_runner_config_schema(self, runner_name: str) -> dict | None:
        """Return config_schema() for a runner, cached lazily."""
        if not runner_name:
            return None
        if self._runner_config_schemas is None:
            self._runner_config_schemas = {}
            try:
                from brix.runners.base import discover_runners

                for step_type, runner_cls in discover_runners().items():
                    try:
                        schema = runner_cls().config_schema()
                    except Exception:
                        continue
                    if isinstance(schema, dict):
                        self._runner_config_schemas[step_type] = schema
            except Exception:
                self._runner_config_schemas = {}
        return self._runner_config_schemas.get(runner_name)

    @staticmethod
    def _step_to_validation_config(step: Any) -> dict[str, Any]:
        """Build a config dict that works for top-level fields and params/config usage."""
        raw: dict[str, Any] = {}
        step_dict = getattr(step, "__dict__", None)
        if isinstance(step_dict, dict):
            raw.update(
                {
                    key: value
                    for key, value in step_dict.items()
                    if not key.startswith("_") and value is not None
                }
            )
        elif hasattr(step, "model_dump"):
            try:
                raw.update(step.model_dump(exclude_none=True))
            except Exception:
                pass

        for nested_key in ("config", "params"):
            nested = raw.get(nested_key)
            if isinstance(nested, dict):
                for key, value in nested.items():
                    raw.setdefault(key, value)
        return raw

    def _resolve_step_schema(self, step) -> dict | None:
        """Resolve JSON schema for a step via BrickRegistry first, then runner schema."""
        registry = BrickRegistry()
        analysis = step if isinstance(step, StepAnalysis) else self._validation_ctx.for_step(step)
        step_type = getattr(analysis.step, "type", "") or ""
        effective_type = analysis.effective_type

        brick = registry.get(effective_type)
        if brick is None:
            brick = next((b for b in registry.list_all() if b.type == effective_type), None)

        if brick is not None:
            brick_schema = brick.to_json_schema()
            if brick_schema.get("properties") or brick_schema.get("required"):
                return brick_schema
            runner_name = brick.runner or ""
            if runner_name:
                runner_schema = self._get_runner_config_schema(runner_name)
                if runner_schema:
                    return runner_schema

        runner_candidates = [
            step_type,
            effective_type,
            effective_type.replace(".", "_"),
        ]
        for candidate in runner_candidates:
            schema = self._get_runner_config_schema(candidate)
            if schema:
                return schema
        return None

    def _check_brick_config_schema(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Validate step config against brick or runner JSON schema."""
        for analysis in self._validation_ctx.analyses:
            step = analysis.step
            schema = self._resolve_step_schema(analysis)
            if not schema:
                continue
            instance = self._step_to_validation_config(step)
            schema_ref = self._step_schema_ref(analysis)
            try:
                jsonschema.validate(instance=instance, schema=schema)
            except ValidationError as exc:
                if exc.validator == "required":
                    missing_field = exc.message.split("'")[1] if "'" in exc.message else str(exc.validator_value)
                    result.add_warning(
                        f'Step "{step.id}": missing required field "{missing_field}".',
                        hint=f'{schema_ref} shows required fields.' if schema_ref else "Check the brick schema for required fields.",
                        schema_ref=schema_ref,
                    )
                    continue

                if exc.validator == "type":
                    field_name = str(exc.path[-1]) if exc.path else "<value>"
                    actual_type = self._type_name(exc.instance)
                    expected_type = (
                        ", ".join(exc.validator_value)
                        if isinstance(exc.validator_value, list)
                        else str(exc.validator_value)
                    )
                    hint = "Use the value type required by the schema."
                    if expected_type == "integer":
                        hint = "Use int value or {{ input.limit | int }}"
                    result.add_warning(
                        f'Step "{step.id}": "{field_name}" is {actual_type}, schema expects {expected_type}.',
                        hint=hint,
                        schema_ref=schema_ref,
                    )
                    continue

                result.add_warning(
                    f"Step '{step.id}': config does not match schema: {exc.message}",
                    hint="Align the step fields with the brick schema.",
                    schema_ref=schema_ref,
                )

    @staticmethod
    def _collect_template_strings(value: Any, path: str = "") -> list[tuple[str, str]]:
        """Recursively collect Jinja template strings from nested config."""
        collected: list[tuple[str, str]] = []
        if isinstance(value, str):
            if any(token in value for token in ("{{", "{%", "{#")):
                collected.append((path or "<value>", value))
            return collected
        if isinstance(value, dict):
            for key, nested in value.items():
                next_path = f"{path}.{key}" if path else str(key)
                collected.extend(PipelineValidator._collect_template_strings(nested, next_path))
            return collected
        if isinstance(value, (list, tuple)):
            for idx, nested in enumerate(value):
                next_path = f"{path}[{idx}]" if path else f"[{idx}]"
                collected.extend(PipelineValidator._collect_template_strings(nested, next_path))
        return collected

    class _JinjaRootNameVisitor(NodeVisitor):
        """Collect load-context root names while respecting local template bindings."""

        def __init__(self):
            self.names: set[str] = set()
            self._locals_stack: list[set[str]] = [set()]

        def _declare_target(self, target: nodes.Node) -> None:
            if isinstance(target, nodes.Name):
                self._locals_stack[-1].add(target.name)
            elif isinstance(target, (nodes.Tuple, nodes.List)):
                for item in target.items:
                    self._declare_target(item)

        def _is_local(self, name: str) -> bool:
            return any(name in scope for scope in reversed(self._locals_stack))

        def visit_Name(self, node: nodes.Name, *args: Any, **kwargs: Any) -> None:
            if node.ctx == "load" and not self._is_local(node.name):
                self.names.add(node.name)

        def visit_Assign(self, node: nodes.Assign, *args: Any, **kwargs: Any) -> None:
            self.visit(node.node, *args, **kwargs)
            self._declare_target(node.target)

        def visit_AssignBlock(self, node: nodes.AssignBlock, *args: Any, **kwargs: Any) -> None:
            self._locals_stack.append(set())
            self.generic_visit(node, *args, **kwargs)
            self._locals_stack.pop()
            self._declare_target(node.target)

        def visit_For(self, node: nodes.For, *args: Any, **kwargs: Any) -> None:
            self.visit(node.iter, *args, **kwargs)
            if node.test is not None:
                self.visit(node.test, *args, **kwargs)
            self._locals_stack.append(set())
            self._declare_target(node.target)
            for child in node.body:
                self.visit(child, *args, **kwargs)
            for child in node.else_:
                self.visit(child, *args, **kwargs)
            self._locals_stack.pop()

        def visit_Macro(self, node: nodes.Macro, *args: Any, **kwargs: Any) -> None:
            macro_scope = {arg.name for arg in node.args}
            self._locals_stack.append(macro_scope)
            for child in node.body:
                self.visit(child, *args, **kwargs)
            self._locals_stack.pop()

    def _check_jinja_ast(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Parse Jinja templates and warn on unknown root names."""
        env = jinja2.Environment()
        step_ids = [step.id for step in pipeline.steps]
        known_names = set(step_ids)
        known_names.update(
            {
                "input",
                "item",
                "credentials",
                "var",
                "store",
                "last_output",
                "now",
                "utcnow",
                "uuid4",
                "zip",
                "env",
                "fail",
                "range",
                "dict",
                "namespace",
                "loop",
                "true",
                "false",
                "none",
                "default",
                "length",
                "selectattr",
                "list",
                "join",
                "int",
                "float",
                "string",
                "tojson",
                "fromjson",
                "iif",
                "b64encode",
                "b64decode",
            }
        )

        for step in pipeline.steps:
            templates: list[tuple[str, str]] = []
            templates.extend(self._collect_template_strings(getattr(step, "params", None), "params"))
            templates.extend(self._collect_template_strings(getattr(step, "config", None), "config"))
            if getattr(step, "when", None):
                templates.extend(self._collect_template_strings(step.when, "when"))
            if getattr(step, "foreach", None):
                templates.extend(self._collect_template_strings(step.foreach, "foreach"))

            for field_path, template in templates:
                try:
                    ast = env.parse(template)
                except jinja2.TemplateSyntaxError as exc:
                    result.add_error(
                        f"Step '{step.id}': Jinja2 syntax error in {field_path}: {exc}",
                        hint="Check brackets and filters.",
                    )
                    continue

                visitor = self._JinjaRootNameVisitor()
                visitor.visit(ast)
                unknown = sorted(name for name in visitor.names if name not in known_names)
                for name in unknown:
                    suggestion = difflib.get_close_matches(name, step_ids, n=1)
                    suggestion_text = f' Did you mean "{suggestion[0]}"?' if suggestion else ""
                    result.add_warning(
                        f"Step '{step.id}': template references unknown '{name}' in {field_path}.",
                        hint=f"Available step IDs: {step_ids}.{suggestion_text}".strip(),
                    )

    def _check_deprecated_step_types(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Escalate legacy flat step types according to pipeline policy."""
        policy_level = self._effective_policy_level(pipeline)
        for step in pipeline.steps:
            replacement = LEGACY_ALIASES.get(step.type)
            if not replacement:
                continue
            finding = (
                f'Step "{step.id}": type "{step.type}" is deprecated.'
                if policy_level == "permissive"
                else f'Step "{step.id}": type "{step.type}" is not allowed under {policy_level} policy.'
            )
            hint = (
                f'Use "{replacement}" instead. See list_bricks().'
                if policy_level == "permissive"
                else f'Use "{replacement}" instead.'
            )
            if policy_level == "permissive":
                result.add_warning(
                    finding,
                    hint=hint,
                    schema_ref=self._schema_ref(replacement),
                )
            else:
                result.add_error(
                    finding,
                    hint=hint,
                    schema_ref=self._schema_ref(replacement),
                )
        if policy_level != "locked":
            return

        registry = BrickRegistry()
        for step in pipeline.steps:
            if "{{" in step.type:
                continue
            if LEGACY_ALIASES.get(step.type):
                continue
            brick = registry.get(step.type)
            if brick is None:
                result.add_error(
                    f'Step "{step.id}": type "{step.type}" is not allowed under locked policy.',
                    hint="Use a registered dot-notation brick type from list_bricks().",
                    schema_ref=self._schema_ref(step.type),
                )

    def _check_config_param_misplacement(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when runner-consumed fields are placed in params instead of config."""
        field_map: dict[str, tuple[str, ...]] = {
            "flow.pipeline": ("pipeline",),
            "script.python": ("helper", "script"),
            "db.query": ("connection",),
            "db.exec": ("connection",),
            "db.upsert": ("connection",),
            "mcp.call": ("server", "tool"),
            "script.cli": ("command",),
        }
        for step in pipeline.steps:
            analysis = self._validation_ctx.for_step(step)
            effective_type = analysis.effective_type
            candidate_fields = field_map.get(effective_type)
            if not candidate_fields:
                continue
            params = analysis.params_dict
            config = analysis.config
            if not params:
                continue
            for field in candidate_fields:
                if field in params and field not in config:
                    result.add_warning(
                        f'Step "{step.id}": "{field}" found in params but should be in config.',
                        hint=f'{self._schema_ref(effective_type)} shows config structure.' if effective_type else "Move the field into config.",
                        schema_ref=self._schema_ref(effective_type),
                    )

    def _check_config_toplevel_conflicts(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when config and top-level step fields disagree after merge semantics."""
        for step in pipeline.steps:
            config = getattr(step, "config", None)
            if not isinstance(config, dict) or not config:
                continue

            for field in self._CONFIG_CONFLICT_FIELDS:
                config_value = config.get(field)
                toplevel_value = getattr(step, field, None)
                if config_value is None or toplevel_value is None:
                    continue
                if field in self._CONFIG_CONFLICT_DEFAULTS and toplevel_value == self._CONFIG_CONFLICT_DEFAULTS[field]:
                    continue
                if config_value == toplevel_value:
                    continue

                result.add_warning(
                    f"Step {step.id}: config.{field}={config_value!r} differs from step.{field}={toplevel_value!r}. Config takes precedence after merge.",
                    hint="Remove the top-level value or set it to match config.",
                    schema_ref=self._schema_ref(step.type),
                )

    @staticmethod
    def _is_dynamic_ref(value: Any) -> bool:
        return isinstance(value, str) and any(token in value for token in ("{{", "{%", "{#"))

    def _check_sub_pipeline_existence(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Ensure statically referenced sub-pipelines exist."""
        store = PipelineStore()
        for analysis in self._validation_ctx.analyses:
            step = analysis.step
            if analysis.effective_type != "flow.pipeline":
                continue
            pipeline_name = (
                getattr(step, "pipeline", None)
                or analysis.params_get("pipeline")
                or analysis.config.get("pipeline")
            )
            if not pipeline_name or self._is_dynamic_ref(pipeline_name):
                continue
            try:
                store.load(pipeline_name)
            except FileNotFoundError:
                result.add_error(
                    f"Step '{step.id}': sub-pipeline '{pipeline_name}' not found.",
                    hint="list_pipelines() shows available pipelines.",
                    schema_ref=self._schema_ref("flow.pipeline"),
                )

    def _collect_connection_refs(self, pipeline: Pipeline) -> list[tuple[str, str]]:
        refs: list[tuple[str, str]] = []
        for analysis in self._validation_ctx.analyses:
            step = analysis.step
            connection_name = (
                getattr(step, "connection", None)
                or analysis.params_get("connection")
                or analysis.config.get("connection")
            )
            if not connection_name or self._is_dynamic_ref(connection_name):
                continue
            refs.append((step.id, connection_name))
        return refs

    def _check_connection_existence(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Ensure statically referenced named connections exist."""
        refs = self._collect_connection_refs(pipeline)
        if not refs:
            return

        try:
            from brix.db import BrixDB

            manager = ConnectionManager(BrixDB())
            known = {item.get("name") for item in manager.list()}
        except Exception as exc:
            result.add_warning(f"Could not verify connection existence: {exc}")
            return

        for step_id, name in refs:
            if name not in known:
                result.add_error(
                    f'Step "{step_id}": connection "{name}" not found.',
                    hint="connection_list() shows registered connections.",
                    schema_ref=self._schema_ref("db.query"),
                )

    def _check_connection_health(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Active connection ping for deep validation mode."""
        refs = self._collect_connection_refs(pipeline)
        if not refs:
            return

        try:
            from brix.db import BrixDB

            manager = ConnectionManager(BrixDB())
        except Exception as exc:
            result.add_warning(f"Could not initialize connection tests: {exc}")
            return

        seen: set[str] = set()
        for _, name in refs:
            if name in seen:
                continue
            seen.add(name)
            test_result = manager.test(name)
            if test_result.get("success"):
                result.add_check(f"Connection '{name}': test passed")
            elif "not found" not in str(test_result.get("error", "")).lower():
                result.add_warning(
                    f"Connection '{name}': test failed: {test_result.get('error', 'unknown error')}"
                )

    # ---------------------------------------------------------------------------
    # V6-13: Schema-Contracts
    # ---------------------------------------------------------------------------

    def _check_schema_contracts(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Check inter-step schema compatibility (T-BRIX-V6-13).

        When step A has output_schema and step B has input_schema AND B references
        A's output, verify that the fields declared in B's input_schema are a subset
        of the fields declared in A's output_schema.
        """
        # Build a map from step_id → output_schema
        output_schemas: dict[str, dict] = {}
        for step in pipeline.steps:
            schema = getattr(step, "output_schema", None) or {}
            if schema:
                output_schemas[step.id] = schema

        if not output_schemas:
            return  # Nothing to check

        # For each step with input_schema, find which earlier step it references
        for step in pipeline.steps:
            if not getattr(step, "input_schema", None):
                continue

            # Collect all step IDs referenced in this step's params/foreach
            referenced_ids: set[str] = set()
            fields_to_scan = []
            if step.params:
                fields_to_scan.extend(str(v) for v in self._params_values(step))
            if step.foreach:
                fields_to_scan.append(step.foreach)

            for field_val in fields_to_scan:
                if "{{" in field_val:
                    refs = re.findall(r'\{\{\s*(\w+)\.output', field_val)
                    referenced_ids.update(refs)

            # Check each referenced step that has output_schema
            for ref_id in referenced_ids:
                if ref_id not in output_schemas:
                    continue
                src_schema = output_schemas[ref_id]
                # input_schema keys are the fields the step expects from the upstream output
                step_input_schema = getattr(step, "input_schema", None) or {}
                missing = [k for k in step_input_schema if k not in src_schema]
                if missing:
                    result.add_warning(
                        f"Step '{step.id}': input_schema expects fields {missing} "
                        f"not declared in step '{ref_id}' output_schema (T-BRIX-V6-13)"
                    )
                else:
                    result.add_check(
                        f"Step '{step.id}': schema contract with '{ref_id}' is compatible"
                    )

    # ---------------------------------------------------------------------------
    # V6-16: Pipeline Linting Rules
    # ---------------------------------------------------------------------------

    def _run_lint_rules(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Apply configurable linting rules to the pipeline (T-BRIX-V6-16)."""
        rules = self._load_lint_rules()
        for rule in rules:
            check = rule.get("check")
            if check == "max_concurrency":
                self._lint_max_concurrency(pipeline, rule, result)
            elif check == "no_base64_foreach":
                self._lint_no_base64_foreach(pipeline, rule, result)
            elif check == "progress_on_long_timeout":
                self._lint_progress_on_long_timeout(pipeline, rule, result)
            # Custom / unknown rules are silently skipped

    def _lint_max_concurrency(self, pipeline: Pipeline, rule: dict, result: ValidationResult) -> None:
        """Warn when a step of the specified type exceeds max concurrency."""
        target_type = rule.get("type")
        max_conc = rule.get("max", 5)
        for step in pipeline.steps:
            if target_type and step.type != target_type:
                continue
            if step.parallel and step.concurrency > max_conc:
                result.add_warning(
                    f"Step '{step.id}': concurrency {step.concurrency} exceeds "
                    f"recommended max {max_conc} for {step.type} steps "
                    f"[lint:{rule.get('id', 'max-concurrency')}]"
                )

    def _lint_no_base64_foreach(self, pipeline: Pipeline, rule: dict, result: ValidationResult) -> None:
        """Warn when a foreach step's params contain 'base64' — OOM risk."""
        for step in pipeline.steps:
            if not step.foreach:
                continue
            if step.params:
                for k, v in self._params_items(step):
                    if "base64" in str(k).lower() or "base64" in str(v).lower():
                        result.add_warning(
                            f"Step '{step.id}': param '{k}' contains 'base64' in a foreach step "
                            f"— large base64 payloads in foreach loops can cause OOM "
                            f"[lint:{rule.get('id', 'no-base64-foreach')}]"
                        )

    def _lint_progress_on_long_timeout(self, pipeline: Pipeline, rule: dict, result: ValidationResult) -> None:
        """Warn when a step has a long timeout but progress:false.

        MCP steps calling external servers are excluded — progress:true only makes
        sense for runners that natively emit progress events (python, cli, http).
        """
        threshold = rule.get("timeout_threshold_seconds", 60)
        # Runners that do not support progress events
        _PROGRESS_UNSUPPORTED = {"mcp", "mcp.call"}
        for step in pipeline.steps:
            if not step.timeout:
                continue
            # Skip MCP steps — external servers don't support Brix progress events
            if step.type in _PROGRESS_UNSUPPORTED:
                continue
            timeout_secs = self._parse_timeout_seconds(step.timeout)
            if timeout_secs is not None and timeout_secs > threshold and not step.progress:
                result.add_warning(
                    f"Step '{step.id}': timeout={step.timeout} (>{threshold}s) but progress:true "
                    f"is not set — consider enabling progress for long-running steps "
                    f"[lint:{rule.get('id', 'progress-on-long-timeout')}]"
                )

    @staticmethod
    def _parse_timeout_seconds(timeout_str: str) -> Optional[float]:
        """Parse a timeout string like '30s', '5m', '1h' to seconds."""
        if not timeout_str:
            return None
        timeout_str = timeout_str.strip()
        if timeout_str.endswith("s"):
            try:
                return float(timeout_str[:-1])
            except ValueError:
                return None
        if timeout_str.endswith("m"):
            try:
                return float(timeout_str[:-1]) * 60
            except ValueError:
                return None
        if timeout_str.endswith("h"):
            try:
                return float(timeout_str[:-1]) * 3600
            except ValueError:
                return None
        try:
            return float(timeout_str)
        except ValueError:
            return None

    @staticmethod
    def _normalise_output_type(output_type: str) -> str:
        return (output_type or "").strip().lower()

    def _build_step_output_type_map(self, pipeline: Pipeline) -> dict[str, str]:
        """Resolve output types for all steps using BrickRegistry with known fallbacks."""
        known_output_types = {
            "db.query": "list[dict]",
            "db.exec": "dict",
            "db.upsert": "dict",
            "flow.filter": "list[dict]",
            "flow.transform": "any",
            "mcp.call": "any",
            "script.python": "any",
            "flow.set": "dict",
            "flow.merge": "list[dict]",
        }
        step_output_types: dict[str, str] = {}
        try:
            registry = BrickRegistry()
        except Exception:
            registry = None

        for step in pipeline.steps:
            effective_type = LEGACY_ALIASES.get(step.type, step.type)
            output_type = ""
            if registry is not None:
                try:
                    brick = registry.get(effective_type)
                except Exception:
                    brick = None
                if brick and getattr(brick, "output_type", ""):
                    output_type = str(brick.output_type)
            if not output_type:
                output_type = known_output_types.get(effective_type, "")
            if output_type:
                step_output_types[step.id] = output_type
        return step_output_types

    @staticmethod
    def _extract_step_output_refs(value: Any) -> list[str]:
        if not isinstance(value, str) or "{{" not in value:
            return []
        return [match.group(1) for match in re.finditer(r'\{\{\s*(\w+)\.output\b', value)]

    def _is_expected_list_type(self, output_type: str) -> bool:
        norm = self._normalise_output_type(output_type)
        if not norm or norm in {"any", "*"}:
            return True
        if norm.startswith("list[") or norm == "list":
            return True
        return False

    def _is_expected_dict_type(self, output_type: str) -> bool:
        norm = self._normalise_output_type(output_type)
        if not norm or norm in {"any", "*"}:
            return True
        return norm in {"dict", "object", "json"}

    def _is_single_dict_type(self, output_type: str) -> bool:
        return self._normalise_output_type(output_type) in {"dict", "object", "json"}

    def _iter_step_output_type_compatibility_issues(self, pipeline: Pipeline) -> list[dict[str, str]]:
        """Collect output type mismatches for step-to-step references."""
        step_output_types = self._build_step_output_type_map(pipeline)
        issues: list[dict[str, str]] = []
        seen: set[tuple[str, str, str]] = set()

        def add_issue(
            consumer_step_id: str,
            source_step_id: str,
            context_name: str,
            expected_type: str,
            actual_type: str,
            hint: str,
        ) -> None:
            key = (consumer_step_id, context_name, source_step_id)
            if key in seen:
                return
            seen.add(key)
            issues.append(
                {
                    "consumer_step_id": consumer_step_id,
                    "source_step_id": source_step_id,
                    "context_name": context_name,
                    "expected_type": expected_type,
                    "actual_type": actual_type,
                    "hint": hint,
                }
            )

        def check_template_ref(
            consumer_step: Step,
            context_name: str,
            expected_type: str,
            raw_value: Any,
            hint_builder,
        ) -> None:
            for ref_id in self._extract_step_output_refs(raw_value):
                output_type = step_output_types.get(ref_id, "")
                if not output_type:
                    continue
                if expected_type == "list":
                    if self._is_expected_list_type(output_type):
                        continue
                elif expected_type == "dict":
                    if self._is_expected_dict_type(output_type):
                        continue
                elif expected_type == "list[dict]":
                    if self._is_expected_list_type(output_type) and not self._is_single_dict_type(output_type):
                        continue
                add_issue(
                    consumer_step.id,
                    ref_id,
                    context_name,
                    expected_type,
                    output_type,
                    hint_builder(ref_id, output_type),
                )

        for step in pipeline.steps:
            effective_type = LEGACY_ALIASES.get(step.type, step.type)
            params = getattr(step, "params", None)
            config = getattr(step, "config", None) or {}

            if step.foreach:
                check_template_ref(
                    step,
                    "foreach",
                    "list",
                    step.foreach,
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but foreach expects list. "
                        f"Did you mean {{{{ {ref_id}.output.rows }}}} or use flow.flatten first?"
                    ),
                )

            if effective_type == "db.upsert":
                check_template_ref(
                    step,
                    "db.upsert data",
                    "list[dict]",
                    self._params_get(params, "data"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.upsert data expects list[dict]. "
                        f"Did you mean {{{{ {ref_id}.output.rows }}}} or wrap the dict in a one-item list first?"
                    ),
                )
                check_template_ref(
                    step,
                    "db.upsert data",
                    "list[dict]",
                    config.get("data"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.upsert data expects list[dict]. "
                        f"Did you mean {{{{ {ref_id}.output.rows }}}} or wrap the dict in a one-item list first?"
                    ),
                )

            if effective_type == "db.exec":
                check_template_ref(
                    step,
                    "db.exec params",
                    "list",
                    self._params_get(params, "params"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.exec params expects list. "
                        f"Wrap the values in a positional list or map them with flow.transform first."
                    ),
                )
                check_template_ref(
                    step,
                    "db.exec params",
                    "list",
                    config.get("params"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.exec params expects list. "
                        f"Wrap the values in a positional list or map them with flow.transform first."
                    ),
                )

            if effective_type == "db.query":
                check_template_ref(
                    step,
                    "db.query params",
                    "dict",
                    self._params_get(params, "params"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.query params expects dict. "
                        f"Did you mean {{{{ {ref_id}.output[0] }}}} or map the fields with flow.set first?"
                    ),
                )
                check_template_ref(
                    step,
                    "db.query params",
                    "dict",
                    config.get("params"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.query params expects dict. "
                        f"Did you mean {{{{ {ref_id}.output[0] }}}} or map the fields with flow.set first?"
                    ),
                )

            if effective_type == "flow.filter":
                check_template_ref(
                    step,
                    "flow.filter input",
                    "list",
                    self._params_get(params, "input"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but flow.filter input expects list. "
                        f"Did you mean {{{{ {ref_id}.output.rows }}}} or use flow.flatten first?"
                    ),
                )
                check_template_ref(
                    step,
                    "flow.filter input",
                    "list",
                    config.get("input"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but flow.filter input expects list. "
                        f"Did you mean {{{{ {ref_id}.output.rows }}}} or use flow.flatten first?"
                    ),
                )

            if effective_type == "flow.merge":
                input_refs = []
                if isinstance(step.inputs, list):
                    input_refs.extend(ref_id for ref_id in step.inputs if isinstance(ref_id, str))
                params_inputs = self._params_get(params, "inputs")
                if isinstance(params_inputs, list):
                    input_refs.extend(ref_id for ref_id in params_inputs if isinstance(ref_id, str))
                config_inputs = config.get("inputs")
                if isinstance(config_inputs, list):
                    input_refs.extend(ref_id for ref_id in config_inputs if isinstance(ref_id, str))
                for ref_id in input_refs:
                    output_type = step_output_types.get(ref_id, "")
                    if not output_type or self._is_expected_list_type(output_type):
                        continue
                    add_issue(
                        step.id,
                        ref_id,
                        "flow.merge inputs",
                        "list",
                        output_type,
                        (
                            f"Step '{ref_id}' output is {output_type} but flow.merge inputs should resolve to lists. "
                            f"Use flow.flatten or wrap the item in a list first."
                        ),
                    )

        return issues

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-01: Missing .output in step references
    # ---------------------------------------------------------------------------

    def _check_missing_output_in_refs(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when {{ step_id.field }} is used but .output is missing."""
        step_ids = {s.id for s in pipeline.steps}
        # Built-in root names that are NOT step IDs — skip these
        _BUILTINS = {"input", "item", "credentials", "var", "store", "env", "loop"}

        for step in pipeline.steps:
            templates = self._collect_template_strings(getattr(step, "params", None), "params")
            templates.extend(self._collect_template_strings(getattr(step, "config", None), "config"))
            if getattr(step, "when", None):
                templates.extend(self._collect_template_strings(step.when, "when"))
            if getattr(step, "foreach", None):
                templates.extend(self._collect_template_strings(step.foreach, "foreach"))

            for field_path, tmpl in templates:
                # Find patterns like {{ some_step.field }} where some_step is a known step
                # but the access does NOT go through .output
                for match in re.finditer(r'\{\{\s*(\w+)\.(\w+)', tmpl):
                    root_name = match.group(1)
                    attr = match.group(2)
                    if root_name in _BUILTINS:
                        continue
                    if root_name in step_ids and attr != "output":
                        result.add_warning(
                            f"Step '{step.id}': references {{{{ {root_name}.{attr} }}}} "
                            f"— did you mean {{{{ {root_name}.output.{attr} }}}}? (T-BRIX-VAL-01)",
                            hint="Step results are nested under .output — direct attribute access on a step ID is usually a mistake.",
                        )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-02: tojson on already-string values
    # ---------------------------------------------------------------------------

    def _check_tojson_on_string(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when | tojson is applied to a value known to be a string."""
        # Build a map of step_id → output_type from brick registry
        string_output_steps: set[str] = set()
        try:
            registry = BrickRegistry()
            for step in pipeline.steps:
                effective_type = LEGACY_ALIASES.get(step.type, step.type)
                brick = registry.get(effective_type)
                if brick and getattr(brick, "output_type", "") == "string":
                    string_output_steps.add(step.id)
        except Exception:
            return  # Can't determine types — skip

        if not string_output_steps:
            return

        for step in pipeline.steps:
            templates = self._collect_template_strings(getattr(step, "params", None), "params")
            templates.extend(self._collect_template_strings(getattr(step, "config", None), "config"))

            for field_path, tmpl in templates:
                # Look for {{ step_id.output ... | tojson }}
                for match in re.finditer(r'\{\{.*?(\w+)\.output.*?\|\s*tojson.*?\}\}', tmpl):
                    ref_id = match.group(1)
                    if ref_id in string_output_steps:
                        result.add_warning(
                            f"Step '{step.id}': applies | tojson to '{ref_id}.output' "
                            f"which is already a string type (T-BRIX-VAL-02)",
                            hint="| tojson on a string adds extra quotes — remove it if the value is already a string.",
                        )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-03: Helper without code
    # ---------------------------------------------------------------------------

    def _check_helper_without_code(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when a step references a helper that exists but has no code."""
        from brix.helper_registry import HelperRegistry
        registry = HelperRegistry()

        for step in pipeline.steps:
            helper_name = getattr(step, "helper", None)
            if not helper_name:
                continue
            entry = registry.get(helper_name)
            if entry is None:
                continue  # Already handled by _check_helper_reference
            if not entry.code:
                result.add_warning(
                    f"Step '{step.id}': helper '{helper_name}' exists but has no code "
                    f"(has_code=false) (T-BRIX-VAL-03)",
                    hint="Register the helper with actual code or remove the reference.",
                )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-04: Unused steps
    # ---------------------------------------------------------------------------

    def _check_unused_steps(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Info-level hint for steps not referenced by any other step and without side effects."""
        _SIDE_EFFECT_TYPES = {
            "db.upsert", "db.exec", "action.notify", "action.emit",
            "action.queue", "action.approval", "action.respond",
            "script.python", "script.cli", "mcp.call",
            # Legacy aliases
            "python", "cli", "mcp", "notify", "exec",
        }

        step_ids = {s.id for s in pipeline.steps}
        # Collect all step IDs referenced in templates of other steps
        referenced_ids: set[str] = set()
        for step in pipeline.steps:
            templates = self._collect_template_strings(getattr(step, "params", None))
            templates.extend(self._collect_template_strings(getattr(step, "config", None)))
            if getattr(step, "when", None):
                templates.extend(self._collect_template_strings(step.when))
            if getattr(step, "foreach", None):
                templates.extend(self._collect_template_strings(step.foreach))
            if getattr(step, "else_of", None):
                referenced_ids.add(step.else_of)

            for _, tmpl in templates:
                for match in re.finditer(r'\{\{\s*(\w+)\.', tmpl):
                    referenced_ids.add(match.group(1))

        # Also consider output references
        if pipeline.output:
            for key, ref in pipeline.output.items():
                for match in re.finditer(r'\{\{\s*(\w+)\.', str(ref)):
                    referenced_ids.add(match.group(1))

        # Last step is implicitly used (pipeline result)
        last_step_id = pipeline.steps[-1].id if pipeline.steps else None

        for step in pipeline.steps:
            if step.id == last_step_id:
                continue
            if step.id in referenced_ids:
                continue
            effective_type = LEGACY_ALIASES.get(step.type, step.type)
            if effective_type in _SIDE_EFFECT_TYPES or step.type in _SIDE_EFFECT_TYPES:
                continue
            result.add_info(
                f"Step '{step.id}': appears unused — not referenced by any other step "
                f"and has no side effects (T-BRIX-VAL-04)",
                hint="Remove the step or reference its output in a downstream step.",
            )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-05: Output-schema mismatch between sub-pipeline and parent
    # ---------------------------------------------------------------------------

    def _check_sub_pipeline_output_mismatch(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when parent references keys not in sub-pipeline output."""
        store = PipelineStore()
        sub_pipeline_outputs: dict[str, set[str]] = {}  # step_id → output keys

        for step in pipeline.steps:
            if step.type not in {"pipeline", "flow.pipeline"}:
                continue
            params = getattr(step, "params", None) or {}
            config = getattr(step, "config", None) or {}
            pipeline_name = (
                getattr(step, "pipeline", None)
                or (params.get("pipeline") if isinstance(params, dict) else None)
                or (config.get("pipeline") if isinstance(config, dict) else None)
            )
            if not pipeline_name or self._is_dynamic_ref(pipeline_name):
                continue
            try:
                sub = store.load(pipeline_name)
                if sub.output:
                    sub_pipeline_outputs[step.id] = set(sub.output.keys())
            except Exception:
                continue  # Already handled by _check_sub_pipeline_existence

        if not sub_pipeline_outputs:
            return

        # Check if parent step templates reference keys not in sub-pipeline output
        for step in pipeline.steps:
            templates = self._collect_template_strings(getattr(step, "params", None))
            templates.extend(self._collect_template_strings(getattr(step, "config", None)))
            if getattr(step, "foreach", None):
                templates.extend(self._collect_template_strings(step.foreach))

            for _, tmpl in templates:
                for sub_step_id, output_keys in sub_pipeline_outputs.items():
                    # Look for {{ sub_step_id.output.some_key }}
                    for match in re.finditer(
                        rf'\{{\{{\s*{re.escape(sub_step_id)}\.output\.(\w+)',
                        tmpl,
                    ):
                        ref_key = match.group(1)
                        if ref_key not in output_keys:
                            result.add_warning(
                                f"Step '{step.id}': references '{sub_step_id}.output.{ref_key}' "
                                f"but sub-pipeline output only declares {sorted(output_keys)} "
                                f"(T-BRIX-VAL-05)",
                                hint="Check the sub-pipeline's output mapping or update the reference.",
                            )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-06: foreach on non-list expression
    # ---------------------------------------------------------------------------

    def _check_foreach_on_non_list(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when foreach references a step whose output type is not list-like."""
        for issue in self._iter_step_output_type_compatibility_issues(pipeline):
            if issue["context_name"] != "foreach":
                continue
            result.add_warning(
                f"Step '{issue['consumer_step_id']}': foreach references '{issue['source_step_id']}' "
                f"whose output_type is '{issue['actual_type']}' — foreach expects a list (T-BRIX-VAL-06)",
                hint=issue["hint"],
            )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-11: Output type compatibility between referenced steps
    # ---------------------------------------------------------------------------

    def _check_step_output_type_compatibility(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when a step consumes another step's output with an incompatible type."""
        for issue in self._iter_step_output_type_compatibility_issues(pipeline):
            if issue["context_name"] == "foreach":
                continue
            result.add_warning(
                f"Step '{issue['consumer_step_id']}': {issue['context_name']} references "
                f"'{issue['source_step_id']}.output' whose output_type is '{issue['actual_type']}' "
                f"but expects '{issue['expected_type']}' (T-BRIX-VAL-11)",
                hint=issue["hint"],
            )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-07: db.query used for DML
    # ---------------------------------------------------------------------------

    def _check_db_query_dml(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when db.query step contains DML statements."""
        _DML_PATTERN = re.compile(
            r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE)\b',
            re.IGNORECASE,
        )

        for step in pipeline.steps:
            effective_type = LEGACY_ALIASES.get(step.type, step.type)
            if effective_type != "db.query":
                continue

            query = getattr(step, "query", None) or ""
            params = getattr(step, "params", None) or {}
            config = getattr(step, "config", None) or {}
            if not query:
                query = (
                    (params.get("query") if isinstance(params, dict) else "")
                    or (config.get("query") if isinstance(config, dict) else "")
                    or ""
                )

            # Skip Jinja2 dynamic queries — can't statically check
            if not query or self._is_dynamic_ref(query):
                continue

            match = _DML_PATTERN.search(query)
            if match:
                result.add_warning(
                    f"Step '{step.id}': db.query contains DML statement "
                    f"'{match.group(1).upper()}' — use db.exec for DML operations (T-BRIX-VAL-07)",
                    hint="db.query is for SELECT statements. Use db.exec for INSERT/UPDATE/DELETE.",
                )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-08: Duplicate step IDs across sub-pipelines
    # ---------------------------------------------------------------------------

    def _check_duplicate_ids_across_sub_pipelines(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn on step ID collisions between parent and sub-pipelines."""
        store = PipelineStore()
        parent_ids = {s.id for s in pipeline.steps}

        for step in pipeline.steps:
            if step.type not in {"pipeline", "flow.pipeline"}:
                continue
            params = getattr(step, "params", None) or {}
            config = getattr(step, "config", None) or {}
            pipeline_name = (
                getattr(step, "pipeline", None)
                or (params.get("pipeline") if isinstance(params, dict) else None)
                or (config.get("pipeline") if isinstance(config, dict) else None)
            )
            if not pipeline_name or self._is_dynamic_ref(pipeline_name):
                continue
            try:
                sub = store.load(pipeline_name)
            except Exception:
                continue

            sub_ids = {s.id for s in sub.steps}
            collisions = parent_ids & sub_ids
            if collisions:
                result.add_warning(
                    f"Step '{step.id}': sub-pipeline '{pipeline_name}' shares step IDs "
                    f"with parent: {sorted(collisions)} (T-BRIX-VAL-08)",
                    hint="Step IDs are scoped per pipeline, but duplicate names can cause confusion in logs.",
                )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-09: Large helper without input_schema/output_schema
    # ---------------------------------------------------------------------------

    def _check_large_helper_without_schema(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when a helper has substantial code but no input_schema."""
        from brix.helper_registry import HelperRegistry
        registry = HelperRegistry()
        checked: set[str] = set()

        for step in pipeline.steps:
            helper_name = getattr(step, "helper", None)
            if not helper_name or helper_name in checked:
                continue
            checked.add(helper_name)
            entry = registry.get(helper_name)
            if entry is None:
                continue
            if len(entry.code) > 500 and not entry.input_schema.get("properties"):
                result.add_warning(
                    f"Step '{step.id}': helper '{helper_name}' has {len(entry.code)} chars of code "
                    f"but no input_schema — consider adding input_schema for validation (T-BRIX-VAL-09)",
                    hint="Helpers with substantial logic benefit from declared input_schema for automated validation.",
                )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-10: Cross-helper imports without imports field
    # ---------------------------------------------------------------------------

    def _check_cross_helper_imports(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Warn when helper code imports another helper not listed in imports field."""
        from brix.helper_registry import HelperRegistry
        registry = HelperRegistry()

        # Build set of all helper names in the registry
        all_helper_names: set[str] = set()
        try:
            for entry in registry.list_all():
                all_helper_names.add(entry.name)
        except Exception:
            return

        if not all_helper_names:
            return

        checked: set[str] = set()
        for step in pipeline.steps:
            helper_name = getattr(step, "helper", None)
            if not helper_name or helper_name in checked:
                continue
            checked.add(helper_name)
            entry = registry.get(helper_name)
            if entry is None or not entry.code:
                continue

            declared_imports = set(entry.imports or [])
            # Scan code for import patterns
            code = entry.code
            # Match "from <name> import ..." and "import <name>"
            import_refs: set[str] = set()
            for match in re.finditer(r'^\s*from\s+(\w+)\s+import', code, re.MULTILINE):
                import_refs.add(match.group(1))
            for match in re.finditer(r'^\s*import\s+(\w+)', code, re.MULTILINE):
                import_refs.add(match.group(1))

            for imp in import_refs:
                if imp in all_helper_names and imp not in declared_imports:
                    result.add_warning(
                        f"Step '{step.id}': helper '{helper_name}' imports '{imp}' "
                        f"which is a registered helper but not listed in helper.imports (T-BRIX-VAL-10)",
                        hint=f"Add '{imp}' to the imports field of helper '{helper_name}'.",
                    )
