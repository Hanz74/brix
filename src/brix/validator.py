"""Pipeline validation without execution."""
import difflib
import os
import re
from dataclasses import dataclass
from contextlib import contextmanager
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
from brix.bricks.contracts import is_dict_like_contract, is_list_like_contract
from brix.bricks.registry import BrickRegistry
from brix.engine import LEGACY_ALIASES
from brix.materialize import MaterializedStep, materialize_step
from brix.step_field_policy import explicit_runner_specific_fields, get_field_migration_policy
from brix.pipeline_store import PipelineStore
from brix.connections import ConnectionManager
from brix.db import BrixDB
from brix.integrity import _normalize_dynamic_values_for_schema
from brix.workaround_patterns import assess_workaround_annotation, detect_workaround_pattern_matches


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


def _load_pipeline_entity_metadata(name: str) -> dict[str, Any]:
    if not name:
        return {}
    try:
        return BrixDB().entity_metadata_get("pipeline", name) or {}
    except Exception:
        return {}


def _coerce_step(step_data: Any) -> Step | None:
    """Best-effort conversion of nested step payloads into Step models."""
    if isinstance(step_data, Step):
        return step_data
    if isinstance(step_data, dict):
        try:
            return Step.model_validate(step_data)
        except Exception:
            return None
    return None


def _collect_all_steps(pipeline: Pipeline) -> list[Step]:
    """Flatten top-level and nested choose/repeat/parallel steps in traversal order."""
    collected: list[Step] = []

    def visit(step_data: Any) -> None:
        step = _coerce_step(step_data)
        if step is None:
            return

        collected.append(step)

        for choice in step.choices or []:
            if not isinstance(choice, dict):
                continue
            for nested in choice.get("steps") or []:
                visit(nested)

        for nested in step.default_steps or []:
            visit(nested)

        for nested in step.sub_steps or []:
            visit(nested)

        for nested in step.sequence or []:
            visit(nested)

    for step in pipeline.steps:
        visit(step)

    return collected


@dataclass(frozen=True)
class StepAnalysis:
    """Normalized, shape-safe read view over one step for validation."""

    step: Step
    index: int
    materialized: MaterializedStep
    effective_type: str
    raw_params: dict[str, Any] | list[Any] | None
    raw_config: Any

    @classmethod
    def from_step(cls, step: Step, index: int = -1) -> "StepAnalysis":
        materialized = materialize_step(step)
        return cls(
            step=step,
            index=index,
            materialized=materialized,
            effective_type=materialized.effective_type,
            raw_params=materialized.raw_params,
            raw_config=materialized.raw_config,
        )

    @property
    def normalized_params(self) -> dict[str, Any]:
        params = self.materialized.effective_params
        return params if isinstance(params, dict) else {}

    @property
    def normalized_config(self) -> dict[str, Any]:
        return self.materialized.effective_config

    @property
    def params_list(self) -> list[Any]:
        params = self.materialized.effective_params
        return params if isinstance(params, list) else []

    @property
    def has_params(self) -> bool:
        return bool(self.normalized_params) or bool(self.params_list)

    @property
    def has_config(self) -> bool:
        return bool(self.normalized_config)

    @property
    def params_dict(self) -> dict[str, Any]:
        return self.normalized_params

    @property
    def config(self) -> dict[str, Any]:
        return self.normalized_config

    def param_values(self) -> list[Any]:
        if self.normalized_params:
            return list(self.normalized_params.values())
        return list(self.params_list)

    def config_values(self) -> list[Any]:
        if self.normalized_config:
            return list(self.normalized_config.values())
        return []

    def param_items(self) -> list[tuple[Any, Any]]:
        if self.normalized_params:
            return list(self.normalized_params.items())
        return list(enumerate(self.params_list))

    def config_items(self) -> list[tuple[Any, Any]]:
        if self.normalized_config:
            return list(self.normalized_config.items())
        return []

    def param_keys(self) -> set[Any]:
        if self.normalized_params:
            return set(self.normalized_params.keys())
        return set()

    def param_get(self, key: str, default: Any = None) -> Any:
        return self.normalized_params.get(key, default)

    def config_get(self, key: str, default: Any = None) -> Any:
        return self.normalized_config.get(key, default)

    def params_values(self) -> list[Any]:
        return self.param_values()

    def params_items(self) -> list[tuple[Any, Any]]:
        return self.param_items()

    def params_keys(self) -> set[Any]:
        return self.param_keys()

    def params_get(self, key: str, default: Any = None) -> Any:
        return self.param_get(key, default)


@dataclass(frozen=True)
class ValidationContext:
    """Shared per-validate() analysis context."""

    pipeline: Pipeline
    steps: tuple[StepAnalysis, ...]
    step_map: dict[str, StepAnalysis]
    known_step_ids: set[str]
    pipeline_metadata: dict[str, Any]

    @classmethod
    def from_pipeline(cls, pipeline: Pipeline) -> "ValidationContext":
        analyses = tuple(
            StepAnalysis.from_step(step, index=index)
            for index, step in enumerate(_collect_all_steps(pipeline))
        )
        return cls(
            pipeline=pipeline,
            steps=analyses,
            step_map={analysis.step.id: analysis for analysis in analyses},
            known_step_ids={analysis.step.id for analysis in analyses},
            pipeline_metadata={
                "name": pipeline.name,
                "version": pipeline.version,
                "description": pipeline.description,
                "policy_level": pipeline.policy_level,
                "input_keys": tuple(pipeline.input.keys()),
                "entity_metadata": _load_pipeline_entity_metadata(pipeline.name),
            },
        )

    def for_step(self, step: Step) -> StepAnalysis:
        return self.step_map.get(step.id) or StepAnalysis.from_step(step)

    @property
    def analyses(self) -> tuple[StepAnalysis, ...]:
        return self.steps

    @property
    def by_step_id(self) -> dict[str, StepAnalysis]:
        return self.step_map


@dataclass(frozen=True)
class ValidationFinding:
    code: str
    severity: str
    category: str
    step_id: str | None
    field: str | None
    message: str
    why: str = ""
    hint: str = ""
    suggestion: dict[str, Any] | None = None
    schema_ref: str = ""


class ValidationResult:
    _SEVERITY_ORDER = {"error": 0, "warning": 1, "info": 2}
    _CATEGORY_ORDER = {"core": 0, "schema": 1, "reference": 2, "flow": 3, "lint": 4}
    _DEFAULT_CATEGORIES = ("core", "schema", "reference", "flow", "lint")

    def __init__(self):
        self.findings: list[ValidationFinding] = []
        self.checks: list[str] = []  # successful checks
        self._current_category = "core"

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    @property
    def errors(self) -> list[str]:
        return [
            self._format_finding(finding.message, hint=finding.hint or None, schema_ref=finding.schema_ref or None)
            for finding in self.findings
            if finding.severity == "error"
        ]

    @property
    def warnings(self) -> list[str]:
        return [
            self._format_finding(finding.message, hint=finding.hint or None, schema_ref=finding.schema_ref or None)
            for finding in self.findings
            if finding.severity == "warning"
        ]

    @property
    def infos(self) -> list[str]:
        return [
            self._format_finding(finding.message, hint=finding.hint or None, schema_ref=finding.schema_ref or None)
            for finding in self.findings
            if finding.severity == "info"
        ]

    def add_check(self, msg: str):
        self.checks.append(msg)

    def add_finding(
        self,
        *,
        code: str,
        severity: str,
        message: str,
        category: str | None = None,
        step_id: str | None = None,
        field: str | None = None,
        why: str = "",
        hint: str = "",
        suggestion: dict[str, Any] | None = None,
        schema_ref: str = "",
    ) -> None:
        self.findings.append(
            ValidationFinding(
                code=code,
                severity=severity,
                category=category or self._current_category,
                step_id=step_id,
                field=field,
                message=message,
                why=why,
                hint=hint,
                suggestion=suggestion,
                schema_ref=schema_ref,
            )
        )

    @contextmanager
    def category_scope(self, category: str):
        previous = self._current_category
        self._current_category = category
        try:
            yield
        finally:
            self._current_category = previous

    def sorted_findings(self) -> list[ValidationFinding]:
        return sorted(
            self.findings,
            key=lambda finding: (
                self._SEVERITY_ORDER.get(finding.severity, 99),
                self._CATEGORY_ORDER.get(finding.category, 99),
                finding.step_id or "",
                finding.field or "",
                finding.code,
                finding.message,
            ),
        )

    def summary(self) -> dict[str, int]:
        errors = sum(1 for finding in self.findings if finding.severity == "error")
        warnings = sum(1 for finding in self.findings if finding.severity == "warning")
        infos = sum(1 for finding in self.findings if finding.severity == "info")
        return {
            "errors": errors,
            "warnings": warnings,
            "infos": infos,
            "total": errors + warnings + infos,
        }

    @staticmethod
    def _serialize_finding(finding: ValidationFinding) -> dict[str, Any]:
        data: dict[str, Any] = {
            "code": finding.code,
            "severity": finding.severity,
            "category": finding.category,
            "message": finding.message,
        }
        if finding.step_id is not None:
            data["step_id"] = finding.step_id
        if finding.field is not None:
            data["field"] = finding.field
        if finding.why:
            data["why"] = finding.why
        if finding.hint:
            data["hint"] = finding.hint
        if finding.suggestion is not None:
            data["suggestion"] = finding.suggestion
        if finding.schema_ref:
            data["schema_ref"] = finding.schema_ref
        return data

    def findings_by_category(self) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {
            category: [] for category in self._DEFAULT_CATEGORIES
        }
        for finding in self.sorted_findings():
            grouped.setdefault(finding.category, []).append(self._serialize_finding(finding))
        return grouped

    @staticmethod
    def _finding_to_next_action(finding: ValidationFinding) -> str:
        target = f" in step '{finding.step_id}'" if finding.step_id else ""
        if finding.hint:
            hint = finding.hint.rstrip(".")
            if hint.lower().startswith("fix "):
                return f"{hint}{target}"
            return f"{hint}{target}"
        return f"Fix {finding.message.rstrip('.')}{target}"

    def next_actions(self, limit: int = 3) -> list[str]:
        actions: list[str] = []
        seen: set[str] = set()
        for finding in self.sorted_findings():
            action = self._finding_to_next_action(finding)
            if action in seen:
                continue
            seen.add(action)
            actions.append(action)
            if len(actions) >= limit:
                break
        return actions

    def to_structured_payload(self) -> dict[str, Any]:
        return {
            "valid": self.is_valid,
            "summary": self.summary(),
            "next_actions": self.next_actions(),
            "findings": [self._serialize_finding(finding) for finding in self.sorted_findings()],
            "findings_by_category": self.findings_by_category(),
            "errors": self.errors,
            "warnings": self.warnings,
            "infos": self.infos,
            "checks": self.checks,
        }

    def add_info(
        self,
        msg: str,
        hint: str | None = None,
        schema_ref: str | None = None,
        *,
        code: str = "INFO",
        step_id: str | None = None,
        field: str | None = None,
        why: str = "",
        suggestion: dict[str, Any] | None = None,
    ) -> None:
        self.add_finding(
            code=code,
            severity="info",
            message=msg,
            step_id=step_id,
            field=field,
            why=why,
            hint=hint or "",
            suggestion=suggestion,
            schema_ref=schema_ref or "",
        )

    @staticmethod
    def _format_finding(msg: str, hint: str | None = None, schema_ref: str | None = None) -> str:
        parts = [msg]
        if hint:
            parts.append(f"Hint: {hint}")
        if schema_ref:
            parts.append(f"Schema: {schema_ref}")
        return " | ".join(parts)

    def add_error(
        self,
        msg: str,
        hint: str | None = None,
        schema_ref: str | None = None,
        *,
        code: str = "ERROR",
        step_id: str | None = None,
        field: str | None = None,
        why: str = "",
        suggestion: dict[str, Any] | None = None,
    ) -> None:
        self.add_finding(
            code=code,
            severity="error",
            message=msg,
            step_id=step_id,
            field=field,
            why=why,
            hint=hint or "",
            suggestion=suggestion,
            schema_ref=schema_ref or "",
        )

    def add_warning(
        self,
        msg: str,
        hint: str | None = None,
        schema_ref: str | None = None,
        *,
        code: str = "WARNING",
        step_id: str | None = None,
        field: str | None = None,
        why: str = "",
        suggestion: dict[str, Any] | None = None,
    ) -> None:
        self.add_finding(
            code=code,
            severity="warning",
            message=msg,
            step_id=step_id,
            field=field,
            why=why,
            hint=hint or "",
            suggestion=suggestion,
            schema_ref=schema_ref or "",
        )


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
            return step_or_analysis.param_values()
        return StepAnalysis.from_step(step_or_analysis).param_values()

    @staticmethod
    def _params_items(step_or_analysis: Step | StepAnalysis) -> list[tuple[Any, Any]]:
        if isinstance(step_or_analysis, StepAnalysis):
            return step_or_analysis.param_items()
        return StepAnalysis.from_step(step_or_analysis).param_items()

    @staticmethod
    def _params_keys(step_or_analysis: Step | StepAnalysis) -> set[Any]:
        if isinstance(step_or_analysis, StepAnalysis):
            return step_or_analysis.param_keys()
        return StepAnalysis.from_step(step_or_analysis).param_keys()

    @staticmethod
    def _params_get(params_or_analysis: Any, key: str, default: Any = None) -> Any:
        if isinstance(params_or_analysis, StepAnalysis):
            return params_or_analysis.param_get(key, default)
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

        ctx = ValidationContext.from_pipeline(pipeline)
        result = ValidationResult()
        self._validation_ctx = ctx
        try:
            with result.category_scope("core"):
                self.run_core_checks(ctx, result, pipeline_dir=pipeline_dir)
            if level in {"standard", "deep"}:
                with result.category_scope("schema"):
                    self.run_schema_checks(ctx, result)
                with result.category_scope("reference"):
                    self.run_reference_checks(ctx, result)
                with result.category_scope("flow"):
                    self.run_flow_checks(ctx, result)
                with result.category_scope("lint"):
                    self.run_lint_checks(ctx, result)
            if level == "deep":
                with result.category_scope("reference"):
                    self.run_deep_checks(ctx, result)

            if result.is_valid:
                result.add_check("Pipeline is valid")
            return result
        finally:
            self._validation_ctx = None

    def run_core_checks(
        self,
        ctx: ValidationContext,
        result: ValidationResult,
        pipeline_dir: Path | None = None,
    ) -> None:
        """Run fast structural checks: IDs, references, ordering, and conditions."""
        step_ids = [analysis.step.id for analysis in ctx.steps]
        if len(step_ids) != len(set(step_ids)):
            result.add_error(
                "Duplicate step IDs found",
                code="DUPLICATE_STEP_ID",
                field="steps[].id",
                why="Step IDs must be unique so references resolve unambiguously.",
            )
        else:
            result.add_check("Step IDs are unique")

        for analysis in ctx.steps:
            self._check_step_references(ctx, analysis, result)

        if pipeline_dir:
            for analysis in ctx.steps:
                step = analysis.step
                if analysis.effective_type == "script.python" and step.script:
                    script_path = pipeline_dir / step.script
                    if not script_path.exists():
                        if not Path(step.script).exists():
                            result.add_error(
                                f"Step '{step.id}': Script not found: {step.script}"
                            )
                        else:
                            result.add_check(f"Step '{step.id}': Script exists")
                    else:
                        result.add_check(f"Step '{step.id}': Script exists")

        for analysis in ctx.steps:
            if analysis.step.when:
                self._check_when_default(ctx, analysis, result)

        if ctx.pipeline.output:
            input_keys = set(ctx.pipeline.input.keys())
            for key, ref in ctx.pipeline.output.items():
                for step_id in step_ids:
                    if step_id in ref:
                        break
                else:
                    if "{{" in ref:
                        refs = re.findall(r'\{\{\s*(\w+)\.', str(ref))
                        if any(r == "input" or r in input_keys for r in refs):
                            pass
                        else:
                            result.add_warning(
                                f"Output '{key}': may reference non-existent step"
                            )

        for analysis in ctx.steps:
            step = analysis.step
            if step.when and step.else_of:
                result.add_warning(
                    f"Step '{step.id}': has both 'when' and 'else_of' — these are mutually exclusive. "
                    f"'else_of' already implies a condition (runs only when the referenced step was skipped)."
                )

    def run_schema_checks(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Run schema and config-shape validation checks."""
        for analysis in ctx.steps:
            if analysis.effective_type == "mcp.call" and analysis.step.server and analysis.step.tool:
                self._check_mcp_params(ctx, analysis, result)

        self._check_schema_contracts(ctx, result)
        self._check_config_param_misplacement(ctx, result)
        self._check_config_toplevel_conflicts(ctx, result)
        self._check_brick_config_schema(ctx, result)

    def run_reference_checks(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Run validations for helpers, connections, sub-pipelines, and credentials."""
        for analysis in ctx.steps:
            step = analysis.step
            if analysis.effective_type == "mcp.call":
                if not step.server:
                    result.add_error(f"Step '{step.id}': MCP step needs 'server'")
                if not step.tool:
                    result.add_error(f"Step '{step.id}': MCP step needs 'tool'")
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
                if step.server and step.tool:
                    cached_tools = self.cache.get_tool_names(step.server)
                    if cached_tools and step.tool not in cached_tools:
                        result.add_warning(
                            f"Step '{step.id}': Tool '{step.tool}' not in cached schema for '{step.server}'"
                        )

        for key, cred in ctx.pipeline.credentials.items():
            self._check_credential(key, cred.env, result)

        for analysis in ctx.steps:
            if getattr(analysis.step, "helper", None):
                self._check_helper_reference(ctx, analysis, result)

        self._check_jinja_ast(ctx, result)
        self._check_sub_pipeline_existence(ctx, result)
        self._check_connection_existence(ctx, result)
        self._check_helper_without_code(ctx, result)
        self._check_large_helper_without_schema(ctx, result)
        self._check_cross_helper_imports(ctx, result)

    def run_flow_checks(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Run data-flow and nested-pipeline validation checks."""
        if ctx.pipeline.requirements:
            from brix.deps import check_requirements

            missing = check_requirements(ctx.pipeline.requirements)
            if missing:
                for req in missing:
                    result.add_warning(
                        f"Requirement '{req}' is not installed — will be auto-installed at runtime"
                    )
            else:
                result.add_check(
                    f"All {len(ctx.pipeline.requirements)} requirement(s) installed"
                )

        self._check_missing_output_in_refs(ctx, result)
        self._check_unused_steps(ctx, result)
        self._check_sub_pipeline_output_mismatch(ctx, result)
        self._check_foreach_on_non_list(ctx, result)
        self._check_duplicate_ids_across_sub_pipelines(ctx, result)
        self._check_step_output_type_compatibility(ctx, result)

    def run_lint_checks(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Run lint-style warnings and performance hints."""
        for analysis in ctx.steps:
            step = analysis.step
            if step.on_error == "continue" and analysis.effective_type in ("http.request", "mcp.call"):
                result.add_warning(
                    f"Step '{step.id}': on_error: continue on a {analysis.effective_type} step — "
                    f"consider on_error: retry for transient errors."
                )

        self._run_lint_rules(ctx, result)
        self._check_runner_specific_top_level_fields(ctx, result)
        self._check_deprecated_step_types(ctx, result)
        self._check_tojson_on_string(ctx, result)
        self._check_db_query_dml(ctx, result)
        self._check_workaround_patterns(ctx, result)
        self._check_workaround_annotation(ctx, result)

    def run_deep_checks(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Run expensive deep validation checks."""
        self._check_connection_health(ctx, result)

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

    def _check_step_references(self, ctx: ValidationContext, step_or_analysis, result):
        """Check that step references point to earlier steps."""
        analysis = step_or_analysis if isinstance(step_or_analysis, StepAnalysis) else ctx.for_step(step_or_analysis)
        step = analysis.step
        step_ids = [item.step.id for item in ctx.steps]
        step_idx = analysis.index if analysis.index >= 0 else next(i for i, s in enumerate(ctx.steps) if s.step.id == step.id)
        earlier_ids = set(step_ids[:step_idx])
        input_keys = set(ctx.pipeline.input.keys())

        # Check foreach, params, when, etc. for {{ step_id.output }} references
        fields_to_check = [step.foreach]
        if analysis.has_params:
            fields_to_check.extend(str(v) for v in analysis.param_values())
        if step.when:
            fields_to_check.append(step.when)

        for field in fields_to_check:
            if field and "{{" in str(field):
                # Extract referenced step IDs
                refs = re.findall(r'\{\{\s*(\w+)\.output', str(field))
                for ref in refs:
                    if ref not in earlier_ids and ref != "input" and ref not in input_keys:
                        if ref in ctx.known_step_ids:
                            result.add_error(
                                f"Step '{step.id}' references future step '{ref}'",
                                hint=f"Only earlier step IDs are available here: {sorted(earlier_ids)}",
                                code="FUTURE_STEP_REF",
                                step_id=step.id,
                                field="template_ref",
                                why="Templates can only reference outputs from earlier steps.",
                            )
                        elif ref not in ["item", "credentials"]:
                            result.add_warning(
                                f"Step '{step.id}' references unknown '{ref}'",
                                hint=f"Available step IDs: {sorted(ctx.known_step_ids)}",
                                code="UNKNOWN_STEP_REF",
                                step_id=step.id,
                                field="template_ref",
                                why="The reference root does not match any known step ID or built-in name.",
                            )

    def _check_when_default(self, ctx: ValidationContext, when_step_or_analysis, result):
        """Warn if a conditional step is referenced without | default."""
        when_analysis = (
            when_step_or_analysis
            if isinstance(when_step_or_analysis, StepAnalysis)
            else ctx.for_step(when_step_or_analysis)
        )
        when_step = when_analysis.step
        for analysis in ctx.steps:
            step = analysis.step
            if step.id == when_step.id:
                continue
            guarded_by_when = (
                isinstance(step.when, str)
                and when_step.id in step.when
                and "output" in step.when
                and "defined" in step.when
            )
            fields_to_check = []
            if analysis.has_params:
                fields_to_check.extend(str(v) for v in analysis.param_values())
            if step.foreach:
                fields_to_check.append(step.foreach)

            for field in fields_to_check:
                if when_step.id in str(field) and "output" in str(field):
                    if "default" not in str(field) and not guarded_by_when:
                        result.add_warning(
                            f"Step '{step.id}' references conditional step '{when_step.id}' "
                            f"without | default() — may fail if skipped (D-16)",
                            hint=f"Use something like {{ {when_step.id}.output | default(...) }} when '{when_step.id}' may be skipped.",
                            code="CONDITIONAL_REF_NO_DEFAULT",
                            step_id=step.id,
                            field="template_ref",
                            why="Conditional steps may be skipped, so their output can be undefined.",
                        )

    def _check_helper_reference(self, ctx: ValidationContext, step_or_analysis, result) -> None:
        """Validate a step's ``helper`` field against the HelperRegistry (T-BRIX-V4-BUG-12).

        Checks:
        - The referenced helper exists in the registry.
        - If the helper declares an ``input_schema``, warn about step params that
          do not appear in the schema (schema mismatch) — skips Jinja2 templates.
        """
        from brix.helper_registry import HelperRegistry
        analysis = step_or_analysis if isinstance(step_or_analysis, StepAnalysis) else ctx.for_step(step_or_analysis)
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
        try:
            from brix.helper_governance import assess_helper_governance

            governance = assess_helper_governance(entry.to_dict())
            if not governance.is_complete:
                result.add_warning(
                    f"Step '{step.id}': helper '{step.helper}' governance is incomplete "
                    f"(status={governance.status}) (T-2.2.3)",
                    hint="Update helper metadata with project, tags, schemas, and reason_not_a_brick or brick_candidate_ref.",
                    code="HELPER_GOVERNANCE_INCOMPLETE",
                    step_id=step.id,
                    field="helper",
                    why="Helpers must be explicit brick exceptions or migration candidates.",
                    suggestion={
                        "kind": "update_helper",
                        "missing_metadata": list(governance.missing_metadata),
                        "missing_justification": governance.missing_justification,
                    },
                )
        except Exception:
            pass

        # Schema validation — warn on params not declared in input_schema
        input_schema = entry.input_schema or {}
        schema_properties = input_schema.get("properties", {})
        if schema_properties and analysis.has_params:
            for param_key, param_val in analysis.param_items():
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

    def _check_mcp_params(self, ctx: ValidationContext, step_or_analysis, result):
        """Warn if required MCP tool params are not supplied in the step definition (T-BRIX-V4-21).

        Looks up the cached tool schema and checks whether any schema-required
        params are missing from the step's params dict.  Params that use Jinja2
        templates (``{{ ... }}``) are considered dynamically supplied and are
        not flagged.
        """
        analysis = step_or_analysis if isinstance(step_or_analysis, StepAnalysis) else ctx.for_step(step_or_analysis)
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

        provided_keys = analysis.param_keys() if analysis.has_params else set()
        for req_key in schema_required:
            if req_key not in provided_keys:
                    result.add_warning(
                        f"Step '{step.id}': MCP tool '{step.tool}' requires param '{req_key}' "
                        f"but it is not set in step params (T-BRIX-V4-21)",
                        hint=f"Add '{req_key}' to step params or make it optional in the tool schema.",
                        schema_ref=self._schema_ref("mcp.call"),
                        code="MCP_REQUIRED_PARAM_MISSING",
                        step_id=step.id,
                        field=f"params.{req_key}",
                        why="The cached MCP tool schema marks this parameter as required.",
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
        registry = BrickRegistry(db=BrixDB())
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

    def _check_brick_config_schema(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Validate step config against brick or runner JSON schema."""
        for analysis in ctx.steps:
            step = analysis.step
            schema = self._resolve_step_schema(analysis)
            if not schema:
                continue
            instance = self._step_to_validation_config(step)
            instance = _normalize_dynamic_values_for_schema(instance, schema)
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
                        code="SCHEMA_REQUIRED_FIELD_MISSING",
                        step_id=step.id,
                        field=missing_field,
                        why="The step config does not satisfy the schema's required fields.",
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
                        code="SCHEMA_TYPE_MISMATCH",
                        step_id=step.id,
                        field=field_name,
                        why="The supplied value type does not match the declared schema type.",
                    )
                    continue

                result.add_warning(
                    f"Step '{step.id}': config does not match schema: {exc.message}",
                    hint="Align the step fields with the brick schema.",
                    schema_ref=schema_ref,
                    code="SCHEMA_VALIDATION_FAILED",
                    step_id=step.id,
                    field=str(exc.path[-1]) if exc.path else None,
                    why="JSON Schema validation reported a config mismatch.",
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

    def _check_jinja_ast(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Parse Jinja templates and warn on unknown root names."""
        env = jinja2.Environment()
        step_ids = [analysis.step.id for analysis in ctx.steps]
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

        for analysis in ctx.steps:
            step = analysis.step
            templates: list[tuple[str, str]] = []
            templates.extend(self._collect_template_strings(analysis.raw_params, "params"))
            templates.extend(self._collect_template_strings(analysis.raw_config, "config"))
            if step.when:
                templates.extend(self._collect_template_strings(step.when, "when"))
            if step.foreach:
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

    def _check_deprecated_step_types(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Escalate legacy flat step types according to pipeline policy."""
        policy_level = self._effective_policy_level(ctx.pipeline)
        for analysis in ctx.steps:
            step = analysis.step
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

        registry = BrickRegistry(db=BrixDB())
        for analysis in ctx.steps:
            step = analysis.step
            effective_type = analysis.effective_type
            if "{{" in effective_type:
                continue
            if LEGACY_ALIASES.get(step.type):
                continue
            brick = registry.get(effective_type)
            if brick is None:
                result.add_error(
                    f'Step "{step.id}": type "{step.type}" is not allowed under locked policy.',
                    hint="Use a registered dot-notation brick type from list_bricks().",
                    schema_ref=self._schema_ref(effective_type),
                )

    def _check_config_param_misplacement(self, ctx: ValidationContext, result: ValidationResult) -> None:
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
        for analysis in ctx.steps:
            step = analysis.step
            effective_type = analysis.effective_type
            candidate_fields = field_map.get(effective_type)
            if not candidate_fields:
                continue
            params = analysis.params_dict
            config = analysis.normalized_config
            if not params:
                continue
            for field in candidate_fields:
                if field in params and field not in config:
                    result.add_warning(
                        f'Step "{step.id}": "{field}" found in params but should be in config.',
                        hint=f'{self._schema_ref(effective_type)} shows config structure.' if effective_type else "Move the field into config.",
                        schema_ref=self._schema_ref(effective_type),
                    )

    def _check_config_toplevel_conflicts(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when config and top-level step fields disagree after merge semantics."""
        for analysis in ctx.steps:
            step = analysis.step
            config = analysis.normalized_config
            if not config:
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
                    schema_ref=self._schema_ref(analysis.effective_type),
                )

    @staticmethod
    def _is_dynamic_ref(value: Any) -> bool:
        return isinstance(value, str) and any(token in value for token in ("{{", "{%", "{#"))

    def _check_sub_pipeline_existence(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Ensure statically referenced sub-pipelines exist."""
        store = PipelineStore()
        for analysis in ctx.steps:
            step = analysis.step
            if analysis.effective_type != "flow.pipeline":
                continue
            pipeline_name = (
                getattr(step, "pipeline", None)
                or analysis.param_get("pipeline")
                or analysis.config_get("pipeline")
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

    def _collect_connection_refs(self, ctx: ValidationContext) -> list[tuple[str, str]]:
        refs: list[tuple[str, str]] = []
        for analysis in ctx.steps:
            step = analysis.step
            connection_name = (
                getattr(step, "connection", None)
                or analysis.param_get("connection")
                or analysis.config_get("connection")
            )
            if not connection_name or self._is_dynamic_ref(connection_name):
                continue
            refs.append((step.id, connection_name))
        return refs

    def _check_connection_existence(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Ensure statically referenced named connections exist."""
        refs = self._collect_connection_refs(ctx)
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

    def _check_connection_health(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Active connection ping for deep validation mode."""
        refs = self._collect_connection_refs(ctx)
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

    def _check_schema_contracts(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Check inter-step schema compatibility (T-BRIX-V6-13).

        When step A has output_schema and step B has input_schema AND B references
        A's output, verify that the fields declared in B's input_schema are a subset
        of the fields declared in A's output_schema.
        """
        # Build a map from step_id → output_schema
        output_schemas: dict[str, dict] = {}
        for analysis in ctx.steps:
            step = analysis.step
            schema = getattr(step, "output_schema", None) or {}
            if schema:
                output_schemas[step.id] = schema

        if not output_schemas:
            return  # Nothing to check

        # For each step with input_schema, find which earlier step it references
        for analysis in ctx.steps:
            step = analysis.step
            if not getattr(step, "input_schema", None):
                continue

            # Collect all step IDs referenced in this step's params/foreach
            referenced_ids: set[str] = set()
            fields_to_scan = []
            if analysis.has_params:
                fields_to_scan.extend(str(v) for v in analysis.param_values())
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

    def _run_lint_rules(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Apply configurable linting rules to the pipeline (T-BRIX-V6-16)."""
        rules = self._load_lint_rules()
        for rule in rules:
            check = rule.get("check")
            if check == "max_concurrency":
                self._lint_max_concurrency(ctx, rule, result)
            elif check == "no_base64_foreach":
                self._lint_no_base64_foreach(ctx, rule, result)
            elif check == "progress_on_long_timeout":
                self._lint_progress_on_long_timeout(ctx, rule, result)
            # Custom / unknown rules are silently skipped

    def _lint_max_concurrency(self, ctx: ValidationContext, rule: dict, result: ValidationResult) -> None:
        """Warn when a step of the specified type exceeds max concurrency."""
        target_type = LEGACY_ALIASES.get(rule.get("type"), rule.get("type"))
        max_conc = rule.get("max", 5)
        for analysis in ctx.steps:
            step = analysis.step
            if target_type and analysis.effective_type != target_type:
                continue
            if step.parallel and step.concurrency > max_conc:
                result.add_warning(
                    f"Step '{step.id}': concurrency {step.concurrency} exceeds "
                    f"recommended max {max_conc} for {analysis.effective_type} steps "
                    f"[lint:{rule.get('id', 'max-concurrency')}]"
                )

    def _lint_no_base64_foreach(self, ctx: ValidationContext, rule: dict, result: ValidationResult) -> None:
        """Warn when a foreach step's params contain 'base64' — OOM risk."""
        for analysis in ctx.steps:
            step = analysis.step
            if not step.foreach:
                continue
            if analysis.has_params:
                for k, v in analysis.param_items():
                    if "base64" in str(k).lower() or "base64" in str(v).lower():
                        result.add_warning(
                            f"Step '{step.id}': param '{k}' contains 'base64' in a foreach step "
                            f"— large base64 payloads in foreach loops can cause OOM "
                            f"[lint:{rule.get('id', 'no-base64-foreach')}]"
                        )

    def _lint_progress_on_long_timeout(self, ctx: ValidationContext, rule: dict, result: ValidationResult) -> None:
        """Warn when a step has a long timeout but progress:false.

        MCP steps calling external servers are excluded — progress:true only makes
        sense for runners that natively emit progress events (python, cli, http).
        """
        threshold = rule.get("timeout_threshold_seconds", 60)
        # Runners that do not support progress events
        _PROGRESS_UNSUPPORTED = {"mcp.call"}
        for analysis in ctx.steps:
            step = analysis.step
            if not step.timeout:
                continue
            # Skip MCP steps — external servers don't support Brix progress events
            if analysis.effective_type in _PROGRESS_UNSUPPORTED:
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

    def _build_step_output_type_map(self, ctx: ValidationContext) -> dict[str, str]:
        """Resolve output types for all steps using BrickRegistry with known fallbacks."""
        known_output_types = {
            "db.query": "list[dict]",
            "db.exec": "dict",
            "db.upsert": "dict",
            "flow.filter": "list",
            "flow.transform": "any",
            "flow.flatten": "list",
            "flow.merge": "list",
            "flow.dedup": "list",
            "flow.aggregate": "dict",
            "flow.set": "dict",
            "mcp.call": "any",
            "script.python": "any",
            "http.request": "any",
            "source.fetch": "list",
            "flow.pipeline": "any",
        }
        step_output_types: dict[str, str] = {}
        try:
            registry = BrickRegistry(db=BrixDB())
        except Exception:
            registry = None

        for analysis in ctx.steps:
            step = analysis.step
            effective_type = analysis.effective_type
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
        if is_list_like_contract(norm):
            return True
        if norm.startswith("list[") or norm == "list":
            return True
        return False

    def _is_expected_dict_type(self, output_type: str) -> bool:
        norm = self._normalise_output_type(output_type)
        if not norm or norm in {"any", "*"}:
            return True
        if is_dict_like_contract(norm):
            return True
        return norm in {"dict", "object", "json"}

    def _is_single_dict_type(self, output_type: str) -> bool:
        norm = self._normalise_output_type(output_type)
        return norm in {"dict", "object", "json"} or is_dict_like_contract(norm)

    def _is_list_of_dict_type(self, output_type: str) -> bool:
        norm = self._normalise_output_type(output_type)
        return norm == "list[dict]" or is_list_like_contract(norm)

    def _iter_step_output_type_compatibility_issues(self, ctx: ValidationContext) -> list[dict[str, str]]:
        """Collect output type mismatches for step-to-step references."""
        step_output_types = self._build_step_output_type_map(ctx)
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
                    if self._is_list_of_dict_type(output_type):
                        continue
                elif expected_type == "list of lists":
                    if self._is_expected_list_type(output_type):
                        continue
                add_issue(
                    consumer_step.id,
                    ref_id,
                    context_name,
                    expected_type,
                    output_type,
                    hint_builder(ref_id, output_type),
                )

        for analysis in ctx.steps:
            step = analysis.step
            effective_type = analysis.effective_type

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
                    analysis.param_get("data"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.upsert data expects list[dict]. "
                        f"Did you mean {{{{ {ref_id}.output.rows }}}} or wrap the dict in a one-item list first?"
                    ),
                )
                check_template_ref(
                    step,
                    "db.upsert data",
                    "list[dict]",
                    analysis.config_get("data"),
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
                    analysis.param_get("params"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.exec params expects list. "
                        f"Wrap the values in a positional list or map them with flow.transform first."
                    ),
                )
                check_template_ref(
                    step,
                    "db.exec params",
                    "list",
                    analysis.config_get("params"),
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
                    analysis.param_get("params"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but db.query params expects dict. "
                        f"Did you mean {{{{ {ref_id}.output[0] }}}} or map the fields with flow.set first?"
                    ),
                )
                check_template_ref(
                    step,
                    "db.query params",
                    "dict",
                    analysis.config_get("params"),
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
                    analysis.param_get("input"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but flow.filter input expects list. "
                        f"Did you mean {{{{ {ref_id}.output.rows }}}} or use flow.flatten first?"
                    ),
                )
                check_template_ref(
                    step,
                    "flow.filter input",
                    "list",
                    analysis.config_get("input"),
                    lambda ref_id, output_type: (
                        f"Step '{ref_id}' output is {output_type} but flow.filter input expects list. "
                        f"Did you mean {{{{ {ref_id}.output.rows }}}} or use flow.flatten first?"
                    ),
                )

            if effective_type == "flow.merge":
                input_refs: list[str] = []
                if isinstance(step.inputs, list):
                    for input_value in step.inputs:
                        if not isinstance(input_value, str):
                            continue
                        if "{{" in input_value:
                            input_refs.extend(self._extract_step_output_refs(input_value))
                        else:
                            input_refs.append(input_value)
                params_inputs = analysis.param_get("inputs")
                if isinstance(params_inputs, list):
                    for input_value in params_inputs:
                        if not isinstance(input_value, str):
                            continue
                        if "{{" in input_value:
                            input_refs.extend(self._extract_step_output_refs(input_value))
                        else:
                            input_refs.append(input_value)
                config_inputs = analysis.config_get("inputs")
                if isinstance(config_inputs, list):
                    for input_value in config_inputs:
                        if not isinstance(input_value, str):
                            continue
                        if "{{" in input_value:
                            input_refs.extend(self._extract_step_output_refs(input_value))
                        else:
                            input_refs.append(input_value)
                for ref_id in input_refs:
                    output_type = step_output_types.get(ref_id, "")
                    if not output_type or self._is_expected_list_type(output_type):
                        continue
                    add_issue(
                        step.id,
                        ref_id,
                        "flow.merge inputs",
                        "list of lists",
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

    def _check_missing_output_in_refs(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when {{ step_id.field }} is used but .output is missing."""
        step_ids = ctx.known_step_ids
        # Built-in root names that are NOT step IDs — skip these
        _BUILTINS = {"input", "item", "credentials", "var", "store", "env", "loop"}

        for analysis in ctx.steps:
            step = analysis.step
            templates = self._collect_template_strings(analysis.raw_params, "params")
            templates.extend(self._collect_template_strings(analysis.raw_config, "config"))
            if step.when:
                templates.extend(self._collect_template_strings(step.when, "when"))
            if step.foreach:
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
                            hint=f"Change {{{{ {root_name}.{attr} }}}} to {{{{ {root_name}.output.{attr} }}}}",
                            code="MISSING_OUTPUT_REF",
                            step_id=step.id,
                            field=field_path,
                            why="Step outputs are wrapped in .output layer",
                            suggestion={
                                "kind": "rewrite",
                                "example": f"{{{{ {root_name}.output.{attr} }}}}",
                            },
                        )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-02: tojson on already-string values
    # ---------------------------------------------------------------------------

    def _check_tojson_on_string(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when | tojson is applied to a value known to be a string."""
        # Build a map of step_id → output_type from brick registry
        string_output_steps: set[str] = set()
        try:
            registry = BrickRegistry(db=BrixDB())
            for analysis in ctx.steps:
                step = analysis.step
                effective_type = analysis.effective_type
                brick = registry.get(effective_type)
                if brick and getattr(brick, "output_type", "") == "string":
                    string_output_steps.add(step.id)
        except Exception:
            return  # Can't determine types — skip

        if not string_output_steps:
            return

        for analysis in ctx.steps:
            step = analysis.step
            templates = self._collect_template_strings(analysis.raw_params, "params")
            templates.extend(self._collect_template_strings(analysis.raw_config, "config"))

            for field_path, tmpl in templates:
                # Look for {{ step_id.output ... | tojson }}
                for match in re.finditer(r'\{\{.*?(\w+)\.output.*?\|\s*tojson.*?\}\}', tmpl):
                    ref_id = match.group(1)
                    if ref_id in string_output_steps:
                        result.add_warning(
                            f"Step '{step.id}': applies | tojson to '{ref_id}.output' "
                            f"which is already a string type (T-BRIX-VAL-02)",
                            hint="| tojson on a string adds extra quotes — remove it if the value is already a string.",
                            code="TOJSON_ON_STRING",
                            step_id=step.id,
                            field=field_path,
                            why="Serializing an already-string value usually adds unwanted quoting.",
                        )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-03: Helper without code
    # ---------------------------------------------------------------------------

    def _check_helper_without_code(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when a step references a helper that exists but has no code."""
        from brix.helper_registry import HelperRegistry
        registry = HelperRegistry()

        for analysis in ctx.steps:
            step = analysis.step
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

    def _check_unused_steps(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Info-level hint for steps not referenced by any other step and without side effects."""
        _SIDE_EFFECT_TYPES = {
            "db.upsert", "db.exec", "action.notify", "action.emit",
            "action.queue", "action.approval", "action.respond",
            "script.python", "script.cli", "mcp.call",
            # Legacy aliases
            "python", "cli", "mcp", "notify", "exec",
        }

        # Collect all step IDs referenced in templates of other steps
        referenced_ids: set[str] = set()
        for analysis in ctx.steps:
            step = analysis.step
            templates = self._collect_template_strings(analysis.raw_params)
            templates.extend(self._collect_template_strings(analysis.raw_config))
            if step.when:
                templates.extend(self._collect_template_strings(step.when))
            if step.foreach:
                templates.extend(self._collect_template_strings(step.foreach))
            if step.else_of:
                referenced_ids.add(step.else_of)

            for _, tmpl in templates:
                for match in re.finditer(r'\{\{\s*(\w+)\.', tmpl):
                    referenced_ids.add(match.group(1))

        # Also consider output references
        if ctx.pipeline.output:
            for _, ref in ctx.pipeline.output.items():
                for match in re.finditer(r'\{\{\s*(\w+)\.', str(ref)):
                    referenced_ids.add(match.group(1))

        # Last step is implicitly used (pipeline result)
        last_step_id = ctx.pipeline.steps[-1].id if ctx.pipeline.steps else None

        for analysis in ctx.steps:
            step = analysis.step
            if step.id == last_step_id:
                continue
            if step.id in referenced_ids:
                continue
            effective_type = analysis.effective_type
            if effective_type in _SIDE_EFFECT_TYPES:
                continue
            result.add_info(
                f"Step '{step.id}': appears unused — not referenced by any other step "
                f"and has no side effects (T-BRIX-VAL-04)",
                hint="Remove the step or reference its output in a downstream step.",
                code="UNUSED_STEP",
                step_id=step.id,
                field="id",
                why="The step is neither referenced nor known to cause side effects.",
            )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-05: Output-schema mismatch between sub-pipeline and parent
    # ---------------------------------------------------------------------------

    def _check_sub_pipeline_output_mismatch(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when parent references keys not in sub-pipeline output."""
        store = PipelineStore()
        sub_pipeline_outputs: dict[str, set[str]] = {}  # step_id → output keys

        for analysis in ctx.steps:
            step = analysis.step
            if analysis.effective_type != "flow.pipeline":
                continue
            pipeline_name = (
                getattr(step, "pipeline", None)
                or analysis.param_get("pipeline")
                or analysis.config_get("pipeline")
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
        for analysis in ctx.steps:
            step = analysis.step
            templates = self._collect_template_strings(analysis.raw_params)
            templates.extend(self._collect_template_strings(analysis.raw_config))
            if step.foreach:
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
                                hint="Check sub-pipeline output: section",
                                code="SUB_PIPELINE_OUTPUT_MISMATCH",
                                step_id=step.id,
                                field="template_ref",
                                why="Parent references keys not in sub-pipeline output",
                            )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-06: foreach on non-list expression
    # ---------------------------------------------------------------------------

    def _check_foreach_on_non_list(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when foreach references a step whose output type is not list-like."""
        for issue in self._iter_step_output_type_compatibility_issues(ctx):
            if issue["context_name"] != "foreach":
                continue
            result.add_warning(
                f"Step '{issue['consumer_step_id']}': foreach references '{issue['source_step_id']}' "
                f"whose output_type is '{issue['actual_type']}' — foreach expects a list (T-BRIX-VAL-06)",
                hint="Use {{ step.output.rows }} or flow.flatten",
                code="FOREACH_ON_NON_LIST",
                step_id=issue["consumer_step_id"],
                field="foreach",
                why=f"foreach expects list, got {issue['actual_type']}",
            )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-11: Output type compatibility between referenced steps
    # ---------------------------------------------------------------------------

    def _check_step_output_type_compatibility(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when a step consumes another step's output with an incompatible type."""
        for issue in self._iter_step_output_type_compatibility_issues(ctx):
            if issue["context_name"] == "foreach":
                continue
            result.add_warning(
                f"Step '{issue['consumer_step_id']}': {issue['context_name']} references "
                f"'{issue['source_step_id']}.output' whose output_type is '{issue['actual_type']}' "
                f"but expects '{issue['expected_type']}' (T-BRIX-VAL-11)",
                hint="Add flow.flatten or change source",
                code="STEP_OUTPUT_TYPE_MISMATCH",
                step_id=issue["consumer_step_id"],
                field=issue["context_name"],
                why="Source output type incompatible with consumer",
            )

    # ---------------------------------------------------------------------------
    # T-2.1.2: Runner-specific top-level fields are compatibility inputs
    # ---------------------------------------------------------------------------

    def _check_runner_specific_top_level_fields(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Inform when new definitions still use runner-specific top-level fields."""
        for analysis in ctx.steps:
            explicit_fields = explicit_runner_specific_fields(analysis.step)
            if not explicit_fields:
                continue
            for field_name in explicit_fields:
                policy = get_field_migration_policy(field_name)
                if policy is None:
                    continue
                result.add_info(
                    f"Step '{analysis.step.id}': top-level runner field '{field_name}' is a "
                    f"compatibility input; prefer '{policy.canonical_home}' for brick-first definitions "
                    f"(T-2.1.2)",
                    hint=(
                        f"Move '{field_name}' under config when creating or updating this step. "
                        "Keep the top-level form only for historical compatibility."
                    ),
                    code="RUNNER_TOP_LEVEL_FIELD_COMPAT",
                    step_id=analysis.step.id,
                    field=field_name,
                    why="Brick schemas should own runner-specific semantics; Step fields remain transitional.",
                    suggestion={
                        "kind": "move_to_config",
                        "from": field_name,
                        "to": policy.canonical_home,
                    },
                )

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-07: db.query used for DML
    # ---------------------------------------------------------------------------

    def _check_db_query_dml(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when db.query step contains DML statements."""
        _DML_PATTERN = re.compile(
            r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE)\b',
            re.IGNORECASE,
        )
        policy_level = self._effective_policy_level(ctx.pipeline)

        for analysis in ctx.steps:
            step = analysis.step
            effective_type = analysis.effective_type
            if effective_type != "db.query":
                continue

            query = getattr(step, "query", None) or ""
            if not query:
                query = (
                    analysis.param_get("query", "")
                    or analysis.config_get("query", "")
                    or ""
                )

            # Skip Jinja2 dynamic queries — can't statically check
            if not query or self._is_dynamic_ref(query):
                continue

            match = _DML_PATTERN.search(query)
            if match:
                message = (
                    f"Step '{step.id}': db.query contains DML statement "
                    f"'{match.group(1).upper()}' — use db.exec for DML operations (T-BRIX-VAL-07)"
                )
                common_kwargs = {
                    "hint": "Use db.exec for UPDATE/DELETE/INSERT",
                    "code": "DB_QUERY_DML",
                    "step_id": step.id,
                    "field": "query",
                    "why": "db.query is SELECT-only, no commit",
                    "schema_ref": 'get_brick_schema(name="db.exec")',
                }
                if policy_level in {"strict", "locked"}:
                    result.add_error(message, **common_kwargs)
                else:
                    result.add_warning(message, **common_kwargs)

    def _check_workaround_patterns(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Surface known workaround patterns derived from validator findings."""
        del ctx  # pattern matching uses the accumulated findings
        existing_keys = {
            (finding.code, finding.step_id, finding.field, finding.message)
            for finding in result.findings
        }
        for match in detect_workaround_pattern_matches(result.findings):
            target = f"Step '{match.step_id}': " if match.step_id else ""
            message = (
                f"{target}matches known workaround pattern '{match.pattern.name}' — "
                f"{match.pattern.description}"
            )
            finding_key = ("KNOWN_WORKAROUND_PATTERN", match.step_id, match.field, message)
            if finding_key in existing_keys:
                continue
            severity = match.finding_severity
            if severity not in {"error", "warning", "info"}:
                severity = match.pattern.severity
            result.add_finding(
                code="KNOWN_WORKAROUND_PATTERN",
                severity=severity,
                category="lint",
                message=message,
                step_id=match.step_id,
                field=match.field,
                why=match.pattern.rationale or "Known workaround patterns must remain visible to validator and gatekeeper surfaces.",
                hint=match.pattern.repair_hint,
                suggestion={
                    "kind": "review_workaround_pattern",
                    "pattern": match.pattern.name,
                    "source_finding": match.finding_code,
                },
            )
            existing_keys.add(finding_key)

    def _check_workaround_annotation(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Require owner/replacement metadata when a workaround pattern is present."""
        metadata = dict(ctx.pipeline_metadata.get("entity_metadata") or {})
        assessment = assess_workaround_annotation(result.findings, metadata)
        if not assessment.blocking:
            return

        message = (
            "Pipeline carries known workaround patterns "
            f"{list(assessment.patterns)} but is missing metadata {list(assessment.missing_fields)}."
        )
        kwargs = {
            "code": "WORKAROUND_ANNOTATION_MISSING",
            "field": "metadata",
            "why": "Temporary workaround debt must be explicit, owned, and time-bounded.",
            "hint": "Set owner, replacement_plan, and expiry_condition via update_pipeline metadata fields.",
            "suggestion": {
                "kind": "update_pipeline_metadata",
                "missing_fields": list(assessment.missing_fields),
                "patterns": list(assessment.patterns),
            },
        }
        if self._effective_policy_level(ctx.pipeline) in {"strict", "locked"}:
            result.add_error(message, **kwargs)
        else:
            result.add_warning(message, **kwargs)

    # ---------------------------------------------------------------------------
    # T-BRIX-VAL-08: Duplicate step IDs across sub-pipelines
    # ---------------------------------------------------------------------------

    def _check_duplicate_ids_across_sub_pipelines(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn on step ID collisions between parent and sub-pipelines."""
        store = PipelineStore()
        parent_ids = ctx.known_step_ids

        for analysis in ctx.steps:
            step = analysis.step
            if analysis.effective_type != "flow.pipeline":
                continue
            pipeline_name = (
                getattr(step, "pipeline", None)
                or analysis.param_get("pipeline")
                or analysis.config_get("pipeline")
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

    def _check_large_helper_without_schema(self, ctx: ValidationContext, result: ValidationResult) -> None:
        """Warn when a helper has substantial code but no input_schema."""
        from brix.helper_registry import HelperRegistry
        registry = HelperRegistry()
        checked: set[str] = set()

        for analysis in ctx.steps:
            step = analysis.step
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

    def _check_cross_helper_imports(self, ctx: ValidationContext, result: ValidationResult) -> None:
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
        for analysis in ctx.steps:
            step = analysis.step
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
