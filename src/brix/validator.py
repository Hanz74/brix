"""Pipeline validation without execution."""
import os
import re
from pathlib import Path
from typing import Any, Optional

import jinja2
from jinja2 import nodes
from jinja2.visitor import NodeVisitor
import jsonschema
from jsonschema import ValidationError
import yaml

from brix.models import Pipeline
from brix.cache import SchemaCache
from brix.bricks.registry import BrickRegistry
from brix.engine import LEGACY_ALIASES
from brix.pipeline_store import PipelineStore
from brix.connections import ConnectionManager


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


class ValidationResult:
    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.checks: list[str] = []  # successful checks

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def add_check(self, msg: str):
        self.checks.append(msg)

    def add_error(self, msg: str):
        self.errors.append(msg)

    def add_warning(self, msg: str):
        self.warnings.append(msg)


class PipelineValidator:
    def __init__(self, cache: SchemaCache = None, lint_rules: list = None):
        self.cache = cache or SchemaCache()
        # lint_rules: explicit list (for testing), otherwise load from disk + defaults
        self._lint_rules: list[dict] | None = lint_rules
        self._runner_config_schemas: dict[str, dict] | None = None

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

        # 1. Step IDs unique
        step_ids = [s.id for s in pipeline.steps]
        if len(step_ids) != len(set(step_ids)):
            result.add_error("Duplicate step IDs found")
        else:
            result.add_check("Step IDs are unique")

        # 2. Step references valid (no dangling {{ step.output }})
        for step in pipeline.steps:
            self._check_step_references(step, step_ids, pipeline, result)

        # 3. MCP steps have server + tool
        for step in pipeline.steps:
            if step.type == "mcp":
                if not step.server:
                    result.add_error(f"Step '{step.id}': MCP step needs 'server'")
                if not step.tool:
                    result.add_error(f"Step '{step.id}': MCP step needs 'tool'")
                # Check if server is registered
                if step.server:
                    servers_path = Path.home() / ".brix" / "servers.yaml"
                    if servers_path.exists():
                        import yaml
                        data = yaml.safe_load(servers_path.read_text()) or {}
                        if step.server not in data.get("servers", {}):
                            result.add_warning(
                                f"Step '{step.id}': Server '{step.server}' not registered"
                            )
                    else:
                        result.add_warning(
                            f"No servers.yaml found — cannot verify server '{step.server}'"
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
            env_val = os.environ.get(cred.env)
            if env_val:
                result.add_check(f"Credential '{key}' (env: {cred.env}): set")
            else:
                result.add_warning(f"Credential '{key}' (env: {cred.env}): NOT SET")

        # 6. when + default check
        for step in pipeline.steps:
            if step.when:
                self._check_when_default(step, pipeline.steps, result)

        # 8. MCP step params vs cached tool schema (required params)
        for step in pipeline.steps:
            if step.type == "mcp" and step.server and step.tool:
                self._check_mcp_params(step, result)

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
        for step in pipeline.steps:
            if step.on_error == "continue" and step.type in ("http", "mcp"):
                result.add_warning(
                    f"Step '{step.id}': on_error: continue on a {step.type} step — "
                    f"consider on_error: retry for transient errors."
                )

        # 9b. Helper references — check registry and validate input_schema (T-BRIX-V4-BUG-12)
        for step in pipeline.steps:
            if getattr(step, "helper", None):
                self._check_helper_reference(step, result)

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
            self._check_brick_config_schema(pipeline, result)
            self._check_jinja_ast(pipeline, result)
            self._check_sub_pipeline_existence(pipeline, result)
            self._check_connection_existence(pipeline, result)

        # 15. Deep preflight: active connection tests
        if level == "deep":
            self._check_connection_health(pipeline, result)

        if result.is_valid:
            result.add_check("Pipeline is valid")

        return result

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

    def _check_step_references(self, step, all_step_ids, pipeline, result):
        """Check that step references point to earlier steps."""
        step_idx = next(i for i, s in enumerate(pipeline.steps) if s.id == step.id)
        earlier_ids = set(all_step_ids[:step_idx])
        input_keys = set(pipeline.input.keys())

        # Check foreach, params, when, etc. for {{ step_id.output }} references
        fields_to_check = [step.foreach]
        if step.params:
            fields_to_check.extend(str(v) for v in step.params.values())
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
                                f"Step '{step.id}' references future step '{ref}'"
                            )
                        elif ref not in ["item", "credentials"]:
                            result.add_warning(
                                f"Step '{step.id}' references unknown '{ref}'"
                            )

    def _check_when_default(self, when_step, all_steps, result):
        """Warn if a conditional step is referenced without | default."""
        for step in all_steps:
            if step.id == when_step.id:
                continue
            fields_to_check = []
            if step.params:
                fields_to_check.extend(str(v) for v in step.params.values())
            if step.foreach:
                fields_to_check.append(step.foreach)

            for field in fields_to_check:
                if when_step.id in str(field) and "output" in str(field):
                    if "default" not in str(field):
                        result.add_warning(
                            f"Step '{step.id}' references conditional step '{when_step.id}' "
                            f"without | default() — may fail if skipped (D-16)"
                        )

    def _check_helper_reference(self, step, result) -> None:
        """Validate a step's ``helper`` field against the HelperRegistry (T-BRIX-V4-BUG-12).

        Checks:
        - The referenced helper exists in the registry.
        - If the helper declares an ``input_schema``, warn about step params that
          do not appear in the schema (schema mismatch) — skips Jinja2 templates.
        """
        from brix.helper_registry import HelperRegistry
        registry = HelperRegistry()
        entry = registry.get(step.helper)

        if entry is None:
            result.add_error(
                f"Step '{step.id}': Helper '{step.helper}' not found in registry. "
                f"Register it with: brix__register_helper"
            )
            return

        result.add_check(f"Step '{step.id}': Helper '{step.helper}' found in registry")

        # Schema validation — warn on params not declared in input_schema
        input_schema = entry.input_schema or {}
        schema_properties = input_schema.get("properties", {})
        if schema_properties and step.params:
            for param_key, param_val in step.params.items():
                # Skip Jinja2-template values — considered dynamically supplied
                if "{{" in str(param_val):
                    continue
                if param_key not in schema_properties:
                    result.add_warning(
                        f"Step '{step.id}': param '{param_key}' is not declared in "
                        f"helper '{step.helper}' input_schema (T-BRIX-V4-BUG-12)"
                    )

    def _check_mcp_params(self, step, result):
        """Warn if required MCP tool params are not supplied in the step definition (T-BRIX-V4-21).

        Looks up the cached tool schema and checks whether any schema-required
        params are missing from the step's params dict.  Params that use Jinja2
        templates (``{{ ... }}``) are considered dynamically supplied and are
        not flagged.
        """
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

        provided_keys = set(step.params.keys()) if step.params else set()
        for req_key in schema_required:
            if req_key not in provided_keys:
                result.add_warning(
                    f"Step '{step.id}': MCP tool '{step.tool}' requires param '{req_key}' "
                    f"but it is not set in step params (T-BRIX-V4-21)"
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
        step_type = getattr(step, "type", "") or ""
        effective_type = LEGACY_ALIASES.get(step_type, step_type)

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
        for step in pipeline.steps:
            schema = self._resolve_step_schema(step)
            if not schema:
                continue
            instance = self._step_to_validation_config(step)
            try:
                jsonschema.validate(instance=instance, schema=schema)
            except ValidationError as exc:
                result.add_warning(
                    f"Step '{step.id}': config does not match schema: {exc.message}"
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
        known_names = {step.id for step in pipeline.steps}
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
                        f"Step '{step.id}': Jinja2 syntax error in {field_path}: {exc}"
                    )
                    continue

                visitor = self._JinjaRootNameVisitor()
                visitor.visit(ast)
                unknown = sorted(name for name in visitor.names if name not in known_names)
                for name in unknown:
                    result.add_warning(
                        f"Step '{step.id}': Jinja2 template in {field_path} references unknown name '{name}'"
                    )

    @staticmethod
    def _is_dynamic_ref(value: Any) -> bool:
        return isinstance(value, str) and any(token in value for token in ("{{", "{%", "{#"))

    def _check_sub_pipeline_existence(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Ensure statically referenced sub-pipelines exist."""
        store = PipelineStore()
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
                store.load(pipeline_name)
            except FileNotFoundError:
                result.add_error(
                    f"Step '{step.id}': sub-pipeline '{pipeline_name}' does not exist"
                )

    def _collect_connection_names(self, pipeline: Pipeline) -> set[str]:
        names: set[str] = set()
        for step in pipeline.steps:
            params = getattr(step, "params", None) or {}
            config = getattr(step, "config", None) or {}
            connection_name = (
                getattr(step, "connection", None)
                or (params.get("connection") if isinstance(params, dict) else None)
                or (config.get("connection") if isinstance(config, dict) else None)
            )
            if not connection_name or self._is_dynamic_ref(connection_name):
                continue
            names.add(connection_name)
        return names

    def _check_connection_existence(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Ensure statically referenced named connections exist."""
        names = self._collect_connection_names(pipeline)
        if not names:
            return

        try:
            from brix.db import BrixDB

            manager = ConnectionManager(BrixDB())
            known = {item.get("name") for item in manager.list()}
        except Exception as exc:
            result.add_warning(f"Could not verify connection existence: {exc}")
            return

        for name in sorted(names):
            if name not in known:
                result.add_error(f"Connection '{name}' does not exist")

    def _check_connection_health(self, pipeline: Pipeline, result: ValidationResult) -> None:
        """Active connection ping for deep validation mode."""
        names = self._collect_connection_names(pipeline)
        if not names:
            return

        try:
            from brix.db import BrixDB

            manager = ConnectionManager(BrixDB())
        except Exception as exc:
            result.add_warning(f"Could not initialize connection tests: {exc}")
            return

        for name in sorted(names):
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
                fields_to_scan.extend(str(v) for v in step.params.values())
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
                for k, v in step.params.items():
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
