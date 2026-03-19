"""Pipeline validation without execution."""
import os
import re
from pathlib import Path
from typing import Optional

from brix.models import Pipeline
from brix.cache import SchemaCache


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
    def __init__(self, cache: SchemaCache = None):
        self.cache = cache or SchemaCache()

    def validate(self, pipeline: Pipeline, pipeline_dir: Path = None) -> ValidationResult:
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

        # 7. Output references valid
        if pipeline.output:
            for key, ref in pipeline.output.items():
                for step_id in step_ids:
                    if step_id in ref:
                        break
                else:
                    if "{{" in ref:
                        result.add_warning(
                            f"Output '{key}': may reference non-existent step"
                        )

        if result.is_valid:
            result.add_check("Pipeline is valid")

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
