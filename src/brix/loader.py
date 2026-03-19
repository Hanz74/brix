"""YAML pipeline loader with Jinja2 template support."""

import ast
import json

import yaml
from jinja2 import Undefined
from jinja2.sandbox import SandboxedEnvironment

from brix.models import Pipeline, Step


class PipelineLoader:
    """Loads pipeline YAML files and renders Jinja2 templates."""

    def __init__(self) -> None:
        # SandboxedEnvironment prevents arbitrary code execution (D-13).
        # Undefined variables silently become empty strings instead of raising
        # an error — callers use | default() when they need an explicit fallback.
        self.env = SandboxedEnvironment(undefined=Undefined)

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def load(self, path: str) -> Pipeline:
        """Load a pipeline YAML file and parse into a Pipeline model."""
        with open(path) as f:
            raw = yaml.safe_load(f)
        return Pipeline.model_validate(raw)

    def load_from_string(self, yaml_string: str) -> Pipeline:
        """Load a pipeline from a YAML string."""
        raw = yaml.safe_load(yaml_string)
        return Pipeline.model_validate(raw)

    # ------------------------------------------------------------------
    # Jinja2 rendering
    # ------------------------------------------------------------------

    def render_template(self, template_string: str, context: dict) -> str:
        """Render a Jinja2 template string with the given context.

        User input is NEVER passed through Jinja2 — only pipeline-internal
        references (D-13).  Context typically contains: ``input.*``,
        ``credentials.*``, ``<step_id>.output``, and ``item``.
        """
        template = self.env.from_string(template_string)
        return template.render(context)

    def render_value(self, value, context: dict):
        """Render a value recursively.

        - ``str`` containing ``{{`` → rendered as Jinja2 template; if the
          result is valid JSON it is parsed and returned as the native type.
        - ``dict`` / ``list`` → recursed element by element.
        - Everything else → returned unchanged.
        """
        if isinstance(value, str) and "{{" in value:
            rendered = self.render_template(value, context)
            try:
                return json.loads(rendered)
            except (json.JSONDecodeError, ValueError):
                return rendered
        elif isinstance(value, dict):
            return {k: self.render_value(v, context) for k, v in value.items()}
        elif isinstance(value, list):
            return [self.render_value(item, context) for item in value]
        return value

    def render_step_params(self, step: Step, context: dict) -> dict:
        """Render all Jinja2 templates in a step's parameters.

        Returns a dict with the rendered ``params`` merged with rendered
        type-specific fields stored under ``_url``, ``_command``, ``_args``,
        and ``_headers`` keys.
        """
        rendered: dict = {}

        if step.params:
            rendered = self.render_value(step.params, context)

        # Render type-specific fields into reserved underscore keys so that
        # callers can retrieve them without risk of collision with user-defined
        # param names.
        if step.url:
            rendered["_url"] = self.render_value(step.url, context)
        if step.command:
            rendered["_command"] = self.render_value(step.command, context)
        if step.args:
            rendered["_args"] = self.render_value(step.args, context)
        if step.headers:
            rendered["_headers"] = self.render_value(step.headers, context)

        return rendered

    # ------------------------------------------------------------------
    # Condition / foreach helpers
    # ------------------------------------------------------------------

    def evaluate_condition(self, condition: str | None, context: dict) -> bool:
        """Evaluate a ``when`` condition.

        Returns ``True`` if the step should execute.  An empty or ``None``
        condition always returns ``True``.  The rendered string is compared
        against a set of canonical falsy representations.
        """
        if not condition:
            return True
        rendered = self.render_template(condition, context)
        return rendered.lower() not in ("false", "0", "", "none", "[]", "{}")

    def resolve_foreach(self, foreach_expr: str, context: dict) -> list:
        """Resolve a ``foreach`` expression to a Python list.

        Accepts:
        - A template that renders directly to a ``list``.
        - A template whose rendered result is a JSON-encoded list string.

        Raises ``ValueError`` for anything else.
        """
        result = self.render_value(foreach_expr, context)

        if isinstance(result, list):
            return result

        if isinstance(result, str):
            # Try JSON first (canonical representation)
            try:
                parsed = json.loads(result)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass

            # Fallback: Python literal repr (e.g. "['a', 'b']" from Jinja2 str coercion)
            try:
                parsed = ast.literal_eval(result)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, SyntaxError):
                pass

        raise ValueError(
            f"foreach expression did not resolve to a list: {foreach_expr!r} → {result!r}"
        )
